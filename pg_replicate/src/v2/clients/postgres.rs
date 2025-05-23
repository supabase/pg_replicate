use pg_escape::{quote_identifier, quote_literal};
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use postgres::tokio::options::PgDatabaseOptions;
use postgres_replication::LogicalReplicationStream;
use rustls::{pki_types::CertificateDer, ClientConfig};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{
    config::ReplicationMode, types::PgLsn, Client, Config, Connection, CopyOutStream, NoTls,
    SimpleQueryMessage, Socket,
};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{info, warn};

/// Spawns a background task to monitor a PostgreSQL connection until it terminates.
///
/// The task will log when the connection terminates, either successfully or with an error.
fn spawn_postgres_connection<T>(connection: Connection<Socket, T::Stream>)
where
    T: MakeTlsConnect<Socket>,
    T::Stream: Send + 'static,
{
    // TODO: maybe return a handle for this task to keep track of it.
    tokio::spawn(async move {
        info!("waiting for connection to terminate");
        if let Err(e) = connection.await {
            warn!("connection error: {}", e);
            return;
        }
        info!("connection terminated successfully")
    });
}

/// A logical replication slot in PostgreSQL that maintains a consistent point and snapshot.
///
/// The slot ensures that changes can be replicated from a consistent point in time,
/// represented by the `consistent_point` and `snapshot_name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgReplicationSlot {
    consistent_point: PgLsn,
    snapshot_name: String,
}

impl PgReplicationSlot {
    /// Returns the LSN (Log Sequence Number) that represents the consistent point of this slot.
    pub fn consistent_point(&self) -> &PgLsn {
        &self.consistent_point
    }

    /// Returns the name of the snapshot associated with this replication slot.
    pub fn snapshot_name(&self) -> &String {
        &self.snapshot_name
    }
}

/// A transaction that operates within the context of a replication slot.
///
/// This type ensures that the parent connection remains active for the duration of any
/// transaction spawned by that connection for a given slot.
///
/// The `parent_client` is the client that created the slot and must be active for the duration of
/// the transaction for the snapshot of the slot to be consistent.
#[derive(Debug)]
pub struct PgReplicationSlotTransaction {
    _parent_client: PgReplicationClient,
    client: PgReplicationClient,
}

impl PgReplicationSlotTransaction {
    /// Creates a new transaction within the context of a replication slot.
    ///
    /// The transaction is started with a repeatable read isolation level and uses the
    /// snapshot associated with the provided slot.
    async fn new(
        parent_client: PgReplicationClient,
        client: PgReplicationClient,
        slot: PgReplicationSlot,
    ) -> Result<Self, PgReplicationClientError> {
        client.begin_tx(&slot).await?;

        Ok(Self {
            _parent_client: parent_client,
            client,
        })
    }

    /// Retrieves the schema information for the specified tables.
    ///
    /// If a publication name is provided, only tables included in that publication
    /// will be considered.
    pub async fn get_table_schemas(
        &self,
        table_names: &[TableName],
        publication_name: Option<&str>,
    ) -> Result<HashMap<TableId, TableSchema>, PgReplicationClientError> {
        self.client
            .get_table_schemas(table_names, publication_name)
            .await
    }

    /// Creates a COPY stream for reading data from the specified table.
    ///
    /// The stream will include only the columns specified in `column_schemas`.
    pub async fn get_table_copy_stream(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<CopyOutStream, PgReplicationClientError> {
        self.client
            .get_table_copy_stream(table_name, column_schemas)
            .await
    }

    /// Commits the current transaction.
    pub async fn commit(self) -> Result<(), PgReplicationClientError> {
        self.client.commit_tx().await
    }

    /// Rolls back the current transaction.
    pub async fn rollback(self) -> Result<(), PgReplicationClientError> {
        self.client.rollback_tx().await
    }
}

/// Internal state for a PostgreSQL replication client.
#[derive(Debug)]
struct ClientInner {
    client: Client,
    options: PgDatabaseOptions,
    trusted_root_certs: Vec<CertificateDer<'static>>,
    with_tls: bool,
}

/// A client for interacting with PostgreSQL's logical replication features.
///
/// This client provides methods for creating replication slots, managing transactions,
/// and streaming changes from the database.
#[derive(Debug, Clone)]
pub struct PgReplicationClient {
    inner: Arc<ClientInner>,
}

/// Errors that can occur when using the PostgreSQL replication client.
#[derive(Debug, Error)]
pub enum PgReplicationClientError {
    #[error("tokio_postgres error: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),

    #[error("the replication slot response did not contain the expected fields")]
    ReplicationSlotResponseInvalid,

    #[error("the replication slot creation failed")]
    ReplicationSlotCreationFailed,

    #[error("the postgres lsn is not valid")]
    InvalidPgLsn,

    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),

    #[error("table {0} doesn't exist")]
    MissingTable(TableName),

    #[error("column {0} is missing from table {1}")]
    MissingColumn(String, String),

    #[error("publication {0} doesn't exist")]
    MissingPublication(String),

    #[error("oid column is not a valid u32")]
    OidColumnNotU32,

    #[error("replica identity '{0}' not supported")]
    ReplicaIdentityNotSupported(String),

    #[error("type modifier column is not a valid u32")]
    TypeModifierColumnNotI32,

    #[error("column {0}'s type with oid {1} in relation {2} is not supported")]
    UnsupportedType(String, u32, String),
}

impl PgReplicationClient {
    /// Establishes a connection to PostgreSQL without TLS encryption.
    ///
    /// The connection is configured for logical replication mode.
    pub async fn connect_no_tls(
        options: PgDatabaseOptions,
    ) -> Result<Self, PgReplicationClientError> {
        info!("connecting to postgres without TLS");

        let mut config: Config = options.clone().into();
        config.replication_mode(ReplicationMode::Logical);

        let (client, connection) = config.connect(NoTls).await?;
        spawn_postgres_connection::<NoTls>(connection);

        info!("successfully connected to postgres without TLS");

        let inner = ClientInner {
            client,
            options,
            trusted_root_certs: vec![],
            with_tls: false,
        };
        Ok(PgReplicationClient {
            inner: Arc::new(inner),
        })
    }

    /// Establishes a TLS-encrypted connection to PostgreSQL.
    ///
    /// The connection is configured for logical replication mode and uses the provided
    /// trusted root certificates for TLS verification.
    pub async fn connect_tls(
        options: PgDatabaseOptions,
        trusted_root_certs: Vec<CertificateDer<'static>>,
    ) -> Result<Self, PgReplicationClientError> {
        info!("connecting to postgres with TLS");

        let mut config: Config = options.clone().into();
        config.replication_mode(ReplicationMode::Logical);

        let mut root_store = rustls::RootCertStore::empty();
        for trusted_root_cert in trusted_root_certs.iter() {
            root_store.add(trusted_root_cert.clone())?;
        }
        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let (client, connection) = config.connect(MakeRustlsConnect::new(tls_config)).await?;
        spawn_postgres_connection::<MakeRustlsConnect>(connection);

        info!("successfully connected to postgres with TLS");

        let inner = ClientInner {
            client,
            options,
            trusted_root_certs,
            with_tls: true,
        };
        Ok(PgReplicationClient {
            inner: Arc::new(inner),
        })
    }

    /// Creates a new logical replication slot with the specified name.
    ///
    /// The slot is created with the `pgoutput` plugin and exports a snapshot for
    /// consistent replication.
    pub async fn create_slot(
        &self,
        slot_name: &str,
    ) -> Result<PgReplicationSlot, PgReplicationClientError> {
        let query = format!(
            r#"CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT"#,
            quote_identifier(slot_name)
        );

        let results = self.inner.client.simple_query(&query).await?;
        for result in results {
            if let SimpleQueryMessage::Row(row) = result {
                let consistent_point: PgLsn = row
                    .get("consistent_point")
                    .ok_or(PgReplicationClientError::ReplicationSlotResponseInvalid)?
                    .parse()
                    .map_err(|_| PgReplicationClientError::InvalidPgLsn)?;

                let snapshot_name: String = row
                    .get("snapshot_name")
                    .ok_or(PgReplicationClientError::ReplicationSlotResponseInvalid)?
                    .parse()
                    .map_err(|_| PgReplicationClientError::InvalidPgLsn)?;

                return Ok(PgReplicationSlot {
                    snapshot_name,
                    consistent_point,
                });
            }
        }

        Err(PgReplicationClientError::ReplicationSlotCreationFailed)
    }

    /// Creates a new transaction within the context of a replication slot.
    ///
    /// The transaction will use the snapshot associated with the provided slot.
    pub async fn with_slot(
        &self,
        slot: PgReplicationSlot,
    ) -> Result<PgReplicationSlotTransaction, PgReplicationClientError> {
        let new_client = if self.inner.with_tls {
            PgReplicationClient::connect_tls(
                self.inner.options.clone(),
                self.inner.trusted_root_certs.clone(),
            )
            .await?
        } else {
            PgReplicationClient::connect_no_tls(self.inner.options.clone()).await?
        };

        PgReplicationSlotTransaction::new(self.clone(), new_client, slot).await
    }

    /// Checks if a publication with the given name exists.
    pub async fn publication_exists(
        &self,
        publication: &str,
    ) -> Result<bool, PgReplicationClientError> {
        let publication_exists_query = format!(
            "select 1 as exists from pg_publication where pubname = {};",
            quote_literal(publication)
        );
        for msg in self
            .inner
            .client
            .simple_query(&publication_exists_query)
            .await?
        {
            if let SimpleQueryMessage::Row(_) = msg {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Retrieves the names of all tables included in a publication.
    pub async fn get_publication_table_names(
        &self,
        publication_name: &str,
    ) -> Result<Vec<TableName>, PgReplicationClientError> {
        let publication_query = format!(
            "select schemaname, tablename from pg_publication_tables where pubname = {};",
            quote_literal(publication_name)
        );

        let mut table_names = vec![];
        for msg in self.inner.client.simple_query(&publication_query).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                let schema = row
                    .get(0)
                    .ok_or(PgReplicationClientError::MissingColumn(
                        "schemaname".to_string(),
                        "pg_publication_tables".to_string(),
                    ))?
                    .to_string();

                let name = row
                    .get(1)
                    .ok_or(PgReplicationClientError::MissingColumn(
                        "tablename".to_string(),
                        "pg_publication_tables".to_string(),
                    ))?
                    .to_string();

                table_names.push(TableName { schema, name })
            }
        }

        Ok(table_names)
    }

    /// Starts a logical replication stream from the specified publication and slot.
    ///
    /// The stream will begin reading changes from the provided `start_lsn`.
    pub async fn start_logical_replication(
        &self,
        publication_name: &str,
        slot_name: &str,
        start_lsn: PgLsn,
    ) -> Result<LogicalReplicationStream, PgReplicationClientError> {
        let options = format!(
            r#"("proto_version" '1', "publication_names" {})"#,
            quote_literal(quote_identifier(publication_name).as_ref()),
        );

        let query = format!(
            r#"START_REPLICATION SLOT {} LOGICAL {} {}"#,
            quote_identifier(slot_name),
            start_lsn,
            options
        );

        let copy_stream = self
            .inner
            .client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await?;

        let stream = LogicalReplicationStream::new(copy_stream);

        Ok(stream)
    }

    /// Begins a new transaction with repeatable read isolation level.
    ///
    /// The transaction will use the snapshot associated with the provided slot.
    async fn begin_tx(&self, slot: &PgReplicationSlot) -> Result<(), PgReplicationClientError> {
        self.inner
            .client
            .simple_query("begin read only isolation level repeatable read;")
            .await?;

        let query = format!("set transaction snapshot '{}'", slot.snapshot_name);
        self.inner.client.simple_query(&query).await?;

        Ok(())
    }

    /// Commits the current transaction.
    async fn commit_tx(&self) -> Result<(), PgReplicationClientError> {
        self.inner.client.simple_query("commit;").await?;
        Ok(())
    }

    /// Rolls back the current transaction.
    async fn rollback_tx(&self) -> Result<(), PgReplicationClientError> {
        self.inner.client.simple_query("rollback;").await?;
        Ok(())
    }

    /// Retrieves schema information for multiple tables.
    ///
    /// Tables without primary keys will be skipped and logged with a warning.
    async fn get_table_schemas(
        &self,
        table_names: &[TableName],
        publication_name: Option<&str>,
    ) -> Result<HashMap<TableId, TableSchema>, PgReplicationClientError> {
        let mut table_schemas = HashMap::new();

        // TODO: consider if we want to fail when at least one table was missing or not.
        for table_name in table_names {
            let table_schema = self
                .get_table_schema(table_name.clone(), publication_name)
                .await?;

            if !table_schema.has_primary_keys() {
                warn!(
                    "table {} with id {} will not be copied because it has no primary key",
                    table_schema.table_name, table_schema.table_id
                );
                continue;
            }

            table_schemas.insert(table_schema.table_id, table_schema);
        }

        Ok(table_schemas)
    }

    /// Retrieves the schema for a single table.
    ///
    /// If a publication is specified, only columns included in that publication
    /// will be returned.
    async fn get_table_schema(
        &self,
        table_name: TableName,
        publication: Option<&str>,
    ) -> Result<TableSchema, PgReplicationClientError> {
        let table_id = self
            .get_table_id(&table_name)
            .await?
            .ok_or(PgReplicationClientError::MissingTable(table_name.clone()))?;

        let column_schemas = self.get_column_schemas(table_id, publication).await?;

        Ok(TableSchema {
            table_name,
            table_id,
            column_schemas,
        })
    }

    /// Retrieves the OID of a table and verifies its replica identity.
    ///
    /// Returns `None` if the table doesn't exist. The replica identity must be
    /// either 'd' (default) or 'f' (full).
    async fn get_table_id(
        &self,
        table: &TableName,
    ) -> Result<Option<TableId>, PgReplicationClientError> {
        let quoted_schema = quote_literal(&table.schema);
        let quoted_name = quote_literal(&table.name);

        let table_info_query = format!(
            "select c.oid,
                c.relreplident
            from pg_class c
            join pg_namespace n
                on (c.relnamespace = n.oid)
            where n.nspname = {}
                and c.relname = {}
            ",
            quoted_schema, quoted_name
        );

        for message in self.inner.client.simple_query(&table_info_query).await? {
            if let SimpleQueryMessage::Row(row) = message {
                let replica_identity =
                    row.try_get("relreplident")?
                        .ok_or(PgReplicationClientError::MissingColumn(
                            "relreplident".to_string(),
                            "pg_class".to_string(),
                        ))?;

                if !(replica_identity == "d" || replica_identity == "f") {
                    return Err(PgReplicationClientError::ReplicaIdentityNotSupported(
                        replica_identity.to_string(),
                    ));
                }

                let oid: u32 = row
                    .try_get("oid")?
                    .ok_or(PgReplicationClientError::MissingColumn(
                        "oid".to_string(),
                        "pg_class".to_string(),
                    ))?
                    .parse()
                    .map_err(|_| PgReplicationClientError::OidColumnNotU32)?;
                return Ok(Some(oid));
            }
        }

        Ok(None)
    }

    /// Retrieves schema information for all columns in a table.
    ///
    /// If a publication is specified, only columns included in that publication
    /// will be returned.
    async fn get_column_schemas(
        &self,
        table_id: TableId,
        publication: Option<&str>,
    ) -> Result<Vec<ColumnSchema>, PgReplicationClientError> {
        let (pub_cte, pub_pred) = if let Some(publication) = publication {
            (
                format!(
                    "with pub_attrs as (
                        select unnest(r.prattrs)
                        from pg_publication_rel r
                        left join pg_publication p on r.prpubid = p.oid
                        where p.pubname = {publication}
                        and r.prrelid = {table_id}
                    )",
                    publication = quote_literal(publication),
                ),
                "and (
                    case (select count(*) from pub_attrs)
                    when 0 then true
                    else (a.attnum in (select * from pub_attrs))
                    end
                )",
            )
        } else {
            ("".into(), "")
        };

        let column_info_query = format!(
            "{pub_cte}
            select a.attname,
                a.atttypid,
                a.atttypmod,
                a.attnotnull,
                coalesce(i.indisprimary, false) as primary
            from pg_attribute a
            left join pg_index i
                on a.attrelid = i.indrelid
                and a.attnum = any(i.indkey)
                and i.indisprimary = true
            where a.attnum > 0::int2
            and not a.attisdropped
            and a.attgenerated = ''
            and a.attrelid = {table_id}
            {pub_pred}
            order by a.attnum
            ",
        );

        let mut column_schemas = vec![];

        for message in self.inner.client.simple_query(&column_info_query).await? {
            if let SimpleQueryMessage::Row(row) = message {
                let name = row
                    .try_get("attname")?
                    .ok_or(PgReplicationClientError::MissingColumn(
                        "attname".to_string(),
                        "pg_attribute".to_string(),
                    ))?
                    .to_string();

                let type_oid = row
                    .try_get("atttypid")?
                    .ok_or(PgReplicationClientError::MissingColumn(
                        "atttypid".to_string(),
                        "pg_attribute".to_string(),
                    ))?
                    .parse()
                    .map_err(|_| PgReplicationClientError::OidColumnNotU32)?;

                //TODO: For now we assume all types are simple, fix it later
                let typ = Type::from_oid(type_oid).unwrap_or(Type::new(
                    format!("unnamed(oid: {type_oid})"),
                    type_oid,
                    Kind::Simple,
                    "pg_catalog".to_string(),
                ));

                let modifier = row
                    .try_get("atttypmod")?
                    .ok_or(PgReplicationClientError::MissingColumn(
                        "atttypmod".to_string(),
                        "pg_attribute".to_string(),
                    ))?
                    .parse()
                    .map_err(|_| PgReplicationClientError::TypeModifierColumnNotI32)?;

                let nullable =
                    row.try_get("attnotnull")?
                        .ok_or(PgReplicationClientError::MissingColumn(
                            "attnotnull".to_string(),
                            "pg_attribute".to_string(),
                        ))?
                        == "f";

                let primary =
                    row.try_get("primary")?
                        .ok_or(PgReplicationClientError::MissingColumn(
                            "indisprimary".to_string(),
                            "pg_index".to_string(),
                        ))?
                        == "t";

                column_schemas.push(ColumnSchema {
                    name,
                    typ,
                    modifier,
                    nullable,
                    primary,
                })
            }
        }

        Ok(column_schemas)
    }

    /// Creates a COPY stream for reading data from a table.
    ///
    /// The stream will include only the specified columns and use text format.
    async fn get_table_copy_stream(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<CopyOutStream, PgReplicationClientError> {
        let column_list = column_schemas
            .iter()
            .map(|col| quote_identifier(&col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let copy_query = format!(
            r#"COPY {} ({column_list}) TO STDOUT WITH (FORMAT text);"#,
            table_name.as_quoted_identifier(),
        );

        let stream = self.inner.client.copy_out_simple(&copy_query).await?;

        Ok(stream)
    }
}

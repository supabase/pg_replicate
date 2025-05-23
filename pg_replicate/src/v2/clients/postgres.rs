use pg_escape::{quote_identifier, quote_literal};
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use postgres::tokio::options::PgDatabaseOptions;
use rustls::{pki_types::CertificateDer, ClientConfig};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{
    config::ReplicationMode, types::PgLsn, Client, Config, Connection, NoTls, SimpleQueryMessage,
    Socket,
};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{info, warn};

// TODO: it would be cool if we could bind the slot to the connection instance, so that we could
//  enforce at compile time that you can only use the slot in the context of the connection since
//  connection that created the slot must be always active for that slot to be consistent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgReplicationSlot {
    consistent_point: PgLsn,
    snapshot_name: String,
}

#[derive(Debug)]
pub struct PgReplicationSlotTransaction {
    // We hold a reference to the parent client since that connection must be active for the whole
    // duration of any transaction spawned by that connection for a given slot.
    _parent_client: PgReplicationClient,
    client: PgReplicationClient,
}

impl PgReplicationSlotTransaction {
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

    pub async fn get_table_schemas(
        &self,
        table_names: &[TableName],
        publication_name: Option<&str>,
    ) -> Result<HashMap<TableId, TableSchema>, PgReplicationClientError> {
        self.client
            .get_table_schemas(table_names, publication_name)
            .await
    }

    pub async fn commit(self) -> Result<(), PgReplicationClientError> {
        self.client.commit_tx().await
    }

    pub async fn rollback(self) -> Result<(), PgReplicationClientError> {
        self.client.rollback_tx().await
    }
}

#[derive(Debug)]
struct ClientInner {
    client: Client,
    options: PgDatabaseOptions,
    trusted_root_certs: Vec<CertificateDer<'static>>,
    with_tls: bool,
}

#[derive(Debug, Clone)]
pub struct PgReplicationClient {
    inner: Arc<ClientInner>,
}

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

    // TODO: figure out a way to enforce the slot's lifetime with the connection's lifetime.
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

    async fn begin_tx(&self, slot: &PgReplicationSlot) -> Result<(), PgReplicationClientError> {
        self.inner
            .client
            .simple_query("begin read only isolation level repeatable read;")
            .await?;

        let query = format!("set transaction snapshot '{}'", slot.snapshot_name);
        self.inner.client.simple_query(&query).await?;

        Ok(())
    }

    pub async fn commit_tx(&self) -> Result<(), PgReplicationClientError> {
        self.inner.client.simple_query("commit;").await?;
        Ok(())
    }

    async fn rollback_tx(&self) -> Result<(), PgReplicationClientError> {
        self.inner.client.simple_query("rollback;").await?;
        Ok(())
    }

    async fn get_table_schemas(
        &self,
        table_names: &[TableName],
        publication_name: Option<&str>,
    ) -> Result<HashMap<TableId, TableSchema>, PgReplicationClientError> {
        let mut table_schemas = HashMap::new();

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
}

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

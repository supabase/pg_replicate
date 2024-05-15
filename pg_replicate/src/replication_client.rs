use std::collections::HashMap;

use thiserror::Error;
use tokio_postgres::{
    binary_copy::BinaryCopyOutStream,
    config::ReplicationMode,
    replication::LogicalReplicationStream,
    types::{PgLsn, Type},
    Client as PostgresClient, Config, NoTls, SimpleQueryMessage,
};
use tracing::{info, warn};

use crate::{
    escape::{quote_identifier, quote_literal},
    table::{ColumnSchema, TableId, TableName, TableSchema},
};

pub struct SlotInfo {
    pub confirmed_flush_lsn: PgLsn,
}

/// A client for Postgres logical replication
pub struct ReplicationClient {
    postgres_client: PostgresClient,
}

#[derive(Debug, Error)]
pub enum ReplicationClientError {
    #[error("tokio_postgres error: {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    #[error("column {0} is missing from table {1}")]
    MissingColumn(String, String),

    #[error("oid column is not a valid u32")]
    OidColumnNotU32,

    #[error("type modifier column is not a valid u32")]
    TypeModifierColumnNotI32,

    #[error("unsupported column type with oid {0} in relation id {1}")]
    UnsupportedType(u32, TableId),

    #[error("table {0} doesn't exist")]
    MissingTable(TableName),

    #[error("not a valid PgLsn")]
    InvalidPgLsn,

    #[error("failed to create slot")]
    FailedToCreateSlot,
}

impl ReplicationClient {
    /// Connect to a postgres database in logical replication mode without TLS
    pub async fn connect_no_tls(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: Option<String>,
    ) -> Result<ReplicationClient, ReplicationClientError> {
        info!("connecting to postgres");

        let mut config = Config::new();
        config
            .host(host)
            .port(port)
            .dbname(database)
            .user(username)
            .replication_mode(ReplicationMode::Logical);

        if let Some(password) = password {
            config.password(password);
        }

        let (postgres_client, connection) = config.connect(NoTls).await?;

        tokio::spawn(async move {
            info!("waiting for connection to terminate");
            if let Err(e) = connection.await {
                warn!("connection error: {}", e);
            }
        });

        info!("successfully connected to postgres");

        Ok(ReplicationClient { postgres_client })
    }

    /// Starts a read-only trasaction with repeatable read isolation level
    pub async fn begin_readonly_transaction(&self) -> Result<(), ReplicationClientError> {
        self.postgres_client
            .simple_query("begin read only isolation level repeatable read;")
            .await?;
        Ok(())
    }

    /// Commits a transaction
    pub async fn commit_txn(&self) -> Result<(), ReplicationClientError> {
        self.postgres_client.simple_query("commit;").await?;
        Ok(())
    }

    async fn rollback_txn(&self) -> Result<(), ReplicationClientError> {
        self.postgres_client.simple_query("rollback;").await?;
        Ok(())
    }

    /// Returns a [BinaryCopyOutStream] for a table
    pub async fn get_table_copy_stream(
        &self,
        table_name: &TableName,
        column_types: &[Type],
    ) -> Result<BinaryCopyOutStream, ReplicationClientError> {
        let copy_query = format!(
            r#"COPY {} TO STDOUT WITH (FORMAT binary);"#,
            table_name.as_quoted_identifier()
        );

        let stream = self.postgres_client.copy_out_simple(&copy_query).await?;
        let row_stream = BinaryCopyOutStream::new(stream, column_types);
        Ok(row_stream)
    }

    /// Returns a vector of columns of a table
    pub async fn get_column_schemas(
        &self,
        table_id: TableId,
    ) -> Result<Vec<ColumnSchema>, ReplicationClientError> {
        let column_info_query = format!(
            "SELECT a.attname,
                a.atttypid,
                a.atttypmod,
                a.attnotnull,
                a.attnum = ANY(i.indkey) is_identity
           FROM pg_catalog.pg_attribute a
           LEFT JOIN pg_catalog.pg_index i
                ON (i.indexrelid = pg_get_replica_identity_index({}))
          WHERE a.attnum > 0::pg_catalog.int2
            AND NOT a.attisdropped
            AND a.attrelid = {}
          ORDER BY a.attnum",
            table_id, table_id
        );

        let mut column_schemas = vec![];

        for message in self
            .postgres_client
            .simple_query(&column_info_query)
            .await?
        {
            if let SimpleQueryMessage::Row(row) = message {
                let name = row
                    .try_get("attname")?
                    .ok_or(ReplicationClientError::MissingColumn(
                        "attname".to_string(),
                        "pg_attribute".to_string(),
                    ))?
                    .to_string();

                let type_oid = row
                    .try_get("atttypid")?
                    .ok_or(ReplicationClientError::MissingColumn(
                        "atttypid".to_string(),
                        "pg_attribute".to_string(),
                    ))?
                    .parse()
                    .map_err(|_| ReplicationClientError::OidColumnNotU32)?;

                let typ = Type::from_oid(type_oid)
                    .ok_or(ReplicationClientError::UnsupportedType(type_oid, table_id))?;

                let modifier = row
                    .try_get("atttypmod")?
                    .ok_or(ReplicationClientError::MissingColumn(
                        "atttypmod".to_string(),
                        "pg_attribute".to_string(),
                    ))?
                    .parse()
                    .map_err(|_| ReplicationClientError::TypeModifierColumnNotI32)?;

                let nullable =
                    row.try_get("attnotnull")?
                        .ok_or(ReplicationClientError::MissingColumn(
                            "attnotnull".to_string(),
                            "pg_attribute".to_string(),
                        ))?
                        == "f";

                let identity =
                    row.try_get("is_identity")?
                        .ok_or(ReplicationClientError::MissingColumn(
                            "attnum".to_string(),
                            "pg_attribute".to_string(),
                        ))?
                        == "t";

                column_schemas.push(ColumnSchema {
                    name,
                    typ,
                    modifier,
                    nullable,
                    identity,
                })
            }
        }

        Ok(column_schemas)
    }

    pub async fn get_table_schemas(
        &self,
        table_names: &[TableName],
    ) -> Result<HashMap<TableId, TableSchema>, ReplicationClientError> {
        let mut table_schemas = HashMap::new();

        for table_name in table_names {
            let table_schema = self.get_table_schema(table_name.clone()).await?;
            table_schemas.insert(table_schema.table_id, table_schema);
        }

        Ok(table_schemas)
    }

    async fn get_table_schema(
        &self,
        table_name: TableName,
    ) -> Result<TableSchema, ReplicationClientError> {
        let table_id = self
            .get_table_id(&table_name)
            .await?
            .ok_or(ReplicationClientError::MissingTable(table_name.clone()))?;
        let column_schemas = self.get_column_schemas(table_id).await?;
        Ok(TableSchema {
            table_name,
            table_id,
            column_schemas,
        })
    }

    /// Returns the table id (called relation id in Postgres) of a table
    pub async fn get_table_id(
        &self,
        table: &TableName,
    ) -> Result<Option<TableId>, ReplicationClientError> {
        let quoted_schema = quote_literal(&table.schema);
        let quoted_name = quote_literal(&table.name);

        let table_id_query = format!(
            "SELECT c.oid
          FROM pg_catalog.pg_class c
          INNER JOIN pg_catalog.pg_namespace n
                ON (c.relnamespace = n.oid)
         WHERE n.nspname = {}
           AND c.relname = {};",
            quoted_schema, quoted_name
        );

        for msg in self.postgres_client.simple_query(&table_id_query).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                return Ok(Some(
                    row.get(0)
                        .ok_or(ReplicationClientError::MissingColumn(
                            "oid".to_string(),
                            "pg_namespace".to_string(),
                        ))?
                        .parse::<u32>()
                        .map_err(|_| ReplicationClientError::OidColumnNotU32)?,
                ));
            }
        }

        Ok(None)
    }

    /// Returns the slot info of an existing slot. The slot info currently only has the
    /// confirmed_flush_lsn column of the pg_replication_slots table.
    async fn get_slot(&self, slot_name: &str) -> Result<Option<SlotInfo>, ReplicationClientError> {
        let query = format!(
            r#"select confirmed_flush_lsn from pg_replication_slots where slot_name = {};"#,
            quote_literal(slot_name)
        );

        let query_result = self.postgres_client.simple_query(&query).await?;

        if let SimpleQueryMessage::Row(row) = &query_result[0] {
            let confirmed_flush_lsn = row
                .get("confirmed_flush_lsn")
                .ok_or(ReplicationClientError::MissingColumn(
                    "confirmed_flush_lsn".to_string(),
                    "pg_replication_slots".to_string(),
                ))?
                .parse()
                .map_err(|_| ReplicationClientError::InvalidPgLsn)?;

            Ok(Some(SlotInfo {
                confirmed_flush_lsn,
            }))
        } else {
            Ok(None)
        }
    }

    /// Creates a logical replication slot. This will only succeed if the postgres connection
    /// is in logical replication mode. Otherwise it will fail with the following error:
    /// `syntax error at or near "CREATE_REPLICATION_SLOT"``
    ///
    /// Returns the consistent_point column as slot info.
    async fn create_slot(&self, slot_name: &str) -> Result<SlotInfo, ReplicationClientError> {
        let query = format!(
            r#"CREATE_REPLICATION_SLOT {} LOGICAL pgoutput USE_SNAPSHOT"#,
            quote_identifier(slot_name)
        );
        let slot_query = self.postgres_client.simple_query(&query).await?;
        if let SimpleQueryMessage::Row(row) = &slot_query[0] {
            let consistent_point: PgLsn = row
                .get("consistent_point")
                .ok_or(ReplicationClientError::MissingColumn(
                    "consistent_point".to_string(),
                    "create_replication_slot".to_string(),
                ))?
                .parse()
                .map_err(|_| ReplicationClientError::InvalidPgLsn)?;
            Ok(SlotInfo {
                confirmed_flush_lsn: consistent_point,
            })
        } else {
            Err(ReplicationClientError::FailedToCreateSlot)
        }
    }

    /// Either return the slot info of an existing slot or creates a new
    /// slot and returns its slot info.
    pub async fn get_or_create_slot(
        &self,
        slot_name: &str,
    ) -> Result<SlotInfo, ReplicationClientError> {
        if let Some(slot_info) = self.get_slot(slot_name).await? {
            Ok(slot_info)
        } else {
            self.rollback_txn().await?;
            self.begin_readonly_transaction().await?;
            Ok(self.create_slot(slot_name).await?)
        }
    }

    /// Returns all table names in a publication
    pub async fn get_publication_table_names(
        &self,
        publication: &str,
    ) -> Result<Vec<TableName>, ReplicationClientError> {
        let publication_query = format!(
            "select schemaname, tablename from pg_publication_tables where pubname = {};",
            quote_literal(publication)
        );

        let mut table_names = vec![];
        for msg in self
            .postgres_client
            .simple_query(&publication_query)
            .await?
        {
            if let SimpleQueryMessage::Row(row) = msg {
                let schema = row
                    .get(0)
                    .ok_or(ReplicationClientError::MissingColumn(
                        "schemaname".to_string(),
                        "pg_publication_tables".to_string(),
                    ))?
                    .to_string();

                let name = row
                    .get(1)
                    .ok_or(ReplicationClientError::MissingColumn(
                        "tablename".to_string(),
                        "pg_publication_tables".to_string(),
                    ))?
                    .to_string();

                table_names.push(TableName { schema, name })
            }
        }

        Ok(table_names)
    }

    pub async fn get_logical_replication_stream(
        &self,
        publication: &str,
        slot_name: &str,
        start_lsn: PgLsn,
    ) -> Result<LogicalReplicationStream, ReplicationClientError> {
        let options = format!(
            "(\"proto_version\" '1', \"publication_names\" {})",
            quote_literal(publication)
        );

        let query = format!(
            r#"START_REPLICATION SLOT {} LOGICAL {} {}"#,
            quote_identifier(slot_name),
            start_lsn,
            options
        );

        let copy_stream = self
            .postgres_client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await?;

        let stream = LogicalReplicationStream::new(copy_stream);

        Ok(stream)
    }
}

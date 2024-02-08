use std::{
    collections::HashMap,
    time::{Duration, UNIX_EPOCH},
};

use futures::StreamExt;
use postgres_protocol::message::backend::{LogicalReplicationMessage, ReplicationMessage, Tuple};
use thiserror::Error;
use tokio_postgres::{
    binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream},
    replication::LogicalReplicationStream,
    types::{PgLsn, Type},
    Client, NoTls, SimpleQueryMessage,
};

pub struct ReplicationClient {
    host: String,
    port: u16,
    dbname: String,
    user: String,
}

#[derive(Debug, Error)]
pub enum ReplicationClientError {
    #[error("tokio_postgres error: {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    #[error("unsupported replication message: {0:?}")]
    UnsupportedReplicationMessage(ReplicationMessage<LogicalReplicationMessage>),

    #[error("unsupported logical replication message: {0:?}")]
    UnsupportedLogicalReplicationMessage(LogicalReplicationMessage),

    #[error("relation id {0} not found")]
    RelationIdNotFound(u32),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Table {
    pub schema: String,
    pub name: String,
}

pub struct Attribute {
    pub name: String,
    pub typ: Type,
    pub nullable: bool,
}

pub struct TableSchema {
    pub table: Table,
    pub relation_id: u32,
    pub attributes: Vec<Attribute>,
}

pub enum RowEvent<'a> {
    Insert(Row<'a>),
    Update,
    Delete,
}

pub enum Row<'a> {
    CopyOut(BinaryCopyOutRow),
    Tuple(&'a Tuple),
}

impl ReplicationClient {
    pub fn new(host: String, port: u16, dbname: String, user: String) -> ReplicationClient {
        ReplicationClient {
            host,
            port,
            dbname,
            user,
        }
    }

    pub async fn connect(&self) -> Result<Client, ReplicationClientError> {
        let config = format!(
            "host={} port ={} dbname={} user={} replication=database",
            self.host, self.port, self.dbname, self.user
        );

        let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(client)
    }

    async fn get_publication_tables(
        client: &Client,
        publication: &str,
    ) -> Result<Vec<Table>, ReplicationClientError> {
        //TODO: use a non-replication connection to avoid SQL injection
        let publication_query =
            format!("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = '{publication}';");
        let tables = client
            .simple_query(&publication_query)
            .await?
            .into_iter()
            .filter_map(|msg| {
                if let SimpleQueryMessage::Row(row) = msg {
                    let schema = row
                        .get(0)
                        .expect("failed to read schemaname column")
                        .to_string();
                    let table = row
                        .get(1)
                        .expect("failed to read tablename column")
                        .to_string();
                    Some(Table {
                        schema,
                        name: table,
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Ok(tables)
    }

    async fn get_relation_id(
        client: &Client,
        table: &Table,
    ) -> Result<Option<u32>, ReplicationClientError> {
        let rel_id_query = format!(
            "SELECT c.oid
          FROM pg_catalog.pg_class c
          INNER JOIN pg_catalog.pg_namespace n
                ON (c.relnamespace = n.oid)
         WHERE n.nspname = '{}'
           AND c.relname = '{}';",
            table.schema, table.name
        );

        let rel_id = client
            .simple_query(&rel_id_query)
            .await?
            .into_iter()
            .find_map(|msg| {
                if let SimpleQueryMessage::Row(row) = msg {
                    Some(
                        row.get(0)
                            .expect("failed to read oid columns from pg_namesapce")
                            .parse::<u32>()
                            .expect("oid column in pg_namespace is not a valid u32"),
                    )
                } else {
                    None
                }
            });

        Ok(rel_id)
    }

    async fn get_table_attributes(
        client: &Client,
        relation_id: u32,
    ) -> Result<Vec<Attribute>, ReplicationClientError> {
        let col_info_query = format!(
            "SELECT a.attname,
                a.atttypid,
                a.atttypmod,
                a.attnotnull,
                a.attnum = ANY(i.indkey)
           FROM pg_catalog.pg_attribute a
           LEFT JOIN pg_catalog.pg_index i
                ON (i.indexrelid = pg_get_replica_identity_index({}))
          WHERE a.attnum > 0::pg_catalog.int2
            AND NOT a.attisdropped
            AND a.attrelid = {}
          ORDER BY a.attnum",
            relation_id, relation_id
        );

        let attributes = client
            .simple_query(&col_info_query)
            .await?
            .into_iter()
            .filter_map(|msg| {
                if let SimpleQueryMessage::Row(row) = msg {
                    let name = row
                        .get(0)
                        .expect("failed to get attribute name")
                        .to_string();
                    let typ = Type::from_oid(
                        row.get(1)
                            .expect("failed to get attribute oid")
                            .parse()
                            .expect("attribute oid is not a valid u32"),
                    )
                    .expect("attribute is not valid oid");
                    let nullable = row.get(3).expect("failed to get attribute nullability") == "f";
                    Some(Attribute {
                        name,
                        typ,
                        nullable,
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        Ok(attributes)
    }

    pub async fn get_schema(
        client: &Client,
        publication: &str,
    ) -> Result<Vec<TableSchema>, ReplicationClientError> {
        let tables = Self::get_publication_tables(client, publication).await?;
        let mut schema = Vec::new();

        for table in tables {
            let relation_id = Self::get_relation_id(client, &table).await?;
            if let Some(relation_id) = relation_id {
                let attributes = Self::get_table_attributes(client, relation_id).await?;
                schema.push(TableSchema {
                    table,
                    relation_id,
                    attributes,
                });
            } else {
                //TODO: handle missing relation_id case
            }
        }

        Ok(schema)
    }

    async fn begin_txn(client: &Client) -> Result<(), ReplicationClientError> {
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await?;
        Ok(())
    }

    async fn commit_txn(client: &Client) -> Result<(), ReplicationClientError> {
        client.simple_query("COMMIT;").await?;
        Ok(())
    }

    async fn create_replication_slot(
        client: &Client,
        slot_name: &str,
    ) -> Result<PgLsn, ReplicationClientError> {
        let query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} TEMPORARY LOGICAL pgoutput USE_SNAPSHOT"#,
            slot_name
        );
        let slot_query = client.simple_query(&query).await?;
        let consistent_point: PgLsn = if let SimpleQueryMessage::Row(row) = &slot_query[0] {
            row.get("consistent_point")
                .expect("failed to get consistent_point")
                .parse()
                .expect("failed to parse lsn")
        } else {
            panic!("unexpeced query message");
        };
        Ok(consistent_point)
    }

    async fn copy_table(
        client: &Client,
        table: &Table,
        attr_types: &[Type],
    ) -> Result<BinaryCopyOutStream, ReplicationClientError> {
        let copy_query = format!(
            r#"COPY "{}"."{}" TO STDOUT WITH (FORMAT binary);"#,
            table.schema, table.name
        );

        let stream = client.copy_out_simple(&copy_query).await?;
        let row_stream = BinaryCopyOutStream::new(stream, attr_types);
        Ok(row_stream)
    }

    async fn start_table_copy(
        client: &Client,
        slot_name: &str,
    ) -> Result<PgLsn, ReplicationClientError> {
        Self::begin_txn(client).await?;
        Self::create_replication_slot(client, slot_name).await
    }

    pub async fn get_table_snapshot<F: FnMut(RowEvent, &TableSchema)>(
        &self,
        client: &Client,
        schemas: &[TableSchema],
        mut f: F,
    ) -> Result<(String, PgLsn), ReplicationClientError> {
        let slot_name = "temp_slot".to_string();
        let consistent_point = Self::start_table_copy(client, &slot_name).await?;

        for schema in schemas {
            let types = schema
                .attributes
                .iter()
                .map(|attr| attr.typ.clone())
                .collect::<Vec<_>>();
            let rows = Self::copy_table(client, &schema.table, &types).await?;
            tokio::pin!(rows);
            while let Some(row) = rows.next().await {
                let row = row?;
                f(RowEvent::Insert(Row::CopyOut(row)), schema);
            }
        }

        Self::commit_txn(client).await?;

        Ok((slot_name, consistent_point))
    }

    pub async fn get_realtime_changes<F: FnMut(RowEvent, &TableSchema)>(
        &self,
        client: &Client,
        rel_id_to_schema: &HashMap<u32, &TableSchema>,
        publication: &str,
        slot_name: &str,
        consistent_point: PgLsn,
        mut f: F,
    ) -> Result<(), ReplicationClientError> {
        let logical_stream =
            Self::start_replication_slot(client, publication, slot_name, consistent_point).await?;

        tokio::pin!(logical_stream);

        const TIME_SEC_CONVERSION: u64 = 946_684_800;
        let postgres_epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);

        let mut last_lsn = consistent_point;

        while let Some(replication_msg) = logical_stream.next().await {
            match replication_msg? {
                ReplicationMessage::XLogData(xlog_data) => match xlog_data.into_data() {
                    LogicalReplicationMessage::Begin(_) => {}
                    LogicalReplicationMessage::Commit(commit) => {
                        last_lsn = commit.commit_lsn().into();
                    }
                    LogicalReplicationMessage::Origin(_) => {}
                    LogicalReplicationMessage::Relation(_) => {}
                    LogicalReplicationMessage::Type(_) => {}
                    LogicalReplicationMessage::Insert(insert) => {
                        match rel_id_to_schema.get(&insert.rel_id()) {
                            Some(schema) => {
                                f(RowEvent::Insert(Row::Tuple(insert.tuple())), schema);
                            }
                            None => {
                                //
                                return Err(ReplicationClientError::RelationIdNotFound(
                                    insert.rel_id(),
                                ));
                            }
                        }
                    }
                    LogicalReplicationMessage::Update(_) => {}
                    LogicalReplicationMessage::Delete(_) => {}
                    LogicalReplicationMessage::Truncate(_) => {}
                    msg => {
                        return Err(
                            ReplicationClientError::UnsupportedLogicalReplicationMessage(msg),
                        )
                    }
                },
                ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                    if keepalive.reply() == 1 {
                        let ts = postgres_epoch.elapsed().unwrap().as_micros() as i64;
                        logical_stream
                            .as_mut()
                            .standby_status_update(last_lsn, last_lsn, last_lsn, ts, 0)
                            .await?;
                    }
                }
                msg => return Err(ReplicationClientError::UnsupportedReplicationMessage(msg)),
            }
        }

        Ok(())
    }

    async fn start_replication_slot(
        client: &Client,
        publication: &str,
        slot_name: &str,
        consistent_point: PgLsn,
    ) -> Result<LogicalReplicationStream, ReplicationClientError> {
        let options = format!("(\"proto_version\" '1', \"publication_names\" '{publication}')");
        let query = format!(
            r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
            slot_name, consistent_point, options
        );
        let copy_stream = client.copy_both_simple::<bytes::Bytes>(&query).await?;
        let stream = LogicalReplicationStream::new(copy_stream);
        Ok(stream)
    }
}

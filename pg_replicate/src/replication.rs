use std::{
    collections::HashMap,
    time::{Duration, UNIX_EPOCH},
};

use futures::StreamExt;
use postgres_protocol::message::backend::{
    DeleteBody, InsertBody, LogicalReplicationMessage, RelationBody, ReplicationMessage, UpdateBody,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_postgres::{
    binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream},
    error::SqlState,
    replication::LogicalReplicationStream,
    types::{PgLsn, Type},
    Client, NoTls, SimpleQueryMessage,
};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
pub enum EventType {
    Begin,
    Insert,
    Update,
    Delete,
    Commit,
    Relation,
    Schema,
}

#[derive(Debug)]
pub struct ResumptionData {
    pub resume_lsn: PgLsn,
    pub last_event_type: EventType,
    pub last_file_name: u32,
    pub skipping_events: bool,
}

pub struct ReplicationClient {
    slot_name: String,
    pub consistent_point: PgLsn,
    postgres_client: Client,
    resumption_data: Option<ResumptionData>,
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

    #[error("resumption lsn {0} is older than the slot lsn {1}")]
    CantResume(PgLsn, PgLsn),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Table {
    pub schema: String,
    pub name: String,
}

pub struct Attribute {
    pub name: String,
    pub typ: Type,
    pub type_modifier: i32,
    pub nullable: bool,
    pub identity: bool,
}

pub struct TableSchema {
    pub table: Table,
    pub relation_id: u32,
    pub attributes: Vec<Attribute>,
}

pub enum RowEvent<'a> {
    Insert(Row<'a>),
    Update(&'a UpdateBody),
    Delete(&'a DeleteBody),
    Relation(&'a RelationBody),
}

pub enum Row<'a> {
    CopyOut(BinaryCopyOutRow),
    Insert(&'a InsertBody),
}

impl ReplicationClient {
    pub async fn new(
        host: String,
        port: u16,
        dbname: String,
        user: String,
        slot_name: String,
        resumption_data: Option<ResumptionData>,
    ) -> Result<ReplicationClient, ReplicationClientError> {
        let postgres_client = Self::connect(&host, port, &dbname, &user).await?;
        let consistent_point = Self::create_slot_if_missing(&postgres_client, &slot_name).await?;
        Ok(ReplicationClient {
            slot_name,
            consistent_point,
            postgres_client,
            resumption_data,
        })
    }

    pub fn should_skip(&self, current_lsn: PgLsn, current_event_type: EventType) -> bool {
        if let Some(resumption_data) = &self.resumption_data {
            if resumption_data.skipping_events {
                let current_lsn: u64 = current_lsn.into();
                let resume_lsn: u64 = resumption_data.resume_lsn.into();
                if current_lsn == 0u64 {
                    return false;
                }
                if current_lsn < resume_lsn {
                    return true;
                }
                if current_lsn == resume_lsn
                    && (resumption_data.last_event_type != EventType::Begin
                        || resumption_data.last_event_type == current_event_type)
                {
                    return true;
                }
            }
        }

        false
    }

    pub fn stop_skipping_events(&mut self) {
        if let Some(resumption_data) = &mut self.resumption_data {
            resumption_data.skipping_events = false;
        }
    }

    pub async fn connect(
        host: &str,
        port: u16,
        dbname: &str,
        user: &str,
    ) -> Result<Client, ReplicationClientError> {
        let config = format!(
            "host={} port ={} dbname={} user={} replication=database",
            host, port, dbname, user
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
        &self,
        publication: &str,
    ) -> Result<Vec<Table>, ReplicationClientError> {
        //TODO: use a non-replication connection to avoid SQL injection
        let publication_query =
            format!("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = '{publication}';");
        let tables = self
            .postgres_client
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

    async fn get_relation_id(&self, table: &Table) -> Result<Option<u32>, ReplicationClientError> {
        let rel_id_query = format!(
            "SELECT c.oid
          FROM pg_catalog.pg_class c
          INNER JOIN pg_catalog.pg_namespace n
                ON (c.relnamespace = n.oid)
         WHERE n.nspname = '{}'
           AND c.relname = '{}';",
            table.schema, table.name
        );

        let rel_id = self
            .postgres_client
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
        &self,
        relation_id: u32,
    ) -> Result<Vec<Attribute>, ReplicationClientError> {
        let col_info_query = format!(
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
            relation_id, relation_id
        );

        let attributes = self
            .postgres_client
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
                    let type_modifier = row
                        .get(2)
                        .expect("failed to get type modifier")
                        .parse()
                        .expect("type modifier is not an i32");
                    let nullable = row.get(3).expect("failed to get attribute nullability") == "f";
                    let identity = row.get(4).expect("failed to get is_identity column") == "t";
                    Some(Attribute {
                        name,
                        typ,
                        type_modifier,
                        nullable,
                        identity,
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        Ok(attributes)
    }

    pub async fn get_schemas(
        &self,
        publication: &str,
    ) -> Result<Vec<TableSchema>, ReplicationClientError> {
        let tables = self.get_publication_tables(publication).await?;
        let mut schema = Vec::new();

        for table in tables {
            let relation_id = self.get_relation_id(&table).await?;
            if let Some(relation_id) = relation_id {
                let attributes = self.get_table_attributes(relation_id).await?;
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

    async fn rollback_txn(client: &Client) -> Result<(), ReplicationClientError> {
        client.simple_query("ROLLBACK;").await?;
        Ok(())
    }

    pub async fn commit_txn(&self) -> Result<(), ReplicationClientError> {
        self.postgres_client.simple_query("COMMIT;").await?;
        Ok(())
    }

    async fn create_replication_slot(
        client: &Client,
        slot_name: &str,
    ) -> Result<PgLsn, ReplicationClientError> {
        let query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL pgoutput USE_SNAPSHOT"#,
            slot_name
        );
        let slot_query = client.simple_query(&query).await?;
        let consistent_point: PgLsn = if let SimpleQueryMessage::Row(row) = &slot_query[0] {
            row.get("consistent_point")
                .expect("failed to get consistent_point")
                .parse()
                .expect("failed to parse consistent_point")
        } else {
            panic!("unexpected query message");
        };
        Ok(consistent_point)
    }

    async fn get_replication_slot_details(
        client: &Client,
        slot_name: &str,
    ) -> Result<Option<PgLsn>, ReplicationClientError> {
        let query = format!(
            r#"select confirmed_flush_lsn from pg_replication_slots where slot_name = '{}';"#,
            slot_name
        );
        let query_result = client.simple_query(&query).await?;
        let confirmed_flush_lsn: PgLsn = if let SimpleQueryMessage::Row(row) = &query_result[0] {
            row.get("confirmed_flush_lsn")
                .expect("failed to get confirmed_flush_lsn column")
                .parse()
                .expect("failed to parse confirmed_flush_lsn")
        } else {
            return Ok(None);
        };
        Ok(Some(confirmed_flush_lsn))
    }

    pub async fn copy_table(
        &self,
        table: &Table,
        attr_types: &[Type],
    ) -> Result<BinaryCopyOutStream, ReplicationClientError> {
        let copy_query = format!(
            r#"COPY "{}"."{}" TO STDOUT WITH (FORMAT binary);"#,
            table.schema, table.name
        );

        let stream = self.postgres_client.copy_out_simple(&copy_query).await?;
        let row_stream = BinaryCopyOutStream::new(stream, attr_types);
        Ok(row_stream)
    }

    async fn create_slot_if_missing(
        client: &Client,
        slot_name: &str,
    ) -> Result<PgLsn, ReplicationClientError> {
        Self::begin_txn(client).await?;
        match Self::create_replication_slot(client, slot_name).await {
            Ok(consistent_point) => Ok(consistent_point),
            Err(e) => match e {
                ReplicationClientError::TokioPostgresError(e) => {
                    if let Some(c) = e.code() {
                        if *c != SqlState::DUPLICATE_OBJECT {
                            return Err(e.into());
                        }
                    }
                    Self::rollback_txn(client).await?;
                    Self::begin_txn(client).await?;
                    if let Some(confirmed_flush_lsn) =
                        Self::get_replication_slot_details(client, slot_name).await?
                    {
                        Ok(confirmed_flush_lsn)
                    } else {
                        panic!("slot {slot_name} should exist")
                    }
                }
                e => Err(e),
            },
        }
    }

    pub async fn get_table_snapshot<F: FnMut(RowEvent, &TableSchema)>(
        &self,
        schemas: &[TableSchema],
        mut f: F,
    ) -> Result<(), ReplicationClientError> {
        for schema in schemas {
            let types = schema
                .attributes
                .iter()
                .map(|attr| attr.typ.clone())
                .collect::<Vec<_>>();
            let rows = self.copy_table(&schema.table, &types).await?;
            tokio::pin!(rows);
            while let Some(row) = rows.next().await {
                let row = row?;
                f(RowEvent::Insert(Row::CopyOut(row)), schema);
            }
        }

        self.commit_txn().await?;

        Ok(())
    }

    pub async fn get_realtime_changes<F: FnMut(RowEvent, &TableSchema)>(
        &self,
        rel_id_to_schema: &HashMap<u32, &TableSchema>,
        publication: &str,
        mut f: F,
    ) -> Result<(), ReplicationClientError> {
        let logical_stream = self.start_replication_slot(publication).await?;

        tokio::pin!(logical_stream);

        const TIME_SEC_CONVERSION: u64 = 946_684_800;
        let postgres_epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);

        let mut last_lsn = self.consistent_point;

        while let Some(replication_msg) = logical_stream.next().await {
            match replication_msg? {
                ReplicationMessage::XLogData(xlog_data) => match xlog_data.into_data() {
                    LogicalReplicationMessage::Begin(_) => {}
                    LogicalReplicationMessage::Commit(commit) => {
                        last_lsn = commit.commit_lsn().into();
                    }
                    LogicalReplicationMessage::Origin(_) => {}
                    LogicalReplicationMessage::Relation(relation) => {
                        match rel_id_to_schema.get(&relation.rel_id()) {
                            Some(schema) => f(RowEvent::Relation(&relation), schema),
                            None => {
                                return Err(ReplicationClientError::RelationIdNotFound(
                                    relation.rel_id(),
                                ));
                            }
                        }
                    }
                    LogicalReplicationMessage::Type(_) => {}
                    LogicalReplicationMessage::Insert(insert) => {
                        match rel_id_to_schema.get(&insert.rel_id()) {
                            Some(schema) => {
                                f(RowEvent::Insert(Row::Insert(&insert)), schema);
                            }
                            None => {
                                return Err(ReplicationClientError::RelationIdNotFound(
                                    insert.rel_id(),
                                ));
                            }
                        }
                    }
                    LogicalReplicationMessage::Update(update) => {
                        match rel_id_to_schema.get(&update.rel_id()) {
                            Some(schema) => f(RowEvent::Update(&update), schema),
                            None => {
                                return Err(ReplicationClientError::RelationIdNotFound(
                                    update.rel_id(),
                                ));
                            }
                        }
                    }
                    LogicalReplicationMessage::Delete(delete) => {
                        match rel_id_to_schema.get(&delete.rel_id()) {
                            Some(schema) => f(RowEvent::Delete(&delete), schema),
                            None => {
                                return Err(ReplicationClientError::RelationIdNotFound(
                                    delete.rel_id(),
                                ));
                            }
                        }
                    }
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

    pub async fn start_replication_slot(
        &self,
        publication: &str,
    ) -> Result<LogicalReplicationStream, ReplicationClientError> {
        let options = format!("(\"proto_version\" '1', \"publication_names\" '{publication}')");
        let start_lsn = if let Some(resumption_data) = &self.resumption_data {
            resumption_data.resume_lsn
        } else {
            self.consistent_point
        };
        let query = format!(
            r#"START_REPLICATION SLOT "{}" LOGICAL {} {}"#,
            self.slot_name, start_lsn, options
        );
        let copy_stream = self
            .postgres_client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await?;
        let stream = LogicalReplicationStream::new(copy_stream);
        Ok(stream)
    }
}

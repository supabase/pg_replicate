use futures::StreamExt;
use thiserror::Error;
use tokio_postgres::{
    binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream},
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
    pub attributes: Vec<Attribute>,
}

pub enum RowEvent {
    Insert(Row),
    Update,
    Delete,
}

pub enum Row {
    CopyOut(BinaryCopyOutRow),
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
        let tables = Self::get_publication_tables(&client, publication).await?;
        let mut schema = Vec::new();

        for table in tables {
            let relation_id = Self::get_relation_id(&client, &table).await?;
            if let Some(relation_id) = relation_id {
                let attributes = Self::get_table_attributes(&client, relation_id).await?;
                schema.push(TableSchema { table, attributes });
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
        let row_stream = BinaryCopyOutStream::new(stream, &attr_types);
        Ok(row_stream)
    }

    async fn start_table_copy(
        client: &Client,
        slot_name: &str,
    ) -> Result<PgLsn, ReplicationClientError> {
        Self::begin_txn(&client).await?;
        Ok(Self::create_replication_slot(&client, slot_name).await?)
    }

    pub async fn get_changes<F: FnMut(RowEvent, &TableSchema) -> ()>(
        &self,
        client: &Client,
        schemas: &[TableSchema],
        mut f: F,
    ) -> Result<(), ReplicationClientError> {
        let _consistent_point = Self::start_table_copy(&client, "temp_slot").await?;

        for schema in schemas {
            let types = schema
                .attributes
                .iter()
                .map(|attr| attr.typ.clone())
                .collect::<Vec<_>>();
            let rows = Self::copy_table(&client, &schema.table, &types).await?;
            tokio::pin!(rows);
            while let Some(row) = rows.next().await {
                let row = row?;
                f(RowEvent::Insert(Row::CopyOut(row)), &schema);
            }
        }

        Self::commit_txn(&client).await?;

        Ok(())
    }
}

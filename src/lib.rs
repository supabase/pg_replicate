mod replication;

pub use replication::{
    Attribute, ReplicationClient, ReplicationClientError, Row, RowEvent, Table, TableSchema,
};

use std::collections::{HashMap, VecDeque};
use std::fs::File;

use arrow_array::builder::{ArrayBuilder, Int32Builder};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
// use tempfile::tempfile;
use tokio_postgres::binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream};
use tokio_postgres::types::Type;
// use tokio_postgres::SimpleQueryMessage::Row;
// use tokio_postgres::{types::PgLsn, NoTls};
use futures::StreamExt;
use tokio_postgres::NoTls;
use tokio_postgres::{Client, SimpleQueryMessage};

pub struct CopyTableClient {
    host: String,
    port: u16,
    dbname: String,
    user: String,
}

impl CopyTableClient {
    pub fn new(host: String, port: u16, dbname: String, user: String) -> CopyTableClient {
        CopyTableClient {
            host,
            port,
            dbname,
            user,
        }
    }

    pub async fn copy_table(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<(), tokio_postgres::Error> {
        let client = self.connect().await?;

        let tables_schema =
            Self::read_tables_schema(&client, vec![(namespace.to_string(), table.to_string())])
                .await?;

        Self::copy_table_impl(&client, &tables_schema).await?;
        // Self::write_parquet();

        // let publication = "mypub";
        // //TODO: fix TOCTOU
        // if !Self::publication_exists(&client, publication).await? {
        //     Self::create_publication(&client, publication, namespace, table).await?;
        // }

        // client
        //     .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        //     .await?;

        // let slot_name = "myslot";
        // let query = format!(
        //     r#"CREATE_REPLICATION_SLOT {:?} TEMPORARY LOGICAL pgoutput USE_SNAPSHOT"#,
        //     slot_name
        // );

        // let slot_query = client
        //     .simple_query(&query)
        //     .await
        //     .expect("failed to create replication slot");

        // let consistent_point: PgLsn = if let Row(row) = &slot_query[0] {
        //     row.get("consistent_point")
        //         .expect("failed to get consistent_point")
        //         .parse()
        //         .expect("failed to parse lsn")
        // } else {
        //     panic!("unexpeced query message");
        // };

        // let copy_query = format!(
        //     r#"COPY "{}"."{}" TO STDOUT WITH (FORMAT binary);"#,
        //     namespace, table
        // );

        // let stream = client.copy_out_simple(&copy_query).await?;

        Ok(())
    }

    async fn connect(&self) -> Result<Client, tokio_postgres::Error> {
        let config = format!(
            "host={} port ={} dbname={} user={} replication=database",
            self.host, self.port, self.dbname, self.user
        );

        let (client, connection) = tokio_postgres::connect(&config, NoTls)
            .await
            .expect("failed to connect to database");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(client)
    }

    // async fn create_publication(
    //     client: &Client,
    //     publication: &str,
    //     namespace: &str,
    //     table: &str,
    // ) -> Result<(), tokio_postgres::Error> {
    //     //TODO: use extended query protocol to avoid sql injection
    //     let create_publication_query =
    //         format!("create publication {publication} for table {namespace}.{table}");
    //     let res = client.simple_query(&create_publication_query).await?;
    //     debug_assert_eq!(res.len(), 1);
    //     debug_assert!(matches!(res[0], SimpleQueryMessage::CommandComplete(0)));
    //     Ok(())
    // }

    // async fn publication_exists(
    //     client: &Client,
    //     publication: &str,
    // ) -> Result<bool, tokio_postgres::Error> {
    //     //TODO: use extended query protocol to avoid sql injection
    //     let pub_exists_query =
    //         format!("select exists (select * from pg_publication where pubname = '{publication}')");
    //     let res = client.simple_query(&pub_exists_query).await?;
    //     debug_assert_eq!(res.len(), 2);
    //     Ok(if let SimpleQueryMessage::Row(row) = &res[0] {
    //         row.get(0).unwrap() == "t"
    //     } else {
    //         false
    //     })
    // }

    async fn read_tables_schema(
        client: &Client,
        tables: Vec<(String, String)>,
    ) -> Result<HashMap<(String, String), Vec<(String, Type, bool)>>, tokio_postgres::Error> {
        let mut tables_schema = HashMap::new();
        for (schema, table) in &tables {
            // Get the relation id of the table
            let rel_id_query = format!(
                "SELECT c.oid
              FROM pg_catalog.pg_class c
              INNER JOIN pg_catalog.pg_namespace n
                    ON (c.relnamespace = n.oid)
             WHERE n.nspname = '{}'
               AND c.relname = '{}';",
                schema, table
            );
            let rel_id = client
                .simple_query(&rel_id_query)
                .await?
                .into_iter()
                .find_map(|msg| {
                    if let SimpleQueryMessage::Row(row) = msg {
                        Some(row.get(0).unwrap().parse::<u32>().unwrap())
                    } else {
                        None
                    }
                })
                .unwrap();

            // Get the column type info
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
                rel_id, rel_id
            );

            let col_types = client
                .simple_query(&col_info_query)
                .await?
                .into_iter()
                .filter_map(|msg| {
                    if let SimpleQueryMessage::Row(row) = msg {
                        let name = row.get(0).unwrap();
                        let ty = Type::from_oid(row.get(1).unwrap().parse().unwrap()).unwrap();
                        let not_null = row.get(3).unwrap() == "t";
                        Some((name.to_string(), ty, not_null))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            tables_schema.insert((schema.clone(), table.clone()), col_types);
        }

        Ok(tables_schema)
    }

    async fn copy_table_impl(
        client: &Client,
        tables_schema: &HashMap<(String, String), Vec<(String, Type, bool)>>,
    ) -> Result<(), tokio_postgres::Error> {
        for ((schema, table), types) in tables_schema {
            let copy_query = format!(
                r#"COPY "{}"."{}" TO STDOUT WITH (FORMAT binary);"#,
                schema, table
            );

            let stream = client.copy_out_simple(&copy_query).await?;

            let bare_types = types
                .iter()
                .map(|(_, ty, _)| ty.clone())
                .collect::<Vec<_>>();
            let row_stream = BinaryCopyOutStream::new(stream, &bare_types);
            let rows: Vec<Result<BinaryCopyOutRow, tokio_postgres::Error>> =
                row_stream.collect().await;
            let mut col_builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
            for ty_info in types {
                match ty_info {
                    (_, Type::INT4, _) => col_builders.push(Box::new(Int32Array::builder(128))),
                    _ => panic!("not supported"),
                }
            }
            for row in rows {
                let row = row.unwrap();

                for (i, ty_info) in types.iter().enumerate() {
                    match ty_info {
                        (_, Type::INT4, not_null) => {
                            let col_builder = col_builders[i]
                                .as_any_mut()
                                .downcast_mut::<Int32Builder>()
                                .unwrap();
                            if *not_null {
                                col_builder.append_value(row.get::<i32>(i));
                            } else {
                                col_builder.append_option(row.get::<Option<i32>>(i));
                            }
                        }
                        _ => panic!("not supported"),
                    };
                }
            }

            let mut array_refs: VecDeque<ArrayRef> = col_builders
                .iter_mut()
                .map(|builder| builder.finish())
                .collect();

            let mut cols = vec![];
            for (name, ty, _) in types {
                match *ty {
                    Type::INT4 => {
                        let col = array_refs.pop_front().unwrap() as ArrayRef;
                        cols.push((name, col))
                    }
                    _ => panic!("not supported"),
                }
            }
            let batch = RecordBatch::try_from_iter(cols).unwrap();
            Self::write_parquet(batch, &format!("./{schema}.{table}.parquet"));
        }

        Ok(())
    }

    fn write_parquet(batch: RecordBatch, file_name: &str) {
        let file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(file_name)
            .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

        writer.write(&batch).expect("Writing batch");

        writer.close().unwrap();
    }
}

use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

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

pub struct ReplicationClient {
    host: String,
    port: u16,
    dbname: String,
    user: String,
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
        for ((namespace, table), types) in tables_schema {
            // println!("Copying initial data for {namespace}.{table}:");
            // for (_, typ, is_null) in types {
            //     let typ = match *typ {
            //         Type::BOOL => {
            //             if *is_null {
            //                 "BOOL NULL"
            //             } else {
            //                 "BOOL"
            //             }
            //         }
            //         Type::INT4 => {
            //             if *is_null {
            //                 "INT NULL"
            //             } else {
            //                 "INT"
            //             }
            //         }
            //         Type::TEXT => {
            //             if *is_null {
            //                 "TEXT NULL"
            //             } else {
            //                 "TEXT"
            //             }
            //         }
            //         _ => {
            //             if *is_null {
            //                 "UNKNOWN NULL"
            //             } else {
            //                 "UNKNOWN"
            //             }
            //         }
            //     };
            //     // print!("    {typ}, ");
            // }
            // println!();
            let copy_query = format!(
                r#"COPY "{}"."{}" TO STDOUT WITH (FORMAT binary);"#,
                namespace, table
            );

            let stream = client.copy_out_simple(&copy_query).await?;

            let bare_types = types
                .iter()
                .map(|(_, ty, _)| ty.clone())
                .collect::<Vec<_>>();
            let row_stream = BinaryCopyOutStream::new(stream, &bare_types);
            let rows: Vec<Result<BinaryCopyOutRow, tokio_postgres::Error>> =
                row_stream.collect().await;
            let mut col_vals: Vec<Box<dyn Any>> = Vec::new();
            for ty_info in types {
                match ty_info {
                    (_, Type::INT4, _) => col_vals.push(Box::new(Vec::<i32>::new())),
                    _ => panic!("not supported"),
                }
            }
            for row in rows {
                let row = row.unwrap();

                // print!("    ");
                for (i, ty_info) in types.iter().enumerate() {
                    match ty_info {
                        // (Type::BOOL, false) => Self::print_value(row.get::<Option<bool>>(i)),
                        // (Type::INT4, false) => Self::print_value(row.get::<Option<i32>>(i)),
                        // (Type::TEXT, false) => Self::print_value(row.get::<Option<String>>(i)),
                        // (Type::BOOL, true) => Self::print_value(Some(row.get::<bool>(i))),
                        (_, Type::INT4, true) => {
                            let col = col_vals[i].downcast_mut::<Vec<i32>>().unwrap();
                            col.push(row.get::<i32>(i));
                            // Self::print_value(Some(row.get::<i32>(i)))
                        }
                        // (Type::TEXT, true) => Self::print_value(Some(row.get::<String>(i))),
                        _ => print!("<{:?}>, ", ty_info),
                    };
                }
                // println!();
            }
            let mut cols = vec![];
            for i in (0..types.len()).rev() {
                let (name, ty, _) = &types[i];
                match *ty {
                    Type::INT4 => {
                        let col_val = Int32Array::from(
                            *col_vals.pop().unwrap().downcast::<Vec<i32>>().unwrap(),
                        );
                        cols.push((name, Arc::new(col_val) as ArrayRef));
                    }
                    _ => panic!("not supported"),
                }
            }
            let batch = RecordBatch::try_from_iter(cols).unwrap();
            Self::write_parquet(batch, "./test.parquet");
            // println!();
        }

        Ok(())
    }

    // fn print_value<T>(value: Option<T>)
    // where
    //     T: std::fmt::Display,
    // {
    //     match value {
    //         Some(value) => print!("{value}, "),
    //         None => print!("NULL, "),
    //     }
    // }

    fn write_parquet(batch: RecordBatch, file_name: &str) {
        // let ids = Int32Array::from(vec![1, 2, 3, 4]);
        // let vals = Int32Array::from(vec![5, 6, 7, 8]);
        // let batch = RecordBatch::try_from_iter(vec![
        //     ("id", Arc::new(ids) as ArrayRef),
        //     ("val", Arc::new(vals) as ArrayRef),
        // ])
        // .unwrap();

        // println!("tempdir {:?}", std::env::temp_dir());
        // let file = tempfile().unwrap();
        // let options = OpenOptions::new().read(true).write(true).create_new(true);
        let file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(file_name)
            .unwrap();

        // let file = File::open("./").unwrap();

        // WriterProperties can be used to set Parquet file options
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

        writer.write(&batch).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();
    }
}

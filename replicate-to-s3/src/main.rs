use std::{collections::BTreeMap, error::Error, io::Write};

// use ::s3::error::S3Error;

use ::s3::Bucket;
use awscreds::Credentials;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::StreamExt;
use pg_replicate::{ReplicationClient, TableSchema};
use s3::{error::S3Error, Region};
use serde::{Deserialize, Serialize};
use serde_cbor::Value;
use tokio_postgres::{binary_copy::BinaryCopyOutRow, types::Type};

mod s3_api;

#[derive(Serialize, Deserialize, Debug)]
struct Event {
    event_type: String,
    timestamp: DateTime<Utc>,
    relation_id: u32,
    data: Value,
}
// #[tokio::main]
// async fn main() -> Result<(), S3Error> {
//     s3::list_objects().await
//     // s3::create_object().await
// }
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // s3::test_s3().await
    let repl_client = ReplicationClient::new(
        "localhost".to_string(),
        8080,
        "pagila".to_string(),
        "raminder.singh".to_string(),
        "temp_slot".to_string(),
    )
    .await?;

    let publication = "actor_pub";
    let schemas = repl_client.get_schemas(publication).await?;

    for schema in &schemas {
        copy_table(&schemas, &schema, &repl_client).await?;
    }

    Ok(())
}

async fn copy_table(
    _schemas: &[TableSchema],
    table_schema: &TableSchema,
    repl_client: &ReplicationClient,
) -> Result<(), S3Error> {
    let bucket_name = "test-rust-s3";
    let region = Region::Custom {
        region: "eu-central-1".to_owned(),
        endpoint: "http://localhost:9000".to_owned(),
    };
    let credentials =
        Credentials::new(Some("admin"), Some("password"), None, None, Some("example"))?;

    let bucket = Bucket::new(bucket_name, region.clone(), credentials.clone())?.with_path_style();

    let mut row_count: u32 = 0;
    let mut data_chunk_count: u32 = 0;
    const ROWS_PER_DATA_CHUNK: u32 = 10;

    let mut data_chunk_buf = vec![];

    let types = table_schema
        .attributes
        .iter()
        .map(|attr| attr.typ.clone())
        .collect::<Vec<_>>();
    let rows = repl_client
        .copy_table(&table_schema.table, &types)
        .await
        .expect("failed to call copy table");
    tokio::pin!(rows);
    while let Some(row) = rows.next().await {
        let row = row.expect("failed to get row");
        // f(RowEvent::Insert(Row::CopyOut(row)), schema);
        let now = Utc::now();
        let mut data_map = BTreeMap::new();
        for (i, attr) in table_schema.attributes.iter().enumerate() {
            let val = get_val_from_row(&attr.typ, &row, i);
            data_map.insert(Value::Text(attr.name.clone()), val);
        }
        let event = Event {
            event_type: "insert".to_string(),
            timestamp: now,
            relation_id: table_schema.relation_id,
            data: Value::Map(data_map),
        };
        // let event = serde_json::to_string(&event)
        //     .expect("failed to convert event to json string");
        // println!("{event}");
        // serde_cbor::to_writer(&events_file, &event)
        //     .expect("failed to write insert event to cbor file");
        // println!("saved row");
        let mut event_buf = vec![];
        serde_cbor::to_writer(&mut event_buf, &event).expect("failed to write insert event");
        data_chunk_buf
            .write(&event_buf.len().to_be_bytes())
            .expect("failed to write event_buf len");
        data_chunk_buf
            .write(&event_buf)
            .expect("failed to write event buf");
        // events_file
        //     .write(&vec.len().to_be_bytes())
        //     .expect("failed to write to events file");
        // events_file.write(&vec).expect("failed to write");
        row_count += 1;
        if row_count == ROWS_PER_DATA_CHUNK {
            data_chunk_count += 1;
            let s3_path = format!(
                "table_copies/{}.{}/{}.dat",
                table_schema.table.schema, table_schema.table.name, data_chunk_count
            );
            let response_data = bucket
                .put_object(s3_path, &data_chunk_buf)
                .await
                .expect("failed to put object");
            if response_data.status_code() != 200 {
                //TODO:error
            }
            data_chunk_buf.clear();
            row_count = 0;
        }
    }
    // repl_client
    //     .get_table_snapshot(&schemas, |event, table_schema| match event {
    //         RowEvent::Insert(row) => match row {
    //             pg_replicate::Row::CopyOut(row) => {
    //                 let now = Utc::now();
    //                 let mut data_map = BTreeMap::new();
    //                 for (i, attr) in table_schema.attributes.iter().enumerate() {
    //                     let val = get_val_from_row(&attr.typ, &row, i);
    //                     data_map.insert(Value::Text(attr.name.clone()), val);
    //                 }
    //                 let event = Event {
    //                     event_type: "insert".to_string(),
    //                     timestamp: now,
    //                     relation_id: table_schema.relation_id,
    //                     data: Value::Map(data_map),
    //                 };
    //                 // let event = serde_json::to_string(&event)
    //                 //     .expect("failed to convert event to json string");
    //                 // println!("{event}");
    //                 // serde_cbor::to_writer(&events_file, &event)
    //                 //     .expect("failed to write insert event to cbor file");
    //                 // println!("saved row");
    //                 let mut event_buf = vec![];
    //                 serde_cbor::to_writer(&mut event_buf, &event)
    //                     .expect("failed to write insert event");
    //                 data_chunk_buf
    //                     .write(&event_buf.len().to_be_bytes())
    //                     .expect("failed to write event_buf len");
    //                 data_chunk_buf
    //                     .write(&event_buf)
    //                     .expect("failed to write event buf");
    //                 // events_file
    //                 //     .write(&vec.len().to_be_bytes())
    //                 //     .expect("failed to write to events file");
    //                 // events_file.write(&vec).expect("failed to write");
    //                 row_count += 1;
    //                 if row_count == ROWS_PER_DATA_CHUNK {
    //                     //
    //                     let s3_path = format!(
    //                         "table_copies/{}.{}/{}.dat",
    //                         table_schema.table.schema, table_schema.table.name, data_chunk_count
    //                     );
    //                     let response_data = bucket
    //                         .put_object_blocking(s3_path, &data_chunk_buf)
    //                         .expect("failed to put object");
    //                     if response_data.status_code() != 200 {
    //                         //TODO:error
    //                     }
    //                     data_chunk_buf.clear();
    //                     data_chunk_count += 1;
    //                 }
    //             }
    //             pg_replicate::Row::Insert(_insert) => {
    //                 unreachable!()
    //             }
    //         },
    //         RowEvent::Update(_update) => {}
    //         RowEvent::Delete(_delete) => {}
    //         RowEvent::Relation(_relation) => {}
    //     })
    //     .await
    //     .expect("failed to get table snapshot");

    let s3_path = format!(
        "table_copies/{}.{}/done",
        table_schema.table.schema, table_schema.table.name
    );
    let response_data = bucket
        .put_object(s3_path, "done".as_bytes())
        .await
        .expect("failed to put object");
    if response_data.status_code() != 200 {
        //TODO:error
    }

    Ok(())
}

fn get_val_from_row(typ: &Type, row: &BinaryCopyOutRow, i: usize) -> Value {
    match *typ {
        Type::INT4 => {
            let val = row.get::<i32>(i);
            Value::Integer(val as i128)
        }
        Type::VARCHAR => {
            let val = row.get::<&str>(i);
            Value::Text(val.to_string())
        }
        Type::TIMESTAMP => {
            let val = row.get::<NaiveDateTime>(i);
            Value::Integer(
                val.and_utc()
                    .timestamp_nanos_opt()
                    .expect("failed to get timestamp nanos") as i128,
            )
        }
        ref typ => {
            panic!("unsupported type {typ:?}")
        }
    }
}

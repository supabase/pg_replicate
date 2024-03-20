use std::{collections::BTreeMap, error::Error, io::Write};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3 as s3;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::StreamExt;
use pg_replicate::{ReplicationClient, TableSchema};
use s3::{
    config::Credentials,
    primitives::ByteStream,
    types::{Delete, ObjectIdentifier},
    Client,
};
use serde::{Deserialize, Serialize};
use serde_cbor::Value;
use tokio_postgres::{binary_copy::BinaryCopyOutRow, types::Type};

#[derive(Serialize, Deserialize, Debug)]
struct Event {
    event_type: String,
    timestamp: DateTime<Utc>,
    relation_id: u32,
    data: Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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

    let credentials = Credentials::new("admin", "password", None, None, "example");
    let s3_config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url("http://localhost:9000")
        .credentials_provider(credentials)
        .region(Region::new("eu-central-1"))
        .force_path_style(true) // apply bucketname as path param instead of pre-domain
        .build();
    let client = aws_sdk_s3::Client::from_conf(s3_config);

    let bucket_name = "test-rust-s3";
    for schema in &schemas {
        if !table_copy_done(&client, schema, bucket_name).await? {
            delete_partial_table_copy(&client, schema, bucket_name).await?;
            copy_table(&client, schema, &repl_client, bucket_name).await?;
        }
    }

    Ok(())
}

async fn copy_table(
    client: &Client,
    table_schema: &TableSchema,
    repl_client: &ReplicationClient,
    bucket_name: &str,
) -> Result<(), anyhow::Error> {
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
        let mut event_buf = vec![];
        serde_cbor::to_writer(&mut event_buf, &event).expect("failed to write insert event");
        data_chunk_buf
            .write(&event_buf.len().to_be_bytes())
            .expect("failed to write event_buf len");
        data_chunk_buf
            .write(&event_buf)
            .expect("failed to write event buf");
        row_count += 1;
        if row_count == ROWS_PER_DATA_CHUNK {
            data_chunk_count += 1;
            let s3_path = format!(
                "table_copies/{}.{}/{}.dat",
                table_schema.table.schema, table_schema.table.name, data_chunk_count
            );
            let byte_stream = ByteStream::from(data_chunk_buf.clone());
            client
                .put_object()
                .bucket(bucket_name)
                .key(s3_path)
                .body(byte_stream)
                .send()
                .await?;
            data_chunk_buf.clear();
            row_count = 0;
        }
    }

    let s3_path = format!(
        "table_copies/{}.{}/done",
        table_schema.table.schema, table_schema.table.name
    );
    let byte_stream = ByteStream::from(vec![]);
    client
        .put_object()
        .bucket(bucket_name)
        .key(s3_path)
        .body(byte_stream)
        .send()
        .await?;

    Ok(())
}

async fn delete_partial_table_copy(
    client: &Client,
    table_schema: &TableSchema,
    bucket_name: &str,
) -> Result<(), anyhow::Error> {
    let s3_prefix = format!(
        "table_copies/{}.{}",
        table_schema.table.schema, table_schema.table.name
    );
    let objects = list_objects(client, bucket_name, &s3_prefix).await?;
    if objects.is_empty() {
        return Ok(());
    }
    client
        .delete_objects()
        .bucket(bucket_name)
        .delete(Delete::builder().set_objects(Some(objects)).build()?)
        .send()
        .await?;
    Ok(())
}

pub async fn list_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<ObjectIdentifier>, anyhow::Error> {
    let mut response = client
        .list_objects_v2()
        .bucket(bucket.to_owned())
        .prefix(prefix)
        .max_keys(100)
        .into_paginator()
        .send();

    let mut objects = vec![];
    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for object in output.contents() {
                    let obj_id = ObjectIdentifier::builder()
                        .set_key(Some(object.key().expect("missing key").to_string()))
                        .build()?;
                    objects.push(obj_id);
                }
            }
            Err(err) => {
                Err(err)?;
            }
        }
    }

    Ok(objects)
}

async fn table_copy_done(
    client: &Client,
    table_schema: &TableSchema,
    bucket_name: &str,
) -> Result<bool, anyhow::Error> {
    let s3_path = format!(
        "table_copies/{}.{}/done",
        table_schema.table.schema, table_schema.table.name
    );

    match client
        .get_object()
        .bucket(bucket_name)
        .key(s3_path)
        .send()
        .await
    {
        Err(e) => {
            if e.raw_response()
                .expect("no raw response")
                .status()
                .is_client_error()
            {
                return Ok(false);
            }
        }
        _ => {}
    }

    Ok(true)
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

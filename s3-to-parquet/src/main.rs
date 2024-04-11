use core::panic;
use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fs::File,
    io::Write,
    // time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{
    builder::{ArrayBuilder, GenericByteBuilder, Int32Builder, TimestampMicrosecondBuilder},
    types::Utf8Type,
    Int32Array, RecordBatch, TimestampMicrosecondArray,
};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{config::Credentials, types::ObjectIdentifier, Client};
use chrono::{DateTime, Utc};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
// use pg_replicate::{Attribute, EventType, Table, TableSchema};
use pg_replicate::EventType;
use serde::{Deserialize, Serialize};
use serde_cbor::Value;
// use tokio_postgres::{binary_copy::BinaryCopyOutRow, types::Type};

#[derive(Serialize, Deserialize, Debug)]
struct Event {
    event_type: EventType,
    timestamp: DateTime<Utc>,
    relation_id: Option<u32>,
    last_lsn: u64,
    data: Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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
    // let mut column_builders = create_column_builders(&schemas);
    read_table_copies(&client, bucket_name).await?;
    // print_realtime_changes(&client, bucket_name).await?;
    Ok(())
}

pub async fn read_table_copies(client: &Client, bucket_name: &str) -> Result<(), anyhow::Error> {
    let s3_prefix = "table_copies/";
    let objects = list_objects(client, bucket_name, s3_prefix).await?;
    let mut table_copy_files: HashMap<&str, (bool, Vec<&str>)> = HashMap::new();
    for obj in &objects {
        let tokens: Vec<&str> = obj.key.split('/').collect();
        if tokens.len() == 3 {
            let (done, files) = table_copy_files.entry(tokens[1]).or_default();
            if tokens[2] == "done" {
                *done = true;
            }
            files.push(tokens[2]);
        } else {
            //todo: error?
        }
    }

    for (key, (done, files)) in table_copy_files {
        if !done {
            println!("Table {key} is not fully copied.");
            return Ok(());
        }
        print!("Table {key} is fully copied with the following files:");
        println!("fully copied with the following files:");
        let mut files: Vec<u32> = files
            .iter()
            .filter(|&&file| file != "done")
            .map(|file| {
                let file_int: u32 = file.parse().expect("file is not an integer");
                file_int
            })
            .collect();

        files.sort();

        let (attributes, mut builders) =
            create_attributes_and_builders(client, bucket_name, &format!("table_copies/{key}/1"))
                .await?;

        for file in files {
            let file_name = format!("table_copies/{key}/{file}");
            print_events_in_file(client, bucket_name, &file_name, &attributes, &mut builders)
                .await?;
        }
        let batch = create_record_batch(builders);
        write_parquet(batch, &format!("./{key}.parquet"));
    }

    Ok(())
}

// async fn print_realtime_changes(client: &Client, bucket_name: &str) -> Result<(), anyhow::Error> {
//     let s3_prefix = "realtime_changes/";
//     let objects = list_objects(client, bucket_name, s3_prefix).await?;
//     let mut realtime_files = vec![];
//     for obj in &objects {
//         let tokens: Vec<&str> = obj.key.split('/').collect();
//         if tokens.len() == 2 {
//             realtime_files.push(tokens[1]);
//         } else {
//             //todo: error?
//         }
//     }

//     let mut files: Vec<u32> = realtime_files
//         .iter()
//         .map(|file| {
//             let file_int: u32 = file.parse().expect("file is not an integer");
//             file_int
//         })
//         .collect();

//     files.sort();
//     for file in files {
//         let file_name = format!("realtime_changes/{file}");
//         print_events_in_file(client, bucket_name, &file_name).await?;
//     }
//     Ok(())
// }

type AttributesAndBuilders = (Vec<ColumnDescriptor>, Vec<(String, Box<dyn ArrayBuilder>)>);

async fn create_attributes_and_builders(
    client: &Client,
    bucket_name: &str,
    file_name: &str,
) -> Result<AttributesAndBuilders, anyhow::Error> {
    let mut file_contents = client
        .get_object()
        .bucket(bucket_name)
        .key(file_name)
        .send()
        .await?;

    let mut v = vec![];
    while let Some(bytes) = file_contents.body.try_next().await? {
        v.write_all(&bytes)?;
    }

    let start = 0;
    let size: [u8; 8] = (&v[start..start + 8]).try_into()?;
    let size = usize::from_be_bytes(size);
    let new_start = start + 8 + size;
    let event_data = &v[start + 8..new_start];
    let event: Event = serde_cbor::from_reader(event_data)?;
    let column_descriptors = read_column_descriptors(&event);
    let column_builders = create_column_builders_for_table(&column_descriptors);

    Ok((column_descriptors, column_builders))
}

async fn print_events_in_file(
    client: &Client,
    bucket_name: &str,
    file_name: &str,
    attributes: &[ColumnDescriptor],
    builders: &mut [(String, Box<dyn ArrayBuilder>)],
) -> Result<(), anyhow::Error> {
    let mut file_contents = client
        .get_object()
        .bucket(bucket_name)
        .key(file_name)
        .send()
        .await?;

    let mut v = vec![];
    while let Some(bytes) = file_contents.body.try_next().await? {
        v.write_all(&bytes)?;
    }

    // let mut column_descriptors_and_builders: Option<AttributesAndBuilders> = None;

    let mut start = 0;
    loop {
        let size: [u8; 8] = (&v[start..start + 8]).try_into()?;
        let size = usize::from_be_bytes(size);
        let new_start = start + 8 + size;
        let event_data = &v[start + 8..new_start];
        let event: Event = serde_cbor::from_reader(event_data)?;
        if event.event_type == EventType::Insert {
            if let Value::Map(row) = &event.data {
                insert_in_col(attributes, builders, row);
            } else {
                panic!("event data must be a map");
            }
        }
        // if let Some((attributes, builders)) = column_descriptors_and_builders {
        //     if event.event_type != EventType::Insert {
        //         panic!("table copies must have only insert events");
        //     }
        //     if let Value::Map(row) = &event.data {
        //         let builders = insert_in_col(&attributes, builders, row);
        //         column_descriptors_and_builders = Some((attributes, builders));
        //     } else {
        //         panic!("event data must be a map");
        //     }
        // } else {
        //     let column_descriptors = read_column_descriptors(&event);
        //     let column_builders = create_column_builders_for_table(&column_descriptors);

        //     column_descriptors_and_builders = Some((column_descriptors, column_builders));
        // }
        println!("Event: {event:#?}");
        start = new_start;
        if v.len() <= new_start {
            break;
        }
    }

    Ok(())
}

struct ColumnDescriptor {
    name: String,
    type_id: u32,
    // identity: bool,
    nullable: bool,
    // type_modifier: i32,
}

fn read_column_descriptors(event: &Event) -> Vec<ColumnDescriptor> {
    if event.event_type == EventType::Schema {
        if let Value::Map(m) = &event.data {
            if let Some(Value::Array(a)) = m.get(&Value::Text("columns".to_string())) {
                let attributes: Vec<ColumnDescriptor> = a
                    .iter()
                    .map(|i| {
                        if let Value::Map(m) = i {
                            let name = m
                                .get(&Value::Text("name".to_string()))
                                .expect("failed to get name");
                            let name = if let Value::Text(name) = name {
                                name.clone()
                            } else {
                                panic!("name is not a string");
                            };

                            let type_id = m
                                .get(&Value::Text("type_id".to_string()))
                                .expect("failed to get name");
                            let type_id = if let Value::Integer(type_id) = type_id {
                                *type_id as u32
                            } else {
                                panic!("type_id is not an integer");
                            };

                            // let identity = m
                            //     .get(&Value::Text("identity".to_string()))
                            //     .expect("failed to get identity");
                            // let identity = if let Value::Bool(identity) = identity {
                            //     *identity
                            // } else {
                            //     panic!("identity is not a boolean");
                            // };

                            let nullable = m
                                .get(&Value::Text("nullable".to_string()))
                                .expect("failed to get nullable");
                            let nullable = if let Value::Bool(nullable) = nullable {
                                *nullable
                            } else {
                                panic!("nullable is not a boolean");
                            };

                            // let type_modifier = m
                            //     .get(&Value::Text("type_modifier".to_string()))
                            //     .expect("failed to get type_modifier");
                            // let type_modifier = if let Value::Integer(type_modifier) = type_modifier
                            // {
                            //     *type_modifier as i32
                            // } else {
                            //     panic!("type_modifier is not an integer");
                            // };

                            ColumnDescriptor {
                                name,
                                type_id,
                                // identity,
                                nullable,
                                // type_modifier,
                            }
                        } else {
                            panic!("array item not a map")
                        }
                    })
                    .collect();
                return attributes;
            }
        }
    }

    panic!("first event type should be schema type");
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

fn create_record_batch(builders: Vec<(String, Box<dyn ArrayBuilder>)>) -> RecordBatch {
    let mut cols = vec![];
    for (name, mut builder) in builders {
        cols.push((name, builder.finish()))
    }

    RecordBatch::try_from_iter(cols).expect("failed to create record batch")
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

fn insert_in_col(
    attributes: &[ColumnDescriptor],
    builders: &mut [(String, Box<dyn ArrayBuilder>)],
    row: &BTreeMap<Value, Value>,
) {
    for (i, attr) in attributes.iter().enumerate() {
        match attr.type_id {
            23 => {
                let col_builder = builders[i]
                    .1
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .expect("builder of incorrect type");

                let v = row.get(&Value::Text(attr.name.clone()));
                let v = v.map(|v| {
                    if let Value::Integer(i) = v {
                        *i as i32
                    } else {
                        panic!("column is not of type integer");
                    }
                });

                if attr.nullable {
                    col_builder.append_option(v);
                } else {
                    col_builder.append_value(v.expect("missing int column"));
                }
            }
            1043 => {
                let col_builder = builders[i]
                    .1
                    .as_any_mut()
                    .downcast_mut::<GenericByteBuilder<Utf8Type>>()
                    .expect("builder of incorrect type");

                let v = row.get(&Value::Text(attr.name.clone()));
                let v = v.map(|v| {
                    if let Value::Text(i) = v {
                        i
                    } else {
                        panic!("column is not of type integer");
                    }
                });

                if attr.nullable {
                    col_builder.append_option(v);
                } else {
                    col_builder.append_value(v.expect("missing string column"));
                }
            }
            1114 => {
                let col_builder = builders[i]
                    .1
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                    .expect("builder of incorrect type");

                let v = row.get(&Value::Text(attr.name.clone()));
                let v = v.map(|v| {
                    if let Value::Integer(i) = v {
                        *i as i64
                    } else {
                        panic!("column is not of type integer");
                    }
                });

                if attr.nullable {
                    col_builder.append_option(v);
                } else {
                    col_builder.append_value(v.expect("missing timestamp column"));
                }
            }
            ref t => panic!("type {t:?} not yet supported"),
        }
    }
}

// fn insert_in_col(
//     attributes: &[Attribute],
//     builders: &mut [(String, Box<dyn ArrayBuilder>)],
//     row: &BinaryCopyOutRow,
// ) {
//     for (i, attr) in attributes.iter().enumerate() {
//         match attr.typ {
//             Type::INT4 => {
//                 let col_builder = builders[i]
//                     .1
//                     .as_any_mut()
//                     .downcast_mut::<Int32Builder>()
//                     .expect("builder of incorrect type");
//                 if attr.nullable {
//                     col_builder.append_option(row.get::<Option<i32>>(i));
//                 } else {
//                     col_builder.append_value(row.get::<i32>(i));
//                 }
//             }
//             Type::VARCHAR => {
//                 let col_builder = builders[i]
//                     .1
//                     .as_any_mut()
//                     .downcast_mut::<GenericByteBuilder<Utf8Type>>()
//                     .expect("builder of incorrect type");
//                 if attr.nullable {
//                     col_builder.append_option(row.get::<Option<&str>>(i));
//                 } else {
//                     col_builder.append_value(row.get::<&str>(i));
//                 }
//             }
//             Type::TIMESTAMP => {
//                 let col_builder = builders[i]
//                     .1
//                     .as_any_mut()
//                     .downcast_mut::<TimestampMicrosecondBuilder>()
//                     .expect("builder of incorrect type");
//                 if attr.nullable {
//                     let st = row.get::<Option<SystemTime>>(i).map(|t| {
//                         t.duration_since(UNIX_EPOCH)
//                             .expect("failed to get duration since unix epoch")
//                             .as_micros() as i64
//                     });
//                     col_builder.append_option(st);
//                 } else {
//                     let st = row.get::<SystemTime>(i);
//                     let dur = st
//                         .duration_since(UNIX_EPOCH)
//                         .expect("failed to get duration since unix epoch")
//                         .as_micros() as i64;
//                     col_builder.append_value(dur);
//                 }
//             }
//             ref t => panic!("type {t:?} not yet supported"),
//         };
//     }
// }

// type ColumnBuilders = HashMap<Table, Vec<(String, Box<dyn ArrayBuilder>)>>;

// fn create_column_builders(schemas: &[TableSchema]) -> ColumnBuilders {
//     let mut table_to_col_builders = HashMap::new();

//     for schema in schemas {
//         table_to_col_builders.insert(
//             schema.table.clone(),
//             create_column_builders_for_table(&schema.attributes),
//         );
//     }

//     table_to_col_builders
// }

fn create_column_builders_for_table(
    attributes: &[ColumnDescriptor],
) -> Vec<(String, Box<dyn ArrayBuilder>)> {
    let mut col_builders: Vec<(String, Box<dyn ArrayBuilder>)> = Vec::new();
    for col_desc in attributes {
        match col_desc.type_id {
            23 => col_builders.push((col_desc.name.clone(), Box::new(Int32Array::builder(128)))),
            1043 => col_builders.push((
                col_desc.name.clone(),
                Box::new(GenericByteBuilder::<Utf8Type>::new()),
            )),
            1114 => col_builders.push((
                col_desc.name.clone(),
                Box::new(TimestampMicrosecondArray::builder(128)),
            )),
            ref t => panic!("type {t:?} not yet supported"),
        }
    }
    col_builders
}

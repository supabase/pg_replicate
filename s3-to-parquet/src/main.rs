use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    fs::File,
    hash::{Hash, Hasher},
    io::Write,
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
use pg_replicate::EventType;
use serde::{Deserialize, Serialize};
use serde_cbor::Value;

pub struct ColumnDescriptor {
    pub name: String,
    pub type_id: u32,
    pub identity: bool,
    pub nullable: bool,
}

pub type AttributesAndBuilders = (
    Vec<ColumnDescriptor>,
    Vec<(String, Box<dyn ArrayBuilder>)>,
    Option<u32>,
);

#[derive(Serialize, Deserialize, Debug)]
pub struct Event {
    pub event_type: EventType,
    pub timestamp: DateTime<Utc>,
    pub relation_id: Option<u32>,
    pub last_lsn: u64,
    pub data: Value,
}

struct Row<'a> {
    attributes: &'a [ColumnDescriptor],
    data: &'a Value,
}

impl<'a> Row<'a> {
    fn get_identity_col(&self, attr: &ColumnDescriptor) -> i128 {
        let this = if let Value::Map(m) = self.data {
            if let Some(value) = m.get(&Value::Text(attr.name.clone())) {
                if let Value::Integer(i) = value {
                    i
                } else {
                    panic!("identity is not integer");
                }
            } else {
                panic!("attribute not found");
            }
        } else {
            panic!("identity is not a map in eq");
        };
        *this
    }
}

impl<'a> Hash for Row<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for attr in self.attributes {
            if attr.identity {
                if let Value::Map(m) = self.data {
                    if let Some(value) = m.get(&Value::Text(attr.name.clone())) {
                        HashedValue { value }.hash(state)
                    }
                } else {
                    panic!("identity is not a map");
                }
            }
        }
    }
}

impl<'a> PartialEq for Row<'a> {
    fn eq(&self, other: &Self) -> bool {
        for attr in self.attributes {
            if attr.identity {
                let this = self.get_identity_col(attr);
                let other = other.get_identity_col(attr);
                if this != other {
                    return false;
                }
            }
        }
        true
    }
}

impl<'a> PartialOrd for Row<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Row<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for attr in self.attributes {
            if attr.identity {
                let this = self.get_identity_col(attr);
                let other = other.get_identity_col(attr);
                return this.cmp(&other);
            }
        }
        panic!("");
    }
}

impl<'a> Eq for Row<'a> {}

#[derive(Eq)]
struct HashedValue<'a> {
    value: &'a Value,
}

impl<'a> Hash for HashedValue<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.value {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Array(a) => {
                for i in a {
                    HashedValue { value: i }.hash(state)
                }
            }
            Value::Integer(i) => i.hash(state),
            Value::Float(f) => (*f as u64).hash(state),
            Value::Bytes(b) => b.hash(state),
            Value::Text(t) => t.hash(state),
            Value::Map(m) => {
                for (k, v) in m {
                    HashedValue { value: k }.hash(state);
                    HashedValue { value: v }.hash(state)
                }
            }
            Value::Tag(t, v) => {
                t.hash(state);
                HashedValue { value: v }.hash(state)
            }
            _ => panic!("unknown value type"),
        }
    }
}

impl<'a> PartialEq for HashedValue<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self.value, other.value) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(b1), Value::Bool(b2)) => b1 == b2,
            (Value::Bytes(b1), Value::Bytes(b2)) => b1 == b2,
            (Value::Float(f1), Value::Float(f2)) => f1 == f2,
            (Value::Integer(i1), Value::Integer(i2)) => i1 == i2,
            (Value::Text(t1), Value::Text(t2)) => t1 == t2,
            (Value::Array(a1), Value::Array(a2)) => a1 == a2,
            _ => false,
        }
    }
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

    let mut relation_id_to_events: HashMap<u32, Vec<Event>> = HashMap::new();
    let mut relation_id_to_table_data =
        get_events_from_table_copies(&client, bucket_name, &mut relation_id_to_events).await?;
    get_events_from_realtime_changes(&client, bucket_name, &mut relation_id_to_events).await?;

    for (relation_id, events) in relation_id_to_events {
        let (attributes, mut builders) = relation_id_to_table_data
            .remove(&relation_id)
            .expect("failed to get table data");
        let mut rows = BTreeMap::new();
        for event in &events {
            if event.event_type == EventType::Insert || event.event_type == EventType::Update {
                let row_key = Row {
                    attributes: &attributes,
                    data: &event.data,
                };
                let row_value = Row {
                    attributes: &attributes,
                    data: &event.data,
                };
                rows.insert(row_key, row_value);
            } else if event.event_type == EventType::Delete {
                let row_key = Row {
                    attributes: &attributes,
                    data: &event.data,
                };
                rows.remove(&row_key);
            }
        }
        for row in rows.values() {
            let data = if let Value::Map(data) = row.data {
                data
            } else {
                panic!("row data is not a map");
            };
            insert_in_col(&attributes, &mut builders, data);
        }

        let batch = create_record_batch(builders);
        write_parquet(batch, &format!("./{relation_id}.parquet"));
    }

    Ok(())
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

async fn get_events_from_table_copies(
    client: &Client,
    bucket_name: &str,
    relation_id_to_events: &mut HashMap<u32, Vec<Event>>,
) -> Result<
    HashMap<u32, (Vec<ColumnDescriptor>, Vec<(String, Box<dyn ArrayBuilder>)>)>,
    anyhow::Error,
> {
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
            panic!("invalid file name");
        }
    }

    let mut relation_id_to_table_data = HashMap::new();

    for (key, (done, files)) in table_copy_files {
        if !done {
            continue;
        }
        let mut files: Vec<u32> = files
            .iter()
            .filter(|&&file| file != "done")
            .map(|file| {
                let file_int: u32 = file.parse().expect("file is not an integer");
                file_int
            })
            .collect();

        let mut table_data_initialized = HashSet::new();

        files.sort();
        for file in files {
            let file_name = format!("table_copies/{key}/{file}");
            get_events_from_file(client, bucket_name, &file_name, relation_id_to_events).await?;

            if !table_data_initialized.contains(key) {
                let (attributes, builders, relation_id) =
                    create_attributes_and_builders(client, bucket_name, &file_name).await?;
                relation_id_to_table_data.insert(
                    relation_id.expect("missing relation id"),
                    (attributes, builders),
                );
                table_data_initialized.insert(key);
            }
        }
    }

    Ok(relation_id_to_table_data)
}

async fn get_events_from_file(
    client: &Client,
    bucket_name: &str,
    file_name: &str,
    relation_id_to_events: &mut HashMap<u32, Vec<Event>>,
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

    let mut start = 0;
    loop {
        let size: [u8; 8] = (&v[start..start + 8]).try_into()?;
        let size = usize::from_be_bytes(size);
        let new_start = start + 8 + size;
        let event_data = &v[start + 8..new_start];
        let event: Event = serde_cbor::from_reader(event_data)?;
        if event.event_type == EventType::Insert
            || event.event_type == EventType::Update
            || event.event_type == EventType::Delete
        {
            let events = relation_id_to_events
                .entry(event.relation_id.expect("missing relation_id in event"))
                .or_default();
            events.push(event);
        }
        start = new_start;
        if v.len() <= new_start {
            break;
        }
    }

    Ok(())
}

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

    Ok((column_descriptors, column_builders, event.relation_id))
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

                            let identity = m
                                .get(&Value::Text("identity".to_string()))
                                .expect("failed to get identity");
                            let identity = if let Value::Bool(identity) = identity {
                                *identity
                            } else {
                                panic!("identity is not a boolean");
                            };

                            let nullable = m
                                .get(&Value::Text("nullable".to_string()))
                                .expect("failed to get nullable");
                            let nullable = if let Value::Bool(nullable) = nullable {
                                *nullable
                            } else {
                                panic!("nullable is not a boolean");
                            };

                            ColumnDescriptor {
                                name,
                                type_id,
                                identity,
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

async fn get_events_from_realtime_changes(
    client: &Client,
    bucket_name: &str,
    relation_id_to_events: &mut HashMap<u32, Vec<Event>>,
) -> Result<(), anyhow::Error> {
    let s3_prefix = "realtime_changes/";
    let objects = list_objects(client, bucket_name, s3_prefix).await?;
    let mut realtime_files = vec![];
    for obj in &objects {
        let tokens: Vec<&str> = obj.key.split('/').collect();
        if tokens.len() == 2 {
            realtime_files.push(tokens[1]);
        } else {
            //todo: error?
        }
    }

    let mut files: Vec<u32> = realtime_files
        .iter()
        .map(|file| {
            let file_int: u32 = file.parse().expect("file is not an integer");
            file_int
        })
        .collect();

    files.sort();
    for file in files {
        let file_name = format!("realtime_changes/{file}");
        get_events_from_file(client, bucket_name, &file_name, relation_id_to_events).await?;
    }

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

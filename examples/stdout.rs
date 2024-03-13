use std::{collections::HashMap, error::Error, str::from_utf8};

use chrono::{DateTime, NaiveDateTime, Utc};
use pg_replicate::{ReplicationClient, RowEvent, TableSchema};
use postgres_protocol::message::backend::{RelationBody, Tuple, TupleData};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use thiserror::Error;
use tokio_postgres::{binary_copy::BinaryCopyOutRow, types::Type};

#[derive(Serialize, Debug)]
struct Event {
    event_type: String,
    timestamp: DateTime<Utc>,
    relation_id: u32,
    data: Value,
}

#[derive(Serialize, Deserialize)]
struct ReplicationState {
    table_copied: bool,
    last_lsn: u64,
}

#[derive(Error, Debug)]
enum StateError {
    #[error("error while reading state {0}")]
    Io(#[from] std::io::Error),

    #[error("error while deserializing state {0}")]
    Serde(#[from] serde_json::Error),
}

// const STATE_FILE: &'static str = "./replication_state.json";

// async fn load_state() -> Result<ReplicationState, StateError> {
//     let state_str = tokio::fs::read_to_string(STATE_FILE).await?;
//     let state = serde_json::from_str(&state_str)?;
//     Ok(state)
// }

// async fn save_state(state: &ReplicationState) -> Result<(), StateError> {
//     let state_str = serde_json::to_string(state)?;
//     tokio::fs::write(STATE_FILE, state_str).await?;
//     Ok(())
// }

// async fn load_or_create_state() -> Result<ReplicationState, StateError> {
//     match load_state().await {
//         Ok(state) => Ok(state),
//         Err(_) => {
//             let state = ReplicationState {
//                 table_copied: false,
//                 last_lsn: 0,
//             };
//             save_state(&state).await?;
//             load_state().await
//         }
//     }
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut repl_client = ReplicationClient::new(
        "localhost".to_string(),
        8080,
        "pagila".to_string(),
        "raminder.singh".to_string(),
        "temp_slot".to_string(),
        None,
        // Some("0/33FF821".parse().expect("invalid resume_lsn")),
        // Some("0/33F81A8".parse().expect("invalid resume_lsn")),//commit msg end_lsn
        // Some("0/33F8178".parse().expect("invalid resume_lsn")), //commit msg commit lsn
        // Some("0/33F8050".parse().expect("invalid resume_lsn")), //delete msg wal start/end lsn
    )
    .await?;

    let publication = "actor_pub";
    let schemas = repl_client.get_schemas(publication).await?;

    let mut rel_id_to_schema = HashMap::new();
    let now = Utc::now();
    for schema in &schemas {
        rel_id_to_schema.insert(schema.relation_id, schema);
        let event = Event {
            event_type: "schema".to_string(),
            timestamp: now,
            relation_id: schema.relation_id,
            data: schema_to_event_data(schema),
        };
        let event = serde_json::to_string(&event).expect("failed to convert event to json string");
        println!("{event}");
    }

    repl_client
        .get_table_snapshot(&schemas, |event, table_schema| match event {
            RowEvent::Insert(row) => match row {
                pg_replicate::Row::CopyOut(row) => {
                    let now = Utc::now();
                    let mut data_map = Map::new();
                    for (i, attr) in table_schema.attributes.iter().enumerate() {
                        let val = get_val_from_row(&attr.typ, &row, i);
                        data_map.insert(attr.name.clone(), json!(val));
                    }
                    let event = Event {
                        event_type: "insert".to_string(),
                        timestamp: now,
                        relation_id: table_schema.relation_id,
                        data: Value::Object(data_map),
                    };
                    let event = serde_json::to_string(&event)
                        .expect("failed to convert event to json string");
                    println!("{event}");
                }
                pg_replicate::Row::Insert(_insert) => {
                    unreachable!()
                }
            },
            RowEvent::Update(_update) => {}
            RowEvent::Delete(_delete) => {}
            RowEvent::Relation(_relation) => {}
        })
        .await?;

    repl_client
        .get_realtime_changes(&rel_id_to_schema, publication, |event, table_schema| {
            let (data, event_type) = match event {
                RowEvent::Insert(row) => match row {
                    pg_replicate::Row::CopyOut(_row) => {
                        unreachable!()
                    }
                    pg_replicate::Row::Insert(insert) => {
                        let data = get_data(table_schema, insert.tuple());
                        (data, "insert".to_string())
                    }
                },
                RowEvent::Update(update) => {
                    let data = get_data(table_schema, update.new_tuple());
                    (data, "update".to_string())
                }
                RowEvent::Delete(delete) => {
                    let tuple = delete
                        .key_tuple()
                        .or(delete.old_tuple())
                        .expect("no tuple found in delete message");
                    let data = get_data(table_schema, tuple);
                    (data, "delete".to_string())
                }
                RowEvent::Relation(relation) => {
                    let data = relation_body_to_event_data(relation);
                    (data, "relation".to_string())
                }
            };
            let now = Utc::now();
            let event = Event {
                event_type,
                timestamp: now,
                relation_id: table_schema.relation_id,
                data,
            };
            let event =
                serde_json::to_string(&event).expect("failed to convert event to json string");
            println!("{event}");
        })
        .await?;

    Ok(())
}

fn relation_body_to_event_data(relation: &RelationBody) -> Value {
    let schema = relation.namespace().expect("invalid relation namespace");
    let table = relation.name().expect("invalid relation name");
    let cols: Vec<Value> = relation
        .columns()
        .iter()
        .map(|col| {
            let name = col.name().expect("invalid column name");
            json!({ "name": name, "identity": col.flags() == 1, "type_id": col.type_id(), "type_modifier": col.type_modifier() })
        })
        .collect();
    json!({"schema": schema, "table": table, "columns": cols })
}

fn schema_to_event_data(schema: &TableSchema) -> Value {
    let attrs: Vec<Value> = schema
        .attributes
        .iter()
        .map(|attr| json!({"name": attr.name, "nullable": attr.nullable, "type_oid": attr.typ.oid(), "type_modifier": attr.type_modifier, "identity": attr.identity}))
        .collect();
    json!({"schema": schema.table.schema, "table": schema.table.name, "attrs": attrs})
}

fn get_val_from_row(typ: &Type, row: &BinaryCopyOutRow, i: usize) -> Value {
    match *typ {
        Type::INT4 => {
            let val = row.get::<i32>(i);
            json!(val)
        }
        Type::VARCHAR => {
            let val = row.get::<&str>(i);
            json!(val)
        }
        Type::TIMESTAMP => {
            let val = row.get::<NaiveDateTime>(i);
            json!(val)
        }
        ref typ => {
            panic!("unsupported type {typ:?}")
        }
    }
}

fn get_val_from_tuple_data(typ: &Type, val: &TupleData) -> Value {
    let val = match val {
        TupleData::Null => {
            return Value::Null;
        }
        TupleData::UnchangedToast => panic!("unchanged toast"),
        TupleData::Text(bytes) => from_utf8(&bytes[..]).expect("failed to get val"),
    };
    match *typ {
        Type::INT4 => {
            let val: i32 = val.parse().expect("value not i32");
            Value::Number(val.into())
        }
        Type::VARCHAR => {
            json!(val)
        }
        Type::TIMESTAMP => {
            let val = NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.f")
                .expect("invalid timestamp");
            json!(val)
        }
        ref typ => {
            panic!("unsupported type {typ:?}")
        }
    }
}

fn get_data(table_schema: &TableSchema, tuple: &Tuple) -> Value {
    let data = tuple.tuple_data();
    let mut data_map = Map::new();
    for (i, attr) in table_schema.attributes.iter().enumerate() {
        let val = get_val_from_tuple_data(&attr.typ, &data[i]);
        data_map.insert(attr.name.clone(), json!(val));
    }
    Value::Object(data_map)
}

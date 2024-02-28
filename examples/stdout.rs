use std::{collections::HashMap, error::Error, str::from_utf8};

use chrono::{DateTime, NaiveDateTime, Utc};
use pg_replicate::{ReplicationClient, RowEvent, TableSchema};
use postgres_protocol::message::backend::{RelationBody, Tuple, TupleData};
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio_postgres::{binary_copy::BinaryCopyOutRow, types::Type};

#[derive(Serialize, Debug)]
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

fn get_val_from_tuple_data(val: &TupleData) -> &str {
    match val {
        TupleData::Null => "null",
        TupleData::UnchangedToast => "unchanged toast",
        TupleData::Text(bytes) => from_utf8(&bytes[..]).expect("failed to get val"),
    }
}

fn get_data(table_schema: &TableSchema, tuple: &Tuple) -> Value {
    let data = tuple.tuple_data();
    let mut data_map = Map::new();
    for (i, attr) in table_schema.attributes.iter().enumerate() {
        let val = get_val_from_tuple_data(&data[i]);
        data_map.insert(attr.name.clone(), json!(val));
    }
    Value::Object(data_map)
}

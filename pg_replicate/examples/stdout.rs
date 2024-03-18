use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fs::File,
    io::{Read, Write},
    os::unix::fs::FileExt,
    str::from_utf8,
};

use chrono::{DateTime, NaiveDateTime, Utc};
use pg_replicate::{ReplicationClient, RowEvent, TableSchema};
use postgres_protocol::message::backend::{RelationBody, Tuple, TupleData};
use serde::{Deserialize, Serialize};
use serde_cbor::Value;
// use serde_json::{json, Map, Value};
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
    let mut events_file = File::open("events.cbor")?;
    let mut size = [0; 8];
    events_file.read_exact(&mut size)?;
    let size = usize::from_le_bytes(size);
    // println!("SIZE: {size}");
    let mut buf = vec![0; size];
    // println!("BUF SIZE: {}", buf.len());
    events_file.read_exact_at(&mut buf, 8)?;
    // println!("BUF: {buf:#?}");
    let event: Event = serde_cbor::from_reader(&buf[..])?;
    println!("{:?}", event);

    // let repl_client = ReplicationClient::new(
    //     "localhost".to_string(),
    //     8080,
    //     "pagila".to_string(),
    //     "raminder.singh".to_string(),
    //     "temp_slot".to_string(),
    // )
    // .await?;

    // let publication = "actor_pub";
    // let schemas = repl_client.get_schemas(publication).await?;
    // let mut events_file = File::create("events.cbor")?;
    // let mut rel_id_to_schema = HashMap::new();
    // let now = Utc::now();
    // for schema in &schemas {
    //     rel_id_to_schema.insert(schema.relation_id, schema);
    //     let event = Event {
    //         event_type: "schema".to_string(),
    //         timestamp: now,
    //         relation_id: schema.relation_id,
    //         data: schema_to_event_data(schema),
    //     };
    //     // let event = serde_json::to_string(&event).expect("failed to convert event to json string");
    //     // println!("{event}");
    //     let mut vec = vec![];
    //     serde_cbor::to_writer(&mut vec, &event)?;
    //     // println!("SCHEMA: {vec:#?}");
    //     events_file
    //         .write(&vec.len().to_le_bytes())
    //         .expect("failed to write to events file");
    //     events_file.write(&vec)?;
    //     // println!("saved schema");
    // }

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
    //                 let mut vec = vec![];
    //                 serde_cbor::to_writer(&mut vec, &event).expect("failed to write insert event");
    //                 events_file
    //                     .write(&vec.len().to_be_bytes())
    //                     .expect("failed to write to events file");
    //                 events_file.write(&vec).expect("failed to write");
    //             }
    //             pg_replicate::Row::Insert(_insert) => {
    //                 unreachable!()
    //             }
    //         },
    //         RowEvent::Update(_update) => {}
    //         RowEvent::Delete(_delete) => {}
    //         RowEvent::Relation(_relation) => {}
    //     })
    //     .await?;

    // repl_client
    //     .get_realtime_changes(&rel_id_to_schema, publication, |event, table_schema| {
    //         let (data, event_type) = match event {
    //             RowEvent::Insert(row) => match row {
    //                 pg_replicate::Row::CopyOut(_row) => {
    //                     unreachable!()
    //                 }
    //                 pg_replicate::Row::Insert(insert) => {
    //                     let data = get_data(table_schema, insert.tuple());
    //                     (data, "insert".to_string())
    //                 }
    //             },
    //             RowEvent::Update(update) => {
    //                 let data = get_data(table_schema, update.new_tuple());
    //                 (data, "update".to_string())
    //             }
    //             RowEvent::Delete(delete) => {
    //                 let tuple = delete
    //                     .key_tuple()
    //                     .or(delete.old_tuple())
    //                     .expect("no tuple found in delete message");
    //                 let data = get_data(table_schema, tuple);
    //                 (data, "delete".to_string())
    //             }
    //             RowEvent::Relation(relation) => {
    //                 let data = relation_body_to_event_data(relation);
    //                 (data, "relation".to_string())
    //             }
    //         };
    //         let now = Utc::now();
    //         let event = Event {
    //             event_type,
    //             timestamp: now,
    //             relation_id: table_schema.relation_id,
    //             data,
    //         };
    //         // let event =
    //         //     serde_json::to_string(&event).expect("failed to convert event to json string");
    //         // println!("{event}");
    //         // serde_cbor::to_writer(&events_file, &event)
    //         //     .expect("failed to write event to cbor file");
    //         // println!("saved event");
    //         let mut vec = vec![];
    //         serde_cbor::to_writer(&mut vec, &event).expect("failed to write event");
    //         events_file
    //             .write(&vec.len().to_be_bytes())
    //             .expect("failed to write to events file");
    //         events_file.write(&vec).expect("failed to write");
    //     })
    //     .await?;

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
            let mut map = BTreeMap::new();
            map.insert(
                Value::Text("name".to_string()),
                Value::Text(name.to_string()),
            );
            map.insert(
                Value::Text("identity".to_string()),
                Value::Bool(col.flags() == 1),
            );
            map.insert(
                Value::Text("type_id".to_string()),
                Value::Integer(col.type_id() as i128),
            );
            map.insert(
                Value::Text("type_modifier".to_string()),
                Value::Integer(col.type_modifier() as i128),
            );
            Value::Map(map)
            // json!({ "name": name, "identity": col.flags() == 1, "type_id": col.type_id(), "type_modifier": col.type_modifier() })
        })
        .collect();
    let mut map = BTreeMap::new();
    map.insert(
        Value::Text("schema".to_string()),
        Value::Text(schema.to_string()),
    );
    map.insert(
        Value::Text("table".to_string()),
        Value::Text(table.to_string()),
    );
    map.insert(Value::Text("columns".to_string()), Value::Array(cols));
    Value::Map(map)
    // json!({"schema": schema, "table": table, "columns": cols })
}

fn schema_to_event_data(schema: &TableSchema) -> Value {
    let attrs: Vec<Value> = schema
        .attributes
        .iter()
        .map(|attr| {
            //
            // json!({"name": attr.name, "nullable": attr.nullable, "type_oid": attr.typ.oid(), "type_modifier": attr.type_modifier, "identity": attr.identity})

            let mut map = BTreeMap::new();
            map.insert(
                Value::Text("name".to_string()),
                Value::Text(attr.name.to_string()),
            );
            map.insert(
                Value::Text("nullable".to_string()),
                Value::Bool(attr.nullable),
            );
            map.insert(
                Value::Text("type_oid".to_string()),
                Value::Integer(attr.typ.oid() as i128),
            );
            map.insert(
                Value::Text("type_modifier".to_string()),
                Value::Integer(attr.type_modifier as i128),
            );
            map.insert(
                Value::Text("identity".to_string()),
                Value::Bool(attr.identity),
            );
            Value::Map(map)
        })
        .collect();
    let mut map = BTreeMap::new();
    map.insert(
        Value::Text("schema".to_string()),
        Value::Text(schema.table.schema.to_string()),
    );
    map.insert(
        Value::Text("table".to_string()),
        Value::Text(schema.table.name.to_string()),
    );
    map.insert(Value::Text("attrs".to_string()), Value::Array(attrs));
    Value::Map(map)
    // json!({"schema": schema.table.schema, "table": schema.table.name, "attrs": attrs})
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
            Value::Integer(val.into())
        }
        Type::VARCHAR => Value::Text(val.to_string()),
        Type::TIMESTAMP => {
            let val = NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.f")
                .expect("invalid timestamp");
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

fn get_data(table_schema: &TableSchema, tuple: &Tuple) -> Value {
    let data = tuple.tuple_data();
    let mut data_map = BTreeMap::new();
    for (i, attr) in table_schema.attributes.iter().enumerate() {
        let val = get_val_from_tuple_data(&attr.typ, &data[i]);
        data_map.insert(Value::Text(attr.name.clone()), val);
    }
    Value::Map(data_map)
}

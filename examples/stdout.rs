use std::{collections::HashMap, error::Error, str::from_utf8, time::SystemTime};

use chrono::{DateTime, NaiveDateTime, Utc};
use pg_replicate::{ReplicationClient, RowEvent, TableSchema};
use postgres_protocol::message::backend::{Tuple, TupleData};
use tokio_postgres::types::Type;

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
    for schema in &schemas {
        rel_id_to_schema.insert(schema.relation_id, schema);
    }

    repl_client
        .get_table_snapshot(&schemas, |event, table_schema| match event {
            RowEvent::Insert(row) => {
                match row {
                    pg_replicate::Row::CopyOut(row) => {
                        let now: DateTime<Utc> = Utc::now();
                        print!(
                            "{{\"event_type\": \"insert\", \"timestamp\": \"{now}\" \"data\": {{"
                        );
                        let len = table_schema.attributes.len();
                        for (i, attr) in table_schema.attributes.iter().enumerate() {
                            let name = &attr.name;
                            match attr.typ {
                                Type::INT4 => {
                                    let val = row.get::<i32>(i);
                                    print!("\"{name}\": {val}");
                                }
                                Type::VARCHAR => {
                                    let val = row.get::<&str>(i);
                                    print!("\"{name}\": \"{val}\"");
                                }
                                Type::TIMESTAMP => {
                                    let val = row.get::<NaiveDateTime>(i);
                                    print!("\"{name}\": \"{val:?}\"");
                                }
                                _ => {}
                            }
                            if i < len - 1 {
                                print!(", ");
                            }
                        }
                        println!("}}}}");
                    }
                    pg_replicate::Row::Insert(_insert) => {
                        //
                    }
                }
            }
            RowEvent::Update(_update) => {}
            RowEvent::Delete(_delete) => {}
        })
        .await?;

    repl_client
        .get_realtime_changes(
            &rel_id_to_schema,
            publication,
            |event, table_schema| match event {
                RowEvent::Insert(row) => match row {
                    pg_replicate::Row::CopyOut(_row) => {
                        //
                    }
                    pg_replicate::Row::Insert(insert) => {
                        let now: DateTime<Utc> = Utc::now();
                        print!("{{\"event_type\": \"insert\", \"timestamp\": \"{now}\", \"data\": {{");
                        print_data(table_schema, insert.tuple());
                    }
                },
                RowEvent::Update(update) => {
                    let now: DateTime<Utc> = Utc::now();
                    print!("{{\"event_type\": \"update\", \"timestamp\": \"{now}\", \"data\": {{");
                    print_data(table_schema, update.new_tuple());
                }
                RowEvent::Delete(delete) => {
                    let now: DateTime<Utc> = Utc::now();
                    print!("{{\"event_type\": \"delete\", \"timestamp\": \"{now}\" \"data\": {{");
                    let tuple = delete
                        .key_tuple()
                        .or(delete.old_tuple())
                        .expect("no tuple found in delete message");
                    print_data(table_schema, tuple);
                    println!("}}}}");
                }
            },
        )
        .await?;

    Ok(())
}

fn print_data(table_schema: &TableSchema, tuple: &Tuple) {
    let data = tuple.tuple_data();
    let len = table_schema.attributes.len();
    for (i, attr) in table_schema.attributes.iter().enumerate() {
        let name = &attr.name;
        let val = &data[i];
        match attr.typ {
            Type::INT4 => {
                let val = match val {
                    TupleData::Null => "null",
                    TupleData::UnchangedToast => "unchanged toast",
                    TupleData::Text(bytes) => from_utf8(&bytes[..]).expect("failed to get val"),
                };
                print!("\"{name}\": {val}");
            }
            Type::VARCHAR => {
                let val = match val {
                    TupleData::Null => "null",
                    TupleData::UnchangedToast => "unchanged toast",
                    TupleData::Text(bytes) => from_utf8(&bytes[..]).expect("failed to get val"),
                };
                print!("\"{name}\": \"{val}\"");
            }
            Type::TIMESTAMP => {
                let val = match val {
                    TupleData::Null => "null",
                    TupleData::UnchangedToast => "unchanged toast",
                    TupleData::Text(bytes) => from_utf8(&bytes[..]).expect("failed to get val"),
                };
                print!("\"{name}\": \"{val:?}\"");
            }
            ref typ => {
                panic!("Unsupported type: {typ:?}");
            }
        }
        if i < len - 1 {
            print!(", ");
        }
    }
    println!("}}}}");
}

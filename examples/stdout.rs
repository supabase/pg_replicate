use std::{collections::HashMap, error::Error, str::from_utf8, time::SystemTime};

use pg_replicate::{ReplicationClient, RowEvent};
use postgres_protocol::message::backend::TupleData;
use tokio_postgres::types::Type;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let repl_client = ReplicationClient::new(
        "localhost".to_string(),
        5431,
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
                        print!("{{\"event_type\": \"insert\", \"data\": {{");
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
                                    let val = row.get::<SystemTime>(i);
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
                    pg_replicate::Row::Tuple(_tuple) => {
                        //
                    }
                }
            }
            RowEvent::Update => {}
            RowEvent::Delete => {}
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
                    pg_replicate::Row::Tuple(tuple) => {
                        let data = tuple.tuple_data();
                        print!("{{\"event_type\": \"insert\", \"data\": {{");
                        let len = table_schema.attributes.len();
                        for (i, attr) in table_schema.attributes.iter().enumerate() {
                            let name = &attr.name;
                            match attr.typ {
                                Type::INT4 => {
                                    let val = &data[i];
                                    let val = match val {
                                        TupleData::Null => "null",
                                        TupleData::UnchangedToast => "unchanged toast",
                                        TupleData::Text(bytes) => {
                                            from_utf8(&bytes[..]).expect("failed to get val")
                                        }
                                    };
                                    print!("\"{name}\": {val}");
                                }
                                Type::VARCHAR => {
                                    let val = &data[i];
                                    let val = match val {
                                        TupleData::Null => "null",
                                        TupleData::UnchangedToast => "unchanged toast",
                                        TupleData::Text(bytes) => {
                                            from_utf8(&bytes[..]).expect("failed to get val")
                                        }
                                    };
                                    print!("\"{name}\": \"{val}\"");
                                }
                                Type::TIMESTAMP => {
                                    let val = &data[i];
                                    let val = match val {
                                        TupleData::Null => "null",
                                        TupleData::UnchangedToast => "unchanged toast",
                                        TupleData::Text(bytes) => {
                                            from_utf8(&bytes[..]).expect("failed to get val")
                                        }
                                    };
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
                },
                RowEvent::Update => {}
                RowEvent::Delete => {}
            },
        )
        .await?;

    Ok(())
}

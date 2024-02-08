use std::{collections::HashMap, error::Error, str::from_utf8, time::SystemTime};

use pg_replicate::{ReplicationClient, RowEvent};
use postgres_protocol::message::backend::TupleData;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let repl_client = ReplicationClient::new(
        "localhost".to_string(),
        8080,
        "pagila".to_string(),
        "raminder.singh".to_string(),
    );

    let publication = "actor_pub";
    let postgres_client = repl_client.connect().await?;
    let schemas = ReplicationClient::get_schema(&postgres_client, publication).await?;

    let mut rel_id_to_schema = HashMap::new();
    for schema in &schemas {
        rel_id_to_schema.insert(schema.relation_id, schema);
    }

    let (slot_name, consistent_point) = repl_client
        .get_table_snapshot(
            &postgres_client,
            &schemas,
            |event, table_schema| match event {
                RowEvent::Insert(row) => {
                    match row {
                        pg_replicate::Row::CopyOut(row) => {
                            let actor_id = row.get::<i32>(0);
                            let first_name = row.get::<&str>(1);
                            let last_name = row.get::<&str>(2);
                            let last_update = row.get::<SystemTime>(3);
                            println!(
                                "Row inserted in table {}.{}: ActorId {}, FirstName {}, LastName {}, LastUpdate {:?}",
                                table_schema.table.schema, table_schema.table.name, actor_id, first_name, last_name, last_update
                            );
                        }
                        pg_replicate::Row::Tuple(_tuple) => {
                            //
                        }
                    }
                }
                RowEvent::Update => {}
                RowEvent::Delete => {}
            },
        )
        .await?;

    repl_client
        .get_realtime_changes(
            &postgres_client,
            &rel_id_to_schema,
            publication,
            &slot_name,
            consistent_point,
            |event, table_schema| match event {
                RowEvent::Insert(row) => match row {
                    pg_replicate::Row::CopyOut(_row) => {
                        //
                    }
                    pg_replicate::Row::Tuple(tuple) => {
                        let data = tuple.tuple_data();
                        assert_eq!(data.len(), 4);
                        let actor_id = &data[0];
                        let actor_id = match actor_id {
                            TupleData::Null => "null",
                            TupleData::UnchangedToast => "unchanged toast",
                            TupleData::Text(bytes) => {
                                from_utf8(&bytes[..]).expect("failed to get actor_id")
                            }
                        };
                        let first_name = &data[1];
                        let first_name = match first_name {
                            TupleData::Null => "null",
                            TupleData::UnchangedToast => "unchanged toast",
                            TupleData::Text(bytes) => {
                                from_utf8(&bytes[..]).expect("failed to get first_name")
                            }
                        };
                        let last_name = &data[2];
                        let last_name = match last_name {
                            TupleData::Null => "null",
                            TupleData::UnchangedToast => "unchanged toast",
                            TupleData::Text(bytes) => {
                                from_utf8(&bytes[..]).expect("failed to get last_name")
                            }
                        };
                        let last_update = &data[3];
                        let last_update = match last_update {
                            TupleData::Null => "null",
                            TupleData::UnchangedToast => "unchanged toast",
                            TupleData::Text(bytes) => {
                                from_utf8(&bytes[..]).expect("failed to get last_update")
                            }
                        };
                        println!(
                            "Row inserted in table {}.{}: ActorId {}, FirstName {}, LastName {}, LastUpdate {:?}",
                            table_schema.table.schema, table_schema.table.name, actor_id, first_name, last_name, last_update
                        );
                    }
                },
                RowEvent::Update => {}
                RowEvent::Delete => {}
            },
        )
        .await?;

    Ok(())
}

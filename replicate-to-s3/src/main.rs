mod avro;
mod avro_test;

use std::error::Error;

use apache_avro::{Schema, Writer};
use avro::{create_table_schema_schema_json, write_table_schema};
use pg_replicate::ReplicationClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // test_schema();
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

    // let mut rel_id_to_schema = HashMap::new();
    // let now = Utc::now();
    for schema in &schemas {
        // write_table_schema(schema);
        // rel_id_to_schema.insert(schema.relation_id, schema);
        // let event = Event {
        //     event_type: "schema".to_string(),
        //     timestamp: now,
        //     relation_id: schema.relation_id,
        //     data: schema_to_event_data(schema),
        // };
        // let event = serde_json::to_string(&event).expect("failed to convert event to json string");
        // println!("{event}");
        let table_schema_schema_json = create_table_schema_schema_json(schema);
        // println!(
        //     "JSON SCHEMA: {}",
        //     serde_json::to_string_pretty(&table_schema_schema_json)
        //         .expect("failed to convert to json")
        // );
        let table_schema_schema =
            Schema::parse(&table_schema_schema_json).expect("failed to parse table schema schema");
        let writer = Writer::new(&table_schema_schema, Vec::new());
        write_table_schema(writer, schema);
    }

    Ok(())
}

use postgres::schema::{Oid, TableName};
use postgres::tokio::test_utils::PgDatabase;

use crate::common::database::{spawn_database, test_table_name};
use crate::common::pipeline_v2::spawn_pg_pipeline;

struct DatabaseSchema {
    users_table_name: TableName,
    users_table_id: Oid,
    orders_table_name: TableName,
    orders_table_id: Oid,
    publication_name: String,
}

async fn setup_database(database: &PgDatabase) -> DatabaseSchema {
    let users_table_name = test_table_name("users");
    let users_table_id = database
        .create_table(
            users_table_name.clone(),
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await
        .expect("Failed to create users table");

    let orders_table_name = test_table_name("orders");
    let orders_table_id = database
        .create_table(
            orders_table_name.clone(),
            &[("description", "text not null")],
        )
        .await
        .expect("Failed to create orders table");

    // Create publication for both tables
    let publication_name = "users_orders_pub";
    database
        .create_publication(
            publication_name,
            &[users_table_name.clone(), orders_table_name.clone()],
        )
        .await
        .expect("Failed to create publication");

    DatabaseSchema {
        users_table_name,
        users_table_id,
        orders_table_name,
        orders_table_id,
        publication_name: publication_name.to_owned(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline() {
    let database = spawn_database().await;
    let database_schema = setup_database(&database).await;

    let (state_store, destination, mut pipeline) =
        spawn_pg_pipeline(&database_schema.publication_name, &database.options).await;
    
    let users_table_name = database_schema.users_table_name.clone();
    let schemas_notify = destination.notify_on_schemas(move |schemas| {
        for schema in schemas {
            if schema.name == users_table_name {
                return true;
            }
        }
        
        false
    }).await;
    
    pipeline.start().await.unwrap();
    
    // schemas_notify.notified().await;
}

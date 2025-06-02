use etl::v2::destination::base::Destination;
use postgres::schema::{Oid, TableName};
use postgres::tokio::test_utils::PgDatabase;
use std::sync::Arc;
use tokio::sync::Notify;

use crate::common::database::{spawn_database, test_table_name};
use crate::common::destination_v2::TestDestination;
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

    let (_, destination, mut pipeline) =
        spawn_pg_pipeline(&database_schema.publication_name, &database.options).await;

    let schemas_notify = destination
        .wait_for_schemas(vec![
            database_schema.users_table_name.clone(),
            database_schema.orders_table_name.clone(),
        ])
        .await;

    pipeline.start().await.unwrap();

    schemas_notify.notified().await;
}

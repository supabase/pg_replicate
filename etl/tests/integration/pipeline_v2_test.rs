use etl::v2::state::table::TableReplicationPhaseType;
use postgres::schema::{ColumnSchema, TableSchema};
use postgres::tokio::test_utils::{PgDatabase, TableModification};
use tokio_postgres::types::Type;

use crate::common::database::{spawn_database, test_table_name};
use crate::common::destination_v2::TestDestination;
use crate::common::pipeline_v2::spawn_pg_pipeline;
use crate::common::state_store::TestStateStore;

struct DatabaseSchema {
    users_table_schema: TableSchema,
    orders_table_schema: TableSchema,
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

    let users_table_schema = TableSchema::new(
        users_table_id,
        users_table_name,
        vec![
            PgDatabase::id_column_schema(),
            ColumnSchema {
                name: "name".to_string(),
                typ: Type::TEXT,
                modifier: -1,
                nullable: false,
                primary: false,
            },
            ColumnSchema {
                name: "age".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: false,
                primary: false,
            },
        ],
    );

    let orders_table_schema = TableSchema::new(
        orders_table_id,
        orders_table_name,
        vec![
            PgDatabase::id_column_schema(),
            ColumnSchema {
                name: "description".to_string(),
                typ: Type::TEXT,
                modifier: -1,
                nullable: false,
                primary: false,
            },
        ],
    );

    DatabaseSchema {
        users_table_schema,
        orders_table_schema,
        publication_name: publication_name.to_owned(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_copy_with_retry() {
    let database = spawn_database().await;
    let database_schema = setup_database(&database).await;

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    // We start the pipeline from scratch.
    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );
    let pipeline_id = pipeline.identity().id();

    // We wait for two table schemas to be received.
    let schemas_notify = destination.wait_for_n_schemas(2).await;
    // We wait for both table states to be in finished done (sync wait is only memory and not
    // available on the store).
    let state_notify = state_store
        .notify_on_replication_phases(
            pipeline_id,
            vec![
                (
                    database_schema.users_table_schema.id,
                    TableReplicationPhaseType::FinishedCopy,
                ),
                (
                    database_schema.orders_table_schema.id,
                    TableReplicationPhaseType::FinishedCopy,
                ),
            ],
        )
        .await;

    // We start the pipeline.
    pipeline.start().await.unwrap();

    schemas_notify.notified().await;
    state_notify.notified().await;

    // We check that the states are correctly set.
    let table_replication_states = state_store.get_table_replication_states().await;
    assert_eq!(table_replication_states.len(), 2);
    assert_eq!(
        table_replication_states
            .get(&(pipeline_id, database_schema.users_table_schema.id))
            .unwrap()
            .phase
            .as_type(),
        TableReplicationPhaseType::FinishedCopy
    );
    assert_eq!(
        table_replication_states
            .get(&(pipeline_id, database_schema.orders_table_schema.id))
            .unwrap()
            .phase
            .as_type(),
        TableReplicationPhaseType::FinishedCopy
    );

    // We check that the table schemas have been stored.
    let mut first_table_schemas = destination.get_table_schemas().await;
    first_table_schemas.sort();
    assert_eq!(first_table_schemas.len(), 2);
    assert_eq!(first_table_schemas[0], database_schema.orders_table_schema);
    assert_eq!(first_table_schemas[1], database_schema.users_table_schema);

    // We assume now that the schema of a table changes before sync done is performed.
    database
        .alter_table(
            database_schema.orders_table_schema.name.clone(),
            &[TableModification::AddColumn {
                name: "date",
                data_type: "integer",
            }],
        )
        .await
        .unwrap();
    let mut extended_orders_table_schema = database_schema.orders_table_schema.clone();
    extended_orders_table_schema
        .column_schemas
        .push(ColumnSchema {
            name: "date".to_string(),
            typ: Type::INT4,
            modifier: -1,
            nullable: true,
            primary: false,
        });

    let destination = TestDestination::new();

    // We recreate a pipeline, assuming the other one was stopped, using the same state.
    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );

    // We wait for two table schemas to be received.
    let schemas_notify = destination.wait_for_n_schemas(2).await;

    // We start the pipeline.
    pipeline.start().await.unwrap();

    schemas_notify.notified().await;

    // We check that the table schema for orders has changed and for users has not.
    let mut second_table_schemas = destination.get_table_schemas().await;
    second_table_schemas.sort();
    assert_eq!(second_table_schemas.len(), 2);
    assert_eq!(second_table_schemas[0], extended_orders_table_schema);
    assert_ne!(first_table_schemas[0], second_table_schemas[0]);
    assert_eq!(second_table_schemas[1], database_schema.users_table_schema);
}

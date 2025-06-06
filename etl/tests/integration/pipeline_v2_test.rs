use etl::conversions::Cell;
use etl::v2::state::table::TableReplicationPhaseType;
use etl::v2::workers::base::WorkerWaitError;
use postgres::schema::{ColumnSchema, Oid, TableName, TableSchema};
use postgres::tokio::test_utils::{PgDatabase, TableModification};
use tokio_postgres::types::Type;

use crate::common::database::{spawn_database, test_table_name};
use crate::common::destination_v2::TestDestination;
use crate::common::pipeline_v2::spawn_pg_pipeline;
use crate::common::state_store::{
    FaultConfig, FaultInjectingStateStore, FaultType, StateStoreMethod, TestStateStore,
};
use std::fs::OpenOptions;
use std::io::Write;

#[derive(Debug)]
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

async fn insert_mock_data(
    database: &PgDatabase,
    users_table_name: TableName,
    orders_table_name: TableName,
    n: usize,
) {
    // Insert users with deterministic data
    for i in 1..=n {
        database
            .insert_values(
                users_table_name.clone(),
                &["name", "age"],
                &[&format!("user_{}", i), &(i as i32)],
            )
            .await
            .expect("Failed to insert users");
    }

    // Insert orders with deterministic data
    for i in 1..=n {
        database
            .insert_values(
                orders_table_name.clone(),
                &["description"],
                &[&format!("description_{}", i)],
            )
            .await
            .expect("Failed to insert orders");
    }
}

async fn get_users_age_sum_from_rows(destination: TestDestination, table_id: Oid) -> i32 {
    let mut actual_sum = 0;

    let tables_rows = destination.get_table_rows().await;
    let table_rows = tables_rows.get(&table_id).unwrap();
    for table_row in table_rows {
        if let Cell::I32(age) = &table_row.values[2] {
            actual_sum += age;
        }
    }

    actual_sum
}

fn get_n_integers_sum(n: usize) -> i32 {
    ((n * (n + 1)) / 2) as i32
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline_with_apply_worker_panic() {
    let database = spawn_database().await;
    let database_schema = setup_database(&database).await;

    let fault_config = FaultConfig {
        load_replication_origin_state: Some(FaultType::Panic),
        ..Default::default()
    };
    let state_store = FaultInjectingStateStore::wrap(TestStateStore::new(), fault_config);
    let destination = TestDestination::new();

    // We start the pipeline from scratch.
    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // We stop and inspect errors.
    let errors = pipeline.shutdown_and_wait().await.err().unwrap();
    assert_eq!(errors.len(), 1);
    assert!(matches!(errors[0], WorkerWaitError::TaskFailed(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline_with_apply_worker_error() {
    let database = spawn_database().await;
    let database_schema = setup_database(&database).await;

    let fault_config = FaultConfig {
        load_replication_origin_state: Some(FaultType::Error),
        ..Default::default()
    };
    let state_store = FaultInjectingStateStore::wrap(TestStateStore::new(), fault_config);
    let destination = TestDestination::new();

    // We start the pipeline from scratch.
    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // We stop and inspect errors.
    let errors = pipeline.shutdown_and_wait().await.err().unwrap();
    assert_eq!(errors.len(), 1);
    assert!(matches!(
        errors[0],
        WorkerWaitError::ApplyWorkerPropagated(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline_with_table_sync_worker_panic() {
    let database = spawn_database().await;
    let database_schema = setup_database(&database).await;

    let fault_config = FaultConfig {
        store_table_schema: Some(FaultType::Panic),
        ..Default::default()
    };
    let state_store = FaultInjectingStateStore::wrap(TestStateStore::new(), fault_config);
    let destination = TestDestination::new();

    // We start the pipeline from scratch.
    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );
    let pipeline_id = pipeline.identity().id();

    // We register the interest in waiting for both table syncs to have started.
    let users_state_notify = state_store
        .get_inner()
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.users_table_schema.id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = state_store
        .get_inner()
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.orders_table_schema.id,
            TableReplicationPhaseType::DataSync,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // We stop and inspect errors.
    let errors = pipeline.shutdown_and_wait().await.err().unwrap();
    assert_eq!(errors.len(), 2);
    assert!(matches!(errors[0], WorkerWaitError::TaskFailed(_)));
    assert!(matches!(errors[1], WorkerWaitError::TaskFailed(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline_with_table_sync_worker_error() {
    let database = spawn_database().await;
    let database_schema = setup_database(&database).await;

    let fault_config = FaultConfig {
        store_table_schema: Some(FaultType::Error),
        ..Default::default()
    };
    let state_store = FaultInjectingStateStore::wrap(TestStateStore::new(), fault_config);
    let destination = TestDestination::new();

    // We start the pipeline from scratch.
    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );
    let pipeline_id = pipeline.identity().id();

    // We register the interest in waiting for both table syncs to have started.
    let users_state_notify = state_store
        .get_inner()
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.users_table_schema.id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = state_store
        .get_inner()
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.orders_table_schema.id,
            TableReplicationPhaseType::DataSync,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // We stop and inspect errors.
    let errors = pipeline.shutdown_and_wait().await.err().unwrap();
    assert_eq!(errors.len(), 2);
    assert!(matches!(
        errors[0],
        WorkerWaitError::TableSyncWorkerPropagated(_)
    ));
    assert!(matches!(
        errors[1],
        WorkerWaitError::TableSyncWorkerPropagated(_)
    ));
}

async fn log_to_file(test_name: &str, message: &str) {
    let file_path = format!("{}.log", test_name);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
        .unwrap();
    writeln!(file, "{}", message).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_copy_with_data_sync_retry() {
    let test_name = "test_table_schema_copy_with_data_sync_retry";
    log_to_file(test_name, "============================================").await;
    log_to_file(
        test_name,
        "Running test_table_schema_copy_with_data_sync_retry",
    )
    .await;
    let database = spawn_database().await;
    log_to_file(test_name, "Spawned database").await;
    let database_schema = setup_database(&database).await;
    log_to_file(test_name, "Setup database schema").await;

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    let fault_config = FaultConfig {
        store_table_schema: Some(FaultType::Error),
        ..Default::default()
    };
    let failing_state_store = FaultInjectingStateStore::wrap(state_store.clone(), fault_config);
    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        failing_state_store.clone(),
        destination.clone(),
    );
    let pipeline_id = pipeline.identity().id();
    log_to_file(test_name, "Spaned pg pipeline").await;

    let users_state_notify = failing_state_store
        .get_inner()
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.users_table_schema.id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = failing_state_store
        .get_inner()
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.orders_table_schema.id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    log_to_file(test_name, "Registered state notifications").await;

    pipeline.start().await.unwrap();
    log_to_file(test_name, "Pipeline started").await;

    users_state_notify.notified().await;
    orders_state_notify.notified().await;
    log_to_file(test_name, "State notifications received").await;

    let errors = pipeline.shutdown_and_wait().await.err().unwrap();
    log_to_file(test_name, "Pipeline shutdown and errors received").await;
    assert_eq!(errors.len(), 2);
    assert!(matches!(
        errors[0],
        WorkerWaitError::TableSyncWorkerPropagated(_)
    ));
    assert!(matches!(
        errors[1],
        WorkerWaitError::TableSyncWorkerPropagated(_)
    ));
    log_to_file(test_name, "Asserted errors").await;

    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );
    let pipeline_id = pipeline.identity().id();
    log_to_file(test_name, "Recreated pg pipeline").await;

    let schemas_notify = destination.wait_for_n_schemas(2).await;
    log_to_file(test_name, "Waited for schemas").await;

    let users_state_notify = state_store
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.users_table_schema.id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.orders_table_schema.id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    log_to_file(test_name, "Registered final state notifications").await;

    pipeline.start().await.unwrap();
    log_to_file(test_name, "Pipeline started again").await;

    schemas_notify.notified().await;
    users_state_notify.notified().await;
    orders_state_notify.notified().await;
    log_to_file(test_name, "State notifications received again").await;

    pipeline.shutdown_and_wait().await.unwrap();
    log_to_file(test_name, "Pipeline shutdown again").await;

    let table_replication_states = state_store.get_table_replication_states().await;
    log_to_file(test_name, "Got table replication states").await;
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
    log_to_file(test_name, "Asserted table replication states").await;

    let mut first_table_schemas = destination.get_table_schemas().await;
    log_to_file(test_name, "Got table schemas").await;
    first_table_schemas.sort();
    log_to_file(test_name, "Sorted table schemas").await;
    assert_eq!(first_table_schemas.len(), 2);
    assert_eq!(first_table_schemas[0], database_schema.orders_table_schema);
    assert_eq!(first_table_schemas[1], database_schema.users_table_schema);
    log_to_file(test_name, "Asserted table schemas").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_copy_with_finished_copy_retry() {
    let test_name = "test_table_schema_copy_with_finished_copy_retry";
    log_to_file(test_name, "============================================").await;
    log_to_file(
        test_name,
        "Running test_table_schema_copy_with_finished_copy_retry",
    )
    .await;
    let database = spawn_database().await;
    log_to_file(test_name, "Spawned database").await;
    let database_schema = setup_database(&database).await;
    log_to_file(test_name, "Setup database schema").await;

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );
    let pipeline_id = pipeline.identity().id();
    log_to_file(test_name, "Spawned pg pipeline").await;

    let schemas_notify = destination.wait_for_n_schemas(2).await;
    log_to_file(test_name, "Waited for schemas").await;

    let users_state_notify = state_store
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.users_table_schema.id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.orders_table_schema.id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    log_to_file(test_name, "Registered state notifications").await;

    pipeline.start().await.unwrap();
    log_to_file(test_name, "Pipeline started").await;

    schemas_notify.notified().await;
    users_state_notify.notified().await;
    orders_state_notify.notified().await;
    log_to_file(test_name, "State notifications received").await;

    pipeline.shutdown_and_wait().await.unwrap();
    log_to_file(test_name, "Pipeline shutdown").await;

    let table_replication_states = state_store.get_table_replication_states().await;
    log_to_file(test_name, "Got table replication states").await;

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
    log_to_file(test_name, "Asserted table replication states").await;

    let mut first_table_schemas = destination.get_table_schemas().await;
    log_to_file(test_name, "Got table schemas").await;
    first_table_schemas.sort();
    log_to_file(test_name, "Sorted table schemas").await;

    assert_eq!(first_table_schemas.len(), 2);
    assert_eq!(first_table_schemas[0], database_schema.orders_table_schema);
    assert_eq!(first_table_schemas[1], database_schema.users_table_schema);
    log_to_file(test_name, "Asserted table schemas").await;

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
    log_to_file(test_name, "Altered orders table schema to add date column").await;
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
    log_to_file(test_name, "Extended orders table schema with date column").await;

    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );
    log_to_file(test_name, "Recreated pg pipeline").await;

    let load_state_notify = state_store
        .notify_on_method_call(StateStoreMethod::LoadReplicationOriginState)
        .await;
    log_to_file(test_name, "Registered load state notification").await;

    pipeline.start().await.unwrap();
    log_to_file(test_name, "Pipeline started again").await;

    load_state_notify.notified().await;
    log_to_file(test_name, "Load state notification received").await;

    pipeline.shutdown_and_wait().await.unwrap();
    log_to_file(test_name, "Pipeline shutdown again").await;

    let mut first_table_schemas = destination.get_table_schemas().await;
    log_to_file(test_name, "Got table schemas again").await;
    first_table_schemas.sort();
    log_to_file(test_name, "Sorted table schemas again").await;
    assert_eq!(first_table_schemas.len(), 2);
    assert_eq!(first_table_schemas[0], database_schema.orders_table_schema);
    assert_eq!(first_table_schemas[1], database_schema.users_table_schema);
    log_to_file(test_name, "Asserted table schemas again").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy() {
    let test_name = "test_table_copy";
    log_to_file(test_name, "============================================").await;
    log_to_file(test_name, "Running test_table_copy").await;
    let database = spawn_database().await;
    log_to_file(test_name, "Spawned database").await;
    let database_schema = setup_database(&database).await;
    log_to_file(test_name, "Setup database schema").await;

    let rows_inserted = 10;
    insert_mock_data(
        &database,
        database_schema.users_table_schema.name,
        database_schema.orders_table_schema.name,
        rows_inserted,
    )
    .await;
    log_to_file(test_name, "Inserted mock data").await;

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );
    let pipeline_id = pipeline.identity().id();
    log_to_file(test_name, "Spawned pg pipeline").await;

    let users_state_notify = state_store
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.users_table_schema.id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            pipeline_id,
            database_schema.orders_table_schema.id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    log_to_file(test_name, "Registered state notifications").await;

    pipeline.start().await.unwrap();
    log_to_file(test_name, "Pipeline started").await;

    users_state_notify.notified().await;
    orders_state_notify.notified().await;
    log_to_file(test_name, "State notifications received").await;

    pipeline.shutdown_and_wait().await.unwrap();
    log_to_file(test_name, "Pipeline shutdown").await;

    let table_rows = destination.get_table_rows().await;
    log_to_file(test_name, "Got table rows").await;
    let users_table_rows = table_rows
        .get(&database_schema.users_table_schema.id)
        .unwrap();
    let orders_table_rows = table_rows
        .get(&database_schema.orders_table_schema.id)
        .unwrap();
    log_to_file(test_name, "Retrieved users and orders table rows").await;
    assert_eq!(users_table_rows.len(), rows_inserted);
    assert_eq!(orders_table_rows.len(), rows_inserted);
    log_to_file(test_name, "Asserted rows count").await;
    let expected_age_sum = get_n_integers_sum(rows_inserted);
    let age_sum =
        get_users_age_sum_from_rows(destination, database_schema.users_table_schema.id).await;
    log_to_file(test_name, &format!("Calculated age sum: {}", age_sum)).await;
    assert_eq!(age_sum, expected_age_sum);
    log_to_file(test_name, "Asserted age sum").await;
}

use crate::common::database::{spawn_database, test_table_name};
use crate::common::destination_v2::TestDestination;
use crate::common::pipeline_v2::spawn_pg_pipeline;
use crate::common::state_store::{
    FaultConfig, FaultInjectingStateStore, FaultType, StateStoreMethod, TestStateStore,
};
use etl::conversions::Cell;
use etl::v2::state::table::TableReplicationPhaseType;
use etl::v2::workers::base::WorkerWaitError;
use postgres::schema::{ColumnSchema, Oid, TableName, TableSchema};
use postgres::tokio::test_utils::{PgDatabase, TableModification};
use tokio_postgres::types::Type;

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

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_copy_with_finished_copy_retry() {
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

    pipeline.start().await.unwrap();

    schemas_notify.notified().await;
    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

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

    // We recreate a pipeline, assuming the other one was stopped, using the same state and destination.
    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );

    // We wait for the load replication origin state method to be called, since that is called in the
    // branch where the copy was already finished.
    let load_state_notify = state_store
        .notify_on_method_call(StateStoreMethod::LoadReplicationOriginState)
        .await;

    pipeline.start().await.unwrap();

    load_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We check that the table schemas haven't changed.
    let mut first_table_schemas = destination.get_table_schemas().await;
    first_table_schemas.sort();
    assert_eq!(first_table_schemas.len(), 2);
    assert_eq!(first_table_schemas[0], database_schema.orders_table_schema);
    assert_eq!(first_table_schemas[1], database_schema.users_table_schema);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy() {
    let database = spawn_database().await;
    let database_schema = setup_database(&database).await;

    // Insert test data
    let rows_inserted = 10;
    insert_mock_data(
        &database,
        database_schema.users_table_schema.name,
        database_schema.orders_table_schema.name,
        rows_inserted,
    )
    .await;

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    // Start the pipeline from scratch
    let mut pipeline = spawn_pg_pipeline(
        &database_schema.publication_name,
        &database.options,
        state_store.clone(),
        destination.clone(),
    );
    let pipeline_id = pipeline.identity().id();

    // Wait for both table states to be in finished copy
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

    pipeline.start().await.unwrap();

    // Wait for notifications with timeout
    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Get all table rows
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows
        .get(&database_schema.users_table_schema.id)
        .unwrap();
    let orders_table_rows = table_rows
        .get(&database_schema.orders_table_schema.id)
        .unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);
    assert_eq!(orders_table_rows.len(), rows_inserted);
    let expected_age_sum = get_n_integers_sum(rows_inserted);
    let age_sum =
        get_users_age_sum_from_rows(destination, database_schema.users_table_schema.id).await;
    assert_eq!(age_sum, expected_age_sum);
}

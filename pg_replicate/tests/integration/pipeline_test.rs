use crate::common::database::{spawn_database, test_table_name};
use crate::common::destination::TestDestination;
use crate::common::pipeline::{spawn_async_pg_pipeline, spawn_pg_pipeline, PipelineMode};
use crate::common::table::assert_table_schema;
use crate::common::wait_for_condition;
use pg_replicate::conversions::cdc_event::CdcEvent;
use pg_replicate::conversions::Cell;
use postgres::schema::{ColumnSchema, TableId};
use postgres::tokio::test_utils::PgDatabase;
use std::ops::Range;
use tokio_postgres::types::Type;

fn get_expected_ages_sum(num_users: usize) -> i32 {
    ((num_users * (num_users + 1)) / 2) as i32
}

async fn create_users_table(database: &PgDatabase) -> TableId {
    let table_id = database
        .create_table(test_table_name("users"), &[("age", "integer")])
        .await
        .unwrap();

    table_id
}

async fn create_users_table_with_publication(
    database: &PgDatabase,
    publication_name: &str,
) -> TableId {
    let table_id = create_users_table(database).await;

    database
        .create_publication(publication_name, &[test_table_name("users")])
        .await
        .unwrap();

    table_id
}

async fn fill_users(database: &PgDatabase, num_users: usize) {
    for i in 0..num_users {
        let age = i as i32 + 1;
        database
            .insert_values(test_table_name("users"), &["age"], &[&age])
            .await
            .unwrap();
    }
}

async fn double_users_ages(database: &PgDatabase) {
    database
        .update_values(test_table_name("users"), &["age"], &["age * 2"])
        .await
        .unwrap();
}

fn assert_users_table_schema(sink: &TestSink, users_table_id: TableId, schema_index: usize) {
    let additional_expected_columns = [ColumnSchema {
        name: "age".to_string(),
        typ: Type::INT4,
        modifier: -1,
        nullable: true,
        primary: false,
    }];

    assert_table_schema(
        destination,
        users_table_id,
        schema_index,
        test_table_name("users"),
        &additional_expected_columns,
    );
}

fn get_users_age_sum_from_rows(destination: &TestDestination, users_table_id: TableId) -> i32 {
    let mut actual_sum = 0;

    let tables_rows = destination.get_tables_rows();
    let table_rows = tables_rows.get(&users_table_id).unwrap();
    for table_row in table_rows {
        if let Cell::I32(age) = &table_row.values[1] {
            actual_sum += age;
        }
    }

    actual_sum
}

fn get_users_age_sum_from_events(
    destination: &TestDestination,
    users_table_id: TableId,
    // We use a range since events are not indexed by table id but just an ordered sequence which
    // we want to slice through.
    range: Range<usize>,
) -> i32 {
    let mut actual_sum = 0;

    let mut i = 0;
    for event in destination.get_events() {
        match event.as_ref() {
            CdcEvent::Insert((table_id, table_row)) | CdcEvent::Update((table_id, table_row))
                if table_id == &users_table_id =>
            {
                if range.contains(&i) {
                    if let Cell::I32(age) = &table_row.values[1] {
                        actual_sum += age;
                    }
                }
                i += 1;
            }
            _ => {}
        }
    }

    actual_sum
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_with_insert_and_update() {
    let database = spawn_database().await;

    // We insert 100 rows.
    let users_table_id = create_users_table(&database).await;
    fill_users(&database, 100).await;

    // We create a pipeline that copies the users table.
    let mut pipeline = spawn_pg_pipeline(
        &database.options,
        PipelineMode::CopyTable {
            table_names: vec![test_table_name("users")],
        },
        TestDestination::new(),
    )
    .await;
    pipeline.start().await.unwrap();

    assert_users_table_schema(pipeline.destination(), users_table_id, 0);
    let expected_sum = get_expected_ages_sum(100);
    let actual_sum = get_users_age_sum_from_rows(pipeline.destination(), users_table_id);
    assert_eq!(actual_sum, expected_sum);
    assert_eq!(pipeline.destination().get_tables_copied(), 1);
    assert_eq!(pipeline.destination().get_tables_truncated(), 1);

    // We double the user ages.
    double_users_ages(&database).await;

    // We recreate the pipeline to copy again and see if we have the new data.
    let mut pipeline = spawn_pg_pipeline(
        &database.options,
        PipelineMode::CopyTable {
            table_names: vec![test_table_name("users")],
        },
        TestDestination::new(),
    )
    .await;
    pipeline.start().await.unwrap();

    assert_users_table_schema(pipeline.destination(), users_table_id, 0);
    let expected_sum = expected_sum * 2;
    let actual_sum = get_users_age_sum_from_rows(pipeline.destination(), users_table_id);
    assert_eq!(actual_sum, expected_sum);
    assert_eq!(pipeline.destination().get_tables_copied(), 1);
    assert_eq!(pipeline.destination().get_tables_truncated(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cdc_with_insert_and_update() {
    let database = spawn_database().await;

    // We create the table and publication.
    let users_table_id = create_users_table_with_publication(&database, "users_publication").await;

    let destination = TestDestination::new();
    let mode = PipelineMode::Cdc {
        publication: "users_publication".to_owned(),
        slot_name: "users_slot".to_string(),
    };

    // We create a pipeline that subscribes to the changes of the users table.
    let mut pipeline = spawn_async_pg_pipeline(&database.options, mode.clone(), sink.clone()).await;

    // We insert 100 rows.
    fill_users(&database, 100).await;

    // We run the pipeline in the background which should correctly pick up entries from the start
    // even though the pipeline was started after insertions.
    let pipeline_task_handle = pipeline.run().await;

    // Wait for all events to be processed.
    let expected_sum = get_expected_ages_sum(100);
    wait_for_condition(|| {
        let actual_sum = get_users_age_sum_from_events(&destination, users_table_id, 0..100);
        actual_sum == expected_sum
    })
    .await;

    pipeline.stop_and_wait(pipeline_task_handle).await;

    assert_users_table_schema(&sink, users_table_id, 0);
    assert_eq!(sink.get_tables_copied(), 0);
    assert_eq!(sink.get_tables_truncated(), 0);

    // We recreate the pipeline with the same details since we want to resume streaming.
    let mut pipeline = spawn_async_pg_pipeline(&database.options, mode.clone(), sink.clone()).await;

    // We double the user ages.
    double_users_ages(&database).await;

    // We run the pipeline to get the new 100 updates of the rows.
    let pipeline_task_handle = pipeline.run().await;

    // Wait for all events to be processed.
    let expected_sum = expected_sum * 2;
    wait_for_condition(|| {
        let actual_sum = get_users_age_sum_from_dml_events(&sink, users_table_id, 100..200);
        actual_sum == expected_sum
    })
    .await;

    pipeline.stop_and_wait(pipeline_task_handle).await;

    assert_users_table_schema(&sink, users_table_id, 1);
    assert_eq!(sink.get_tables_copied(), 0);
    assert_eq!(sink.get_tables_truncated(), 0);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_cdc_table_schema_consistency() {
    let database = spawn_database().await;

    // We create the first table.
    database
        .create_table(test_table_name("table_1"), &[])
        .await
        .unwrap();
    database
        .create_publication("publication_1", &[test_table_name("table_1")])
        .await
        .unwrap();

    let sink = TestSink::new();
    let slot_name = "tables_slot";

    // We create a pipeline that subscribes to the changes of the table.
    let mut pipeline = spawn_async_pg_pipeline(
        &database.options,
        PipelineMode::Cdc {
            publication: "publication_1".to_owned(),
            slot_name: slot_name.to_owned(),
        },
        sink.clone(),
    )
    .await;

    // We run and stop the pipeline immediately since tables are copied immediately.
    let pipeline_task_handle = pipeline.run().await;
    pipeline.stop_and_wait(pipeline_task_handle).await;

    // We get the consistent table schemas which are the ones created on the first pipeline run
    // when the slot was not created.
    let consistent_table_schemas = sink.get_tables_schemas()[0].clone();

    // We create a new table.
    database
        .create_table(test_table_name("table_2"), &[])
        .await
        .unwrap();
    database
        .create_publication(
            "publication_2",
            &[test_table_name("table_1"), test_table_name("table_2")],
        )
        .await
        .unwrap();

    // We recreate the pipeline, simulating that the system crashed.
    let mut pipeline = spawn_async_pg_pipeline(
        &database.options,
        PipelineMode::Cdc {
            publication: "publication_2".to_owned(),
            slot_name: slot_name.to_owned(),
        },
        sink.clone(),
    )
    .await;

    // We run and stop the pipeline immediately since tables are copied immediately.
    let pipeline_task_handle = pipeline.run().await;
    pipeline.stop_and_wait(pipeline_task_handle).await;

    // The current pipeline implementation allows for table schemas to be downloaded every time
    // cdc starts even if the slot was already existing. This is a big problem because the new schema
    // will likely be different from the schema that we would take with the snapshot that we have during
    // table creation.
    //
    // This test is ignored because it fails, but it HAS to be unignored once the pipeline behavior
    // is fixed. We expect either to throw an error or to restart the replication with a new slot.
    assert_eq!(sink.get_tables_schemas()[1], consistent_table_schemas);
}

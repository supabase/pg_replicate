use crate::common::database::{spawn_database, test_table_name};
use crate::common::pipeline::test_slot_name;
use crate::common::table::assert_table_schema;
use futures::StreamExt;
use pg_replicate::v2::clients::postgres::PgReplicationClient;
use postgres::schema::ColumnSchema;
use postgres::tokio::test_utils::{PgDatabase, TableModification};
use tokio::pin;
use tokio_postgres::types::Type;
use tokio_postgres::CopyOutStream;

async fn count_stream_rows(stream: CopyOutStream) -> u64 {
    pin!(stream);

    let mut row_count = 0;
    while let Some(row) = stream.next().await {
        row.unwrap();
        row_count += 1;
    }

    row_count
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replication_client_creates_slot() {
    let database = spawn_database().await;

    let client = PgReplicationClient::connect_no_tls(database.options.clone())
        .await
        .unwrap();

    assert!(client.create_slot(&test_slot_name("my_slot")).await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replication_client_doesnt_recreate_slot() {
    let database = spawn_database().await;

    let client = PgReplicationClient::connect_no_tls(database.options.clone())
        .await
        .unwrap();

    let slot_name = test_slot_name("my_slot");
    assert!(client.create_slot(&slot_name).await.is_ok());
    assert!(client.create_slot(&slot_name).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_copy_is_consistent() {
    let database = spawn_database().await;

    let parent_client = PgReplicationClient::connect_no_tls(database.options.clone())
        .await
        .unwrap();

    let table_1_id = database
        .create_table(test_table_name("table_1"), &[("age", "integer")])
        .await
        .unwrap();
    let table_1_age_schema = ColumnSchema {
        name: "age".to_string(),
        typ: Type::INT4,
        modifier: -1,
        nullable: true,
        primary: false,
    };

    // We create the slot when the database schema contains only 'table_1'.
    let slot = parent_client
        .create_slot(&test_slot_name("my_slot"))
        .await
        .unwrap();

    // We create a transaction to read the new schema consistently.
    let transaction = parent_client.with_slot(slot.clone()).await.unwrap();
    let table_1_schema = transaction
        .get_table_schemas(&[test_table_name("table_1")], None)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    assert_table_schema(
        &table_1_schema,
        table_1_id,
        test_table_name("table_1"),
        &[PgDatabase::id_column_schema(), table_1_age_schema.clone()],
    );

    // We create a new table in the database and update the schema of the old one.
    database
        .create_table(test_table_name("table_2"), &[("year", "integer")])
        .await
        .unwrap();
    database
        .alter_table(
            test_table_name("table_1"),
            &[TableModification::AddColumn {
                name: "year",
                data_type: "integer",
            }],
        )
        .await
        .unwrap();

    // We create a new transaction using the same slot to read the database consistently even after
    // a new table was added.
    let transaction = parent_client.with_slot(slot).await.unwrap();
    let table_1_schema = transaction
        .get_table_schemas(&[test_table_name("table_1")], None)
        .await
        .unwrap();
    let table_2_schema = transaction
        .get_table_schemas(&[test_table_name("table_2")], None)
        .await;
    transaction.commit().await.unwrap();

    assert_table_schema(
        &table_1_schema,
        table_1_id,
        test_table_name("table_1"),
        &[PgDatabase::id_column_schema(), table_1_age_schema.clone()],
    );
    // We expect the schema of table_2 to not be there since we have an older snapshot.
    assert!(table_2_schema.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_stream_is_consistent() {
    let database = spawn_database().await;

    let parent_client = PgReplicationClient::connect_no_tls(database.options.clone())
        .await
        .unwrap();

    // We create a table and insert one row.
    database
        .create_table(test_table_name("table_1"), &[("age", "integer")])
        .await
        .unwrap();
    database
        .insert_values(test_table_name("table_1"), &["age"], &[&10])
        .await
        .unwrap();

    // We create the slot when the database schema contains only 'table_1' data.
    let slot = parent_client
        .create_slot(&test_slot_name("my_slot"))
        .await
        .unwrap();

    // We create a transaction to copy the table data consistently.
    let transaction = parent_client.with_slot(slot.clone()).await.unwrap();
    let stream = transaction
        .get_table_copy_stream(
            &test_table_name("table_1"),
            &[ColumnSchema {
                name: "age".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: true,
                primary: false,
            }],
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let rows_count = count_stream_rows(stream).await;

    // We expect to have only one row since we just inserted one.
    assert_eq!(rows_count, 1);

    // We insert an additional value.
    database
        .insert_values(test_table_name("table_1"), &["age"], &[&15])
        .await
        .unwrap();

    // We create a transaction to copy the table data consistently.
    let transaction = parent_client.with_slot(slot).await.unwrap();
    let stream = transaction
        .get_table_copy_stream(
            &test_table_name("table_1"),
            &[ColumnSchema {
                name: "age".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: true,
                primary: false,
            }],
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let rows_count = count_stream_rows(stream).await;

    // We expect to still have only one row since we should not have in the snapshot the newly
    // added row.
    assert_eq!(rows_count, 1);
}

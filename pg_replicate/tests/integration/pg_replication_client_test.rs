use crate::common::database::{spawn_database, test_table_name};
use crate::common::pipeline::test_slot_name;
use crate::common::table::{assert_table_schema, id_column_schema};
use pg_replicate::v2::clients::postgres::PgReplicationClient;
use postgres::schema::ColumnSchema;
use postgres::tokio::test_utils::TableModification;
use tokio_postgres::types::Type;

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

    assert!(client.create_slot(&test_slot_name("my_slot")).await.is_ok());
    assert!(client
        .create_slot(&test_slot_name("my_slot"))
        .await
        .is_err());
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
        &[
            id_column_schema(),
            ColumnSchema {
                name: "age".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: true,
                primary: false,
            },
        ],
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
        &[
            id_column_schema(),
            ColumnSchema {
                name: "age".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: true,
                primary: false,
            },
        ],
    );
    // We expect the schema of table_2 to not be there since we have an older snapshot.
    assert!(table_2_schema.is_err());
}

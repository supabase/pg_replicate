use crate::common::database::spawn_database;
use pg_replicate::v2::clients::postgres::PgReplicationClient;
use crate::common::pipeline::test_slot_name;

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
    assert!(client.create_slot(&test_slot_name("my_slot")).await.is_err());
}

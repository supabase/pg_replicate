use crate::common::database::spawn_database;

#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline() {
    // TODO: implement end to end test of the pipeline.
    let database = spawn_database().await;
}

use crate::common::destination_v2::TestDestination;
use crate::common::state_store::TestStateStore;
use etl::v2::pipeline::{Pipeline, PipelineIdentity};
use postgres::tokio::options::PgDatabaseOptions;

pub async fn spawn_pg_pipeline(
    publication_name: &str,
    options: &PgDatabaseOptions,
) -> (
    TestStateStore,
    TestDestination,
    Pipeline<TestStateStore, TestDestination>,
) {
    let pipeline_identity = PipelineIdentity::new(0, publication_name);

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    let pipeline = Pipeline::new(
        pipeline_identity,
        options.clone(),
        vec![],
        state_store.clone(),
        destination.clone(),
    );

    (state_store, destination, pipeline)
}

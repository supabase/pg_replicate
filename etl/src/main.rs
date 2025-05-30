use etl::v2::destination::memory::MemoryDestination;
use etl::v2::pipeline::Pipeline;
use etl::v2::state::store::base::PipelineStateStore;
use etl::v2::state::store::memory::MemoryPipelineStateStore;
use etl::v2::state::table::{TableReplicationPhase, TableReplicationState};
use tracing_subscriber::EnvFilter;

// Temporary main method to quickly validate the inner workings of the new v2 architecture.
#[tokio::main]
async fn main() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // 2. Install a pretty-printing subscriber that writes to stdout
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // We create a store with mock table ids.
    let state_store = MemoryPipelineStateStore::new();
    for i in 0..1 {
        state_store
            .store_table_replication_state(TableReplicationState::new(
                i,
                TableReplicationPhase::Init,
            ))
            .await;
    }

    let destination = MemoryDestination::new();

    let mut pipeline =
        Pipeline::new(1, "my_publication".to_owned(), state_store, destination).await;

    pipeline.start().await.unwrap();
    pipeline.wait().await;
}

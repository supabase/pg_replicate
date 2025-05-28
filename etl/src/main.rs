use etl::v2::destination::memory::MemoryDestination;
use etl::v2::pipeline::Pipeline;
use etl::v2::state::relation_subscription::{TableReplicationPhase, TableReplicationState};
use etl::v2::state::store::base::PipelineStateStore;
use etl::v2::state::store::memory::MemoryPipelineStateStore;

// Temporary main method to quickly validate the inner workings of the new v2 architecture.
#[tokio::main]
async fn main() {
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

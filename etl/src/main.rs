use etl::v2::destination::memory::MemoryDestination;
use etl::v2::pipeline::Pipeline;
use etl::v2::state::store::memory::MemoryPipelineStateStore;

// Temporary main method to quickly validate the inner workings of the new v2 architecture.
#[tokio::main]
async fn main() {
    let state_store = MemoryPipelineStateStore::new();
    let destination = MemoryDestination::new();

    let pipeline = Pipeline::new(1, "my_publication".to_owned(), state_store, destination).await;
}

use crate::v2::destination::Destination;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::base::{Worker, WorkerHandle};
use crate::v2::workers::table_sync::TableSyncWorkerHandles;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct ApplyWorkerHandle {
    handle: Option<JoinHandle<()>>,
}

impl WorkerHandle<()> for ApplyWorkerHandle {
    fn state(&self) -> () {}

    async fn wait(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };

        // TODO: properly handle message.
        handle.await.unwrap();
    }
}

#[derive(Debug)]
pub struct ApplyWorker<S, D> {
    state_store: S,
    destination: D,
    table_sync_workers: TableSyncWorkerHandles,
}

impl<S, D> ApplyWorker<S, D>
where
    S: PipelineStateStore,
    D: Destination,
{
    pub fn new(state_store: S, destination: D, table_sync_workers: TableSyncWorkerHandles) -> Self {
        Self {
            state_store,
            destination,
            table_sync_workers,
        }
    }
}

impl<S, D> Worker<ApplyWorkerHandle, ()> for ApplyWorker<S, D>
where
    S: PipelineStateStore + Send + 'static,
    D: Destination + Send + 'static,
{
    async fn start(self) -> ApplyWorkerHandle {
        let handle = tokio::spawn(apply_worker(self.state_store, self.table_sync_workers));

        ApplyWorkerHandle {
            handle: Some(handle),
        }
    }
}

async fn apply_worker<S>(state_store: S, table_sync_workers: TableSyncWorkerHandles)
where
    S: PipelineStateStore + Send + 'static,
{
    let pipeline_state = state_store.load_pipeline_state().await;

    // Steps:
    // 1. Run apply loop which is a struct that takes an implementation of trait called TableSync which calls the
    //   sync tables method every time
    // 2. Implement TableSync for this specific worker which does some specific amount of work
}

// TODO: implementing TablesSyncing.

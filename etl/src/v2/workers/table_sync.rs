use crate::v2::workers::base::WorkerHandle;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

#[derive(Debug)]
struct StateInner {}

#[derive(Debug, Clone)]
pub struct TableSyncWorkerState {
    inner: Arc<Mutex<StateInner>>,
}

#[derive(Debug)]
pub struct TableSyncWorkerHandle {
    state: TableSyncWorkerState,
    handle: Option<JoinHandle<()>>,
}

impl WorkerHandle<TableSyncWorkerState> for TableSyncWorkerHandle {
    fn state(&self) -> TableSyncWorkerState {
        self.state.clone()
    }

    async fn wait(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };

        // TODO: properly handle message.
        handle.await.unwrap();
    }
}

use crate::v2::workers::base::{Worker, WorkerHandle};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;

#[derive(Debug)]
struct StateInner {
    last_lsn: PgLsn,
}

#[derive(Debug, Clone)]
pub struct ApplyWorkerState {
    inner: Arc<Mutex<StateInner>>,
}

impl ApplyWorkerState {
    fn new(lsn: PgLsn) -> Self {
        let inner = StateInner { last_lsn: lsn };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

#[derive(Debug)]
pub struct ApplyWorkerHandle {
    state: ApplyWorkerState,
    handle: Option<JoinHandle<()>>,
}

impl WorkerHandle<ApplyWorkerState> for ApplyWorkerHandle {
    fn state(&self) -> ApplyWorkerState {
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

#[derive(Debug)]
pub struct ApplyWorker {}

impl ApplyWorker {
    pub fn new() -> Self {
        Self {}
    }
}

impl Worker<ApplyWorkerHandle, ApplyWorkerState> for ApplyWorker {
    async fn start(self) -> ApplyWorkerHandle {
        let state = ApplyWorkerState::new(PgLsn::from(0));
        let handle = tokio::spawn(async move {});

        ApplyWorkerHandle {
            state,
            handle: Some(handle),
        }
    }
}

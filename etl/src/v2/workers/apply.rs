use crate::v2::state::store::base::PipelineStateStore;
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
pub struct ApplyWorker<S> {
    state_store: S,
}

impl<S> ApplyWorker<S>
where
    S: PipelineStateStore,
{
    pub fn new(state_store: S) -> Self {
        Self { state_store }
    }
}

impl<S> Worker<ApplyWorkerHandle, ApplyWorkerState> for ApplyWorker<S>
where
    S: PipelineStateStore,
{
    async fn start(self) -> ApplyWorkerHandle {
        let state = ApplyWorkerState::new(PgLsn::from(0));

        let handle = tokio::spawn(apply_loop(state.clone()));

        ApplyWorkerHandle {
            state,
            handle: Some(handle),
        }
    }
}

async fn apply_loop(state: ApplyWorkerState) {
    loop {}
}

fn sync_tables_for_apply() {}

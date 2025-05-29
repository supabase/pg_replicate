use crate::v2::concurrency::future::ReactiveFutureCallback;
use crate::v2::destination::base::Destination;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::base::{Worker, WorkerError, WorkerHandle};
use crate::v2::workers::table_sync::{
    TableSyncWorker, TableSyncWorkerHandle, TableSyncWorkerState,
};
use postgres::schema::Oid;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Debug)]
pub enum TableSyncWorkerInactiveReason {
    Success,
    Error(String),
}

#[derive(Debug)]
pub struct TableSyncWorkerPoolInner {
    /// The table sync workers that are currently active.
    active: HashMap<Oid, TableSyncWorkerHandle>,
    /// The table sync workers that are inactive, meaning that they are completed or errored.
    ///
    /// Having the state of finished workers gives us the power to reschedule failed table sync
    /// workers very cheaply since the state can be fed into a new table worker future as if it was
    /// read initially from the state store.
    inactive: HashMap<Oid, Vec<(TableSyncWorkerInactiveReason, TableSyncWorkerHandle)>>,
}

impl TableSyncWorkerPoolInner {
    fn new() -> Self {
        Self {
            active: HashMap::new(),
            inactive: HashMap::new(),
        }
    }

    pub async fn start_worker<S, D>(&mut self, worker: TableSyncWorker<S, D>) -> bool
    where
        S: PipelineStateStore + Clone + Send + 'static,
        D: Destination + Clone + Send + 'static,
    {
        let table_id = worker.table_id();
        if self.active.contains_key(&table_id) {
            warn!("Worker for table {} already exists in pool", table_id);
            return false;
        }

        let Some(handle) = worker.start().await else {
            warn!("Failed to start worker for table {}", table_id);
            return false;
        };

        self.active.insert(table_id, handle);
        info!("Successfully added worker for table {} to pool", table_id);

        true
    }

    pub fn get_worker_state(&self, table_id: Oid) -> Option<TableSyncWorkerState> {
        let state = self.active.get(&table_id)?.state().clone();
        info!("Retrieved worker state for table {}", table_id);

        Some(state)
    }

    pub fn set_worker_finished(&mut self, table_id: Oid, reason: TableSyncWorkerInactiveReason) {
        let removed_worker = self.active.remove(&table_id);
        if let Some(removed_worker) = removed_worker {
            info!(
                "Marked worker for table {} as inactive with reason {:?}",
                table_id, reason
            );

            self.inactive
                .entry(table_id)
                .or_default()
                .push((reason, removed_worker));
        }
    }

    pub async fn wait_all(&mut self) -> Vec<WorkerError> {
        let worker_count = self.active.len();
        info!("Waiting for {} workers to complete", worker_count);

        let mut errors = Vec::new();

        let active = mem::take(&mut self.active);
        for (_, worker) in active {
            if let Err(err) = worker.wait().await {
                errors.push(err);
            }
        }

        let finished = mem::take(&mut self.inactive);
        for (_, workers) in finished {
            for (finish, worker) in workers {
                if let Err(err) = worker.wait().await {
                    errors.push(err);
                }

                // If we have a failure in the worker we should not have a failure here, since the
                // custom future we run table syncs with, catches panics, however, we do not want
                // to make that assumption here.
                if let TableSyncWorkerInactiveReason::Error(err) = finish {
                    errors.push(WorkerError::Caught(err));
                }
            }
        }

        info!("All {} workers completed", worker_count);

        errors
    }
}

impl ReactiveFutureCallback<Oid> for TableSyncWorkerPoolInner {
    fn on_complete(&mut self, id: Oid) {
        self.set_worker_finished(id, TableSyncWorkerInactiveReason::Success);
    }

    fn on_error(&mut self, id: Oid, error: String) {
        self.set_worker_finished(id, TableSyncWorkerInactiveReason::Error(error));
    }
}

#[derive(Debug, Clone)]
pub struct TableSyncWorkerPool {
    workers: Arc<RwLock<TableSyncWorkerPoolInner>>,
}

impl TableSyncWorkerPool {
    pub fn new() -> Self {
        Self {
            workers: Arc::new(RwLock::new(TableSyncWorkerPoolInner::new())),
        }
    }

    pub fn workers(&self) -> Arc<RwLock<TableSyncWorkerPoolInner>> {
        self.workers.clone()
    }
}

impl Deref for TableSyncWorkerPool {
    type Target = RwLock<TableSyncWorkerPoolInner>;

    fn deref(&self) -> &Self::Target {
        &self.workers
    }
}

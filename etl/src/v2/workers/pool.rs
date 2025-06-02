use postgres::schema::Oid;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::v2::concurrency::future::ReactiveFutureCallback;
use crate::v2::destination::base::Destination;
use crate::v2::state::store::base::StateStore;
use crate::v2::workers::base::{Worker, WorkerHandle, WorkerWaitError};
use crate::v2::workers::table_sync::{
    TableSyncWorker, TableSyncWorkerError, TableSyncWorkerHandle, TableSyncWorkerState,
};

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

    pub async fn start_worker<S, D>(
        &mut self,
        worker: TableSyncWorker<S, D>,
    ) -> Result<bool, TableSyncWorkerError>
    where
        S: StateStore + Clone + Send + 'static,
        D: Destination + Clone + Send + 'static,
    {
        let table_id = worker.table_id();
        if self.active.contains_key(&table_id) {
            warn!("Worker for table {} already exists in pool", table_id);
            return Ok(false);
        }

        let handle = worker.start().await?;
        self.active.insert(table_id, handle);
        info!("Successfully added worker for table {} to pool", table_id);

        Ok(true)
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

    pub async fn wait_all(&mut self) -> Result<(), Vec<WorkerWaitError>> {
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
                // If there is an error while waiting for the task, we can assume that there was un
                // uncaught panic or a propagated error.
                if let Err(err) = worker.wait().await {
                    errors.push(err);
                    continue;
                }

                // If we arrive here, it means that the worker task did fail but silently, since
                // the error we see here was reported by the `ReactiveFuture` and swallowed.
                // This should not happen since right now the `ReactiveFuture` is configured to
                // re-propagate the error after marking a table sync worker as finished.
                if let TableSyncWorkerInactiveReason::Error(err) = finish {
                    errors.push(WorkerWaitError::TaskSilentlyFailed(err));
                }
            }
        }

        info!("All {} workers completed", worker_count);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
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

impl Default for TableSyncWorkerPool {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for TableSyncWorkerPool {
    type Target = RwLock<TableSyncWorkerPoolInner>;

    fn deref(&self) -> &Self::Target {
        &self.workers
    }
}

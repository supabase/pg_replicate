use std::any::Any;
use postgres::schema::Oid;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::v2::destination::base::Destination;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::base::{Worker, WorkerHandle};
use crate::v2::workers::table_sync::{
    TableSyncWorker, TableSyncWorkerHandle, TableSyncWorkerState,
};

#[derive(Debug)]
pub enum TableSyncWorkerFinish {
    Success,
    Error(Box<dyn Any + Send>),
}

#[derive(Debug)]
pub struct TableSyncWorkerPoolInner {
    active: HashMap<Oid, TableSyncWorkerHandle>,
    finished: HashMap<Oid, Vec<(TableSyncWorkerFinish, TableSyncWorkerHandle)>>,
}

impl TableSyncWorkerPoolInner {
    fn new() -> Self {
        Self {
            active: HashMap::new(),
            finished: HashMap::new(),
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

    pub fn finished_worker(&mut self, table_id: Oid, table_sync_worker_finish: TableSyncWorkerFinish) {
        let removed_worker = self.active.remove(&table_id);
        if let Some(removed_worker) = removed_worker {
            self.finished.entry(table_id).or_default().push((table_sync_worker_finish, removed_worker));
        }
    }

    pub async fn wait_all(&mut self) {
        let worker_count = self.active.len();
        info!("Waiting for {} workers to complete", worker_count);

        let workers = mem::take(&mut self.active);
        for (_, worker) in workers {
            worker.wait().await;
        }

        info!("All {} workers completed", worker_count);
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
}

impl Deref for TableSyncWorkerPool {
    type Target = RwLock<TableSyncWorkerPoolInner>;

    fn deref(&self) -> &Self::Target {
        &self.workers
    }
}

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
pub struct TableSyncWorkerPoolInner {
    workers: HashMap<Oid, TableSyncWorkerHandle>,
}

impl TableSyncWorkerPoolInner {
    fn new() -> Self {
        Self {
            workers: HashMap::new(),
        }
    }

    pub async fn start_worker<S, D>(&mut self, worker: TableSyncWorker<S, D>) -> bool
    where
        S: PipelineStateStore + Clone + Send + 'static,
        D: Destination + Clone + Send + 'static,
    {
        let table_id = worker.table_id();
        if self.workers.contains_key(&table_id) {
            warn!("Worker for table {} already exists in pool", table_id);
            return false;
        }

        info!("Starting new worker for table {}", table_id);
        let Some(handle) = worker.start().await else {
            warn!("Failed to start worker for table {}", table_id);
            return false;
        };

        self.workers.insert(table_id, handle);
        info!("Successfully added worker for table {} to pool", table_id);

        true
    }

    pub async fn get_worker_state(&self, table_id: Oid) -> Option<TableSyncWorkerState> {
        let state = self.workers.get(&table_id)?.state().clone();
        info!("Retrieved worker state for table {}", table_id);

        Some(state)
    }

    pub async fn remove_worker(&mut self, table_id: Oid) {
        if self.workers.remove(&table_id).is_some() {
            info!("Removed worker for table {} from pool", table_id);
        } else {
            warn!(
                "Attempted to remove non-existent worker for table {}",
                table_id
            );
        }
    }

    pub async fn wait_all(&mut self) {
        let worker_count = self.workers.len();
        info!("Waiting for {} workers to complete", worker_count);

        let workers = mem::take(&mut self.workers);
        for (table_id, worker) in workers {
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

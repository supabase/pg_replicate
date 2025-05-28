use postgres::schema::Oid;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;

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
            return false;
        }

        let Some(handle) = worker.start().await else {
            return false;
        };

        self.workers.insert(table_id, handle);

        true
    }

    pub async fn get_worker_state(&self, table_id: Oid) -> Option<TableSyncWorkerState> {
        Some(self.workers.get(&table_id)?.state().clone())
    }

    pub async fn remove_worker(&mut self, table_id: Oid) {
        self.workers.remove(&table_id);
    }

    pub async fn wait_all(&mut self) {
        let workers = mem::take(&mut self.workers);
        for (_, worker) in workers {
            worker.wait().await;
        }
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

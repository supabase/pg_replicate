use postgres::schema::Oid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::v2::destination::Destination;
use crate::v2::state::store::PipelineStateStore;
use crate::v2::workers::apply::{ApplyWorker, ApplyWorkerHandle};
use crate::v2::workers::base::{Worker, WorkerHandle};
use crate::v2::workers::table_sync::TableSyncWorkerHandle;

#[derive(Debug)]
enum PipelineWorkers {
    NotStarted,
    Started {
        // TODO: investigate whether we could benefit from a central launcher that deals at a high-level
        //  with workers management, which should not be done in the pipeline.
        apply_worker_handle: ApplyWorkerHandle,
        table_sync_workers_handles: Arc<Mutex<HashMap<Oid, TableSyncWorkerHandle>>>,
    },
}

#[derive(Debug)]
pub struct Pipeline<S, D> {
    id: u64,
    publication_name: String,
    state_store: S,
    destination: D,
    workers: PipelineWorkers,
}

impl<S, D> Pipeline<S, D>
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    pub fn new(id: u64, publication_name: String, state_store: S, destination: D) -> Self {
        Self {
            id,
            publication_name,
            state_store,
            destination,
            workers: PipelineWorkers::NotStarted,
        }
    }

    pub async fn start(&mut self) {
        let apply_worker = ApplyWorker::new();
        self.workers = PipelineWorkers::Started {
            apply_worker_handle: apply_worker.start().await,
            table_sync_workers_handles: Arc::new(Mutex::new(HashMap::new())),
        };
    }

    pub async fn wait(self) {
        let PipelineWorkers::Started {
            mut apply_worker_handle,
            table_sync_workers_handles,
        } = self.workers
        else {
            return;
        };

        let mut table_sync_workers_handles = table_sync_workers_handles.lock().await;
        for handle in table_sync_workers_handles.values_mut() {
            handle.wait().await;
        }

        apply_worker_handle.wait().await;
    }
}

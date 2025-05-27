use crate::v2::destination::Destination;
use crate::v2::replication::client::PgReplicationClient;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::apply::{ApplyWorker, ApplyWorkerHandle};
use crate::v2::workers::base::{Worker, WorkerHandle};
use crate::v2::workers::table_sync::TableSyncWorkerHandle;
use postgres::schema::Oid;
use postgres::tokio::options::PgDatabaseOptions;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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
    replication_client: PgReplicationClient,
    state_store: S,
    destination: D,
    workers: PipelineWorkers,
}

impl<S, D> Pipeline<S, D>
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    pub async fn new(
        id: u64,
        publication_name: String,
        options: PgDatabaseOptions,
        state_store: S,
        destination: D,
    ) -> Self {
        let replication_client = PgReplicationClient::new(options.clone());

        Self {
            id,
            publication_name,
            replication_client,
            state_store,
            destination,
            workers: PipelineWorkers::NotStarted,
        }
    }

    pub async fn start(&mut self) {
        let apply_worker = ApplyWorker::new(self.state_store.clone());
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

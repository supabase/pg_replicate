use postgres::tokio::options::PgDatabaseOptions;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::v2::destination::Destination;
use crate::v2::replication::client::PgReplicationClient;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::apply::{ApplyWorker, ApplyWorkerHandle};
use crate::v2::workers::base::{Worker, WorkerHandle};
use crate::v2::workers::table_sync::TableSyncWorkerHandles;

#[derive(Debug)]
enum PipelineWorkers {
    NotStarted,
    Started {
        // TODO: investigate whether we could benefit from a central launcher that deals at a high-level
        //  with workers management, which should not be done in the pipeline.
        apply_worker: ApplyWorkerHandle,
        table_sync_workers: TableSyncWorkerHandles,
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
        // We synchronize the relation subscription states with the publication, to make sure we
        // always know which tables to work with. Maybe in the future we also want to react in real
        // time to new relation ids being sent over by the cdc event stream.
        self.sync_relation_subscription_states().await;

        // We create the table sync workers shared memory area.
        let table_sync_workers = Arc::new(RwLock::new(HashMap::new()));

        // We create and start the apply worker.
        let apply_worker = ApplyWorker::new(
            self.state_store.clone(),
            self.destination.clone(),
            table_sync_workers.clone(),
        )
        .start()
        .await;

        self.workers = PipelineWorkers::Started {
            apply_worker,
            table_sync_workers,
        };
    }

    async fn sync_relation_subscription_states(&self) {
        // TODO: in this function we want to:
        //  1. Load all tables for the publication
        //  2. For each table, we check if it already exists in the store
        //  3. If the table is not there, add it with `Init` state
        //  4. If it's there do not do anything
    }

    pub async fn wait(self) {
        let PipelineWorkers::Started {
            apply_worker: mut apply_worker_handle,
            table_sync_workers: table_sync_workers_handles,
        } = self.workers
        else {
            return;
        };

        let mut table_sync_workers_handles = table_sync_workers_handles.write().await;
        for handle in table_sync_workers_handles.values_mut() {
            handle.wait().await;
        }

        apply_worker_handle.wait().await;
    }
}

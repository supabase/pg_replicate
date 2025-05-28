use crate::v2::destination::base::Destination;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopHook};
use crate::v2::state::relation_subscription::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::base::{Worker, WorkerHandle};
use crate::v2::workers::table_sync::{TableSyncWorker, TableSyncWorkers};

use postgres::schema::Oid;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;

#[derive(Debug)]
pub struct ApplyWorkerHandle {
    handle: Option<JoinHandle<()>>,
}

impl WorkerHandle<()> for ApplyWorkerHandle {
    fn state(&self) -> () {}

    async fn wait(mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };

        // TODO: properly handle failure.
        handle.await.expect("Apply worker failed");
    }
}

#[derive(Debug)]
pub struct ApplyWorker<S, D> {
    state_store: S,
    destination: D,
    table_sync_workers: TableSyncWorkers,
}

impl<S, D> ApplyWorker<S, D> {
    pub fn new(state_store: S, destination: D, table_sync_workers: TableSyncWorkers) -> Self {
        Self {
            state_store,
            destination,
            table_sync_workers,
        }
    }
}

impl<S, D> Worker<ApplyWorkerHandle, ()> for ApplyWorker<S, D>
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    async fn start(self) -> Option<ApplyWorkerHandle> {
        println!("Starting apply worker");
        let handle = tokio::spawn(async move {
            // We load the initial state that will be used for the apply worker.
            let pipeline_state = self.state_store.load_pipeline_state().await;

            // We start the applying loop by starting from the last LSN that we know was applied
            // by the destination.
            let hook = Hook {
                table_sync_workers: self.table_sync_workers,
            };
            start_apply_loop(
                self.state_store,
                self.destination,
                hook,
                pipeline_state.last_lsn,
            )
            .await;
        });

        Some(ApplyWorkerHandle {
            handle: Some(handle),
        })
    }
}

#[derive(Debug)]
struct Hook {
    table_sync_workers: TableSyncWorkers,
}

impl<S, D> ApplyLoopHook<S, D> for Hook
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    async fn process_syncing_tables(
        &self,
        state_store: S,
        destination: D,
        current_lsn: PgLsn,
    ) -> () {
        let table_replication_states = state_store.load_table_replication_states().await;

        for table_replication_state in table_replication_states {
            if let TableReplicationPhase::SyncDone { lsn } = table_replication_state.phase {
                if current_lsn >= lsn {
                    let table_replication_state = table_replication_state
                        .with_phase(TableReplicationPhase::Ready { lsn: current_lsn });
                    state_store
                        .store_table_replication_state(table_replication_state)
                        .await;
                }
            } else {
                if let Some(table_sync_worker_state) = self
                    .table_sync_workers
                    .get_worker_state(table_replication_state.id)
                    .await
                {
                    let mut inner = table_sync_worker_state.inner().write().await;

                    let mut catchup_started = false;
                    if inner.get_phase().as_type() == TableReplicationPhaseType::SyncWait {
                        inner.set_phase(TableReplicationPhase::Catchup { lsn: current_lsn });
                        catchup_started = true;
                    }

                    drop(inner);

                    if catchup_started {
                        let _ = table_sync_worker_state
                            .wait_for_phase_type(TableReplicationPhaseType::SyncDone)
                            .await;
                        println!("Just caught up with the apply worker.")
                    }
                } else {
                    let worker = TableSyncWorker::new(
                        state_store.clone(),
                        destination.clone(),
                        table_replication_state.id,
                    );
                    self.table_sync_workers.start_worker(worker).await;
                }
            }
        }
    }

    async fn should_apply_changes(&self, table_id: Oid, remote_final_lsn: PgLsn) -> bool {
        let Some(table_sync_worker_state) =
            self.table_sync_workers.get_worker_state(table_id).await
        else {
            return false;
        };

        let inner = table_sync_worker_state.inner().read().await;
        match inner.get_phase() {
            TableReplicationPhase::Ready { .. } => true,
            TableReplicationPhase::SyncDone { lsn } => lsn <= remote_final_lsn,
            _ => false,
        }
    }
}

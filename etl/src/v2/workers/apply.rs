use crate::v2::destination::Destination;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::base::{Worker, WorkerHandle};
use crate::v2::workers::table_sync::TableSyncWorkers;
use std::future::Future;

use crate::v2::replication::apply::{start_apply_loop, ApplyLoopHook};
use crate::v2::replication::base::ApplyLoopHook;
use crate::v2::state::relation_subscription::{
    RelationSubscriptionState, RelationSubscriptionStatus,
};
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
    async fn start(self) -> ApplyWorkerHandle {
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

        ApplyWorkerHandle {
            handle: Some(handle),
        }
    }
}

#[derive(Debug)]
struct Hook {
    table_sync_workers: TableSyncWorkers,
}

impl<S, D> ApplyLoopHook<S, D> for Hook
where
    S: PipelineStateStore + Send + 'static,
    D: Destination + Send + 'static,
{
    async fn process_syncing_tables(
        &self,
        state_store: S,
        destination: D,
        current_lsn: PgLsn,
    ) -> () {
        let relation_subscription_states = state_store.load_relation_subscription_states().await;

        for relation_subscription_state in relation_subscription_states {
            if let RelationSubscriptionStatus::SyncDone { lsn } = relation_subscription_state.status
            {
                // TODO: implement of syncdone.
            } else {
                if let Some(table_sync_worker_state) = self
                    .table_sync_workers
                    .get_worker_state(relation_subscription_state.rel_id)
                    .await
                {
                } else {
                    // TODO: spawn worker.
                }
            }
        }
    }

    fn should_apply_changes(
        &self,
        relation_subscription_state: &RelationSubscriptionState,
    ) -> bool {
        /*
        (rel->state == SUBREL_STATE_READY ||
                    (rel->state == SUBREL_STATE_SYNCDONE &&
                     rel->statelsn <= remote_final_lsn));
         */
        todo!()
    }
}

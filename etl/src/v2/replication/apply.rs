use crate::v2::destination::Destination;
use crate::v2::state::relation_subscription::RelationSubscriptionState;
use crate::v2::state::store::base::PipelineStateStore;
use postgres::schema::Oid;
use std::future::Future;
use tokio_postgres::types::PgLsn;

pub trait ApplyLoopHook<S, D>
where
    S: PipelineStateStore + Send + 'static,
    D: Destination + Send + 'static,
{
    fn process_syncing_tables(
        &self,
        state_store: S,
        destination: D,
        current_lsn: PgLsn,
    ) -> impl Future<Output = ()> + Send;

    fn should_apply_changes(&self, rel_id: Oid) -> bool;
}

pub async fn start_apply_loop<S, D, T>(state_store: S, destination: D, hook: T, last_lsn: PgLsn)
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook<S, D>,
{
    loop {
        // Read from socket until end or until boundary. (Modify custom cdcstream if needed)

        // For each operation apply it and call the table syncing and update the last lsn

        // If the stream ended in a transaction do one last sync, if it did not, do not do it.

        hook.process_syncing_tables(state_store.clone(), destination.clone(), last_lsn)
            .await;
    }
}

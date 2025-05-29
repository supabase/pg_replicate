use postgres::schema::Oid;
use std::future::Future;
use tokio_postgres::types::PgLsn;

use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::destination::base::Destination;

pub trait ApplyLoopHook<S, D>
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    fn process_syncing_tables(
        &self,
        state_store: S,
        destination: D,
        current_lsn: PgLsn,
    ) -> impl Future<Output = ()> + Send;

    fn should_apply_changes(
        &self,
        table_id: Oid,
        remote_final_lsn: PgLsn,
    ) -> impl Future<Output = bool> + Send;
}

pub async fn start_apply_loop<S, D, T>(state_store: S, destination: D, hook: T, last_lsn: PgLsn)
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook<S, D>,
{
    // Create a select between:
    //  - Shutdown signal -> when called we stop the apply operation.
    //  - Logical replication stream socket -> when an event is received, we handle it in a special
    //      processing component.
    //  - Else we do the table syncing if we are not at a boundary -> we perform table syncing to make
    //     sure progress is happening in table sync workers.

    // TODO: implement.
}

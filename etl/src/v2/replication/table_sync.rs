use crate::v2::destination::base::Destination;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::table_sync::TableSyncWorkerState;

pub async fn start_table_sync<S, D>(
    state_store: S,
    destination: D,
    table_sync_worker_state: TableSyncWorkerState,
) where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    // 1. Load the state from the TableSyncWorkerState (must be done there since we know that the
    //    in-memory state is consistent from this point, if we re-read from the store, we already
    //    gave out the state which could have been changed already).
    // 2. Check the state and exit if SyncDone, Ready, Unknown
    // 3. Compute the slot name
    // 4. Make sure the state is either Init, DataSync, FinishedCopy
    // 5. Handle slot deletion in case it's in DataSync
    // 6. Start copy table with transaction
    // 7. Mark the table as FinishedCopy

    // Mark the table as SyncWait in memory only

    // Wait until the catchup is reached

    // TODO: implement.
}

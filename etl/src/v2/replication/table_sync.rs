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
    // Load the relation's subscription given the rel_id

    // Check the state and exit if SyncDone, Ready, Unknown

    // Compute the slot name

    // Make sure the state is either Init, DataSync, FinishedCopy

    // Handle slot deletion in case it's in data sync

    // Start copy table with transaction

    // Mark the table as FinishedCopy

    // Mark the table as SyncWait in memory only

    // Wait until the catchup is reached
}

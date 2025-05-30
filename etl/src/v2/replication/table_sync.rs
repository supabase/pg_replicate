use crate::v2::destination::base::Destination;
use crate::v2::replication::client::PgReplicationClient;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::state::table::TableReplicationPhaseType;
use crate::v2::workers::table_sync::TableSyncWorkerState;
use std::any::Any;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TableSyncError {
    #[error("The phase {0} is not expected")]
    InvalidPhase(TableReplicationPhaseType),
}

#[derive(Debug)]
pub enum TableSyncResult {
    SyncNotRequired,
    SyncCompleted,
}

pub async fn start_table_sync<S, D>(
    replication_client: PgReplicationClient,
    table_sync_worker_state: TableSyncWorkerState,
    state_store: S,
    destination: D,
) -> Result<TableSyncResult, TableSyncError>
where
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

    let inner = table_sync_worker_state.inner().read().await;

    let phase_type = inner.phase().as_type();

    // We have two cases:
    // 1. The table sync is performed on a worker that is supposed to be finished.
    //  In this case, we want to do a noop, since there is no need to sync any table.
    // 2. The table sync is performed on a worker that is waiting to catchup, or it's catching up.
    //  In this case, we error, since this situation should not occur because a new worker should
    //  be restarted in case of a failure and `SyncWait` and `Ready` are not persisted.
    if phase_type == TableReplicationPhaseType::SyncDone
        || phase_type == TableReplicationPhaseType::Ready
        || phase_type == TableReplicationPhaseType::Unknown
    {
        return Ok(TableSyncResult::SyncNotRequired);
    } else if phase_type == TableReplicationPhaseType::SyncWait
        || phase_type == TableReplicationPhaseType::Catchup
    {
        return Err(TableSyncError::InvalidPhase(phase_type));
    }

    let slot_name = "";
    Ok(TableSyncResult::SyncCompleted)
}

use crate::v2::destination::base::Destination;
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError, SlotUsage};
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::workers::table_sync::TableSyncWorkerState;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

#[derive(Debug, Error)]
pub enum TableSyncError {
    #[error("The phase {0} is not expected")]
    InvalidPhase(TableReplicationPhaseType),

    #[error("The slot name {0} is not valid")]
    InvalidSlotName(#[from] SlotError),

    #[error("An error occurred when dealing with Postgres replication: {0}")]
    PgReplication(#[from] PgReplicationError),
}

#[derive(Debug)]
pub enum TableSyncResult {
    SyncNotRequired,
    SyncCompleted { consistent_point: PgLsn },
}

pub async fn start_table_sync<S, D>(
    identity: PipelineIdentity,
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
    let inner = table_sync_worker_state.inner().read().await;

    let table_id = inner.table_id();
    let phase_type = inner.replication_phase().as_type();

    // In case the work for this table has been already done, we don't want to continue and we
    // successfully return.
    if phase_type == TableReplicationPhaseType::SyncDone
        || phase_type == TableReplicationPhaseType::Ready
        || phase_type == TableReplicationPhaseType::Unknown
    {
        return Ok(TableSyncResult::SyncNotRequired);
    }

    // In case the phase is different from the standard phases in which a table sync worker can perform
    // table syncing, we want to return an error.
    if !(phase_type == TableReplicationPhaseType::Init
        || phase_type == TableReplicationPhaseType::DataSync
        || phase_type == TableReplicationPhaseType::FinishedCopy)
    {
        return Err(TableSyncError::InvalidPhase(phase_type));
    }

    // We are safe to unlock the state here, since we know that the state will be changed by the
    // apply worker only if `SyncWait` is set, which is not the case if we arrive here, so we are
    // good to reduce the length of the critical section.
    drop(inner);

    let slot_name = get_slot_name(identity, SlotUsage::TableSyncWorker { table_id })?;

    // If we hit this condition, it means that we crashed either before or after finishing the table
    // copying or even during catchup. Since we don't make any time assumptions about when this worker
    // is restarted with this state, we need to delete the existing slot (if any) since we might have
    // lost WAL entries so it's not safe to try and catchup again.
    if phase_type == TableReplicationPhaseType::DataSync
        || phase_type == TableReplicationPhaseType::FinishedCopy
    {
        if let Err(err) = replication_client.delete_slot(&slot_name).await {
            // If the slot is not found, we are safe to continue, for any other error, we bail.
            let PgReplicationError::SlotNotFound(_) = err else {
                return Err(err.into());
            };
        }
    }

    // We are ready to start copying table data and we update the state accordingly.
    let mut inner = table_sync_worker_state.inner().write().await;
    inner
        .set_phase_with(TableReplicationPhase::DataSync, state_store.clone())
        .await;
    drop(inner);

    // TODO: implement table copying.

    Ok(TableSyncResult::SyncCompleted {
        consistent_point: PgLsn::from(0),
    })
}

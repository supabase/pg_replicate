use crate::v2::concurrency::stream::BoundedBatchStream;
use crate::v2::config::batch::BatchConfig;
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError, SlotUsage};
use crate::v2::replication::stream::{TableCopyStream, TableCopyStreamError};
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::workers::table_sync::{TableSyncWorkerState, TableSyncWorkerStateError};
use futures::StreamExt;
use thiserror::Error;
use tokio::pin;
use tokio_postgres::types::PgLsn;

#[derive(Debug, Error)]
pub enum TableSyncError {
    #[error("Invalid replication phase '{0}': expected Init, DataSync, or FinishedCopy")]
    InvalidPhase(TableReplicationPhaseType),

    #[error("Invalid replication slot name: {0}")]
    InvalidSlotName(#[from] SlotError),

    #[error("PostgreSQL replication operation failed: {0}")]
    PgReplication(#[from] PgReplicationError),

    #[error("An error occurred while interacting with the table sync worker state: {0}")]
    TableSyncWorkerState(#[from] TableSyncWorkerStateError),

    #[error("An error occurred while writing to the destination: {0}")]
    Destination(#[from] DestinationError),

    #[error("An error happened in the pipeline state store: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("An error happened in the table copy stream")]
    TableCopyStream(#[from] TableCopyStreamError),
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
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
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

    let slot_name = get_slot_name(&identity, SlotUsage::TableSyncWorker { table_id })?;

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

    // We are ready to start copying table data, and we update the state accordingly.
    let mut inner = table_sync_worker_state.inner().write().await;
    inner
        .set_phase_with(TableReplicationPhase::DataSync, state_store.clone())
        .await?;
    drop(inner);

    // We create the slot with a transaction, since we need to have a consistent snapshot of the database
    // before copying the schema and tables.
    let (transaction, slot) = replication_client
        .create_slot_with_transaction(&slot_name)
        .await?;

    // We copy the table schema and write it both to the state store and destination.
    //
    // Note that we write the schema in both places:
    // - State store -> we write here because the table schema is used across table copying and cdc
    //  for correct decoding, thus we rely on our own state store to preserve this information.
    // - Destination -> we write here because some consumers might want to have the schema of incoming
    //  data.
    let table_schema = transaction
        .get_table_schema(table_id, Some(identity.publication_name()))
        .await?;
    state_store
        .store_table_schema(identity.id(), table_schema.clone(), true)
        .await?;
    destination.write_table_schema(table_schema.clone()).await?;

    // We create the copy table stream.
    let table_copy_stream = transaction
        .get_table_copy_stream(table_id, &table_schema.column_schemas)
        .await?;
    let table_copy_stream = TableCopyStream::wrap(table_copy_stream, &table_schema.column_schemas);
    // TODO: supply the actual pipeline config.
    let table_copy_stream =
        BoundedBatchStream::wrap(table_copy_stream, BatchConfig::default(), None);
    pin!(table_copy_stream);

    // We start consuming the table stream. If any error occurs, we will bail the entire copy since
    // we want to be fully consistent.
    while let Some(table_rows) = table_copy_stream.next().await {
        let table_rows = table_rows.into_iter().collect::<Result<Vec<_>, _>>()?;
        destination.copy_table_rows(table_id, table_rows).await?;
    }

    // We mark that we finished the copy of the table schema and data. Then we immediately set
    // this worker as ready to be synced.
    //
    // Note that `FinishedCopy` will be saved to the store but `SyncWait` will be just saved in the
    // table sync state.
    let mut inner = table_sync_worker_state.inner().write().await;
    inner
        .set_phase_with(TableReplicationPhase::FinishedCopy, state_store.clone())
        .await?;
    inner
        .set_phase_with(TableReplicationPhase::SyncWait, state_store)
        .await?;
    drop(inner);

    Ok(TableSyncResult::SyncCompleted {
        consistent_point: slot.consistent_point,
    })
}

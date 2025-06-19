use crate::v2::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::v2::concurrency::stream::BatchStream;
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError};
use crate::v2::replication::stream::{TableCopyStream, TableCopyStreamError};
use crate::v2::schema::cache::SchemaCache;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::workers::base::WorkerType;
use crate::v2::workers::table_sync::{TableSyncWorkerState, TableSyncWorkerStateError};
use futures::StreamExt;
use postgres::schema::Oid;
use std::sync::Arc;
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

    #[error("An error happened in the state store: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("An error happened in the table copy stream")]
    TableCopyStream(#[from] TableCopyStreamError),
}

#[derive(Debug)]
pub enum TableSyncResult {
    SyncStopped,
    SyncNotRequired,
    SyncCompleted { start_lsn: PgLsn },
}

#[allow(clippy::too_many_arguments)]
pub async fn start_table_sync<S, D>(
    identity: PipelineIdentity,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    table_id: Oid,
    table_sync_worker_state: TableSyncWorkerState,
    schema_cache: SchemaCache,
    state_store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
) -> Result<TableSyncResult, TableSyncError>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    let inner = table_sync_worker_state.get_inner().read().await;
    let phase_type = inner.replication_phase().as_type();

    // In case the work for this table has been already done, we don't want to continue and we
    // successfully return.
    if matches!(
        phase_type,
        TableReplicationPhaseType::SyncDone | TableReplicationPhaseType::Ready
    ) {
        return Ok(TableSyncResult::SyncNotRequired);
    }

    // In case the phase is different from the standard phases in which a table sync worker can perform
    // table syncing, we want to return an error.
    if !matches!(
        phase_type,
        TableReplicationPhaseType::DataSync | TableReplicationPhaseType::FinishedCopy
    ) {
        return Err(TableSyncError::InvalidPhase(phase_type));
    }

    // We are safe to unlock the state here, since we know that the state will be changed by the
    // apply worker only if `SyncWait` is set, which is not the case if we arrive here, so we are
    // good to reduce the length of the critical section.
    drop(inner);

    let slot_name = get_slot_name(&identity, WorkerType::TableSync { table_id })?;

    // There are three phases in which the table can be in:
    // - `Init` -> this means that the table sync was never done, so we just perform it.
    // - `DataSync` -> this means that there was a failure during data sync, and we have to restart
    //  copying all the table data and delete the slot.
    // - `FinishedCopy` -> this means that the table was successfully copied, but we didn't manage to
    //  complete the table sync function, so we just want to load the start lsn from the slot to know
    //  where we want to start the cdc stream.
    //
    // In case the phase is any other phase, we will return an error.
    let start_lsn = match phase_type {
        TableReplicationPhaseType::DataSync => {
            // If we are in `DataSync` it means we failed during table copying, so we want to delete the
            // existing slot before continuing.
            if phase_type == TableReplicationPhaseType::DataSync {
                if let Err(err) = replication_client.delete_slot(&slot_name).await {
                    // If the slot is not found, we are safe to continue, for any other error, we bail.
                    if !matches!(err, PgReplicationError::SlotNotFound(_)) {
                        return Err(err.into());
                    }
                }
            }

            // We are ready to start copying table data, and we update the state accordingly.
            {
                let mut inner = table_sync_worker_state.get_inner().write().await;
                inner
                    .set_phase_with(TableReplicationPhase::DataSync, state_store.clone())
                    .await?;
            }

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
            schema_cache.add_table_schema(table_schema.clone()).await;
            destination.write_table_schema(table_schema.clone()).await?;

            // We create the copy table stream.
            let table_copy_stream = transaction
                .get_table_copy_stream(table_id, &table_schema.column_schemas)
                .await?;
            let table_copy_stream =
                TableCopyStream::wrap(table_copy_stream, &table_schema.column_schemas);
            let table_copy_stream = BatchStream::wrap(
                table_copy_stream,
                config.batch_config.clone(),
                shutdown_rx.clone(),
            );
            pin!(table_copy_stream);

            // We start consuming the table stream. If any error occurs, we will bail the entire copy since
            // we want to be fully consistent.
            while let Some(result) = table_copy_stream.next().await {
                match result {
                    ShutdownResult::Ok(table_rows) => {
                        let table_rows = table_rows.into_iter().collect::<Result<Vec<_>, _>>()?;
                        destination.write_table_rows(table_id, table_rows).await?;
                    }
                    ShutdownResult::Shutdown(_) => {
                        // If we received a shutdown in the middle of a table copy, we bail knowing
                        // that the system can automatically recover if a table copy has failed in
                        // the middle of processing.
                        return Ok(TableSyncResult::SyncStopped);
                    }
                }
            }

            // We commit the transaction before starting the apply loop, otherwise it will fail
            // since no transactions can be running while replication is started.
            transaction.commit().await?;

            // We mark that we finished the copy of the table schema and data.
            {
                let mut inner = table_sync_worker_state.get_inner().write().await;
                inner
                    .set_phase_with(TableReplicationPhase::FinishedCopy, state_store.clone())
                    .await?;
            }
            slot.consistent_point
        }
        TableReplicationPhaseType::FinishedCopy => {
            let slot = replication_client.get_slot(&slot_name).await?;
            slot.confirmed_flush_lsn
        }
        _ => unreachable!("Phase type already validated above"),
    };

    // We mark this worker as `SyncWait` (in memory only) to signal the apply worker that we are
    // ready to start catchup.
    {
        let mut inner = table_sync_worker_state.get_inner().write().await;
        inner
            .set_phase_with(TableReplicationPhase::SyncWait, state_store)
            .await?;
    }

    // We also wait to be signaled to catchup with the main apply worker up to a specific lsn.
    let result = table_sync_worker_state
        .wait_for_phase_type(TableReplicationPhaseType::Catchup, shutdown_rx.clone())
        .await;

    // If we are told to shut down while waiting for a phase change, we will signal this to
    // the caller.
    if result.should_shutdown() {
        return Ok(TableSyncResult::SyncStopped);
    }

    Ok(TableSyncResult::SyncCompleted { start_lsn })
}

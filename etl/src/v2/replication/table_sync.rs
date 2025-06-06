use crate::v2::concurrency::stream::BoundedBatchStream;
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError, SlotUsage};
use crate::v2::replication::stream::{TableCopyStream, TableCopyStreamError};
use crate::v2::state::origin::ReplicationOriginState;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::workers::table_sync::{TableSyncWorkerState, TableSyncWorkerStateError};
use futures::StreamExt;
use std::sync::Arc;
use thiserror::Error;
use tokio::pin;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;
use std::fs::OpenOptions;
use std::io::Write;
use postgres::schema::Oid;

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

    #[error("The replication origin state was not found but is required")]
    ReplicationOriginStateNotFound,
}

#[derive(Debug)]
pub enum TableSyncResult {
    SyncNotRequired,
    SyncCompleted { origin_start_lsn: PgLsn },
}

async fn log_to_file(table_id: Oid, component: &str, message: &str) {
    let file_path = format!("table_sync_replication_{}.log", table_id);
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
    {
        let _ = writeln!(file, "[{}] {}", component, message);
    }
}

pub async fn start_table_sync<S, D>(
    identity: PipelineIdentity,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    table_sync_worker_state: TableSyncWorkerState,
    state_store: S,
    destination: D,
    shutdown_rx: watch::Receiver<()>,
) -> Result<TableSyncResult, TableSyncError>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    let inner = table_sync_worker_state.get_inner().read().await;

    let table_id = inner.table_id();
    let phase_type = inner.replication_phase().as_type();

    log_to_file(
        table_id,
        "REPLICATION",
        &format!("Starting table sync for table {} with phase {:?}", table_id, phase_type),
    )
    .await;

    if matches!(
        phase_type,
        TableReplicationPhaseType::SyncDone
            | TableReplicationPhaseType::Ready
            | TableReplicationPhaseType::Unknown
    ) {
        log_to_file(
            table_id,
            "REPLICATION",
            &format!("Sync not required for table {} as phase is {:?}", table_id, phase_type),
        )
        .await;
        return Ok(TableSyncResult::SyncNotRequired);
    }

    if !matches!(
        phase_type,
        TableReplicationPhaseType::Init
            | TableReplicationPhaseType::DataSync
            | TableReplicationPhaseType::FinishedCopy
    ) {
        log_to_file(
            table_id,
            "REPLICATION",
            &format!("ERROR: Invalid phase {:?} for table {}", phase_type, table_id),
        )
        .await;
        return Err(TableSyncError::InvalidPhase(phase_type));
    }

    drop(inner);

    let slot_name = get_slot_name(&identity, SlotUsage::TableSyncWorker { table_id })?;

    let replication_origin_state = match phase_type {
        TableReplicationPhaseType::Init | TableReplicationPhaseType::DataSync => {
            if phase_type == TableReplicationPhaseType::DataSync {
                log_to_file(
                    table_id,
                    "REPLICATION",
                    &format!("Deleting existing slot {} for table {}", slot_name, table_id),
                )
                .await;

                if let Err(err) = replication_client.delete_slot(&slot_name).await {
                    if !matches!(err, PgReplicationError::SlotNotFound(_)) {
                        log_to_file(
                            table_id,
                            "REPLICATION",
                            &format!(
                                "ERROR: Failed to delete slot {} for table {}: {}",
                                slot_name, table_id, err
                            ),
                        )
                        .await;
                        return Err(err.into());
                    }
                }
            }

            {
                let mut inner = table_sync_worker_state.get_inner().write().await;
                inner
                    .set_phase_with(TableReplicationPhase::DataSync, state_store.clone())
                    .await?;
            }

            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Creating slot {} for table {}", slot_name, table_id),
            )
            .await;

            let (transaction, slot) = replication_client
                .create_slot_with_transaction(&slot_name)
                .await?;

            let replication_origin_state =
                ReplicationOriginState::new(identity.id(), Some(table_id), slot.consistent_point);
            state_store
                .store_replication_origin_state(replication_origin_state.clone(), true)
                .await?;

            log_to_file(
                table_id,
                "REPLICATION",
                &format!(
                    "Stored replication origin state for table {} with LSN {}",
                    table_id, slot.consistent_point
                ),
            )
            .await;

            let table_schema = transaction
                .get_table_schema(table_id, Some(identity.publication_name()))
                .await?;
            state_store
                .store_table_schema(identity.id(), table_schema.clone(), true)
                .await?;
            destination.write_table_schema(table_schema.clone()).await?;

            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Stored table schema for table {}", table_id),
            )
            .await;

            let table_copy_stream = transaction
                .get_table_copy_stream(table_id, &table_schema.column_schemas)
                .await?;
            let table_copy_stream =
                TableCopyStream::wrap(table_copy_stream, &table_schema.column_schemas);
            let table_copy_stream = BoundedBatchStream::wrap(
                table_copy_stream,
                config.batch_config.clone(),
                shutdown_rx,
            );
            pin!(table_copy_stream);

            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Starting table copy for table {}", table_id),
            )
            .await;

            while let Some(table_rows) = table_copy_stream.next().await {
                let table_rows = table_rows.into_iter().collect::<Result<Vec<_>, _>>()?;
                destination.copy_table_rows(table_id, table_rows).await?;
            }

            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Completed table copy for table {}", table_id),
            )
            .await;

            {
                let mut inner = table_sync_worker_state.get_inner().write().await;
                inner
                    .set_phase_with(TableReplicationPhase::FinishedCopy, state_store.clone())
                    .await?;
            }

            replication_origin_state
        }
        TableReplicationPhaseType::FinishedCopy => {
            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Loading replication origin state for table {}", table_id),
            )
            .await;

            state_store
                .load_replication_origin_state(identity.id(), Some(table_id))
                .await?
                .ok_or(TableSyncError::ReplicationOriginStateNotFound)?
        }
        _ => unreachable!("Phase type already validated above"),
    };

    {
        let mut inner = table_sync_worker_state.get_inner().write().await;
        inner
            .set_phase_with(TableReplicationPhase::SyncWait, state_store)
            .await?;
    }

    log_to_file(
        table_id,
        "REPLICATION",
        &format!(
            "Table sync completed for table {} with consistent point {}",
            table_id, replication_origin_state.remote_lsn
        ),
    )
    .await;

    Ok(TableSyncResult::SyncCompleted {
        origin_start_lsn: replication_origin_state.remote_lsn,
    })
}

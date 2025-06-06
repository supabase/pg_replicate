use crate::v2::concurrency::stream::BoundedBatchStream;
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError, SlotUsage};
use crate::v2::replication::stream::{TableCopyStream, TableCopyStreamError};
use crate::v2::state::origin::ReplicationOriginState;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType, TableReplicationState};
use crate::v2::workers::table_sync::{TableSyncWorkerState, TableSyncWorkerStateError};
use futures::StreamExt;
use std::sync::Arc;
use thiserror::Error;
use tokio::pin;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use postgres::schema::Oid;
use chrono::Local;

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
    let logs_dir = Path::new("./logs");
    if !logs_dir.exists() {
        let _ = fs::create_dir_all(logs_dir);
    }
    
    let file_path = format!("./logs/table_sync_replication_{}.log", table_id);
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
    {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let _ = writeln!(file, "[{}] [{}] {}", timestamp, component, message);
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
    log_to_file(
        inner.table_id(),
        "REPLICATION",
        "Successfully acquired read lock on table sync worker state",
    ).await;

    let table_id = inner.table_id();
    let phase_type = inner.replication_phase().as_type();

    log_to_file(
        table_id,
        "REPLICATION",
        &format!("Starting table sync for table {} with phase {:?}", table_id, phase_type),
    ).await;

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
        ).await;
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
        ).await;
        return Err(TableSyncError::InvalidPhase(phase_type));
    }

    drop(inner);
    log_to_file(table_id, "REPLICATION", "Released read lock on table sync worker state").await;

    let slot_name = get_slot_name(&identity, SlotUsage::TableSyncWorker { table_id })?;
    log_to_file(
        table_id,
        "REPLICATION",
        &format!("Generated slot name: {} for table {}", slot_name, table_id),
    ).await;

    let replication_origin_state = match phase_type {
        TableReplicationPhaseType::Init | TableReplicationPhaseType::DataSync => {
            if phase_type == TableReplicationPhaseType::DataSync {
                log_to_file(
                    table_id,
                    "REPLICATION",
                    &format!("Deleting existing slot {} for table {}", slot_name, table_id),
                ).await;

                if let Err(err) = replication_client.delete_slot(&slot_name).await {
                    if !matches!(err, PgReplicationError::SlotNotFound(_)) {
                        log_to_file(
                            table_id,
                            "REPLICATION",
                            &format!(
                                "ERROR: Failed to delete slot {} for table {}: {}",
                                slot_name, table_id, err
                            ),
                        ).await;
                        return Err(err.into());
                    }
                    log_to_file(
                        table_id,
                        "REPLICATION",
                        &format!("Slot {} not found for table {}, continuing with creation", slot_name, table_id),
                    ).await;
                } else {
                    log_to_file(
                        table_id,
                        "REPLICATION",
                        &format!("Successfully deleted slot {} for table {}", slot_name, table_id),
                    ).await;
                }
            }

            {
                let mut inner = table_sync_worker_state.get_inner().write().await;
                inner.set_phase(TableReplicationPhase::DataSync);
                log_to_file(
                    table_id,
                    "REPLICATION",
                    "Successfully updated worker state phase to DataSync",
                ).await;
            }

            state_store.store_table_replication_state(
                TableReplicationState {
                    pipeline_id: identity.id(),
                    table_id: table_id,
                    phase: TableReplicationPhase::DataSync
                },
                true
            ).await?;
            log_to_file(
                table_id,
                "REPLICATION",
                "Successfully stored table replication state in state store",
            ).await;

            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Creating slot {} for table {}", slot_name, table_id),
            ).await;

            let (transaction, slot) = replication_client
                .create_slot_with_transaction(&slot_name)
                .await?;
            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Successfully created slot {} with consistent point {}", slot_name, slot.consistent_point),
            ).await;

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
            ).await;

            let table_schema = transaction
                .get_table_schema(table_id, Some(identity.publication_name()))
                .await?;
            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Retrieved table schema for table {} with {} columns", table_id, table_schema.column_schemas.len()),
            ).await;

            state_store
                .store_table_schema(identity.id(), table_schema.clone(), true)
                .await?;
            log_to_file(
                table_id,
                "REPLICATION",
                "Successfully stored table schema in state store",
            ).await;

            destination.write_table_schema(table_schema.clone()).await?;
            log_to_file(
                table_id,
                "REPLICATION",
                "Successfully wrote table schema to destination",
            ).await;

            let table_copy_stream = transaction
                .get_table_copy_stream(table_id, &table_schema.column_schemas)
                .await?;
            log_to_file(
                table_id,
                "REPLICATION",
                "Successfully created table copy stream",
            ).await;

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
            ).await;

            let mut total_rows_count = 0;
            while let Some(table_rows) = table_copy_stream.next().await {
                let table_rows = table_rows.into_iter().collect::<Result<Vec<_>, _>>()?;
                let current_table_rows = table_rows.len();
                total_rows_count += table_rows.len();
                destination.copy_table_rows(table_id, table_rows).await?;
                log_to_file(
                    table_id,
                    "REPLICATION",
                    &format!("Successfully copied batch of rows, total rows so far: {}", total_rows_count),
                ).await;
            }

            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Completed table copy for table {} with total {} rows", table_id, total_rows_count),
            ).await;

            {
                let mut inner = table_sync_worker_state.get_inner().write().await;
                inner.set_phase(TableReplicationPhase::FinishedCopy);
                log_to_file(
                    table_id,
                    "REPLICATION",
                    "Successfully updated worker state phase to FinishedCopy",
                ).await;
            }

            state_store.store_table_replication_state(
                TableReplicationState {
                    pipeline_id: identity.id(),
                    table_id: table_id,
                    phase: TableReplicationPhase::FinishedCopy
                },
                true
            ).await?;
            log_to_file(
                table_id,
                "REPLICATION",
                "Successfully stored final table replication state in state store",
            ).await;

            replication_origin_state
        }
        TableReplicationPhaseType::FinishedCopy => {
            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Loading replication origin state for table {}", table_id),
            ).await;

            let state = state_store
                .load_replication_origin_state(identity.id(), Some(table_id))
                .await?;
            log_to_file(
                table_id,
                "REPLICATION",
                &format!("Successfully loaded replication origin state for table {}", table_id),
            ).await;

            state.ok_or(TableSyncError::ReplicationOriginStateNotFound)?
        }
        _ => unreachable!("Phase type already validated above"),
    };

    {
        let mut inner = table_sync_worker_state.get_inner().write().await;
        inner.set_phase(TableReplicationPhase::SyncWait);
        log_to_file(
            table_id,
            "REPLICATION",
            "Successfully updated worker state phase to SyncWait",
        ).await;
    }

    log_to_file(
        table_id,
        "REPLICATION",
        &format!(
            "Table sync completed for table {} with consistent point {}",
            table_id, replication_origin_state.remote_lsn
        ),
    ).await;

    Ok(TableSyncResult::SyncCompleted {
        origin_start_lsn: replication_origin_state.remote_lsn,
    })
}

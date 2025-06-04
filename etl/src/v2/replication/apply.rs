use futures::StreamExt;
use postgres::schema::Oid;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::pin;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;

use crate::v2::concurrency::stream::{BatchBoundary, BoundedBatchStream};
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::Destination;
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError, SlotUsage};
use crate::v2::state::store::base::StateStore;
use crate::v2::workers::apply::ApplyWorkerHookError;
use crate::v2::workers::table_sync::TableSyncWorkerHookError;

/// The amount of seconds that pass between syncing via the hook in case nothing else is going on
/// in the system (e.g., no data from the stream and no shutdown signal).
const SYNCING_FREQUENCY_SECONDS: Duration = Duration::from_secs(1);

// TODO: figure out how to break the cycle and remove `Box`.
#[derive(Debug, Error)]
pub enum ApplyLoopError {
    #[error("Apply worker hook operation failed: {0}")]
    ApplyWorkerHook(Box<ApplyWorkerHookError>),

    #[error("Table sync worker hook operation failed: {0}")]
    TableSyncWorkerHook(Box<TableSyncWorkerHookError>),

    #[error("A Postgres replication error occurred in the apply loop: {0}")]
    PgReplication(#[from] PgReplicationError),

    #[error("An error occurred while streaming logical replication changes: {0}")]
    LogicalReplicationStreamFailed(#[from] tokio_postgres::Error),

    #[error("Could not generate slot name in the apply loop: {0}")]
    Slot(#[from] SlotError),
}

impl From<ApplyWorkerHookError> for ApplyLoopError {
    fn from(err: ApplyWorkerHookError) -> Self {
        ApplyLoopError::ApplyWorkerHook(Box::new(err))
    }
}

impl From<TableSyncWorkerHookError> for ApplyLoopError {
    fn from(err: TableSyncWorkerHookError) -> Self {
        ApplyLoopError::TableSyncWorkerHook(Box::new(err))
    }
}

#[derive(Debug)]
pub enum ApplyLoopResult {
    ApplyStopped,
    ApplyCompleted,
}

pub trait ApplyLoopHook {
    type Error: Into<ApplyLoopError>;

    fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn should_apply_changes(
        &self,
        table_id: Oid,
        remote_final_lsn: PgLsn,
    ) -> impl Future<Output = bool> + Send;

    fn slot_usage(&self) -> SlotUsage;
}

impl BatchBoundary for ReplicationMessage<LogicalReplicationMessage> {
    fn is_on_boundary(&self) -> bool {
        match self {
            ReplicationMessage::XLogData(message) => {
                matches!(message.data(), LogicalReplicationMessage::Commit(_))
            }
            ReplicationMessage::PrimaryKeepAlive(_) => true,
            &_ => true,
        }
    }
}

struct ApplyLoopState {
    last_received: PgLsn,
    in_remote_transaction: bool,
}

pub async fn start_apply_loop<S, D, T>(
    identity: PipelineIdentity,
    origin_start_lsn: PgLsn,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    state_store: S,
    destination: D,
    hook: T,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<ApplyLoopResult, ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let mut state = ApplyLoopState {
        last_received: origin_start_lsn,
        in_remote_transaction: false,
    };

    // We compute the slot name for the replication slot that we are going to use for the logical
    // replication. At this point we assume that the slot already exists.
    let slot_name = get_slot_name(&identity, hook.slot_usage())?;

    // We start the logical replication stream with the supplied parameters at a given lsn. That
    // lsn is the last lsn from which we need to start fetching events.
    let logical_replication_stream = replication_client
        .start_logical_replication(&slot_name, identity.publication_name(), origin_start_lsn)
        .await?;
    let logical_replication_stream = BoundedBatchStream::wrap(
        logical_replication_stream,
        config.batch_config.clone(),
        shutdown_rx.clone(),
    );
    pin!(logical_replication_stream);

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                return Ok(ApplyLoopResult::ApplyStopped);
            }
            Some(events) = logical_replication_stream.next() => {
                handle_logical_replication_message_batch(&mut state, events).await?;
            }
            _ = tokio::time::sleep(SYNCING_FREQUENCY_SECONDS) => {
                // This branch will be ready every second, allowing us to perform periodic actions
                // when no other events are occurring
                hook.process_syncing_tables(state.last_received).await?;
            }
        }
    }
}

async fn handle_logical_replication_message_batch(
    state: &mut ApplyLoopState,
    batch: Vec<Result<ReplicationMessage<LogicalReplicationMessage>, tokio_postgres::Error>>,
) -> Result<(), ApplyLoopError> {
    for message in batch {
        // If there was an error processing a message, we will return such error and stop processing
        // further events. This is fine to do, since we will consequently not acknowledge Postgres
        // about any progress on those events, and we could technically fetch them again when the
        // stream is restarted.
        // TODO: check if we want to restart the stream in case specific errors occur, instead of
        //  crashing and restarting the entire process.
        let message = message?;
        handle_logical_replication_message(state, message).await?
    }

    Ok(())
}

async fn handle_logical_replication_message(
    state: &mut ApplyLoopState,
    message: ReplicationMessage<LogicalReplicationMessage>,
) -> Result<(), ApplyLoopError> {
    match message {
        ReplicationMessage::XLogData(message) => {
            let start_lsn = PgLsn::from(message.wal_start());
            if start_lsn > state.last_received {
                state.last_received = start_lsn;
            }

            let end_lsn = PgLsn::from(message.wal_end());
            if end_lsn > state.last_received {
                state.last_received = end_lsn;
            }
        }
        ReplicationMessage::PrimaryKeepAlive(message) => {
            let end_lsn = PgLsn::from(message.wal_end());
            if end_lsn > state.last_received {
                state.last_received = end_lsn;
            }

            // TODO: send feedback back to postgres.
        }
        _ => {}
    }

    Ok(())
}

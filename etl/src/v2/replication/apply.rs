use crate::conversions::cdc_event::CdcEventConverter;
use crate::pipeline::sources::postgres::CdcStreamError::CdcEventConversion;
use crate::v2::concurrency::stream::{BatchBoundary, BoundedBatchStream};
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::conversions::event::EventConverter;
use crate::v2::destination::base::Destination;
use crate::v2::pipeline::{PipelineId, PipelineIdentity};
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError, SlotUsage};
use crate::v2::replication::stream::{EventsStream, EventsStreamError};
use crate::v2::state::origin::ReplicationOriginState;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::workers::apply::ApplyWorkerHookError;
use crate::v2::workers::table_sync::TableSyncWorkerHookError;
use futures::StreamExt;
use postgres::schema::Oid;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::pin;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;

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
    LogicalReplicationStreamFailed(#[from] EventsStreamError),

    #[error("Could not generate slot name in the apply loop: {0}")]
    Slot(#[from] SlotError),

    #[error("An error happened in the state store: {0}")]
    StateStore(#[from] StateStoreError),
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

    fn initialize(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

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

#[derive(Debug, Clone)]
struct LastStatusUpdate {
    last_received: PgLsn,
}

#[derive(Debug, Clone)]
struct ApplyLoopState {
    /// The id of the pipeline in which the apply loop state's is running.
    pipeline_id: PipelineId,
    /// The highest LSN of the received
    last_received: PgLsn,
    /// The LSN of the commit WAL entry of the transaction that is currently being processed.
    ///
    /// This LSN is set at every `BEGIN` of a new transaction.
    remote_final_lsn: PgLsn,
    /// The last status update content, used to validate whether to send a status update again is worth
    /// it.
    last_status_update: Option<LastStatusUpdate>,
}

impl ApplyLoopState {
    fn should_send_status_update(&self) -> bool {
        let Some(last_status_update) = &self.last_status_update else {
            return true;
        };

        self.last_received > last_status_update.last_received
    }

    fn status_update_sent(&mut self) {
        // This method takes a snapshot of the current state, so it must be called after the status
        // update and before any further state change.
        let last_status_update = LastStatusUpdate {
            last_received: self.last_received,
        };

        self.last_status_update = Some(last_status_update);
    }
}

impl From<ApplyLoopState> for ReplicationOriginState {
    fn from(value: ApplyLoopState) -> Self {
        // TODO: if we see that we can resume midway from within a transaction, we might want to also
        //  store the remote_final_lsn on disk, so that we can safely resume.
        Self {
            pipeline_id: value.pipeline_id,
            table_id: None,
            remote_lsn: value.last_received,
        }
    }
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
    // We initialize the shared state that is used throughout the loop to track progress.
    let mut state = ApplyLoopState {
        pipeline_id: identity.id(),
        last_received: origin_start_lsn,
        remote_final_lsn: PgLsn::from(0),
        last_status_update: None,
    };

    // We initialize the apply loop which is based on the hook implementation.
    hook.initialize().await?;

    // We compute the slot name for the replication slot that we are going to use for the logical
    // replication. At this point we assume that the slot already exists.
    let slot_name = get_slot_name(&identity, hook.slot_usage())?;

    // We start the logical replication stream with the supplied parameters at a given lsn. That
    // lsn is the last lsn from which we need to start fetching events.
    let logical_replication_stream = replication_client
        .start_logical_replication(identity.publication_name(), &slot_name, origin_start_lsn)
        .await?;
    let logical_replication_stream = EventsStream::wrap(logical_replication_stream);
    let logical_replication_stream = BoundedBatchStream::wrap(
        logical_replication_stream,
        config.batch_config.clone(),
        shutdown_rx.clone(),
    );
    pin!(logical_replication_stream);

    // We build the event converter, which will convert all the messages from the logical replication
    // protocol to events that are usable by the downstream destination.
    let event_converter = EventConverter::new(identity.id(), state_store.clone());

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                // TODO: make sure that the system is cleanly resumable once a shutdown signal is received.
                return Ok(ApplyLoopResult::ApplyStopped);
            }
            // TODO: figure out if the logical replication stream is cancellation safe.
            Some(messages_batch) = logical_replication_stream.next() => {
                let logical_replication_stream = logical_replication_stream.as_mut();
                let events_stream = unsafe { Pin::new_unchecked(logical_replication_stream.get_unchecked_mut().get_inner_mut()) };

                handle_replication_message_batch(&mut state, events_stream, messages_batch, &event_converter, &state_store, &destination, &hook).await?;
            }
            _ = tokio::time::sleep(SYNCING_FREQUENCY_SECONDS) => {
                // TODO: this is a great place to perform cleanup operations.
               let logical_replication_stream = logical_replication_stream.as_mut();
               let events_stream = unsafe { Pin::new_unchecked(logical_replication_stream.get_unchecked_mut().get_inner_mut()) };

               if state.should_send_status_update() {
                    events_stream.send_status_update(state.last_received).await?;
                    state.status_update_sent();
               }
            }
        }
    }
}

async fn handle_replication_message_batch<S, D, T>(
    state: &mut ApplyLoopState,
    mut stream: Pin<&mut EventsStream>,
    messages_batch: Vec<Result<ReplicationMessage<LogicalReplicationMessage>, EventsStreamError>>,
    event_converter: &EventConverter<S>,
    state_store: &S,
    destination: &D,
    hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    for message in messages_batch {
        let message = message?;
        handle_replication_message(
            state,
            stream.as_mut(),
            message,
            event_converter,
            state_store,
            destination,
            hook,
        )
        .await?
    }

    Ok(())
}

async fn handle_replication_message<S, D, T>(
    state: &mut ApplyLoopState,
    events_stream: Pin<&mut EventsStream>,
    message: ReplicationMessage<LogicalReplicationMessage>,
    event_converter: &EventConverter<S>,
    state_store: &S,
    destination: &D,
    hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
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

            print!(
                "\n\n MESSAGE \n    start_lsn: {:?}, end_lsn: {:?} \n    data: {:?} \n\n",
                message.wal_start(),
                message.wal_end(),
                message.data()
            );

            handle_logical_replication_message(
                state,
                message.into_data(),
                state_store,
                destination,
                hook,
            )
            .await?;

            // After successfully applying an event, we store the progress in the state store so that
            // in case of a restart, we will start from the right position.
            state_store
                .store_replication_origin_state(state.clone().into(), true)
                .await?;
        }
        ReplicationMessage::PrimaryKeepAlive(message) => {
            let end_lsn = PgLsn::from(message.wal_end());
            if end_lsn > state.last_received {
                state.last_received = end_lsn;
            }

            // We store the replication origin state before confirming the Postgres the max LSN we read
            // since in the worst case, we store this in our state and Postgres doesn't know this
            // but on a restart, we will correctly restart from our state and then notify Postgres
            // again when this code path is hit.
            state_store
                .store_replication_origin_state(state.clone().into(), true)
                .await?;

            if state.should_send_status_update() {
                events_stream
                    .send_status_update(state.last_received)
                    .await?;
                state.status_update_sent();
            }
        }
        _ => {}
    }

    Ok(())
}

async fn handle_logical_replication_message<S, D, T>(
    state: &mut ApplyLoopState,
    message: LogicalReplicationMessage,
    state_store: &S,
    _destination: &D,
    hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    match message {
        LogicalReplicationMessage::Begin(message) => {
            handle_begin_message(state, message, state_store, hook).await
        }
        LogicalReplicationMessage::Commit(message) => {
            handle_commit_message(state, message, state_store, hook).await
        }
        LogicalReplicationMessage::Origin(message) => {
            handle_origin_message(state, message, state_store, hook).await
        }
        LogicalReplicationMessage::Relation(message) => {
            handle_relation_message(state, message, state_store, hook).await
        }
        LogicalReplicationMessage::Type(message) => {
            handle_type_message(state, message, state_store, hook).await
        }
        LogicalReplicationMessage::Insert(message) => {
            handle_insert_message(state, message, state_store, hook).await
        }
        LogicalReplicationMessage::Update(message) => {
            handle_update_message(state, message, state_store, hook).await
        }
        LogicalReplicationMessage::Delete(message) => {
            handle_delete_message(state, message, state_store, hook).await
        }
        LogicalReplicationMessage::Truncate(message) => {
            handle_truncate_message(state, message, state_store, hook).await
        }
        _ => Ok(()),
    }
}

async fn handle_begin_message<S, T>(
    state: &mut ApplyLoopState,
    message: postgres_replication::protocol::BeginBody,
    _state_store: &S,
    _hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    // We track the final lsn of this transaction, which should be equal to the `commit_lsn` of the
    // `Commit` message.
    let final_lsn = PgLsn::from(message.final_lsn());
    state.remote_final_lsn = final_lsn;

    Ok(())
}

async fn handle_commit_message<S, T>(
    _state: &mut ApplyLoopState,
    message: postgres_replication::protocol::CommitBody,
    _state_store: &S,
    hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    // We process syncing tables since we just arrived at the end of a transaction, and we want to
    // synchronize all the workers.
    //
    // The `end_lsn` here refers to the LSN of the record right after the commit record.
    let end_lsn = PgLsn::from(message.end_lsn());
    hook.process_syncing_tables(end_lsn).await?;

    Ok(())
}

async fn handle_origin_message<S, T>(
    _state: &mut ApplyLoopState,
    _message: postgres_replication::protocol::OriginBody,
    _state_store: &S,
    _hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    Ok(())
}

async fn handle_relation_message<S, T>(
    _state: &mut ApplyLoopState,
    _message: postgres_replication::protocol::RelationBody,
    _state_store: &S,
    _hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    Ok(())
}

async fn handle_type_message<S, T>(
    _state: &mut ApplyLoopState,
    _message: postgres_replication::protocol::TypeBody,
    _state_store: &S,
    _hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    Ok(())
}

async fn handle_insert_message<S, T>(
    _state: &mut ApplyLoopState,
    _message: postgres_replication::protocol::InsertBody,
    _state_store: &S,
    _hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    Ok(())
}

async fn handle_update_message<S, T>(
    _state: &mut ApplyLoopState,
    _message: postgres_replication::protocol::UpdateBody,
    _state_store: &S,
    _hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    Ok(())
}

async fn handle_delete_message<S, T>(
    _state: &mut ApplyLoopState,
    _message: postgres_replication::protocol::DeleteBody,
    _state_store: &S,
    _hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    Ok(())
}

async fn handle_truncate_message<S, T>(
    _state: &mut ApplyLoopState,
    _message: postgres_replication::protocol::TruncateBody,
    _state_store: &S,
    _hook: &T,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    Ok(())
}

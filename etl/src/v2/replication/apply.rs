use futures::StreamExt;
use postgres::schema::Oid;
use postgres_replication::protocol;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::pin;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;
use tracing::error;

use crate::v2::concurrency::stream::BatchStream;
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::conversions::event::{
    BeginEvent, CommitEvent, Event, EventConversionError, EventConverter, TruncateEvent,
};
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError, SlotUsage};
use crate::v2::replication::stream::{EventsStream, EventsStreamError};
use crate::v2::state::origin::ReplicationOriginState;
use crate::v2::state::store::base::{StateStore, StateStoreError};
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

    #[error(
        "An error occurred while streaming logical replication changes in the apply loop: {0}"
    )]
    LogicalReplicationStreamFailed(#[from] EventsStreamError),

    #[error("Could not generate slot name in the apply loop: {0}")]
    Slot(#[from] SlotError),

    #[error("An error happened in the state store while in the apply loop: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("An error occurred while building an event from a message in the apply loop: {0}")]
    EventConversion(#[from] EventConversionError),

    #[error("An error occurred when interacting with the destination in the apply loop: {0}")]
    Destination(#[from] DestinationError),

    #[error("Incorrect commit LSN {0} in COMMIT message (expected {1})")]
    InvalidCommitLsn(PgLsn, PgLsn),

    #[error("An invalid event {0} was received (expected {1})")]
    InvalidEvent(String, String),
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
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn should_apply_changes(
        &self,
        table_id: Oid,
        remote_final_lsn: PgLsn,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn slot_usage(&self) -> SlotUsage;
}

#[derive(Debug, Clone)]
struct ApplyLoopState {
    /// The highest LSN of the received events.
    ///
    /// This LSN is extracted from the `start_lsn` and `end_lsn` of each incoming event.
    last_received: PgLsn,
    /// The LSN of the commit WAL entry of the transaction that is currently being processed.
    ///
    /// This LSN is set at every `BEGIN` of a new transaction.
    remote_final_lsn: PgLsn,
    /// A boolean indicating whether the loop should be completed.
    should_complete: bool,
}

#[allow(clippy::too_many_arguments)]
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
        last_received: origin_start_lsn,
        remote_final_lsn: PgLsn::from(0),
        should_complete: false,
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
    let logical_replication_stream = BatchStream::wrap(
        logical_replication_stream,
        config.batch_config.clone(),
        shutdown_rx.clone(),
    );
    pin!(logical_replication_stream);

    // We build the event converter, which will convert all the messages from the logical replication
    // protocol to events that are usable by the downstream destination.
    let event_converter = EventConverter::new(identity.id(), state_store.clone());

    loop {
        if state.should_complete {
            return Ok(ApplyLoopResult::ApplyCompleted);
        }

        tokio::select! {
            biased;

            _ = shutdown_rx.changed() => {
                return Ok(ApplyLoopResult::ApplyStopped);
            }
            Some(messages_batch) = logical_replication_stream.next() => {
                let logical_replication_stream = logical_replication_stream.as_mut();
                let events_stream = unsafe { Pin::new_unchecked(logical_replication_stream.get_unchecked_mut().get_inner_mut()) };

                handle_replication_message_batch(&identity, &mut state, events_stream, messages_batch, &event_converter, &state_store, &destination, &hook).await?;
            }
            _ = tokio::time::sleep(SYNCING_FREQUENCY_SECONDS) => {
                // TODO: this is a great place to perform cleanup operations.
               let logical_replication_stream = logical_replication_stream.as_mut();
               let events_stream = unsafe { Pin::new_unchecked(logical_replication_stream.get_unchecked_mut().get_inner_mut()) };

               events_stream.send_status_update(state.last_received).await?;
            }
        }
    }
}

async fn handle_replication_message_batch<S, D, T>(
    identity: &PipelineIdentity,
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
    let batch_size = messages_batch.len();
    for (index, message) in messages_batch.into_iter().enumerate() {
        let message = message?;
        handle_replication_message(
            &identity,
            state,
            stream.as_mut(),
            message,
            event_converter,
            state_store,
            destination,
            hook,
        )
        .await?;

        // If we should complete after processing a message, we want to finish the loop early, even
        // though we assume that when `should_complete` is flipped to true, it means a boundary element
        // has been found, meaning that it should be the last in batch. If that's not the case, we
        // want to emit an error.
        if state.should_complete {
            let remaining_messages = batch_size - index - 1;
            if remaining_messages > 0 {
                error!("There are {} remaining messages in the batch even though the should stop the apply loop", remaining_messages);
            }

            return Ok(());
        }
    }

    Ok(())
}

async fn handle_replication_message<S, D, T>(
    identity: &PipelineIdentity,
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

            handle_logical_replication_message(
                identity,
                state,
                message.into_data(),
                event_converter,
                state_store,
                destination,
                hook,
            )
            .await?;
        }
        ReplicationMessage::PrimaryKeepAlive(message) => {
            let end_lsn = PgLsn::from(message.wal_end());
            if end_lsn > state.last_received {
                state.last_received = end_lsn;
            }

            events_stream
                .send_status_update(state.last_received)
                .await?;
        }
        _ => {}
    }

    Ok(())
}

async fn handle_logical_replication_message<S, D, T>(
    identity: &PipelineIdentity,
    state: &mut ApplyLoopState,
    message: LogicalReplicationMessage,
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
    // We perform the conversion of the message to our own event format which is used downstream
    // by the destination.
    let event = event_converter.convert(&message).await?;

    // For each message, we handle it separately.
    match message {
        LogicalReplicationMessage::Begin(message) => {
            let Event::Begin(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    format!("{:?}", event),
                    "Event::Begin".to_string(),
                ));
            };

            handle_begin_event::<D, T>(state, message, event, destination).await
        }
        LogicalReplicationMessage::Commit(message) => {
            let Event::Commit(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    format!("{:?}", event),
                    "Event::Commit".to_string(),
                ));
            };

            handle_commit_event(
                identity,
                state,
                message,
                event,
                state_store,
                destination,
                hook,
            )
            .await
        }
        LogicalReplicationMessage::Origin(_) => Ok(()),
        LogicalReplicationMessage::Relation(_) => Ok(()),
        LogicalReplicationMessage::Type(_) => Ok(()),
        LogicalReplicationMessage::Insert(message) => {
            let Event::Insert(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    format!("{:?}", event),
                    "Event::Insert".to_string(),
                ));
            };

            handle_simple_dml_event(
                state,
                message.rel_id(),
                Event::Insert(event),
                destination,
                hook,
            )
            .await
        }
        LogicalReplicationMessage::Update(message) => {
            let Event::Update(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    format!("{:?}", event),
                    "Event::Update".to_string(),
                ));
            };
            handle_simple_dml_event(
                state,
                message.rel_id(),
                Event::Update(event),
                destination,
                hook,
            )
            .await
        }
        LogicalReplicationMessage::Delete(message) => {
            let Event::Delete(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    format!("{:?}", event),
                    "Event::Delete".to_string(),
                ));
            };
            handle_simple_dml_event(
                state,
                message.rel_id(),
                Event::Delete(event),
                destination,
                hook,
            )
            .await
        }
        LogicalReplicationMessage::Truncate(message) => {
            let Event::Truncate(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    format!("{:?}", event),
                    "Event::Truncate".to_string(),
                ));
            };

            handle_truncate_event(state, message, event, destination, hook).await
        }
        _ => Ok(()),
    }
}

async fn handle_begin_event<D, T>(
    state: &mut ApplyLoopState,
    message: protocol::BeginBody,
    event: BeginEvent,
    destination: &D,
) -> Result<(), ApplyLoopError>
where
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    // We track the final lsn of this transaction, which should be equal to the `commit_lsn` of the
    // `Commit` message.
    state.remote_final_lsn = message.final_lsn().into();

    // We send the event to the destination unconditionally.
    destination.apply_event(Event::Begin(event)).await?;

    Ok(())
}

async fn handle_commit_event<S, D, T>(
    identity: &PipelineIdentity,
    state: &mut ApplyLoopState,
    message: protocol::CommitBody,
    event: CommitEvent,
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
    // If the commit lsn of the message is different from the remote final lsn, it means that the
    // transaction that was started expect a different commit lsn in the commit message. In this case,
    // we want to bail assuming we are in an inconsistent state.
    let commit_lsn = PgLsn::from(message.commit_lsn());
    if commit_lsn != state.remote_final_lsn {
        return Err(ApplyLoopError::InvalidCommitLsn(
            commit_lsn,
            state.remote_final_lsn,
        ));
    }

    let end_lsn = PgLsn::from(message.end_lsn());

    // We send the event to the destination unconditionally.
    destination.apply_event(Event::Commit(event)).await?;

    // After successfully applying a commit event in the destination, we update our replication origin
    // state to point to the `end_lsn` of the commit message, which points to the next WAL entry that
    // we should continue/start from.
    let replication_origin_state =
        ReplicationOriginState::new(identity.id(), None, PgLsn::from(end_lsn));
    state_store
        .store_replication_origin_state(replication_origin_state, true)
        .await?;

    // We process syncing tables since we just arrived at the end of a transaction, and we want to
    // synchronize all the workers.
    //
    // The `end_lsn` here refers to the LSN of the record right after the commit record.
    let continue_loop = hook
        .process_syncing_tables(message.end_lsn().into())
        .await?;
    if !continue_loop {
        state.should_complete = true;
    }

    Ok(())
}

async fn handle_simple_dml_event<D, T>(
    state: &mut ApplyLoopState,
    table_id: Oid,
    event: Event,
    destination: &D,
    hook: &T,
) -> Result<(), ApplyLoopError>
where
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    if !hook
        .should_apply_changes(table_id, state.remote_final_lsn)
        .await?
    {
        return Ok(());
    }

    destination.apply_event(event).await?;

    Ok(())
}

async fn handle_truncate_event<D, T>(
    state: &mut ApplyLoopState,
    message: protocol::TruncateBody,
    mut event: TruncateEvent,
    destination: &D,
    hook: &T,
) -> Result<(), ApplyLoopError>
where
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let mut rel_ids = Vec::with_capacity(message.rel_ids().len());
    for &table_id in message.rel_ids().iter() {
        if hook
            .should_apply_changes(table_id, state.remote_final_lsn)
            .await?
        {
            rel_ids.push(table_id);
        }
    }
    event.rel_ids = rel_ids;

    destination.apply_event(Event::Truncate(event)).await?;

    Ok(())
}

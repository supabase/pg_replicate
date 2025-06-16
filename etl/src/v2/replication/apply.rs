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
use tracing::error;

use crate::v2::concurrency::stream::BatchStream;
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::conversions::event::{
    convert_message_to_event, Event, EventConversionError, EventType,
};
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError, SlotUsage};
use crate::v2::replication::stream::{EventsStream, EventsStreamError};
use crate::v2::schema::cache::SchemaCache;
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

    #[error("A transaction should have started for the action ({0}) to be performed")]
    InvalidTransaction(String),

    #[error("Incorrect commit LSN {0} in COMMIT message (expected {1})")]
    InvalidCommitLsn(PgLsn, PgLsn),

    #[error("An invalid event {0} was received (expected {1})")]
    InvalidEvent(EventType, EventType),
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

#[derive(Debug)]
struct StatusUpdate {
    write_lsn: PgLsn,
    flush_lsn: PgLsn,
    apply_lsn: PgLsn,
}

impl StatusUpdate {
    fn update_write_lsn(&mut self, new_write_lsn: PgLsn) {
        if new_write_lsn <= self.write_lsn {
            return;
        }

        self.write_lsn = new_write_lsn;
    }

    fn update_flush_lsn(&mut self, flush_lsn: PgLsn) {
        if flush_lsn <= self.flush_lsn {
            return;
        }

        self.flush_lsn = flush_lsn;
    }

    fn update_apply_lsn(&mut self, apply_lsn: PgLsn) {
        if apply_lsn <= self.apply_lsn {
            return;
        }

        self.apply_lsn = apply_lsn;
    }
}

#[derive(Debug)]
struct ApplyLoopState {
    /// The highest LSN received from the `end_lsn` field of replication messages.
    ///
    /// This LSN is set with the `end_lsn` of each incoming message, and it's used for:
    /// - Storing how far we have come when it comes to processing events (stored in the replication
    /// origin state)
    /// - Notifying Postgres about how far we have flushed events in our destination (so that Postgres
    /// can perform WAL pruning)
    last_end_lsn: Option<PgLsn>,
    /// The LSN of the commit WAL entry of the transaction that is currently being processed.
    ///
    /// This LSN is set at every `BEGIN` of a new transaction, and it's used to know the `commit_lsn`
    /// of the transaction which is currently being processed.
    remote_final_lsn: Option<PgLsn>,
    /// The LSNs of the status update that we want to send to Postgres.
    next_status_update: StatusUpdate,
    /// A boolean indicating whether the loop should be completed.
    should_complete: bool,
}

impl ApplyLoopState {
    fn update_last_end_lsn(&mut self, new_last_end_lsn: PgLsn) {
        let Some(last_end_lsn) = &mut self.last_end_lsn else {
            self.last_end_lsn = Some(new_last_end_lsn);
            return;
        };

        if new_last_end_lsn <= *last_end_lsn {
            return;
        }

        *last_end_lsn = new_last_end_lsn;
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn start_apply_loop<S, D, T>(
    identity: PipelineIdentity,
    origin_start_lsn: PgLsn,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    schema_cache: SchemaCache,
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
    // The last status update is defaulted from the origin start lsn since at this point we haven't
    // processed anything.
    let last_status_update = StatusUpdate {
        write_lsn: origin_start_lsn,
        flush_lsn: origin_start_lsn,
        apply_lsn: origin_start_lsn,
    };

    // We initialize the shared state that is used throughout the loop to track progress.
    let mut state = ApplyLoopState {
        last_end_lsn: None,
        remote_final_lsn: None,
        next_status_update: last_status_update,
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

                handle_replication_message_batch(&identity, &mut state, events_stream, messages_batch, &schema_cache, &state_store, &destination, &hook).await?;
            }
            _ = tokio::time::sleep(SYNCING_FREQUENCY_SECONDS) => {
                // TODO: this is a great place to perform cleanup operations.
               let logical_replication_stream = logical_replication_stream.as_mut();
               let events_stream = unsafe { Pin::new_unchecked(logical_replication_stream.get_unchecked_mut().get_inner_mut()) };

               events_stream.send_status_update(state.next_status_update.write_lsn, state.next_status_update.flush_lsn, state.next_status_update.apply_lsn).await?;
            }
        }
    }
}

async fn handle_replication_message_batch<S, D, T>(
    identity: &PipelineIdentity,
    state: &mut ApplyLoopState,
    mut stream: Pin<&mut EventsStream>,
    messages_batch: Vec<Result<ReplicationMessage<LogicalReplicationMessage>, EventsStreamError>>,
    schema_cache: &SchemaCache,
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
    let mut events_batch = Vec::with_capacity(batch_size);

    for message in messages_batch {
        let event =
            handle_replication_message(state, stream.as_mut(), message?, schema_cache, hook)
                .await?;

        if let Some(event) = event {
            events_batch.push(event);
        }

        // If we should complete after processing a message, we want to finish the loop early, to
        // avoid processing additional elements which might lead to duplication.
        //
        // This can happen for example when a table sync worker has caught up with the apply worker
        // but its batch contained more elements after the caught up element, in that case we don't
        // want to process those elements, otherwise if we do, the apply worker will process them too
        // causing duplicate data.
        if state.should_complete {
            return Ok(());
        }
    }

    // We apply the batch of events to the destination.
    destination.apply_events(events_batch).await?;

    // At this point, the `last_end_lsn` will contain the LSN of the next entry that we want to fetch.
    if let Some(last_end_lsn) = state.last_end_lsn.take() {
        // We store the LSN in the replication origin state in order to be able to restart from that
        // LSN.
        let replication_origin_state =
            ReplicationOriginState::new(identity.id(), None, last_end_lsn);
        state_store
            .store_replication_origin_state(replication_origin_state, true)
            .await?;

        // We also prepare the next status update for Postgres, where we will confirm that we flushed
        // data up to this LSN to allow for WAL pruning on the database side.
        // TODO: check if we want to send `apply_lsn` as a different value.
        state.next_status_update.update_flush_lsn(last_end_lsn);
        state.next_status_update.update_apply_lsn(last_end_lsn);
    }

    Ok(())
}

async fn handle_replication_message<T>(
    state: &mut ApplyLoopState,
    events_stream: Pin<&mut EventsStream>,
    message: ReplicationMessage<LogicalReplicationMessage>,
    schema_cache: &SchemaCache,
    hook: &T,
) -> Result<Option<Event>, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    match message {
        ReplicationMessage::XLogData(message) => {
            let start_lsn = PgLsn::from(message.wal_start());
            state.next_status_update.update_write_lsn(start_lsn);

            let end_lsn = PgLsn::from(message.wal_end());
            state.next_status_update.update_write_lsn(end_lsn);
            state.update_last_end_lsn(end_lsn);

            handle_logical_replication_message(state, message.into_data(), schema_cache, hook).await
        }
        ReplicationMessage::PrimaryKeepAlive(message) => {
            let end_lsn = PgLsn::from(message.wal_end());
            state.next_status_update.update_write_lsn(end_lsn);
            state.update_last_end_lsn(end_lsn);

            events_stream
                .send_status_update(
                    state.next_status_update.write_lsn,
                    state.next_status_update.flush_lsn,
                    state.next_status_update.apply_lsn,
                )
                .await?;

            Ok(None)
        }
        _ => Ok(None),
    }
}

async fn handle_logical_replication_message<T>(
    state: &mut ApplyLoopState,
    message: LogicalReplicationMessage,
    schema_cache: &SchemaCache,
    hook: &T,
) -> Result<Option<Event>, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    // We perform the conversion of the message to our own event format which is used downstream
    // by the destination.
    let event = convert_message_to_event(&schema_cache, &message).await?;

    // For each message, we handle it separately.
    match message {
        LogicalReplicationMessage::Begin(message) => {
            let Event::Begin(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(event.into(), EventType::Begin));
            };

            // We track the final lsn of this transaction, which should be equal to the `commit_lsn` of the
            // `Commit` message.
            let final_lsn = PgLsn::from(message.final_lsn());
            state.remote_final_lsn = Some(final_lsn);

            Ok(Some(Event::Begin(event)))
        }
        LogicalReplicationMessage::Commit(message) => {
            let Event::Commit(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    event.into(),
                    EventType::Commit,
                ));
            };

            // We take the LSN that belongs to the current transaction, however, if there is no
            // LSN, it means that a `BEGIN` message was not received before this `COMMIT` which means
            // we are in an inconsistent state.
            let Some(remote_final_lsn) = state.remote_final_lsn.take() else {
                return Err(ApplyLoopError::InvalidTransaction(
                    "handle_commit_message".to_owned(),
                ));
            };

            // If the commit lsn of the message is different from the remote final lsn, it means that the
            // transaction that was started expect a different commit lsn in the commit message. In this case,
            // we want to bail assuming we are in an inconsistent state.
            let commit_lsn = PgLsn::from(message.commit_lsn());
            if commit_lsn != remote_final_lsn {
                return Err(ApplyLoopError::InvalidCommitLsn(
                    commit_lsn,
                    remote_final_lsn,
                ));
            }

            // We process syncing tables since we just arrived at the end of a transaction, and we want to
            // synchronize all the workers.
            //
            // The `end_lsn` here refers to the LSN of the record right after the commit record.
            let end_lsn = PgLsn::from(message.end_lsn());
            let continue_loop = hook.process_syncing_tables(end_lsn).await?;
            if !continue_loop {
                state.should_complete = true;
            }

            Ok(Some(Event::Commit(event)))
        }
        LogicalReplicationMessage::Relation(message) => {
            let Event::Relation(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    event.into(),
                    EventType::Relation,
                ));
            };

            // We compare the table schema from the relation message.
            // TODO:

            Ok(None)
        }
        LogicalReplicationMessage::Insert(message) => {
            let Event::Insert(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    event.into(),
                    EventType::Insert,
                ));
            };

            let Some(remote_final_lsn) = state.remote_final_lsn else {
                return Err(ApplyLoopError::InvalidTransaction(
                    "handle_insert_message".to_owned(),
                ));
            };

            if !hook
                .should_apply_changes(message.rel_id(), remote_final_lsn)
                .await?
            {
                return Ok(None);
            }

            Ok(Some(Event::Insert(event)))
        }
        LogicalReplicationMessage::Update(message) => {
            let Event::Update(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    event.into(),
                    EventType::Update,
                ));
            };

            let Some(remote_final_lsn) = state.remote_final_lsn else {
                return Err(ApplyLoopError::InvalidTransaction(
                    "handle_update_message".to_owned(),
                ));
            };

            if !hook
                .should_apply_changes(message.rel_id(), remote_final_lsn)
                .await?
            {
                return Ok(None);
            }

            Ok(Some(Event::Update(event)))
        }
        LogicalReplicationMessage::Delete(message) => {
            let Event::Delete(event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    event.into(),
                    EventType::Delete,
                ));
            };

            let Some(remote_final_lsn) = state.remote_final_lsn else {
                return Err(ApplyLoopError::InvalidTransaction(
                    "handle_delete_message".to_owned(),
                ));
            };

            if !hook
                .should_apply_changes(message.rel_id(), remote_final_lsn)
                .await?
            {
                return Ok(None);
            }

            Ok(Some(Event::Delete(event)))
        }
        LogicalReplicationMessage::Truncate(message) => {
            let Event::Truncate(mut event) = event else {
                return Err(ApplyLoopError::InvalidEvent(
                    event.into(),
                    EventType::Truncate,
                ));
            };

            let Some(remote_final_lsn) = state.remote_final_lsn else {
                return Err(ApplyLoopError::InvalidTransaction(
                    "handle_truncate_message".to_owned(),
                ));
            };

            let mut rel_ids = Vec::with_capacity(message.rel_ids().len());
            for &table_id in message.rel_ids().iter() {
                if hook
                    .should_apply_changes(table_id, remote_final_lsn)
                    .await?
                {
                    rel_ids.push(table_id);
                }
            }
            event.rel_ids = rel_ids;

            Ok(Some(Event::Truncate(event)))
        }
        LogicalReplicationMessage::Origin(_) => Ok(None),
        LogicalReplicationMessage::Type(_) => Ok(None),
        _ => Ok(None),
    }
}

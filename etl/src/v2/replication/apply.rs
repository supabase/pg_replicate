use postgres::schema::Oid;
use std::future::Future;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

use crate::v2::destination::base::Destination;
use crate::v2::replication::client::PgReplicationClient;
use crate::v2::state::store::base::StateStore;
use crate::v2::workers::apply::ApplyWorkerHookError;
use crate::v2::workers::table_sync::TableSyncWorkerHookError;

// TODO: figure out how to break the cycle and remove `Box`.
#[derive(Debug, Error)]
pub enum ApplyLoopError {
    #[error("Apply worker hook operation failed: {0}")]
    ApplyWorkerHook(Box<ApplyWorkerHookError>),

    #[error("Table sync worker hook operation failed: {0}")]
    TableSyncWorkerHook(Box<TableSyncWorkerHookError>),
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
}

pub async fn start_apply_loop<S, D, T>(
    hook: T,
    _replication_client: PgReplicationClient,
    _last_received: PgLsn,
    _state_store: S,
    _destination: D,
) -> Result<ApplyLoopResult, ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    // Create a select between:
    //  - Shutdown signal -> when called we stop the apply operation.
    //  - Logical replication stream socket -> when an event is received, we handle it in a special
    //      processing component.
    //  - Else we do the table syncing if we are not at a boundary -> we perform table syncing to make
    //     sure progress is happening in table sync workers.

    // TODO: for now we are looping indefinitely, implement the actual apply logic.
    loop {
        hook.process_syncing_tables(PgLsn::from(0)).await?;
    }
}

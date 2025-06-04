use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::Destination;
use crate::v2::replication::client::PgReplicationClient;
use crate::v2::state::store::base::StateStore;
use crate::v2::workers::apply::ApplyWorkerHookError;
use crate::v2::workers::table_sync::TableSyncWorkerHookError;
use postgres::schema::Oid;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use tokio::task;
use tokio_postgres::types::PgLsn;

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
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    origin_start_lsn: PgLsn,
    state_store: S,
    destination: D,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<ApplyLoopResult, ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                return Ok(ApplyLoopResult::ApplyCompleted);
            }
            result = inner_apply_loop(&hook, config.clone(), replication_client.clone(), origin_start_lsn, state_store.clone(), destination.clone()) => {
                result?;
            }
        }
    }
}

async fn inner_apply_loop<S, D, T>(
    hook: &T,
    _config: Arc<PipelineConfig>,
    _replication_client: PgReplicationClient,
    _origin_start_lsn: PgLsn,
    _state_store: S,
    _destination: D,
) -> Result<ApplyLoopResult, ApplyLoopError>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    hook.process_syncing_tables(PgLsn::from(0)).await?;
    task::yield_now().await;

    Ok(ApplyLoopResult::ApplyCompleted)
}

use crate::v2::workers::apply::ApplyWorkerError;
use crate::v2::workers::table_sync::TableSyncWorkerError;
use std::future::Future;
use thiserror::Error;

/// Errors that can occur during worker execution.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// The worker task failed to join, typically due to a panic or cancellation.
    #[error("The worker experienced an uncaught error: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// The worker encountered a caught error during execution.
    #[error("The worker experienced a caught error: {0}")]
    Caught(String),

    #[error("The apply worker has experienced an error: {0}")]
    ApplyWorker(#[from] ApplyWorkerError),

    #[error("The table sync worker has experienced an error: {0}")]
    TableSyncWorker(#[from] TableSyncWorkerError),
}

/// A trait for types that can be started as workers.
///
/// The generic parameter `H` represents the handle type that will be returned when the worker starts,
/// and `S` represents the state type that can be accessed through the handle.
pub trait Worker<H, S>
where
    H: WorkerHandle<S>,
{
    /// Starts the worker and returns a future that resolves to an optional handle.
    ///
    /// The handle can be used to monitor and control the worker's execution.
    fn start(self) -> impl Future<Output = Result<H, WorkerError>> + Send;
}

/// A handle to a running worker that provides access to its state and completion status.
///
/// The generic parameter `S` represents the type of state that can be accessed through this handle.
pub trait WorkerHandle<S> {
    /// Returns the current state of the worker.
    ///
    /// Note that the state of the worker is expected to NOT be tied with its lifetime, so if you
    /// hold a reference to the state, it won't say anything about the worker's status, however it
    /// could be used to encode it's state but this is based on the semantics of the concrete type
    /// and not this abstraction.
    fn state(&self) -> S;

    /// Returns a future that resolves when the worker completes.
    ///
    /// The future resolves to a [`Result`] indicating whether the worker completed successfully
    /// or encountered an error.
    fn wait(self) -> impl Future<Output = Result<(), WorkerError>> + Send;
}

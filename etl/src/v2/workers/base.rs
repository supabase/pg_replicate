use futures::future::{BoxFuture, CatchUnwind, FutureExt};
use pin_project_lite::pin_project;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::sync::RwLock;

/// Errors that can occur during worker execution.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// The worker task failed to join, typically due to a panic or cancellation.
    #[error("The worker experienced an uncaught error: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// The worker encountered a caught error during execution.
    #[error("The worker experienced a caught error: {0}")]
    Caught(String),
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
    fn start(self) -> impl Future<Output = Option<H>> + Send;
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

/// A callback interface for receiving notifications about future completion.
///
/// The generic parameter `I` represents the identifier type used to track futures.
pub trait ReactiveFutureCallback<I> {
    /// Called when a future completes successfully.
    fn on_complete(&mut self, id: I);

    /// Called when a future encounters an error.
    fn on_error(&mut self, id: I, error: String);
}

/// Represents the internal state of a [`ReactiveFuture`].
enum ReactiveFutureState {
    /// The inner future is currently being polled.
    PollInnerFuture,
    /// The inner future has completed, and we have to notify the callback.
    InvokeCallback(Option<String>),
    /// The future has finished all processing.
    Finished,
}

pin_project! {
    /// A future that notifies a callback when it completes or encounters an error.
    ///
    /// The generic parameters are:
    /// - `Fut`: The underlying future type
    /// - `I`: The identifier type for tracking futures
    /// - `C`: The callback type that implements [`ReactiveFutureCallback`]
    pub struct ReactiveFuture<Fut, I, C> {
        #[pin]
        future: CatchUnwind<AssertUnwindSafe<Fut>>,
        #[pin]
        callback_fut: Option<BoxFuture<'static, ()>>,
        state: ReactiveFutureState,
        id: I,
        callback: Arc<RwLock<C>>,
    }
}

impl<Fut, I, C> ReactiveFuture<Fut, I, C>
where
    Fut: Future<Output = ()>,
{
    /// Creates a new reactive future that will notify the callback when it completes.
    ///
    /// The callback will be notified of either success or failure, and will receive the provided
    /// identifier to track which future completed.
    pub fn new(future: Fut, id: I, callback: Arc<RwLock<C>>) -> Self {
        Self {
            future: AssertUnwindSafe(future).catch_unwind(),
            callback_fut: None,
            state: ReactiveFutureState::PollInnerFuture,
            id,
            callback,
        }
    }
}

impl<Fut, I, C> Future for ReactiveFuture<Fut, I, C>
where
    Fut: Future<Output = ()>,
    I: Clone + Send + 'static,
    C: ReactiveFutureCallback<I> + Send + Sync + 'static,
{
    type Output = ();

    /// Polls the future, handling both successful completion and errors.
    ///
    /// This implementation ensures that the callback is notified appropriately when the future
    /// completes, whether successfully or with an error. It also handles panics by converting
    /// them into error notifications.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state {
                ReactiveFutureState::PollInnerFuture => match this.future.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        *this.state = ReactiveFutureState::InvokeCallback(None);
                    }
                    Poll::Ready(Err(err)) => {
                        // We want to handle the most common panic types.
                        let err = if let Some(s) = err.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = err.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "Unknown panic error".to_string()
                        };

                        *this.state = ReactiveFutureState::InvokeCallback(Some(err));
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                ReactiveFutureState::InvokeCallback(error) => {
                    if this.callback_fut.is_none() {
                        let id = this.id.clone();
                        let callback = this.callback.clone();
                        let error = error.clone();

                        let callback_fut = async move {
                            let mut guard = callback.write().await;
                            match error {
                                Some(err) => guard.on_error(id, err),
                                None => guard.on_complete(id),
                            };
                        }
                        .boxed();

                        *this.callback_fut = Some(callback_fut);
                    }

                    if let Some(callback_fut) = this.callback_fut.as_mut().as_pin_mut() {
                        match callback_fut.poll(cx) {
                            Poll::Ready(()) => {
                                *this.state = ReactiveFutureState::Finished;
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    }
                }
                ReactiveFutureState::Finished => {
                    return Poll::Ready(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::v2::workers::base::{ReactiveFuture, ReactiveFutureCallback};
    use std::future::Future;
    use std::panic;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::sync::RwLock;

    struct ImmediateFuture;

    impl Future for ImmediateFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(())
        }
    }

    struct PanicFuture {
        panic_any: bool,
    }

    impl Future for PanicFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.panic_any {
                panic::panic_any(150);
            } else {
                panic!("The future had an error");
            }
        }
    }

    struct PendingFuture {
        polled: bool,
    }

    impl PendingFuture {
        pub fn new() -> Self {
            Self { polled: false }
        }
    }

    impl Future for PendingFuture {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.polled {
                Poll::Ready(())
            } else {
                self.polled = true;
                Poll::Pending
            }
        }
    }

    #[derive(Default)]
    struct MockCallback {
        complete_called: bool,
        error_called: bool,
        error_message: Option<String>,
    }

    impl ReactiveFutureCallback<i32> for MockCallback {
        fn on_complete(&mut self, _id: i32) {
            self.complete_called = true;
        }

        fn on_error(&mut self, _id: i32, error: String) {
            self.error_called = true;
            self.error_message = Some(error);
        }
    }

    #[tokio::test]
    async fn test_successful_completion() {
        let callback = Arc::new(RwLock::new(MockCallback::default()));
        let table_id = 1;

        let future = ReactiveFuture::new(ImmediateFuture, table_id, callback.clone());
        future.await;

        let guard = callback.read().await;
        assert!(guard.complete_called);
        assert!(!guard.error_called);
    }

    #[tokio::test]
    async fn test_panic_handling() {
        let callback = Arc::new(RwLock::new(MockCallback::default()));
        let table_id = 1;

        let future =
            ReactiveFuture::new(PanicFuture { panic_any: false }, table_id, callback.clone());
        future.await;

        {
            let guard = callback.read().await;
            assert!(!guard.complete_called);
            assert!(guard.error_called);
            assert_eq!(
                guard.error_message,
                Some("The future had an error".to_string())
            );
        }

        let future =
            ReactiveFuture::new(PanicFuture { panic_any: true }, table_id, callback.clone());
        future.await;

        {
            let guard = callback.read().await;
            assert!(!guard.complete_called);
            assert!(guard.error_called);
            assert_eq!(guard.error_message, Some("Unknown panic error".to_string()));
        }
    }

    #[tokio::test]
    async fn test_pending_state() {
        let callback = Arc::new(RwLock::new(MockCallback::default()));
        let table_id = 1;

        let pending_future = PendingFuture::new();
        let future = ReactiveFuture::new(pending_future, table_id, callback.clone());

        // First poll should return pending
        let mut pinned_future = Box::pin(future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(
            pinned_future.as_mut().poll(&mut cx),
            Poll::Pending
        ));

        // The future will be ready on the second poll
        assert!(matches!(
            pinned_future.as_mut().poll(&mut cx),
            Poll::Ready(())
        ));

        // Verify the callback was notified of success
        let guard = callback.read().await;
        assert!(guard.complete_called);
        assert!(!guard.error_called);
    }
}

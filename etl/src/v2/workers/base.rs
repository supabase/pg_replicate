use futures::future::{BoxFuture, CatchUnwind, FutureExt};
use pin_project_lite::pin_project;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("The worker experienced an uncaught error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("The worker experienced a caught error: {0}")]
    Caught(String),
}

pub trait Worker<H, S>
where
    H: WorkerHandle<S>,
{
    fn start(self) -> impl Future<Output = Option<H>> + Send;
}

pub trait WorkerHandle<S> {
    fn state(&self) -> S;

    fn wait(self) -> impl Future<Output = Result<(), WorkerError>> + Send;
}

pub trait SafeFutureCallback<I> {
    fn on_complete(&mut self, id: I);

    fn on_error(&mut self, id: I, error: String);
}

enum State {
    PollMainFuture,
    NotifyingPool(Option<String>),
    Finished,
}

pin_project! {
    pub struct SafeFuture<Fut, I, C> {
        #[pin]
        future: CatchUnwind<AssertUnwindSafe<Fut>>,
        #[pin]
        callback_fut: Option<BoxFuture<'static, ()>>,
        state: State,
        id: I,
        callback: Arc<RwLock<C>>,
    }
}

impl<Fut, I, C> SafeFuture<Fut, I, C>
where
    Fut: Future<Output = ()>,
{
    pub fn new(future: Fut, id: I, callback: Arc<RwLock<C>>) -> Self {
        Self {
            future: AssertUnwindSafe(future).catch_unwind(),
            callback_fut: None,
            state: State::PollMainFuture,
            id,
            callback,
        }
    }
}

impl<Fut, I, C> Future for SafeFuture<Fut, I, C>
where
    Fut: Future<Output = ()>,
    I: Clone + Send + 'static,
    C: SafeFutureCallback<I> + Send + Sync + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state {
                State::PollMainFuture => match this.future.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        *this.state = State::NotifyingPool(None);
                    }
                    Poll::Ready(Err(err)) => {
                        let err = if let Some(s) = err.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = err.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "Unknown panic error".to_string()
                        };

                        *this.state = State::NotifyingPool(Some(err));
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                State::NotifyingPool(error) => {
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
                                *this.state = State::Finished;
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    }
                }
                State::Finished => {
                    return Poll::Ready(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::v2::workers::base::{SafeFuture, SafeFutureCallback};
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
        success_called: bool,
        error_called: bool,
        error_message: Option<String>,
    }

    impl SafeFutureCallback<i32> for MockCallback {
        fn on_complete(&mut self, _id: i32) {
            self.success_called = true;
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

        let future = SafeFuture::new(ImmediateFuture, table_id, callback.clone());
        future.await;

        let guard = callback.read().await;
        assert!(guard.success_called);
        assert!(!guard.error_called);
    }

    #[tokio::test]
    async fn test_panic_handling() {
        let callback = Arc::new(RwLock::new(MockCallback::default()));
        let table_id = 1;

        let future = SafeFuture::new(PanicFuture { panic_any: false }, table_id, callback.clone());
        future.await;

        {
            let guard = callback.read().await;
            assert!(!guard.success_called);
            assert!(guard.error_called);
            assert_eq!(
                guard.error_message,
                Some("The future had an error".to_string())
            );
        }

        let future = SafeFuture::new(PanicFuture { panic_any: true }, table_id, callback.clone());
        future.await;

        {
            let guard = callback.read().await;
            assert!(!guard.success_called);
            assert!(guard.error_called);
            assert_eq!(guard.error_message, Some("Unknown panic error".to_string()));
        }
    }

    #[tokio::test]
    async fn test_pending_state() {
        let callback = Arc::new(RwLock::new(MockCallback::default()));
        let table_id = 1;

        let pending_future = PendingFuture::new();
        let future = SafeFuture::new(pending_future, table_id, callback.clone());

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
        assert!(guard.success_called);
        assert!(!guard.error_called);
    }
}

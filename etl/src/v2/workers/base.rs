use futures::future::{CatchUnwind, FutureExt};
use pin_project_lite::pin_project;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};

pub trait Worker<H, S>
where
    H: WorkerHandle<S>,
{
    fn start(self) -> impl Future<Output = Option<H>> + Send;
}

pub trait WorkerHandle<S> {
    fn state(&self) -> S;

    fn wait(self) -> impl Future<Output = ()> + Send;
}

pin_project! {
    pub struct CatchFuture<Fut, ClbFut> {
        #[pin]
        future: CatchUnwind<AssertUnwindSafe<Fut>>,
        on_err: Box<dyn FnMut() -> ClbFut + Send>
    }
}

impl<Fut, ClbFut> CatchFuture<Fut, ClbFut>
where
    Fut: Future<Output = ()>,
    ClbFut: Future<Output = ()>,
{
    pub fn new(future: Fut, on_err: impl FnMut() -> ClbFut + Send + 'static) -> Self {
        Self {
            future: AssertUnwindSafe(future).catch_unwind(),
            on_err: Box::new(on_err),
        }
    }
}

impl<Fut, ClbFut> Future for CatchFuture<Fut, ClbFut>
where
    Fut: Future<Output = ()>,
    ClbFut: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        match this.future.as_mut().poll(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(()),
            // TODO: propagate error in some way.
            Poll::Ready(Err(err)) => {
                let on_err_fut = pin!(this.on_err.as_mut()());
                match on_err_fut.poll(cx) {
                    Poll::Ready(_) => Poll::Ready(()),
                    Poll::Pending => Poll::Pending,
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

use std::any::Any;
use futures::future::{CatchUnwind, FutureExt};
use pin_project_lite::pin_project;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::{Pin};
use std::task::{Context, Poll};
use thiserror::Error;
use postgres::schema::Oid;

use crate::v2::workers::pool::{TableSyncWorkerFinish, TableSyncWorkerPool};

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("The worker had an error while waiting")]
    Join(#[from] tokio::task::JoinError),
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

enum State {
    PollMainFuture,
    PollFinishedWorker(Option<Box<dyn Any + Send>>),
}

pin_project! {
    pub struct TableSyncPoolFuture<Fut> {
        #[pin]
        future: CatchUnwind<AssertUnwindSafe<Fut>>,
        state: State,
        table_id: Oid,
        pool: TableSyncWorkerPool,
    }
}

impl <Fut> TableSyncPoolFuture<Fut>
where Fut: Future<Output = ()> {

    pub fn new(future: Fut, table_id: Oid, pool: TableSyncWorkerPool) -> Self {
        Self {
            future: AssertUnwindSafe(future).catch_unwind(),
            state: State::PollMainFuture,
            table_id,
            pool
        }
    }
}

impl<Fut> Future for TableSyncPoolFuture<Fut>
where Fut: Future<Output = ()> {

    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state {
                State::PollMainFuture => {
                    match this.future.as_mut().poll(cx) {
                        Poll::Ready(Ok(())) => {
                            *this.state = State::PollFinishedWorker(None);
                        },
                        Poll::Ready(Err(err)) => {
                            *this.state = State::PollFinishedWorker(Some(err));
                        },
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                },
                State::PollFinishedWorker(err) => {
                    let mut pool = this.pool.blocking_write();
                    let table_sync_worker_finish = match err.take() {
                        Some(err) => TableSyncWorkerFinish::Error(err),
                        None => TableSyncWorkerFinish::Success,
                    };
                    
                    pool.finished_worker(*this.table_id, table_sync_worker_finish);
                }
            }
        }
    }
}

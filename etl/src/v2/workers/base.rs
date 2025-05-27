use std::future::Future;

pub trait Worker<H, S>
where
    H: WorkerHandle<S>,
{
    fn start(self) -> impl Future<Output = H> + Send;
}

pub trait WorkerHandle<S> {
    fn state(&self) -> S;

    fn wait(&mut self) -> impl Future<Output = ()> + Send;
}

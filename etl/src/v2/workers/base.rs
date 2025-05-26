pub trait Worker<H, S>
where
    H: WorkerHandle<S>,
{
    async fn start(self) -> H;
}

pub trait WorkerHandle<S> {
    fn state(&self) -> S;

    async fn wait(&mut self);
}

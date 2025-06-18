use tokio::sync::watch;

pub type ShutdownTx = watch::Sender<()>;
pub type ShutdownRx = watch::Receiver<()>;

pub enum ShutdownResult<T, I> {
    Ok(T),
    Shutdown(I),
}

impl<T, I> ShutdownResult<T, I> {
    pub fn should_shutdown(&self) -> bool {
        matches!(self, ShutdownResult::Shutdown(_))
    }
}

pub fn create_shutdown_channel() -> (ShutdownTx, ShutdownRx) {
    watch::channel(())
}

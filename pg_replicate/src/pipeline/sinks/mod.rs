use thiserror::Error;

pub mod stdout;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("some error")]
    SomeError,
}

// #[async_trait]
pub trait Sink<TRO: Send + Sync> {
    fn write_table_row(&self, row: TRO) -> Result<(), SinkError>;
}

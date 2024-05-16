use async_trait::async_trait;
use thiserror::Error;

pub mod stdout;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("some error")]
    SomeError,
}

#[async_trait]
pub trait Sink<TRO: Send + Sync, CRO: Send + Sync> {
    async fn write_table_row(&self, row: TRO) -> Result<(), SinkError>;
    async fn write_cdc_event(&self, event: CRO) -> Result<(), SinkError>;
}

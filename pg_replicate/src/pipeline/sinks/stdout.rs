use std::fmt::Display;

use async_trait::async_trait;
use tracing::info;

use super::{Sink, SinkError};

pub struct StdoutSink;

#[async_trait]
impl<TRO: Send + Sync + Display + 'static> Sink<TRO> for StdoutSink {
    async fn write_table_row(&self, row: TRO) -> Result<(), SinkError> {
        info!("{row}");
        Ok(())
    }
}

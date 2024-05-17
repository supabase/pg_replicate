use std::fmt::Display;

use async_trait::async_trait;
use tracing::info;

use crate::conversions::table_row::TableRow;

use super::{Sink, SinkError};

pub struct StdoutSink;

#[async_trait]
impl<CRO: Send + Sync + Display + 'static> Sink<CRO> for StdoutSink {
    async fn write_table_row(&self, row: TableRow) -> Result<(), SinkError> {
        info!("{row:?}");
        Ok(())
    }

    async fn write_cdc_event(&self, event: CRO) -> Result<(), SinkError> {
        info!("{event}");
        Ok(())
    }
}

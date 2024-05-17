use std::collections::HashMap;

use async_trait::async_trait;
use tracing::info;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::TableSchema,
};

use super::{Sink, SinkError};

pub struct StdoutSink;

#[async_trait]
impl Sink for StdoutSink {
    async fn write_table_schemas(
        &self,
        table_schemas: HashMap<u32, TableSchema>,
    ) -> Result<(), SinkError> {
        info!("{table_schemas:?}");
        Ok(())
    }

    async fn write_table_row(&self, row: TableRow) -> Result<(), SinkError> {
        info!("{row:?}");
        Ok(())
    }

    async fn write_cdc_event(&self, event: CdcEvent) -> Result<(), SinkError> {
        info!("{event:?}");
        Ok(())
    }
}

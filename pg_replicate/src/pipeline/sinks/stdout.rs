use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use tracing::info;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::PipelineResumptionState,
    table::{TableId, TableSchema},
};

use super::{Sink, SinkError};

pub struct StdoutSink;

#[async_trait]
impl Sink for StdoutSink {
    async fn get_resumption_state(&self) -> Result<PipelineResumptionState, SinkError> {
        Ok(PipelineResumptionState {
            copied_tables: HashSet::new(),
        })
    }

    async fn write_table_schemas(
        &self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), SinkError> {
        info!("{table_schemas:?}");
        Ok(())
    }

    async fn write_table_row(&self, row: TableRow, _table_id: TableId) -> Result<(), SinkError> {
        info!("{row:?}");
        Ok(())
    }

    async fn write_cdc_event(&self, event: CdcEvent) -> Result<(), SinkError> {
        info!("{event:?}");
        Ok(())
    }
}

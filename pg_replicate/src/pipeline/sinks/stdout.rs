use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use tokio_postgres::types::PgLsn;
use tracing::info;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::PipelineResumptionState,
    table::{TableId, TableSchema},
};

use super::{Sink, SinkError, InfallibleSinkError};

pub struct StdoutSink;

#[async_trait]
impl Sink for StdoutSink {
    type Error = InfallibleSinkError;
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        Ok(PipelineResumptionState {
            copied_tables: HashSet::new(),
            last_lsn: PgLsn::from(0),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        info!("{table_schemas:?}");
        Ok(())
    }

    async fn write_table_row(
        &mut self,
        row: TableRow,
        _table_id: TableId,
    ) -> Result<(), Self::Error> {
        info!("{row:?}");
        Ok(())
    }

    async fn write_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, Self::Error> {
        info!("{event:?}");
        Ok(PgLsn::from(0))
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        info!("table {table_id} copied");
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        info!("table {table_id} truncated");
        Ok(())
    }
}

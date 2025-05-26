use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use postgres::schema::{TableId, TableSchema};
use tokio_postgres::types::PgLsn;
use tracing::info;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::PipelineResumptionState,
};

use super::{BatchDestination, InfallibleDestinationError};

pub struct StdoutDestination;

#[async_trait]
impl BatchDestination for StdoutDestination {
    type Error = InfallibleDestinationError;
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

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        _table_id: TableId,
    ) -> Result<(), Self::Error> {
        for row in rows {
            info!("{row:?}");
        }
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        for event in events {
            info!("{event:?}");
        }
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

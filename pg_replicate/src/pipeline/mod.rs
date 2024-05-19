use std::collections::HashSet;

use futures::StreamExt;
use thiserror::Error;
use tokio::pin;
use tokio_postgres::types::PgLsn;

use crate::table::TableId;

use self::{
    sinks::{Sink, SinkError},
    sources::{Source, SourceError},
};

pub mod sinks;
pub mod sources;

pub enum PipelineAction {
    TableCopiesOnly,
    CdcOnly,
    Both,
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("source error: {0}")]
    SourceError(#[from] SourceError),

    #[error("sink error: {0}")]
    SinkError(#[from] SinkError),
}

pub struct PipelineResumptionState {
    copied_tables: HashSet<TableId>,
}

pub struct DataPipeline<Src: Source, Snk: Sink> {
    source: Src,
    sink: Snk,
    action: PipelineAction,
}

impl<Src: Source, Snk: Sink> DataPipeline<Src, Snk> {
    pub fn new(source: Src, sink: Snk, action: PipelineAction) -> Self {
        DataPipeline {
            source,
            sink,
            action,
        }
    }

    async fn copy_table_schemas(
        &mut self,
        copied_tables: &HashSet<TableId>,
    ) -> Result<(), PipelineError> {
        let table_schemas = self.source.get_table_schemas();
        let mut table_schemas = table_schemas.clone();
        for copied_table in copied_tables {
            table_schemas.remove(copied_table);
        }

        if !table_schemas.is_empty() {
            self.sink.write_table_schemas(table_schemas).await?;
        }

        Ok(())
    }

    async fn copy_tables(&mut self, copied_tables: &HashSet<TableId>) -> Result<(), PipelineError> {
        let table_schemas = self.source.get_table_schemas();

        for table_schema in table_schemas.values() {
            if copied_tables.contains(&table_schema.table_id) {
                continue;
            }
            let table_rows = self
                .source
                .get_table_copy_stream(&table_schema.table_name, &table_schema.column_schemas)
                .await?;

            pin!(table_rows);

            while let Some(row) = table_rows.next().await {
                let row = row.map_err(SourceError::TableCopyStream)?;
                self.sink
                    .write_table_row(row, table_schema.table_id)
                    .await?;
            }

            self.sink.table_copied(table_schema.table_id).await?;
            self.source.commit_transaction().await?;
        }

        Ok(())
    }

    async fn copy_cdc_events(&mut self) -> Result<(), PipelineError> {
        let cdc_events = self.source.get_cdc_stream(PgLsn::from(0)).await?;

        pin!(cdc_events);

        while let Some(cdc_event) = cdc_events.next().await {
            let cdc_event = cdc_event.map_err(SourceError::CdcStream)?;
            self.sink.write_cdc_event(cdc_event).await?;
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), PipelineError> {
        let resumption_state = self.sink.get_resumption_state().await?;
        match self.action {
            PipelineAction::TableCopiesOnly => {
                self.copy_table_schemas(&resumption_state.copied_tables)
                    .await?;
                self.copy_tables(&resumption_state.copied_tables).await?;
            }
            PipelineAction::CdcOnly => {
                self.copy_table_schemas(&resumption_state.copied_tables)
                    .await?;
                self.copy_cdc_events().await?;
            }
            PipelineAction::Both => {
                self.copy_table_schemas(&resumption_state.copied_tables)
                    .await?;
                self.copy_tables(&resumption_state.copied_tables).await?;
                self.copy_cdc_events().await?;
            }
        }

        Ok(())
    }
}

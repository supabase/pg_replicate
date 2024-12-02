use std::collections::HashSet;

use futures::StreamExt;
use tokio::pin;
use tokio_postgres::types::PgLsn;

use crate::{
    conversions::cdc_event::CdcEvent, pipeline::sources::CommonSourceError, table::TableId,
};

use super::{sinks::Sink, sources::Source, PipelineAction, PipelineError};

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

    async fn copy_table_schemas(&mut self) -> Result<(), PipelineError<Src::Error, Snk::Error>> {
        let table_schemas = self.source.get_table_schemas();
        let table_schemas = table_schemas.clone();

        if !table_schemas.is_empty() {
            self.sink
                .write_table_schemas(table_schemas)
                .await
                .map_err(PipelineError::Sink)?;
        }

        Ok(())
    }

    async fn copy_tables(
        &mut self,
        copied_tables: &HashSet<TableId>,
    ) -> Result<(), PipelineError<Src::Error, Snk::Error>> {
        let table_schemas = self.source.get_table_schemas();

        let mut keys: Vec<u32> = table_schemas.keys().copied().collect();
        keys.sort();

        for key in keys {
            let table_schema = table_schemas.get(&key).expect("failed to get table key");
            if copied_tables.contains(&table_schema.table_id) {
                continue;
            }

            self.sink
                .truncate_table(table_schema.table_id)
                .await
                .map_err(PipelineError::Sink)?;

            let table_rows = self
                .source
                .get_table_copy_stream(&table_schema.table_name, &table_schema.column_schemas)
                .await
                .map_err(PipelineError::Source)?;

            pin!(table_rows);

            while let Some(row) = table_rows.next().await {
                let row = row.map_err(CommonSourceError::TableCopyStream)?;
                self.sink
                    .write_table_row(row, table_schema.table_id)
                    .await
                    .map_err(PipelineError::Sink)?;
            }

            self.sink
                .table_copied(table_schema.table_id)
                .await
                .map_err(PipelineError::Sink)?;
        }
        self.source
            .commit_transaction()
            .await
            .map_err(PipelineError::Source)?;

        Ok(())
    }

    async fn copy_cdc_events(
        &mut self,
        last_lsn: PgLsn,
    ) -> Result<(), PipelineError<Src::Error, Snk::Error>> {
        let mut last_lsn: u64 = last_lsn.into();
        last_lsn += 1;
        let cdc_events = self
            .source
            .get_cdc_stream(last_lsn.into())
            .await
            .map_err(PipelineError::Source)?;

        pin!(cdc_events);

        while let Some(cdc_event) = cdc_events.next().await {
            let cdc_event = cdc_event.map_err(CommonSourceError::CdcStream)?;
            let send_status_update = if let CdcEvent::KeepAliveRequested { reply } = cdc_event {
                reply
            } else {
                false
            };
            let last_lsn = self
                .sink
                .write_cdc_event(cdc_event)
                .await
                .map_err(PipelineError::Sink)?;
            if send_status_update {
                cdc_events
                    .as_mut()
                    .send_status_update(last_lsn)
                    .await
                    .map_err(CommonSourceError::StatusUpdate)?;
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), PipelineError<Src::Error, Snk::Error>> {
        let resumption_state = self
            .sink
            .get_resumption_state()
            .await
            .map_err(PipelineError::Sink)?;
        match self.action {
            PipelineAction::TableCopiesOnly => {
                self.copy_table_schemas().await?;
                self.copy_tables(&resumption_state.copied_tables).await?;
            }
            PipelineAction::CdcOnly => {
                self.copy_table_schemas().await?;
                self.copy_cdc_events(resumption_state.last_lsn).await?;
            }
            PipelineAction::Both => {
                self.copy_table_schemas().await?;
                self.copy_tables(&resumption_state.copied_tables).await?;
                self.copy_cdc_events(resumption_state.last_lsn).await?;
            }
        }

        Ok(())
    }
}

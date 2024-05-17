use std::marker::PhantomData;

use futures::StreamExt;
use thiserror::Error;
use tokio::pin;
use tokio_postgres::types::PgLsn;

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

pub struct DataPipeline<'a, Src: Source<'a>, Snk: Sink> {
    source: Src,
    sink: Snk,
    action: PipelineAction,
    phantom_a: PhantomData<&'a Src>,
}

impl<'a, Src: Source<'a>, Snk: Sink> DataPipeline<'a, Src, Snk> {
    pub fn new(source: Src, sink: Snk, action: PipelineAction) -> Self {
        DataPipeline {
            source,
            sink,
            action,
            phantom_a: PhantomData,
        }
    }

    async fn copy_table_schemas(&'a self) -> Result<(), PipelineError> {
        let table_schemas = self.source.get_table_schemas();
        self.sink.write_table_schemas(table_schemas).await?;

        Ok(())
    }

    async fn copy_tables(&'a self) -> Result<(), PipelineError> {
        let table_schemas = self.source.get_table_schemas();

        for table_schema in table_schemas.values() {
            let table_rows = self
                .source
                .get_table_copy_stream(&table_schema.table_name, &table_schema.column_schemas)
                .await?;

            pin!(table_rows);

            while let Some(row) = table_rows.next().await {
                let row = row.map_err(SourceError::TableCopyStream)?;
                self.sink.write_table_row(row).await?;
            }

            self.source.commit_transaction().await?;
        }

        Ok(())
    }

    async fn copy_cdc_events(&'a self) -> Result<(), PipelineError> {
        let cdc_events = self.source.get_cdc_stream(PgLsn::from(0)).await?;

        pin!(cdc_events);

        while let Some(cdc_event) = cdc_events.next().await {
            let cdc_event = cdc_event.map_err(SourceError::CdcStream)?;
            self.sink.write_cdc_event(cdc_event).await?;
        }

        Ok(())
    }

    pub async fn start(&'a self) -> Result<(), PipelineError> {
        match self.action {
            PipelineAction::TableCopiesOnly => {
                self.copy_table_schemas().await?;
                self.copy_tables().await?;
            }
            PipelineAction::CdcOnly => {
                self.copy_table_schemas().await?;
                self.copy_cdc_events().await?;
            }
            PipelineAction::Both => {
                self.copy_table_schemas().await?;
                self.copy_tables().await?;
                self.copy_cdc_events().await?;
            }
        }

        Ok(())
    }
}

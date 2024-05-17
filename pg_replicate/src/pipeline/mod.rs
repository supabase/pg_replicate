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

pub enum PipelinAction {
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
    action: PipelinAction,
    phantom_a: PhantomData<&'a Src>,
}

impl<'a, Src: Source<'a>, Snk: Sink> DataPipeline<'a, Src, Snk> {
    pub fn new(source: Src, sink: Snk, action: PipelinAction) -> Self {
        DataPipeline {
            source,
            sink,
            action,
            phantom_a: PhantomData,
        }
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

    async fn cdc(&'a self) -> Result<(), PipelineError> {
        let cdc_events = self.source.get_cdc_stream(PgLsn::from(0)).await?;

        pin!(cdc_events);

        while let Some(cdc_event) = cdc_events.next().await {
            let cdc_event = cdc_event.map_err(SourceError::CdcStream)?;
            self.sink.write_cdc_event(cdc_event).await?;
            // match cdc_event {
            //     CdcMessage::Begin(msg) => {
            //         info!("Begin: {msg}");
            //     }
            //     CdcMessage::Commit { lsn, body } => {
            //         last_lsn = lsn;
            //         info!("Commit: {body}");
            //     }
            //     CdcMessage::Insert(msg) => {
            //         info!("Insert: {msg}");
            //     }
            //     CdcMessage::Update(msg) => {
            //         info!("Update: {msg}");
            //     }
            //     CdcMessage::Delete(msg) => {
            //         info!("Delete: {msg}");
            //     }
            //     CdcMessage::Relation(msg) => {
            //         info!("Relation: {msg}");
            //     }
            //     CdcMessage::KeepAliveRequested { reply } => {
            //         if reply {
            //             cdc_events.as_mut().send_status_update(last_lsn).await?;
            //         }
            //         info!("got keep alive msg")
            //     }
            // }
        }

        Ok(())
    }

    pub async fn start(&'a self) -> Result<(), PipelineError> {
        match self.action {
            PipelinAction::TableCopiesOnly => {
                self.copy_tables().await?;
            }
            PipelinAction::CdcOnly => {
                self.cdc().await?;
            }
            PipelinAction::Both => {
                self.copy_tables().await?;
                self.cdc().await?;
            }
        }

        Ok(())
    }
}

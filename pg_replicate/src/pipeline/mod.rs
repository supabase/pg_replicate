use std::{fmt::Display, marker::PhantomData};

use futures::StreamExt;
use thiserror::Error;
use tokio::pin;
use tokio_postgres::types::PgLsn;

use crate::conversions::{TryFromReplicationMessage, TryFromTableRow};

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
pub enum PipelineError<TE, RE> {
    #[error("source error: {0}")]
    SourceError(#[from] SourceError<TE, RE>),

    #[error("sink error: {0}")]
    SinkError(#[from] SinkError),
}

pub struct DataPipeline<
    'a,
    'b,
    TE,
    TR: TryFromTableRow<TE> + Send + Sync,
    RE,
    RM: TryFromReplicationMessage<RE> + Send + Sync,
    Src: Source<'a, 'b, TE, TR, RE, RM>,
    Snk: Sink<TR::Output, RM::Output>,
> where
    TR::Output: Send + Sync + Display,
    RM::Output: Send + Sync + Display,
{
    source: Src,
    sink: Snk,
    action: PipelinAction,
    table_row_converter: TR,
    cdc_converter: RM,
    phantom_a: PhantomData<&'a RM>,
    phantom_b: PhantomData<&'b dyn Source<'a, 'b, TE, TR, RE, RM>>,
    phantom_te: PhantomData<TE>,
    phantom_tr: PhantomData<TR>,
    phantom_re: PhantomData<RE>,
    phantom_rm: PhantomData<RM>,
}

impl<
        'a,
        'b,
        TE,
        TR: TryFromTableRow<TE> + Send + Sync,
        RE,
        RM: TryFromReplicationMessage<RE> + Send + Sync,
        Src: Source<'a, 'b, TE, TR, RE, RM>,
        Snk: Sink<TR::Output, RM::Output>,
    > DataPipeline<'a, 'b, TE, TR, RE, RM, Src, Snk>
where
    TR::Output: Send + Sync + Display,
    RM::Output: Send + Sync + Display,
{
    pub fn new(
        source: Src,
        sink: Snk,
        action: PipelinAction,
        table_row_converter: TR,
        cdc_converter: RM,
    ) -> Self {
        DataPipeline {
            source,
            sink,
            action,
            table_row_converter,
            cdc_converter,
            phantom_a: PhantomData,
            phantom_b: PhantomData,
            phantom_te: PhantomData,
            phantom_tr: PhantomData,
            phantom_re: PhantomData,
            phantom_rm: PhantomData,
        }
    }

    async fn copy_tables(&'a self) -> Result<(), PipelineError<TE, RE>> {
        let table_schemas = self.source.get_table_schemas();

        for table_schema in table_schemas.values() {
            let table_rows = self
                .source
                .get_table_copy_stream(
                    &table_schema.table_name,
                    &table_schema.column_schemas,
                    &self.table_row_converter,
                )
                .await?;

            pin!(table_rows);

            while let Some(row) = table_rows.next().await {
                let row = row.map_err(|e| SourceError::TableCopyStream(e))?;
                self.sink.write_table_row(row).await?;
            }

            self.source.commit_transaction().await?;
        }

        Ok(())
    }

    async fn cdc(&'a self) -> Result<(), PipelineError<TE, RE>> {
        let cdc_events = self
            .source
            .get_cdc_stream(PgLsn::from(0), &self.cdc_converter)
            .await?;

        pin!(cdc_events);

        while let Some(cdc_event) = cdc_events.next().await {
            let cdc_event = cdc_event.map_err(|e| SourceError::CdcStream(e))?;
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

    pub async fn start(&'a self) -> Result<(), PipelineError<TE, RE>> {
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

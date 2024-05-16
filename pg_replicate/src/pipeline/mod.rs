use std::marker::PhantomData;

use futures::StreamExt;
use thiserror::Error;
use tokio::pin;

use crate::conversion::{TryFromReplicationMessage, TryFromTableRow};

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
pub enum PipelineError<TE> {
    #[error("source error: {0}")]
    SourceError(#[from] SourceError<TE>),

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
    Snk: Sink<TR::Output>,
> where
    TR::Output: Send + Sync,
{
    source: Src,
    sink: Snk,
    action: PipelinAction,
    table_row_converter: TR,
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
        Snk: Sink<TR::Output>,
    > DataPipeline<'a, 'b, TE, TR, RE, RM, Src, Snk>
where
    TR::Output: Send + Sync,
{
    pub fn new(source: Src, sink: Snk, action: PipelinAction, table_row_converter: TR) -> Self {
        DataPipeline {
            source,
            sink,
            action,
            table_row_converter,
            phantom_a: PhantomData,
            phantom_b: PhantomData,
            phantom_te: PhantomData,
            phantom_tr: PhantomData,
            phantom_re: PhantomData,
            phantom_rm: PhantomData,
        }
    }

    pub async fn start(&'a self) -> Result<(), PipelineError<TE>> {
        match self.action {
            PipelinAction::TableCopiesOnly => {
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
                        self.sink.write_table_row(row)?;
                        // info!("row in json format: {row}");
                    }
                }
            }
            PipelinAction::CdcOnly => todo!(),
            PipelinAction::Both => todo!(),
        }

        Ok(())
    }
}

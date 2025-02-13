use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::Utc;
use thiserror::Error;
use tokio_postgres::types::PgLsn;
use tracing::info;

use crate::{
    clients::nats::{MessageMapper, SubjectMapper, NatsClient},
    conversions::{cdc_event::CdcEvent, table_row::TableRow, Cell},
    pipeline::{
        sinks::{BatchSink, SinkError},
        PipelineResumptionState,
    },
    table::{TableId, TableSchema},
};

#[derive(Debug, Error)]
pub enum NatsSinkError {
    #[error("incorrect commit lsn: {0}(expected: {0})")]
    IncorrectCommitLsn(PgLsn, PgLsn),

    #[error("commit message without begin message")]
    CommitWithoutBegin,

    #[error("nats error: {0}")]
    Nats(#[from] async_nats::Error),

    #[error("missing table schemas")]
    MissingTableSchemas,
}

pub struct NatsBatchSink<M: MessageMapper + Send + Sync, S: SubjectMapper + Send + Sync> {
    client: NatsClient<M, S>,
    committed_lsn: Option<PgLsn>,
    final_lsn: Option<PgLsn>,
    table_schemas: HashMap<TableId, TableSchema>,
}

impl<M: MessageMapper + Send + Sync, S: SubjectMapper + Send + Sync> NatsBatchSink<M, S> {
    pub async fn new(
        address: &str,
        message_mapper: M,
        subject_mapper: S
    ) -> Result<NatsBatchSink<M, S>, async_nats::ConnectError> {
        let client = NatsClient::new(address.to_string(), message_mapper, subject_mapper).await?;
        Ok(NatsBatchSink {
            client,
            committed_lsn: None,
            final_lsn: None,
            table_schemas: HashMap::new(),
        })
    }
}

impl SinkError for NatsSinkError {}

#[async_trait]
impl<M: MessageMapper + Send + Sync, S: SubjectMapper + Send + Sync> BatchSink for NatsBatchSink<M, S> {
    type Error = NatsSinkError;
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        if !self.client.bucket_exists().await {
            self.client.create_bucket().await?;
            self.client.insert_last_lsn_row().await?;
        } else {
            info!("bucket already exists")
        }

        let last_lsn = self.client.get_last_lsn().await?;
        self.committed_lsn = Some(last_lsn);

        Ok(PipelineResumptionState {
            copied_tables: HashSet::new(),
            last_lsn,
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        self.table_schemas = table_schemas;
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        let mut rows_batch: HashMap<TableId, Vec<TableRow>> = HashMap::new();
        let mut new_last_lsn = PgLsn::from(0);

        for event in events {
            match event {
                CdcEvent::Begin(begin_body) => {
                    let final_lsn_u64 = begin_body.final_lsn();
                    self.final_lsn = Some(final_lsn_u64.into());
                }
                CdcEvent::Commit(commit_body) => {
                    let commit_lsn: PgLsn = commit_body.commit_lsn().into();
                    if let Some(final_lsn) = self.final_lsn {
                        if commit_lsn == final_lsn {
                            new_last_lsn = commit_lsn;
                        } else {
                            Err(NatsSinkError::IncorrectCommitLsn(commit_lsn, final_lsn))?
                        }
                    } else {
                        Err(NatsSinkError::CommitWithoutBegin)?
                    }
                }
                CdcEvent::Insert(insert) => {
                    let (table_id, table_row) = insert;
                    let schema = self
                        .table_schemas
                        .get(&table_id)
                        .ok_or(NatsSinkError::MissingTableSchemas)?;

                    self.client.publish(table_id, table_row, schema).await?;
                }
                CdcEvent::Update(_) => {}
                CdcEvent::Delete(_) => {}
                CdcEvent::Relation(_) => {}
                CdcEvent::KeepAliveRequested { reply: _ } => {}
                CdcEvent::Type(_) => {}
            };
        }

        if new_last_lsn != PgLsn::from(0) {
            self.client.set_last_lsn(new_last_lsn).await?;
            self.committed_lsn = Some(new_last_lsn);
        }

        let committed_lsn = self.committed_lsn.expect("committed lsn is none");
        Ok(committed_lsn)
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

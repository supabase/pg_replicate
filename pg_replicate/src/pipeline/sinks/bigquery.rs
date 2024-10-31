use std::collections::HashMap;

use async_trait::async_trait;
use gcp_bigquery_client::error::BQError;
use thiserror::Error;
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use crate::{
    clients::bigquery::BigQueryClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow, Cell},
    pipeline::PipelineResumptionState,
    table::{ColumnSchema, TableId, TableSchema},
};

use super::{BatchSink, SinkError};

#[derive(Debug, Error)]
pub enum BigQuerySinkError {
    #[error("big query error: {0}")]
    BigQuery(#[from] BQError),

    #[error("missing table schemas")]
    MissingTableSchemas,

    #[error("missing table id: {0}")]
    MissingTableId(TableId),

    #[error("incorrect commit lsn: {0}(expected: {0})")]
    IncorrectCommitLsn(PgLsn, PgLsn),

    #[error("commit message without begin message")]
    CommitWithoutBegin,
}

pub struct BigQueryBatchSink {
    client: BigQueryClient,
    dataset_id: String,
    table_schemas: Option<HashMap<TableId, TableSchema>>,
    committed_lsn: Option<PgLsn>,
}

impl BigQueryBatchSink {
    pub async fn new_with_key_path(
        project_id: String,
        dataset_id: String,
        gcp_sa_key_path: &str,
    ) -> Result<BigQueryBatchSink, BQError> {
        let client = BigQueryClient::new_with_key_path(project_id, gcp_sa_key_path).await?;
        Ok(BigQueryBatchSink {
            client,
            dataset_id,
            table_schemas: None,
            committed_lsn: None,
        })
    }

    pub async fn new_with_key(
        project_id: String,
        dataset_id: String,
        gcp_sa_key: &str,
    ) -> Result<BigQueryBatchSink, BQError> {
        let client = BigQueryClient::new_with_key(project_id, gcp_sa_key).await?;
        Ok(BigQueryBatchSink {
            client,
            dataset_id,
            table_schemas: None,
            committed_lsn: None,
        })
    }

    fn get_table_schema(&self, table_id: TableId) -> Result<&TableSchema, BigQuerySinkError> {
        self.table_schemas
            .as_ref()
            .ok_or(BigQuerySinkError::MissingTableSchemas)?
            .get(&table_id)
            .ok_or(BigQuerySinkError::MissingTableId(table_id))
    }
}

#[async_trait]
impl BatchSink for BigQueryBatchSink {
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, SinkError> {
        info!("getting resumption state from bigquery");
        let copied_table_column_schemas = [ColumnSchema {
            name: "table_id".to_string(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
            identity: true,
        }];

        self.client
            .create_table_if_missing(
                &self.dataset_id,
                "copied_tables",
                &copied_table_column_schemas,
            )
            .await?;

        let last_lsn_column_schemas = [
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                identity: true,
            },
            ColumnSchema {
                name: "lsn".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                identity: false,
            },
        ];
        if self
            .client
            .create_table_if_missing(&self.dataset_id, "last_lsn", &last_lsn_column_schemas)
            .await?
        {
            self.client.insert_last_lsn_row(&self.dataset_id).await?;
        }

        let copied_tables = self.client.get_copied_table_ids(&self.dataset_id).await?;
        let last_lsn = self.client.get_last_lsn(&self.dataset_id).await?;

        self.committed_lsn = Some(last_lsn);

        Ok(PipelineResumptionState {
            copied_tables,
            last_lsn,
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), SinkError> {
        for table_schema in table_schemas.values() {
            self.client
                .create_table_if_missing(
                    &self.dataset_id,
                    &table_schema.table_name.name,
                    &table_schema.column_schemas,
                )
                .await?;
        }

        self.table_schemas = Some(table_schemas);

        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        mut table_rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), SinkError> {
        let table_schema = self.get_table_schema(table_id)?;
        //TODO: remove this clone
        let table_name = &table_schema.table_name.name.clone();
        let table_descriptor = table_schema.into();

        for table_row in &mut table_rows {
            table_row.values.push(Cell::String("UPSERT".to_string()));
        }

        self.client
            .stream_rows(&self.dataset_id, table_name, &table_descriptor, &table_rows)
            .await?;

        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, SinkError> {
        let mut table_name_to_table_rows = HashMap::new();
        let mut new_last_lsn = PgLsn::from(0);
        let mut final_lsn: Option<PgLsn> = None;
        for event in events {
            match event {
                CdcEvent::Begin(begin_body) => {
                    let final_lsn_u64 = begin_body.final_lsn();
                    final_lsn = Some(final_lsn_u64.into());
                }
                CdcEvent::Commit(commit_body) => {
                    let commit_lsn: PgLsn = commit_body.commit_lsn().into();
                    if let Some(final_lsn) = final_lsn {
                        if commit_lsn == final_lsn {
                            new_last_lsn = commit_lsn;
                        } else {
                            Err(BigQuerySinkError::IncorrectCommitLsn(commit_lsn, final_lsn))?
                        }
                    } else {
                        Err(BigQuerySinkError::CommitWithoutBegin)?
                    }
                }
                CdcEvent::Insert((table_id, mut table_row)) => {
                    table_row.values.push(Cell::String("UPSERT".to_string()));
                    let table_rows: &mut Vec<TableRow> =
                        table_name_to_table_rows.entry(table_id).or_default();
                    table_rows.push(table_row);
                }
                CdcEvent::Update((table_id, mut table_row)) => {
                    table_row.values.push(Cell::String("UPSERT".to_string()));
                    let table_rows: &mut Vec<TableRow> =
                        table_name_to_table_rows.entry(table_id).or_default();
                    table_rows.push(table_row);
                }
                CdcEvent::Delete((table_id, mut table_row)) => {
                    table_row.values.push(Cell::String("DELETE".to_string()));
                    let table_rows: &mut Vec<TableRow> =
                        table_name_to_table_rows.entry(table_id).or_default();
                    table_rows.push(table_row);
                }
                CdcEvent::Relation(_) => {}
                CdcEvent::KeepAliveRequested { reply: _ } => {}
            }
        }

        for (table_id, table_rows) in table_name_to_table_rows {
            let table_schema = self.get_table_schema(table_id)?;
            let table_name = &table_schema.table_name.name.clone();
            let table_descriptor = table_schema.into();
            self.client
                .stream_rows(&self.dataset_id, table_name, &table_descriptor, &table_rows)
                .await?;
        }

        if new_last_lsn != PgLsn::from(0) {
            self.client
                .set_last_lsn(&self.dataset_id, new_last_lsn)
                .await?;
            self.committed_lsn = Some(new_last_lsn);
        }

        let committed_lsn = self.committed_lsn.expect("committed lsn is none");
        Ok(committed_lsn)
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), SinkError> {
        self.client
            .insert_into_copied_tables(&self.dataset_id, table_id)
            .await?;
        Ok(())
    }

    async fn truncate_table(&mut self, _table_id: TableId) -> Result<(), SinkError> {
        Ok(())
    }
}

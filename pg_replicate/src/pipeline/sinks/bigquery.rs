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
    table::{ColumnSchema, TableId, TableName, TableSchema},
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

impl SinkError for BigQuerySinkError {}

pub struct BigQueryBatchSink {
    client: BigQueryClient,
    dataset_id: String,
    table_schemas: Option<HashMap<TableId, TableSchema>>,
    committed_lsn: Option<PgLsn>,
    final_lsn: Option<PgLsn>,
    max_staleness_mins: u16,
}

impl BigQueryBatchSink {
    pub async fn new_with_key_path(
        project_id: String,
        dataset_id: String,
        gcp_sa_key_path: &str,
        max_staleness_mins: u16,
    ) -> Result<BigQueryBatchSink, BQError> {
        let client = BigQueryClient::new_with_key_path(project_id, gcp_sa_key_path).await?;
        Ok(BigQueryBatchSink {
            client,
            dataset_id,
            table_schemas: None,
            committed_lsn: None,
            final_lsn: None,
            max_staleness_mins,
        })
    }

    pub async fn new_with_key(
        project_id: String,
        dataset_id: String,
        gcp_sa_key: &str,
        max_staleness_mins: u16,
    ) -> Result<BigQueryBatchSink, BQError> {
        let client = BigQueryClient::new_with_key(project_id, gcp_sa_key).await?;
        Ok(BigQueryBatchSink {
            client,
            dataset_id,
            table_schemas: None,
            committed_lsn: None,
            final_lsn: None,
            max_staleness_mins,
        })
    }

    #[expect(clippy::result_large_err)]
    fn get_table_schema(&self, table_id: TableId) -> Result<&TableSchema, BigQuerySinkError> {
        self.table_schemas
            .as_ref()
            .ok_or(BigQuerySinkError::MissingTableSchemas)?
            .get(&table_id)
            .ok_or(BigQuerySinkError::MissingTableId(table_id))
    }

    fn table_name_in_bq(table_name: &TableName) -> String {
        format!("{}_{}", table_name.schema, table_name.name)
    }
}

#[async_trait]
impl BatchSink for BigQueryBatchSink {
    type Error = BigQuerySinkError;
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        info!("getting resumption state from bigquery");
        let copied_table_column_schemas = [ColumnSchema {
            name: "table_id".to_string(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
            primary: true,
        }];

        self.client
            .create_table_if_missing(
                &self.dataset_id,
                "copied_tables",
                &copied_table_column_schemas,
                self.max_staleness_mins,
            )
            .await?;

        let last_lsn_column_schemas = [
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                primary: true,
            },
            ColumnSchema {
                name: "lsn".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                primary: false,
            },
        ];
        if self
            .client
            .create_table_if_missing(
                &self.dataset_id,
                "last_lsn",
                &last_lsn_column_schemas,
                self.max_staleness_mins,
            )
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
    ) -> Result<(), Self::Error> {
        for table_schema in table_schemas.values() {
            let table_name = Self::table_name_in_bq(&table_schema.table_name);
            self.client
                .create_table_if_missing(
                    &self.dataset_id,
                    &table_name,
                    &table_schema.column_schemas,
                    self.max_staleness_mins,
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
    ) -> Result<(), Self::Error> {
        let table_schema = self.get_table_schema(table_id)?;
        let table_name = Self::table_name_in_bq(&table_schema.table_name);
        let table_descriptor = table_schema.into();

        for table_row in &mut table_rows {
            table_row.values.push(Cell::String("UPSERT".to_string()));
        }

        self.client
            .stream_rows(&self.dataset_id, table_name, &table_descriptor, &table_rows)
            .await?;

        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        let mut table_name_to_table_rows = HashMap::new();
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
                CdcEvent::Origin(_) => {}
                CdcEvent::Truncate(_) => {
                    // BigQuery doesn't support TRUNCATE DML statement when using the storage write API.
                    // If you try to truncate a table that has a streaming buffer, you will get the following error:
                    // TRUNCATE DML statement over table <tablename> would affect rows in the streaming buffer, which is not supported
                }
                CdcEvent::Relation(_) => {}
                CdcEvent::KeepAliveRequested { reply: _ } => {}
                CdcEvent::Type(_) => {}
            }
        }

        for (table_id, table_rows) in table_name_to_table_rows {
            let table_schema = self.get_table_schema(table_id)?;
            let table_name = Self::table_name_in_bq(&table_schema.table_name);
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

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        self.client
            .insert_into_copied_tables(&self.dataset_id, table_id)
            .await?;
        Ok(())
    }

    async fn truncate_table(&mut self, _table_id: TableId) -> Result<(), Self::Error> {
        Ok(())
    }
}

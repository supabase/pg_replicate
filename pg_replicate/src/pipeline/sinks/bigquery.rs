use std::collections::HashMap;

use async_trait::async_trait;
use gcp_bigquery_client::error::BQError;
use thiserror::Error;
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use crate::{
    clients::bigquery::BigQueryClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::PipelineResumptionState,
    table::{ColumnSchema, TableId, TableSchema},
};

use super::{Sink, SinkError};

#[derive(Debug, Error)]
pub enum BigQuerySinkError {
    #[error("big query error: {0}")]
    BigQuery(#[from] BQError),

    #[error("missing table schemas")]
    MissingTableSchemas,

    #[error("missing table id: {0}")]
    MissingTableId(TableId),
}

pub struct BigQuerySink {
    dataset_id: String,
    table_schemas: Option<HashMap<TableId, TableSchema>>,
    pub client: BigQueryClient,
}

impl BigQuerySink {
    pub async fn new(
        project_id: String,
        dataset_id: String,
        gcp_sa_key_path: &str,
    ) -> Result<BigQuerySink, BQError> {
        let client = BigQueryClient::new(project_id, gcp_sa_key_path).await?;
        Ok(BigQuerySink {
            dataset_id,
            table_schemas: None,
            client,
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
impl Sink for BigQuerySink {
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, SinkError> {
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

        let last_lsn_column_schemas = [ColumnSchema {
            name: "lsn".to_string(),
            typ: Type::INT8,
            modifier: 0,
            nullable: false,
            identity: true,
        }];
        if self
            .client
            .create_table_if_missing(&self.dataset_id, "last_lsn", &last_lsn_column_schemas)
            .await?
        {
            self.client.insert_last_lsn_row(&self.dataset_id).await?;
        }

        let copied_tables = self.client.get_copied_table_ids(&self.dataset_id).await?;
        let last_lsn = self.client.get_last_lsn(&self.dataset_id).await?;

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

    async fn write_table_row(
        &mut self,
        row: TableRow,
        _table_id: TableId,
    ) -> Result<(), SinkError> {
        info!("{row:?}");
        Ok(())
    }

    async fn write_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, SinkError> {
        info!("{event:?}");
        Ok(PgLsn::from(0))
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), SinkError> {
        self.client
            .insert_into_copied_tables(&self.dataset_id, table_id)
            .await?;
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), SinkError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client
            .truncate_table(&self.dataset_id, &table_schema.table_name.name)
            .await?;
        Ok(())
    }
}

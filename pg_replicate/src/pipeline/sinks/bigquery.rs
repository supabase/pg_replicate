use std::collections::HashMap;

use async_trait::async_trait;
use gcp_bigquery_client::error::BQError;
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use crate::{
    clients::bigquery::BigQueryClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::PipelineResumptionState,
    table::{ColumnSchema, TableId, TableName, TableSchema},
};

use super::{Sink, SinkError};

pub struct BigQuerySink {
    dataset_id: String,
    pub client: BigQueryClient,
}

impl BigQuerySink {
    pub async fn new(
        project_id: String,
        dataset_id: String,
        gcp_sa_key_path: &str,
    ) -> Result<BigQuerySink, BQError> {
        let client = BigQueryClient::new(project_id, gcp_sa_key_path).await?;
        Ok(BigQuerySink { dataset_id, client })
    }
}

#[async_trait]
impl Sink for BigQuerySink {
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, SinkError> {
        let copied_tables_table_name = TableName {
            schema: self.dataset_id.clone(),
            name: "copied_tables".to_string(),
        };
        let copied_table_column_schemas = [ColumnSchema {
            name: "table_id".to_string(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
            identity: true,
        }];

        self.client
            .create_table_if_missing(&copied_tables_table_name, &copied_table_column_schemas)
            .await?;

        let last_lsn_table_name = TableName {
            schema: self.dataset_id.clone(),
            name: "last_lsn".to_string(),
        };
        let last_lsn_column_schemas = [ColumnSchema {
            name: "lsn".to_string(),
            typ: Type::INT8,
            modifier: 0,
            nullable: false,
            identity: true,
        }];
        if self
            .client
            .create_table_if_missing(&last_lsn_table_name, &last_lsn_column_schemas)
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
        info!("{table_schemas:?}");
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
        info!("table {table_id} copied");
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), SinkError> {
        info!("table {table_id} truncated");
        Ok(())
    }
}

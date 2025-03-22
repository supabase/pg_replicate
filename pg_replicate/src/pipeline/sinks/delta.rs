use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::Utc;
use tokio_postgres::types::PgLsn;
use tracing::info;

use super::{BatchSink, SinkError};
use crate::{
    clients::delta::DeltaClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow, Cell},
    pipeline::PipelineResumptionState,
    table::{TableId, TableSchema},
};
use deltalake::arrow::error::ArrowError;
use deltalake::{arrow::datatypes::Schema, DeltaTableError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeltaSinkError {
    #[error("arrow error: {0}")]
    DeltaArrow(#[from] ArrowError),

    #[error("delta error: {0}")]
    Delta(#[from] DeltaTableError),

    #[error("missing table schemas")]
    MissingTableSchemas,

    #[error("missing delta schemas")]
    MissingDeltaSchemas(String),

    #[error("missing table id: {0}")]
    MissingTableId(TableId),

    #[error("incorrect commit lsn: {0}(expected: {0})")]
    IncorrectCommitLsn(PgLsn, PgLsn),

    #[error("commit message without begin message")]
    CommitWithoutBegin,
}

pub struct DeltaSink {
    client: DeltaClient,
}

impl DeltaSink {
    pub fn new(path: String) -> Self {
        DeltaSink {
            client: DeltaClient {
                path,
                table_schemas: None,
                delta_schemas: None,
            },
        }
    }

    fn add_optional_columns(table_row: &mut TableRow, op: &str) {
        let op = Cell::String(String::from(op));
        let current_time = Cell::TimeStamp(Utc::now().naive_utc());
        table_row.values.push(op);
        table_row.values.push(current_time);
    }
}

impl SinkError for DeltaSinkError {}

#[async_trait]
impl BatchSink for DeltaSink {
    type Error = DeltaSinkError;
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        Ok(PipelineResumptionState {
            copied_tables: HashSet::new(),
            last_lsn: PgLsn::from(0),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        let mut delta_schema: HashMap<String, Arc<Schema>> = HashMap::new();

        for table_schema in table_schemas.values() {
            let table_name = DeltaClient::table_name_in_delta(&table_schema.table_name);

            let schema = self
                .client
                .create_table_schema(&table_name, &table_schema.column_schemas)
                .await?;

            delta_schema.insert(table_name, schema);
        }
        self.client.delta_schemas = Some(delta_schema);
        self.client.table_schemas = Some(table_schemas);

        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        let mut rows_batch: HashMap<TableId, Vec<TableRow>> = HashMap::new();

        let updated_rows: Vec<TableRow> = rows
            .into_iter()
            .map(|mut table_row| {
                Self::add_optional_columns(&mut table_row, "I");
                table_row
            })
            .collect();

        rows_batch.entry(table_id).or_default().extend(updated_rows);
        self.client.write_to_table_batch(rows_batch).await?;

        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        let mut rows_batch: HashMap<TableId, Vec<TableRow>> = HashMap::new();

        for event in events {
            match event {
                CdcEvent::Begin(_) => {}
                CdcEvent::Commit(_) => {}
                CdcEvent::Insert(insert) => {
                    let (table_id, mut table_row) = insert;
                    Self::add_optional_columns(&mut table_row, "I");
                    rows_batch.entry(table_id).or_default().push(table_row);
                }
                CdcEvent::Update(update) => {
                    let (table_id, mut table_row) = update;
                    Self::add_optional_columns(&mut table_row, "U");
                    rows_batch.entry(table_id).or_default().push(table_row);
                }
                CdcEvent::Delete(delete) => {
                    let (table_id, mut table_row) = delete;
                    Self::add_optional_columns(&mut table_row, "D");
                    rows_batch.entry(table_id).or_default().push(table_row);
                }
                CdcEvent::Relation(_) => {}
                CdcEvent::KeepAliveRequested { reply: _ } => {}
                CdcEvent::Type(_) => {}
            };
        }

        self.client.write_to_table_batch(rows_batch).await?;
        Ok(PgLsn::from(0))
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

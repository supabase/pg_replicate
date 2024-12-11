use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::Utc;
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use super::{BatchSink, SinkError};
use crate::{
    clients::delta::DeltaClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow, Cell},
    pipeline::PipelineResumptionState,
    table::{ColumnSchema, TableId, TableSchema},
};
use deltalake::arrow::error::ArrowError;
use deltalake::DeltaTableError;
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
    committed_lsn: Option<PgLsn>,
    final_lsn: Option<PgLsn>,
}

impl DeltaSink {
    pub fn new(path: String) -> Self {
        DeltaSink {
            client: DeltaClient {
                path,
                table_schemas: None,
                delta_schemas: None,
            },
            committed_lsn: None,
            final_lsn: None,
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

        if !self.client.delta_table_exists("last_lsn").await {
            self.client
                .create_table("last_lsn", &last_lsn_column_schemas)
                .await?;

            self.client.insert_last_lsn_row().await?;
        } else {
            info!("last_lsn table already exists")
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
        let mut delta_schema = HashMap::new();

        for table_schema in table_schemas.values() {
            let table_name = DeltaClient::table_name_in_delta(&table_schema.table_name);

            let schema = if !self.client.delta_table_exists(&table_name).await {
                self.client
                    .create_table(&table_name, &table_schema.column_schemas)
                    .await?
            } else {
                info!("Table already exists: {}", table_name);
                DeltaClient::generate_schema(&table_schema.column_schemas)?
            };

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
                            Err(DeltaSinkError::IncorrectCommitLsn(commit_lsn, final_lsn))?
                        }
                    } else {
                        Err(DeltaSinkError::CommitWithoutBegin)?
                    }
                }
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

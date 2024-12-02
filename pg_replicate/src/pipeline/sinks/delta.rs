use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use tokio_postgres::types::PgLsn;
use tracing::info;

use super::{BatchSink, SinkError};
use crate::{
    clients::delta::DeltaClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::PipelineResumptionState,
    table::{TableId, TableSchema},
};
use deltalake::arrow::error::ArrowError;
use deltalake::{arrow::datatypes::Schema, DeltaTableError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeltaSinkError {
    #[error("Arrow error: {0}")]
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

        for (_, table_schema) in &table_schemas {
            let table_name = DeltaClient::table_name_in_delta(&table_schema.table_name);

            let schema = self
                .client
                .create_table_schema(table_name.clone(), &table_schema.column_schemas)
                .await?;

            delta_schema.insert(table_name.clone(), schema);
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
        for row in rows {
            self.client
                .write_to_table(row, table_id, String::from("I"))
                .await?;
        }
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        for event in events {
            info!("{event:?}");
            match event {
                CdcEvent::Begin(_) => info!(""),
                CdcEvent::Commit(_) => info!(""),
                CdcEvent::Insert(insert) => {
                    let (table_id, row) = insert;
                    self.client
                        .write_to_table(row, table_id, String::from("I"))
                        .await?;
                }
                CdcEvent::Update(update) => {
                    let (table_id, row) = update;
                    self.client
                        .write_to_table(row, table_id, String::from("U"))
                        .await?;
                }
                CdcEvent::Delete(delete) => {
                    let (table_id, row) = delete;
                    self.client
                        .write_to_table(row, table_id, String::from("D"))
                        .await?;
                }
                CdcEvent::Relation(relation_body) => info!("{relation_body:?}"),
                CdcEvent::Type(type_body) => info!("{type_body:?}"),
                CdcEvent::KeepAliveRequested { reply } => info!("{reply:?}"),
            };
        }
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

use std::{collections::HashMap, path::Path};

use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use async_trait::async_trait;
use tokio_postgres::types::{PgLsn, Type};
use tracing::error;

use crate::{
    clients::duckdb::DuckDbClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::PipelineResumptionState,
    table::{ColumnSchema, TableId, TableName, TableSchema},
};

use super::{Sink, SinkError};

pub enum DuckDbRequest {
    GetResumptionState,
    CreateTables(HashMap<TableId, TableSchema>),
    InsertRow(TableRow, TableId),
    HandleCdcEvent(CdcEvent),
    TableCopied(TableId),
    TruncateTable(TableId),
}

pub enum DuckDbResponse {
    ResumptionState(Result<PipelineResumptionState, DuckDbExecutorError>),
    CreateTablesResponse(Result<(), DuckDbExecutorError>),
    InsertRowResponse(Result<(), DuckDbExecutorError>),
    HandleCdcEventResponse(Result<PgLsn, DuckDbExecutorError>),
    TableCopiedResponse(Result<(), DuckDbExecutorError>),
    TruncateTableResponse(Result<(), DuckDbExecutorError>),
}

#[derive(Debug, Error)]
pub enum DuckDbExecutorError {
    #[error("duckdb error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("missing table schemas")]
    MissingTableSchemas,

    #[error("missing table id: {0}")]
    MissingTableId(TableId),

    #[error("incorrect commit lsn: {0}(expected: {0})")]
    IncorrectCommitLsn(PgLsn, PgLsn),

    #[error("commit message without begin message")]
    CommitWithoutBegin,
}

struct DuckDbExecutor {
    client: DuckDbClient,
    req_receiver: Receiver<DuckDbRequest>,
    res_sender: Sender<DuckDbResponse>,
    table_schemas: Option<HashMap<TableId, TableSchema>>,
    final_lsn: Option<PgLsn>,
    committed_lsn: Option<PgLsn>,
}

impl DuckDbExecutor {
    pub fn start(mut self) {
        tokio::spawn(async move {
            while let Some(req) = self.req_receiver.recv().await {
                match req {
                    DuckDbRequest::GetResumptionState => {
                        let result = self.get_resumption_state();
                        let result = result.map(|rs| {
                            self.committed_lsn = Some(rs.last_lsn);
                            rs
                        });
                        let response = DuckDbResponse::ResumptionState(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::CreateTables(table_schemas) => {
                        let result = self.create_tables(&table_schemas);
                        self.table_schemas = Some(table_schemas);
                        let response = DuckDbResponse::CreateTablesResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::InsertRow(row, table_id) => {
                        let result = self.insert_row(table_id, row);
                        let response = DuckDbResponse::InsertRowResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::HandleCdcEvent(event) => {
                        let result = match event {
                            CdcEvent::Begin(begin_body) => {
                                let final_lsn = begin_body.final_lsn();
                                self.final_lsn = Some(final_lsn.into());
                                self.begin_transaction()
                            }
                            CdcEvent::Commit(commit_body) => {
                                let commit_lsn: PgLsn = commit_body.commit_lsn().into();
                                if let Some(final_lsn) = self.final_lsn {
                                    if commit_lsn == final_lsn {
                                        let res =
                                            self.set_last_lsn_and_commit_transaction(commit_lsn);
                                        self.committed_lsn = Some(commit_lsn);
                                        res
                                    } else {
                                        Err(DuckDbExecutorError::IncorrectCommitLsn(
                                            commit_lsn, final_lsn,
                                        ))
                                    }
                                } else {
                                    Err(DuckDbExecutorError::CommitWithoutBegin)
                                }
                            }
                            CdcEvent::Insert((table_id, table_row)) => {
                                self.insert_row(table_id, table_row)
                            }
                            CdcEvent::Update((table_id, table_row)) => {
                                self.update_row(table_id, table_row)
                            }
                            CdcEvent::Delete((table_id, table_row)) => {
                                self.delete_row(table_id, table_row)
                            }
                            CdcEvent::Relation(_) => Ok(()),
                            CdcEvent::KeepAliveRequested { reply: _ } => Ok(()),
                        };

                        let committed_lsn = self.committed_lsn.expect("committed lsn is none");
                        let result = result.map(|_| committed_lsn);
                        let response = DuckDbResponse::HandleCdcEventResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::TableCopied(table_id) => {
                        let result = self.table_copied(table_id);
                        let response = DuckDbResponse::TableCopiedResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::TruncateTable(table_id) => {
                        let result = self.truncate_table(table_id);
                        let response = DuckDbResponse::TruncateTableResponse(result);
                        self.send_response(response).await;
                    }
                }
            }
        });
    }

    async fn send_response(&mut self, response: DuckDbResponse) {
        match self.res_sender.send(response).await {
            Ok(_) => {}
            Err(e) => error!("failed to send response: {e}"),
        }
    }

    fn get_resumption_state(&self) -> Result<PipelineResumptionState, DuckDbExecutorError> {
        let copied_tables_table_name = TableName {
            schema: "pg_replicate".to_string(),
            name: "copied_tables".to_string(),
        };
        let copied_table_column_schemas = vec![ColumnSchema {
            name: "table_id".to_string(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
            identity: true,
        }];
        self.client
            .create_schema_if_missing(&copied_tables_table_name.schema)?;

        self.client
            .create_table_if_missing(&copied_tables_table_name, &copied_table_column_schemas)?;

        let last_lsn_table_name = TableName {
            schema: "pg_replicate".to_string(),
            name: "last_lsn".to_string(),
        };
        let last_lsn_column_schemas = vec![ColumnSchema {
            name: "lsn".to_string(),
            typ: Type::INT8,
            modifier: 0,
            nullable: false,
            identity: true,
        }];
        if self
            .client
            .create_table_if_missing(&last_lsn_table_name, &last_lsn_column_schemas)?
        {
            self.client.insert_last_lsn_row()?;
        }

        let copied_tables = self.client.get_copied_table_ids()?;
        let last_lsn = self.client.get_last_lsn()?;

        Ok(PipelineResumptionState {
            copied_tables,
            last_lsn,
        })
    }

    fn create_tables(
        &self,
        table_schemas: &HashMap<u32, TableSchema>,
    ) -> Result<(), DuckDbExecutorError> {
        for table_schema in table_schemas.values() {
            let schema = &table_schema.table_name.schema;

            self.client.create_schema_if_missing(schema)?;
            self.client
                .create_table_if_missing(&table_schema.table_name, &table_schema.column_schemas)?;
        }

        Ok(())
    }

    fn insert_row(
        &self,
        table_id: TableId,
        table_row: TableRow,
    ) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client
            .insert_row(&table_schema.table_name, &table_row)?;
        Ok(())
    }

    fn update_row(
        &self,
        table_id: TableId,
        table_row: TableRow,
    ) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client.update_row(table_schema, &table_row)?;
        Ok(())
    }

    fn delete_row(
        &self,
        table_id: TableId,
        table_row: TableRow,
    ) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client.delete_row(table_schema, &table_row)?;
        Ok(())
    }

    fn get_table_schema(&self, table_id: TableId) -> Result<&TableSchema, DuckDbExecutorError> {
        self.table_schemas
            .as_ref()
            .ok_or(DuckDbExecutorError::MissingTableSchemas)?
            .get(&table_id)
            .ok_or(DuckDbExecutorError::MissingTableId(table_id))
    }

    fn table_copied(&self, table_id: TableId) -> Result<(), DuckDbExecutorError> {
        self.client.insert_into_copied_tables(table_id)?;
        Ok(())
    }

    fn truncate_table(&self, table_id: TableId) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client.truncate_table(&table_schema.table_name)?;
        Ok(())
    }

    fn begin_transaction(&self) -> Result<(), DuckDbExecutorError> {
        self.client.begin_transaction()?;
        Ok(())
    }

    fn commit_transaction(&self) -> Result<(), DuckDbExecutorError> {
        self.client.commit_transaction()?;
        Ok(())
    }

    fn set_last_lsn_and_commit_transaction(
        &self,
        last_lsn: PgLsn,
    ) -> Result<(), DuckDbExecutorError> {
        self.client.set_last_lsn(last_lsn)?;
        self.commit_transaction()?;
        Ok(())
    }
}

pub struct DuckDbSink {
    req_sender: Sender<DuckDbRequest>,
    res_receiver: Receiver<DuckDbResponse>,
}

const CHANNEL_SIZE: usize = 32;

impl DuckDbSink {
    pub async fn file<P: AsRef<Path>>(file_name: P) -> Result<DuckDbSink, duckdb::Error> {
        let (req_sender, req_receiver) = channel(CHANNEL_SIZE);
        let (res_sender, res_receiver) = channel(CHANNEL_SIZE);
        let client = DuckDbClient::open_file(file_name)?;
        let executor = DuckDbExecutor {
            client,
            req_receiver,
            res_sender,
            table_schemas: None,
            final_lsn: None,
            committed_lsn: None,
        };
        executor.start();
        Ok(DuckDbSink {
            req_sender,
            res_receiver,
        })
    }

    pub async fn in_memory() -> Result<DuckDbSink, duckdb::Error> {
        let (req_sender, req_receiver) = channel(CHANNEL_SIZE);
        let (res_sender, res_receiver) = channel(CHANNEL_SIZE);
        let client = DuckDbClient::open_in_memory()?;
        let executor = DuckDbExecutor {
            client,
            req_receiver,
            res_sender,
            table_schemas: None,
            final_lsn: None,
            committed_lsn: None,
        };
        executor.start();
        Ok(DuckDbSink {
            req_sender,
            res_receiver,
        })
    }

    pub async fn execute(&mut self, req: DuckDbRequest) -> Result<DuckDbResponse, SinkError> {
        self.req_sender.send(req).await?;
        if let Some(res) = self.res_receiver.recv().await {
            Ok(res)
        } else {
            Err(SinkError::NoResponseReceived)
        }
    }
}

#[async_trait]
impl Sink for DuckDbSink {
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, SinkError> {
        let req = DuckDbRequest::GetResumptionState;
        match self.execute(req).await? {
            DuckDbResponse::ResumptionState(res) => {
                let resumption_state = res?;
                Ok(resumption_state)
            }
            _ => panic!("invalid response to GetResumptionState request"),
        }
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), SinkError> {
        let req = DuckDbRequest::CreateTables(table_schemas);
        match self.execute(req).await? {
            DuckDbResponse::CreateTablesResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to CreateTables request"),
        }

        Ok(())
    }

    async fn write_table_row(&mut self, row: TableRow, table_id: TableId) -> Result<(), SinkError> {
        let req = DuckDbRequest::InsertRow(row, table_id);
        match self.execute(req).await? {
            DuckDbResponse::InsertRowResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to InsertRow request"),
        }
        Ok(())
    }

    async fn write_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, SinkError> {
        let req = DuckDbRequest::HandleCdcEvent(event);
        let last_lsn = match self.execute(req).await? {
            DuckDbResponse::HandleCdcEventResponse(res) => res?,
            _ => panic!("invalid response to HandleCdcEvent request"),
        };
        Ok(last_lsn)
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), SinkError> {
        let req = DuckDbRequest::TableCopied(table_id);
        match self.execute(req).await? {
            DuckDbResponse::TableCopiedResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to TableCopied request"),
        }
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), SinkError> {
        let req = DuckDbRequest::TruncateTable(table_id);
        match self.execute(req).await? {
            DuckDbResponse::TruncateTableResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to TruncateTable request"),
        }
        Ok(())
    }
}

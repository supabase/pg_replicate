use core::str;
use postgres::schema::{ColumnSchema, TableId, TableSchema};
use postgres_replication::protocol;
use protocol::{
    BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, OriginBody,
    RelationBody, TruncateBody, TupleData, TypeBody, UpdateBody,
};
use std::{io, str::Utf8Error};
use thiserror::Error;

use crate::conversions::table_row::TableRow;
use crate::conversions::text::{FromTextError, TextFormatConverter};
use crate::conversions::Cell;
use crate::v2::pipeline::PipelineId;
use crate::v2::state::store::base::{StateStore, StateStoreError};

#[derive(Debug, Error)]
pub enum EventConversionError {
    #[error("An unknown replication message type was encountered")]
    UnknownReplicationMessage,

    #[error("Binary format is not supported for data conversion")]
    BinaryFormatNotSupported,

    #[error("Missing tuple data in delete body")]
    MissingTupleInDeleteBody,

    #[error("Table schema not found for table id {0}")]
    MissingSchema(TableId),

    #[error("Error converting from bytes: {0}")]
    FromBytes(#[from] FromTextError),

    #[error("Invalid string value encountered: {0}")]
    InvalidStr(#[from] Utf8Error),

    #[error("IO error encountered: {0}")]
    Io(#[from] io::Error),

    #[error("An error occurred in the state store: {0}")]
    StateStore(#[from] StateStoreError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BeginEvent {
    pub final_lsn: u64,
    pub timestamp: i64,
    pub xid: u32,
}

impl BeginEvent {
    pub fn from_protocol(begin_body: &BeginBody) -> Self {
        Self {
            final_lsn: begin_body.final_lsn(),
            timestamp: begin_body.timestamp(),
            xid: begin_body.xid(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommitEvent {
    pub flags: i8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub timestamp: i64,
}

impl CommitEvent {
    pub fn from_protocol(commit_body: &CommitBody) -> Self {
        Self {
            flags: commit_body.flags(),
            commit_lsn: commit_body.commit_lsn(),
            end_lsn: commit_body.end_lsn(),
            timestamp: commit_body.timestamp(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationEvent {
    pub rel_id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<Column>,
}

impl RelationEvent {
    pub fn from_protocol(relation_body: &RelationBody) -> Result<Self, EventConversionError> {
        Ok(Self {
            rel_id: relation_body.rel_id(),
            namespace: relation_body.namespace()?.to_string(),
            name: relation_body.name()?.to_string(),
            replica_identity: ReplicaIdentity::from_protocol(relation_body.replica_identity()),
            columns: relation_body
                .columns()
                .iter()
                .map(Column::from_protocol)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertEvent {
    pub table_id: TableId,
    pub row: TableRow,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateEvent {
    pub table_id: TableId,
    pub row: TableRow,
    pub identity_row: TableRow,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteEvent {
    pub table_id: TableId,
    pub identity_row: TableRow,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TruncateEvent {
    pub options: i8,
    pub rel_ids: Vec<u32>,
}

impl TruncateEvent {
    pub fn from_protocol(truncate_body: &TruncateBody) -> Self {
        Self {
            options: truncate_body.options(),
            rel_ids: truncate_body.rel_ids().to_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeepAliveEvent {
    pub reply: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

impl ReplicaIdentity {
    fn from_protocol(identity: &protocol::ReplicaIdentity) -> Self {
        match identity {
            protocol::ReplicaIdentity::Default => Self::Default,
            protocol::ReplicaIdentity::Nothing => Self::Nothing,
            protocol::ReplicaIdentity::Full => Self::Full,
            protocol::ReplicaIdentity::Index => Self::Index,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub flags: i8,
    pub name: String,
    pub type_id: i32,
    pub type_modifier: i32,
}

impl Column {
    fn from_protocol(column: &protocol::Column) -> Result<Self, EventConversionError> {
        Ok(Self {
            flags: column.flags(),
            name: column.name()?.to_string(),
            type_id: column.type_id(),
            type_modifier: column.type_modifier(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Begin(BeginEvent),
    Commit(CommitEvent),
    Insert(InsertEvent),
    Update(UpdateEvent),
    Delete(DeleteEvent),
    Relation(RelationEvent),
    Truncate(TruncateEvent),
    Unsupported
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventConverter<S> {
    pipeline_id: PipelineId,
    state_store: S,
}

impl<S> EventConverter<S>
where
    S: StateStore,
{
    pub fn new(pipeline_id: PipelineId, state_store: S) -> Self {
        Self {
            pipeline_id,
            state_store,
        }
    }

    async fn get_table_schema(
        &self,
        table_id: TableId,
    ) -> Result<TableSchema, EventConversionError> {
        self.state_store
            .load_table_schema(self.pipeline_id, table_id)
            .await?
            .ok_or(EventConversionError::MissingSchema(table_id))
    }

    fn convert_tuple_to_row(
        column_schemas: &[ColumnSchema],
        tuple_data: &[TupleData],
    ) -> Result<TableRow, EventConversionError> {
        let mut values = Vec::with_capacity(column_schemas.len());

        for (i, column_schema) in column_schemas.iter().enumerate() {
            let cell = match &tuple_data[i] {
                TupleData::Null => Cell::Null,
                TupleData::UnchangedToast => TextFormatConverter::default_value(&column_schema.typ),
                TupleData::Binary(_) => return Err(EventConversionError::BinaryFormatNotSupported),
                TupleData::Text(bytes) => {
                    let str = str::from_utf8(&bytes[..])?;
                    TextFormatConverter::try_from_str(&column_schema.typ, str)?
                }
            };
            values.push(cell);
        }

        Ok(TableRow { values })
    }

    async fn convert_insert_to_event(
        &self,
        insert_body: &InsertBody,
    ) -> Result<Event, EventConversionError> {
        let table_id = insert_body.rel_id();
        let table_schema = self.get_table_schema(table_id).await?;
        let row = Self::convert_tuple_to_row(
            &table_schema.column_schemas,
            insert_body.tuple().tuple_data(),
        )?;

        Ok(Event::Insert(InsertEvent { table_id, row }))
    }

    async fn convert_update_to_event(
        &self,
        update_body: &UpdateBody,
    ) -> Result<Event, EventConversionError> {
        let table_id = update_body.rel_id();
        let table_schema = self.get_table_schema(table_id).await?;
        let identity = update_body
            .key_tuple()
            .or(update_body.old_tuple())
            .ok_or(EventConversionError::MissingTupleInDeleteBody)?;

        let identity_row =
            Self::convert_tuple_to_row(&table_schema.column_schemas, identity.tuple_data())?;
        let row = Self::convert_tuple_to_row(
            &table_schema.column_schemas,
            update_body.new_tuple().tuple_data(),
        )?;

        Ok(Event::Update(UpdateEvent {
            table_id,
            row,
            identity_row,
        }))
    }

    async fn convert_delete_to_event(
        &self,
        delete_body: &DeleteBody,
    ) -> Result<Event, EventConversionError> {
        let table_id = delete_body.rel_id();
        let table_schema = self.get_table_schema(table_id).await?;
        let identity = delete_body
            .key_tuple()
            .or(delete_body.old_tuple())
            .ok_or(EventConversionError::MissingTupleInDeleteBody)?;

        let identity_row =
            Self::convert_tuple_to_row(&table_schema.column_schemas, identity.tuple_data())?;

        Ok(Event::Delete(DeleteEvent {
            table_id,
            identity_row,
        }))
    }

    pub async fn convert(
        &self,
        message: &LogicalReplicationMessage,
    ) -> Result<Event, EventConversionError> {
        match message {
            LogicalReplicationMessage::Begin(begin_body) => {
                Ok(Event::Begin(BeginEvent::from_protocol(begin_body)))
            }
            LogicalReplicationMessage::Commit(commit_body) => {
                Ok(Event::Commit(CommitEvent::from_protocol(commit_body)))
            }
            LogicalReplicationMessage::Relation(relation_body) => Ok(Event::Relation(
                RelationEvent::from_protocol(relation_body)?,
            )),
            LogicalReplicationMessage::Insert(insert_body) => {
                self.convert_insert_to_event(insert_body).await
            }
            LogicalReplicationMessage::Update(update_body) => {
                self.convert_update_to_event(update_body).await
            }
            LogicalReplicationMessage::Delete(delete_body) => {
                self.convert_delete_to_event(delete_body).await
            }
            LogicalReplicationMessage::Truncate(truncate_body) => {
                Ok(Event::Truncate(TruncateEvent::from_protocol(truncate_body)))
            }
            LogicalReplicationMessage::Origin(_) | LogicalReplicationMessage::Type(_) => {
                Ok(Event::Unsupported)
            }
            _ => Err(EventConversionError::UnknownReplicationMessage),
        }
    }
}

use std::{
    collections::HashMap,
    num::ParseIntError,
    str::{from_utf8, ParseBoolError, Utf8Error},
};

use chrono::NaiveDateTime;
use postgres_protocol::message::backend::{
    BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, RelationBody,
    ReplicationMessage, TupleData, UpdateBody,
};
use thiserror::Error;
use tokio_postgres::types::Type;

use crate::{
    pipeline::batching::BatchBoundary,
    table::{ColumnSchema, TableId, TableSchema},
};

use super::table_row::{Cell, TableRow};

#[derive(Debug, Error)]
pub enum CdcEventConversionError {
    #[error("message not supported")]
    MessageNotSupported,

    #[error("unknown replication message")]
    UnknownReplicationMessage,

    #[error("unchanged toast not yet supported")]
    UnchangedToastNotSupported,

    #[error("invalid string value")]
    InvalidStr(#[from] Utf8Error),

    #[error("invalid bool value")]
    InvalidBool(#[from] ParseBoolError),

    #[error("invalid int value")]
    InvalidInt(#[from] ParseIntError),

    #[error("invalid timestamp value")]
    InvalidTimestamp(#[from] chrono::ParseError),

    #[error("unsupported type")]
    UnsupportedType(String),

    #[error("out of range timestamp")]
    OutOfRangeTimestamp,

    #[error("missing tuple in delete body")]
    MissingTupleInDeleteBody,

    #[error("schema missing for table id {0}")]
    MissingSchema(TableId),

    #[error("invalid namespace: {0}")]
    InvalidNamespace(String),

    #[error("invalid relation name: {0}")]
    InvalidRelationName(String),

    #[error("invalid column name: {0}")]
    InvalidColumnName(String),
}

pub struct CdcEventConverter;

impl CdcEventConverter {
    fn from_tuple_data(typ: &Type, val: &TupleData) -> Result<Cell, CdcEventConversionError> {
        let bytes = match val {
            TupleData::Null => {
                return Ok(Cell::Null);
            }
            TupleData::UnchangedToast => {
                return Err(CdcEventConversionError::UnchangedToastNotSupported)
            }
            TupleData::Text(bytes) => &bytes[..],
        };
        match *typ {
            Type::BOOL => {
                let val = from_utf8(bytes)?;
                let val: bool = val.parse()?;
                Ok(Cell::Bool(val))
            }
            // Type::BYTEA => Ok(Value::Bytes(bytes.to_vec())),
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                let val = from_utf8(bytes)?;
                Ok(Cell::String(val.to_string()))
            }
            Type::INT2 => {
                let val = from_utf8(bytes)?;
                let val: i16 = val.parse()?;
                Ok(Cell::I16(val))
            }
            Type::INT4 => {
                let val = from_utf8(bytes)?;
                let val: i32 = val.parse()?;
                Ok(Cell::I32(val))
            }
            Type::INT8 => {
                let val = from_utf8(bytes)?;
                let val: i64 = val.parse()?;
                Ok(Cell::I64(val))
            }
            Type::TIMESTAMP => {
                let val = from_utf8(bytes)?;
                let val = NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.f")?;
                let val = val.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                Ok(Cell::TimeStamp(val))
            }
            ref typ => Err(CdcEventConversionError::UnsupportedType(typ.to_string())),
        }
    }

    fn from_tuple_data_slice(
        column_schemas: &[ColumnSchema],
        tuple_data: &[TupleData],
    ) -> Result<TableRow, CdcEventConversionError> {
        let mut values = Vec::with_capacity(column_schemas.len());

        for (i, column_schema) in column_schemas.iter().enumerate() {
            let val = Self::from_tuple_data(&column_schema.typ, &tuple_data[i])?;
            values.push(val);
        }

        Ok(TableRow { values })
    }

    fn from_insert_body(
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        insert_body: InsertBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let row = Self::from_tuple_data_slice(column_schemas, insert_body.tuple().tuple_data())?;

        Ok(CdcEvent::Insert((table_id, row)))
    }

    //TODO: handle when identity columns are changed
    fn from_update_body(
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        update_body: UpdateBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let row =
            Self::from_tuple_data_slice(column_schemas, update_body.new_tuple().tuple_data())?;

        Ok(CdcEvent::Update((table_id, row)))
    }

    fn from_delete_body(
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        delete_body: DeleteBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let tuple = delete_body
            .key_tuple()
            .or(delete_body.old_tuple())
            .ok_or(CdcEventConversionError::MissingTupleInDeleteBody)?;

        let row = Self::from_tuple_data_slice(column_schemas, tuple.tuple_data())?;

        Ok(CdcEvent::Delete((table_id, row)))
    }

    pub fn try_from(
        value: ReplicationMessage<LogicalReplicationMessage>,
        table_schemas: &HashMap<TableId, TableSchema>,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        match value {
            ReplicationMessage::XLogData(xlog_data) => match xlog_data.into_data() {
                LogicalReplicationMessage::Begin(begin_body) => Ok(CdcEvent::Begin(begin_body)),
                LogicalReplicationMessage::Commit(commit_body) => Ok(CdcEvent::Commit(commit_body)),
                LogicalReplicationMessage::Origin(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                LogicalReplicationMessage::Relation(relation_body) => {
                    Ok(CdcEvent::Relation(relation_body))
                }
                LogicalReplicationMessage::Type(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                LogicalReplicationMessage::Insert(insert_body) => {
                    let table_id = insert_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(Self::from_insert_body(
                        table_id,
                        column_schemas,
                        insert_body,
                    )?)
                }
                LogicalReplicationMessage::Update(update_body) => {
                    let table_id = update_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(Self::from_update_body(
                        table_id,
                        column_schemas,
                        update_body,
                    )?)
                }
                LogicalReplicationMessage::Delete(delete_body) => {
                    let table_id = delete_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(Self::from_delete_body(
                        table_id,
                        column_schemas,
                        delete_body,
                    )?)
                }
                LogicalReplicationMessage::Truncate(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                _ => Err(CdcEventConversionError::UnknownReplicationMessage),
            },
            ReplicationMessage::PrimaryKeepAlive(keep_alive) => Ok(CdcEvent::KeepAliveRequested {
                reply: keep_alive.reply() == 1,
            }),
            _ => Err(CdcEventConversionError::UnknownReplicationMessage),
        }
    }
}

#[derive(Debug)]
pub enum CdcEvent {
    Begin(BeginBody),
    Commit(CommitBody),
    Insert((TableId, TableRow)),
    Update((TableId, TableRow)),
    Delete((TableId, TableRow)),
    Relation(RelationBody),
    KeepAliveRequested { reply: bool },
}

impl BatchBoundary for CdcEvent {
    fn is_last_in_batch(&self) -> bool {
        matches!(
            self,
            CdcEvent::Commit(_) | CdcEvent::KeepAliveRequested { reply: _ }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::batching::BatchBoundary;
    use bytes::Bytes;

    #[test]
    fn test_cdc_event_is_last_in_batch() {
        // For testing we'll skip BeginBody, CommitBody and RelationBody and only
        // test TableRow-based events plus KeepAliveRequested

        // Test that KeepAliveRequested is marked as last in batch
        let keep_alive_event = CdcEvent::KeepAliveRequested { reply: true };
        assert!(keep_alive_event.is_last_in_batch());

        // Test that other events are not last in batch
        let dummy_table_row = TableRow { values: vec![] };

        let insert_event = CdcEvent::Insert((123, TableRow { values: vec![] }));
        assert!(!insert_event.is_last_in_batch());

        let update_event = CdcEvent::Update((123, TableRow { values: vec![] }));
        assert!(!update_event.is_last_in_batch());

        let delete_event = CdcEvent::Delete((123, dummy_table_row));
        assert!(!delete_event.is_last_in_batch());
    }

    #[test]
    fn test_cell_conversion_from_tuple_data() {
        // Test null conversion
        let null_result = CdcEventConverter::from_tuple_data(&Type::BOOL, &TupleData::Null);
        assert!(matches!(null_result, Ok(Cell::Null)));

        // Test unchanged toast error
        let toast_result =
            CdcEventConverter::from_tuple_data(&Type::BOOL, &TupleData::UnchangedToast);
        assert!(matches!(
            toast_result,
            Err(CdcEventConversionError::UnchangedToastNotSupported)
        ));

        // Test string conversion - since most types are handled similarly in the from_tuple_data method
        let string_bytes = Bytes::from("test_string");
        let string_result =
            CdcEventConverter::from_tuple_data(&Type::TEXT, &TupleData::Text(string_bytes));
        assert!(matches!(string_result, Ok(Cell::String(s)) if s == "test_string"));

        // Test i16 conversion
        let i16_bytes = Bytes::from("42");
        let i16_result =
            CdcEventConverter::from_tuple_data(&Type::INT2, &TupleData::Text(i16_bytes));
        assert!(matches!(i16_result, Ok(Cell::I16(42))));

        // Test i32 conversion
        let i32_bytes = Bytes::from("12345");
        let i32_result =
            CdcEventConverter::from_tuple_data(&Type::INT4, &TupleData::Text(i32_bytes));
        assert!(matches!(i32_result, Ok(Cell::I32(12345))));

        // Test i64 conversion
        let i64_bytes = Bytes::from("9876543210");
        let i64_result =
            CdcEventConverter::from_tuple_data(&Type::INT8, &TupleData::Text(i64_bytes));
        assert!(matches!(i64_result, Ok(Cell::I64(9876543210))));
    }

    #[test]
    fn test_from_tuple_data_slice() {
        // Create column schemas
        let column_schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT4,
                modifier: 0,
                nullable: false,
                identity: true,
            },
            ColumnSchema {
                name: "name".to_string(),
                typ: Type::TEXT,
                modifier: 0,
                nullable: true,
                identity: false,
            },
            ColumnSchema {
                name: "active".to_string(),
                typ: Type::BOOL,
                modifier: 0,
                nullable: true,
                identity: false,
            },
        ];

        // Create tuple data
        let tuple_data = vec![
            TupleData::Text(Bytes::from("123")),
            TupleData::Text(Bytes::from("test_name")),
            TupleData::Null,
        ];

        // Test conversion
        let result = CdcEventConverter::from_tuple_data_slice(&column_schemas, &tuple_data);

        // Assert the result
        assert!(result.is_ok());

        // Check the contents of the TableRow
        let table_row = result.unwrap();
        assert_eq!(table_row.values.len(), 3);

        // Check each value
        assert!(matches!(table_row.values[0], Cell::I32(123)));
        assert!(matches!(table_row.values[1], Cell::String(ref s) if s == "test_name"));
        assert!(matches!(table_row.values[2], Cell::Null));
    }
}

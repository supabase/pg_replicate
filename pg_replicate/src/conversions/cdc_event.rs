use std::{
    collections::HashMap,
    num::{ParseFloatError, ParseIntError},
    str::{from_utf8, FromStr, ParseBoolError, Utf8Error},
    string::FromUtf8Error,
};

use bigdecimal::{BigDecimal, ParseBigDecimalError};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use postgres_replication::protocol::{
    BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, RelationBody,
    ReplicationMessage, TupleData, TypeBody, UpdateBody,
};
use thiserror::Error;
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::{
    conversions::hex::from_bytea_hex,
    pipeline::batching::BatchBoundary,
    table::{ColumnSchema, TableId, TableSchema},
};

use super::{hex::ByteaHexParseError, numeric::PgNumeric, table_row::TableRow, Cell};

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

    #[error("invalid float value")]
    InvalidFloat(#[from] ParseFloatError),

    #[error("invalid numeric: {0}")]
    InvalidNumeric(#[from] ParseBigDecimalError),

    #[error("invalid uuid: {0}")]
    InvalidUuid(#[from] uuid::Error),

    #[error("invalid json: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("invalid bytea: {0}")]
    InvalidBytea(#[from] ByteaHexParseError),

    #[error("invalid timestamp: {0} ")]
    InvalidTimestamp(#[from] chrono::ParseError),

    #[error("invalid string: {0}")]
    InvalidString(#[from] FromUtf8Error),

    #[error("unsupported type: {0}")]
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
    // Make sure any changes here are also done in TableRowConverter::get_cell_value
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
                let val = match val {
                    "t" => true,
                    "f" => false,
                    _ => val.parse()?,
                };
                Ok(Cell::Bool(val))
            }
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
            Type::FLOAT4 => {
                let val = from_utf8(bytes)?;
                let val: f32 = val.parse()?;
                Ok(Cell::F32(val))
            }
            Type::FLOAT8 => {
                let val = from_utf8(bytes)?;
                let val: f64 = val.parse()?;
                Ok(Cell::F64(val))
            }
            Type::NUMERIC => {
                let val = from_utf8(bytes)?;
                let val = BigDecimal::from_str(val)?;
                let val = PgNumeric::new(Some(val));
                Ok(Cell::Numeric(val))
            }
            Type::BYTEA => {
                let val = from_utf8(bytes)?;
                let val = from_bytea_hex(val)?;
                Ok(Cell::Bytes(val))
            }
            Type::DATE => {
                let val = from_utf8(bytes)?;
                let val = NaiveDate::parse_from_str(val, "%Y-%m-%d")?;
                Ok(Cell::Date(val))
            }
            Type::TIME => {
                let val = from_utf8(bytes)?;
                let val = NaiveTime::parse_from_str(val, "%H:%M:%S%.f")?;
                Ok(Cell::Time(val))
            }
            Type::TIMESTAMP => {
                let val = from_utf8(bytes)?;
                let val = NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.f")?;
                Ok(Cell::TimeStamp(val))
            }
            Type::TIMESTAMPTZ => {
                let val = from_utf8(bytes)?;
                let val = DateTime::<FixedOffset>::parse_from_rfc3339(val)?;
                Ok(Cell::TimeStampTz(val.into()))
            }
            Type::UUID => {
                let val = from_utf8(bytes)?;
                let val = Uuid::parse_str(val)?;
                Ok(Cell::Uuid(val))
            }
            Type::JSON | Type::JSONB => {
                let val = from_utf8(bytes)?;
                let val = serde_json::from_str(val)?;
                Ok(Cell::Json(val))
            }
            Type::OID => {
                let val = from_utf8(bytes)?;
                let val: u32 = val.parse()?;
                Ok(Cell::U32(val))
            }
            #[cfg(feature = "unknown_types_to_bytes")]
            _ => {
                let s = String::from_utf8(bytes.to_vec())?;
                Ok(Cell::String(s))
            }
            #[cfg(not(feature = "unknown_types_to_bytes"))]
            _ => Err(CdcEventConversionError::UnsupportedType(
                typ.name().to_string(),
            )),
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
                LogicalReplicationMessage::Type(type_body) => Ok(CdcEvent::Type(type_body)),
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
    Type(TypeBody),
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

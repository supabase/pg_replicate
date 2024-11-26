use core::str;
use std::{
    collections::HashMap,
    num::{ParseFloatError, ParseIntError},
    str::Utf8Error,
    string::FromUtf8Error,
};

use bigdecimal::ParseBigDecimalError;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use postgres_replication::protocol::{
    BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, RelationBody,
    ReplicationMessage, TupleData, TypeBody, UpdateBody,
};
use thiserror::Error;
use tokio_postgres::types::{FromSql, Type};
use tracing::info;
use uuid::Uuid;

use crate::{
    conversions::bool::parse_bool,
    pipeline::batching::BatchBoundary,
    table::{ColumnSchema, TableId, TableSchema},
};

use super::{bool::ParseBoolError, numeric::PgNumeric, table_row::TableRow, ArrayCell, Cell};

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

    #[error("row get error: {0:?}")]
    RowGetError(#[from] Box<dyn std::error::Error + Sync + Send>),
}

pub trait FromTupleData {
    fn try_from_tuple_data(
        &self,
        typ: &Type,
        bytes: &[u8],
    ) -> Result<Cell, CdcEventConversionError>;
}

pub struct BinaryFormatConverter;

impl FromTupleData for BinaryFormatConverter {
    fn try_from_tuple_data(
        &self,
        typ: &Type,
        bytes: &[u8],
    ) -> Result<Cell, CdcEventConversionError> {
        match *typ {
            Type::BOOL => {
                let val = bool::from_sql(typ, bytes)?;
                Ok(Cell::Bool(val))
            }
            Type::BOOL_ARRAY => {
                let val = Vec::<Option<bool>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::Bool(val)))
            }
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                let val = String::from_sql(typ, bytes)?;
                Ok(Cell::String(val.to_string()))
            }
            Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY => {
                let val = Vec::<Option<String>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::String(val)))
            }
            Type::INT2 => {
                let val = i16::from_sql(typ, bytes)?;
                Ok(Cell::I16(val))
            }
            Type::INT2_ARRAY => {
                let val = Vec::<Option<i16>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::I16(val)))
            }
            Type::INT4 => {
                let val = i32::from_sql(typ, bytes)?;
                Ok(Cell::I32(val))
            }
            Type::INT4_ARRAY => {
                let val = Vec::<Option<i32>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::I32(val)))
            }
            Type::INT8 => {
                let val = i64::from_sql(typ, bytes)?;
                Ok(Cell::I64(val))
            }
            Type::INT8_ARRAY => {
                let val = Vec::<Option<i64>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::I64(val)))
            }
            Type::FLOAT4 => {
                let val = f32::from_sql(typ, bytes)?;
                Ok(Cell::F32(val))
            }
            Type::FLOAT4_ARRAY => {
                let val = Vec::<Option<f32>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::F32(val)))
            }
            Type::FLOAT8 => {
                let val = f64::from_sql(typ, bytes)?;
                Ok(Cell::F64(val))
            }
            Type::FLOAT8_ARRAY => {
                let val = Vec::<Option<f64>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::F64(val)))
            }
            Type::NUMERIC => {
                let val = PgNumeric::from_sql(typ, bytes)?;
                Ok(Cell::Numeric(val))
            }
            Type::NUMERIC_ARRAY => {
                let val = Vec::<Option<PgNumeric>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::Numeric(val)))
            }
            Type::BYTEA => {
                let val = Vec::<u8>::from_sql(typ, bytes)?;
                Ok(Cell::Bytes(val))
            }
            Type::BYTEA_ARRAY => {
                let val = Vec::<Option<Vec<u8>>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::Bytes(val)))
            }
            Type::DATE => {
                let val = NaiveDate::from_sql(typ, bytes)?;
                Ok(Cell::Date(val))
            }
            Type::DATE_ARRAY => {
                let val = Vec::<Option<NaiveDate>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::Date(val)))
            }
            Type::TIME => {
                let val = NaiveTime::from_sql(typ, bytes)?;
                Ok(Cell::Time(val))
            }
            Type::TIME_ARRAY => {
                let val = Vec::<Option<NaiveTime>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::Time(val)))
            }
            Type::TIMESTAMP => {
                let val = NaiveDateTime::from_sql(typ, bytes)?;
                Ok(Cell::TimeStamp(val))
            }
            Type::TIMESTAMP_ARRAY => {
                let val = Vec::<Option<NaiveDateTime>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::TimeStamp(val)))
            }
            Type::TIMESTAMPTZ => {
                let val = DateTime::<FixedOffset>::from_sql(typ, bytes)?;
                Ok(Cell::TimeStampTz(val.into()))
            }
            Type::TIMESTAMPTZ_ARRAY => {
                let mut val = Vec::<Option<DateTime<FixedOffset>>>::from_sql(typ, bytes)?;
                let val: Vec<Option<DateTime<Utc>>> =
                    val.drain(..).map(|v| v.map(|v| v.into())).collect();
                Ok(Cell::Array(ArrayCell::TimeStampTz(val)))
            }
            Type::UUID => {
                let val = Uuid::from_sql(typ, bytes)?;
                Ok(Cell::Uuid(val))
            }
            Type::UUID_ARRAY => {
                let val = Vec::<Option<Uuid>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::Uuid(val)))
            }
            Type::JSON | Type::JSONB => {
                let val = serde_json::Value::from_sql(typ, bytes)?;
                Ok(Cell::Json(val))
            }
            Type::JSON_ARRAY | Type::JSONB_ARRAY => {
                let val = Vec::<Option<serde_json::Value>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::Json(val)))
            }
            Type::OID => {
                let val = u32::from_sql(typ, bytes)?;
                Ok(Cell::U32(val))
            }
            Type::OID_ARRAY => {
                let val = Vec::<Option<u32>>::from_sql(typ, bytes)?;
                Ok(Cell::Array(ArrayCell::U32(val)))
            }
            #[cfg(feature = "unknown_types_to_bytes")]
            _ => {
                let val = String::from_sql(typ, bytes)?;
                Ok(Cell::String(val.to_string()))
            }
            #[cfg(not(feature = "unknown_types_to_bytes"))]
            _ => Err(CdcEventConversionError::UnsupportedType(
                typ.name().to_string(),
            )),
        }
    }
}

pub struct TextFormatConverter;

impl FromTupleData for TextFormatConverter {
    fn try_from_tuple_data(
        &self,
        typ: &Type,
        bytes: &[u8],
    ) -> Result<Cell, CdcEventConversionError> {
        let str = str::from_utf8(bytes)?;
        info!("TYP: {typ:#?}, STR: {str:#?}");
        match *typ {
            // Type::BOOL => {
            //     let val = bool::from_sql(typ, bytes)?;
            //     Ok(Cell::Bool(val))
            // }
            Type::BOOL => Ok(Cell::Bool(parse_bool(str)?)),
            // Type::BOOL_ARRAY => {
            //     let val = Vec::<Option<bool>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::Bool(val)))
            // }
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                Ok(Cell::String(str.to_string()))
            }
            // Type::CHAR_ARRAY
            // | Type::BPCHAR_ARRAY
            // | Type::VARCHAR_ARRAY
            // | Type::NAME_ARRAY
            // | Type::TEXT_ARRAY => {
            //     let val = Vec::<Option<String>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::String(val)))
            // }
            Type::INT2 => Ok(Cell::I16(str.parse()?)),
            // Type::INT2_ARRAY => {
            //     let val = Vec::<Option<i16>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::I16(val)))
            // }
            Type::INT4 => Ok(Cell::I32(str.parse()?)),
            // Type::INT4_ARRAY => {
            //     let val = Vec::<Option<i32>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::I32(val)))
            // }
            Type::INT8 => Ok(Cell::I64(str.parse()?)),
            // Type::INT8_ARRAY => {
            //     let val = Vec::<Option<i64>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::I64(val)))
            // }
            // Type::FLOAT4 => {
            //     let val = f32::from_sql(typ, bytes)?;
            //     Ok(Cell::F32(val))
            // }
            // Type::FLOAT4_ARRAY => {
            //     let val = Vec::<Option<f32>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::F32(val)))
            // }
            // Type::FLOAT8 => {
            //     let val = f64::from_sql(typ, bytes)?;
            //     Ok(Cell::F64(val))
            // }
            // Type::FLOAT8_ARRAY => {
            //     let val = Vec::<Option<f64>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::F64(val)))
            // }
            // Type::NUMERIC => {
            //     let val = PgNumeric::from_sql(typ, bytes)?;
            //     Ok(Cell::Numeric(val))
            // }
            // Type::NUMERIC_ARRAY => {
            //     let val = Vec::<Option<PgNumeric>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::Numeric(val)))
            // }
            // Type::BYTEA => {
            //     let val = Vec::<u8>::from_sql(typ, bytes)?;
            //     Ok(Cell::Bytes(val))
            // }
            // Type::BYTEA_ARRAY => {
            //     let val = Vec::<Option<Vec<u8>>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::Bytes(val)))
            // }
            // Type::DATE => {
            //     let val = NaiveDate::from_sql(typ, bytes)?;
            //     Ok(Cell::Date(val))
            // }
            // Type::DATE_ARRAY => {
            //     let val = Vec::<Option<NaiveDate>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::Date(val)))
            // }
            // Type::TIME => {
            //     let val = NaiveTime::from_sql(typ, bytes)?;
            //     Ok(Cell::Time(val))
            // }
            // Type::TIME_ARRAY => {
            //     let val = Vec::<Option<NaiveTime>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::Time(val)))
            // }
            // Type::TIMESTAMP => {
            //     let val = NaiveDateTime::from_sql(typ, bytes)?;
            //     Ok(Cell::TimeStamp(val))
            // }
            // Type::TIMESTAMP_ARRAY => {
            //     let val = Vec::<Option<NaiveDateTime>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::TimeStamp(val)))
            // }
            // Type::TIMESTAMPTZ => {
            //     let val = DateTime::<FixedOffset>::from_sql(typ, bytes)?;
            //     Ok(Cell::TimeStampTz(val.into()))
            // }
            // Type::TIMESTAMPTZ_ARRAY => {
            //     let mut val = Vec::<Option<DateTime<FixedOffset>>>::from_sql(typ, bytes)?;
            //     let val: Vec<Option<DateTime<Utc>>> =
            //         val.drain(..).map(|v| v.map(|v| v.into())).collect();
            //     Ok(Cell::Array(ArrayCell::TimeStampTz(val)))
            // }
            // Type::UUID => {
            //     let val = Uuid::from_sql(typ, bytes)?;
            //     Ok(Cell::Uuid(val))
            // }
            // Type::UUID_ARRAY => {
            //     let val = Vec::<Option<Uuid>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::Uuid(val)))
            // }
            // Type::JSON | Type::JSONB => {
            //     let val = serde_json::Value::from_sql(typ, bytes)?;
            //     Ok(Cell::Json(val))
            // }
            // Type::JSON_ARRAY | Type::JSONB_ARRAY => {
            //     let val = Vec::<Option<serde_json::Value>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::Json(val)))
            // }
            // Type::OID => {
            //     let val = u32::from_sql(typ, bytes)?;
            //     Ok(Cell::U32(val))
            // }
            // Type::OID_ARRAY => {
            //     let val = Vec::<Option<u32>>::from_sql(typ, bytes)?;
            //     Ok(Cell::Array(ArrayCell::U32(val)))
            // }
            #[cfg(feature = "unknown_types_to_bytes")]
            _ => Ok(Cell::String(str.to_string())),
            #[cfg(not(feature = "unknown_types_to_bytes"))]
            _ => Err(CdcEventConversionError::UnsupportedType(
                typ.name().to_string(),
            )),
        }
    }
}

pub struct CdcEventConverter {
    pub tuple_converter: Box<dyn FromTupleData>,
}

impl CdcEventConverter {
    fn try_from_tuple_data_slice(
        &self,
        column_schemas: &[ColumnSchema],
        tuple_data: &[TupleData],
    ) -> Result<TableRow, CdcEventConversionError> {
        let mut values = Vec::with_capacity(column_schemas.len());

        for (i, column_schema) in column_schemas.iter().enumerate() {
            let cell = match &tuple_data[i] {
                TupleData::Null => Cell::Null,
                TupleData::UnchangedToast => {
                    return Err(CdcEventConversionError::UnchangedToastNotSupported)
                }
                TupleData::Text(bytes) | TupleData::Binary(bytes) => self
                    .tuple_converter
                    .try_from_tuple_data(&column_schema.typ, &bytes[..])?,
            };
            values.push(cell);
        }

        Ok(TableRow { values })
    }

    fn try_from_insert_body(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        insert_body: InsertBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let row =
            self.try_from_tuple_data_slice(column_schemas, insert_body.tuple().tuple_data())?;

        Ok(CdcEvent::Insert((table_id, row)))
    }

    //TODO: handle when identity columns are changed
    fn try_from_update_body(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        update_body: UpdateBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let row =
            self.try_from_tuple_data_slice(column_schemas, update_body.new_tuple().tuple_data())?;

        Ok(CdcEvent::Update((table_id, row)))
    }

    fn try_from_delete_body(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        delete_body: DeleteBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let tuple = delete_body
            .key_tuple()
            .or(delete_body.old_tuple())
            .ok_or(CdcEventConversionError::MissingTupleInDeleteBody)?;

        let row = self.try_from_tuple_data_slice(column_schemas, tuple.tuple_data())?;

        Ok(CdcEvent::Delete((table_id, row)))
    }

    pub fn try_from(
        &self,
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
                    Ok(self.try_from_insert_body(table_id, column_schemas, insert_body)?)
                }
                LogicalReplicationMessage::Update(update_body) => {
                    let table_id = update_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(self.try_from_update_body(table_id, column_schemas, update_body)?)
                }
                LogicalReplicationMessage::Delete(delete_body) => {
                    let table_id = delete_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(self.try_from_delete_body(table_id, column_schemas, delete_body)?)
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

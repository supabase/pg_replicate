use std::{
    collections::HashMap,
    num::ParseIntError,
    str::{from_utf8, ParseBoolError, Utf8Error},
};

use chrono::{DateTime, NaiveDateTime, Utc};
use postgres_protocol::message::backend::{
    BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, RelationBody,
    ReplicationMessage, TupleData, UpdateBody,
};
use serde_json::{json, Map, Value};
use thiserror::Error;
use tokio_postgres::{
    binary_copy::BinaryCopyOutRow,
    types::{PgLsn, Type},
};

use crate::table::{ColumnSchema, TableId, TableSchema};

pub trait TryFromTableRow<Err> {
    type Output;

    fn try_from(
        &self,
        row: &BinaryCopyOutRow,
        column_schemas: &[ColumnSchema],
    ) -> Result<Self::Output, Err>;
}

pub trait TryFromReplicationMessage<Err> {
    type Output;

    fn try_from(
        &self,
        message: ReplicationMessage<LogicalReplicationMessage>,
        table_schemas: &HashMap<TableId, TableSchema>,
    ) -> Result<Self::Output, Err>;
}

#[derive(Debug, Error)]
pub enum JsonConversionError {
    #[error("unsupported type {0}")]
    UnsupportedType(Type),

    #[error("failed to get timestamp nanos from {0}")]
    NoTimestampNanos(DateTime<Utc>),
}

pub struct TableRowToJsonConverter;

impl TableRowToJsonConverter {
    fn get_cell_value(
        &self,
        row: &BinaryCopyOutRow,
        column_schema: &ColumnSchema,
        i: usize,
    ) -> Result<Value, JsonConversionError> {
        match column_schema.typ {
            Type::BOOL => {
                let val = row.get::<bool>(i);
                Ok(Value::Bool(val))
            }
            // Type::BYTEA => {
            //     let bytes = row.get(i);
            //     Ok(Value::Bytes(bytes))
            // }
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                let val = row.get::<&str>(i);
                Ok(Value::String(val.to_string()))
            }
            // Type::JSON | Type::JSONB => {
            //     let val = row.get::<serde_json::Value>(i);
            //     let val = json_to_cbor_value(&val);
            //     Ok(val)
            // }
            Type::INT2 => {
                let val = row.get::<i16>(i);
                Ok(Value::Number(val.into()))
            }
            Type::INT4 => {
                let val = row.get::<i32>(i);
                Ok(Value::Number(val.into()))
            }
            Type::INT8 => {
                let val = row.get::<i64>(i);
                Ok(Value::Number(val.into()))
            }
            Type::TIMESTAMP => {
                let val = row.get::<NaiveDateTime>(i);
                let utc_val = val.and_utc();
                Ok(Value::Number(
                    utc_val
                        .timestamp_nanos_opt()
                        .ok_or(JsonConversionError::NoTimestampNanos(utc_val))?
                        .into(),
                ))
            }
            ref typ => Err(JsonConversionError::UnsupportedType(typ.clone())),
        }
    }
}

impl TryFromTableRow<JsonConversionError> for TableRowToJsonConverter {
    type Output = serde_json::Value;

    fn try_from(
        &self,
        row: &BinaryCopyOutRow,
        column_schemas: &[ColumnSchema],
    ) -> Result<serde_json::Value, JsonConversionError> {
        let mut map = Map::new();

        for (i, column_schema) in column_schemas.iter().enumerate() {
            let value = self.get_cell_value(row, column_schema, i)?;
            map.insert(column_schema.name.clone(), value);
        }

        Ok(serde_json::Value::Object(map))
    }
}

#[derive(Debug, Error)]
pub enum ReplicationMsgJsonConversionError {
    // #[error("unsupported type {0}")]
    // UnsupportedType(Type),

    // #[error("failed to get timestamp nanos from {0}")]
    // NoTimestampNanos(DateTime<Utc>),
    #[error("missing unchanged toast value")]
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

pub enum CdcMessage<V> {
    Begin(V),
    Commit { lsn: PgLsn, body: V },
    Insert(V),
    Update(V),
    Delete(V),
    Relation(V),
    KeepAliveRequested { reply: bool },
}

pub struct ReplicationMsgToCdcMsgConverter;

impl ReplicationMsgToCdcMsgConverter {
    fn from_begin_body(begin: &BeginBody) -> Value {
        json!({
            "final_lsn": begin.final_lsn(),
            "timestamp": begin.timestamp(),
            "xid": begin.xid(),
        })
    }

    fn from_commit_body(commit: &CommitBody) -> Value {
        json! ({
            "commit_lsn": commit.commit_lsn(),
            "end_lsn": commit.end_lsn(),
            "timestamp": commit.timestamp(),
            "flags": commit.flags()
        })
    }

    fn from_insert_body(
        column_schemas: &[ColumnSchema],
        insert: &InsertBody,
    ) -> Result<Value, ReplicationMsgJsonConversionError> {
        let tuple = Self::from_tuple_data_slice(column_schemas, insert.tuple().tuple_data())?;

        Ok(json!({
            "table_id": insert.rel_id(),
            "tuple": tuple
        }))
    }

    fn from_update_body(
        column_schemas: &[ColumnSchema],
        update: &UpdateBody,
    ) -> Result<Value, ReplicationMsgJsonConversionError> {
        let tuple = Self::from_tuple_data_slice(column_schemas, update.new_tuple().tuple_data())?;

        Ok(json!({
            "table_id": update.rel_id(),
            "tuple": tuple
        }))
    }

    fn from_delete_body(
        column_schemas: &[ColumnSchema],
        delete: &DeleteBody,
    ) -> Result<Value, ReplicationMsgJsonConversionError> {
        let tuple = delete
            .key_tuple()
            .or(delete.old_tuple())
            .ok_or(ReplicationMsgJsonConversionError::MissingTupleInDeleteBody)?;

        let tuple = Self::from_tuple_data_slice(column_schemas, tuple.tuple_data())?;

        Ok(json!({
            "table_id": delete.rel_id(),
            "tuple": tuple
        }))
    }

    fn from_tuple_data_slice(
        column_schemas: &[ColumnSchema],
        tuple_data: &[TupleData],
    ) -> Result<Value, ReplicationMsgJsonConversionError> {
        let mut map = Map::with_capacity(column_schemas.len());

        for (i, column_schema) in column_schemas.iter().enumerate() {
            let val = Self::from_tuple_data(&column_schema.typ, &tuple_data[i])?;
            map.insert(column_schema.name.clone(), val);
        }

        Ok(Value::Object(map))
    }

    fn from_tuple_data(
        typ: &Type,
        val: &TupleData,
    ) -> Result<Value, ReplicationMsgJsonConversionError> {
        let bytes = match val {
            TupleData::Null => {
                return Ok(Value::Null);
            }
            TupleData::UnchangedToast => {
                return Err(ReplicationMsgJsonConversionError::UnchangedToastNotSupported)
            }
            TupleData::Text(bytes) => &bytes[..],
        };
        match *typ {
            Type::BOOL => {
                let val = from_utf8(bytes)?;
                let val: bool = val.parse()?;
                Ok(Value::Bool(val))
            }
            // Type::BYTEA => Ok(Value::Bytes(bytes.to_vec())),
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                let val = from_utf8(bytes)?;
                Ok(Value::String(val.to_string()))
            }
            Type::INT2 => {
                let val = from_utf8(bytes)?;
                let val: i16 = val.parse()?;
                Ok(Value::Number(val.into()))
            }
            Type::INT4 => {
                let val = from_utf8(bytes)?;
                let val: i32 = val.parse()?;
                Ok(Value::Number(val.into()))
            }
            Type::INT8 => {
                let val = from_utf8(bytes)?;
                let val: i64 = val.parse()?;
                Ok(Value::Number(val.into()))
            }
            Type::TIMESTAMP => {
                let val = from_utf8(bytes)?;
                let val = NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.f")?;
                Ok(Value::Number(
                    val.and_utc()
                        .timestamp_nanos_opt()
                        .ok_or(ReplicationMsgJsonConversionError::OutOfRangeTimestamp)?
                        .into(),
                ))
            }
            ref typ => Err(ReplicationMsgJsonConversionError::UnsupportedType(
                typ.to_string(),
            )),
        }
    }

    fn from_relation_body(
        relation: &RelationBody,
    ) -> Result<Value, ReplicationMsgJsonConversionError> {
        let schema = relation
            .namespace()
            .map_err(|e| ReplicationMsgJsonConversionError::InvalidNamespace(e.to_string()))?;
        let table = relation
            .name()
            .map_err(|e| ReplicationMsgJsonConversionError::InvalidRelationName(e.to_string()))?;

        let mut cols = vec![];
        for col in relation.columns() {
            let name = col
                .name()
                .map_err(|e| ReplicationMsgJsonConversionError::InvalidColumnName(e.to_string()))?;
            let mut map = Map::new();
            map.insert("name".to_string(), Value::String(name.to_string()));
            map.insert("identity".to_string(), Value::Bool(col.flags() == 1));
            map.insert("type_id".to_string(), Value::Number(col.type_id().into()));
            map.insert(
                "type_modifier".to_string(),
                Value::Number(col.type_modifier().into()),
            );

            cols.push(Value::Object(map));
        }

        let mut map = Map::new();

        map.insert("schema".to_string(), Value::String(schema.to_string()));
        map.insert("table".to_string(), Value::String(table.to_string()));
        map.insert("columns".to_string(), Value::Array(cols));

        Ok(Value::Object(map))
    }
}

impl TryFromReplicationMessage<ReplicationMsgJsonConversionError>
    for ReplicationMsgToCdcMsgConverter
{
    type Output = CdcMessage<serde_json::Value>;

    fn try_from(
        &self,
        message: ReplicationMessage<LogicalReplicationMessage>,
        table_schemas: &HashMap<TableId, TableSchema>,
    ) -> Result<Self::Output, ReplicationMsgJsonConversionError> {
        Ok(match message {
            ReplicationMessage::XLogData(xlog_data) => match xlog_data.into_data() {
                LogicalReplicationMessage::Begin(begin_body) => {
                    CdcMessage::Begin(Self::from_begin_body(&begin_body))
                }
                LogicalReplicationMessage::Commit(commit_body) => CdcMessage::Commit {
                    lsn: PgLsn::from(commit_body.end_lsn()),
                    body: Self::from_commit_body(&commit_body),
                },
                LogicalReplicationMessage::Origin(_) => {
                    panic!("LogicalReplication::Origin not yet supported")
                }
                LogicalReplicationMessage::Relation(relation_body) => {
                    CdcMessage::Relation(Self::from_relation_body(&relation_body)?)
                }
                LogicalReplicationMessage::Type(_) => {
                    panic!("LogicalReplication::Type not yet supported")
                }
                LogicalReplicationMessage::Insert(insert_body) => {
                    let table_id = insert_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(ReplicationMsgJsonConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    CdcMessage::Insert(Self::from_insert_body(column_schemas, &insert_body)?)
                }
                LogicalReplicationMessage::Update(update_body) => {
                    let table_id = update_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(ReplicationMsgJsonConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    CdcMessage::Update(Self::from_update_body(column_schemas, &update_body)?)
                }
                LogicalReplicationMessage::Delete(delete_body) => {
                    let table_id = delete_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(ReplicationMsgJsonConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    CdcMessage::Delete(Self::from_delete_body(column_schemas, &delete_body)?)
                }
                LogicalReplicationMessage::Truncate(_) => {
                    panic!("LogicalReplication::Truncate not yet supported")
                }
                _ => panic!("unknown LogicalReplicationMessage type"),
            },
            ReplicationMessage::PrimaryKeepAlive(keep_alive_body) => {
                CdcMessage::KeepAliveRequested {
                    reply: keep_alive_body.reply() == 1,
                }
            }
            _ => panic!("unknown ReplicationMessage type"),
        })
    }
}

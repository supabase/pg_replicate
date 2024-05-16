use std::collections::HashMap;

use chrono::{DateTime, NaiveDateTime, Utc};
use thiserror::Error;
use tokio_postgres::{binary_copy::BinaryCopyOutRow, types::Type};

use crate::table::ColumnSchema;

use super::TryFromTableRow;

pub enum Cell {
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    I64(i64),
    TimeStamp(i64),
}

pub struct TableRow {
    pub values: HashMap<String, Cell>,
}

#[derive(Debug, Error)]
pub enum TableRowError {
    #[error("unsupported type {0}")]
    UnsupportedType(Type),

    #[error("failed to get timestamp nanos from {0}")]
    NoTimestampNanos(DateTime<Utc>),
}

pub struct TableRowConverter;

impl TableRowConverter {
    fn get_cell_value(
        &self,
        row: &BinaryCopyOutRow,
        column_schema: &ColumnSchema,
        i: usize,
    ) -> Result<Cell, TableRowError> {
        match column_schema.typ {
            Type::BOOL => {
                let val = row.get::<bool>(i);
                Ok(Cell::Bool(val))
            }
            // Type::BYTEA => {
            //     let bytes = row.get(i);
            //     Ok(Value::Bytes(bytes))
            // }
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                let val = row.get::<&str>(i);
                Ok(Cell::String(val.to_string()))
            }
            // Type::JSON | Type::JSONB => {
            //     let val = row.get::<serde_json::Value>(i);
            //     let val = json_to_cbor_value(&val);
            //     Ok(val)
            // }
            Type::INT2 => {
                let val = row.get::<i16>(i);
                Ok(Cell::I16(val))
            }
            Type::INT4 => {
                let val = row.get::<i32>(i);
                Ok(Cell::I32(val))
            }
            Type::INT8 => {
                let val = row.get::<i64>(i);
                Ok(Cell::I64(val))
            }
            Type::TIMESTAMP => {
                let val = row.get::<NaiveDateTime>(i);
                let utc_val = val.and_utc();
                Ok(Cell::TimeStamp(
                    utc_val
                        .timestamp_nanos_opt()
                        .ok_or(TableRowError::NoTimestampNanos(utc_val))?,
                ))
            }
            ref typ => Err(TableRowError::UnsupportedType(typ.clone())),
        }
    }
}

impl TryFromTableRow<TableRowError> for TableRowConverter {
    type Output = TableRow;

    fn try_from(
        &self,
        row: &tokio_postgres::binary_copy::BinaryCopyOutRow,
        column_schemas: &[crate::table::ColumnSchema],
    ) -> Result<Self::Output, TableRowError> {
        let mut values = HashMap::with_capacity(column_schemas.len());
        for (i, column_schema) in column_schemas.iter().enumerate() {
            let value = self.get_cell_value(row, column_schema, i)?;
            values.insert(column_schema.name.clone(), value);
        }

        Ok(TableRow { values })
    }
}

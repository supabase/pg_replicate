use chrono::{DateTime, NaiveDateTime, Utc};
use thiserror::Error;
use tokio_postgres::{binary_copy::BinaryCopyOutRow, types::Type};

use crate::{pipeline::batching::BatchBoundary, table::ColumnSchema};

#[derive(Debug)]
pub enum Cell {
    Null,
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    I64(i64),
    TimeStamp(String),
}

#[derive(Debug)]
pub struct TableRow {
    pub values: Vec<Cell>,
}

impl BatchBoundary for TableRow {
    fn is_last_in_batch(&self) -> bool {
        true
    }
}

#[derive(Debug, Error)]
pub enum TableRowConversionError {
    #[error("unsupported type {0}")]
    UnsupportedType(Type),

    #[error("failed to get timestamp nanos from {0}")]
    NoTimestampNanos(DateTime<Utc>),
}

pub struct TableRowConverter;

impl TableRowConverter {
    fn get_cell_value(
        row: &BinaryCopyOutRow,
        column_schema: &ColumnSchema,
        i: usize,
    ) -> Result<Cell, TableRowConversionError> {
        match column_schema.typ {
            Type::BOOL => {
                let val = if column_schema.nullable {
                    match row.try_get::<bool>(i) {
                        Ok(b) => Cell::Bool(b),
                        //TODO: Only return null if the error is WasNull from tokio_postgres crate
                        Err(_) => Cell::Null,
                    }
                } else {
                    let val = row.get::<bool>(i);
                    Cell::Bool(val)
                };
                Ok(val)
            }
            // Type::BYTEA => {
            //     let bytes = row.get(i);
            //     Ok(Value::Bytes(bytes))
            // }
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                let val = if column_schema.nullable {
                    match row.try_get::<&str>(i) {
                        Ok(s) => Cell::String(s.to_string()),
                        //TODO: Only return null if the error is WasNull from tokio_postgres crate
                        Err(_) => Cell::Null,
                    }
                } else {
                    let val = row.get::<&str>(i);
                    Cell::String(val.to_string())
                };
                Ok(val)
            }
            // Type::JSON | Type::JSONB => {
            //     let val = row.get::<serde_json::Value>(i);
            //     let val = json_to_cbor_value(&val);
            //     Ok(val)
            // }
            Type::INT2 => {
                let val = if column_schema.nullable {
                    match row.try_get::<i16>(i) {
                        Ok(i) => Cell::I16(i),
                        Err(_) => {
                            //TODO: Only return null if the error is WasNull from tokio_postgres crate
                            Cell::Null
                        }
                    }
                } else {
                    let val = row.get::<i16>(i);
                    Cell::I16(val)
                };
                Ok(val)
            }
            Type::INT4 => {
                let val = if column_schema.nullable {
                    match row.try_get::<i32>(i) {
                        Ok(i) => Cell::I32(i),
                        Err(_) => {
                            //TODO: Only return null if the error is WasNull from tokio_postgres crate
                            Cell::Null
                        }
                    }
                } else {
                    let val = row.get::<i32>(i);
                    Cell::I32(val)
                };
                Ok(val)
            }
            Type::INT8 => {
                let val = if column_schema.nullable {
                    match row.try_get::<i64>(i) {
                        Ok(i) => Cell::I64(i),
                        Err(_) => {
                            //TODO: Only return null if the error is WasNull from tokio_postgres crate
                            Cell::Null
                        }
                    }
                } else {
                    let val = row.get::<i64>(i);
                    Cell::I64(val)
                };
                Ok(val)
            }
            Type::TIMESTAMP => {
                let val = if column_schema.nullable {
                    match row.try_get::<NaiveDateTime>(i) {
                        Ok(s) => {
                            let s = s.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                            Cell::TimeStamp(s.to_string())
                        }
                        Err(_) => {
                            //TODO: Only return null if the error is WasNull from tokio_postgres crate
                            Cell::Null
                        }
                    }
                } else {
                    let val = row.get::<NaiveDateTime>(i);
                    let val = val.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                    Cell::TimeStamp(val)
                };
                Ok(val)
            }
            ref typ => Err(TableRowConversionError::UnsupportedType(typ.clone())),
        }
    }

    pub fn try_from(
        row: &tokio_postgres::binary_copy::BinaryCopyOutRow,
        column_schemas: &[crate::table::ColumnSchema],
    ) -> Result<TableRow, TableRowConversionError> {
        let mut values = Vec::with_capacity(column_schemas.len());
        for (i, column_schema) in column_schemas.iter().enumerate() {
            let value = Self::get_cell_value(row, column_schema, i)?;
            values.push(value);
        }

        Ok(TableRow { values })
    }
}

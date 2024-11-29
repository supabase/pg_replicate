use core::str;
use std::str::Utf8Error;

use thiserror::Error;
use tokio_postgres::types::Type;
use tracing::{error, info};

use crate::{conversions::text::TextFormatConverter, pipeline::batching::BatchBoundary};

use super::{text::FromTextError, Cell};

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

    #[error("invalid string: {0}")]
    InvalidString(#[from] Utf8Error),

    #[error("mismatch in num of columns in schema and row")]
    NumColsMismatch,

    #[error("invalid value: {0}")]
    InvalidValue(#[from] FromTextError),
}

pub struct TableRowConverter;

impl TableRowConverter {
    pub fn try_from(
        row: &[u8],
        column_schemas: &[crate::table::ColumnSchema],
    ) -> Result<TableRow, TableRowConversionError> {
        let mut values = Vec::with_capacity(column_schemas.len());

        let row_str = str::from_utf8(row)?;
        info!("ROW: {row_str:#?}");
        let mut column_schemas_iter = column_schemas.iter();
        let mut chars = row_str.chars();
        let mut val_str = String::with_capacity(10);
        let mut in_escape = false;
        let mut done = false;

        while !done {
            loop {
                match chars.next() {
                    Some(c) => match c {
                        c if in_escape => {
                            val_str.push(c);
                            in_escape = false;
                        }
                        '\t' => {
                            break;
                        }
                        '\\' => in_escape = true,
                        c => {
                            val_str.push(c);
                        }
                    },
                    None => {
                        done = true;
                        break;
                    }
                }
            }

            let Some(column_schema) = column_schemas_iter.next() else {
                return Err(TableRowConversionError::NumColsMismatch);
            };

            info!("VALUE STR: {val_str}");
            let value = TextFormatConverter::try_from_str(&column_schema.typ, &val_str)?;
            info!("GOT VALUE: {value:#?}");
            values.push(value);
            val_str.clear();
        }

        Ok(TableRow { values })
    }
}

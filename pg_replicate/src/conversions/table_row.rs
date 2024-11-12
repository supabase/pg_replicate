use std::string::FromUtf8Error;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
#[cfg(feature = "unknown_types_to_bytes")]
use postgres_protocol::types;
use thiserror::Error;
#[cfg(feature = "unknown_types_to_bytes")]
use tokio_postgres::types::FromSql;
use tokio_postgres::{
    binary_copy::BinaryCopyOutRow,
    error::Error,
    types::{Type, WasNull},
};
use uuid::Uuid;

use crate::{pipeline::batching::BatchBoundary, table::ColumnSchema};

use super::{numeric::PgNumeric, ArrayCell, Cell};

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
    InvalidString(#[from] FromUtf8Error),

    #[error("row get error: {0:?}")]
    RowGetError(Option<Box<dyn std::error::Error + Sync + Send>>),
}

pub struct TableRowConverter;

/// A wrapper type over Vec<u8> to help implement the FromSql trait.
/// The wrapper is needed to avoid Rust's trait coherence rules. i.e.
/// one of the trait or the implementing type must be part of the
/// current crate.
///
/// This type is useful in retriveing bytes from the Postgres wire
/// protocol for the fallback case of unsupported type.
#[cfg(feature = "unknown_types_to_bytes")]
struct VecWrapper(Vec<u8>);

#[cfg(feature = "unknown_types_to_bytes")]
impl<'a> FromSql<'a> for VecWrapper {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> Result<VecWrapper, Box<dyn std::error::Error + Sync + Send>> {
        let v = types::bytea_from_sql(raw).to_owned();
        Ok(VecWrapper(v))
    }

    /// Because of the fallback nature of this impl, we accept all types here
    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(VecWrapper(vec![]))
    }

    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match raw {
            Some(raw) => Self::from_sql(ty, raw),
            None => Self::from_sql_null(ty),
        }
    }
}

impl TableRowConverter {
    // Make sure any changes here are also done in CdcEventConverter::from_tuple_data
    fn get_cell_value(
        row: &BinaryCopyOutRow,
        column_schema: &ColumnSchema,
        i: usize,
    ) -> Result<Cell, TableRowConversionError> {
        match column_schema.typ {
            Type::BOOL => Self::get_from_row(row, i, column_schema.nullable, |val: bool| {
                Ok(Cell::Bool(val))
            }),
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                Self::get_from_row(row, i, column_schema.nullable, |val: &str| {
                    Ok(Cell::String(val.to_string()))
                })
            }
            Type::INT2 => Self::get_from_row(row, i, column_schema.nullable, |val: i16| {
                Ok(Cell::I16(val))
            }),
            Type::INT4 => Self::get_from_row(row, i, column_schema.nullable, |val: i32| {
                Ok(Cell::I32(val))
            }),
            Type::INT8 => Self::get_from_row(row, i, column_schema.nullable, |val: i64| {
                Ok(Cell::I64(val))
            }),
            Type::FLOAT4 => Self::get_from_row(row, i, column_schema.nullable, |val: f32| {
                Ok(Cell::F32(val))
            }),
            Type::FLOAT8 => Self::get_from_row(row, i, column_schema.nullable, |val: f64| {
                Ok(Cell::F64(val))
            }),
            Type::NUMERIC => {
                Self::get_from_row(row, i, column_schema.nullable, |val: PgNumeric| {
                    Ok(Cell::Numeric(val))
                })
            }
            Type::BYTEA => Self::get_from_row(row, i, column_schema.nullable, |val: Vec<u8>| {
                Ok(Cell::Bytes(val))
            }),
            Type::DATE => Self::get_from_row(row, i, column_schema.nullable, |val: NaiveDate| {
                Ok(Cell::Date(val))
            }),
            Type::TIME => Self::get_from_row(row, i, column_schema.nullable, |val: NaiveTime| {
                Ok(Cell::Time(val))
            }),
            Type::TIMESTAMP => {
                Self::get_from_row(row, i, column_schema.nullable, |val: NaiveDateTime| {
                    Ok(Cell::TimeStamp(val))
                })
            }
            Type::TIMESTAMPTZ => Self::get_from_row(
                row,
                i,
                column_schema.nullable,
                |val: DateTime<FixedOffset>| Ok(Cell::TimeStampTz(val.into())),
            ),
            Type::UUID => Self::get_from_row(row, i, column_schema.nullable, |val: Uuid| {
                Ok(Cell::Uuid(val))
            }),
            Type::JSON | Type::JSONB => {
                Self::get_from_row(row, i, column_schema.nullable, |val: serde_json::Value| {
                    Ok(Cell::Json(val))
                })
            }
            Type::OID => Self::get_from_row(row, i, column_schema.nullable, |val: u32| {
                Ok(Cell::U32(val))
            }),
            Type::BOOL_ARRAY => {
                Self::get_from_row(row, i, column_schema.nullable, |val: Vec<Option<bool>>| {
                    Ok(Cell::Array(ArrayCell::Bool(val)))
                })
            }
            Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY => Self::get_from_row(
                row,
                i,
                column_schema.nullable,
                |val: Vec<Option<String>>| Ok(Cell::Array(ArrayCell::String(val))),
            ),
            Type::INT2_ARRAY => {
                Self::get_from_row(row, i, column_schema.nullable, |val: Vec<Option<i16>>| {
                    Ok(Cell::Array(ArrayCell::I16(val)))
                })
            }
            Type::INT4_ARRAY => {
                Self::get_from_row(row, i, column_schema.nullable, |val: Vec<Option<i32>>| {
                    Ok(Cell::Array(ArrayCell::I32(val)))
                })
            }
            Type::INT8_ARRAY => {
                Self::get_from_row(row, i, column_schema.nullable, |val: Vec<Option<i64>>| {
                    Ok(Cell::Array(ArrayCell::I64(val)))
                })
            }
            Type::FLOAT4_ARRAY => {
                Self::get_from_row(row, i, column_schema.nullable, |val: Vec<Option<f32>>| {
                    Ok(Cell::Array(ArrayCell::F32(val)))
                })
            }
            Type::FLOAT8_ARRAY => {
                Self::get_from_row(row, i, column_schema.nullable, |val: Vec<Option<f64>>| {
                    Ok(Cell::Array(ArrayCell::F64(val)))
                })
            }
            Type::NUMERIC_ARRAY => Self::get_from_row(
                row,
                i,
                column_schema.nullable,
                |val: Vec<Option<PgNumeric>>| Ok(Cell::Array(ArrayCell::Numeric(val))),
            ),
            Type::BYTEA_ARRAY => Self::get_from_row(
                row,
                i,
                column_schema.nullable,
                |val: Vec<Option<Vec<u8>>>| Ok(Cell::Array(ArrayCell::Bytes(val))),
            ),
            Type::DATE_ARRAY => Self::get_from_row(
                row,
                i,
                column_schema.nullable,
                |val: Vec<Option<NaiveDate>>| Ok(Cell::Array(ArrayCell::Date(val))),
            ),
            Type::TIME_ARRAY => Self::get_from_row(
                row,
                i,
                column_schema.nullable,
                |val: Vec<Option<NaiveTime>>| Ok(Cell::Array(ArrayCell::Time(val))),
            ),
            Type::TIMESTAMP_ARRAY => Self::get_from_row(
                row,
                i,
                column_schema.nullable,
                |val: Vec<Option<NaiveDateTime>>| Ok(Cell::Array(ArrayCell::TimeStamp(val))),
            ),
            Type::TIMESTAMPTZ_ARRAY => Self::get_from_row(
                row,
                i,
                column_schema.nullable,
                |mut val: Vec<Option<DateTime<FixedOffset>>>| {
                    let val: Vec<Option<DateTime<Utc>>> =
                        val.drain(..).map(|v| v.map(|v| v.into())).collect();
                    Ok(Cell::Array(ArrayCell::TimeStampTz(val)))
                },
            ),
            Type::UUID_ARRAY => {
                Self::get_from_row(row, i, column_schema.nullable, |val: Vec<Option<Uuid>>| {
                    Ok(Cell::Array(ArrayCell::Uuid(val)))
                })
            }
            Type::JSON_ARRAY | Type::JSONB_ARRAY => Self::get_from_row(
                row,
                i,
                column_schema.nullable,
                |val: Vec<Option<serde_json::Value>>| Ok(Cell::Array(ArrayCell::Json(val))),
            ),
            Type::OID_ARRAY => {
                Self::get_from_row(row, i, column_schema.nullable, |val: Vec<Option<u32>>| {
                    Ok(Cell::Array(ArrayCell::U32(val)))
                })
            }
            #[cfg(not(feature = "unknown_types_to_bytes"))]
            ref t => Err(TableRowConversionError::UnsupportedType(t.clone())),
            #[cfg(feature = "unknown_types_to_bytes")]
            _ => Self::get_from_row(row, i, column_schema.nullable, |val: VecWrapper| {
                let s = String::from_utf8(val.0)?;
                Ok(Cell::String(s))
            }),
        }
    }

    fn get_from_row<'a, T: FromSql<'a>, F>(
        row: &'a BinaryCopyOutRow,
        i: usize,
        nullable: bool,
        f: F,
    ) -> Result<Cell, TableRowConversionError>
    where
        F: FnOnce(T) -> Result<Cell, TableRowConversionError>,
    {
        match row.try_get::<T>(i) {
            Ok(val) => Ok(f(val)?),
            Err(e) => Self::error_or_null(e, nullable),
        }
    }

    fn error_or_null(e: Error, nullable: bool) -> Result<Cell, TableRowConversionError> {
        let source_error = e.into_source();
        let was_null = source_error
            .as_ref()
            .and_then(|e| e.downcast_ref::<WasNull>());
        if was_null.is_some() && nullable {
            Ok(Cell::Null)
        } else {
            Err(TableRowConversionError::RowGetError(source_error))
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

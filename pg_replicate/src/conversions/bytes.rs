use core::str;
use std::{
    num::{ParseFloatError, ParseIntError},
    str::Utf8Error,
    string::FromUtf8Error,
};

use bigdecimal::ParseBigDecimalError;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use thiserror::Error;
use tokio_postgres::types::{FromSql, Type};
use uuid::Uuid;

use crate::conversions::{bool::parse_bool, hex};

use super::{bool::ParseBoolError, hex::ByteaHexParseError, numeric::PgNumeric, ArrayCell, Cell};

#[derive(Debug, Error)]
pub enum FromBytesError {
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

    #[error("invalid bytea: {0}")]
    InvalidBytea(#[from] ByteaHexParseError),

    #[error("invalid uuid: {0}")]
    InvalidUuid(#[from] uuid::Error),

    #[error("invalid json: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("invalid timestamp: {0} ")]
    InvalidTimestamp(#[from] chrono::ParseError),

    #[error("invalid string: {0}")]
    InvalidString(#[from] FromUtf8Error),

    #[error("invalid array: {0}")]
    InvalidArray(#[from] ArrayParseError),

    #[error("row get error: {0:?}")]
    RowGetError(#[from] Box<dyn std::error::Error + Sync + Send>),
}

pub trait FromBytes {
    fn try_from_tuple_data(&self, typ: &Type, bytes: &[u8]) -> Result<Cell, FromBytesError>;
}

pub struct BinaryFormatConverter;

impl FromBytes for BinaryFormatConverter {
    fn try_from_tuple_data(&self, typ: &Type, bytes: &[u8]) -> Result<Cell, FromBytesError> {
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

#[derive(Debug, Error)]
pub enum ArrayParseError {
    #[error("input too short")]
    InputTooShort,

    #[error("missing brances")]
    MissingBraces,
}

impl TextFormatConverter {
    fn parse_array<P, M, T>(str: &str, mut parse: P, m: M) -> Result<Cell, FromBytesError>
    where
        P: FnMut(&str) -> Result<Option<T>, FromBytesError>,
        M: FnOnce(Vec<Option<T>>) -> ArrayCell,
    {
        if str.len() < 2 {
            return Err(ArrayParseError::InputTooShort.into());
        }

        if !str.starts_with('{') || !str.ends_with('}') {
            return Err(ArrayParseError::MissingBraces.into());
        }

        let mut res = vec![];
        let str = &str[1..(str.len() - 1)];
        let mut val_str = String::with_capacity(10);
        let mut in_quotes = false;
        let mut in_escape = false;
        let mut chars = str.chars();
        let mut done = str.is_empty();

        while !done {
            loop {
                match chars.next() {
                    Some(c) => match c {
                        c if in_escape => {
                            val_str.push(c);
                            in_escape = false;
                        }
                        '"' => in_quotes = !in_quotes,
                        '\\' => in_escape = true,
                        ',' if !in_quotes => {
                            break;
                        }
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
            let val = if val_str.to_lowercase() == "null" {
                None
            } else {
                parse(&val_str)?
            };
            res.push(val);
            val_str.clear();
        }

        Ok(Cell::Array(m(res)))
    }
}

impl FromBytes for TextFormatConverter {
    fn try_from_tuple_data(&self, typ: &Type, bytes: &[u8]) -> Result<Cell, FromBytesError> {
        let str = str::from_utf8(bytes)?;
        match *typ {
            Type::BOOL => Ok(Cell::Bool(parse_bool(str)?)),
            Type::BOOL_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(parse_bool(str)?)),
                ArrayCell::Bool,
            ),
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                Ok(Cell::String(str.to_string()))
            }
            Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(str.to_string())),
                ArrayCell::String,
            ),
            Type::INT2 => Ok(Cell::I16(str.parse()?)),
            Type::INT2_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I16)
            }
            Type::INT4 => Ok(Cell::I32(str.parse()?)),
            Type::INT4_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I32)
            }
            Type::INT8 => Ok(Cell::I64(str.parse()?)),
            Type::INT8_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I64)
            }
            Type::FLOAT4 => Ok(Cell::F32(str.parse()?)),
            Type::FLOAT4_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::F32)
            }
            Type::FLOAT8 => Ok(Cell::F64(str.parse()?)),
            Type::FLOAT8_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::F64)
            }
            Type::NUMERIC => Ok(Cell::Numeric(str.parse()?)),
            Type::NUMERIC_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(str.parse()?)),
                ArrayCell::Numeric,
            ),
            Type::BYTEA => Ok(Cell::Bytes(hex::from_bytea_hex(str)?)),
            Type::BYTEA_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(hex::from_bytea_hex(str)?)),
                ArrayCell::Bytes,
            ),
            Type::DATE => {
                let val = NaiveDate::parse_from_str(str, "%Y-%m-%d")?;
                Ok(Cell::Date(val))
            }
            Type::DATE_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(NaiveDate::parse_from_str(str, "%Y-%m-%d")?)),
                ArrayCell::Date,
            ),
            Type::TIME => {
                let val = NaiveTime::parse_from_str(str, "%H:%M:%S%.f")?;
                Ok(Cell::Time(val))
            }
            Type::TIME_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(NaiveTime::parse_from_str(str, "%H:%M:%S%.f")?)),
                ArrayCell::Time,
            ),
            Type::TIMESTAMP => {
                let val = NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f")?;
                Ok(Cell::TimeStamp(val))
            }
            Type::TIMESTAMP_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| {
                    Ok(Some(NaiveDateTime::parse_from_str(
                        str,
                        "%Y-%m-%d %H:%M:%S%.f",
                    )?))
                },
                ArrayCell::TimeStamp,
            ),
            Type::TIMESTAMPTZ => {
                let val = DateTime::<FixedOffset>::parse_from_rfc3339(str)?;
                Ok(Cell::TimeStampTz(val.into()))
            }
            Type::TIMESTAMPTZ_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| {
                    Ok(Some(
                        DateTime::<FixedOffset>::parse_from_rfc3339(str)?.into(),
                    ))
                },
                ArrayCell::TimeStampTz,
            ),
            Type::UUID => {
                let val = Uuid::parse_str(str)?;
                Ok(Cell::Uuid(val))
            }
            Type::UUID_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(Uuid::parse_str(str)?)),
                ArrayCell::Uuid,
            ),
            Type::JSON | Type::JSONB => {
                let val = serde_json::from_str(str)?;
                Ok(Cell::Json(val))
            }
            Type::JSON_ARRAY | Type::JSONB_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(serde_json::from_str(str)?)),
                ArrayCell::Json,
            ),
            Type::OID => {
                let val: u32 = str.parse()?;
                Ok(Cell::U32(val))
            }
            Type::OID_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::U32)
            }
            #[cfg(feature = "unknown_types_to_bytes")]
            _ => Ok(Cell::String(str.to_string())),
            #[cfg(not(feature = "unknown_types_to_bytes"))]
            _ => Err(CdcEventConversionError::UnsupportedType(
                typ.name().to_string(),
            )),
        }
    }
}

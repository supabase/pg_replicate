use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use numeric::PgNumeric;

pub mod cdc_event;
pub mod hex;
pub mod numeric;
pub mod table_row;

#[derive(Debug)]
pub enum Cell {
    Null,
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Numeric(PgNumeric),
    Date(NaiveDate),
    TimeStamp(NaiveDateTime),
    TimeStampTz(DateTime<Utc>),
    Bytes(Vec<u8>),
}

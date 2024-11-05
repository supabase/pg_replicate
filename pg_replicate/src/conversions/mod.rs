use chrono::{DateTime, NaiveDateTime, Utc};

pub mod cdc_event;
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
    TimeStamp(NaiveDateTime),
    TimeStampTz(DateTime<Utc>),
    Bytes(Vec<u8>),
}

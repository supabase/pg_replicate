use chrono::Timelike;
use chrono::{NaiveDate, NaiveTime, Utc};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use deltalake::datafusion::execution::context::SessionContext;
use deltalake::datafusion::prelude::col;
use deltalake::open_table;
use deltalake::operations::create::CreateBuilder;
use deltalake::{kernel::DataType, DeltaOps, DeltaTableError};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::types::{PgLsn, Type};

use crate::{
    conversions::{table_row::TableRow, Cell},
    table::{ColumnSchema, TableId, TableName, TableSchema},
};
use deltalake::arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
    RecordBatch as DeltaRecordBatch, StringArray, TimestampMicrosecondArray, UInt32Array,
};

pub struct DeltaClient {
    pub path: String,
    pub table_schemas: Option<HashMap<TableId, TableSchema>>,
    pub delta_schemas: Option<HashMap<String, Arc<Schema>>>,
}

impl DeltaClient {
    fn naive_date_to_arrow(date: NaiveDate) -> i32 {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (date - epoch).num_days() as i32
    }

    fn postgres_to_delta(typ: &Type) -> DataType {
        match typ {
            &Type::BOOL => DataType::BOOLEAN,
            &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => {
                DataType::STRING
            }
            &Type::INT2 | &Type::INT4 | &Type::INT8 => DataType::INTEGER,
            &Type::FLOAT4 | &Type::FLOAT8 | &Type::NUMERIC => DataType::FLOAT,
            &Type::DATE => DataType::DATE,
            &Type::TIME | &Type::TIMESTAMP | &Type::TIMESTAMPTZ => DataType::TIMESTAMP,
            &Type::UUID => DataType::STRING,
            &Type::OID => DataType::INTEGER,
            &Type::BYTEA => DataType::BYTE,
            _ => DataType::STRING,
        }
    }

    fn postgres_to_arrow(typ: &Type) -> ArrowDataType {
        match typ {
            &Type::BOOL => ArrowDataType::Boolean,
            &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => {
                ArrowDataType::Utf8
            }
            &Type::INT2 | &Type::INT4 | &Type::INT8 => ArrowDataType::Int32,
            &Type::FLOAT4 | &Type::FLOAT8 | &Type::NUMERIC => ArrowDataType::Float32,
            &Type::DATE => ArrowDataType::Date32,
            &Type::TIME | &Type::TIMESTAMP | &Type::TIMESTAMPTZ => {
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
            }
            &Type::UUID => ArrowDataType::Utf8,
            &Type::OID => ArrowDataType::Int32,
            &Type::BYTEA => ArrowDataType::Binary,
            _ => ArrowDataType::Utf8,
        }
    }

    pub async fn delta_table_exists(&self, table_name: &str) -> bool {
        let uri = self.delta_full_path(table_name);
        open_table(uri).await.is_ok()
    }

    pub async fn set_last_lsn(&self, lsn: PgLsn) -> Result<(), DeltaTableError> {
        let uri = self.delta_full_path("last_lsn");

        let lsn_value: u64 = lsn.into();

        let ctx = SessionContext::new();

        let id_array = Arc::new(Int32Array::from(vec![1])); // ID column
        let lsn_array = Arc::new(Int32Array::from(vec![lsn_value as i32])); // LSN column
        let operation: Arc<dyn Array> = Arc::new(StringArray::from(vec!["I"]));
        let inserted_at: Arc<dyn Array> = Arc::new(TimestampMicrosecondArray::from(vec![
            Utc::now().timestamp_micros(),
        ]));

        let schema = Self::get_lsn_schema().await?;
        let batch =
            DeltaRecordBatch::try_new(schema, vec![id_array, lsn_array, operation, inserted_at])?;
        let source = ctx.read_batch(batch)?;

        let table = open_table(uri).await?;
        DeltaOps(table)
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|update| {
                update.update("lsn", col("source.lsn")).update(
                    "pg_replicate_inserted_time",
                    col("source.pg_replicate_inserted_time"),
                )
            })?
            .await?;

        Ok(())
    }

    pub async fn get_last_lsn(&self) -> Result<PgLsn, DeltaTableError> {
        let uri = self.delta_full_path("last_lsn");
        let table = open_table(uri).await?;

        let ctx = SessionContext::new();
        ctx.register_table("last_lsn", Arc::new(table))?;

        let batches = ctx.sql("SELECT lsn FROM last_lsn").await?.collect().await?;

        if batches.is_empty() {
            return Err(DeltaTableError::Generic(
                "No data returned from the query".to_string(),
            ));
        }

        let batch = &batches[0];
        let column = batch.column(0);

        let array = column
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DeltaTableError::Generic("Failed to downcast column to Int64Array".to_string())
            })?;

        if array.is_empty() {
            return Err(DeltaTableError::Generic(
                "No data in the 'lsn' column".to_string(),
            ));
        }
        let lsn_value = array.value(0);

        Ok((lsn_value as u64).into())
    }

    pub async fn create_table(
        &mut self,
        table_name: &str,
        columns: &[ColumnSchema],
    ) -> Result<Arc<Schema>, DeltaTableError> {
        let full_path = self.delta_full_path(table_name);

        let table = DeltaOps::try_from_uri(&full_path)
            .await?
            .create()
            .with_table_name(table_name);

        let arrow_schema = Self::generate_schema(columns, table)?;

        Ok(arrow_schema)
    }

    fn generate_schema(
        columns: &[ColumnSchema],
        table: CreateBuilder,
    ) -> Result<Arc<Schema>, DeltaTableError> {
        let mut schema: Vec<Field> = vec![];

        let mut final_table = table;

        for column in columns {
            final_table = final_table.with_column(
                column.name.as_str(),
                Self::postgres_to_delta(&column.typ),
                false,
                None,
            );

            schema.push(Field::new(
                column.name.as_str(),
                Self::postgres_to_arrow(&column.typ),
                true,
            ));
        }

        schema.push(Field::new("OP", ArrowDataType::Utf8, true));
        schema.push(Field::new(
            "pg_replicate_inserted_time",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        Ok(Arc::new(Schema::new(schema)))
    }

    fn get_table_schema(&self, table_id: TableId) -> Result<&TableSchema, DeltaTableError> {
        self.table_schemas
            .as_ref()
            .ok_or_else(|| {
                DeltaTableError::Generic("Table schemas are not initialized".to_string())
            })?
            .get(&table_id)
            .ok_or_else(|| {
                DeltaTableError::Generic(format!("Table schema not found for ID: {:?}", table_id))
            })
    }

    fn get_delta_schema(&self, table_name: &str) -> Result<&Arc<Schema>, DeltaTableError> {
        self.delta_schemas
            .as_ref()
            .ok_or_else(|| {
                DeltaTableError::Generic("Delta schemas are not initialized".to_string())
            })?
            .get(table_name)
            .ok_or_else(|| DeltaTableError::NoSchema)
    }

    fn naive_time_to_microseconds(&self, time: NaiveTime) -> i64 {
        (time.hour() as i64 * 3_600_000_000)
            + (time.minute() as i64 * 60_000_000)
            + (time.second() as i64 * 1_000_000)
            + (time.nanosecond() as i64 / 1_000) // Convert nanoseconds to microseconds
    }

    fn cell_to_arrow(&self, typ: &Cell) -> Arc<dyn Array> {
        match typ {
            Cell::Null => Arc::new(StringArray::from(vec![String::from("")])),
            Cell::Uuid(value) => Arc::new(StringArray::from(vec![value.to_string()])),
            Cell::Bytes(value) => {
                let data = std::str::from_utf8(value)
                    .map_err(|e| format!("Failed to convert to string: {}", e));
                let result = match data {
                    Ok(value) => value.to_string(),
                    Err(err_msg) => err_msg,
                };
                Arc::new(StringArray::from(vec![result]))
            }
            Cell::Json(value) => Arc::new(StringArray::from(vec![value.to_string()])),
            Cell::Bool(value) => Arc::new(BooleanArray::from(vec![*value])),
            Cell::String(value) => Arc::new(StringArray::from(vec![value.to_string()])),
            Cell::I16(value) => Arc::new(Int32Array::from(vec![*value as i32])),
            Cell::I32(value) => Arc::new(Int32Array::from(vec![*value])),
            Cell::U32(value) => Arc::new(UInt32Array::from(vec![*value])),
            Cell::I64(value) => Arc::new(Int32Array::from(vec![*value as i32])),
            Cell::F32(value) => Arc::new(Float32Array::from(vec![*value])),
            Cell::F64(value) => Arc::new(Float64Array::from(vec![*value])),
            Cell::Numeric(value) => {
                let data = value.clone().to_string();
                Arc::new(StringArray::from(vec![data]))
            }
            Cell::Date(value) => {
                Arc::new(Date32Array::from(vec![Self::naive_date_to_arrow(*value)]))
            }
            Cell::Time(value) => {
                let final_time = self.naive_time_to_microseconds(*value);
                Arc::new(TimestampMicrosecondArray::from(vec![final_time]))
            }
            Cell::TimeStamp(value) => Arc::new(TimestampMicrosecondArray::from(vec![value
                .and_utc()
                .timestamp_micros()])),
            Cell::TimeStampTz(value) => Arc::new(TimestampMicrosecondArray::from(vec![
                value.timestamp_micros()
            ])),
            Cell::Array(_) => {
                Arc::new(StringArray::from(vec![String::from("not implemented yet")]))
            }
        }
    }

    pub fn table_name_in_delta(table_name: &TableName) -> String {
        format!("{}_{}", table_name.schema, table_name.name)
    }

    fn delta_full_path(&self, table_name: &str) -> String {
        format!("{}/{}", self.path, table_name)
    }

    async fn get_lsn_schema() -> Result<Arc<Schema>, DeltaTableError> {
        let last_lsn_column_schemas = [
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                primary: true,
            },
            ColumnSchema {
                name: "lsn".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                primary: false,
            },
        ];

        let mut delta_schema: Vec<Field> = vec![];

        for column in last_lsn_column_schemas {
            delta_schema.push(Field::new(
                column.name.as_str(),
                Self::postgres_to_arrow(&column.typ),
                true,
            ));
        }

        delta_schema.push(Field::new("OP", ArrowDataType::Utf8, true));
        delta_schema.push(Field::new(
            "pg_replicate_inserted_time",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        Ok(Arc::new(Schema::new(delta_schema)))
    }
    pub async fn insert_last_lsn_row(&self) -> Result<(), DeltaTableError> {
        let full_path = self.delta_full_path("last_lsn");
        let delta_schema = Self::get_lsn_schema().await?;

        let id: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1]));
        let last_lsn: Arc<dyn Array> = Arc::new(Int32Array::from(vec![0]));

        let operation: Arc<dyn Array> = Arc::new(StringArray::from(vec!["I"]));
        let inserted_at: Arc<dyn Array> = Arc::new(TimestampMicrosecondArray::from(vec![
            Utc::now().timestamp_micros(),
        ]));

        let arrow_vect: Vec<Arc<dyn Array>> = vec![id, last_lsn, operation, inserted_at];

        let batches = DeltaRecordBatch::try_new(delta_schema, arrow_vect)?;
        let data: Vec<DeltaRecordBatch> = vec![batches];

        DeltaOps::try_from_uri(full_path).await?.write(data).await?;

        Ok(())
    }

    pub async fn write_to_table(
        &mut self,
        row: TableRow,
        table_id: TableId,
        op: &str,
    ) -> Result<(), DeltaTableError> {
        let table_schema = self.get_table_schema(table_id)?;
        let table_name = Self::table_name_in_delta(&table_schema.table_name);

        let full_path = self.delta_full_path(&table_name);

        let data = row.values;

        let mut arrow_vect: Vec<Arc<dyn Array>> =
            data.iter().map(|cell| self.cell_to_arrow(cell)).collect();

        let operation: Arc<dyn Array> = Arc::new(StringArray::from(vec![op]));
        arrow_vect.push(operation);

        let inserted_at: Arc<dyn Array> = Arc::new(TimestampMicrosecondArray::from(vec![
            Utc::now().timestamp_micros(),
        ]));
        arrow_vect.push(inserted_at);

        let mut data: Vec<DeltaRecordBatch> = Vec::new();
        let delta_schema = self.get_delta_schema(&table_name)?;

        let batches = DeltaRecordBatch::try_new(delta_schema.clone(), arrow_vect)?;

        data.push(batches);

        DeltaOps::try_from_uri(full_path).await?.write(data).await?;

        Ok(())
    }

    pub async fn write_to_table_batch(
        &mut self,
        rows_batch: HashMap<TableId, Vec<TableRow>>,
    ) -> Result<(), DeltaTableError> {
        for (table_id, data) in rows_batch {
            let table_schema = self.get_table_schema(table_id)?;
            let table_name = Self::table_name_in_delta(&table_schema.table_name);

            let full_path = self.delta_full_path(&table_name);
            let delta_schema = self.get_delta_schema(&table_name)?; // Move schema retrieval outside the loop

            let data: Vec<DeltaRecordBatch> = data
                .into_iter()
                .map(|row| {
                    let data = row.values;
                    let arrow_vect: Vec<Arc<dyn Array>> =
                        data.iter().map(|cell| self.cell_to_arrow(cell)).collect();

                    // Return DeltaRecordBatch or propagate the error
                    DeltaRecordBatch::try_new(delta_schema.clone(), arrow_vect)
                })
                .collect::<Result<_, _>>()?; // Collect into Vec and handle errors

            DeltaOps::try_from_uri(full_path).await?.write(data).await?;
        }

        Ok(())
    }
}

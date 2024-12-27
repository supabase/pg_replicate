use aws_config::BehaviorVersion;
use chrono::Timelike;
use chrono::{NaiveDate, NaiveTime, Utc};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use deltalake::aws::constants::{
    AWS_ALLOW_HTTP, AWS_ENDPOINT_URL, AWS_FORCE_CREDENTIAL_LOAD, AWS_S3_ALLOW_UNSAFE_RENAME,
};
use deltalake::datafusion::execution::context::SessionContext;
use deltalake::datafusion::prelude::col;
use deltalake::kernel::{PrimitiveType, StructField, TableFeatures};
use deltalake::operations::add_feature;
use deltalake::operations::write::WriteBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{aws, open_table, open_table_with_storage_options, DeltaTable};
use deltalake::{kernel::DataType, DeltaOps, DeltaTableError};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::types::{PgLsn, Type};

use crate::{
    conversions::{table_row::TableRow, Cell},
    table::{ColumnSchema, TableId, TableName, TableSchema},
};
use deltalake::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, RecordBatch as DeltaRecordBatch, StringArray,
    TimestampMicrosecondArray, UInt32Array,
};

pub struct DeltaClient {
    pub path: String,
    pub table_schemas: Option<HashMap<TableId, TableSchema>>,
    pub delta_schemas: Option<HashMap<String, Arc<Schema>>>,
}

impl DeltaClient {
    async fn aws_config() -> HashMap<String, String> {
        let mut storage_options = HashMap::new();

        storage_options.insert(AWS_FORCE_CREDENTIAL_LOAD.to_string(), "true".to_string());
        storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_string(), "true".to_string());
        let config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;

        match config.endpoint_url() {
            Some(endpoint) => {
                storage_options.insert(AWS_ENDPOINT_URL.to_string(), endpoint.to_string());
                storage_options.insert(AWS_ALLOW_HTTP.to_string(), "true".to_string());
            }
            None => (),
        }

        storage_options
    }

    async fn is_local_storage(uri: &str) -> Result<bool, DeltaTableError> {
        if uri.starts_with("s3://") {
            Ok(false)
        } else if uri.starts_with("file://") {
            Ok(true)
        } else {
            Err(DeltaTableError::Generic(
                "storage type should start with either s3:// or file://".to_string(),
            ))
        }
    }

    /// strips file:// prefix from the uri
    fn strip_file_prefix(uri: &str) -> &str {
        if let Some(path) = uri.strip_prefix("file://") {
            path
        } else {
            panic!("strip_file_prefix called on a non-file uri")
        }
    }

    async fn open_delta_table(uri: &str) -> Result<DeltaTable, DeltaTableError> {
        let is_local_storage = Self::is_local_storage(uri).await?;
        if is_local_storage {
            open_table(uri).await
        } else {
            let uri = Self::strip_file_prefix(uri).await?;

            open_table_with_storage_options(uri, Self::aws_config().await).await
        }
    }

    async fn write_batch_to_s3(
        uri: &str,
        data: Vec<DeltaRecordBatch>,
        save_mode: SaveMode,
    ) -> Result<(), DeltaTableError> {
        println!("{}", uri);
        let table = Self::open_delta_table(uri).await?;
        let table_snapshot = table.snapshot()?.clone();
        let table_logstore = table.log_store();
        let add_feature = add_feature::AddTableFeatureBuilder::new(
            table_logstore.clone(),
            table_snapshot.clone(),
        )
        .with_feature(TableFeatures::TimestampWithoutTimezone)
        .with_allow_protocol_versions_increase(true)
        .await?;

        WriteBuilder::new(
            add_feature.log_store(),
            Some(add_feature.snapshot()?.clone()),
        )
        .with_save_mode(save_mode)
        .with_input_batches(data)
        .await?;

        Ok(())
    }

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
            &Type::INT4 => DataType::INTEGER,
            &Type::INT8 => DataType::LONG,
            &Type::INT2 => DataType::SHORT,
            &Type::FLOAT4 => DataType::FLOAT,
            &Type::FLOAT8 | &Type::NUMERIC => DataType::DOUBLE,
            &Type::DATE => DataType::DATE,
            &Type::TIME | &Type::TIMESTAMP | &Type::TIMESTAMPTZ => {
                DataType::Primitive(PrimitiveType::TimestampNtz)
            }
            &Type::UUID => DataType::STRING,
            &Type::OID => DataType::INTEGER,
            &Type::BYTEA => DataType::BINARY,
            _ => DataType::STRING,
        }
    }

    fn postgres_to_arrow(typ: &Type) -> ArrowDataType {
        match typ {
            &Type::BOOL => ArrowDataType::Boolean,
            &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => {
                ArrowDataType::Utf8
            }
            &Type::INT2 => ArrowDataType::Int16,
            &Type::INT4 => ArrowDataType::Int32,
            &Type::INT8 => ArrowDataType::Int64,
            &Type::FLOAT4 => ArrowDataType::Float32,
            &Type::FLOAT8 | &Type::NUMERIC => ArrowDataType::Float64,
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

    pub(crate) async fn delta_table_exists(&self, table_name: &str) -> bool {
        let uri = self.delta_full_path(table_name);

        Self::open_delta_table(&uri).await.is_ok()
    }

    pub(crate) async fn set_last_lsn(&self, lsn: PgLsn) -> Result<(), DeltaTableError> {
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

        let table = Self::open_delta_table(&uri).await?;
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

    pub(crate) async fn get_last_lsn(&self) -> Result<PgLsn, DeltaTableError> {
        let uri = self.delta_full_path("last_lsn");
        let table = Self::open_delta_table(&uri).await?;

        let ctx = SessionContext::new();
        ctx.register_table("last_lsn", Arc::new(table))?;

        let batches = ctx.sql("SELECT lsn FROM last_lsn").await?.collect().await?;

        if batches.is_empty() {
            return Err(DeltaTableError::Generic(
                "failed to get last_lsn".to_string(),
            ));
        }

        let batch = &batches[0];
        let column = batch.column(0);

        let array = column
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DeltaTableError::Generic("failed to downcast column to Int32Array".to_string())
            })?;

        if array.is_empty() {
            return Err(DeltaTableError::Generic(
                "no data in the 'lsn' column".to_string(),
            ));
        }
        let lsn_value = array.value(0);

        Ok((lsn_value as u64).into())
    }

    pub(crate) async fn create_table(
        &mut self,
        table_name: &str,
        columns: &[ColumnSchema],
    ) -> Result<Arc<Schema>, DeltaTableError> {
        let uri = self.delta_full_path(table_name);
        aws::register_handlers(None);

        let struct_fields = columns
            .iter()
            .map(|col| StructField::new(col.name.as_str(), Self::postgres_to_delta(&col.typ), true))
            .chain([
                StructField::new("OP", Self::postgres_to_delta(&Type::VARCHAR), true),
                StructField::new(
                    "pg_replicate_inserted_time",
                    Self::postgres_to_delta(&Type::TIMESTAMP),
                    true,
                ),
            ])
            .collect::<Vec<_>>();

        let is_local_storage = Self::is_local_storage(&uri).await?;
        let delta_ops = if is_local_storage {
            DeltaOps::try_from_uri(&uri).await?
        } else {
            let uri = Self::strip_file_prefix(&uri).await?;
            DeltaOps::try_from_uri_with_storage_options(uri, Self::aws_config().await).await?
        };

        delta_ops
            .create()
            .with_table_name(table_name)
            .with_columns(struct_fields)
            .await?;

        Self::generate_schema(columns)
    }

    pub(crate) fn generate_schema(
        columns: &[ColumnSchema],
    ) -> Result<Arc<Schema>, DeltaTableError> {
        let mut schema: Vec<Field> = vec![];

        for column in columns {
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
                DeltaTableError::Generic("table schemas are not initialized".to_string())
            })?
            .get(&table_id)
            .ok_or_else(|| {
                DeltaTableError::Generic(format!("table schema not found for id: {:?}", table_id))
            })
    }

    fn get_delta_schema(&self, table_name: &str) -> Result<&Arc<Schema>, DeltaTableError> {
        self.delta_schemas
            .as_ref()
            .ok_or_else(|| {
                DeltaTableError::Generic("delta schemas are not initialized".to_string())
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
                let data: Vec<&[u8]> = value
                    .iter()
                    .map(|item| std::slice::from_ref(item))
                    .collect();
                Arc::new(BinaryArray::from(data))
            }
            Cell::Json(value) => Arc::new(StringArray::from(vec![value.to_string()])),
            Cell::Bool(value) => Arc::new(BooleanArray::from(vec![*value])),
            Cell::String(value) => Arc::new(StringArray::from(vec![value.to_string()])),
            Cell::I16(value) => Arc::new(Int16Array::from(vec![*value])),
            Cell::I32(value) => Arc::new(Int32Array::from(vec![*value])),
            Cell::U32(value) => Arc::new(UInt32Array::from(vec![*value])),
            Cell::I64(value) => Arc::new(Int64Array::from(vec![*value])),
            Cell::F32(value) => Arc::new(Float32Array::from(vec![*value])),
            Cell::F64(value) => Arc::new(Float64Array::from(vec![*value])),
            Cell::Numeric(value) => {
                let data = value.to_string().parse::<f64>().unwrap();
                Arc::new(Float64Array::from(vec![data]))
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

    pub(crate) fn table_name_in_delta(table_name: &TableName) -> String {
        format!("{}_{}", table_name.schema, table_name.name)
    }

    fn delta_full_path(&self, table_name: &str) -> String {
        format!("{}/{}", self.path, table_name)
    }

    async fn get_lsn_schema() -> Result<Arc<Schema>, DeltaTableError> {
        let delta_schema: Vec<Field> = vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("lsn", ArrowDataType::Int32, false),
            Field::new("OP", ArrowDataType::Utf8, true),
            Field::new(
                "pg_replicate_inserted_time",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ];

        Ok(Arc::new(Schema::new(delta_schema)))
    }

    pub(crate) async fn insert_last_lsn_row(&self) -> Result<(), DeltaTableError> {
        let uri = self.delta_full_path("last_lsn");
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

        let is_local_storage = Self::is_local_storage(&uri).await?;

        if is_local_storage {
            Self::write_batch_to_local(&uri, data, SaveMode::Overwrite).await?;
        } else {
            let uri = Self::strip_file_prefix(&uri).await?;
            Self::write_batch_to_s3(uri, data, SaveMode::Overwrite).await?;
        }

        Ok(())
    }

    pub(crate) async fn write_to_table_batch(
        &mut self,
        rows_batch: HashMap<TableId, Vec<TableRow>>,
    ) -> Result<(), DeltaTableError> {
        for (table_id, data) in rows_batch {
            let table_schema = self.get_table_schema(table_id)?;
            let table_name = Self::table_name_in_delta(&table_schema.table_name);

            let uri = self.delta_full_path(&table_name);
            let delta_schema = self.get_delta_schema(&table_name)?; // Move schema retrieval outside the loop

            let data: Vec<DeltaRecordBatch> = data
                .into_iter()
                .map(|row| {
                    let cells = row.values;
                    let arrow_vect: Vec<Arc<dyn Array>> =
                        cells.iter().map(|cell| self.cell_to_arrow(cell)).collect();

                    DeltaRecordBatch::try_new(delta_schema.clone(), arrow_vect)
                })
                .collect::<Result<_, _>>()?;

            let is_local_storage = Self::is_local_storage(&uri).await?;
            if is_local_storage {
                Self::write_batch_to_local(&uri, data, SaveMode::Append).await?;
            } else {
                let uri = Self::strip_file_prefix(&uri).await?;
                Self::write_batch_to_s3(uri, data, SaveMode::Append).await?;
            }
        }

        Ok(())
    }

    async fn write_batch_to_local(
        uri: &str,
        data: Vec<DeltaRecordBatch>,
        save_mode: SaveMode,
    ) -> Result<(), DeltaTableError> {
        DeltaOps::try_from_uri(uri)
            .await?
            .write(data)
            .with_save_mode(save_mode)
            .await?;

        Ok(())
    }
}

use chrono::{NaiveDate, Utc};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use deltalake::operations::create::CreateBuilder;
use deltalake::{kernel::DataType, DeltaOps, DeltaTableError};
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio_postgres::types::Type;

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

impl Default for DeltaClient {
    fn default() -> Self {
        Self {
            path: Default::default(),
            table_schemas: Default::default(),
            delta_schemas: Default::default(),
        }
    }
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
            &Type::FLOAT4 | &Type::FLOAT8 | &Type::NUMERIC => ArrowDataType::Float16,
            &Type::DATE => ArrowDataType::Date32,
            &Type::TIME | &Type::TIMESTAMP | &Type::TIMESTAMPTZ => ArrowDataType::Utf8,
            &Type::UUID => ArrowDataType::Utf8,
            &Type::OID => ArrowDataType::Utf8,
            _ => ArrowDataType::Utf8,
        }
    }

    pub async fn create_table_schema(
        &mut self,
        tb_name: String,
        columns: &[ColumnSchema],
    ) -> Result<Arc<Schema>, DeltaTableError> {
        let full_path = self.delta_full_path(tb_name.clone());

        if Path::new(&full_path).join("_delta_log").exists() {
            DeltaOps::try_from_uri(&full_path).await?.delete().await?;
        }

        let mut table = DeltaOps::try_from_uri(&full_path)
            .await?
            .create()
            .with_table_name(tb_name.clone());

        let arrow_schema = Self::generate_schema(columns, &mut table)?;

        Ok(arrow_schema)
    }

    fn generate_schema(
        columns: &[ColumnSchema],
        table: &mut CreateBuilder,
    ) -> Result<Arc<Schema>, DeltaTableError> {
        let mut schema: Vec<Field> = vec![];

        for column in columns {
            *table = table.clone().with_column(
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

    fn get_delta_schema(&self, table_name: String) -> Result<&Arc<Schema>, DeltaTableError> {
        self.delta_schemas
            .as_ref()
            .ok_or_else(|| {
                DeltaTableError::Generic("Delta schemas are not initialized".to_string())
            })?
            .get(&table_name)
            .ok_or_else(|| {
                DeltaTableError::Generic(format!(
                    "Delta schema not found for table: {}",
                    table_name
                ))
            })
    }

    fn cell_to_arrow(&self, typ: &Cell) -> Arc<dyn Array> {
        match typ {
            Cell::Null =>  Arc::new(StringArray::from(vec![String::from("")])),
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
            Cell::Time(value) => Arc::new(StringArray::from(vec![value.to_string()])),
            Cell::TimeStamp(value) => Arc::new(StringArray::from(vec![value.to_string()])),
            Cell::TimeStampTz(value) => Arc::new(StringArray::from(vec![value.to_string()])),
            Cell::Array(_) => {
                Arc::new(StringArray::from(vec![String::from("not implemented yet")]))
            }
        }
    }

    pub fn table_name_in_delta(table_name: &TableName) -> String {
        format!("{}_{}", table_name.schema, table_name.name)
    }

    fn delta_full_path(&self, table_name: String) -> String {
        format!("{}/{}", self.path, table_name)
    }

    pub async fn write_to_table(
        &mut self,
        row: TableRow,
        table_id: TableId,
        op: String,
    ) -> Result<(), DeltaTableError> {
        let table_schema = self.get_table_schema(table_id)?;
        let table_name = Self::table_name_in_delta(&table_schema.table_name);

        let full_path = self.delta_full_path(table_name.clone());

        let data = row.values;

        let mut arrow_vect: Vec<Arc<dyn Array>> = vec![];

        data.iter().for_each(|cell| {
            let arrow_array = self.cell_to_arrow(cell);
            arrow_vect.push(arrow_array);
        });

        let operation: Arc<dyn Array> = Arc::new(StringArray::from(vec![op]));
        arrow_vect.push(operation);

        let inserted_at: Arc<dyn Array> = Arc::new(TimestampMicrosecondArray::from(vec![
            Utc::now().timestamp_micros(),
        ]));
        arrow_vect.push(inserted_at);

        let mut data: Vec<DeltaRecordBatch> = Vec::new();
        let delta_schema = self.get_delta_schema(table_name)?;

        let batches = DeltaRecordBatch::try_new(delta_schema.clone(), arrow_vect)?;

        data.push(batches);

        DeltaOps::try_from_uri(full_path).await?.write(data).await?;

        Ok(())
    }
}

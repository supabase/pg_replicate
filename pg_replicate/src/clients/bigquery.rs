use std::{collections::HashSet, fs};

use bytes::{Buf, BufMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::StreamExt;
use gcp_bigquery_client::storage::ColumnMode;
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use gcp_bigquery_client::{
    error::BQError,
    google::cloud::bigquery::storage::v1::{WriteStream, WriteStreamView},
    model::{
        query_request::QueryRequest, query_response::ResultSet,
        table_data_insert_all_request::TableDataInsertAllRequest,
    },
    storage::{ColumnType, FieldDescriptor, StreamName, TableDescriptor},
    Client,
};
use prost::Message;
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;
use uuid::Uuid;

use crate::conversions::numeric::PgNumeric;
use crate::conversions::{ArrayCell, Cell};
use crate::{
    conversions::table_row::TableRow,
    table::{ColumnSchema, TableId, TableSchema},
};

pub struct BigQueryClient {
    project_id: String,
    client: Client,
}

//TODO: fix all SQL injections
impl BigQueryClient {
    pub async fn new_with_key_path(
        project_id: String,
        gcp_sa_key_path: &str,
    ) -> Result<BigQueryClient, BQError> {
        let gcp_sa_key = fs::read_to_string(gcp_sa_key_path)?;
        let service_account_key = parse_service_account_key(gcp_sa_key)?;
        let client = Client::from_service_account_key(service_account_key, false).await?;

        Ok(BigQueryClient { project_id, client })
    }

    pub async fn new_with_key(
        project_id: String,
        gcp_sa_key: &str,
    ) -> Result<BigQueryClient, BQError> {
        let service_account_key = parse_service_account_key(gcp_sa_key)?;
        let client = Client::from_service_account_key(service_account_key, false).await?;

        Ok(BigQueryClient { project_id, client })
    }

    pub async fn create_table_if_missing(
        &self,
        dataset_id: &str,
        table_name: &str,
        column_schemas: &[ColumnSchema],
    ) -> Result<bool, BQError> {
        if self.table_exists(dataset_id, table_name).await? {
            Ok(false)
        } else {
            self.create_table(dataset_id, table_name, column_schemas)
                .await?;
            Ok(true)
        }
    }

    fn postgres_to_bigquery_type(typ: &Type) -> &'static str {
        match typ {
            &Type::BOOL => "bool",
            &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "string",
            &Type::INT2 | &Type::INT4 | &Type::INT8 => "int64",
            &Type::FLOAT4 | &Type::FLOAT8 => "float64",
            &Type::NUMERIC => "bignumeric",
            &Type::DATE => "date",
            &Type::TIME => "time",
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => "timestamp",
            &Type::UUID => "string",
            &Type::JSON | &Type::JSONB => "json",
            &Type::OID => "int64",
            &Type::BYTEA => "bytes",
            &Type::BOOL_ARRAY => "array<bool>",
            &Type::CHAR_ARRAY
            | &Type::BPCHAR_ARRAY
            | &Type::VARCHAR_ARRAY
            | &Type::NAME_ARRAY
            | &Type::TEXT_ARRAY => "array<string>",
            &Type::INT2_ARRAY | &Type::INT4_ARRAY | &Type::INT8_ARRAY => "array<int64>",
            &Type::FLOAT4_ARRAY | &Type::FLOAT8_ARRAY => "array<float64>",
            &Type::NUMERIC_ARRAY => "array<bignumeric>",
            &Type::DATE_ARRAY => "array<date>",
            &Type::TIME_ARRAY => "array<time>",
            &Type::TIMESTAMP_ARRAY | &Type::TIMESTAMPTZ_ARRAY => "array<timestamp>",
            &Type::UUID_ARRAY => "array<string>",
            &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "array<json>",
            &Type::OID_ARRAY => "array<int64>",
            &Type::BYTEA_ARRAY => "array<bytes>",
            _ => "string",
        }
    }

    fn is_array_type(typ: &Type) -> bool {
        matches!(
            typ,
            &Type::BOOL_ARRAY
                | &Type::CHAR_ARRAY
                | &Type::BPCHAR_ARRAY
                | &Type::VARCHAR_ARRAY
                | &Type::NAME_ARRAY
                | &Type::TEXT_ARRAY
                | &Type::INT2_ARRAY
                | &Type::INT4_ARRAY
                | &Type::INT8_ARRAY
                | &Type::FLOAT4_ARRAY
                | &Type::FLOAT8_ARRAY
                | &Type::NUMERIC_ARRAY
                | &Type::DATE_ARRAY
                | &Type::TIME_ARRAY
                | &Type::TIMESTAMP_ARRAY
                | &Type::TIMESTAMPTZ_ARRAY
                | &Type::UUID_ARRAY
                | &Type::JSON_ARRAY
                | &Type::JSONB_ARRAY
                | &Type::OID_ARRAY
                | &Type::BYTEA_ARRAY
        )
    }

    fn column_spec(column_schema: &ColumnSchema, s: &mut String) {
        s.push('`');
        s.push_str(&column_schema.name);
        s.push('`');
        s.push(' ');
        let typ = Self::postgres_to_bigquery_type(&column_schema.typ);
        s.push_str(typ);
        if !column_schema.nullable && !Self::is_array_type(&column_schema.typ) {
            s.push_str(" not null");
        };
    }

    fn add_primary_key_clause(column_schemas: &[ColumnSchema], s: &mut String) {
        let identity_columns = column_schemas.iter().filter(|s| s.primary);

        s.push_str("primary key (");

        for column in identity_columns {
            s.push('`');
            s.push_str(&column.name);
            s.push('`');
            s.push(',');
        }

        s.pop(); //','
        s.push_str(") not enforced");
    }

    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> String {
        let mut s = String::new();
        s.push('(');

        for column_schema in column_schemas.iter() {
            Self::column_spec(column_schema, &mut s);
            s.push(',');
        }

        let has_identity_cols = column_schemas.iter().any(|s| s.primary);
        if has_identity_cols {
            Self::add_primary_key_clause(column_schemas, &mut s);
        } else {
            s.pop(); //','
        }

        s.push(')');

        s
    }

    fn max_staleness_option(max_staleness_mins: u16) -> String {
        format!("options (max_staleness = interval {max_staleness_mins} minute)")
    }

    pub async fn create_table(
        &self,
        dataset_id: &str,
        table_name: &str,
        column_schemas: &[ColumnSchema],
    ) -> Result<(), BQError> {
        let columns_spec = Self::create_columns_spec(column_schemas);
        let max_staleness_option = Self::max_staleness_option(5);
        let project_id = &self.project_id;
        info!("creating table {project_id}.{dataset_id}.{table_name} in bigquery");
        let query =
            format!("create table `{project_id}.{dataset_id}.{table_name}` {columns_spec} {max_staleness_option}",);
        let _ = self.query(query).await?;
        Ok(())
    }

    pub async fn get_default_stream(
        &mut self,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<WriteStream, BQError> {
        let stream_name = StreamName::new_default(
            self.project_id.clone(),
            dataset_id.to_string(),
            table_name.to_string(),
        );
        let write_stream = self
            .client
            .storage_mut()
            .get_write_stream(&stream_name, WriteStreamView::Full)
            .await?;
        Ok(write_stream)
    }

    pub async fn table_exists(&self, dataset_id: &str, table_name: &str) -> Result<bool, BQError> {
        let query = format!(
            "select exists
                (
                    select * from
                    {dataset_id}.INFORMATION_SCHEMA.TABLES
                    where table_name = '{table_name}'
                ) as table_exists;",
        );

        let mut rs = self.query(query).await?;

        let mut exists = false;

        if rs.next_row() {
            exists = rs
                .get_bool_by_name("table_exists")?
                .expect("no column named `table_exists` found in query result");
        }

        Ok(exists)
    }

    pub async fn get_last_lsn(&self, dataset_id: &str) -> Result<PgLsn, BQError> {
        let project_id = &self.project_id;
        let query = format!("select lsn from `{project_id}.{dataset_id}.last_lsn`",);

        let mut rs = self.query(query).await?;

        let lsn: i64 = if rs.next_row() {
            rs.get_i64_by_name("lsn")?
                .expect("no column named `lsn` found in query result")
        } else {
            //TODO: return error instead of panicking
            panic!("failed to get lsn");
        };

        Ok((lsn as u64).into())
    }

    pub async fn set_last_lsn(&self, dataset_id: &str, lsn: PgLsn) -> Result<(), BQError> {
        let lsn: u64 = lsn.into();

        let project_id = &self.project_id;
        let query =
            format!("update `{project_id}.{dataset_id}.last_lsn` set lsn = {lsn} where id = 1",);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn insert_last_lsn_row(&self, dataset_id: &str) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let query =
            format!("insert into `{project_id}.{dataset_id}.last_lsn` (id, lsn) values (1, 0)",);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn get_copied_table_ids(
        &self,
        dataset_id: &str,
    ) -> Result<HashSet<TableId>, BQError> {
        let project_id = &self.project_id;
        let query = format!("select table_id from `{project_id}.{dataset_id}.copied_tables`",);

        let mut rs = self.query(query).await?;
        let mut table_ids = HashSet::new();
        while rs.next_row() {
            let table_id = rs
                .get_i64_by_name("table_id")?
                .expect("no column named `table_id` found in query result");
            table_ids.insert(table_id as TableId);
        }

        Ok(table_ids)
    }

    pub async fn insert_into_copied_tables(
        &self,
        dataset_id: &str,
        table_id: TableId,
    ) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let query = format!(
            "insert into `{project_id}.{dataset_id}.copied_tables` (table_id) values ({table_id})",
        );

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn insert_row(
        &self,
        dataset_id: &str,
        table_name: &str,
        table_row: &TableRow,
    ) -> Result<(), BQError> {
        let query = self.create_insert_row_query(dataset_id, table_name, table_row);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn stream_rows(
        &mut self,
        dataset_id: &str,
        table_name: String,
        table_descriptor: &TableDescriptor,
        table_rows: &[TableRow],
    ) -> Result<(), BQError> {
        let default_stream = StreamName::new_default(
            self.project_id.clone(),
            dataset_id.to_string(),
            table_name.to_string(),
        );

        let trace_id = "pg_replicate bigquery client".to_string();
        let mut response_stream = self
            .client
            .storage_mut()
            .append_rows(&default_stream, table_descriptor, table_rows, trace_id)
            .await?;

        if let Some(r) = response_stream.next().await {
            let _ = r?;
        }

        Ok(())
    }

    pub async fn insert_rows(
        &self,
        dataset_id: &str,
        table_name: &str,
        insert_request: TableDataInsertAllRequest,
    ) -> Result<(), BQError> {
        self.client
            .tabledata()
            .insert_all(&self.project_id, dataset_id, table_name, insert_request)
            .await?;
        Ok(())
    }

    fn create_insert_row_query(
        &self,
        dataset_id: &str,
        table_name: &str,
        table_row: &TableRow,
    ) -> String {
        let mut s = String::new();

        let project_id = &self.project_id;
        s.push_str(&format!(
            "insert into `{project_id}.{dataset_id}.{table_name}`"
        ));
        s.push_str(" values(");

        for (i, value) in table_row.values.iter().enumerate() {
            Self::cell_to_query_value(value, &mut s);

            if i < table_row.values.len() - 1 {
                s.push(',');
            }
        }

        s.push(')');

        s
    }

    fn cell_to_query_value(cell: &Cell, s: &mut String) {
        match cell {
            Cell::Null => s.push_str("null"),
            Cell::Bool(b) => s.push_str(&format!("{b}")),
            Cell::String(str) => s.push_str(&format!("'{str}'")),
            Cell::I16(i) => s.push_str(&format!("{i}")),
            Cell::I32(i) => s.push_str(&format!("{i}")),
            Cell::I64(i) => s.push_str(&format!("{i}")),
            Cell::F32(i) => s.push_str(&format!("{i}")),
            Cell::F64(i) => s.push_str(&format!("{i}")),
            Cell::Numeric(n) => s.push_str(&format!("{n}")),
            Cell::Date(t) => s.push_str(&format!("'{t}'")),
            Cell::Time(t) => s.push_str(&format!("'{t}'")),
            Cell::TimeStamp(t) => s.push_str(&format!("'{t}'")),
            Cell::TimeStampTz(t) => s.push_str(&format!("'{t}'")),
            Cell::Uuid(t) => s.push_str(&format!("'{t}'")),
            Cell::Json(j) => s.push_str(&format!("'{j}'")),
            Cell::U32(u) => s.push_str(&format!("{u}")),
            Cell::Bytes(b) => {
                let bytes: String = b.iter().map(|b| *b as char).collect();
                s.push_str(&format!("b'{bytes}'"))
            }
            Cell::Array(_) => unreachable!(),
        }
    }

    pub async fn update_row(
        &self,
        dataset_id: &str,
        table_schema: &TableSchema,
        table_row: &TableRow,
    ) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let table_name = &table_schema.table_name.name;
        let table_name = &format!("`{project_id}.{dataset_id}.{table_name}`");
        let column_schemas = &table_schema.column_schemas;
        let query = Self::create_update_row_query(table_name, column_schemas, table_row);
        let _ = self.query(query).await?;
        Ok(())
    }

    fn create_update_row_query(
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_row: &TableRow,
    ) -> String {
        let mut s = String::new();

        s.push_str("update ");
        s.push_str(table_name);
        s.push_str(" set ");

        let mut remove_comma = false;

        for (cell, column) in table_row.values.iter().zip(column_schemas) {
            if !column.primary {
                s.push_str(&column.name);
                s.push_str(" = ");
                Self::cell_to_query_value(cell, &mut s);
                s.push(',');
                remove_comma = true;
            }
        }

        if remove_comma {
            s.pop();
        }

        Self::add_identities_where_clause(&mut s, column_schemas, table_row);

        s
    }

    /// Adds a where clause for the identity columns
    fn add_identities_where_clause(
        s: &mut String,
        column_schemas: &[ColumnSchema],
        table_row: &TableRow,
    ) {
        s.push_str(" where ");

        let mut remove_and = false;
        for (cell, column) in table_row.values.iter().zip(column_schemas) {
            if column.primary {
                s.push_str(&column.name);
                s.push_str(" = ");
                Self::cell_to_query_value(cell, s);
                s.push_str(" and ");
                remove_and = true;
            }
        }

        if remove_and {
            s.pop(); //' '
            s.pop(); //'d'
            s.pop(); //'n'
            s.pop(); //'a'
            s.pop(); //' '
        }
    }

    pub async fn delete_row(
        &self,
        dataset_id: &str,
        table_schema: &TableSchema,
        table_row: &TableRow,
    ) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let table_name = &table_schema.table_name.name;
        let table_name = &format!("`{project_id}.{dataset_id}.{table_name}`");
        let column_schemas = &table_schema.column_schemas;
        let query = Self::create_delete_row_query(table_name, column_schemas, table_row);
        let _ = self.query(query).await?;

        Ok(())
    }

    fn create_delete_row_query(
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_row: &TableRow,
    ) -> String {
        let mut s = String::new();

        s.push_str("delete from ");
        s.push_str(table_name);

        Self::add_identities_where_clause(&mut s, column_schemas, table_row);

        s
    }

    pub async fn drop_table(&self, dataset_id: &str, table_name: &str) -> Result<(), BQError> {
        let project_id = &self.project_id;
        info!("dropping table {project_id}.{dataset_id}.{table_name} in bigquery");
        let query = format!("drop table `{project_id}.{dataset_id}.{table_name}`",);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn begin_transaction(&self) -> Result<(), BQError> {
        let _ = self.query("begin transaction".to_string()).await?;

        Ok(())
    }

    pub async fn commit_transaction(&self) -> Result<(), BQError> {
        let _ = self.query("commit".to_string()).await?;

        Ok(())
    }

    pub async fn set_last_lsn_and_commit_transaction(
        &self,
        dataset_id: &str,
        last_lsn: PgLsn,
    ) -> Result<(), BQError> {
        self.set_last_lsn(dataset_id, last_lsn).await?;
        self.commit_transaction().await?;
        Ok(())
    }

    async fn query(&self, query: String) -> Result<ResultSet, BQError> {
        let query_response = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;
        Ok(ResultSet::new_from_query_response(query_response))
    }
}

impl Message for TableRow {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let mut tag = 1;
        for cell in &self.values {
            cell.encode_raw(tag, buf);
            tag += 1;
        }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: prost::encoding::WireType,
        _buf: &mut impl Buf,
        _ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!("merge_field not implemented yet");
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        let mut tag = 1;
        for cell in &self.values {
            len += cell.encoded_len(tag);
            tag += 1;
        }
        len
    }

    fn clear(&mut self) {
        for cell in &mut self.values {
            cell.clear();
        }
    }
}

impl Cell {
    fn encode_raw(&self, tag: u32, buf: &mut impl BufMut) {
        match self {
            Cell::Null => {}
            Cell::Bool(b) => {
                ::prost::encoding::bool::encode(tag, b, buf);
            }
            Cell::String(s) => {
                ::prost::encoding::string::encode(tag, s, buf);
            }
            Cell::I16(i) => {
                let val = *i as i32;
                ::prost::encoding::int32::encode(tag, &val, buf);
            }
            Cell::I32(i) => {
                ::prost::encoding::int32::encode(tag, i, buf);
            }
            Cell::I64(i) => {
                ::prost::encoding::int64::encode(tag, i, buf);
            }
            Cell::F32(i) => {
                ::prost::encoding::float::encode(tag, i, buf);
            }
            Cell::F64(i) => {
                ::prost::encoding::double::encode(tag, i, buf);
            }
            Cell::Numeric(n) => {
                let s = n.to_string();
                ::prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::Date(t) => {
                let s = t.format("%Y-%m-%d").to_string();
                ::prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::Time(t) => {
                let s = t.format("%H:%M:%S%.f").to_string();
                ::prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::TimeStamp(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                ::prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::TimeStampTz(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
                ::prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::Uuid(u) => {
                let s = u.to_string();
                ::prost::encoding::string::encode(tag, &s, buf)
            }
            Cell::Json(j) => {
                let s = j.to_string();
                ::prost::encoding::string::encode(tag, &s, buf)
            }
            Cell::U32(i) => {
                ::prost::encoding::uint32::encode(tag, i, buf);
            }
            Cell::Bytes(b) => {
                ::prost::encoding::bytes::encode(tag, b, buf);
            }
            Cell::Array(a) => {
                a.clone().encode_raw(tag, buf);
            }
        }
    }

    fn encoded_len(&self, tag: u32) -> usize {
        match self {
            Cell::Null => 0,
            Cell::Bool(b) => ::prost::encoding::bool::encoded_len(tag, b),
            Cell::String(s) => ::prost::encoding::string::encoded_len(tag, s),
            Cell::I16(i) => {
                let val = *i as i32;
                ::prost::encoding::int32::encoded_len(tag, &val)
            }
            Cell::I32(i) => ::prost::encoding::int32::encoded_len(tag, i),
            Cell::I64(i) => ::prost::encoding::int64::encoded_len(tag, i),
            Cell::F32(i) => ::prost::encoding::float::encoded_len(tag, i),
            Cell::F64(i) => ::prost::encoding::double::encoded_len(tag, i),
            Cell::Numeric(n) => {
                let s = n.to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Date(t) => {
                let s = t.format("%Y-%m-%d").to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Time(t) => {
                let s = t.format("%H:%M:%S%.f").to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::TimeStamp(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::TimeStampTz(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Uuid(u) => {
                let s = u.to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Json(j) => {
                let s = j.to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::U32(i) => ::prost::encoding::uint32::encoded_len(tag, i),
            Cell::Bytes(b) => ::prost::encoding::bytes::encoded_len(tag, b),
            Cell::Array(array_cell) => array_cell.clone().encoded_len(tag),
        }
    }

    fn clear(&mut self) {
        match self {
            Cell::Null => {}
            Cell::Bool(b) => *b = false,
            Cell::String(s) => s.clear(),
            Cell::I16(i) => *i = 0,
            Cell::I32(i) => *i = 0,
            Cell::I64(i) => *i = 0,
            Cell::F32(i) => *i = 0.,
            Cell::F64(i) => *i = 0.,
            Cell::Numeric(n) => *n = PgNumeric::default(),
            Cell::Date(t) => *t = NaiveDate::default(),
            Cell::Time(t) => *t = NaiveTime::default(),
            Cell::TimeStamp(t) => *t = NaiveDateTime::default(),
            Cell::TimeStampTz(t) => *t = DateTime::<Utc>::default(),
            Cell::Uuid(u) => *u = Uuid::default(),
            Cell::Json(j) => *j = serde_json::Value::default(),
            Cell::U32(u) => *u = 0,
            Cell::Bytes(b) => b.clear(),
            Cell::Array(vec) => {
                vec.clear();
            }
        }
    }
}

impl ArrayCell {
    fn encode_raw(self, tag: u32, buf: &mut impl BufMut) {
        match self {
            ArrayCell::Null => {}
            ArrayCell::Bool(mut vec) => {
                let vec: Vec<bool> = vec.drain(..).flatten().collect();
                ::prost::encoding::bool::encode_packed(tag, &vec, buf);
            }
            ArrayCell::String(mut vec) => {
                let vec: Vec<String> = vec.drain(..).flatten().collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::I16(mut vec) => {
                let vec: Vec<i32> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap() as i32)
                    .collect();
                ::prost::encoding::int32::encode_packed(tag, &vec, buf);
            }
            ArrayCell::I32(mut vec) => {
                let vec: Vec<i32> = vec.drain(..).flatten().collect();
                ::prost::encoding::int32::encode_packed(tag, &vec, buf);
            }
            ArrayCell::U32(mut vec) => {
                let vec: Vec<u32> = vec.drain(..).flatten().collect();
                ::prost::encoding::uint32::encode_packed(tag, &vec, buf);
            }
            ArrayCell::I64(mut vec) => {
                let vec: Vec<i64> = vec.drain(..).flatten().collect();
                ::prost::encoding::int64::encode_packed(tag, &vec, buf);
            }
            ArrayCell::F32(mut vec) => {
                let vec: Vec<f32> = vec.drain(..).flatten().collect();
                ::prost::encoding::float::encode_packed(tag, &vec, buf);
            }
            ArrayCell::F64(mut vec) => {
                let vec: Vec<f64> = vec.drain(..).flatten().collect();
                ::prost::encoding::double::encode_packed(tag, &vec, buf);
            }
            ArrayCell::Numeric(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Date(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d").to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Time(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%H:%M:%S%.f").to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::TimeStamp(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::TimeStampTz(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Uuid(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Json(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Bytes(mut vec) => {
                let vec: Vec<Vec<u8>> = vec.drain(..).flatten().collect();
                ::prost::encoding::bytes::encode_repeated(tag, &vec, buf);
            }
        }
    }

    fn encoded_len(self, tag: u32) -> usize {
        match self {
            ArrayCell::Null => 0,
            ArrayCell::Bool(mut vec) => {
                let vec: Vec<bool> = vec.drain(..).flatten().collect();
                ::prost::encoding::bool::encoded_len_packed(tag, &vec)
            }
            ArrayCell::String(mut vec) => {
                let vec: Vec<String> = vec.drain(..).flatten().collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::I16(mut vec) => {
                let vec: Vec<i32> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap() as i32)
                    .collect();
                ::prost::encoding::int32::encoded_len_packed(tag, &vec)
            }
            ArrayCell::I32(mut vec) => {
                let vec: Vec<i32> = vec.drain(..).flatten().collect();
                ::prost::encoding::int32::encoded_len_packed(tag, &vec)
            }
            ArrayCell::U32(mut vec) => {
                let vec: Vec<u32> = vec.drain(..).flatten().collect();
                ::prost::encoding::uint32::encoded_len_packed(tag, &vec)
            }
            ArrayCell::I64(mut vec) => {
                let vec: Vec<i64> = vec.drain(..).flatten().collect();
                ::prost::encoding::int64::encoded_len_packed(tag, &vec)
            }
            ArrayCell::F32(mut vec) => {
                let vec: Vec<f32> = vec.drain(..).flatten().collect();
                ::prost::encoding::float::encoded_len_packed(tag, &vec)
            }
            ArrayCell::F64(mut vec) => {
                let vec: Vec<f64> = vec.drain(..).flatten().collect();
                ::prost::encoding::double::encoded_len_packed(tag, &vec)
            }
            ArrayCell::Numeric(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Date(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d").to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Time(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%H:%M:%S%.f").to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::TimeStamp(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::TimeStampTz(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Uuid(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Json(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Bytes(mut vec) => {
                let vec: Vec<Vec<u8>> = vec.drain(..).flatten().collect();
                ::prost::encoding::bytes::encoded_len_repeated(tag, &vec)
            }
        }
    }

    fn clear(&mut self) {
        match self {
            ArrayCell::Null => {}
            ArrayCell::Bool(vec) => vec.clear(),
            ArrayCell::String(vec) => vec.clear(),
            ArrayCell::I16(vec) => vec.clear(),
            ArrayCell::I32(vec) => vec.clear(),
            ArrayCell::U32(vec) => vec.clear(),
            ArrayCell::I64(vec) => vec.clear(),
            ArrayCell::F32(vec) => vec.clear(),
            ArrayCell::F64(vec) => vec.clear(),
            ArrayCell::Numeric(vec) => vec.clear(),
            ArrayCell::Date(vec) => vec.clear(),
            ArrayCell::Time(vec) => vec.clear(),
            ArrayCell::TimeStamp(vec) => vec.clear(),
            ArrayCell::TimeStampTz(vec) => vec.clear(),
            ArrayCell::Uuid(vec) => vec.clear(),
            ArrayCell::Json(vec) => vec.clear(),
            ArrayCell::Bytes(vec) => vec.clear(),
        }
    }
}

impl From<&TableSchema> for TableDescriptor {
    fn from(table_schema: &TableSchema) -> Self {
        let mut field_descriptors = Vec::with_capacity(table_schema.column_schemas.len());
        let mut number = 1;
        for column_schema in &table_schema.column_schemas {
            let typ = match column_schema.typ {
                Type::BOOL => ColumnType::Bool,
                Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                    ColumnType::String
                }
                Type::INT2 => ColumnType::Int32,
                Type::INT4 => ColumnType::Int32,
                Type::INT8 => ColumnType::Int64,
                Type::FLOAT4 => ColumnType::Float,
                Type::FLOAT8 => ColumnType::Double,
                Type::NUMERIC => ColumnType::String,
                Type::DATE => ColumnType::String,
                Type::TIME => ColumnType::String,
                Type::TIMESTAMP => ColumnType::String,
                Type::TIMESTAMPTZ => ColumnType::String,
                Type::UUID => ColumnType::String,
                Type::JSON => ColumnType::String,
                Type::JSONB => ColumnType::String,
                Type::OID => ColumnType::Int32,
                Type::BYTEA => ColumnType::Bytes,
                Type::BOOL_ARRAY => ColumnType::Bool,
                Type::CHAR_ARRAY
                | Type::BPCHAR_ARRAY
                | Type::VARCHAR_ARRAY
                | Type::NAME_ARRAY
                | Type::TEXT_ARRAY => ColumnType::String,
                Type::INT2_ARRAY => ColumnType::Int32,
                Type::INT4_ARRAY => ColumnType::Int32,
                Type::INT8_ARRAY => ColumnType::Int64,
                Type::FLOAT4_ARRAY => ColumnType::Float,
                Type::FLOAT8_ARRAY => ColumnType::Double,
                Type::NUMERIC_ARRAY => ColumnType::String,
                Type::DATE_ARRAY => ColumnType::String,
                Type::TIME_ARRAY => ColumnType::String,
                Type::TIMESTAMP_ARRAY => ColumnType::String,
                Type::TIMESTAMPTZ_ARRAY => ColumnType::String,
                Type::UUID_ARRAY => ColumnType::String,
                Type::JSON_ARRAY => ColumnType::String,
                Type::JSONB_ARRAY => ColumnType::String,
                Type::OID_ARRAY => ColumnType::Int32,
                Type::BYTEA_ARRAY => ColumnType::Bytes,
                _ => ColumnType::String,
            };

            let mode = match column_schema.typ {
                Type::BOOL_ARRAY
                | Type::CHAR_ARRAY
                | Type::BPCHAR_ARRAY
                | Type::VARCHAR_ARRAY
                | Type::NAME_ARRAY
                | Type::TEXT_ARRAY
                | Type::INT2_ARRAY
                | Type::INT4_ARRAY
                | Type::INT8_ARRAY
                | Type::FLOAT4_ARRAY
                | Type::FLOAT8_ARRAY
                | Type::NUMERIC_ARRAY
                | Type::DATE_ARRAY
                | Type::TIME_ARRAY
                | Type::TIMESTAMP_ARRAY
                | Type::TIMESTAMPTZ_ARRAY
                | Type::UUID_ARRAY
                | Type::JSON_ARRAY
                | Type::JSONB_ARRAY
                | Type::OID_ARRAY
                | Type::BYTEA_ARRAY => ColumnMode::Repeated,
                _ => {
                    if column_schema.nullable {
                        ColumnMode::Nullable
                    } else {
                        ColumnMode::Required
                    }
                }
            };

            field_descriptors.push(FieldDescriptor {
                number,
                name: column_schema.name.clone(),
                typ,
                mode,
            });
            number += 1;
        }

        field_descriptors.push(FieldDescriptor {
            number,
            name: "_CHANGE_TYPE".to_string(),
            typ: ColumnType::String,
            mode: ColumnMode::Required,
        });

        TableDescriptor { field_descriptors }
    }
}

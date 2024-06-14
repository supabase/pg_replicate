use std::collections::HashSet;

use gcp_bigquery_client::{error::BQError, model::query_request::QueryRequest, Client};
use tokio_postgres::types::{PgLsn, Type};

use crate::table::{ColumnSchema, TableId, TableName};

pub struct BigQueryClient {
    project_id: String,
    client: Client,
}

//TODO: fix all SQL injections
impl BigQueryClient {
    pub async fn new(project_id: String, gcp_sa_key_path: &str) -> Result<BigQueryClient, BQError> {
        let client = Client::from_service_account_key_file(gcp_sa_key_path).await?;

        Ok(BigQueryClient { project_id, client })
    }

    pub async fn create_table_if_missing(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<bool, BQError> {
        if self.table_exists(table_name).await? {
            Ok(false)
        } else {
            self.create_table(table_name, column_schemas).await?;
            Ok(true)
        }
    }

    fn postgres_type_to_bigquery_type(typ: &Type) -> &'static str {
        match typ {
            &Type::INT2 | &Type::INT4 | &Type::INT8 => "int64",
            &Type::BOOL => "bool",
            &Type::BYTEA => "bytes",
            &Type::VARCHAR | &Type::BPCHAR => "string",
            &Type::TIMESTAMP => "timestamp",
            typ => panic!("bigquery doesn't yet support type {typ}"),
        }
    }

    fn column_spec(column_schema: &ColumnSchema, s: &mut String) {
        s.push_str(&column_schema.name);
        s.push(' ');
        let typ = Self::postgres_type_to_bigquery_type(&column_schema.typ);
        s.push_str(typ);
        if !column_schema.nullable {
            s.push_str(" not null");
        };
    }

    fn add_primary_key_clause(column_schemas: &[ColumnSchema], s: &mut String) {
        let identity_columns = column_schemas.iter().filter(|s| s.identity);

        s.push_str("primary key (");

        for column in identity_columns {
            s.push_str(&column.name);
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

        let has_identity_cols = column_schemas.iter().any(|s| s.identity);
        if has_identity_cols {
            Self::add_primary_key_clause(column_schemas, &mut s);
        } else {
            s.pop(); //','
        }

        s.push(')');

        s
    }

    pub async fn create_table(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<(), BQError> {
        let columns_spec = Self::create_columns_spec(column_schemas);
        let query = format!(
            "create table `{}.{}.{}` {}",
            self.project_id, table_name.schema, table_name.name, columns_spec
        );
        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;
        Ok(())
    }

    pub async fn table_exists(&self, table_name: &TableName) -> Result<bool, BQError> {
        let query = format!(
            "select exists
                (
                    select * from
                    {}.INFORMATION_SCHEMA.TABLES
                    where table_name = '{}'
                ) as table_exists;",
            table_name.schema, table_name.name
        );
        let mut res = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

        let mut exists = false;
        if res.next_row() {
            exists = res
                .get_bool_by_name("table_exists")?
                .expect("no column named `table_exists` found in query result");
        }

        Ok(exists)
    }

    pub async fn get_last_lsn(&self, dataset_id: &str) -> Result<PgLsn, BQError> {
        let query = format!(
            "select lsn from `{}.{}.last_lsn`",
            self.project_id, dataset_id
        );

        let mut res = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

        let lsn: i64 = if res.next_row() {
            res.get_i64_by_name("lsn")?
                .expect("no column named `lsn` found in query result")
        } else {
            //TODO: return error instead of panicking
            panic!("failed to get lsn");
        };

        Ok((lsn as u64).into())
    }

    pub async fn set_last_lsn(&self, dataset_id: &str, lsn: PgLsn) -> Result<(), BQError> {
        let lsn: u64 = lsn.into();

        let query = format!(
            "update `{}.{}.last_lsn` set lsn = {lsn}",
            self.project_id, dataset_id
        );

        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

        Ok(())
    }

    pub async fn insert_last_lsn_row(&self, dataset_id: &str) -> Result<(), BQError> {
        let query = format!(
            "insert into `{}.{}.last_lsn` (lsn) values (0)",
            self.project_id, dataset_id
        );

        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

        Ok(())
    }

    pub async fn get_copied_table_ids(
        &self,
        dataset_id: &str,
    ) -> Result<HashSet<TableId>, BQError> {
        let query = format!(
            "select table_id from `{}.{}.copied_tables`",
            self.project_id, dataset_id
        );

        let mut res = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

        let mut table_ids = HashSet::new();
        while res.next_row() {
            let table_id = res
                .get_i64_by_name("table_id")?
                .expect("no column named `table_id` found in query result");
            table_ids.insert(table_id as TableId);
        }

        Ok(table_ids)
    }
}

use std::collections::HashSet;

use gcp_bigquery_client::{error::BQError, model::query_request::QueryRequest, Client};
use tokio_postgres::types::{PgLsn, Type};

use crate::{
    conversions::table_row::{Cell, TableRow},
    table::{ColumnSchema, TableId, TableSchema},
};

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
        dataset_id: &str,
        table_name: &str,
        column_schemas: &[ColumnSchema],
    ) -> Result<(), BQError> {
        let columns_spec = Self::create_columns_spec(column_schemas);
        let project_id = &self.project_id;
        let query =
            format!("create table `{project_id}.{dataset_id}.{table_name}` {columns_spec}",);
        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;
        Ok(())
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
        let project_id = &self.project_id;
        let query = format!("select lsn from `{project_id}.{dataset_id}.last_lsn`",);

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

        let project_id = &self.project_id;
        let query = format!("update `{project_id}.{dataset_id}.last_lsn` set lsn = {lsn}",);

        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

        Ok(())
    }

    pub async fn insert_last_lsn_row(&self, dataset_id: &str) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let query = format!("insert into `{project_id}.{dataset_id}.last_lsn` (lsn) values (0)",);

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
        let project_id = &self.project_id;
        let query = format!("select table_id from `{project_id}.{dataset_id}.copied_tables`",);

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

    pub async fn insert_into_copied_tables(
        &self,
        dataset_id: &str,
        table_id: TableId,
    ) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let query = format!(
            "insert into `{project_id}.{dataset_id}.copied_tables` (table_id) values ({table_id})",
        );

        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

        Ok(())
    }

    pub async fn insert_row(
        &self,
        dataset_id: &str,
        table_name: &str,
        table_row: &TableRow,
    ) -> Result<(), BQError> {
        let query = self.create_insert_row_query(dataset_id, table_name, table_row);

        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
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
            Cell::TimeStamp(t) => s.push_str(&format!("'{t}'")),
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
        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;
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
            if !column.identity {
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
            if column.identity {
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
        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

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

    pub async fn truncate_table(&self, dataset_id: &str, table_name: &str) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let query = format!("truncate table `{project_id}.{dataset_id}.{table_name}`",);

        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

        Ok(())
    }

    pub async fn begin_transaction(&self) -> Result<(), BQError> {
        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new("begin transaction"))
            .await?;

        Ok(())
    }

    pub async fn commit_transaction(&self) -> Result<(), BQError> {
        let _ = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new("commit"))
            .await?;

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
}

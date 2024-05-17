use std::path::Path;

use duckdb::{params_from_iter, types::ToSqlOutput, Connection, ToSql};
use tokio_postgres::types::Type;

use crate::{
    conversions::table_row::{Cell, TableRow},
    table::{ColumnSchema, TableName, TableSchema},
};

pub struct DuckDbClient {
    conn: Connection,
}

//TODO: fix all sql injections
impl DuckDbClient {
    pub fn open_in_memory() -> Result<DuckDbClient, duckdb::Error> {
        let conn = Connection::open_in_memory()?;
        Ok(DuckDbClient { conn })
    }

    pub fn open_file<P: AsRef<Path>>(file_name: P) -> Result<DuckDbClient, duckdb::Error> {
        let conn = Connection::open(file_name)?;
        Ok(DuckDbClient { conn })
    }

    pub fn close(self) -> Result<(), duckdb::Error> {
        match self.conn.close() {
            Ok(_) => Ok(()),
            Err((_, e)) => Err(e),
        }
    }

    pub fn create_schema_if_missing(&self, schema_name: &str) -> Result<(), duckdb::Error> {
        if !self.schema_exists(schema_name)? {
            self.create_schema(schema_name)?;
        }

        Ok(())
    }

    pub fn create_schema(&self, schema_name: &str) -> Result<(), duckdb::Error> {
        let query = format!("create schema {schema_name}");
        self.conn.execute(&query, [])?;
        Ok(())
    }

    pub fn schema_exists(&self, schema_name: &str) -> Result<bool, duckdb::Error> {
        let query = "select * from information_schema.schemata where schema_name = ?;";
        let mut stmt = self.conn.prepare(query)?;
        let exists = stmt.exists([schema_name])?;
        Ok(exists)
    }

    pub fn create_table_if_missing(&self, table_schema: &TableSchema) -> Result<(), duckdb::Error> {
        if !self.table_exists(&table_schema.table_name)? {
            self.create_table(table_schema)?;
        }

        Ok(())
    }

    fn postgres_typ_to_duckdb_typ(typ: &Type) -> &'static str {
        match typ {
            &Type::INT2 | &Type::INT4 | &Type::INT8 => "integer",
            &Type::VARCHAR => "text",
            &Type::TIMESTAMP => "timestamp",
            typ => panic!("duckdb doesn't yet support type {typ}"),
        }
    }

    fn duckdb_column_spec(column_schema: &ColumnSchema, s: &mut String) {
        s.push_str(&column_schema.name);
        s.push(' ');
        let typ = Self::postgres_typ_to_duckdb_typ(&column_schema.typ);
        s.push_str(typ);
        if column_schema.identity {
            s.push_str(" primary key");
        };
    }

    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> String {
        let mut s = String::new();
        s.push('(');

        for (i, column_schema) in column_schemas.iter().enumerate() {
            Self::duckdb_column_spec(column_schema, &mut s);
            if i < column_schemas.len() - 1 {
                s.push_str(", ");
            }
        }

        s.push(')');

        s
    }

    pub fn create_table(&self, schema: &TableSchema) -> Result<(), duckdb::Error> {
        let columns_spec = Self::create_columns_spec(&schema.column_schemas);
        let query = format!(
            "create table {}.{} {}",
            schema.table_name.schema, schema.table_name.name, columns_spec
        );
        self.conn.execute(&query, [])?;
        Ok(())
    }

    pub fn table_exists(&self, table_name: &TableName) -> Result<bool, duckdb::Error> {
        let query =
            "select * from information_schema.tables where table_schema = ? and table_name = ?;";
        let mut stmt = self.conn.prepare(query)?;
        let exists = stmt.exists([&table_name.schema, &table_name.name])?;
        Ok(exists)
    }

    pub fn insert_row(
        &self,
        table_name: &TableName,
        table_row: &TableRow,
    ) -> Result<(), duckdb::Error> {
        let table_name = format!("{}.{}", table_name.schema, table_name.name);
        let column_count = table_row.values.len();
        //TODO: Remove -1
        let query = Self::create_insert_row_query(&table_name, column_count - 1);
        let mut stmt = self.conn.prepare(&query)?;
        //TODO: remove take(3)
        stmt.execute(params_from_iter(
            table_row.values.iter().take(3).map(|(_, v)| v),
        ))?;

        Ok(())
    }

    fn create_insert_row_query(table_name: &str, column_count: usize) -> String {
        let mut s = String::new();

        s.push_str("insert into ");
        s.push_str(table_name);
        s.push_str(" values(");
        s.push_str(&Self::repeat_vars(column_count));
        //TODO: replace with s.push(')')
        s.push_str(", now())");

        s
    }

    fn repeat_vars(count: usize) -> String {
        assert_ne!(count, 0);
        let mut s = " ?,".repeat(count);
        s.pop();
        s
    }
}

impl ToSql for Cell {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        match self {
            Cell::Bool(b) => b.to_sql(),
            Cell::String(s) => s.to_sql(),
            Cell::I16(i) => i.to_sql(),
            Cell::I32(i) => i.to_sql(),
            Cell::I64(i) => i.to_sql(),
            Cell::TimeStamp(t) => t.to_sql(),
        }
    }
}

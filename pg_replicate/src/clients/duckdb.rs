use duckdb::Connection;
use tokio_postgres::types::Type;

use crate::table::{ColumnSchema, TableName, TableSchema};

pub struct DuckDbClient {
    conn: Connection,
}

//TODO: fix all sql injections
impl DuckDbClient {
    pub fn open_in_memory() -> Result<DuckDbClient, duckdb::Error> {
        let conn = Connection::open_in_memory()?;
        Ok(DuckDbClient { conn })
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
}

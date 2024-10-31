use std::{collections::HashSet, path::Path};

use duckdb::{
    params_from_iter,
    types::{Null, ToSqlOutput},
    Config, Connection, ToSql,
};
use tokio_postgres::types::{PgLsn, Type};

use crate::{
    conversions::{table_row::TableRow, Cell},
    table::{ColumnSchema, TableId, TableName, TableSchema},
};

pub struct DuckDbClient {
    conn: Connection,
    current_database: String,
}

//TODO: fix all sql injections
impl DuckDbClient {
    pub fn open_in_memory() -> Result<DuckDbClient, duckdb::Error> {
        let conn = Connection::open_in_memory()?;
        let current_database = Self::current_database(&conn)?;
        Ok(DuckDbClient {
            conn,
            current_database,
        })
    }

    pub fn open_file<P: AsRef<Path>>(file_name: P) -> Result<DuckDbClient, duckdb::Error> {
        let conn = Connection::open(file_name)?;
        let current_database = Self::current_database(&conn)?;
        Ok(DuckDbClient {
            conn,
            current_database,
        })
    }

    pub fn open_mother_duck(
        access_token: &str,
        db_name: &str,
    ) -> Result<DuckDbClient, duckdb::Error> {
        let conf = Config::default()
            .with("motherduck_token", access_token)?
            .with("custom_user_agent", "pg_replicate")?;

        let conn = Connection::open_with_flags(format!("md:{db_name}"), conf)?;
        let current_database = Self::current_database(&conn)?;
        Ok(DuckDbClient {
            conn,
            current_database,
        })
    }

    fn current_database(conn: &Connection) -> Result<String, duckdb::Error> {
        let mut stmt = conn.prepare("select current_database()")?;
        let mut rows = stmt.query([])?;

        let row = rows
            .next()?
            .expect("no rows returned when getting current database");
        row.get(0)
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
        let query =
            "select * from information_schema.schemata where catalog_name = ? and schema_name = ?;";
        let mut stmt = self.conn.prepare(query)?;
        let exists = stmt.exists([&self.current_database, schema_name])?;
        Ok(exists)
    }

    pub fn create_table_if_missing(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<bool, duckdb::Error> {
        if self.table_exists(table_name)? {
            Ok(false)
        } else {
            self.create_table(table_name, column_schemas)?;
            Ok(true)
        }
    }

    fn postgres_typ_to_duckdb_typ(typ: &Type) -> &'static str {
        match typ {
            &Type::INT2 | &Type::INT4 | &Type::INT8 => "integer",
            &Type::BOOL => "bool",
            &Type::BYTEA => "bytea",
            &Type::VARCHAR | &Type::BPCHAR => "text",
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

    pub fn create_table(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<(), duckdb::Error> {
        let columns_spec = Self::create_columns_spec(column_schemas);
        let query = format!(
            "create table {}.{} {}",
            table_name.schema, table_name.name, columns_spec
        );
        self.conn.execute(&query, [])?;
        Ok(())
    }

    pub fn table_exists(&self, table_name: &TableName) -> Result<bool, duckdb::Error> {
        let query =
            "select * from information_schema.tables where table_catalog = ? and table_schema = ? and table_name = ?;";
        let mut stmt = self.conn.prepare(query)?;
        let exists = stmt.exists([&self.current_database, &table_name.schema, &table_name.name])?;
        Ok(exists)
    }

    pub fn insert_row(
        &self,
        table_name: &TableName,
        table_row: &TableRow,
    ) -> Result<(), duckdb::Error> {
        let table_name = format!("{}.{}", table_name.schema, table_name.name);
        let column_count = table_row.values.len();
        let query = Self::create_insert_row_query(&table_name, column_count);
        let mut stmt = self.conn.prepare(&query)?;
        stmt.execute(params_from_iter(table_row.values.iter()))?;

        Ok(())
    }

    fn create_insert_row_query(table_name: &str, column_count: usize) -> String {
        let mut s = String::new();

        s.push_str("insert into ");
        s.push_str(table_name);
        s.push_str(" values(");
        s.push_str(&Self::repeat_vars(column_count));
        s.push(')');

        s
    }

    fn repeat_vars(count: usize) -> String {
        assert_ne!(count, 0);
        let mut s = " ?,".repeat(count);
        s.pop();
        s
    }

    pub fn update_row(
        &self,
        table_schema: &TableSchema,
        table_row: &TableRow,
    ) -> Result<(), duckdb::Error> {
        let table_name = &table_schema.table_name;
        let column_schemas = &table_schema.column_schemas;
        let table_name = format!("{}.{}", table_name.schema, table_name.name);
        let query = Self::create_update_row_query(&table_name, column_schemas);
        let mut stmt = self.conn.prepare(&query)?;
        let non_identity_cells = column_schemas
            .iter()
            .zip(table_row.values.iter())
            .filter(|(s, _)| !s.identity)
            .map(|(_, c)| c);
        let identity_cells = column_schemas
            .iter()
            .zip(table_row.values.iter())
            .filter(|(s, _)| s.identity)
            .map(|(_, c)| c);
        stmt.execute(params_from_iter(non_identity_cells.chain(identity_cells)))?;
        Ok(())
    }

    fn create_update_row_query(table_name: &str, column_schemas: &[ColumnSchema]) -> String {
        let mut s = String::new();

        s.push_str("update ");
        s.push_str(table_name);
        s.push_str(" set ");

        let mut remove_comma = false;
        let non_identity_columns = column_schemas.iter().filter(|s| !s.identity);
        for column in non_identity_columns {
            s.push_str(&column.name);
            s.push_str(" = ?,");
            remove_comma = true;
        }
        if remove_comma {
            s.pop();
        }

        Self::add_identities_where_clause(&mut s, column_schemas);

        s
    }

    /// Adds a where clause for the identity columns
    fn add_identities_where_clause(s: &mut String, column_schemas: &[ColumnSchema]) {
        s.push_str(" where ");

        let mut remove_and = false;
        let identity_columns = column_schemas.iter().filter(|s| s.identity);
        for column in identity_columns {
            s.push_str(&column.name);
            s.push_str(" = ? and ");
            remove_and = true;
        }
        if remove_and {
            s.pop(); //' '
            s.pop(); //'d'
            s.pop(); //'n'
            s.pop(); //'a'
            s.pop(); //' '
        }
    }

    pub fn delete_row(
        &self,
        table_schema: &TableSchema,
        table_row: &TableRow,
    ) -> Result<(), duckdb::Error> {
        let table_name = &table_schema.table_name;
        let column_schemas = &table_schema.column_schemas;
        let table_name = format!("{}.{}", table_name.schema, table_name.name);
        let query = Self::create_delete_row_query(&table_name, column_schemas);
        let mut stmt = self.conn.prepare(&query)?;
        let identity_cells = column_schemas
            .iter()
            .zip(table_row.values.iter())
            .filter(|(s, _)| s.identity)
            .map(|(_, c)| c);
        stmt.execute(params_from_iter(identity_cells))?;
        Ok(())
    }

    fn create_delete_row_query(table_name: &str, column_schemas: &[ColumnSchema]) -> String {
        let mut s = String::new();

        s.push_str("delete from ");
        s.push_str(table_name);

        Self::add_identities_where_clause(&mut s, column_schemas);

        s
    }

    pub fn get_copied_table_ids(&self) -> Result<HashSet<TableId>, duckdb::Error> {
        let mut stmt = self
            .conn
            .prepare("select table_id from pg_replicate.copied_tables")?;
        let mut rows = stmt.query([])?;

        let mut res = HashSet::new();
        while let Some(row) = rows.next()? {
            res.insert(row.get(0)?);
        }

        Ok(res)
    }

    pub fn get_last_lsn(&self) -> Result<PgLsn, duckdb::Error> {
        let mut stmt = self.conn.prepare("select lsn from pg_replicate.last_lsn")?;
        let lsn = stmt.query_row::<u64, _, _>([], |r| r.get(0))?;
        Ok(lsn.into())
    }

    pub fn set_last_lsn(&self, lsn: PgLsn) -> Result<(), duckdb::Error> {
        let lsn: u64 = lsn.into();
        let mut stmt = self
            .conn
            .prepare("update pg_replicate.last_lsn set lsn = ?")?;
        stmt.execute([lsn])?;
        Ok(())
    }

    pub fn insert_last_lsn_row(&self) -> Result<(), duckdb::Error> {
        self.conn
            .execute("insert into pg_replicate.last_lsn values (0)", [])?;
        Ok(())
    }

    pub fn insert_into_copied_tables(&self, table_id: TableId) -> Result<(), duckdb::Error> {
        let mut stmt = self
            .conn
            .prepare("insert into pg_replicate.copied_tables values (?)")?;
        stmt.execute([table_id])?;

        Ok(())
    }

    pub fn truncate_table(&self, table_name: &TableName) -> Result<(), duckdb::Error> {
        let query = format!("delete from {}.{}", table_name.schema, table_name.name);
        let mut stmt = self.conn.prepare(&query)?;
        stmt.execute([])?;
        Ok(())
    }

    pub fn begin_transaction(&self) -> Result<(), duckdb::Error> {
        let mut stmt = self.conn.prepare("begin transaction")?;
        stmt.execute([])?;
        Ok(())
    }

    pub fn commit_transaction(&self) -> Result<(), duckdb::Error> {
        let mut stmt = self.conn.prepare("commit")?;
        stmt.execute([])?;
        Ok(())
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
            Cell::TimeStampTz(t) => t.to_sql(),
            Cell::Null => Null.to_sql(),
            Cell::Bytes(b) => b.to_sql(),
        }
    }
}

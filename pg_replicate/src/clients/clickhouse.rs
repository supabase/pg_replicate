
use clickhouse::Client;

// use crate::{
//     conversions::{table_row::TableRow, Cell},
//     table::{ColumnSchema, TableId, TableName, TableSchema},
// };


pub struct ClickhouseClient {
    client: Client,
}

pub struct ClickhouseConfig {
    url: String,
    user: String,
    pasword: String,
    database: String
}

impl ClickhouseClient {
    pub fn create_client(config: &ClickhouseConfig) -> Result<ClickhouseClient, clickhouse::error::Error> {
        let client = Client::default()
                // should include both protocol and port
                .with_url(config.url)
                .with_user(config.user)
                .with_password(config.password)
                .with_database(config.database);
        Ok(ClickhouseClient {
            client
        })
    }

    // fn async_insert(client: &Client) -> 



    pub fn create_table_if_missing(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<bool, clickhouse::error::Error> {
        if self.table_exists(table_name)? {
            Ok(false)
        } else {
            self.create_table(table_name, column_schemas)?;
            Ok(true)
        }
    }



    pub fn create_table(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<(), clickhouse::error::Error> {
        let columns_spec = Self::create_columns_spec(column_schemas);
        let query = format!(
            "create table {}.{} {}",
            table_name.schema, table_name.name, columns_spec
        );
        self.conn.execute(&query, [])?;
        Ok(())
    }

    pub fn table_exists(&self, table_name: &TableName) -> Result<bool, clickhouse::error::Error> {
        let query =
            "select * from information_schema.tables where table_catalog = ? and table_schema = ? and table_name = ?;";
        let mut stmt = self.conn.prepare(query)?;
        let exists = stmt.exists([&self.current_database, &table_name.schema, &table_name.name])?;
        Ok(exists)
    }

    pub fn insert_rows(
        &self,
        table_name: &TableName,
        table_rows: &Vec<TableRow>,
    ) -> Result<(), clickhouse::error::Error> {
        let table_name = format!("{}.{}", table_name.schema, table_name.name);
        let column_count = table_row.values.len();
        let query = Self::create_insert_row_query(&table_name, column_count);
        let mut stmt = self.conn.prepare(&query)?;
        stmt.execute(params_from_iter(table_row.values.iter()))?;

        Ok(())
    }

}
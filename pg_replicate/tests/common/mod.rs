use postgres::schema::TableName;

pub mod database;
pub mod pipeline;

/// Creates a [`TableName`] on the `test` schema.
pub fn test_table_name(name: &str) -> TableName {
    TableName {
        schema: "test".to_owned(),
        name: name.to_owned(),
    }
}

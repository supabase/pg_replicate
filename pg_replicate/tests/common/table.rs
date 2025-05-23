use crate::common::sink::TestSink;
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use std::collections::HashMap;
use tokio_postgres::types::Type;

pub fn id_column_schema() -> ColumnSchema {
    ColumnSchema {
        name: "id".to_string(),
        typ: Type::INT8,
        modifier: -1,
        nullable: false,
        primary: true,
    }
}

pub fn assert_table_schema_from_sink(
    sink: &TestSink,
    table_id: TableId,
    schema_index: usize,
    expected_table_name: TableName,
    expected_columns: &[ColumnSchema],
) {
    let tables_schemas = &sink.get_tables_schemas()[schema_index];

    assert_table_schema(
        tables_schemas,
        table_id,
        expected_table_name,
        expected_columns,
    )
}

pub fn assert_table_schema(
    tables_schemas: &HashMap<TableId, TableSchema>,
    table_id: TableId,
    expected_table_name: TableName,
    expected_columns: &[ColumnSchema],
) {
    let table_schema = tables_schemas.get(&table_id).unwrap();

    assert_eq!(table_schema.table_id, table_id);
    assert_eq!(table_schema.table_name, expected_table_name);

    let columns = &table_schema.column_schemas;
    assert_eq!(columns.len(), expected_columns.len());

    for (actual, expected) in columns.iter().zip(expected_columns.iter()) {
        assert_eq!(actual.name, expected.name);
        assert_eq!(actual.typ, expected.typ);
        assert_eq!(actual.modifier, expected.modifier);
        assert_eq!(actual.nullable, expected.nullable);
        assert_eq!(actual.primary, expected.primary);
    }
}

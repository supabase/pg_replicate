use crate::common::sink::TestSink;
use postgres::schema::{ColumnSchema, TableId, TableName};

pub fn assert_table_schema(
    sink: &TestSink,
    table_id: TableId,
    schema_index: usize,
    expected_table_name: TableName,
    expected_columns: &[ColumnSchema],
) {
    let tables_schemas = &sink.get_tables_schemas()[schema_index];
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

use std::fmt::Display;

use tokio_postgres::types::Type;

use crate::escape::quote_identifier;

#[derive(Debug, Clone)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}

impl TableName {
    pub fn as_quoted_identifier(&self) -> String {
        let quoted_schema = quote_identifier(&self.schema);
        let quoted_name = quote_identifier(&self.name);
        format!("{quoted_schema}.{quoted_name}")
    }
}

impl Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{0}.{1}", self.schema, self.name))
    }
}

type TypeModifier = i32;

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub typ: Type,
    pub modifier: TypeModifier,
    pub nullable: bool,
    pub identity: bool,
}

pub type TableId = u32;

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_name: TableName,
    pub table_id: TableId,
    pub column_schemas: Vec<ColumnSchema>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_as_quoted_identifier() {
        let table = TableName {
            schema: "public".to_string(),
            name: "users".to_string(),
        };
        assert_eq!(table.as_quoted_identifier(), "public.users");
    }

    #[test]
    fn test_table_name_as_quoted_identifier_with_quotes() {
        let table = TableName {
            schema: "public".to_string(),
            name: "user\"s".to_string(),
        };
        // The name should have doubled quotes
        assert_eq!(table.as_quoted_identifier(), "public.user\"\"s");
    }

    #[test]
    fn test_table_name_display() {
        let table = TableName {
            schema: "public".to_string(),
            name: "users".to_string(),
        };
        assert_eq!(format!("{}", table), "public.users");
    }

    #[test]
    fn test_table_schema_creation() {
        let table_name = TableName {
            schema: "public".to_string(),
            name: "users".to_string(),
        };
        
        let column = ColumnSchema {
            name: "id".to_string(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
            identity: true,
        };
        
        let table_schema = TableSchema {
            table_name,
            table_id: 12345,
            column_schemas: vec![column],
        };
        
        assert_eq!(table_schema.table_id, 12345);
        assert_eq!(table_schema.table_name.schema, "public");
        assert_eq!(table_schema.table_name.name, "users");
        assert_eq!(table_schema.column_schemas.len(), 1);
        assert_eq!(table_schema.column_schemas[0].name, "id");
        assert!(!table_schema.column_schemas[0].nullable);
        assert!(table_schema.column_schemas[0].identity);
    }
}

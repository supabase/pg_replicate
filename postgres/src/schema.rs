use std::cmp::Ordering;
use std::fmt;

use pg_escape::quote_identifier;
use tokio_postgres::types::Type;

/// An object identifier in PostgreSQL.
pub type Oid = u32;

/// A fully qualified PostgreSQL table name consisting of a schema and table name.
///
/// This type represents a table identifier in PostgreSQL, which requires both a schema name
/// and a table name. It provides methods for formatting the name in different contexts.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct TableName {
    /// The schema name containing the table
    pub schema: String,
    /// The name of the table within the schema
    pub name: String,
}

impl TableName {
    pub fn new(schema: String, name: String) -> TableName {
        Self { schema, name }
    }

    /// Returns the table name as a properly quoted PostgreSQL identifier.
    ///
    /// This method ensures the schema and table names are properly escaped according to
    /// PostgreSQL identifier quoting rules.
    pub fn as_quoted_identifier(&self) -> String {
        let quoted_schema = quote_identifier(&self.schema);
        let quoted_name = quote_identifier(&self.name);
        format!("{quoted_schema}.{quoted_name}")
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{0}.{1}", self.schema, self.name))
    }
}

/// A type alias for PostgreSQL type modifiers.
///
/// Type modifiers in PostgreSQL are used to specify additional type-specific attributes,
/// such as length for varchar or precision for numeric types.
type TypeModifier = i32;

/// Represents the schema of a single column in a PostgreSQL table.
///
/// This type contains all metadata about a column including its name, data type,
/// type modifier, nullability, and whether it's part of the primary key.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnSchema {
    /// The name of the column
    pub name: String,
    /// The PostgreSQL data type of the column
    pub typ: Type,
    /// Type-specific modifier value (e.g., length for varchar)
    pub modifier: TypeModifier,
    /// Whether the column can contain NULL values
    pub nullable: bool,
    /// Whether the column is part of the table's primary key
    pub primary: bool,
}

impl ColumnSchema {
    pub fn new(
        name: String,
        typ: Type,
        modifier: TypeModifier,
        nullable: bool,
        primary: bool,
    ) -> ColumnSchema {
        Self {
            name,
            typ,
            modifier,
            nullable,
            primary,
        }
    }

    /// Compares two [`ColumnSchema`] instances, excluding the `nullable` field.
    ///
    /// Return `true` if all fields except `nullable` are equal, `false` otherwise.
    ///
    /// This method is used for comparing table schemas loaded via the initial table sync and the
    /// relation messages received via CDC. The reason for skipping the `nullable` field is that
    /// unfortunately Postgres doesn't seem to propagate nullable information of a column via
    /// relation messages.
    fn partial_eq(&self, other: &ColumnSchema) -> bool {
        self.name == other.name
            && self.typ == other.typ
            && self.modifier == other.modifier
            && self.primary == other.primary
    }
}

/// A type alias for PostgreSQL table OIDs.
///
/// Table OIDs are unique identifiers assigned to tables in PostgreSQL.
// TODO: delete this in favor of `Oid`.
pub type TableId = u32;

/// Represents the complete schema of a PostgreSQL table.
///
/// This type contains all metadata about a table including its name, OID,
/// and the schemas of all its columns.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableSchema {
    /// The PostgreSQL OID of the table
    pub id: Oid,
    /// The fully qualified name of the table
    pub name: TableName,
    /// The schemas of all columns in the table
    pub column_schemas: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(id: Oid, name: TableName, column_schemas: Vec<ColumnSchema>) -> Self {
        Self {
            id,
            name,
            column_schemas,
        }
    }

    /// Returns whether the table has any primary key columns.
    ///
    /// This method checks if any column in the table is marked as part of the primary key.
    pub fn has_primary_keys(&self) -> bool {
        self.column_schemas.iter().any(|cs| cs.primary)
    }

    /// Compares two [`TableSchema`] instances, excluding the [`ColumnSchema`]'s `nullable` field.
    ///
    /// Return `true` if all fields except `nullable` are equal, `false` otherwise.
    pub fn partial_eq(&self, other: &TableSchema) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.column_schemas.len() == other.column_schemas.len()
            && self
                .column_schemas
                .iter()
                .zip(other.column_schemas.iter())
                .all(|(c1, c2)| c1.partial_eq(c2))
    }
}

impl PartialOrd for TableSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TableSchema {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

use postgres::schema::TableName;
use postgres::tokio::options::PgDatabaseOptions;
use postgres::tokio::test_utils::PgDatabase;
use tokio_postgres::config::SslMode;
use uuid::Uuid;

/// The default schema name used for test tables.
const TEST_DATABASE_SCHEMA: &str = "test";

/// Creates a [`TableName`] in the test schema.
///
/// This helper function constructs a [`TableName`] with the schema set to the test schema
/// and the provided name as the table name.
pub fn test_table_name(name: &str) -> TableName {
    TableName {
        schema: TEST_DATABASE_SCHEMA.to_owned(),
        name: name.to_owned(),
    }
}

/// Creates a new test database instance.
///
/// This function spawns a new PostgreSQL database with a random UUID as its name,
/// using default credentials and disabled SSL. It also creates a test schema
/// for organizing test tables.
///
/// # Panics
///
/// Panics if the test schema cannot be created.
pub async fn spawn_database() -> PgDatabase {
    let options = PgDatabaseOptions {
        host: "localhost".to_owned(),
        port: 5430,
        // We create a random database name to avoid conflicts with existing databases.
        name: Uuid::new_v4().to_string(),
        username: "postgres".to_owned(),
        password: Some("postgres".to_owned()),
        ssl_mode: SslMode::Disable,
    };

    let database = PgDatabase::new(options).await;

    // Create the test schema.
    database
        .client
        .execute(&format!("CREATE SCHEMA {}", TEST_DATABASE_SCHEMA), &[])
        .await
        .expect("Failed to create test schema");

    database
}

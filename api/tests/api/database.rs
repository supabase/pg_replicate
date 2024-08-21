use api::configuration::{get_configuration, DatabaseSettings};
use sqlx::{Connection, Executor, PgConnection, PgPool, Row};

pub async fn configure_database(config: &DatabaseSettings) -> PgPool {
    // Create database
    let mut connection = PgConnection::connect_with(&config.without_db())
        .await
        .expect("Failed to connect to Postgres");
    connection
        .execute(&*format!(r#"CREATE DATABASE "{}";"#, config.name))
        .await
        .expect("Failed to create database.");

    // Migrate database
    let connection_pool = PgPool::connect_with(config.with_db())
        .await
        .expect("Failed to connect to Postgres.");
    sqlx::migrate!("./migrations")
        .run(&connection_pool)
        .await
        .expect("Failed to migrate the database");

    connection_pool
}

// This is not an actual test. It is only used to delete test databases.
// Enabling it might interfere with other running tests, so keep the
// #[ignore] attribute. But remember to temporarily comment it out before
// running the test when you do want to cleanup the database.
#[ignore]
#[tokio::test]
async fn delete_test_databases() {
    delete_all_test_databases().await;
}

async fn delete_all_test_databases() {
    let config = get_configuration().expect("Failed to read configuration");
    let mut connection = PgConnection::connect_with(&config.database.without_db())
        .await
        .expect("Failed to connect to Postgres");
    let databases = connection
        .fetch_all(&*format!(r#"select datname from pg_database where datname not in ('postgres', 'template0', 'template1');"#))
        .await
        .expect("Failed to get databases.");
    for database in databases {
        let database_name: String = database.get("datname");
        connection
            .execute(&*format!(r#"drop database "{database_name}""#))
            .await
            .expect("Failed to delete database");
    }
}

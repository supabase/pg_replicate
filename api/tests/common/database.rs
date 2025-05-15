use api::configuration::DatabaseSettings;
use sqlx::{Connection, Executor, PgConnection, PgPool};

pub async fn create_and_configure_database(settings: &DatabaseSettings) -> PgPool {
    // Create the database via a single connection.
    let mut connection = PgConnection::connect_with(&settings.without_db())
        .await
        .expect("Failed to connect to Postgres");
    connection
        .execute(&*format!(r#"CREATE DATABASE "{}";"#, settings.name))
        .await
        .expect("Failed to create database.");

    // Create a connection pool to the database and run the migration.
    let connection_pool = PgPool::connect_with(settings.with_db())
        .await
        .expect("Failed to connect to Postgres.");
    sqlx::migrate!("./migrations")
        .run(&connection_pool)
        .await
        .expect("Failed to migrate the database");

    connection_pool
}

pub async fn destroy_database(settings: &DatabaseSettings) {
    // Connect to the default database.
    let mut connection = PgConnection::connect_with(&settings.without_db())
        .await
        .expect("Failed to connect to Postgres");

    // Terminate any remaining connections to the database. This code assumes that those connections
    // are not used anymore and do not outlive the `TestApp` instance.
    connection
        .execute(&*format!(
            r#"
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{}'
            AND pid <> pg_backend_pid();"#,
            settings.name
        ))
        .await
        .expect("Failed to terminate database connections");
    
    // Drop the database
    connection
        .execute(&*format!(r#"DROP DATABASE IF EXISTS "{}";"#, settings.name))
        .await
        .expect("Failed to destroy database.");
}

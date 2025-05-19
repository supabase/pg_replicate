use crate::tokio::options::PgDatabaseOptions;
use tokio::runtime::Handle;
use tokio_postgres::{Client, NoTls};

struct PgDatabase {
    options: PgDatabaseOptions,
    client: Client,
}

impl PgDatabase {
    pub async fn new(options: PgDatabaseOptions) -> Self {
        let client = create_pg_database(&options).await;

        Self { options, client }
    }
}

impl Drop for PgDatabase {
    fn drop(&mut self) {
        // To use `block_in_place,` we need a multithreaded runtime since when a blocking
        // task is issued, the runtime will offload existing tasks to another worker.
        tokio::task::block_in_place(move || {
            Handle::current().block_on(async move { drop_pg_database(&self.options).await });
        });
    }
}

/// Creates a new PostgreSQL database and returns a client connected to it.
///
/// Establishes a connection to the PostgreSQL server using the provided options,
/// creates a new database, and returns a [`Client`] connected to the new database.
/// Panics if the connection fails or if database creation fails.
pub async fn create_pg_database(options: &PgDatabaseOptions) -> Client {
    // Create the database via a single connection
    let (client, connection) = options
        .without_db()
        .connect(NoTls)
        .await
        .expect("Failed to connect to Postgres");

    // Spawn the connection on a new task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create the database
    client
        .execute(&*format!(r#"CREATE DATABASE "{}";"#, options.name), &[])
        .await
        .expect("Failed to create database");

    // Create a new client connected to the created database
    let (client, connection) = options
        .with_db()
        .connect(NoTls)
        .await
        .expect("Failed to connect to Postgres");

    // Spawn the connection on a new task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
}

/// Drops a PostgreSQL database and cleans up all connections.
///
/// Connects to the PostgreSQL server, forcefully terminates all active connections
/// to the target database, and drops the database if it exists. Useful for cleaning
/// up test databases. Takes a reference to [`PgDatabaseOptions`] specifying the database
/// to drop. Panics if any operation fails.
pub async fn drop_pg_database(options: &PgDatabaseOptions) {
    // Connect to the default database
    let (client, connection) = options
        .without_db()
        .connect(NoTls)
        .await
        .expect("Failed to connect to Postgres");

    // Spawn the connection on a new task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Forcefully terminate any remaining connections to the database
    client
        .execute(
            &*format!(
                r#"
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{}'
                AND pid <> pg_backend_pid();"#,
                options.name
            ),
            &[],
        )
        .await
        .expect("Failed to terminate database connections");

    // Drop the database
    client
        .execute(
            &*format!(r#"DROP DATABASE IF EXISTS "{}";"#, options.name),
            &[],
        )
        .await
        .expect("Failed to destroy database");
}

use crate::schema::{TableId, TableName};
use crate::tokio::options::PgDatabaseOptions;
use tokio::runtime::Handle;
use tokio_postgres::{Client, NoTls};

pub struct PgDatabase {
    pub options: PgDatabaseOptions,
    pub client: Client,
}

impl PgDatabase {
    pub async fn new(options: PgDatabaseOptions) -> Self {
        let client = create_pg_database(&options).await;

        Self { options, client }
    }

    /// Creates a new publication for the specified tables.
    pub async fn create_publication(
        &self,
        publication_name: &str,
        table_names: &[TableName],
    ) -> Result<String, tokio_postgres::Error> {
        let table_names = table_names
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>();

        let create_publication_query = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            publication_name,
            table_names
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        self.client.execute(&create_publication_query, &[]).await?;

        Ok(publication_name.to_string())
    }

    /// Creates a new table with the specified name and columns.
    pub async fn create_table(
        &self,
        table_name: TableName,
        columns: &[(&str, &str)], // (column_name, column_type)
    ) -> Result<TableId, tokio_postgres::Error> {
        let columns_str = columns
            .iter()
            .map(|(name, typ)| format!("{} {}", name, typ.to_string()))
            .collect::<Vec<_>>()
            .join(", ");

        let create_table_query = format!(
            "CREATE TABLE {} (id BIGSERIAL PRIMARY KEY, {})",
            table_name.as_quoted_identifier(),
            columns_str
        );
        self.client.execute(&create_table_query, &[]).await?;

        // Get the OID of the newly created table
        let row = self
            .client
            .query_one(
                "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
            WHERE n.nspname = $1 AND c.relname = $2",
                &[&table_name.schema, &table_name.name],
            )
            .await?;

        let table_id: TableId = row.get(0);

        Ok(table_id)
    }

    /// Inserts values into the specified table.
    pub async fn insert_values(
        &self,
        table_name: TableName,
        columns: &[&str],
        values: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        let columns_str = columns.join(", ");
        let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("${}", i)).collect();
        let placeholders_str = placeholders.join(", ");

        let insert_query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name.as_quoted_identifier(),
            columns_str,
            placeholders_str
        );

        self.client.execute(&insert_query, values).await
    }

    /// Updates all rows in the specified table with the given values.
    pub async fn update_values(
        &self,
        table_name: TableName,
        columns: &[&str],
        values: &[&str],
    ) -> Result<u64, tokio_postgres::Error> {
        let set_clauses: Vec<String> = columns
            .iter()
            .zip(values.iter())
            .map(|(col, val)| format!("{} = {}", col, val))
            .collect();
        let set_clause = set_clauses.join(", ");

        let update_query = format!(
            "UPDATE {} SET {}",
            table_name.as_quoted_identifier(),
            set_clause
        );

        self.client.execute(&update_query, &[]).await
    }

    /// Queries rows from a single column of a table.
    pub async fn query_table<T>(
        &self,
        table_name: &TableName,
        column: &str,
        where_clause: Option<&str>,
    ) -> Result<Vec<T>, tokio_postgres::Error>
    where
        T: for<'a> tokio_postgres::types::FromSql<'a>,
    {
        let where_str = where_clause.map_or(String::new(), |w| format!(" WHERE {}", w));
        let query = format!(
            "SELECT {} FROM {}{}",
            column,
            table_name.as_quoted_identifier(),
            where_str
        );

        let rows = self.client.query(&query, &[]).await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
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

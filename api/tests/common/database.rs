use postgres::options::PgDatabaseOptions;
use postgres::test_utils::create_pg_database;
use sqlx::PgPool;

/// Creates and configures a new PostgreSQL database for the API.
///
/// Similar to [`create_pg_database`], but additionally runs all database migrations
/// from the "./migrations" directory after creation. Returns a [`PgPool`]
/// connected to the newly created and migrated database. Panics if database
/// creation or migration fails.
pub async fn create_pg_replicate_api_database(options: &PgDatabaseOptions) -> PgPool {
    let connection_pool = create_pg_database(&options).await;

    sqlx::migrate!("./migrations")
        .run(&connection_pool)
        .await
        .expect("Failed to migrate the database");

    connection_pool
}

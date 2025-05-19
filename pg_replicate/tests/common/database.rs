use tokio_postgres::config::SslMode;
use uuid::Uuid;
use postgres::schema::TableName;
use postgres::tokio::options::PgDatabaseOptions;
use postgres::tokio::test_utils::PgDatabase;

pub async fn spawn_database() -> PgDatabase {
    let options = PgDatabaseOptions {
        host: "localhost".to_owned(),
        port: 5430,
        name: Uuid::new_v4().to_string(),
        username: "postgres".to_owned(),
        password: Some("postgres".to_owned()),
        ssl_mode: SslMode::Disable,
    };

    PgDatabase::new(options).await
}

pub async fn spawn_database_with_publication(
    table_names: Vec<TableName>,
    publication_name: Option<String>,
) -> PgDatabase {
    let database = spawn_database().await;

    if let Some(publication_name) = publication_name {
        database
            .create_publication(&publication_name, &table_names)
            .await
            .expect("Error while creating a publication");
    }

    database
}
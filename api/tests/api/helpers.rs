use std::net::TcpListener;

use api::{
    configuration::{get_configuration, DatabaseSettings},
    startup::{get_connection_pool, run},
};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, Executor, PgConnection, PgPool, Row};
use uuid::Uuid;

pub struct TestApp {
    pub address: String,
    pub api_client: reqwest::Client,
}

#[derive(Serialize)]
pub struct TenantRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct TenantIdResponse {
    pub id: i64,
}

#[derive(Deserialize)]
pub struct TenantResponse {
    pub id: i64,
    pub name: String,
}

impl TestApp {
    pub async fn create_tenant(&self, tenant: &TenantRequest) -> reqwest::Response {
        self.api_client
            .post(&format!("{}/v1/tenants", &self.address))
            .json(tenant)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_tenant(&self, tenant_id: i64) -> reqwest::Response {
        self.api_client
            .get(&format!("{}/v1/tenants/{tenant_id}", &self.address))
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn update_tenant(&self, tenant_id: i64, tenant: &TenantRequest) -> reqwest::Response {
        self.api_client
            .post(&format!("{}/v1/tenants/{tenant_id}", &self.address))
            .json(tenant)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn delete_tenant(&self, tenant_id: i64) -> reqwest::Response {
        self.api_client
            .delete(&format!("{}/v1/tenants/{tenant_id}", &self.address))
            .send()
            .await
            .expect("Failed to execute request.")
    }
}

pub async fn spawn_app() -> TestApp {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind random port");
    let port = listener.local_addr().unwrap().port();
    let mut configuration = get_configuration().expect("Failed to read configuration");
    configuration.database.name = Uuid::new_v4().to_string();
    let connection_pool = get_connection_pool(&configuration.database);
    configure_database(&configuration.database).await;
    let server = run(listener, connection_pool.clone())
        .await
        .expect("failed to bind address");
    tokio::spawn(server);
    let address = format!("http://127.0.0.1:{port}");
    let api_client = reqwest::Client::new();
    TestApp {
        address,
        api_client,
    }
}

async fn configure_database(config: &DatabaseSettings) -> PgPool {
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

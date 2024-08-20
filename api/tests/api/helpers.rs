use std::net::TcpListener;

use api::{
    configuration::get_configuration,
    startup::{get_connection_pool, run},
};
use serde::{Deserialize, Serialize};

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
    pub async fn post_tenant(&self, tenant: &TenantRequest) -> reqwest::Response {
        self.api_client
            .post(&format!("{}/v1/tenants", &self.address))
            .json(tenant)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn get_tenant(&self, tenant_id: i64) -> reqwest::Response {
        self.api_client
            .get(&format!("{}/v1/tenants/{tenant_id}", &self.address))
            .send()
            .await
            .expect("failed to execute request")
    }
}

pub async fn spawn_app() -> TestApp {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind random port");
    let port = listener.local_addr().unwrap().port();
    let configuration = get_configuration().expect("Failed to read configuration");
    let connection_pool = get_connection_pool(&configuration.database);
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

use std::net::TcpListener;

use api::{
    configuration::get_configuration,
    db::{pipelines::PipelineConfig, sinks::SinkConfig, sources::SourceConfig},
    startup::{get_connection_pool, run},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::database::configure_database;

pub struct TestApp {
    pub address: String,
    pub api_client: reqwest::Client,
}

#[derive(Serialize)]
pub struct CreateTenantRequest {
    pub name: String,
    pub supabase_project_ref: Option<String>,
}

#[derive(Serialize)]
pub struct UpdateTenantRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct CreateTenantResponse {
    pub id: i64,
}

#[derive(Deserialize)]
pub struct TenantResponse {
    pub id: i64,
    pub name: String,
    pub supabase_project_ref: Option<String>,
    pub prefix: String,
}

#[derive(Serialize)]
pub struct CreateSourceRequest {
    pub config: SourceConfig,
}

#[derive(Deserialize)]
pub struct CreateSourceResponse {
    pub id: i64,
}

#[derive(Serialize)]
pub struct UpdateSourceRequest {
    pub config: SourceConfig,
}

#[derive(Deserialize)]
pub struct SourceResponse {
    pub id: i64,
    pub tenant_id: i64,
    pub config: SourceConfig,
}

#[derive(Serialize)]
pub struct CreateSinkRequest {
    pub config: SinkConfig,
}

#[derive(Deserialize)]
pub struct CreateSinkResponse {
    pub id: i64,
}

#[derive(Serialize)]
pub struct UpdateSinkRequest {
    pub config: SinkConfig,
}

#[derive(Deserialize)]
pub struct SinkResponse {
    pub id: i64,
    pub tenant_id: i64,
    pub config: SinkConfig,
}

#[derive(Serialize)]
pub struct CreatePipelineRequest {
    pub source_id: i64,
    pub sink_id: i64,
    pub config: PipelineConfig,
}

#[derive(Deserialize)]
pub struct CreatePipelineResponse {
    pub id: i64,
}

#[derive(Deserialize)]
pub struct PipelineResponse {
    pub id: i64,
    pub tenant_id: i64,
    pub source_id: i64,
    pub sink_id: i64,
    pub config: PipelineConfig,
}

impl TestApp {
    pub async fn create_tenant(&self, tenant: &CreateTenantRequest) -> reqwest::Response {
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

    pub async fn update_tenant(
        &self,
        tenant_id: i64,
        tenant: &UpdateTenantRequest,
    ) -> reqwest::Response {
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

    pub async fn create_source(
        &self,
        tenant_id: i64,
        source: &CreateSourceRequest,
    ) -> reqwest::Response {
        self.api_client
            .post(&format!("{}/v1/sources", &self.address))
            .header("tenant_id", tenant_id)
            .json(source)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_source(&self, tenant_id: i64, source_id: i64) -> reqwest::Response {
        self.api_client
            .get(&format!("{}/v1/sources/{source_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn update_source(
        &self,
        tenant_id: i64,
        source_id: i64,
        source: &UpdateSourceRequest,
    ) -> reqwest::Response {
        self.api_client
            .post(&format!("{}/v1/sources/{source_id}", &self.address))
            .header("tenant_id", tenant_id)
            .json(source)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn delete_source(&self, tenant_id: i64, source_id: i64) -> reqwest::Response {
        self.api_client
            .delete(&format!("{}/v1/sources/{source_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn create_sink(&self, tenant_id: i64, sink: &CreateSinkRequest) -> reqwest::Response {
        self.api_client
            .post(&format!("{}/v1/sinks", &self.address))
            .header("tenant_id", tenant_id)
            .json(sink)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_sink(&self, tenant_id: i64, sink_id: i64) -> reqwest::Response {
        self.api_client
            .get(&format!("{}/v1/sinks/{sink_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn update_sink(
        &self,
        tenant_id: i64,
        sink_id: i64,
        sink: &UpdateSinkRequest,
    ) -> reqwest::Response {
        self.api_client
            .post(&format!("{}/v1/sinks/{sink_id}", &self.address))
            .header("tenant_id", tenant_id)
            .json(sink)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn delete_sink(&self, tenant_id: i64, sink_id: i64) -> reqwest::Response {
        self.api_client
            .delete(&format!("{}/v1/sinks/{sink_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn create_pipeline(
        &self,
        tenant_id: i64,
        pipeline: &CreatePipelineRequest,
    ) -> reqwest::Response {
        self.api_client
            .post(&format!("{}/v1/pipelines", &self.address))
            .header("tenant_id", tenant_id)
            .json(pipeline)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_pipeline(&self, tenant_id: i64, pipeline_id: i64) -> reqwest::Response {
        self.api_client
            .get(&format!("{}/v1/pipelines/{pipeline_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("failed to execute request")
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

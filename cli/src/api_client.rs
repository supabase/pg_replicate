use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub struct ApiClient {
    pub address: String,
    pub client: reqwest::Client,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum SourceConfig {
    Postgres {
        /// Host on which Postgres is running
        host: String,

        /// Port on which Postgres is running
        port: u16,

        /// Postgres database name
        name: String,

        /// Postgres database user name
        username: String,

        /// Postgres database user password
        password: Option<String>,

        /// Postgres slot name
        slot_name: String,
    },
}

impl Display for SourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let SourceConfig::Postgres {
            host,
            port,
            name,
            username,
            password,
            slot_name,
        } = self;
        write!(
            f,
            "host: {host}, port: {port}, name: {name}, username: {username}, password: {password:?}, slot_name: {slot_name}",
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum SinkConfig {
    BigQuery {
        /// BigQuery project id
        project_id: String,

        /// BigQuery dataset id
        dataset_id: String,

        /// BigQuery service account key
        service_account_key: String,
    },
}

impl Display for SinkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let SinkConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key,
        } = self;
        write!(f, "project_id: {project_id}, dataset_id: {dataset_id}, service_account_key: {service_account_key}")
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PipelineConfig {
    pub config: BatchConfig,
}

impl Display for PipelineConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "config: {}", self.config)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct BatchConfig {
    /// maximum batch size in number of events
    pub max_size: usize,

    /// maximum duration, in seconds, to wait for a batch to fill
    pub max_fill_secs: u64,
}

impl Display for BatchConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "max_size: {}, max_fill_secs: {}",
            self.max_size, self.max_fill_secs
        )
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PublicationConfig {
    pub table_names: Vec<String>,
}

impl Display for PublicationConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "table_names: {:?}", self.table_names)
    }
}

#[derive(Serialize)]
pub struct CreateTenantRequest {
    pub name: String,
}

#[derive(Serialize)]
pub struct UpdateTenantRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct CreateTenantResponse {
    pub id: i64,
}

impl Display for CreateTenantResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {}", self.id)
    }
}

#[derive(Deserialize)]
pub struct TenantResponse {
    pub id: i64,
    pub name: String,
}

impl Display for TenantResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {}, name: {}", self.id, self.name,)
    }
}

#[derive(Serialize)]
pub struct CreateSourceRequest {
    pub config: SourceConfig,
}

#[derive(Deserialize)]
pub struct CreateSourceResponse {
    pub id: i64,
}

impl Display for CreateSourceResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {}", self.id)
    }
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

impl Display for SourceResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_id: {}, id: {}, config: {}",
            self.tenant_id, self.id, self.config
        )
    }
}

#[derive(Serialize)]
pub struct CreateSinkRequest {
    pub config: SinkConfig,
}

#[derive(Deserialize)]
pub struct CreateSinkResponse {
    pub id: i64,
}

impl Display for CreateSinkResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {}", self.id)
    }
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

impl Display for SinkResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_id: {}, id: {}, config: {}",
            self.tenant_id, self.id, self.config
        )
    }
}

#[derive(Serialize)]
pub struct CreatePipelineRequest {
    pub source_id: i64,
    pub sink_id: i64,
    pub publication_id: i64,
    pub config: PipelineConfig,
}

#[derive(Deserialize)]
pub struct CreatePipelineResponse {
    pub id: i64,
}

impl Display for CreatePipelineResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {}", self.id)
    }
}

#[derive(Deserialize)]
pub struct PipelineResponse {
    pub id: i64,
    pub tenant_id: i64,
    pub source_id: i64,
    pub sink_id: i64,
    pub publication_id: i64,
    pub config: PipelineConfig,
}

impl Display for PipelineResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_id: {}, id: {}, source_id: {}, sink_id: {}, publication_id: {}, config: {}",
            self.tenant_id, self.id, self.source_id, self.sink_id, self.publication_id, self.config
        )
    }
}

#[derive(Serialize)]
pub struct UpdatePipelineRequest {
    pub source_id: i64,
    pub sink_id: i64,
    pub publication_id: i64,
    pub config: PipelineConfig,
}

#[derive(Serialize)]
pub struct CreatePublicationRequest {
    pub source_id: i64,
    pub name: String,
    pub config: PublicationConfig,
}

#[derive(Deserialize)]
pub struct CreatePublicationResponse {
    pub id: i64,
}

impl Display for CreatePublicationResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {}", self.id)
    }
}

#[derive(Deserialize)]
pub struct PublicationResponse {
    pub id: i64,
    pub tenant_id: i64,
    pub source_id: i64,
    pub name: String,
    pub config: PublicationConfig,
}

impl Display for PublicationResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_id: {}, id: {}, source_id: {}, name: {}, config: {}",
            self.tenant_id, self.id, self.source_id, self.name, self.config
        )
    }
}

#[derive(Serialize)]
pub struct UpdatePublicationRequest {
    pub source_id: i64,
    pub name: String,
    pub config: PublicationConfig,
}

#[derive(Debug, Error)]
pub enum ApiClientError {
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("api error: {0}")]
    ApiError(String),
}

#[derive(Deserialize)]
pub struct ErrorMessage {
    pub error: String,
}

impl ApiClient {
    pub fn new(address: String) -> ApiClient {
        let client = reqwest::Client::new();
        ApiClient { address, client }
    }

    pub async fn create_tenant(
        &self,
        tenant: &CreateTenantRequest,
    ) -> Result<CreateTenantResponse, ApiClientError> {
        let response = self
            .client
            .post(&format!("{}/v1/tenants", &self.address))
            .json(tenant)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn read_tenant(&self, tenant_id: i64) -> Result<TenantResponse, ApiClientError> {
        let response = self
            .client
            .get(&format!("{}/v1/tenants/{tenant_id}", &self.address))
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn update_tenant(
        &self,
        tenant_id: i64,
        tenant: &UpdateTenantRequest,
    ) -> Result<(), ApiClientError> {
        let response = self
            .client
            .post(&format!("{}/v1/tenants/{tenant_id}", &self.address))
            .json(tenant)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn delete_tenant(&self, tenant_id: i64) -> Result<(), ApiClientError> {
        let response = self
            .client
            .delete(&format!("{}/v1/tenants/{tenant_id}", &self.address))
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn read_all_tenants(&self) -> Result<Vec<TenantResponse>, ApiClientError> {
        let response = self
            .client
            .get(&format!("{}/v1/tenants", &self.address))
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn create_source(
        &self,
        tenant_id: i64,
        source: &CreateSourceRequest,
    ) -> Result<CreateSourceResponse, ApiClientError> {
        let response = self
            .client
            .post(&format!("{}/v1/sources", &self.address))
            .header("tenant_id", tenant_id)
            .json(source)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn read_source(
        &self,
        tenant_id: i64,
        source_id: i64,
    ) -> Result<SourceResponse, ApiClientError> {
        let response = self
            .client
            .get(&format!("{}/v1/sources/{source_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn update_source(
        &self,
        tenant_id: i64,
        source_id: i64,
        source: &UpdateSourceRequest,
    ) -> Result<(), ApiClientError> {
        let response = self
            .client
            .post(&format!("{}/v1/sources/{source_id}", &self.address))
            .header("tenant_id", tenant_id)
            .json(source)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn delete_source(
        &self,
        tenant_id: i64,
        source_id: i64,
    ) -> Result<(), ApiClientError> {
        let response = self
            .client
            .delete(&format!("{}/v1/sources/{source_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn read_all_sources(
        &self,
        tenant_id: i64,
    ) -> Result<Vec<SourceResponse>, ApiClientError> {
        let response = self
            .client
            .get(&format!("{}/v1/sources", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn create_sink(
        &self,
        tenant_id: i64,
        sink: &CreateSinkRequest,
    ) -> Result<CreateSinkResponse, ApiClientError> {
        let response = self
            .client
            .post(&format!("{}/v1/sinks", &self.address))
            .header("tenant_id", tenant_id)
            .json(sink)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn read_sink(
        &self,
        tenant_id: i64,
        sink_id: i64,
    ) -> Result<SinkResponse, ApiClientError> {
        let response = self
            .client
            .get(&format!("{}/v1/sinks/{sink_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn update_sink(
        &self,
        tenant_id: i64,
        sink_id: i64,
        sink: &UpdateSinkRequest,
    ) -> Result<(), ApiClientError> {
        let response = self
            .client
            .post(&format!("{}/v1/sinks/{sink_id}", &self.address))
            .header("tenant_id", tenant_id)
            .json(sink)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn delete_sink(&self, tenant_id: i64, sink_id: i64) -> Result<(), ApiClientError> {
        let response = self
            .client
            .delete(&format!("{}/v1/sinks/{sink_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn read_all_sinks(
        &self,
        tenant_id: i64,
    ) -> Result<Vec<SinkResponse>, ApiClientError> {
        let response = self
            .client
            .get(&format!("{}/v1/sinks", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn create_pipeline(
        &self,
        tenant_id: i64,
        pipeline: &CreatePipelineRequest,
    ) -> Result<CreatePipelineResponse, ApiClientError> {
        let response = self
            .client
            .post(&format!("{}/v1/pipelines", &self.address))
            .header("tenant_id", tenant_id)
            .json(pipeline)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn read_pipeline(
        &self,
        tenant_id: i64,
        pipeline_id: i64,
    ) -> Result<PipelineResponse, ApiClientError> {
        let response = self
            .client
            .get(&format!("{}/v1/pipelines/{pipeline_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn update_pipeline(
        &self,
        tenant_id: i64,
        pipeline_id: i64,
        pipeline: &UpdatePipelineRequest,
    ) -> Result<(), ApiClientError> {
        let response = self
            .client
            .post(&format!("{}/v1/pipelines/{pipeline_id}", &self.address))
            .header("tenant_id", tenant_id)
            .json(pipeline)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn delete_pipeline(
        &self,
        tenant_id: i64,
        pipeline_id: i64,
    ) -> Result<(), ApiClientError> {
        let response = self
            .client
            .delete(&format!("{}/v1/pipelines/{pipeline_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn read_all_pipelines(
        &self,
        tenant_id: i64,
    ) -> Result<Vec<PipelineResponse>, ApiClientError> {
        let response = self
            .client
            .get(&format!("{}/v1/pipelines", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn create_publication(
        &self,
        tenant_id: i64,
        publication: &CreatePublicationRequest,
    ) -> Result<CreatePublicationResponse, ApiClientError> {
        let response = self
            .client
            .post(&format!("{}/v1/publications", &self.address))
            .header("tenant_id", tenant_id)
            .json(publication)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn read_publication(
        &self,
        tenant_id: i64,
        publication_id: i64,
    ) -> Result<PublicationResponse, ApiClientError> {
        let response = self
            .client
            .get(&format!(
                "{}/v1/publications/{publication_id}",
                &self.address
            ))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn update_publication(
        &self,
        tenant_id: i64,
        publication_id: i64,
        publication: &UpdatePublicationRequest,
    ) -> Result<(), ApiClientError> {
        let response = self
            .client
            .post(&format!(
                "{}/v1/publications/{publication_id}",
                &self.address
            ))
            .header("tenant_id", tenant_id)
            .json(publication)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn delete_publication(
        &self,
        tenant_id: i64,
        publication_id: i64,
    ) -> Result<(), ApiClientError> {
        let response = self
            .client
            .delete(&format!(
                "{}/v1/publications/{publication_id}",
                &self.address
            ))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }

    pub async fn read_all_publications(
        &self,
        tenant_id: i64,
    ) -> Result<Vec<PublicationResponse>, ApiClientError> {
        let response = self
            .client
            .get(&format!("{}/v1/publications", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let message: ErrorMessage = response.json().await?;
            Err(ApiClientError::ApiError(message.error))
        }
    }
}

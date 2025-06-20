use crate::db;
use crate::db::destinations::{destination_exists, Destination, DestinationsDbError};
use crate::db::images::Image;
use crate::db::pipelines::{Pipeline, PipelineConfig};
use crate::db::replicators::Replicator;
use crate::db::sources::{source_exists, Source, SourceConfig, SourcesDbError};
use crate::encryption::EncryptionKey;
use crate::k8s_client::{
    HttpK8sClient, K8sClient, K8sError, PodPhase, TRUSTED_ROOT_CERT_CONFIG_MAP_NAME,
};
use crate::routes::{extract_tenant_id, ErrorMessage, TenantIdError};
use actix_web::{
    delete, get,
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json, Path},
    HttpRequest, HttpResponse, Responder, ResponseError,
};
use config::shared::{
    BatchConfig, DestinationConfig, ReplicatorConfig, StateStoreConfig, SupabaseConfig, TlsConfig,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use thiserror::Error;
use utoipa::ToSchema;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Secrets {
    pub postgres_password: String,
    pub bigquery_service_account_key: String,
}

#[derive(Debug, Error)]
enum PipelineError {
    #[error("database error: {0}")]
    DatabaseError(sqlx::Error),

    #[error("pipeline with id {0} not found")]
    PipelineNotFound(i64),

    #[error("source with id {0} not found")]
    SourceNotFound(i64),

    #[error("destination with id {0} not found")]
    DestinationNotFound(i64),

    #[error("replicator with pipeline id {0} not found")]
    ReplicatorNotFound(i64),

    #[error("image with replicator id {0} not found")]
    ImageNotFound(i64),

    #[error("no default image found")]
    NoDefaultImageFound,

    #[error("tenant id error: {0}")]
    TenantId(#[from] TenantIdError),

    #[error("invalid destination config")]
    InvalidConfig(#[from] serde_json::Error),

    #[error("k8s error: {0}")]
    K8sError(#[from] K8sError),

    #[error("sources db error: {0}")]
    SourcesDb(#[from] SourcesDbError),

    #[error("destinations db error: {0}")]
    DestinationsDb(#[from] DestinationsDbError),

    #[error("trusted root certs config not found")]
    TrustedRootCertsConfigMissing,

    #[error("a pipeline already exists for this source and destination combination")]
    DuplicatePipeline,
}

impl From<sqlx::Error> for PipelineError {
    fn from(e: sqlx::Error) -> Self {
        if db::pipelines::is_duplicate_pipeline_error(&e) {
            return PipelineError::DuplicatePipeline;
        }
        PipelineError::DatabaseError(e)
    }
}

impl PipelineError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            PipelineError::DatabaseError(_) => "internal server error".to_string(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for PipelineError {
    fn status_code(&self) -> StatusCode {
        match self {
            PipelineError::DatabaseError(_)
            | PipelineError::InvalidConfig(_)
            | PipelineError::ReplicatorNotFound(_)
            | PipelineError::ImageNotFound(_)
            | PipelineError::NoDefaultImageFound
            | PipelineError::SourcesDb(_)
            | PipelineError::DestinationsDb(_)
            | PipelineError::K8sError(_)
            | PipelineError::TrustedRootCertsConfigMissing => StatusCode::INTERNAL_SERVER_ERROR,
            PipelineError::PipelineNotFound(_) => StatusCode::NOT_FOUND,
            PipelineError::TenantId(_)
            | PipelineError::SourceNotFound(_)
            | PipelineError::DestinationNotFound(_) => StatusCode::BAD_REQUEST,
            PipelineError::DuplicatePipeline => StatusCode::CONFLICT,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let error_message = ErrorMessage {
            error: self.to_message(),
        };
        let body =
            serde_json::to_string(&error_message).expect("failed to serialize error message");
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(body)
    }
}

#[derive(Deserialize, ToSchema)]
pub struct PostPipelineRequest {
    pub source_id: i64,
    pub destination_id: i64,
    pub publication_name: String,
    pub config: PipelineConfig,
}

#[derive(Serialize, ToSchema)]
pub struct PostPipelineResponse {
    id: i64,
}

#[derive(Serialize, ToSchema)]
pub struct GetPipelineResponse {
    id: i64,
    tenant_id: String,
    source_id: i64,
    source_name: String,
    destination_id: i64,
    destination_name: String,
    replicator_id: i64,
    publication_name: String,
    config: PipelineConfig,
}

#[derive(Serialize, ToSchema)]
pub struct GetPipelinesResponse {
    pipelines: Vec<GetPipelineResponse>,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostPipelineRequest,
    responses(
        (status = 200, description = "Create new pipeline", body = PostPipelineResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines")]
pub async fn create_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline: Json<PostPipelineRequest>,
) -> Result<impl Responder, PipelineError> {
    let pipeline = pipeline.0;
    let tenant_id = extract_tenant_id(&req)?;
    let config = pipeline.config;

    if !source_exists(&pool, tenant_id, pipeline.source_id).await? {
        return Err(PipelineError::SourceNotFound(pipeline.source_id));
    }

    if !destination_exists(&pool, tenant_id, pipeline.destination_id).await? {
        return Err(PipelineError::DestinationNotFound(pipeline.destination_id));
    }

    let image = db::images::read_default_image(&pool)
        .await?
        .ok_or(PipelineError::NoDefaultImageFound)?;

    let id = db::pipelines::create_pipeline(
        &pool,
        tenant_id,
        pipeline.source_id,
        pipeline.destination_id,
        image.id,
        config,
    )
    .await?;

    let response = PostPipelineResponse { id };
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
    ),
    responses(
        (status = 200, description = "Return pipeline with id = pipeline_id", body = GetPipelineResponse),
        (status = 404, description = "Pipeline not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/pipelines/{pipeline_id}")]
pub async fn read_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let response = db::pipelines::read_pipeline(&pool, tenant_id, pipeline_id)
        .await?
        .map(|s| {
            Ok::<GetPipelineResponse, serde_json::Error>(GetPipelineResponse {
                id: s.id,
                tenant_id: s.tenant_id,
                source_id: s.source_id,
                source_name: s.source_name,
                destination_id: s.destination_id,
                destination_name: s.destination_name,
                replicator_id: s.replicator_id,
                publication_name: s.publication_name,
                config: s.config,
            })
        })
        .transpose()?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostDestinationRequest,
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
    ),
    responses(
        (status = 200, description = "Update pipeline with id = pipeline_id"),
        (status = 404, description = "Pipeline not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/{pipeline_id:\\d+}")]
pub async fn update_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
    pipeline: Json<PostPipelineRequest>,
) -> Result<impl Responder, PipelineError> {
    let pipeline = pipeline.0;
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let config = &pipeline.config;
    let source_id = pipeline.source_id;
    let destination_id = pipeline.destination_id;
    let publication_name = pipeline.publication_name;

    if !source_exists(&pool, tenant_id, source_id).await? {
        return Err(PipelineError::SourceNotFound(source_id));
    }

    if !destination_exists(&pool, tenant_id, destination_id).await? {
        return Err(PipelineError::DestinationNotFound(destination_id));
    }

    db::pipelines::update_pipeline(
        &pool,
        tenant_id,
        pipeline_id,
        source_id,
        destination_id,
        &publication_name,
        config,
    )
    .await?
    .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
    ),
    responses(
        (status = 200, description = "Delete pipeline with id = pipeline_id"),
        (status = 404, description = "Pipeline not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[delete("/pipelines/{pipeline_id}")]
pub async fn delete_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    db::pipelines::delete_pipeline(&pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Return all pipelines"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/pipelines")]
pub async fn read_all_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let mut pipelines = vec![];
    for pipeline in db::pipelines::read_all_pipelines(&pool, tenant_id).await? {
        let config: PipelineConfig = serde_json::from_value(pipeline.config)?;
        let pipeline = GetPipelineResponse {
            id: pipeline.id,
            tenant_id: pipeline.tenant_id,
            source_id: pipeline.source_id,
            source_name: pipeline.source_name,
            destination_id: pipeline.destination_id,
            destination_name: pipeline.destination_name,
            replicator_id: pipeline.replicator_id,
            publication_name: pipeline.publication_name,
            config,
        };
        pipelines.push(pipeline);
    }
    let response = GetPipelinesResponse { pipelines };
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Start a pipeline"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/{pipeline_id}/start")]
pub async fn start_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    k8s_client: Data<Arc<HttpK8sClient>>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let (pipeline, replicator, image, source, destination) =
        read_data(&pool, tenant_id, pipeline_id, &encryption_key).await?;

    let supabase_config = SupabaseConfig {
        project: tenant_id.to_owned(),
    };

    let (secrets, config) = create_configs(
        &k8s_client,
        source.config,
        destination.config,
        supabase_config,
    )
    .await?;
    let prefix = create_prefix(tenant_id, replicator.id);

    create_or_update_secrets(&k8s_client, &prefix, secrets).await?;
    create_or_update_config(&k8s_client, &prefix, config).await?;
    create_or_update_replicator(&k8s_client, &prefix, image.name).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Stop a pipeline"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/{pipeline_id}/stop")]
pub async fn stop_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<Arc<HttpK8sClient>>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let replicator = db::replicators::read_replicator_by_pipeline_id(&pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let prefix = create_prefix(tenant_id, replicator.id);
    delete_secrets(&k8s_client, &prefix).await?;
    delete_config(&k8s_client, &prefix).await?;
    delete_replicator(&k8s_client, &prefix).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Stop all pipelines"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/stop")]
pub async fn stop_all_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<Arc<HttpK8sClient>>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let replicators = db::replicators::read_replicators(&pool, tenant_id).await?;
    for replicator in replicators {
        let prefix = create_prefix(tenant_id, replicator.id);
        delete_secrets(&k8s_client, &prefix).await?;
        delete_config(&k8s_client, &prefix).await?;
        delete_replicator(&k8s_client, &prefix).await?;
    }
    Ok(HttpResponse::Ok().finish())
}

#[derive(Serialize, ToSchema)]
pub enum PipelineStatus {
    Stopped,
    Starting,
    Started,
    Stopping,
    Unknown,
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Get pipeline status"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/pipelines/{pipeline_id}/status")]
pub async fn get_pipeline_status(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<Arc<HttpK8sClient>>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let replicator = db::replicators::read_replicator_by_pipeline_id(&pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let prefix = create_prefix(tenant_id, replicator.id);

    let pod_phase = k8s_client.get_pod_phase(&prefix).await?;

    let status = match pod_phase {
        PodPhase::Pending => PipelineStatus::Starting,
        PodPhase::Running => PipelineStatus::Started,
        PodPhase::Succeeded => PipelineStatus::Stopped,
        PodPhase::Failed => PipelineStatus::Stopped,
        PodPhase::Unknown => PipelineStatus::Unknown,
    };

    Ok(Json(status))
}

async fn read_data(
    pool: &PgPool,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<(Pipeline, Replicator, Image, Source, Destination), PipelineError> {
    let pipeline = db::pipelines::read_pipeline(pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;
    let replicator = db::replicators::read_replicator_by_pipeline_id(pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;
    let image = db::images::read_image_by_replicator_id(pool, replicator.id)
        .await?
        .ok_or(PipelineError::ImageNotFound(replicator.id))?;
    let source_id = pipeline.source_id;
    let source = db::sources::read_source(pool, tenant_id, source_id, encryption_key)
        .await?
        .ok_or(PipelineError::SourceNotFound(source_id))?;
    let destination_id = pipeline.destination_id;
    let destination =
        db::destinations::read_destination(pool, tenant_id, destination_id, encryption_key)
            .await?
            .ok_or(PipelineError::DestinationNotFound(destination_id))?;

    Ok((pipeline, replicator, image, source, destination))
}

async fn create_configs(
    k8s_client: &Arc<HttpK8sClient>,
    source_config: SourceConfig,
    state_store_config: StateStoreConfig,
    destination_config: DestinationConfig,
    pipeline_config: PipelineConfig,
    supabase_config: SupabaseConfig,
) -> Result<(Secrets, ReplicatorConfig), PipelineError> {
    let SourceConfig::Postgres {
        host,
        port,
        name,
        username,
        password: postgres_password,
        slot_name,
    } = source_config;

    let DestinationConfig::BigQuery {
        project_id,
        dataset_id,
        service_account_key: bigquery_service_account_key,
        max_staleness_mins,
    } = destination_config;

    let secrets = Secrets {
        postgres_password: postgres_password.unwrap_or_default(),
        bigquery_service_account_key,
    };

    let publication = pipeline.publication_name;

    let destination_config = DestinationConfig::BigQuery {
        project_id,
        dataset_id,
        max_staleness_mins,
    };

    let pipeline_config: PipelineConfig = serde_json::from_value(pipeline.config)?;
    let batch_config = pipeline_config.config;
    let batch_config = BatchConfig {
        max_size: batch_config.max_size,
        max_fill_secs: batch_config.max_fill_secs,
    };

    let trusted_root_certs_cm = k8s_client
        .get_config_map(TRUSTED_ROOT_CERT_CONFIG_MAP_NAME)
        .await?
        .data
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?;
    let trusted_root_certs = trusted_root_certs_cm
        .get("trusted_root_certs")
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?;

    let tls_config = TlsConfig {
        trusted_root_certs: trusted_root_certs.clone(),
        enabled: true,
    };

    let supabase_config = SupabaseConfig { project };

    let config = ReplicatorConfig {
        source: source_config,
        destination: destination_config,
        batch: batch_config,
        tls: tls_config,
        supabase: supabase_config,
    };

    Ok((secrets, config))
}

fn create_prefix(tenant_id: &str, replicator_id: i64) -> String {
    format!("{tenant_id}-{replicator_id}")
}

async fn create_or_update_secrets(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
    secrets: Secrets,
) -> Result<(), PipelineError> {
    k8s_client
        .create_or_update_postgres_secret(prefix, &secrets.postgres_password)
        .await?;
    k8s_client
        .create_or_update_bq_secret(prefix, &secrets.bigquery_service_account_key)
        .await?;
    Ok(())
}

async fn create_or_update_config(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
    config: ReplicatorConfig,
) -> Result<(), PipelineError> {
    let base_config = "";
    let prod_config = serde_json::to_string(&config)?;
    k8s_client
        .create_or_update_config_map(prefix, base_config, &prod_config)
        .await?;
    Ok(())
}

async fn create_or_update_replicator(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
    replicator_image: String,
) -> Result<(), PipelineError> {
    k8s_client
        .create_or_update_stateful_set(prefix, &replicator_image)
        .await?;

    Ok(())
}

async fn delete_secrets(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_postgres_secret(prefix).await?;
    k8s_client.delete_bq_secret(prefix).await?;
    Ok(())
}

async fn delete_config(k8s_client: &Arc<HttpK8sClient>, prefix: &str) -> Result<(), PipelineError> {
    k8s_client.delete_config_map(prefix).await?;
    Ok(())
}

async fn delete_replicator(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_stateful_set(prefix).await?;
    Ok(())
}

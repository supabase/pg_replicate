use std::sync::Arc;

use actix_web::{
    delete, get,
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json, Path},
    HttpRequest, HttpResponse, Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    db::{
        self,
        images::Image,
        pipelines::{Pipeline, PipelineConfig},
        replicators::Replicator,
        sinks::{sink_exists, Sink, SinkConfig, SinksDbError},
        sources::{source_exists, Source, SourceConfig, SourcesDbError},
    },
    encryption::EncryptionKey,
    k8s_client::{HttpK8sClient, K8sClient, K8sError, PodPhase},
    replicator_config,
    worker::Secrets,
};

use super::ErrorMessage;

#[derive(Debug, Error)]
enum PipelineError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("pipeline with id {0} not found")]
    PipelineNotFound(i64),

    #[error("source with id {0} not found")]
    SourceNotFound(i64),

    #[error("sink with id {0} not found")]
    SinkNotFound(i64),

    #[error("replicator with pipeline id {0} not found")]
    ReplicatorNotFound(i64),

    #[error("image with replicator id {0} not found")]
    ImageNotFound(i64),

    #[error("no default image found")]
    NoDefaultImageFound,

    #[error("tenant id missing in request")]
    TenantIdMissing,

    #[error("tenant id ill formed in request")]
    TenantIdIllFormed,

    #[error("invalid sink config")]
    InvalidConfig(#[from] serde_json::Error),

    #[error("k8s error: {0}")]
    K8sError(#[from] K8sError),

    #[error("sources db error: {0}")]
    SourcesDb(#[from] SourcesDbError),

    #[error("sinks db error: {0}")]
    SinksDb(#[from] SinksDbError),
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
            | PipelineError::SinksDb(_)
            | PipelineError::K8sError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PipelineError::PipelineNotFound(_) => StatusCode::NOT_FOUND,
            PipelineError::TenantIdMissing
            | PipelineError::TenantIdIllFormed
            | PipelineError::SourceNotFound(_)
            | PipelineError::SinkNotFound(_) => StatusCode::BAD_REQUEST,
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
    pub sink_id: i64,
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
    sink_id: i64,
    sink_name: String,
    replicator_id: i64,
    publication_name: String,
    config: PipelineConfig,
}

// TODO: read tenant_id from a jwt
fn extract_tenant_id(req: &HttpRequest) -> Result<&str, PipelineError> {
    let headers = req.headers();
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(PipelineError::TenantIdMissing)?;
    let tenant_id = tenant_id
        .to_str()
        .map_err(|_| PipelineError::TenantIdIllFormed)?;
    Ok(tenant_id)
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

    if !sink_exists(&pool, tenant_id, pipeline.sink_id).await? {
        return Err(PipelineError::SinkNotFound(pipeline.sink_id));
    }

    let image = db::images::read_default_image(&pool)
        .await?
        .ok_or(PipelineError::NoDefaultImageFound)?;

    let id = db::pipelines::create_pipeline(
        &pool,
        tenant_id,
        pipeline.source_id,
        pipeline.sink_id,
        image.id,
        pipeline.publication_name,
        &config,
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
            let config: PipelineConfig = serde_json::from_value(s.config)?;
            Ok::<GetPipelineResponse, serde_json::Error>(GetPipelineResponse {
                id: s.id,
                tenant_id: s.tenant_id,
                source_id: s.source_id,
                source_name: s.source_name,
                sink_id: s.sink_id,
                sink_name: s.sink_name,
                replicator_id: s.replicator_id,
                publication_name: s.publication_name,
                config,
            })
        })
        .transpose()?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostSinkRequest,
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
    ),
    responses(
        (status = 200, description = "Update pipeline with id = pipeline_id"),
        (status = 404, description = "Pipeline not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/{pipeline_id}")]
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
    let sink_id = pipeline.sink_id;
    let publication_name = pipeline.publication_name;

    if !source_exists(&pool, tenant_id, source_id).await? {
        return Err(PipelineError::SourceNotFound(source_id));
    }

    if !sink_exists(&pool, tenant_id, sink_id).await? {
        return Err(PipelineError::SinkNotFound(sink_id));
    }

    db::pipelines::update_pipeline(
        &pool,
        tenant_id,
        pipeline_id,
        source_id,
        sink_id,
        publication_name,
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
        let sink = GetPipelineResponse {
            id: pipeline.id,
            tenant_id: pipeline.tenant_id,
            source_id: pipeline.source_id,
            source_name: pipeline.source_name,
            sink_id: pipeline.sink_id,
            sink_name: pipeline.sink_name,
            replicator_id: pipeline.replicator_id,
            publication_name: pipeline.publication_name,
            config,
        };
        pipelines.push(sink);
    }
    Ok(Json(pipelines))
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

    let (pipeline, replicator, image, source, sink) =
        read_data(&pool, tenant_id, pipeline_id, &encryption_key).await?;

    let (secrets, config) = create_configs(source.config, sink.config, pipeline)?;
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
) -> Result<(Pipeline, Replicator, Image, Source, Sink), PipelineError> {
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
    let sink_id = pipeline.sink_id;
    let sink = db::sinks::read_sink(pool, tenant_id, sink_id, encryption_key)
        .await?
        .ok_or(PipelineError::SinkNotFound(sink_id))?;

    Ok((pipeline, replicator, image, source, sink))
}

fn create_configs(
    source_config: SourceConfig,
    sink_config: SinkConfig,
    pipeline: Pipeline,
) -> Result<(Secrets, replicator_config::Config), PipelineError> {
    let SourceConfig::Postgres {
        host,
        port,
        name,
        username,
        password: postgres_password,
        slot_name,
    } = source_config;

    let SinkConfig::BigQuery {
        project_id,
        dataset_id,
        service_account_key: bigquery_service_account_key,
    } = sink_config;

    let secrets = Secrets {
        postgres_password: postgres_password.unwrap_or_default(),
        bigquery_service_account_key,
    };

    let publication = pipeline.publication_name;
    let source_config = replicator_config::SourceConfig::Postgres {
        host,
        port,
        name,
        username,
        slot_name,
        publication,
    };

    let sink_config = replicator_config::SinkConfig::BigQuery {
        project_id,
        dataset_id,
    };

    let pipeline_config: PipelineConfig = serde_json::from_value(pipeline.config)?;
    let batch_config = pipeline_config.config;
    let batch_config = replicator_config::BatchConfig {
        max_size: batch_config.max_size,
        max_fill_secs: batch_config.max_fill_secs,
    };

    let config = replicator_config::Config {
        source: source_config,
        sink: sink_config,
        batch: batch_config,
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
    config: replicator_config::Config,
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

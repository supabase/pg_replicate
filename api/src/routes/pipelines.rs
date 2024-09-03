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

use crate::{
    db::{
        self,
        pipelines::{Pipeline, PipelineConfig},
        replicators::Replicator,
        sinks::{sink_exists, Sink, SinkConfig},
        sources::{source_exists, Source, SourceConfig},
    },
    queue::enqueue_task,
    replicator_config,
    worker::{Request, Secrets},
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

    #[error("tenant id missing in request")]
    TenantIdMissing,

    #[error("tenant id ill formed in request")]
    TenantIdIllFormed,

    #[error("invalid sink config")]
    InvalidConfig(#[from] serde_json::Error),
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
            | PipelineError::ReplicatorNotFound(_) => StatusCode::INTERNAL_SERVER_ERROR,
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

#[derive(Deserialize)]
struct PostPipelineRequest {
    pub source_id: i64,
    pub sink_id: i64,
    pub publication_name: String,
    pub config: PipelineConfig,
}

#[derive(Serialize)]
struct PostPipelineResponse {
    id: i64,
}

#[derive(Serialize)]
struct GetPipelineResponse {
    id: i64,
    tenant_id: i64,
    source_id: i64,
    sink_id: i64,
    replicator_id: i64,
    publication_name: String,
    config: PipelineConfig,
}

// TODO: read tenant_id from a jwt
fn extract_tenant_id(req: &HttpRequest) -> Result<i64, PipelineError> {
    let headers = req.headers();
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(PipelineError::TenantIdMissing)?;
    let tenant_id = tenant_id
        .to_str()
        .map_err(|_| PipelineError::TenantIdIllFormed)?;
    let tenant_id: i64 = tenant_id
        .parse()
        .map_err(|_| PipelineError::TenantIdIllFormed)?;
    Ok(tenant_id)
}

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

    let id = db::pipelines::create_pipeline(
        &pool,
        tenant_id,
        pipeline.source_id,
        pipeline.sink_id,
        pipeline.publication_name,
        &config,
    )
    .await?;

    let response = PostPipelineResponse { id };
    Ok(Json(response))
}

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
                sink_id: s.sink_id,
                replicator_id: s.replicator_id,
                publication_name: s.publication_name,
                config,
            })
        })
        .transpose()?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(Json(response))
}

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
        .ok_or(PipelineError::PipelineNotFound(tenant_id))?;
    Ok(HttpResponse::Ok().finish())
}

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
            sink_id: pipeline.sink_id,
            replicator_id: pipeline.replicator_id,
            publication_name: pipeline.publication_name,
            config,
        };
        pipelines.push(sink);
    }
    Ok(Json(pipelines))
}

#[post("/pipelines/{pipeline_id}/start")]
pub async fn start_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let (pipeline, replicator, source, sink) = read_data(&pool, tenant_id, pipeline_id).await?;

    let (secrets, config) = create_configs(source.config, sink.config, pipeline)?;

    enqueue_create_or_update_secrets_task(&pool, tenant_id, replicator.id, secrets).await?;

    enqueue_create_or_update_config_task(&pool, tenant_id, replicator.id, config).await?;

    enqueue_create_or_update_replicator_task(&pool, tenant_id, replicator.id).await?;

    Ok(HttpResponse::Ok().finish())
}

async fn read_data(
    pool: &PgPool,
    tenant_id: i64,
    pipeline_id: i64,
) -> Result<(Pipeline, Replicator, Source, Sink), PipelineError> {
    let pipeline = db::pipelines::read_pipeline(pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;
    let replicator = db::replicators::read_replicator_by_pipeline_id(pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;
    let source_id = pipeline.source_id;
    let source = db::sources::read_source(pool, tenant_id, source_id)
        .await?
        .ok_or(PipelineError::SourceNotFound(source_id))?;
    let sink_id = pipeline.sink_id;
    let sink = db::sinks::read_sink(pool, tenant_id, sink_id)
        .await?
        .ok_or(PipelineError::SinkNotFound(sink_id))?;
    Ok((pipeline, replicator, source, sink))
}

fn create_configs(
    source_config: serde_json::Value,
    sink_config: serde_json::Value,
    pipeline: Pipeline,
) -> Result<(Secrets, replicator_config::Config), PipelineError> {
    let source_config: SourceConfig = serde_json::from_value(source_config)?;
    let sink_config: SinkConfig = serde_json::from_value(sink_config)?;

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

async fn enqueue_create_or_update_secrets_task(
    pool: &PgPool,
    tenant_id: i64,
    replicator_id: i64,
    secrets: Secrets,
) -> Result<(), PipelineError> {
    let req = Request::CreateOrUpdateSecrets {
        tenant_id,
        replicator_id,
        secrets,
    };

    let task_data = serde_json::to_value(req)?;
    enqueue_task(pool, "create_or_update_secrets", task_data).await?;
    Ok(())
}

async fn enqueue_create_or_update_config_task(
    pool: &PgPool,
    tenant_id: i64,
    replicator_id: i64,
    config: replicator_config::Config,
) -> Result<(), PipelineError> {
    let req = Request::CreateOrUpdateConfig {
        tenant_id,
        replicator_id,
        config,
    };

    let task_data = serde_json::to_value(req)?;
    enqueue_task(pool, "create_or_update_config", task_data).await?;
    Ok(())
}

async fn enqueue_create_or_update_replicator_task(
    pool: &PgPool,
    tenant_id: i64,
    replicator_id: i64,
) -> Result<(), PipelineError> {
    let req = Request::CreateOrUpdateReplicator {
        tenant_id,
        replicator_id,
        replicator_image: "ramsup/replicator:main.96457e346eed76c0dce648dbfd5a6f645220ea9a"
            .to_string(), //TODO: remove hardcode image
    };

    let task_data = serde_json::to_value(req)?;
    enqueue_task(pool, "create_or_update_replicator", task_data).await?;

    Ok(())
}

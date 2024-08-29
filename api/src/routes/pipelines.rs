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

use crate::db::{
    self, pipelines::PipelineConfig, publications::publication_exists, sinks::sink_exists,
    sources::source_exists,
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

    #[error("publication with id {0} not found")]
    PublicationNotFound(i64),

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
            PipelineError::DatabaseError(_) | PipelineError::InvalidConfig(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            PipelineError::PipelineNotFound(_) => StatusCode::NOT_FOUND,
            PipelineError::TenantIdMissing
            | PipelineError::TenantIdIllFormed
            | PipelineError::SourceNotFound(_)
            | PipelineError::SinkNotFound(_)
            | PipelineError::PublicationNotFound(_) => StatusCode::BAD_REQUEST,
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
    pub publication_id: i64,
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
    publication_id: i64,
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

    if !publication_exists(&pool, tenant_id, pipeline.publication_id).await? {
        return Err(PipelineError::PublicationNotFound(pipeline.publication_id));
    }

    let id = db::pipelines::create_pipeline(
        &pool,
        tenant_id,
        pipeline.source_id,
        pipeline.sink_id,
        pipeline.publication_id,
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
                publication_id: s.publication_id,
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
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let config = &pipeline.config;

    if !source_exists(&pool, tenant_id, pipeline.source_id).await? {
        return Err(PipelineError::SourceNotFound(pipeline.source_id));
    }

    if !sink_exists(&pool, tenant_id, pipeline.sink_id).await? {
        return Err(PipelineError::SinkNotFound(pipeline.sink_id));
    }

    if !publication_exists(&pool, tenant_id, pipeline.publication_id).await? {
        return Err(PipelineError::PublicationNotFound(pipeline.publication_id));
    }

    db::pipelines::update_pipeline(
        &pool,
        tenant_id,
        pipeline_id,
        pipeline.source_id,
        pipeline.sink_id,
        pipeline.publication_id,
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
            publication_id: pipeline.publication_id,
            config,
        };
        pipelines.push(sink);
    }
    Ok(Json(pipelines))
}

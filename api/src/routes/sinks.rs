use actix_web::{
    delete, get,
    http::StatusCode,
    post,
    web::{Data, Json, Path},
    HttpRequest, HttpResponse, Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

use crate::db::{self, sinks::SinkConfig};

#[derive(Debug, Error)]
enum SinkError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("sink with id {0} not found")]
    NotFound(i64),

    #[error("tenant id missing in request")]
    TenantIdMissing,

    #[error("tenant id ill formed in request")]
    TenantIdIllFormed,

    #[error("invalid sink config")]
    InvalidConfig(#[from] serde_json::Error),
}

impl ResponseError for SinkError {
    fn status_code(&self) -> StatusCode {
        match self {
            SinkError::DatabaseError(_) | SinkError::InvalidConfig(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            SinkError::NotFound(_) => StatusCode::NOT_FOUND,
            SinkError::TenantIdMissing | SinkError::TenantIdIllFormed => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Deserialize)]
struct PostSinkRequest {
    pub config: SinkConfig,
}

#[derive(Serialize)]
struct PostSinkResponse {
    id: i64,
}

#[derive(Serialize)]
struct GetSinkResponse {
    id: i64,
    tenant_id: i64,
    config: SinkConfig,
}

// TODO: read tenant_id from a jwt
fn extract_tenant_id(req: &HttpRequest) -> Result<i64, SinkError> {
    let headers = req.headers();
    let tenant_id = headers.get("tenant_id").ok_or(SinkError::TenantIdMissing)?;
    let tenant_id = tenant_id
        .to_str()
        .map_err(|_| SinkError::TenantIdIllFormed)?;
    let tenant_id: i64 = tenant_id
        .parse()
        .map_err(|_| SinkError::TenantIdIllFormed)?;
    Ok(tenant_id)
}

#[post("/sinks")]
pub async fn create_sink(
    req: HttpRequest,
    pool: Data<PgPool>,
    sink: Json<PostSinkRequest>,
) -> Result<impl Responder, SinkError> {
    let sink = sink.0;
    let tenant_id = extract_tenant_id(&req)?;
    let config = sink.config;
    let id = db::sinks::create_sink(&pool, tenant_id, &config).await?;
    let response = PostSinkResponse { id };
    Ok(Json(response))
}

#[get("/sinks/{sink_id}")]
pub async fn read_sink(
    req: HttpRequest,
    pool: Data<PgPool>,
    sink_id: Path<i64>,
) -> Result<impl Responder, SinkError> {
    let tenant_id = extract_tenant_id(&req)?;
    let sink_id = sink_id.into_inner();
    let response = db::sinks::read_sink(&pool, tenant_id, sink_id)
        .await?
        .map(|s| {
            let config: SinkConfig = serde_json::from_value(s.config)?;
            Ok::<GetSinkResponse, serde_json::Error>(GetSinkResponse {
                id: s.id,
                tenant_id: s.tenant_id,
                config,
            })
        })
        .transpose()?
        .ok_or(SinkError::NotFound(sink_id))?;
    Ok(Json(response))
}

#[post("/sinks/{sink_id}")]
pub async fn update_sink(
    req: HttpRequest,
    pool: Data<PgPool>,
    sink_id: Path<i64>,
    sink: Json<PostSinkRequest>,
) -> Result<impl Responder, SinkError> {
    let tenant_id = extract_tenant_id(&req)?;
    let sink_id = sink_id.into_inner();
    let config = &sink.config;
    db::sinks::update_sink(&pool, tenant_id, sink_id, config)
        .await?
        .ok_or(SinkError::NotFound(sink_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[delete("/sinks/{sink_id}")]
pub async fn delete_sink(
    req: HttpRequest,
    pool: Data<PgPool>,
    sink_id: Path<i64>,
) -> Result<impl Responder, SinkError> {
    let tenant_id = extract_tenant_id(&req)?;
    let sink_id = sink_id.into_inner();
    db::sinks::delete_sink(&pool, tenant_id, sink_id)
        .await?
        .ok_or(SinkError::NotFound(tenant_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[get("/sinks")]
pub async fn read_all_sinks(
    req: HttpRequest,
    pool: Data<PgPool>,
) -> Result<impl Responder, SinkError> {
    let tenant_id = extract_tenant_id(&req)?;
    let mut sinks = vec![];
    for sink in db::sinks::read_all_sinks(&pool, tenant_id).await? {
        let config: SinkConfig = serde_json::from_value(sink.config)?;
        let sink = GetSinkResponse {
            id: sink.id,
            tenant_id: sink.tenant_id,
            config,
        };
        sinks.push(sink);
    }
    Ok(Json(sinks))
}

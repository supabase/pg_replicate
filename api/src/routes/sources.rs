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

use crate::db::{self, sources::SourceConfig};

#[derive(Debug, Error)]
enum SourceError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("source with id {0} not found")]
    SourceNotFound(i64),

    #[error("tenant id missing in request")]
    TenantIdMissing,

    #[error("tenant id ill formed in request")]
    TenantIdIllFormed,

    #[error("invalid source config")]
    InvalidConfig(#[from] serde_json::Error),
}

impl ResponseError for SourceError {
    fn status_code(&self) -> StatusCode {
        match self {
            SourceError::DatabaseError(_) | SourceError::InvalidConfig(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            SourceError::SourceNotFound(_) => StatusCode::NOT_FOUND,
            SourceError::TenantIdMissing | SourceError::TenantIdIllFormed => {
                StatusCode::BAD_REQUEST
            }
        }
    }
}

#[derive(Deserialize)]
struct PostSourceRequest {
    pub config: SourceConfig,
}

#[derive(Serialize)]
struct PostSourceResponse {
    id: i64,
}

#[derive(Serialize)]
struct GetSourceResponse {
    id: i64,
    tenant_id: i64,
    config: SourceConfig,
}

// TODO: read tenant_id from a jwt
fn extract_tenant_id(req: &HttpRequest) -> Result<i64, SourceError> {
    let headers = req.headers();
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(SourceError::TenantIdMissing)?;
    let tenant_id = tenant_id
        .to_str()
        .map_err(|_| SourceError::TenantIdIllFormed)?;
    let tenant_id: i64 = tenant_id
        .parse()
        .map_err(|_| SourceError::TenantIdIllFormed)?;
    Ok(tenant_id)
}

#[post("/sources")]
pub async fn create_source(
    req: HttpRequest,
    pool: Data<PgPool>,
    source: Json<PostSourceRequest>,
) -> Result<impl Responder, SourceError> {
    let source = source.0;
    let tenant_id = extract_tenant_id(&req)?;
    let config = source.config;
    let id = db::sources::create_source(&pool, tenant_id, &config).await?;
    let response = PostSourceResponse { id };
    Ok(Json(response))
}

#[get("/sources/{source_id}")]
pub async fn read_source(
    req: HttpRequest,
    pool: Data<PgPool>,
    source_id: Path<i64>,
) -> Result<impl Responder, SourceError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();
    let response = db::sources::read_source(&pool, tenant_id, source_id)
        .await?
        .map(|s| {
            let config: SourceConfig = serde_json::from_value(s.config)?;
            Ok::<GetSourceResponse, serde_json::Error>(GetSourceResponse {
                id: s.id,
                tenant_id: s.tenant_id,
                config,
            })
        })
        .transpose()?
        .ok_or(SourceError::SourceNotFound(source_id))?;
    Ok(Json(response))
}

#[post("/sources/{source_id}")]
pub async fn update_source(
    req: HttpRequest,
    pool: Data<PgPool>,
    source_id: Path<i64>,
    source: Json<PostSourceRequest>,
) -> Result<impl Responder, SourceError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();
    let config = &source.config;
    db::sources::update_source(&pool, tenant_id, source_id, config)
        .await?
        .ok_or(SourceError::SourceNotFound(source_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[delete("/sources/{source_id}")]
pub async fn delete_source(
    req: HttpRequest,
    pool: Data<PgPool>,
    source_id: Path<i64>,
) -> Result<impl Responder, SourceError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();
    db::sources::delete_source(&pool, tenant_id, source_id)
        .await?
        .ok_or(SourceError::SourceNotFound(tenant_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[get("/sources")]
pub async fn read_all_sources(
    req: HttpRequest,
    pool: Data<PgPool>,
) -> Result<impl Responder, SourceError> {
    let tenant_id = extract_tenant_id(&req)?;
    let mut sources = vec![];
    for source in db::sources::read_all_sources(&pool, tenant_id).await? {
        let config: SourceConfig = serde_json::from_value(source.config)?;
        let source = GetSourceResponse {
            id: source.id,
            tenant_id: source.tenant_id,
            config,
        };
        sources.push(source);
    }
    Ok(Json(sources))
}

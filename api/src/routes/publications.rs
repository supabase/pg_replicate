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

use crate::db::{self, publications::PublicationConfig, sources::source_exists};

#[derive(Debug, Error)]
enum PublicationError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("publication with id {0} not found")]
    PublicationNotFound(i64),

    #[error("source with id {0} not found")]
    SourceNotFound(i64),

    #[error("tenant id missing in request")]
    TenantIdMissing,

    #[error("tenant id ill formed in request")]
    TenantIdIllFormed,

    #[error("invalid sink config")]
    InvalidConfig(#[from] serde_json::Error),
}

impl ResponseError for PublicationError {
    fn status_code(&self) -> StatusCode {
        match self {
            PublicationError::DatabaseError(_) | PublicationError::InvalidConfig(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            PublicationError::PublicationNotFound(_) => StatusCode::NOT_FOUND,
            PublicationError::TenantIdMissing
            | PublicationError::TenantIdIllFormed
            | PublicationError::SourceNotFound(_) => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Deserialize)]
struct PostPublicationRequest {
    pub source_id: i64,
    pub config: PublicationConfig,
}

#[derive(Serialize)]
struct PostPublicationResponse {
    id: i64,
}

#[derive(Serialize)]
struct GetPublicationResponse {
    id: i64,
    tenant_id: i64,
    source_id: i64,
    config: PublicationConfig,
}

// TODO: read tenant_id from a jwt
fn extract_tenant_id(req: &HttpRequest) -> Result<i64, PublicationError> {
    let headers = req.headers();
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(PublicationError::TenantIdMissing)?;
    let tenant_id = tenant_id
        .to_str()
        .map_err(|_| PublicationError::TenantIdIllFormed)?;
    let tenant_id: i64 = tenant_id
        .parse()
        .map_err(|_| PublicationError::TenantIdIllFormed)?;
    Ok(tenant_id)
}

#[post("/publications")]
pub async fn create_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    publication: Json<PostPublicationRequest>,
) -> Result<impl Responder, PublicationError> {
    let publication = publication.0;
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = publication.source_id;
    let config = publication.config;

    if !source_exists(&pool, tenant_id, source_id).await? {
        return Err(PublicationError::SourceNotFound(source_id));
    }

    let id = db::publications::create_publication(&pool, tenant_id, source_id, &config).await?;
    let response = PostPublicationResponse { id };

    Ok(Json(response))
}

#[get("/publications/{publication_id}")]
pub async fn read_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    publication_id: Path<i64>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let publication_id = publication_id.into_inner();

    let response = db::publications::read_publication(&pool, tenant_id, publication_id)
        .await?
        .map(|s| {
            let config: PublicationConfig = serde_json::from_value(s.config)?;
            Ok::<GetPublicationResponse, serde_json::Error>(GetPublicationResponse {
                id: s.id,
                tenant_id: s.tenant_id,
                source_id: s.source_id,
                config,
            })
        })
        .transpose()?
        .ok_or(PublicationError::PublicationNotFound(publication_id))?;

    Ok(Json(response))
}

#[post("/publications/{publication_id}")]
pub async fn update_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    publication_id: Path<i64>,
    publication: Json<PostPublicationRequest>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let publication_id = publication_id.into_inner();
    let source_id = publication.source_id;
    let config = &publication.config;

    if !source_exists(&pool, tenant_id, source_id).await? {
        return Err(PublicationError::SourceNotFound(source_id));
    }

    db::publications::update_publication(&pool, tenant_id, publication_id, source_id, config)
        .await?
        .ok_or(PublicationError::PublicationNotFound(publication_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[delete("/publications/{publication_id}")]
pub async fn delete_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    publication_id: Path<i64>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let publication_id = publication_id.into_inner();

    db::publications::delete_publication(&pool, tenant_id, publication_id)
        .await?
        .ok_or(PublicationError::PublicationNotFound(tenant_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[get("/publications")]
pub async fn read_all_publications(
    req: HttpRequest,
    pool: Data<PgPool>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let mut publications = vec![];

    for publication in db::publications::read_all_publications(&pool, tenant_id).await? {
        let config: PublicationConfig = serde_json::from_value(publication.config)?;
        let sink = GetPublicationResponse {
            id: publication.id,
            tenant_id: publication.tenant_id,
            source_id: publication.source_id,
            config,
        };
        publications.push(sink);
    }

    Ok(Json(publications))
}

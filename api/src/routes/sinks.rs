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
        sinks::{SinkConfig, SinksDbError},
    },
    encryption::EncryptionKey,
};

use super::ErrorMessage;

#[derive(Debug, Error)]
enum SinkError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("sink with id {0} not found")]
    SinkNotFound(i64),

    #[error("tenant id missing in request")]
    TenantIdMissing,

    #[error("tenant id ill formed in request")]
    TenantIdIllFormed,

    #[error("sinks db error: {0}")]
    SinksDb(#[from] SinksDbError),
}

impl SinkError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            SinkError::DatabaseError(_) => "internal server error".to_string(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for SinkError {
    fn status_code(&self) -> StatusCode {
        match self {
            SinkError::DatabaseError(_) | SinkError::SinksDb(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            SinkError::SinkNotFound(_) => StatusCode::NOT_FOUND,
            SinkError::TenantIdMissing | SinkError::TenantIdIllFormed => StatusCode::BAD_REQUEST,
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
pub struct PostSinkRequest {
    pub config: SinkConfig,
}

#[derive(Serialize, ToSchema)]
pub struct PostSinkResponse {
    id: i64,
}

#[derive(Serialize, ToSchema)]
pub struct GetSinkResponse {
    #[schema(example = 1)]
    id: i64,
    #[schema(example = 1)]
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

#[utoipa::path(
    context_path = "/v1",
    request_body = PostSinkRequest,
    responses(
        (status = 200, description = "Create new sink", body = PostSinkResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/sinks")]
pub async fn create_sink(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    sink: Json<PostSinkRequest>,
) -> Result<impl Responder, SinkError> {
    let sink = sink.0;
    let tenant_id = extract_tenant_id(&req)?;
    let config = sink.config;
    let id = db::sinks::create_sink(&pool, tenant_id, config, &encryption_key).await?;
    let response = PostSinkResponse { id };
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("sink_id" = i64, Path, description = "Id of the sink"),
    ),
    responses(
        (status = 200, description = "Return sink with id = sink_id", body = GetSourceResponse),
        (status = 404, description = "Sink not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/sinks/{sink_id}")]
pub async fn read_sink(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    sink_id: Path<i64>,
) -> Result<impl Responder, SinkError> {
    let tenant_id = extract_tenant_id(&req)?;
    let sink_id = sink_id.into_inner();
    let response = db::sinks::read_sink(&pool, tenant_id, sink_id, &encryption_key)
        .await?
        .map(|s| GetSinkResponse {
            id: s.id,
            tenant_id: s.tenant_id,
            config: s.config,
        })
        .ok_or(SinkError::SinkNotFound(sink_id))?;
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostSinkRequest,
    params(
        ("sink_id" = i64, Path, description = "Id of the sink"),
    ),
    responses(
        (status = 200, description = "Update sink with id = sink_id"),
        (status = 404, description = "Sink not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/sinks/{sink_id}")]
pub async fn update_sink(
    req: HttpRequest,
    pool: Data<PgPool>,
    sink_id: Path<i64>,
    encryption_key: Data<EncryptionKey>,
    sink: Json<PostSinkRequest>,
) -> Result<impl Responder, SinkError> {
    let sink = sink.0;
    let tenant_id = extract_tenant_id(&req)?;
    let sink_id = sink_id.into_inner();
    let config = sink.config;
    db::sinks::update_sink(&pool, tenant_id, sink_id, config, &encryption_key)
        .await?
        .ok_or(SinkError::SinkNotFound(sink_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("sink_id" = i64, Path, description = "Id of the sink"),
    ),
    responses(
        (status = 200, description = "Delete sink with id = sink_id"),
        (status = 404, description = "Sink not found"),
        (status = 500, description = "Internal server error")
    )
)]
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
        .ok_or(SinkError::SinkNotFound(tenant_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Return all sinks"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/sinks")]
pub async fn read_all_sinks(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
) -> Result<impl Responder, SinkError> {
    let tenant_id = extract_tenant_id(&req)?;
    let mut sinks = vec![];
    for sink in db::sinks::read_all_sinks(&pool, tenant_id, &encryption_key).await? {
        let sink = GetSinkResponse {
            id: sink.id,
            tenant_id: sink.tenant_id,
            config: sink.config,
        };
        sinks.push(sink);
    }
    Ok(Json(sinks))
}

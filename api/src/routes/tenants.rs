use actix_web::{
    delete, get,
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json, Path},
    HttpResponse, Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

use crate::db;

use super::ErrorMessage;

#[derive(Deserialize)]
struct PostTenantRequest {
    name: String,
}

#[derive(Serialize)]
struct PostTenantResponse {
    id: i64,
}

#[derive(Debug, Error)]
enum TenantError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("tenant with id {0} not found")]
    TenantNotFound(i64),
}

impl TenantError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            TenantError::DatabaseError(_) => "internal server error".to_string(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for TenantError {
    fn status_code(&self) -> StatusCode {
        match self {
            TenantError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            TenantError::TenantNotFound(_) => StatusCode::NOT_FOUND,
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

#[derive(Serialize)]
struct GetTenantResponse {
    id: i64,
    name: String,
}

#[post("/tenants")]
pub async fn create_tenant(
    pool: Data<PgPool>,
    tenant: Json<PostTenantRequest>,
) -> Result<impl Responder, TenantError> {
    let tenant = tenant.0;
    let name = tenant.name;
    let id = db::tenants::create_tenant(&pool, &name).await?;
    let response = PostTenantResponse { id };
    Ok(Json(response))
}

#[get("/tenants/{tenant_id}")]
pub async fn read_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<i64>,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();
    let response = db::tenants::read_tenant(&pool, tenant_id)
        .await?
        .map(|t| GetTenantResponse {
            id: t.id,
            name: t.name,
        })
        .ok_or(TenantError::TenantNotFound(tenant_id))?;
    Ok(Json(response))
}

#[post("/tenants/{tenant_id}")]
pub async fn update_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<i64>,
    tenant: Json<PostTenantRequest>,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();
    db::tenants::update_tenant(&pool, tenant_id, &tenant.0.name)
        .await?
        .ok_or(TenantError::TenantNotFound(tenant_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[delete("/tenants/{tenant_id}")]
pub async fn delete_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<i64>,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();
    db::tenants::delete_tenant(&pool, tenant_id)
        .await?
        .ok_or(TenantError::TenantNotFound(tenant_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[get("/tenants")]
pub async fn read_all_tenants(pool: Data<PgPool>) -> Result<impl Responder, TenantError> {
    let response: Vec<GetTenantResponse> = db::tenants::read_all_tenants(&pool)
        .await?
        .drain(..)
        .map(|t| GetTenantResponse {
            id: t.id,
            name: t.name,
        })
        .collect();
    Ok(Json(response))
}

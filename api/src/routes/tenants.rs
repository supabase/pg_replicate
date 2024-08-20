use actix_web::{
    get,
    http::StatusCode,
    post,
    web::{Data, Json, Path},
    Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

use crate::db;

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
    NotFound(i64),
}

impl ResponseError for TenantError {
    fn status_code(&self) -> StatusCode {
        match self {
            TenantError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            TenantError::NotFound(_) => StatusCode::NOT_FOUND,
        }
    }
}

#[post("/tenants")]
pub async fn create_tenant(
    pool: Data<PgPool>,
    tenant: Json<PostTenantRequest>,
) -> Result<impl Responder, TenantError> {
    let id = db::tenants::save_tenant(&pool, &tenant.0.name).await?;
    let response = PostTenantResponse { id };
    Ok(Json(response))
}

#[derive(Serialize)]
struct GetTenantResponse {
    id: i64,
    name: String,
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
        .ok_or(TenantError::NotFound(tenant_id))?;
    Ok(Json(response))
}

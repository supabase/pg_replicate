use actix_web::{
    delete, get,
    http::StatusCode,
    post,
    web::{Data, Json, Path},
    HttpResponse, Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

use crate::{db, utils::generate_random_alpha_str};

#[derive(Deserialize)]
struct PostTenantRequest {
    name: String,
    supabase_project_ref: Option<String>,
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

#[derive(Serialize)]
struct GetTenantResponse {
    id: i64,
    name: String,
    supabase_project_ref: Option<String>,
    prefix: String,
}

#[post("/tenants")]
pub async fn create_tenant(
    pool: Data<PgPool>,
    tenant: Json<PostTenantRequest>,
) -> Result<impl Responder, TenantError> {
    let tenant = tenant.0;
    let name = tenant.name;
    let spr = tenant.supabase_project_ref;
    let id = match spr {
        Some(spr) => {
            db::tenants::create_tenant(&pool, &name, Some(spr.as_str()), spr.as_str()).await?
        }
        None => {
            let prefix = generate_random_alpha_str(20);
            db::tenants::create_tenant(&pool, &name, None, &prefix).await?
        }
    };
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
            supabase_project_ref: t.supabase_project_ref,
            prefix: t.prefix,
        })
        .ok_or(TenantError::NotFound(tenant_id))?;
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
        .ok_or(TenantError::NotFound(tenant_id))?;
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
        .ok_or(TenantError::NotFound(tenant_id))?;
    Ok(HttpResponse::Ok().finish())
}

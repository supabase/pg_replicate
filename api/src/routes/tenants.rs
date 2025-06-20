use actix_web::{
    delete, get,
    http::{header::ContentType, StatusCode},
    post, put,
    web::{Data, Json, Path},
    HttpResponse, Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use tracing_actix_web::RootSpan;
use utoipa::ToSchema;

use crate::db;
use crate::routes::ErrorMessage;

#[derive(Deserialize, ToSchema)]
pub struct CreateTenantRequest {
    #[schema(example = "abcdefghijklmnopqrst", required = true)]
    id: String,
    #[schema(example = "Tenant Name", required = true)]
    name: String,
}

#[derive(Deserialize, ToSchema)]
pub struct UpdateTenantRequest {
    #[schema(example = "Tenant Name", required = true)]
    name: String,
}

#[derive(Serialize, ToSchema)]
pub struct PostTenantResponse {
    id: String,
}

#[derive(Debug, Error)]
pub enum TenantError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("tenant with id {0} not found")]
    TenantNotFound(String),
}

impl TenantError {
    pub fn to_message(&self) -> String {
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

#[derive(Serialize, ToSchema)]
pub struct GetTenantResponse {
    #[schema(example = 1)]
    id: String,
    #[schema(example = "Tenant name")]
    name: String,
}

#[derive(Serialize, ToSchema)]
pub struct GetTenantsResponse {
    tenants: Vec<GetTenantResponse>,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = CreateTenantRequest,
    responses(
        (status = 200, description = "Create new tenant", body = PostTenantResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/tenants")]
pub async fn create_tenant(
    pool: Data<PgPool>,
    tenant: Json<CreateTenantRequest>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant = tenant.0;
    let id = tenant.id;
    root_span.record("project", &id);
    let name = tenant.name;
    let id = db::tenants::create_tenant(&pool, &id, &name).await?;
    let response = PostTenantResponse { id };
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = UpdateTenantRequest,
    responses(
        (status = 200, description = "Create a new tenant or update an existing one", body = PostTenantResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[put("/tenants/{tenant_id}")]
pub async fn create_or_update_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<String>,
    tenant: Json<UpdateTenantRequest>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant = tenant.0;
    let tenant_id = tenant_id.into_inner();
    root_span.record("project", &tenant_id);
    let name = tenant.name;
    let id = db::tenants::create_or_update_tenant(&pool, &tenant_id, &name).await?;
    let response = PostTenantResponse { id };
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("tenant_id" = i64, Path, description = "Id of the tenant"),
    ),
    responses(
        (status = 200, description = "Return tenant with id = tenant_id", body = GetTenantResponse),
        (status = 404, description = "Tenant not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/tenants/{tenant_id}")]
pub async fn read_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<String>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();
    root_span.record("project", &tenant_id);
    let response = db::tenants::read_tenant(&pool, &tenant_id)
        .await?
        .map(|t| GetTenantResponse {
            id: t.id,
            name: t.name,
        })
        .ok_or(TenantError::TenantNotFound(tenant_id))?;
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = UpdateTenantRequest,
    params(
        ("tenant_id" = i64, Path, description = "Id of the tenant"),
    ),
    responses(
        (status = 200, description = "Update tenant with id = tenant_id"),
        (status = 404, description = "Tenant not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/tenants/{tenant_id}")]
pub async fn update_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<String>,
    tenant: Json<UpdateTenantRequest>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();
    root_span.record("project", &tenant_id);
    db::tenants::update_tenant(&pool, &tenant_id, &tenant.0.name)
        .await?
        .ok_or(TenantError::TenantNotFound(tenant_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("tenant_id" = i64, Path, description = "Id of the tenant"),
    ),
    responses(
        (status = 200, description = "Delete tenant with id = tenant_id"),
        (status = 404, description = "Tenant not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[delete("/tenants/{tenant_id}")]
pub async fn delete_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<String>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();
    root_span.record("project", &tenant_id);
    db::tenants::delete_tenant(&pool, &tenant_id).await?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Return all tenants"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/tenants")]
pub async fn read_all_tenants(pool: Data<PgPool>) -> Result<impl Responder, TenantError> {
    let tenants: Vec<GetTenantResponse> = db::tenants::read_all_tenants(&pool)
        .await?
        .drain(..)
        .map(|t| GetTenantResponse {
            id: t.id,
            name: t.name,
        })
        .collect();
    let response = GetTenantsResponse { tenants };
    Ok(Json(response))
}

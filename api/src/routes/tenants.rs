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
pub async fn post_tenant(
    pool: Data<PgPool>,
    tenant: Json<PostTenantRequest>,
) -> Result<impl Responder, TenantError> {
    let id = save_tenant(&pool, tenant.0).await?;
    let response = PostTenantResponse { id };
    Ok(Json(response))
}

#[derive(Serialize)]
struct GetTenantResponse {
    id: i64,
    name: String,
}

#[get("/tenants/{tenant_id}")]
pub async fn get_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<i64>,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();
    let response = read_tenant(&pool, tenant_id)
        .await?
        .ok_or(TenantError::NotFound(tenant_id))?;
    Ok(Json(response))
}

async fn save_tenant(pool: &PgPool, tenant: PostTenantRequest) -> Result<i64, TenantError> {
    let record = sqlx::query!(
        r#"
        insert into tenants (name)
        values ($1)
        returning id
        "#,
        tenant.name
    )
    .fetch_one(pool)
    .await?;

    Ok(record.id)
}

async fn read_tenant(
    pool: &PgPool,
    tenant_id: i64,
) -> Result<Option<GetTenantResponse>, TenantError> {
    let record = sqlx::query!(
        r#"
        select id, name
        from tenants
        where id = $1
        "#,
        tenant_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| GetTenantResponse {
        id: r.id,
        name: r.name,
    }))
}

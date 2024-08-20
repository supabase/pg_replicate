use actix_web::{
    http::StatusCode,
    post,
    web::{Data, Json},
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
enum PostTenantError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
}

impl ResponseError for PostTenantError {
    fn status_code(&self) -> StatusCode {
        match self {
            PostTenantError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[post("/tenants")]
pub async fn post_tenant(
    pool: Data<PgPool>,
    tenant: Json<PostTenantRequest>,
) -> Result<impl Responder, PostTenantError> {
    let id = save_tenant(&pool, tenant.0).await?;
    let response = PostTenantResponse { id };
    Ok(Json(response))
}

async fn save_tenant(pool: &PgPool, tenant: PostTenantRequest) -> Result<i64, PostTenantError> {
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

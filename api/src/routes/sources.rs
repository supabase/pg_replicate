use actix_web::{
    http::StatusCode,
    post,
    web::{Data, Json},
    Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

use crate::db::{self, sources::SourceConfig};

#[derive(Debug, Error)]
enum SourceError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
    // #[error("source with id {0} not found")]
    // NotFound(i64),
}

impl ResponseError for SourceError {
    fn status_code(&self) -> StatusCode {
        match self {
            SourceError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            // SourceError::NotFound(_) => StatusCode::NOT_FOUND,
        }
    }
}

#[derive(Deserialize)]
struct PostSourceRequest {
    pub tenant_id: i64,
    pub config: SourceConfig,
}

#[derive(Serialize)]
struct PostSourceResponse {
    id: i64,
}

#[post("/sources")]
pub async fn create_source(
    pool: Data<PgPool>,
    source: Json<PostSourceRequest>,
) -> Result<impl Responder, SourceError> {
    let source = source.0;
    let tenant_id = source.tenant_id;
    let config = source.config;
    let id = db::sources::create_source(&pool, tenant_id, &config).await?;
    let response = PostSourceResponse { id };
    Ok(Json(response))
}

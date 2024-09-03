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

#[derive(Debug, Error)]
enum ImageError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("source with id {0} not found")]
    ImageNotFound(i64),
}

impl ImageError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            ImageError::DatabaseError(_) => "internal server error".to_string(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for ImageError {
    fn status_code(&self) -> StatusCode {
        match self {
            ImageError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ImageError::ImageNotFound(_) => StatusCode::NOT_FOUND,
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

#[derive(Deserialize)]
struct PostImageRequest {
    pub name: String,
    pub is_default: bool,
}

#[derive(Serialize)]
struct PostImageResponse {
    id: i64,
}

#[derive(Serialize)]
struct GetImageResponse {
    id: i64,
    name: String,
    is_default: bool,
}

#[post("/images")]
pub async fn create_image(
    pool: Data<PgPool>,
    image: Json<PostImageRequest>,
) -> Result<impl Responder, ImageError> {
    let image = image.0;
    let id = db::images::create_image(&pool, &image.name, image.is_default).await?;
    let response = PostImageResponse { id };
    Ok(Json(response))
}

#[get("/images/{image_id}")]
pub async fn read_image(
    pool: Data<PgPool>,
    image_id: Path<i64>,
) -> Result<impl Responder, ImageError> {
    let image_id = image_id.into_inner();
    let response = db::images::read_image(&pool, image_id)
        .await?
        .map(|s| GetImageResponse {
            id: s.id,
            name: s.name,
            is_default: s.is_default,
        })
        .ok_or(ImageError::ImageNotFound(image_id))?;
    Ok(Json(response))
}

#[post("/images/{image_id}")]
pub async fn update_image(
    pool: Data<PgPool>,
    image_id: Path<i64>,
    image: Json<PostImageRequest>,
) -> Result<impl Responder, ImageError> {
    let image_id = image_id.into_inner();
    db::images::update_image(&pool, image_id, &image.name, image.is_default)
        .await?
        .ok_or(ImageError::ImageNotFound(image_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[delete("/images/{image_id}")]
pub async fn delete_image(
    pool: Data<PgPool>,
    image_id: Path<i64>,
) -> Result<impl Responder, ImageError> {
    let image_id = image_id.into_inner();
    db::images::delete_image(&pool, image_id)
        .await?
        .ok_or(ImageError::ImageNotFound(image_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[get("/images")]
pub async fn read_all_images(pool: Data<PgPool>) -> Result<impl Responder, ImageError> {
    let mut sources = vec![];
    for image in db::images::read_all_images(&pool).await? {
        let image = GetImageResponse {
            id: image.id,
            name: image.name,
            is_default: image.is_default,
        };
        sources.push(image);
    }
    Ok(Json(sources))
}

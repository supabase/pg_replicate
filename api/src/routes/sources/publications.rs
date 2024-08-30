use actix_web::{
    delete, get,
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json, Path},
    HttpRequest, HttpResponse, Responder, ResponseError,
};
use serde::Deserialize;
use sqlx::PgPool;
use thiserror::Error;

use crate::{
    db::{self, publications::Publication, sources::SourceConfig, tables::Table},
    routes::ErrorMessage,
};

#[derive(Debug, Error)]
enum PublicationError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("source with id {0} not found")]
    SourceNotFound(i64),

    #[error("publication with name {0} not found")]
    PublicationNotFound(String),

    #[error("tenant id missing in request")]
    TenantIdMissing,

    #[error("tenant id ill formed in request")]
    TenantIdIllFormed,

    #[error("invalid source config")]
    InvalidConfig(#[from] serde_json::Error),
}

impl PublicationError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            PublicationError::DatabaseError(_) => "internal server error".to_string(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for PublicationError {
    fn status_code(&self) -> StatusCode {
        match self {
            PublicationError::DatabaseError(_) | PublicationError::InvalidConfig(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            PublicationError::SourceNotFound(_) | PublicationError::PublicationNotFound(_) => {
                StatusCode::NOT_FOUND
            }
            PublicationError::TenantIdMissing | PublicationError::TenantIdIllFormed => {
                StatusCode::BAD_REQUEST
            }
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

#[derive(Deserialize)]
struct CreatePublicationRequest {
    name: String,
    tables: Vec<Table>,
}

#[derive(Deserialize)]
struct DeletePublicationRequest {
    name: String,
}

#[post("/sources/{source_id}/publications")]
pub async fn create_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    source_id: Path<i64>,
    publication: Json<CreatePublicationRequest>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();

    let config = db::sources::read_source(&pool, tenant_id, source_id)
        .await?
        .map(|s| {
            let config: SourceConfig = serde_json::from_value(s.config)?;
            Ok::<SourceConfig, serde_json::Error>(config)
        })
        .transpose()?
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.connect_options();
    let publication = publication.0;
    let publication = Publication {
        name: publication.name,
        tables: publication.tables,
    };
    db::publications::create_publication(&publication, &options).await?;

    Ok(HttpResponse::Ok().finish())
}

#[get("/sources/{source_id}/publications/{publication_name}")]
pub async fn read_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    source_id_and_pub_name: Path<(i64, String)>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let (source_id, publication_name) = source_id_and_pub_name.into_inner();

    let config = db::sources::read_source(&pool, tenant_id, source_id)
        .await?
        .map(|s| {
            let config: SourceConfig = serde_json::from_value(s.config)?;
            Ok::<SourceConfig, serde_json::Error>(config)
        })
        .transpose()?
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.connect_options();
    let publications = db::publications::read_publication(&publication_name, &options)
        .await?
        .ok_or(PublicationError::PublicationNotFound(publication_name))?;

    Ok(Json(publications))
}

#[get("/sources/{source_id}/publications")]
pub async fn read_all_publications(
    req: HttpRequest,
    pool: Data<PgPool>,
    source_id: Path<i64>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();

    let config = db::sources::read_source(&pool, tenant_id, source_id)
        .await?
        .map(|s| {
            let config: SourceConfig = serde_json::from_value(s.config)?;
            Ok::<SourceConfig, serde_json::Error>(config)
        })
        .transpose()?
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.connect_options();
    let publications = db::publications::read_all_publications(&options).await?;

    Ok(Json(publications))
}

#[delete("/sources/{source_id}/publications")]
pub async fn delete_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    source_id: Path<i64>,
    publication: Json<DeletePublicationRequest>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();

    let config = db::sources::read_source(&pool, tenant_id, source_id)
        .await?
        .map(|s| {
            let config: SourceConfig = serde_json::from_value(s.config)?;
            Ok::<SourceConfig, serde_json::Error>(config)
        })
        .transpose()?
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.connect_options();
    let publication = publication.0;
    db::publications::drop_publication(&publication.name, &options).await?;

    Ok(HttpResponse::Ok().finish())
}

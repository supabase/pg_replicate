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
use utoipa::ToSchema;

use crate::{
    db::{self, publications::Publication, sources::SourcesDbError, tables::Table},
    encryption::EncryptionKey,
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

    #[error("sources db error: {0}")]
    SourcesDb(#[from] SourcesDbError),
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
            PublicationError::DatabaseError(_) | PublicationError::SourcesDb(_) => {
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
fn extract_tenant_id(req: &HttpRequest) -> Result<&str, PublicationError> {
    let headers = req.headers();
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(PublicationError::TenantIdMissing)?;
    let tenant_id = tenant_id
        .to_str()
        .map_err(|_| PublicationError::TenantIdIllFormed)?;
    Ok(tenant_id)
}

#[derive(Deserialize, ToSchema)]
pub struct CreatePublicationRequest {
    name: String,
    tables: Vec<Table>,
}

#[derive(Deserialize, ToSchema)]
pub struct UpdatePublicationRequest {
    tables: Vec<Table>,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = CreatePublicationRequest,
    responses(
        (status = 200, description = "Create new publication"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/sources/{source_id}/publications")]
pub async fn create_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id: Path<i64>,
    publication: Json<CreatePublicationRequest>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();

    let config = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
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

#[utoipa::path(
    context_path = "/v1",
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
        ("publication_name" = i64, Path, description = "Name of the publication"),
    ),
    responses(
        (status = 200, description = "Return publication with name = publication_name from source with id = source_id", body = Publication),
        (status = 404, description = "Publication not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/sources/{source_id}/publications/{publication_name}")]
pub async fn read_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id_and_pub_name: Path<(i64, String)>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let (source_id, publication_name) = source_id_and_pub_name.into_inner();

    let config = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.connect_options();
    let publications = db::publications::read_publication(&publication_name, &options)
        .await?
        .ok_or(PublicationError::PublicationNotFound(publication_name))?;

    Ok(Json(publications))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = UpdatePublicationRequest,
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
        ("publication_name" = i64, Path, description = "Name of the publication"),
    ),
    responses(
        (status = 200, description = "Update publication with name = publication_name from source with id = source_id"),
        (status = 404, description = "Publication not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/sources/{source_id}/publications/{publication_name}")]
pub async fn update_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id_and_pub_name: Path<(i64, String)>,
    publication: Json<UpdatePublicationRequest>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let (source_id, publication_name) = source_id_and_pub_name.into_inner();

    let config = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.connect_options();
    let publication = publication.0;
    let publication = Publication {
        name: publication_name,
        tables: publication.tables,
    };
    db::publications::update_publication(&publication, &options).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
        ("publication_name" = i64, Path, description = "Name of the publication"),
    ),
    responses(
        (status = 200, description = "Delete publication with name = publication_name from source with id = source_id"),
        (status = 404, description = "Publication not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[delete("/sources/{source_id}/publications/{publication_name}")]
pub async fn delete_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id_and_pub_name: Path<(i64, String)>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let (source_id, publication_name) = source_id_and_pub_name.into_inner();

    let config = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.connect_options();
    db::publications::drop_publication(&publication_name, &options).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
    ),
    responses(
        (status = 200, description = "Return all publications"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/sources/{source_id}/publications")]
pub async fn read_all_publications(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id: Path<i64>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();

    let config = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.connect_options();
    let publications = db::publications::read_all_publications(&options).await?;

    Ok(Json(publications))
}

use actix_web::HttpRequest;
use serde::Serialize;
use thiserror::Error;

pub mod health_check;
pub mod images;
pub mod pipelines;
pub mod sinks;
pub mod sources;
pub mod tenants;
pub mod tenants_sources;

#[derive(Serialize)]
pub struct ErrorMessage {
    pub error: String,
}

#[derive(Debug, Error)]
pub enum TenantIdError {
    #[error("tenant id missing in request")]
    TenantIdMissing,

    #[error("tenant id ill formed in request")]
    TenantIdIllFormed,
}

fn extract_tenant_id(req: &HttpRequest) -> Result<&str, TenantIdError> {
    let headers = req.headers();
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(TenantIdError::TenantIdMissing)?;
    let tenant_id = tenant_id
        .to_str()
        .map_err(|_| TenantIdError::TenantIdIllFormed)?;
    Ok(tenant_id)
}

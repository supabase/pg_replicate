use std::num::ParseIntError;

use rustyline::{error::ReadlineError, DefaultEditor};
use thiserror::Error;

use crate::api_client::{
    ApiClient, ApiClientError, CreateTenantRequest, CreateTenantResponse, TenantResponse,
};

#[derive(Debug, Error)]
pub enum CliError {
    #[error("readline error: {0}")]
    Readline(#[from] ReadlineError),

    #[error("api client error: {0}")]
    ApiClient(#[from] ApiClientError),

    #[error("parse int error: {0}")]
    ParseInt(#[from] ParseIntError),
}

pub async fn create_tenant(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<CreateTenantResponse, CliError> {
    let name = editor.readline("enter tenant name: ")?;
    let name = name.trim().to_string();

    let project_ref = editor.readline(
        "enter supabase project ref (leave emptry is project is not hosted on supabase): ",
    )?;
    let project_ref = project_ref.trim().to_lowercase();
    let supabase_project_ref = if project_ref.is_empty() {
        None
    } else {
        Some(project_ref)
    };

    let tenant = api_client
        .create_tenant(&CreateTenantRequest {
            name,
            supabase_project_ref,
        })
        .await?;

    Ok(tenant)
}

pub async fn read_tenant(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<TenantResponse, CliError> {
    let tenant_id = editor.readline("enter tenant id: ")?;
    let tenant_id = tenant_id.trim().parse()?;
    let tenant = api_client.read_tenant(tenant_id).await?;
    Ok(tenant)
}

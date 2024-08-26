use rustyline::{error::ReadlineError, DefaultEditor};
use thiserror::Error;

use crate::api_client::{ApiClient, ApiClientError, CreateTenantRequest, CreateTenantResponse};

#[derive(Debug, Error)]
pub enum CreateTenantError {
    #[error("readline error: {0}")]
    Readline(#[from] ReadlineError),

    #[error("api client error: {0}")]
    ApiClient(#[from] ApiClientError),
}

pub async fn create_tenant(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<CreateTenantResponse, CreateTenantError> {
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

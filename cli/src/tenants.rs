use std::num::ParseIntError;

use rustyline::{error::ReadlineError, DefaultEditor};
use thiserror::Error;

use crate::api_client::{
    ApiClient, ApiClientError, CreateTenantRequest, CreateTenantResponse, TenantResponse,
    UpdateTenantRequest,
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
    let name = get_tenant_name(editor)?;
    let supabase_project_ref = get_project_ref(editor)?;

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
    let tenant_id = get_tenant_id(editor)?;

    let tenant = api_client.read_tenant(tenant_id).await?;

    Ok(tenant)
}

pub async fn update_tenant(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<(), CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let name = get_tenant_name(editor)?;

    let tenant = UpdateTenantRequest { name };
    api_client.update_tenant(tenant_id, &tenant).await?;

    Ok(())
}

pub async fn delete_tenant(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<(), CliError> {
    let tenant_id = get_tenant_id(editor)?;

    api_client.delete_tenant(tenant_id).await?;

    Ok(())
}

fn get_tenant_name(editor: &mut DefaultEditor) -> Result<String, CliError> {
    let name = editor.readline("enter tenant name: ")?;
    let name = name.trim().to_string();
    Ok(name)
}

fn get_project_ref(editor: &mut DefaultEditor) -> Result<Option<String>, CliError> {
    let project_ref = editor.readline(
        "enter supabase project ref (leave emptry is project is not hosted on supabase): ",
    )?;
    let project_ref = project_ref.trim().to_lowercase();
    let supabase_project_ref = if project_ref.is_empty() {
        None
    } else {
        Some(project_ref)
    };
    Ok(supabase_project_ref)
}

fn get_tenant_id(editor: &mut DefaultEditor) -> Result<i64, CliError> {
    let tenant_id = editor.readline("enter tenant id: ")?;
    let tenant_id = tenant_id.trim().parse()?;
    Ok(tenant_id)
}

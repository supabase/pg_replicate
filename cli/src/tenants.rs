use rustyline::DefaultEditor;

use crate::{
    api_client::{
        ApiClient, CreateTenantRequest, CreateTenantResponse, TenantResponse, UpdateTenantRequest,
    },
    get_id, get_string, CliError,
};

pub async fn create_tenant(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<CreateTenantResponse, CliError> {
    let name = get_tenant_name(editor)?;

    let tenant = api_client
        .create_tenant(&CreateTenantRequest { name })
        .await?;

    Ok(tenant)
}

pub async fn show_tenant(
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

pub async fn list_tenants(api_client: &ApiClient) -> Result<Vec<TenantResponse>, CliError> {
    let tenants = api_client.read_all_tenants().await?;

    Ok(tenants)
}

fn get_tenant_name(editor: &mut DefaultEditor) -> Result<String, CliError> {
    get_string(editor, "enter tenant name: ")
}

pub fn get_tenant_id(editor: &mut DefaultEditor) -> Result<i64, CliError> {
    get_id(editor, "enter tenant id: ")
}

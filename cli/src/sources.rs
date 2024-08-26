use rustyline::DefaultEditor;

use crate::{
    api_client::{
        ApiClient, CreateSourceRequest, CreateSourceResponse, SourceConfig, SourceResponse,
        UpdateSourceRequest,
    },
    get_id, get_string,
    tenants::get_tenant_id,
    CliError,
};

pub async fn create_source(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<CreateSourceResponse, CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let config = get_source_config(editor)?;
    let source = api_client
        .create_source(tenant_id, &CreateSourceRequest { config })
        .await?;

    Ok(source)
}

pub async fn show_source(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<SourceResponse, CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let source_id = get_source_id(editor)?;

    let source = api_client.read_source(tenant_id, source_id).await?;

    Ok(source)
}

pub async fn update_source(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<(), CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let source_id = get_source_id(editor)?;
    let config = get_source_config(editor)?;

    let source = UpdateSourceRequest { config };
    api_client
        .update_source(tenant_id, source_id, &source)
        .await?;

    Ok(())
}

pub async fn delete_source(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<(), CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let source_id = get_source_id(editor)?;

    api_client.delete_source(tenant_id, source_id).await?;

    Ok(())
}

pub async fn list_sources(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<Vec<SourceResponse>, CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let tenants = api_client.read_all_sources(tenant_id).await?;

    Ok(tenants)
}

fn get_source_config(editor: &mut DefaultEditor) -> Result<SourceConfig, CliError> {
    let host = get_string(editor, "enter host: ")?;
    let port = get_string(editor, "enter port: ")?;
    let port: u16 = port.parse()?;
    let name = get_string(editor, "enter name: ")?;
    let username = get_string(editor, "enter username: ")?;
    let password = get_string(editor, "enter password: ")?;
    let password = if password.is_empty() {
        None
    } else {
        Some(password)
    };
    let slot_name = get_string(editor, "enter slot name: ")?;
    let publication = get_string(editor, "enter publication: ")?;

    Ok(SourceConfig::Postgres {
        host,
        port,
        name,
        username,
        password,
        slot_name,
        publication,
    })
}

pub fn get_source_id(editor: &mut DefaultEditor) -> Result<i64, CliError> {
    get_id(editor, "enter source id: ")
}

use rustyline::DefaultEditor;

use crate::{
    api_client::{
        ApiClient, CreatePublicationRequest, CreatePublicationResponse, PublicationConfig,
        PublicationResponse, UpdatePublicationRequest,
    },
    get_id, get_string,
    sources::get_source_id,
    tenants::get_tenant_id,
    CliError,
};

pub async fn create_publication(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<CreatePublicationResponse, CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let source_id = get_source_id(editor)?;
    let config = get_publication_config(editor)?;
    let publication = api_client
        .create_publication(tenant_id, &CreatePublicationRequest { source_id, config })
        .await?;

    Ok(publication)
}

pub async fn show_publication(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<PublicationResponse, CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let publication_id = get_publication_id(editor)?;

    let publication = api_client
        .read_publication(tenant_id, publication_id)
        .await?;

    Ok(publication)
}

pub async fn update_publication(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<(), CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let source_id = get_source_id(editor)?;
    let publication_id = get_publication_id(editor)?;
    let config = get_publication_config(editor)?;

    let publication = UpdatePublicationRequest { source_id, config };
    api_client
        .update_publication(tenant_id, publication_id, &publication)
        .await?;

    Ok(())
}

pub async fn delete_publication(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<(), CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let publication_id = get_publication_id(editor)?;

    api_client
        .delete_publication(tenant_id, publication_id)
        .await?;

    Ok(())
}

pub async fn list_publications(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<Vec<PublicationResponse>, CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let tenants = api_client.read_all_publications(tenant_id).await?;

    Ok(tenants)
}

fn get_publication_config(editor: &mut DefaultEditor) -> Result<PublicationConfig, CliError> {
    let table_names = get_string(editor, "enter comma separated table names: ")?;
    let table_names: Vec<String> = table_names
        .split(',')
        .map(|table_name| table_name.trim().to_string())
        .collect();
    Ok(PublicationConfig { table_names })
}

pub fn get_publication_id(editor: &mut DefaultEditor) -> Result<i64, CliError> {
    get_id(editor, "enter publication id: ")
}

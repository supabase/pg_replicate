use rustyline::DefaultEditor;

use crate::{
    api_client::{ApiClient, CreateSourceRequest, CreateSourceResponse, SourceConfig},
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

use rustyline::DefaultEditor;

use crate::{
    api_client::{ApiClient, CreateSinkRequest, CreateSinkResponse, SinkConfig},
    get_id, get_string,
    tenants::get_tenant_id,
    CliError,
};

pub async fn create_sink(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<CreateSinkResponse, CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let config = get_sink_config(editor)?;
    let sink = api_client
        .create_sink(tenant_id, &CreateSinkRequest { config })
        .await?;

    Ok(sink)
}

fn get_sink_config(editor: &mut DefaultEditor) -> Result<SinkConfig, CliError> {
    let project_id = get_string(editor, "enter project_id: ")?;
    let dataset_id = get_string(editor, "enter dataset_id: ")?;
    let service_account_key = get_string(editor, "enter service_account_key: ")?;
    Ok(SinkConfig::BigQuery {
        project_id,
        dataset_id,
        service_account_key,
    })
}

pub fn get_sink_id(editor: &mut DefaultEditor) -> Result<i64, CliError> {
    get_id(editor, "enter sink id: ")
}

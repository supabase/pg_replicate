use rustyline::DefaultEditor;

use crate::{
    api_client::{
        ApiClient, BatchConfig, CreatePipelineRequest, CreatePipelineResponse, PipelineConfig,
    },
    get_u64, get_usize,
    sinks::get_sink_id,
    sources::get_source_id,
    tenants::get_tenant_id,
    CliError,
};

pub async fn create_pipeline(
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) -> Result<CreatePipelineResponse, CliError> {
    let tenant_id = get_tenant_id(editor)?;
    let source_id = get_source_id(editor)?;
    let sink_id = get_sink_id(editor)?;
    let config = get_pipeline_config(editor)?;
    let pipeline = api_client
        .create_pipeline(
            tenant_id,
            &CreatePipelineRequest {
                source_id,
                sink_id,
                config,
            },
        )
        .await?;

    Ok(pipeline)
}

// pub fn get_pipeline_id(editor: &mut DefaultEditor) -> Result<i64, CliError> {
//     get_id(editor, "enter pipeline id: ")
// }

fn get_pipeline_config(editor: &mut DefaultEditor) -> Result<PipelineConfig, CliError> {
    let max_size = get_usize(editor, "enter max batch size: ")?;
    let max_fill_secs = get_u64(editor, "enter max batch fill seconds: ")?;
    let config = BatchConfig {
        max_size,
        max_fill_secs,
    };
    Ok(PipelineConfig { config })
}

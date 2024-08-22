use api::db::pipelines::{BatchConfig, PipelineConfig};

use crate::{
    sinks::create_sink,
    sources::create_source,
    tenants::create_tenant,
    test_app::{spawn_app, CreatePipelineRequest, CreatePipelineResponse},
};

fn new_pipeline_config() -> PipelineConfig {
    PipelineConfig {
        config: BatchConfig {
            max_size: 1000,
            max_fill_secs: 5,
        },
    }
}

#[tokio::test]
async fn pipeline_can_be_created() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let sink_id = create_sink(&app, tenant_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id,
        sink_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

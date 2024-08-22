use api::db::pipelines::{BatchConfig, PipelineConfig};
use reqwest::StatusCode;

use crate::{
    sinks::create_sink,
    sources::create_source,
    tenants::create_tenant,
    test_app::{
        spawn_app, CreatePipelineRequest, CreatePipelineResponse, PipelineResponse,
        UpdatePipelineRequest,
    },
};

fn new_pipeline_config() -> PipelineConfig {
    PipelineConfig {
        config: BatchConfig {
            max_size: 1000,
            max_fill_secs: 5,
        },
    }
}

fn updated_pipeline_config() -> PipelineConfig {
    PipelineConfig {
        config: BatchConfig {
            max_size: 2000,
            max_fill_secs: 10,
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

#[tokio::test]
async fn an_existing_pipeline_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let sink_id = create_sink(&app, tenant_id).await;
    let pipeline = CreatePipelineRequest {
        source_id,
        sink_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let response = app.read_pipeline(tenant_id, pipeline_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: PipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, sink_id);
    assert_eq!(response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.sink_id, sink_id);
    assert_eq!(response.config, pipeline.config);
}

#[tokio::test]
async fn an_non_existing_pipeline_cant_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let response = app.read_pipeline(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_pipeline_can_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let sink_id = create_sink(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        sink_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let source_id = create_source(&app, tenant_id).await;
    let sink_id = create_sink(&app, tenant_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id,
        sink_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant_id, pipeline_id, &updated_config)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: PipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.sink_id, sink_id);
    assert_eq!(response.config, updated_config.config);
}

#[tokio::test]
async fn an_non_existing_pipeline_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let sink_id = create_sink(&app, tenant_id).await;

    // Act
    let updated_config = UpdatePipelineRequest {
        source_id,
        sink_id,
        config: updated_pipeline_config(),
    };
    let response = app.update_pipeline(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

use api::db::pipelines::{BatchConfig, PipelineConfig};
use reqwest::StatusCode;

use crate::{
    publications::create_publication,
    sinks::create_sink,
    sources::create_source,
    tenants::create_tenant,
    test_app::{
        spawn_app, CreatePipelineRequest, CreatePipelineResponse, PipelineResponse, TestApp,
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

pub async fn create_pipeline_with_config(
    app: &TestApp,
    tenant_id: i64,
    source_id: i64,
    sink_id: i64,
    publication_id: i64,
    config: PipelineConfig,
) -> i64 {
    let pipeline = CreatePipelineRequest {
        source_id,
        sink_id,
        publication_id,
        config,
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    response.id
}

#[tokio::test]
async fn pipeline_can_be_created() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let sink_id = create_sink(&app, tenant_id).await;
    let publication_id = create_publication(&app, tenant_id, source_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id,
        sink_id,
        publication_id,
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
async fn pipeline_with_another_tenants_source_cant_be_created() {
    // Arrange
    let app = spawn_app().await;
    let tenant1_id = create_tenant(&app).await;
    let tenant2_id = create_tenant(&app).await;
    let source1_id = create_source(&app, tenant1_id).await;
    let source2_id = create_source(&app, tenant2_id).await;
    let sink1_id = create_sink(&app, tenant1_id).await;
    let publication1_id = create_publication(&app, tenant1_id, source1_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id: source2_id,
        sink_id: sink1_id,
        publication_id: publication1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn pipeline_with_another_tenants_sink_cant_be_created() {
    // Arrange
    let app = spawn_app().await;
    let tenant1_id = create_tenant(&app).await;
    let tenant2_id = create_tenant(&app).await;
    let source1_id = create_source(&app, tenant1_id).await;
    let sink2_id = create_sink(&app, tenant2_id).await;
    let publication1_id = create_publication(&app, tenant1_id, source1_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        sink_id: sink2_id,
        publication_id: publication1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn pipeline_with_another_tenants_publication_cant_be_created() {
    // Arrange
    let app = spawn_app().await;
    let tenant1_id = create_tenant(&app).await;
    let tenant2_id = create_tenant(&app).await;
    let source1_id = create_source(&app, tenant1_id).await;
    let source2_id = create_source(&app, tenant2_id).await;
    let sink1_id = create_sink(&app, tenant1_id).await;
    let publication2_id = create_publication(&app, tenant2_id, source2_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        sink_id: sink1_id,
        publication_id: publication2_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn an_existing_pipeline_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let sink_id = create_sink(&app, tenant_id).await;
    let publication_id = create_publication(&app, tenant_id, source_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        sink_id,
        publication_id,
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
    let publication_id = create_publication(&app, tenant_id, source_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        sink_id,
        publication_id,
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
    let publication_id = create_publication(&app, tenant_id, source_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id,
        sink_id,
        publication_id,
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
async fn pipeline_with_another_tenants_source_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant1_id = create_tenant(&app).await;
    let tenant2_id = create_tenant(&app).await;
    let source1_id = create_source(&app, tenant1_id).await;
    let sink1_id = create_sink(&app, tenant1_id).await;
    let publication1_id = create_publication(&app, tenant1_id, source1_id).await;

    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        sink_id: sink1_id,
        publication_id: publication1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let source2_id = create_source(&app, tenant2_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id: source2_id,
        sink_id: sink1_id,
        publication_id: publication1_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant1_id, pipeline_id, &updated_config)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn pipeline_with_another_tenants_sink_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant1_id = create_tenant(&app).await;
    let tenant2_id = create_tenant(&app).await;
    let source1_id = create_source(&app, tenant1_id).await;
    let sink1_id = create_sink(&app, tenant1_id).await;
    let publication1_id = create_publication(&app, tenant1_id, source1_id).await;

    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        sink_id: sink1_id,
        publication_id: publication1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let sink2_id = create_sink(&app, tenant2_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id: source1_id,
        sink_id: sink2_id,
        publication_id: publication1_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant1_id, pipeline_id, &updated_config)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn pipeline_with_another_tenants_publication_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant1_id = create_tenant(&app).await;
    let tenant2_id = create_tenant(&app).await;
    let source1_id = create_source(&app, tenant1_id).await;
    let source2_id = create_source(&app, tenant2_id).await;
    let sink1_id = create_sink(&app, tenant1_id).await;
    let publication1_id = create_publication(&app, tenant1_id, source1_id).await;

    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        sink_id: sink1_id,
        publication_id: publication1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let publication2_id = create_publication(&app, tenant2_id, source2_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id: source1_id,
        sink_id: sink1_id,
        publication_id: publication2_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant1_id, pipeline_id, &updated_config)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn an_non_existing_pipeline_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let sink_id = create_sink(&app, tenant_id).await;
    let publication_id = create_publication(&app, tenant_id, source_id).await;

    // Act
    let updated_config = UpdatePipelineRequest {
        source_id,
        sink_id,
        publication_id,
        config: updated_pipeline_config(),
    };
    let response = app.update_pipeline(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_pipeline_can_be_deleted() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let sink_id = create_sink(&app, tenant_id).await;
    let publication_id = create_publication(&app, tenant_id, source_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        sink_id,
        publication_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let response = app.delete_pipeline(tenant_id, pipeline_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_non_existing_pipeline_cant_be_deleted() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let response = app.delete_pipeline(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn all_pipelines_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source1_id = create_source(&app, tenant_id).await;
    let source2_id = create_source(&app, tenant_id).await;
    let sink1_id = create_sink(&app, tenant_id).await;
    let sink2_id = create_sink(&app, tenant_id).await;
    let publication1_id = create_publication(&app, tenant_id, source1_id).await;
    let publication2_id = create_publication(&app, tenant_id, source2_id).await;

    let pipeline1_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source1_id,
        sink1_id,
        publication1_id,
        new_pipeline_config(),
    )
    .await;
    let pipeline2_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source2_id,
        sink2_id,
        publication2_id,
        updated_pipeline_config(),
    )
    .await;

    // Act
    let response = app.read_all_pipelines(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: Vec<PipelineResponse> = response
        .json()
        .await
        .expect("failed to deserialize response");
    for pipeline in response {
        if pipeline.id == pipeline1_id {
            let config = new_pipeline_config();
            assert_eq!(pipeline.tenant_id, tenant_id);
            assert_eq!(pipeline.source_id, source1_id);
            assert_eq!(pipeline.sink_id, sink1_id);
            assert_eq!(pipeline.publication_id, publication1_id);
            assert_eq!(pipeline.config, config);
        } else if pipeline.id == pipeline2_id {
            let config = updated_pipeline_config();
            assert_eq!(pipeline.tenant_id, tenant_id);
            assert_eq!(pipeline.source_id, source2_id);
            assert_eq!(pipeline.sink_id, sink2_id);
            assert_eq!(pipeline.publication_id, publication2_id);
            assert_eq!(pipeline.config, config);
        }
    }
}

use reqwest::StatusCode;

use crate::{
    common::test_app::{
        spawn_test_app, CreateSinkPipelineResponse, PipelineResponse, PostSinkPipelineRequest,
        SinkResponse,
    },
    integration::images_test::create_default_image,
    integration::pipelines_test::{new_pipeline_config, updated_pipeline_config},
    integration::sinks_test::{
        create_sink, new_name, new_sink_config, updated_name, updated_sink_config,
    },
    integration::sources_test::create_source,
    integration::tenants_test::{create_tenant, create_tenant_with_id_and_name},
};

#[tokio::test(flavor = "multi_thread")]
async fn sink_and_pipeline_can_be_created() {
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    create_default_image(&app).await;

    // Act
    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: new_name(),
        sink_config: new_sink_config(),
        source_id,
        publication_name: "publication".to_string(),
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_sink_pipeline(tenant_id, &sink_pipeline).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateSinkPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.sink_id, 1);
    assert_eq!(response.pipeline_id, 1);

    let sink_id = response.sink_id;
    let pipeline_id = response.pipeline_id;

    let response = app.read_sink(tenant_id, sink_id).await;
    let response: SinkResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, sink_id);
    assert_eq!(response.name, sink_pipeline.sink_name);
    assert_eq!(response.config, sink_pipeline.sink_config);

    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: PipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.sink_id, sink_id);
    assert_eq!(response.publication_name, "publication");
    assert_eq!(response.replicator_id, 1);
    assert_eq!(response.config, sink_pipeline.pipeline_config);
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_and_pipeline_with_another_tenants_source_cant_be_created() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;
    let source2_id = create_source(&app, tenant2_id).await;

    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: new_name(),
        sink_config: new_sink_config(),
        source_id: source2_id,
        publication_name: "publication".to_string(),
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_sink_pipeline(tenant1_id, &sink_pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_sink_and_pipeline_can_be_updated() {
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    create_default_image(&app).await;
    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: new_name(),
        sink_config: new_sink_config(),
        source_id,
        publication_name: "publication".to_string(),
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_sink_pipeline(tenant_id, &sink_pipeline).await;
    let response: CreateSinkPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let CreateSinkPipelineResponse {
        sink_id,
        pipeline_id,
    } = response;
    let new_source_id = create_source(&app, tenant_id).await;

    // Act
    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: updated_name(),
        sink_config: updated_sink_config(),
        source_id: new_source_id,
        publication_name: "updated_publication".to_string(),
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_sink_pipeline(tenant_id, sink_id, pipeline_id, &sink_pipeline)
        .await;

    // Assert
    assert!(response.status().is_success());

    let response = app.read_sink(tenant_id, sink_id).await;
    let response: SinkResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, sink_id);
    assert_eq!(response.name, sink_pipeline.sink_name);
    assert_eq!(response.config, sink_pipeline.sink_config);

    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: PipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, sink_pipeline.source_id);
    assert_eq!(response.sink_id, sink_id);
    assert_eq!(response.publication_name, sink_pipeline.publication_name);
    assert_eq!(response.replicator_id, 1);
    assert_eq!(response.config, sink_pipeline.pipeline_config);
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_and_pipeline_with_another_tenants_source_cant_be_updated() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;

    let source1_id = create_source(&app, tenant1_id).await;
    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: new_name(),
        sink_config: new_sink_config(),
        source_id: source1_id,
        publication_name: "publication".to_string(),
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_sink_pipeline(tenant1_id, &sink_pipeline).await;
    let response: CreateSinkPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let CreateSinkPipelineResponse {
        sink_id,
        pipeline_id,
    } = response;

    // Act
    let source2_id = create_source(&app, tenant2_id).await;
    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: updated_name(),
        sink_config: updated_sink_config(),
        source_id: source2_id,
        publication_name: "updated_publication".to_string(),
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_sink_pipeline(tenant1_id, sink_id, pipeline_id, &sink_pipeline)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_and_pipeline_with_another_tenants_sink_cant_be_updated() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;

    let source1_id = create_source(&app, tenant1_id).await;
    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: new_name(),
        sink_config: new_sink_config(),
        source_id: source1_id,
        publication_name: "publication".to_string(),
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_sink_pipeline(tenant1_id, &sink_pipeline).await;
    let response: CreateSinkPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let CreateSinkPipelineResponse { pipeline_id, .. } = response;

    // Act
    let sink2_id = create_sink(&app, tenant2_id).await;
    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: updated_name(),
        sink_config: updated_sink_config(),
        source_id: source1_id,
        publication_name: "updated_publication".to_string(),
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_sink_pipeline(tenant1_id, sink2_id, pipeline_id, &sink_pipeline)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_and_pipeline_with_another_tenants_pipeline_cant_be_updated() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;

    let source1_id = create_source(&app, tenant1_id).await;
    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: new_name(),
        sink_config: new_sink_config(),
        source_id: source1_id,
        publication_name: "publication".to_string(),
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_sink_pipeline(tenant1_id, &sink_pipeline).await;
    let response: CreateSinkPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let CreateSinkPipelineResponse {
        sink_id: sink1_id, ..
    } = response;

    let source2_id = create_source(&app, tenant2_id).await;
    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: new_name(),
        sink_config: new_sink_config(),
        source_id: source2_id,
        publication_name: "publication".to_string(),
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_sink_pipeline(tenant2_id, &sink_pipeline).await;
    let response: CreateSinkPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let CreateSinkPipelineResponse {
        pipeline_id: pipeline2_id,
        ..
    } = response;

    // Act
    let sink_pipeline = PostSinkPipelineRequest {
        sink_name: updated_name(),
        sink_config: updated_sink_config(),
        source_id: source1_id,
        publication_name: "updated_publication".to_string(),
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_sink_pipeline(tenant1_id, sink1_id, pipeline2_id, &sink_pipeline)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

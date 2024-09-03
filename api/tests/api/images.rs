use crate::test_app::{CreateImageRequest, CreateImageResponse, TestApp};

pub async fn create_default_image(app: &TestApp) -> i64 {
    create_image_with_name(app, "some/image".to_string(), true).await
}

pub async fn create_image_with_name(app: &TestApp, name: String, is_default: bool) -> i64 {
    let image = CreateImageRequest { name, is_default };
    let response = app.create_image(&image).await;
    let response: CreateImageResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    response.id
}

use actix_web::{get, HttpResponse, Responder};

#[utoipa::path(
    responses(
        (status = 200, description = "Api is healthy"),
    )
)]
#[get("/health_check")]
pub async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("ok")
}

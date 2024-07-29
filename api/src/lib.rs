use actix_web::{dev::Server, get, App, HttpResponse, HttpServer, Responder};

mod queue;

pub fn run() -> Result<Server, std::io::Error> {
    let server = HttpServer::new(|| App::new().service(health_check))
        .bind("127.0.0.1:8000")?
        .run();

    Ok(server)
}

#[get("/health_check")]
async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("ok")
}

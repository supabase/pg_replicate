use api::run;

#[actix_web::main]
pub async fn main() -> Result<(), std::io::Error> {
    run()?.await
}

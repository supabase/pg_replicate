use actix_web::{dev::ServiceRequest, web::Data, Error};
use actix_web_httpauth::extractors::{
    bearer::{BearerAuth, Config},
    AuthenticationError,
};
use tracing::error;

pub async fn auth_validator(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    let api_key: &str = req.app_data::<Data<String>>().expect("missing api_key");
    let token = credentials.token();

    //TODO: Do a constant time comparison
    if api_key != token {
        error!("authetication failed");
        let config = req
            .app_data::<Config>()
            .cloned()
            .unwrap_or_default()
            .scope("v1");

        return Err((AuthenticationError::from(config).into(), req));
    }

    Ok(req)
}

use actix_web::{dev::ServiceRequest, web::Data, Error};
use actix_web_httpauth::extractors::{
    bearer::{BearerAuth, Config},
    AuthenticationError,
};
use constant_time_eq::constant_time_eq_n;

use crate::config::ApiKey;

pub async fn auth_validator(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    let config = req
        .app_data::<Config>()
        .cloned()
        .unwrap_or_default()
        .scope("v1");

    let api_key: &str = req.app_data::<Data<String>>().expect("missing api_key");
    let token = credentials.token();

    let api_key: ApiKey = match api_key.try_into() {
        Ok(api_key) => api_key,
        Err(_) => {
            return Err((AuthenticationError::from(config).into(), req));
        }
    };

    let token: ApiKey = match token.try_into() {
        Ok(token) => token,
        Err(_) => {
            return Err((AuthenticationError::from(config).into(), req));
        }
    };

    if !constant_time_eq_n(&api_key.key, &token.key) {
        return Err((AuthenticationError::from(config).into(), req));
    }

    Ok(req)
}

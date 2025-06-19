/// The name of the development environment.
pub const DEV_ENV_NAME: &str = "dev";

/// The name of the production environment.
pub const PROD_ENV_NAME: &str = "prod";

/// Represents the runtime environment for the application.
///
/// Use [`Environment`] to distinguish between development and production modes.
pub enum Environment {
    /// Development environment.
    Dev,
    /// Production environment.
    Prod,
}

impl Environment {
    /// Returns the string name of the environment.
    ///
    /// The returned value matches the corresponding constant, such as [`DEV_ENV_NAME`] or [`PROD_ENV_NAME`].
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Dev => DEV_ENV_NAME,
            Environment::Prod => PROD_ENV_NAME,
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = String;

    /// Attempts to create an [`Environment`] from a string, case-insensitively.
    ///
    /// Accepts "dev" or "prod". Returns an error if the input does not match a supported environment.
    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "dev" => Ok(Self::Dev),
            "prod" => Ok(Self::Prod),
            other => Err(format!(
                "{other} is not a supported environment. Use either `{DEV_ENV_NAME}` or `{PROD_ENV_NAME}`.",
            )),
        }
    }
}

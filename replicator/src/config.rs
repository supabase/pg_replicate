use config::replicator::{ReplicatorConfig, ValidationError};
use thiserror::Error;

use crate::environment::{Environment, DEV_ENV_NAME};

/// Directory containing configuration files.
const CONFIGURATION_DIR: &str = "configuration";
/// Name of the base configuration file.
const BASE_CONFIG_FILE: &str = "base.yaml";
/// Prefix for environment variable configuration overrides.
const ENV_PREFIX: &str = "APP";
/// Separator for environment variable prefix.
const ENV_PREFIX_SEPARATOR: &str = "_";
/// Separator for nested environment variable keys.
const ENV_SEPARATOR: &str = "__";

#[derive(Debug, Error)]
pub enum ReplicatorConfigError {
    #[error("An error occurred while validating the loaded configuration: {0}")]
    Validation(#[from] ValidationError),
}

/// Loads the [`ReplicatorConfig`] from configuration files and environment variables.
///
/// The configuration is loaded from the base and environment-specific YAML files in the configuration directory,
/// then overridden by environment variables with the `APP` prefix. After loading, the configuration is validated.
///
/// # Errors
/// Returns [`ReplicatorConfigError`] if loading or validation fails.
///
/// # Panics
/// Panics if the current working directory cannot be determined or if the `APP_ENVIRONMENT` variable is invalid.
pub fn load_config() -> anyhow::Result<ReplicatorConfig> {
    let base_path = std::env::current_dir().expect("Failed to determine the current directory");
    let config_dir = base_path.join(CONFIGURATION_DIR);

    // Detect the running environment.
    let environment: Environment = std::env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| DEV_ENV_NAME.into())
        .try_into()
        .expect("Failed to parse 'APP_ENVIRONMENT'.");
    let environment_filename = format!("{}.yaml", environment.as_str());

    // Load the configuration from multiple sources.
    let config = rust_cli_config::Config::builder()
        .add_source(rust_cli_config::File::from(
            config_dir.join(BASE_CONFIG_FILE),
        ))
        .add_source(rust_cli_config::File::from(
            config_dir.join(environment_filename),
        ))
        .add_source(
            rust_cli_config::Environment::with_prefix(ENV_PREFIX)
                .prefix_separator(ENV_PREFIX_SEPARATOR)
                .separator(ENV_SEPARATOR),
        )
        .build()?;

    // Try to convert the config object into our own object and validate it.
    let config: ReplicatorConfig = config.try_deserialize()?;
    config.validate()?;

    Ok(config)
}

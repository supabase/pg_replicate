use std::fmt::Debug;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SourceSettings {
    Postgres {
        /// Host on which Postgres is running
        host: String,

        /// Port on which Postgres is running
        port: u16,

        /// Postgres database name
        name: String,

        /// Postgres database user name
        username: String,

        /// Postgres database user password
        password: Option<String>,

        /// Postgres slot name
        slot_name: String,

        /// Postgres publication name
        publication: String,
    },
}

impl Debug for SourceSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres {
                host,
                port,
                name,
                username,
                password: _,
                slot_name,
                publication,
            } => f
                .debug_struct("Postgres")
                .field("host", host)
                .field("port", port)
                .field("name", name)
                .field("username", username)
                .field("password", &"REDACTED")
                .field("slot_name", slot_name)
                .field("publication", publication)
                .finish(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SinkSettings {
    BigQuery {
        /// BigQuery project id
        project_id: String,

        /// BigQuery dataset id
        dataset_id: String,

        /// BigQuery service account key
        service_account_key: String,
    },
}

impl Debug for SinkSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key: _,
            } => f
                .debug_struct("BigQuery")
                .field("project_id", project_id)
                .field("dataset_id", dataset_id)
                .field("service_account_key", &"REDACTED")
                .finish(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BatchSettings {
    /// maximum batch size in number of events
    pub max_size: usize,

    /// maximum duration, in seconds, to wait for a batch to fill
    pub max_fill_secs: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Settings {
    pub source: SourceSettings,
    pub sink: SinkSettings,
    pub batch: BatchSettings,
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    let base_path = std::env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join("configuration");

    // Detect the running environment.
    // Default to `dev` if unspecified.
    let environment: Environment = std::env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| DEV_ENV_NAME.into())
        .try_into()
        .expect("Failed to parse APP_ENVIRONMENT.");

    let environment_filename = format!("{}.yaml", environment.as_str());
    let settings = config::Config::builder()
        .add_source(config::File::from(
            configuration_directory.join("base.yaml"),
        ))
        .add_source(config::File::from(
            configuration_directory.join(environment_filename),
        ))
        // Add in settings from environment variables (with a prefix of APP and '__' as separator)
        // E.g. `APP_SINK__BIGQUERY__PROJECT_ID=my-project-id would set `Settings { sink: BigQuery { project_id }}` to my-project-id
        .add_source(
            config::Environment::with_prefix("APP")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    settings.try_deserialize::<Settings>()
}

const DEV_ENV_NAME: &str = "dev";
const PROD_ENV_NAME: &str = "prod";

/// The possible runtime environment for our application.
pub enum Environment {
    Dev,
    Prod,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Dev => DEV_ENV_NAME,
            Environment::Prod => PROD_ENV_NAME,
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = String;

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

#[cfg(test)]
mod tests {
    use crate::{configuration::Settings, BatchSettings, SinkSettings, SourceSettings};

    #[test]
    pub fn deserialize_settings_test() {
        let settings = r#"{
            "source": {
                "Postgres": {
                    "host": "localhost",
                    "port": 5432,
                    "name": "postgres",
                    "username": "postgres",
                    "password": "postgres",
                    "slot_name": "replicator_slot",
                    "publication": "replicator_publication"
                }
            },
            "sink": {
                "BigQuery": {
                    "project_id": "project-id",
                    "dataset_id": "dataset-id",
                    "service_account_key": "key"
                }
            },
            "batch": {
                "max_size": 1000,
                "max_fill_secs": 10
            }
        }"#;
        let actual = serde_json::from_str::<Settings>(settings);
        let expected = Settings {
            source: SourceSettings::Postgres {
                host: "localhost".to_string(),
                port: 5432,
                name: "postgres".to_string(),
                username: "postgres".to_string(),
                password: Some("postgres".to_string()),
                slot_name: "replicator_slot".to_string(),
                publication: "replicator_publication".to_string(),
            },
            sink: SinkSettings::BigQuery {
                project_id: "project-id".to_string(),
                dataset_id: "dataset-id".to_string(),
                service_account_key: "key".to_string(),
            },
            batch: BatchSettings {
                max_size: 1000,
                max_fill_secs: 10,
            },
        };
        assert!(actual.is_ok());
        assert_eq!(expected, actual.unwrap());
    }

    #[test]
    pub fn serialize_settings_test() {
        let actual = Settings {
            source: SourceSettings::Postgres {
                host: "localhost".to_string(),
                port: 5432,
                name: "postgres".to_string(),
                username: "postgres".to_string(),
                password: Some("postgres".to_string()),
                slot_name: "replicator_slot".to_string(),
                publication: "replicator_publication".to_string(),
            },
            sink: SinkSettings::BigQuery {
                project_id: "project-id".to_string(),
                dataset_id: "dataset-id".to_string(),
                service_account_key: "key".to_string(),
            },
            batch: BatchSettings {
                max_size: 1000,
                max_fill_secs: 10,
            },
        };
        let expected = r#"{"source":{"Postgres":{"host":"localhost","port":5432,"name":"postgres","username":"postgres","password":"postgres","slot_name":"replicator_slot","publication":"replicator_publication"}},"sink":{"BigQuery":{"project_id":"project-id","dataset_id":"dataset-id","service_account_key":"key"}},"batch":{"max_size":1000,"max_fill_secs":10}}"#;
        let actual = serde_json::to_string(&actual);
        assert!(actual.is_ok());
        assert_eq!(expected, actual.unwrap());
    }
}

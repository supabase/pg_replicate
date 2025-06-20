use serde::{Deserialize, Serialize};
use std::fmt;

/// Configuration options for supported data destinations.
///
/// This enum is used to specify the destination type and its configuration
/// for the replicator. Variants correspond to different supported destinations.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationConfig {
    /// In-memory destination for ephemeral or test data.
    Memory,
    /// Google BigQuery destination configuration.
    ///
    /// Use this variant to configure a BigQuery destination, including
    /// project and dataset identifiers, service account credentials, and
    /// optional staleness settings.
    BigQuery {
        /// Google Cloud project identifier.
        project_id: String,
        /// BigQuery dataset identifier.
        dataset_id: String,
        /// Service account key for authenticating with BigQuery.
        service_account_key: String,
        /// Maximum staleness in minutes for BigQuery CDC reads.
        ///
        /// If not set, the default staleness behavior is used. See
        /// <https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness>.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
    },
}

impl fmt::Debug for DestinationConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("Memory"),
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key: _,
                max_staleness_mins,
            } => f
                .debug_struct("BigQuery")
                .field("project_id", project_id)
                .field("dataset_id", dataset_id)
                .field("service_account_key", &"REDACTED")
                .field("max_staleness_mins", max_staleness_mins)
                .finish(),
        }
    }
}

impl Default for DestinationConfig {
    fn default() -> Self {
        Self::Memory
    }
}

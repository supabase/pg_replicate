use serde::{Deserialize, Serialize};
use std::fmt;

/// Configurations options for the state store.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateStoreConfig {
    /// The in-memory state store.
    Memory,
}

impl Default for StateStoreConfig {
    fn default() -> Self {
        Self::Memory
    }
}

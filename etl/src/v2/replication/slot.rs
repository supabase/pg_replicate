use postgres::schema::Oid;
use thiserror::Error;

use crate::v2::pipeline::PipelineIdentity;

/// Maximum length for a PostgreSQL replication slot name in bytes.
const MAX_SLOT_NAME_LENGTH: usize = 63;

/// Prefixes for different types of replication slots
const APPLY_WORKER_PREFIX: &str = "supabase_etl_apply";
const TABLE_SYNC_PREFIX: &str = "supabase_etl_table_sync";

/// Error types that can occur when working with replication slots
#[derive(Debug, Error)]
pub enum SlotError {
    #[error("Slot name exceeds maximum length of {MAX_SLOT_NAME_LENGTH} characters")]
    NameTooLong,
}

/// Represents the different types of replication slot usage
#[derive(Debug)]
pub enum SlotUsage {
    /// Slot used by the apply worker for general replication
    ApplyWorker,
    /// Slot used by the table sync worker for specific table replication
    TableSyncWorker { table_id: Oid },
}

/// Generates a replication slot name based on the pipeline identity and usage type.
pub fn get_slot_name(identity: PipelineIdentity, usage: SlotUsage) -> Result<String, SlotError> {
    let slot_name = match usage {
        SlotUsage::ApplyWorker => {
            format!(
                "{}_{}_{}",
                APPLY_WORKER_PREFIX,
                identity.id(),
                identity.publication_name()
            )
        }
        SlotUsage::TableSyncWorker { table_id } => {
            format!(
                "{}_{}_{}_{}",
                TABLE_SYNC_PREFIX,
                identity.id(),
                identity.publication_name(),
                table_id
            )
        }
    };

    if slot_name.len() > MAX_SLOT_NAME_LENGTH {
        return Err(SlotError::NameTooLong);
    }

    Ok(slot_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_worker_slot_name() {
        let identity = PipelineIdentity::new(1, "test_pub");
        let result = get_slot_name(identity, SlotUsage::ApplyWorker).unwrap();
        assert!(result.starts_with(APPLY_WORKER_PREFIX));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
    }

    #[test]
    fn test_table_sync_slot_name() {
        let identity = PipelineIdentity::new(1, "test_pub");
        let result = get_slot_name(identity, SlotUsage::TableSyncWorker { table_id: 123 }).unwrap();
        assert!(result.starts_with(TABLE_SYNC_PREFIX));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
    }
}

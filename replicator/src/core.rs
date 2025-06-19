use tracing::instrument;
use crate::config::ReplicatorConfig;

#[instrument(name = "replication")]
async fn start_replicator() -> anyhow::Result<()> {
    let config = ReplicatorConfig::load()?;
    
    
    
    Ok(())
}
//! engine.rs — `SyncEngine`: top-level coordinator and lifecycle manager for claw-sync.

use crate::{config::SyncConfig, error::SyncResult};

/// Top-level coordinator that starts and manages all sync subsystems.
pub struct SyncEngine {
    config: SyncConfig,
}

impl SyncEngine {
    /// Create a new `SyncEngine` from the supplied configuration.
    pub async fn new(config: SyncConfig) -> SyncResult<Self> {
        Ok(Self { config })
    }

    /// Run the engine until shutdown is signalled.
    pub async fn run(&self) -> SyncResult<()> {
        tracing::info!(workspace_id = %self.config.workspace_id, "SyncEngine starting");
        Ok(())
    }
}

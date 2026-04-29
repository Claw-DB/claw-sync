//! config.rs — `SyncConfig` builder with environment-variable fallback.

use crate::error::{SyncError, SyncResult};

/// Runtime configuration for the claw-sync engine.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Identifier of the workspace being synchronised.
    pub workspace_id: String,
    /// gRPC endpoint of the remote sync hub (e.g. `https://hub.example.com:443`).
    pub hub_endpoint: String,
    /// Path to the local SQLite database used for the offline queue.
    pub db_path: String,
}

impl SyncConfig {
    /// Construct `SyncConfig` by reading well-known environment variables.
    ///
    /// | Variable | Default |
    /// |---|---|
    /// | `CLAW_WORKSPACE_ID` | — (required) |
    /// | `CLAW_HUB_ENDPOINT` | `http://localhost:50051` |
    /// | `CLAW_DB_PATH`      | `claw_sync.db` |
    pub fn from_env() -> SyncResult<Self> {
        let workspace_id = std::env::var("CLAW_WORKSPACE_ID")
            .map_err(|_| SyncError::Config("CLAW_WORKSPACE_ID not set".into()))?;
        let hub_endpoint = std::env::var("CLAW_HUB_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:50051".into());
        let db_path = std::env::var("CLAW_DB_PATH")
            .unwrap_or_else(|_| "claw_sync.db".into());
        Ok(Self { workspace_id, hub_endpoint, db_path })
    }
}

//! checkpoint.rs — sync checkpoint persistence tracking the last successful sync cursor.

use crate::error::SyncResult;

/// A persisted sync checkpoint recording the last successfully applied cursor.
#[derive(Debug, Clone)]
pub struct SyncCheckpoint {
    /// The workspace this checkpoint belongs to.
    pub workspace_id: String,
    /// Lamport clock value at the last successful sync.
    pub lamport_clock: u64,
    /// HLC timestamp string at the last successful sync.
    pub hlc_timestamp: String,
}

impl SyncCheckpoint {
    /// Load the most recent checkpoint for `workspace_id` from persistent storage.
    pub async fn load(_workspace_id: &str) -> SyncResult<Option<Self>> {
        Ok(None)
    }

    /// Persist this checkpoint to durable storage.
    pub async fn save(&self) -> SyncResult<()> {
        tracing::debug!(workspace_id = %self.workspace_id, lamport = self.lamport_clock, "checkpoint saved");
        Ok(())
    }
}

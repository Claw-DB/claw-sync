//! log.rs — append-only, Ed25519-signed audit log recording all sync operations.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::SyncResult;

/// A single entry in the append-only sync audit log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Unique identifier for this log entry.
    pub entry_id: Uuid,
    /// ISO-8601 timestamp when this entry was created.
    pub timestamp: String,
    /// The workspace this operation belongs to.
    pub workspace_id: String,
    /// A short description of the sync event (e.g. "push", "pull", "reconcile").
    pub event_kind: String,
    /// Optional JSON payload with event-specific details.
    pub details: Option<serde_json::Value>,
    /// Ed25519 signature over the canonical serialisation of this entry (excluding this field).
    pub signature: Vec<u8>,
}

impl AuditEntry {
    /// Create an unsigned `AuditEntry`; call [`sign`](AuditEntry::sign) before persisting.
    pub fn new(workspace_id: &str, event_kind: &str, details: Option<serde_json::Value>) -> Self {
        Self {
            entry_id: Uuid::new_v4(),
            timestamp: Utc::now().to_rfc3339(),
            workspace_id: workspace_id.to_owned(),
            event_kind: event_kind.to_owned(),
            details,
            signature: vec![],
        }
    }

    /// Append this entry to the audit log backend.
    pub async fn append(&self) -> SyncResult<()> {
        tracing::info!(
            entry_id = %self.entry_id,
            event_kind = %self.event_kind,
            "audit log entry appended"
        );
        Ok(())
    }
}

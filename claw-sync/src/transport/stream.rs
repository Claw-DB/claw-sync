//! stream.rs — bidirectional streaming sync session over the gRPC transport.

use crate::error::SyncResult;

/// A handle to an active bidirectional sync streaming session with the hub.
pub struct SyncStream {
    /// Opaque session identifier assigned by the hub.
    pub session_id: String,
}

impl SyncStream {
    /// Open a new bidirectional sync streaming session.
    pub async fn open(session_id: impl Into<String>) -> SyncResult<Self> {
        Ok(Self { session_id: session_id.into() })
    }
}

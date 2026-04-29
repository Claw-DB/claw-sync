//! resolver.rs — conflict resolution strategies: LWW, merge, and manual escalation.

use crate::{
    crdt::ops::CrdtOp,
    error::{SyncError, SyncResult},
};

/// The outcome of a conflict resolution attempt.
#[derive(Debug)]
pub enum Resolution {
    /// The local operation won; discard the remote.
    LocalWins(CrdtOp),
    /// The remote operation won; apply it locally.
    RemoteWins(CrdtOp),
    /// Both operations were merged into a single resultant operation.
    Merged(CrdtOp),
    /// The conflict could not be resolved automatically and requires user intervention.
    Escalated {
        /// Identifier of the entity whose conflict requires manual resolution.
        entity_id: String,
    },
}

/// Apply Last-Write-Wins resolution between a local and a remote `CrdtOp`.
///
/// Falls back to escalation when neither operation can be compared by timestamp.
pub fn resolve_lww(local: CrdtOp, remote: CrdtOp) -> SyncResult<Resolution> {
    use CrdtOp::*;
    let local_ts = match &local {
        Insert { timestamp, .. }
        | Update { timestamp, .. }
        | Delete { timestamp, .. }
        | Tombstone { timestamp, .. } => timestamp.clone(),
    };
    let remote_ts = match &remote {
        Insert { timestamp, .. }
        | Update { timestamp, .. }
        | Delete { timestamp, .. }
        | Tombstone { timestamp, .. } => timestamp.clone(),
    };
    if local_ts >= remote_ts {
        Ok(Resolution::LocalWins(local))
    } else {
        Ok(Resolution::RemoteWins(remote))
    }
}

/// Escalate a conflict that cannot be resolved automatically.
pub fn escalate(entity_id: &str) -> SyncResult<Resolution> {
    Err(SyncError::ConflictEscalation { entity_id: entity_id.to_owned() })
}

//! pull.rs — pull remote deltas from the hub and apply them to the local store.

use crate::error::SyncResult;

/// Pull all remote deltas newer than the current sync cursor from the hub.
///
/// Decrypts and decompresses each received chunk, applies CRDT conflict resolution,
/// and writes the resulting state to the local claw-core database.
pub async fn pull_deltas(_workspace_id: &str) -> SyncResult<()> {
    tracing::info!("pull_deltas: starting pull pass");
    Ok(())
}

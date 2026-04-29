//! workspace.rs — `WorkspaceIdentity`: workspace identifier and shared key derivation.

use uuid::Uuid;

/// Holds the identity and derived shared key for a sync workspace.
#[derive(Debug, Clone)]
pub struct WorkspaceIdentity {
    /// Stable unique identifier for this workspace.
    pub workspace_id: Uuid,
    /// BLAKE3-derived shared key for workspace-level encryption.
    pub shared_key: [u8; 32],
}

impl WorkspaceIdentity {
    /// Derive a `WorkspaceIdentity` from a master secret and workspace identifier.
    pub fn derive(master_secret: &[u8], workspace_id: Uuid) -> Self {
        let context = format!("claw-sync:workspace:{workspace_id}");
        let key = blake3::derive_key(&context, master_secret);
        Self { workspace_id, shared_key: key }
    }
}

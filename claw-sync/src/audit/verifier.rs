//! verifier.rs — verifies the Ed25519 signature chain integrity of the audit log.

use crate::{audit::log::AuditEntry, error::SyncResult};

/// Verify the Ed25519 signature on a single [`AuditEntry`].
///
/// Returns `Ok(true)` if the signature is valid, `Ok(false)` if it is invalid,
/// or `Err(_)` if the verification process itself fails.
pub fn verify_entry(entry: &AuditEntry, public_key_bytes: &[u8]) -> SyncResult<bool> {
    let canonical = serde_json::to_vec(&AuditEntry {
        signature: vec![],
        ..entry.clone()
    })?;
    crate::crypto::signing::verify_detached(&canonical, &entry.signature, public_key_bytes)
}

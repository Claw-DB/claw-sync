//! verifier.rs — audit chain verification for the SQLite-backed sync audit log.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use sqlx::SqlitePool;

use crate::{
    audit::log::{row_hash_hex, signature_message, AuditEntry},
    crypto::keys::KeyStore,
    error::{SyncError, SyncResult},
};

/// Aggregate result of verifying the audit chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyResult {
    /// `true` when the full chain verified successfully.
    pub ok: bool,
    /// Number of entries checked.
    pub checked: u64,
    /// Sequence number of the first broken entry, when any.
    pub first_broken_at: Option<u64>,
    /// Human-readable reason for the first verification failure.
    pub broken_reason: Option<String>,
}

/// Verifies the full append-only audit chain in sequence order.
pub async fn verify_chain(pool: &SqlitePool, key_store: &KeyStore) -> SyncResult<VerifyResult> {
    let entries = sqlx::query(
        "SELECT sequence, event_type, workspace_id, device_id, entity_type, entity_ids,
                ops_count, occurred_at, payload_hash, prev_entry_hash, signature, signer_pub_key
         FROM sync_audit_log
         ORDER BY sequence ASC",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(super::log::row_to_entry)
    .collect::<SyncResult<Vec<_>>>()?;

    let mut checked = 0_u64;
    let mut previous_hash = String::new();
    for entry in &entries {
        if let Err(error) = verify_entry(entry, &previous_hash, key_store) {
            return Ok(VerifyResult {
                ok: false,
                checked,
                first_broken_at: Some(entry.sequence),
                broken_reason: Some(error.to_string()),
            });
        }
        previous_hash = row_hash_hex(entry)?;
        checked = checked.saturating_add(1);
    }

    Ok(VerifyResult {
        ok: true,
        checked,
        first_broken_at: None,
        broken_reason: None,
    })
}

/// Verifies a single entry against the expected previous-entry hash.
pub fn verify_entry(entry: &AuditEntry, prev_hash: &str, key_store: &KeyStore) -> SyncResult<()> {
    if entry.prev_entry_hash != prev_hash {
        return Err(SyncError::Validation(format!(
            "audit prev_entry_hash mismatch at sequence {}",
            entry.sequence
        )));
    }

    let signature = STANDARD
        .decode(&entry.signature)
        .map_err(|error| SyncError::Crypto(format!("invalid audit signature encoding: {error}")))?;
    let public_key = if entry.signer_pub_key.is_empty() {
        key_store.signing_keypair().public.0.to_vec()
    } else {
        STANDARD.decode(&entry.signer_pub_key).map_err(|error| {
            SyncError::Crypto(format!("invalid audit signer public key encoding: {error}"))
        })?
    };

    let verified = crate::crypto::signing::verify_detached(
        &signature_message(
            entry.sequence,
            entry.event_type,
            &entry.occurred_at,
            &entry.payload_hash,
            &entry.prev_entry_hash,
        ),
        &signature,
        &public_key,
    )?;
    if !verified {
        return Err(SyncError::Crypto(format!(
            "audit signature verification failed at sequence {}",
            entry.sequence
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{verify_chain, VerifyResult};
    use crate::{
        audit::log::{AuditEvent, AuditEventType, AuditLog},
        crypto::keys::KeyStore,
        delta::extractor::EntityType,
    };

    #[tokio::test]
    async fn verify_chain_accepts_valid_log() {
        let dir = tempdir().expect("tempdir should exist");
        let key_store = Arc::new(
            KeyStore::load_or_create(Path::new(dir.path()), b"audit-verify-test")
                .expect("keystore should load"),
        );
        let log = AuditLog::new(dir.path(), key_store.clone())
            .await
            .expect("audit log should initialise");
        for index in 0..3 {
            log.record(AuditEvent {
                event_type: AuditEventType::Push,
                workspace_id: Uuid::from_bytes([1; 16]),
                device_id: Uuid::from_bytes([2; 16]),
                entity_type: Some(EntityType::MemoryRecords),
                entity_ids: vec![format!("m{index}")],
                ops_count: 1,
                occurred_at: chrono::Utc::now(),
                payload: serde_json::json!({ "index": index }),
            })
            .await
            .expect("record should succeed");
        }

        let result = verify_chain(&log.pool, &key_store)
            .await
            .expect("verification should succeed");
        assert_eq!(
            result,
            VerifyResult {
                ok: true,
                checked: 3,
                first_broken_at: None,
                broken_reason: None,
            }
        );
    }

    #[tokio::test]
    async fn verify_chain_detects_tampering() {
        let dir = tempdir().expect("tempdir should exist");
        let key_store = Arc::new(
            KeyStore::load_or_create(Path::new(dir.path()), b"audit-verify-test-2")
                .expect("keystore should load"),
        );
        let log = AuditLog::new(dir.path(), key_store.clone())
            .await
            .expect("audit log should initialise");
        log.record(AuditEvent {
            event_type: AuditEventType::Pull,
            workspace_id: Uuid::from_bytes([3; 16]),
            device_id: Uuid::from_bytes([4; 16]),
            entity_type: Some(EntityType::MemoryRecords),
            entity_ids: vec!["m1".into()],
            ops_count: 1,
            occurred_at: chrono::Utc::now(),
            payload: serde_json::json!({ "op": 1 }),
        })
        .await
        .expect("record should succeed");

        sqlx::query("UPDATE sync_audit_log SET payload_hash = 'corrupted' WHERE sequence = 1")
            .execute(&log.pool)
            .await
            .expect("tamper should succeed");

        let result = verify_chain(&log.pool, &key_store)
            .await
            .expect("verification should complete");
        assert!(!result.ok);
        assert_eq!(result.first_broken_at, Some(1));
    }
}

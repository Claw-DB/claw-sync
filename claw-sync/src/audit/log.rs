//! log.rs — append-only, cryptographically chained audit log for sync operations.

use std::{path::Path, sync::Arc};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    Row, SqlitePool,
};
use uuid::Uuid;

use crate::{
    crypto::{keys::KeyStore, signing::sign},
    delta::extractor::EntityType,
    error::{SyncError, SyncResult},
};

const AUDIT_DB_NAME: &str = "audit.db";

/// Supported audit event types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuditEventType {
    /// Outbound push completed.
    Push,
    /// Inbound pull completed.
    Pull,
    /// A CRDT conflict was automatically resolved.
    ConflictResolved,
    /// A reconciliation pass completed.
    Reconcile,
    /// The local device was registered with the hub.
    DeviceRegistered,
    /// Local signing or encryption keys were rotated.
    KeyRotated,
}

impl AuditEventType {
    /// Returns the canonical database string for the event type.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Push => "PUSH",
            Self::Pull => "PULL",
            Self::ConflictResolved => "CONFLICT_RESOLVED",
            Self::Reconcile => "RECONCILE",
            Self::DeviceRegistered => "DEVICE_REGISTERED",
            Self::KeyRotated => "KEY_ROTATED",
        }
    }
}

impl std::fmt::Display for AuditEventType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for AuditEventType {
    type Err = SyncError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PUSH" => Ok(Self::Push),
            "PULL" => Ok(Self::Pull),
            "CONFLICT_RESOLVED" => Ok(Self::ConflictResolved),
            "RECONCILE" => Ok(Self::Reconcile),
            "DEVICE_REGISTERED" => Ok(Self::DeviceRegistered),
            "KEY_ROTATED" => Ok(Self::KeyRotated),
            other => Err(SyncError::Validation(format!(
                "unsupported audit event type: {other}"
            ))),
        }
    }
}

/// High-level event payload used to create a persisted audit entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Event category.
    pub event_type: AuditEventType,
    /// Workspace associated with the event.
    pub workspace_id: Uuid,
    /// Device associated with the event.
    pub device_id: Uuid,
    /// Optional affected entity type.
    pub entity_type: Option<EntityType>,
    /// Optional affected entity identifiers.
    pub entity_ids: Vec<String>,
    /// Number of operations represented by the event.
    pub ops_count: u32,
    /// UTC time at which the event occurred.
    pub occurred_at: DateTime<Utc>,
    /// Structured event payload hashed into the row.
    pub payload: serde_json::Value,
}

/// A single persisted entry in the append-only audit log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Monotonic sequence number for the entry.
    pub sequence: u64,
    /// Event type.
    pub event_type: AuditEventType,
    /// Workspace identifier.
    pub workspace_id: Uuid,
    /// Device identifier.
    pub device_id: Uuid,
    /// Optional entity type.
    pub entity_type: Option<String>,
    /// Optional entity ids encoded as a JSON array.
    pub entity_ids: Vec<String>,
    /// Number of operations represented by this entry.
    pub ops_count: u32,
    /// RFC3339 timestamp for the event.
    pub occurred_at: String,
    /// BLAKE3 hex digest of the event payload.
    pub payload_hash: String,
    /// BLAKE3 hex digest of the previous entry's canonical row representation.
    pub prev_entry_hash: String,
    /// Base64 Ed25519 detached signature over the signed fields.
    pub signature: String,
    /// Base64 Ed25519 signer public key.
    pub signer_pub_key: String,
}

/// SQLite-backed append-only audit log.
pub struct AuditLog {
    /// SQLite connection pool for the audit database.
    pub pool: SqlitePool,
    /// Signing material for new audit entries.
    pub key_store: Arc<KeyStore>,
}

impl AuditLog {
    /// Opens or creates the audit database under `data_dir/audit.db`.
    pub async fn new(data_dir: &Path, key_store: Arc<KeyStore>) -> SyncResult<Self> {
        tokio::fs::create_dir_all(data_dir).await?;

        let options = SqliteConnectOptions::new()
            .filename(data_dir.join(AUDIT_DB_NAME))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS sync_audit_log (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                entity_type TEXT,
                entity_ids TEXT,
                ops_count INTEGER NOT NULL DEFAULT 0,
                occurred_at TEXT NOT NULL,
                payload_hash TEXT,
                prev_entry_hash TEXT,
                signature TEXT,
                signer_pub_key TEXT
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_sync_audit_log_occurred_at
             ON sync_audit_log (occurred_at DESC)",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool, key_store })
    }

    /// Records an audit event and returns the assigned sequence number.
    pub async fn record(&self, event: AuditEvent) -> SyncResult<u64> {
        let mut transaction = self.pool.begin().await?;
        let previous = fetch_last_entry(&mut transaction).await?;
        let next_sequence = previous
            .as_ref()
            .map(|entry| entry.sequence.saturating_add(1))
            .unwrap_or(1);
        let prev_entry_hash = previous
            .as_ref()
            .map(row_hash_hex)
            .transpose()?
            .unwrap_or_default();
        let payload_hash = blake3::hash(&serde_json::to_vec(&event.payload)?)
            .to_hex()
            .to_string();
        let occurred_at = event.occurred_at.to_rfc3339();
        let signature_bytes = sign(
            &signature_message(
                next_sequence,
                event.event_type,
                &occurred_at,
                &payload_hash,
                &prev_entry_hash,
            ),
            self.key_store.signing_keypair(),
        );
        let signer_pub_key = STANDARD.encode(self.key_store.signing_keypair().public.0);

        sqlx::query(
            "INSERT INTO sync_audit_log (
                sequence, event_type, workspace_id, device_id, entity_type,
                entity_ids, ops_count, occurred_at, payload_hash,
                prev_entry_hash, signature, signer_pub_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(next_sequence as i64)
        .bind(event.event_type.as_str())
        .bind(event.workspace_id.to_string())
        .bind(event.device_id.to_string())
        .bind(event.entity_type.map(|entity_type| entity_type.to_string()))
        .bind(serde_json::to_string(&event.entity_ids)?)
        .bind(i64::from(event.ops_count))
        .bind(&occurred_at)
        .bind(&payload_hash)
        .bind(&prev_entry_hash)
        .bind(STANDARD.encode(signature_bytes))
        .bind(signer_pub_key)
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;
        Ok(next_sequence)
    }

    /// Returns the newest `limit` entries in sequence order.
    pub async fn tail(&self, limit: u32) -> SyncResult<Vec<AuditEntry>> {
        let rows = sqlx::query(
            "SELECT sequence, event_type, workspace_id, device_id, entity_type, entity_ids,
                    ops_count, occurred_at, payload_hash, prev_entry_hash, signature, signer_pub_key
             FROM sync_audit_log
             ORDER BY sequence DESC
             LIMIT ?",
        )
        .bind(i64::from(limit.max(1)))
        .fetch_all(&self.pool)
        .await?;

        let mut entries = rows
            .into_iter()
            .map(row_to_entry)
            .collect::<SyncResult<Vec<_>>>()?;
        entries.reverse();
        Ok(entries)
    }

    /// Returns entries strictly newer than `sequence` in sequence order.
    pub async fn since(&self, sequence: u64) -> SyncResult<Vec<AuditEntry>> {
        let rows = sqlx::query(
            "SELECT sequence, event_type, workspace_id, device_id, entity_type, entity_ids,
                    ops_count, occurred_at, payload_hash, prev_entry_hash, signature, signer_pub_key
             FROM sync_audit_log
             WHERE sequence > ?
             ORDER BY sequence ASC",
        )
        .bind(sequence as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(row_to_entry).collect()
    }
}

pub(crate) fn row_hash_hex(entry: &AuditEntry) -> SyncResult<String> {
    let material = serde_json::to_vec(entry)?;
    Ok(blake3::hash(&material).to_hex().to_string())
}

pub(crate) fn signature_message(
    sequence: u64,
    event_type: AuditEventType,
    occurred_at: &str,
    payload_hash: &str,
    prev_entry_hash: &str,
) -> Vec<u8> {
    let mut message = Vec::with_capacity(
        8 + event_type.as_str().len()
            + occurred_at.len()
            + payload_hash.len()
            + prev_entry_hash.len()
            + 4,
    );
    message.extend_from_slice(&sequence.to_be_bytes());
    message.push(0x1f);
    message.extend_from_slice(event_type.as_str().as_bytes());
    message.push(0x1f);
    message.extend_from_slice(occurred_at.as_bytes());
    message.push(0x1f);
    message.extend_from_slice(payload_hash.as_bytes());
    message.push(0x1f);
    message.extend_from_slice(prev_entry_hash.as_bytes());
    message
}

async fn fetch_last_entry(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> SyncResult<Option<AuditEntry>> {
    let row = sqlx::query(
        "SELECT sequence, event_type, workspace_id, device_id, entity_type, entity_ids,
                ops_count, occurred_at, payload_hash, prev_entry_hash, signature, signer_pub_key
         FROM sync_audit_log
         ORDER BY sequence DESC
         LIMIT 1",
    )
    .fetch_optional(&mut **transaction)
    .await?;

    row.map(row_to_entry).transpose()
}

pub(crate) fn row_to_entry(row: sqlx::sqlite::SqliteRow) -> SyncResult<AuditEntry> {
    Ok(AuditEntry {
        sequence: row.try_get::<i64, _>("sequence")?.max(0) as u64,
        event_type: row.try_get::<String, _>("event_type")?.parse()?,
        workspace_id: Uuid::parse_str(&row.try_get::<String, _>("workspace_id")?).map_err(
            |error| SyncError::Validation(format!("invalid audit workspace id: {error}")),
        )?,
        device_id: Uuid::parse_str(&row.try_get::<String, _>("device_id")?)
            .map_err(|error| SyncError::Validation(format!("invalid audit device id: {error}")))?,
        entity_type: row.try_get("entity_type")?,
        entity_ids: serde_json::from_str(&row.try_get::<String, _>("entity_ids")?)?,
        ops_count: row.try_get::<i64, _>("ops_count")?.max(0) as u32,
        occurred_at: row.try_get("occurred_at")?,
        payload_hash: row
            .try_get::<Option<String>, _>("payload_hash")?
            .unwrap_or_default(),
        prev_entry_hash: row
            .try_get::<Option<String>, _>("prev_entry_hash")?
            .unwrap_or_default(),
        signature: row
            .try_get::<Option<String>, _>("signature")?
            .unwrap_or_default(),
        signer_pub_key: row
            .try_get::<Option<String>, _>("signer_pub_key")?
            .unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use chrono::Utc;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{row_hash_hex, AuditEvent, AuditEventType, AuditLog};
    use crate::{crypto::keys::KeyStore, delta::extractor::EntityType};

    #[tokio::test]
    async fn record_persists_signed_entries_with_chain_links() {
        let dir = tempdir().expect("tempdir should exist");
        let key_store = Arc::new(
            KeyStore::load_or_create(Path::new(dir.path()), b"audit-test-material")
                .expect("keystore should load"),
        );
        let log = AuditLog::new(dir.path(), key_store)
            .await
            .expect("audit log should initialise");

        let first_sequence = log
            .record(AuditEvent {
                event_type: AuditEventType::Push,
                workspace_id: Uuid::from_bytes([1; 16]),
                device_id: Uuid::from_bytes([2; 16]),
                entity_type: Some(EntityType::MemoryRecords),
                entity_ids: vec!["m1".into()],
                ops_count: 1,
                occurred_at: Utc::now(),
                payload: serde_json::json!({ "chunk_count": 1 }),
            })
            .await
            .expect("first record should succeed");
        let second_sequence = log
            .record(AuditEvent {
                event_type: AuditEventType::Pull,
                workspace_id: Uuid::from_bytes([1; 16]),
                device_id: Uuid::from_bytes([2; 16]),
                entity_type: Some(EntityType::MemoryRecords),
                entity_ids: vec!["m1".into(), "m2".into()],
                ops_count: 2,
                occurred_at: Utc::now(),
                payload: serde_json::json!({ "payloads": 1 }),
            })
            .await
            .expect("second record should succeed");

        assert_eq!(first_sequence, 1);
        assert_eq!(second_sequence, 2);

        let tail = log.tail(10).await.expect("tail should succeed");
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].sequence, 1);
        assert_eq!(
            tail[1].prev_entry_hash,
            row_hash_hex(&tail[0]).expect("hash should compute")
        );
        assert!(!tail[1].signature.is_empty());
    }

    #[tokio::test]
    async fn since_returns_entries_after_sequence() {
        let dir = tempdir().expect("tempdir should exist");
        let key_store = Arc::new(
            KeyStore::load_or_create(Path::new(dir.path()), b"audit-test-material-2")
                .expect("keystore should load"),
        );
        let log = AuditLog::new(dir.path(), key_store)
            .await
            .expect("audit log should initialise");

        for index in 0..3 {
            log.record(AuditEvent {
                event_type: AuditEventType::Reconcile,
                workspace_id: Uuid::from_bytes([4; 16]),
                device_id: Uuid::from_bytes([5; 16]),
                entity_type: None,
                entity_ids: vec![format!("entity-{index}")],
                ops_count: 1,
                occurred_at: Utc::now(),
                payload: serde_json::json!({ "iteration": index }),
            })
            .await
            .expect("record should succeed");
        }

        let entries = log.since(1).await.expect("since should succeed");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 2);
        assert_eq!(entries[1].sequence, 3);
    }
}

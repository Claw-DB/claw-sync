//! apply.rs — applies inbound delta sets to the local claw-core SQLite database.

use std::{collections::BTreeSet, str::FromStr, sync::Arc};

use serde_json::{Map, Value};
use sqlx::{FromRow, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::{
    audit::log::{AuditEvent, AuditEventType, AuditLog},
    crdt::{
        clock::HlcTimestamp,
        ops::{CrdtOp, FieldPatch, OpKind},
        resolver::{ConflictResolver, ResolvedOp},
    },
    delta::extractor::{DeltaSet, EntityType},
    error::{SyncError, SyncResult},
    proto::clawsync::v1::SyncCursor,
    sync::checkpoint::CheckpointStore,
};

/// Aggregate outcome of applying a delta set.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ApplyResult {
    /// Number of inserted entities.
    pub inserted: u32,
    /// Number of updated entities.
    pub updated: u32,
    /// Number of deleted entities.
    pub deleted: u32,
    /// Number of incoming operations skipped because local state was newer or identical.
    pub skipped: u32,
    /// Number of conflicts resolved automatically.
    pub conflicts_resolved: u32,
}

/// Applies inbound delta sets to the local database.
#[derive(Clone)]
pub struct DeltaApplier {
    pool: SqlitePool,
    resolver: Arc<ConflictResolver>,
    audit: Arc<AuditLog>,
    checkpoint: Arc<CheckpointStore>,
    workspace_id: Uuid,
    device_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationOutcome {
    Inserted,
    Updated,
    Skipped,
}

#[derive(Debug, FromRow)]
struct TableColumn {
    name: String,
    pk: i64,
}

#[derive(Debug, FromRow)]
struct EntityVersionRow {
    hlc: String,
    device_id: String,
    is_deleted: i64,
}

#[derive(Debug, FromRow)]
struct FieldVersionRow {
    hlc: String,
    device_id: String,
}

#[derive(Debug, FromRow)]
struct ChangeLogRow {
    operation: String,
    payload: String,
    device_id: String,
    hlc: String,
}

impl DeltaApplier {
    /// Creates a new delta applier.
    pub fn new(
        pool: SqlitePool,
        resolver: Arc<ConflictResolver>,
        audit: Arc<AuditLog>,
        checkpoint: Arc<CheckpointStore>,
        workspace_id: Uuid,
        device_id: Uuid,
    ) -> Self {
        Self {
            pool,
            resolver,
            audit,
            checkpoint,
            workspace_id,
            device_id,
        }
    }

    /// Returns the underlying SQLite pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Applies a delta set inside a single SQL transaction.
    pub async fn apply_delta_set(&self, delta: DeltaSet) -> SyncResult<ApplyResult> {
        ensure_sync_metadata_tables(&self.pool).await?;
        let checkpoint = self
            .checkpoint
            .load(self.workspace_id, self.device_id)
            .await?;

        let mut transaction = self.pool.begin().await?;
        ensure_deleted_at_column(&mut transaction, delta.entity_type.as_str()).await?;
        set_runtime_context(&mut transaction, delta.device_id, true).await?;

        let mut result = ApplyResult::default();
        let mut audit_events = Vec::new();

        for op in &delta.ops {
            let resolved = if let Some(cursor) = checkpoint.as_ref() {
                if let Some(local_change) =
                    detect_local_changes_in_transaction(&mut transaction, op.entity_id(), cursor)
                        .await?
                {
                    Some(self.resolver.resolve(&local_change, op)?)
                } else {
                    None
                }
            } else {
                None
            };

            let op_to_apply = match resolved {
                Some(ResolvedOp::UseLocal) => {
                    result.skipped += 1;
                    continue;
                }
                Some(ResolvedOp::UseRemote) | None => op.clone(),
                Some(ResolvedOp::Merge(merged)) => {
                    result.conflicts_resolved += 1;
                    audit_events.push(AuditEvent {
                        event_type: AuditEventType::ConflictResolved,
                        workspace_id: self.workspace_id,
                        device_id: self.device_id,
                        entity_type: Some(delta.entity_type),
                        entity_ids: vec![op.entity_id().to_owned()],
                        ops_count: 1,
                        occurred_at: chrono::Utc::now(),
                        payload: serde_json::json!({
                            "entity_id": op.entity_id(),
                            "local_hlc": op.hlc().to_string(),
                            "resolved_with": "merge",
                        }),
                    });
                    merged_value_to_op(op, merged)
                }
                Some(ResolvedOp::Escalate(record)) => {
                    let mut record = record;
                    let conflict_id = uuid::Uuid::new_v4().to_string();
                    record.entity_type = delta.entity_type.to_string();
                    record.conflict_id = conflict_id.clone();
                    persist_conflict_record(
                        &mut transaction,
                        &conflict_id,
                        self.workspace_id,
                        delta.entity_type,
                        op.entity_id(),
                        &record,
                    )
                    .await?;
                    reset_runtime_context(&mut transaction).await?;
                    transaction.commit().await?;
                    return Err(SyncError::ConflictEscalation {
                        entity_id: record.entity_id.clone(),
                        record: Box::new(record),
                    });
                }
            };

            let applied = match &op_to_apply {
                CrdtOp::Insert { .. } => {
                    match apply_insert(&mut transaction, delta.entity_type, &op_to_apply).await? {
                        OperationOutcome::Inserted => {
                            result.inserted += 1;
                            true
                        }
                        OperationOutcome::Updated => {
                            result.updated += 1;
                            true
                        }
                        OperationOutcome::Skipped => {
                            result.skipped += 1;
                            false
                        }
                    }
                }
                CrdtOp::Update { .. } => {
                    if apply_update(&mut transaction, delta.entity_type, &op_to_apply).await? {
                        result.updated += 1;
                        true
                    } else {
                        result.skipped += 1;
                        false
                    }
                }
                CrdtOp::Delete { .. } | CrdtOp::Tombstone { .. } => {
                    if apply_delete_like(&mut transaction, delta.entity_type, &op_to_apply).await? {
                        result.deleted += 1;
                        true
                    } else {
                        result.skipped += 1;
                        false
                    }
                }
            };

            if applied {
                let _ = &op_to_apply;
            }
        }

        reset_runtime_context(&mut transaction).await?;
        transaction.commit().await?;

        for event in audit_events {
            self.audit.record(event).await?;
        }

        Ok(result)
    }
}

/// Detects unsynced local changes for `entity_id` newer than `since_cursor`.
pub async fn detect_local_changes(
    pool: &SqlitePool,
    entity_id: &str,
    since_cursor: &SyncCursor,
) -> SyncResult<Option<CrdtOp>> {
    let mut transaction = pool.begin().await?;
    let result =
        detect_local_changes_in_transaction(&mut transaction, entity_id, since_cursor).await?;
    transaction.rollback().await?;
    Ok(result)
}

async fn detect_local_changes_in_transaction(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_id: &str,
    since_cursor: &SyncCursor,
) -> SyncResult<Option<CrdtOp>> {
    let row = sqlx::query_as::<_, ChangeLogRow>(
        "SELECT operation, payload, device_id, hlc
         FROM sync_changelog
         WHERE entity_id = ? AND synced = 0 AND sequence > ?
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(entity_id)
    .bind(since_cursor.lamport_clock)
    .fetch_optional(&mut **transaction)
    .await?;

    row.map(|row| change_log_row_to_crdt(entity_id, row))
        .transpose()
}

async fn apply_insert(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    op: &CrdtOp,
) -> SyncResult<OperationOutcome> {
    let (entity_id, payload, remote_hlc, remote_device_id) = match op {
        CrdtOp::Insert {
            entity_id,
            payload,
            hlc,
            device_id,
        } => (entity_id.as_str(), payload.clone(), hlc.clone(), *device_id),
        _ => {
            return Err(SyncError::Validation(
                "apply_insert received a non-insert op".into(),
            ))
        }
    };

    if let Some((local_hlc, local_device, _)) =
        load_entity_version(transaction, entity_type, entity_id).await?
    {
        if is_remote_newer(&local_hlc, local_device, &remote_hlc, remote_device_id) {
            upsert_row(transaction, entity_type, entity_id, &payload, true).await?;
            save_entity_version(
                transaction,
                entity_type,
                entity_id,
                &remote_hlc,
                remote_device_id,
                false,
            )
            .await?;
            refresh_field_versions(
                transaction,
                entity_type,
                entity_id,
                &payload,
                &remote_hlc,
                remote_device_id,
            )
            .await?;
            Ok(OperationOutcome::Updated)
        } else {
            Ok(OperationOutcome::Skipped)
        }
    } else {
        upsert_row(transaction, entity_type, entity_id, &payload, true).await?;
        save_entity_version(
            transaction,
            entity_type,
            entity_id,
            &remote_hlc,
            remote_device_id,
            false,
        )
        .await?;
        refresh_field_versions(
            transaction,
            entity_type,
            entity_id,
            &payload,
            &remote_hlc,
            remote_device_id,
        )
        .await?;
        Ok(OperationOutcome::Inserted)
    }
}

async fn apply_update(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    op: &CrdtOp,
) -> SyncResult<bool> {
    let (entity_id, field_patches, remote_hlc, remote_device_id) = match op {
        CrdtOp::Update {
            entity_id,
            field_patches,
            hlc,
            device_id,
        } => (entity_id.as_str(), field_patches, hlc.clone(), *device_id),
        _ => {
            return Err(SyncError::Validation(
                "apply_update received a non-update op".into(),
            ))
        }
    };

    let mut current = fetch_entity_payload(transaction, entity_type, entity_id)
        .await?
        .unwrap_or_else(|| Value::Object(Map::new()));
    let mut touched = false;

    for patch in field_patches {
        let local_version =
            load_field_version(transaction, entity_type, entity_id, &patch.field).await?;
        let can_apply = match local_version {
            Some((local_hlc, local_device_id)) => {
                is_remote_newer(&local_hlc, local_device_id, &remote_hlc, remote_device_id)
            }
            None => true,
        };

        if can_apply && apply_patch_field(&mut current, &patch.field, patch.new_value.clone()) {
            save_field_version(
                transaction,
                entity_type,
                entity_id,
                &patch.field,
                &remote_hlc,
                remote_device_id,
            )
            .await?;
            touched = true;
        }
    }

    if !touched {
        return Ok(false);
    }

    upsert_row(transaction, entity_type, entity_id, &current, true).await?;
    save_entity_version(
        transaction,
        entity_type,
        entity_id,
        &remote_hlc,
        remote_device_id,
        false,
    )
    .await?;
    Ok(true)
}

async fn apply_delete_like(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    op: &CrdtOp,
) -> SyncResult<bool> {
    let (entity_id, remote_hlc, remote_device_id) = match op {
        CrdtOp::Delete {
            entity_id,
            hlc,
            device_id,
        } => (entity_id.as_str(), hlc.clone(), *device_id),
        CrdtOp::Tombstone {
            entity_id,
            deleted_at,
        } => (entity_id.as_str(), deleted_at.clone(), deleted_at.node_id),
        _ => {
            return Err(SyncError::Validation(
                "apply_delete_like received a non-delete op".into(),
            ))
        }
    };

    if let Some((local_hlc, local_device, is_deleted)) =
        load_entity_version(transaction, entity_type, entity_id).await?
    {
        if is_deleted || !is_remote_newer(&local_hlc, local_device, &remote_hlc, remote_device_id) {
            return Ok(false);
        }
    }

    let deleted_at = remote_hlc.to_string();
    let (table_name, pk_column) = table_identity(transaction, entity_type).await?;
    let sql = format!(
        "UPDATE \"{table}\" SET \"deleted_at\" = ? WHERE \"{pk}\" = ?",
        table = quote_ident(&table_name),
        pk = quote_ident(&pk_column),
    );
    sqlx::query(&sql)
        .bind(&deleted_at)
        .bind(entity_id)
        .execute(&mut **transaction)
        .await?;

    save_entity_version(
        transaction,
        entity_type,
        entity_id,
        &remote_hlc,
        remote_device_id,
        true,
    )
    .await?;
    delete_field_versions(transaction, entity_type, entity_id).await?;
    Ok(true)
}

fn merged_value_to_op(op: &CrdtOp, merged: Value) -> CrdtOp {
    match op {
        CrdtOp::Insert {
            entity_id,
            hlc,
            device_id,
            ..
        }
        | CrdtOp::Update {
            entity_id,
            hlc,
            device_id,
            ..
        } => CrdtOp::Insert {
            entity_id: entity_id.clone(),
            payload: merged,
            hlc: hlc.clone(),
            device_id: *device_id,
        },
        other => other.clone(),
    }
}

fn change_log_row_to_crdt(entity_id: &str, row: ChangeLogRow) -> SyncResult<CrdtOp> {
    let operation = OpKind::from_str(&row.operation)?;
    let payload: Value = serde_json::from_str(&row.payload)?;
    let device_id = Uuid::parse_str(&row.device_id).map_err(|error| {
        SyncError::Validation(format!("invalid device id in changelog: {error}"))
    })?;
    let hlc = HlcTimestamp::from_str(&row.hlc)?;

    match operation {
        OpKind::Insert => Ok(CrdtOp::Insert {
            entity_id: entity_id.to_owned(),
            payload: payload.get("after").cloned().unwrap_or(payload),
            hlc,
            device_id,
        }),
        OpKind::Update => Ok(CrdtOp::Update {
            entity_id: entity_id.to_owned(),
            field_patches: field_patches_from_payload(&payload),
            hlc,
            device_id,
        }),
        OpKind::Delete => Ok(CrdtOp::Delete {
            entity_id: entity_id.to_owned(),
            hlc,
            device_id,
        }),
    }
}

fn field_patches_from_payload(payload: &Value) -> Vec<FieldPatch> {
    let before = payload.get("before").cloned().unwrap_or(Value::Null);
    let after = payload
        .get("after")
        .cloned()
        .unwrap_or_else(|| payload.clone());
    let mut patches = Vec::new();
    diff_values(None, &before, &after, &mut patches);
    patches
}

fn diff_values(prefix: Option<&str>, before: &Value, after: &Value, patches: &mut Vec<FieldPatch>) {
    if before == after {
        return;
    }

    match (before, after) {
        (Value::Object(before_map), Value::Object(after_map)) => {
            let keys: BTreeSet<&String> = before_map.keys().chain(after_map.keys()).collect();
            for key in keys {
                let path = join_field_path(prefix, key);
                diff_values(
                    Some(&path),
                    before_map.get(key).unwrap_or(&Value::Null),
                    after_map.get(key).unwrap_or(&Value::Null),
                    patches,
                );
            }
        }
        _ => patches.push(FieldPatch {
            field: prefix.unwrap_or("$").to_owned(),
            old_value: before.clone(),
            new_value: after.clone(),
        }),
    }
}

fn join_field_path(prefix: Option<&str>, key: &str) -> String {
    match prefix {
        Some(existing) if existing != "$" => format!("{existing}.{key}"),
        _ => key.to_owned(),
    }
}

fn is_remote_newer(
    local_hlc: &HlcTimestamp,
    local_device_id: Uuid,
    remote_hlc: &HlcTimestamp,
    remote_device_id: Uuid,
) -> bool {
    remote_hlc > local_hlc || (remote_hlc == local_hlc && remote_device_id > local_device_id)
}

async fn ensure_sync_metadata_tables(pool: &SqlitePool) -> SyncResult<()> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS sync_runtime_context (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            default_device_id TEXT NOT NULL,
            current_device_id TEXT NOT NULL,
            current_synced INTEGER NOT NULL DEFAULT 0
        )",
        "CREATE TABLE IF NOT EXISTS sync_entity_versions (
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            hlc TEXT NOT NULL,
            device_id TEXT NOT NULL,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (entity_type, entity_id)
        )",
        "CREATE TABLE IF NOT EXISTS sync_field_versions (
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            field TEXT NOT NULL,
            hlc TEXT NOT NULL,
            device_id TEXT NOT NULL,
            PRIMARY KEY (entity_type, entity_id, field)
        )",
        "CREATE TABLE IF NOT EXISTS conflicts (
            id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            field_path TEXT NOT NULL,
            local_value TEXT NOT NULL,
            remote_value TEXT NOT NULL,
            local_hlc TEXT NOT NULL,
            remote_hlc TEXT NOT NULL,
            detected_at INTEGER NOT NULL,
            resolved INTEGER NOT NULL DEFAULT 0,
            resolution TEXT,
            resolved_at INTEGER
        )",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

async fn persist_conflict_record(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    conflict_id: &str,
    workspace_id: Uuid,
    entity_type: EntityType,
    entity_id: &str,
    record: &crate::proto::clawsync::v1::ConflictRecord,
) -> SyncResult<()> {
    let local_value = String::from_utf8(record.local_value.clone()).map_err(|error| {
        SyncError::Validation(format!("invalid local conflict payload encoding: {error}"))
    })?;
    let remote_value = String::from_utf8(record.remote_value.clone()).map_err(|error| {
        SyncError::Validation(format!("invalid remote conflict payload encoding: {error}"))
    })?;

    sqlx::query(
        "INSERT INTO conflicts (
            id, workspace_id, entity_type, entity_id, field_path,
            local_value, remote_value, local_hlc, remote_hlc, detected_at, resolved, resolution
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)",
    )
    .bind(conflict_id)
    .bind(workspace_id.to_string())
    .bind(entity_type.to_string())
    .bind(entity_id)
    .bind("$")
    .bind(local_value)
    .bind(remote_value)
    .bind(&record.local_hlc)
    .bind(&record.remote_hlc)
    .bind(chrono::Utc::now().timestamp_millis())
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn set_runtime_context(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    device_id: Uuid,
    synced: bool,
) -> SyncResult<()> {
    sqlx::query(
        "INSERT INTO sync_runtime_context (id, default_device_id, current_device_id, current_synced)
         VALUES (1, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
            current_device_id = excluded.current_device_id,
            current_synced = excluded.current_synced",
    )
    .bind(device_id.to_string())
    .bind(device_id.to_string())
    .bind(if synced { 1 } else { 0 })
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn reset_runtime_context(transaction: &mut sqlx::Transaction<'_, Sqlite>) -> SyncResult<()> {
    sqlx::query(
        "UPDATE sync_runtime_context
         SET current_device_id = default_device_id,
             current_synced = 0
         WHERE id = 1",
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn load_entity_version(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    entity_id: &str,
) -> SyncResult<Option<(HlcTimestamp, Uuid, bool)>> {
    let row = sqlx::query_as::<_, EntityVersionRow>(
        "SELECT hlc, device_id, is_deleted FROM sync_entity_versions WHERE entity_type = ? AND entity_id = ?",
    )
    .bind(entity_type.to_string())
    .bind(entity_id)
    .fetch_optional(&mut **transaction)
    .await?;

    row.map(|row| {
        Ok((
            HlcTimestamp::from_str(&row.hlc)?,
            Uuid::parse_str(&row.device_id).map_err(|error| {
                SyncError::Validation(format!("invalid entity version device id: {error}"))
            })?,
            row.is_deleted != 0,
        ))
    })
    .transpose()
}

async fn save_entity_version(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    entity_id: &str,
    hlc: &HlcTimestamp,
    device_id: Uuid,
    is_deleted: bool,
) -> SyncResult<()> {
    sqlx::query(
        "INSERT INTO sync_entity_versions (entity_type, entity_id, hlc, device_id, is_deleted)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(entity_type, entity_id) DO UPDATE SET
            hlc = excluded.hlc,
            device_id = excluded.device_id,
            is_deleted = excluded.is_deleted",
    )
    .bind(entity_type.to_string())
    .bind(entity_id)
    .bind(hlc.to_string())
    .bind(device_id.to_string())
    .bind(if is_deleted { 1 } else { 0 })
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn load_field_version(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    entity_id: &str,
    field: &str,
) -> SyncResult<Option<(HlcTimestamp, Uuid)>> {
    let row = sqlx::query_as::<_, FieldVersionRow>(
        "SELECT hlc, device_id FROM sync_field_versions WHERE entity_type = ? AND entity_id = ? AND field = ?",
    )
    .bind(entity_type.to_string())
    .bind(entity_id)
    .bind(field)
    .fetch_optional(&mut **transaction)
    .await?;

    row.map(|row| {
        Ok((
            HlcTimestamp::from_str(&row.hlc)?,
            Uuid::parse_str(&row.device_id).map_err(|error| {
                SyncError::Validation(format!("invalid field version device id: {error}"))
            })?,
        ))
    })
    .transpose()
}

async fn save_field_version(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    entity_id: &str,
    field: &str,
    hlc: &HlcTimestamp,
    device_id: Uuid,
) -> SyncResult<()> {
    sqlx::query(
        "INSERT INTO sync_field_versions (entity_type, entity_id, field, hlc, device_id)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(entity_type, entity_id, field) DO UPDATE SET
            hlc = excluded.hlc,
            device_id = excluded.device_id",
    )
    .bind(entity_type.to_string())
    .bind(entity_id)
    .bind(field)
    .bind(hlc.to_string())
    .bind(device_id.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn delete_field_versions(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    entity_id: &str,
) -> SyncResult<()> {
    sqlx::query("DELETE FROM sync_field_versions WHERE entity_type = ? AND entity_id = ?")
        .bind(entity_type.to_string())
        .bind(entity_id)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn refresh_field_versions(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    entity_id: &str,
    payload: &Value,
    hlc: &HlcTimestamp,
    device_id: Uuid,
) -> SyncResult<()> {
    delete_field_versions(transaction, entity_type, entity_id).await?;
    for field in flatten_fields(payload) {
        save_field_version(transaction, entity_type, entity_id, &field, hlc, device_id).await?;
    }
    Ok(())
}

fn flatten_fields(value: &Value) -> Vec<String> {
    let mut fields = Vec::new();
    flatten_fields_inner(None, value, &mut fields);
    fields
}

fn flatten_fields_inner(prefix: Option<&str>, value: &Value, fields: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, nested) in map {
                let path = join_field_path(prefix, key);
                if nested.is_object() {
                    flatten_fields_inner(Some(&path), nested, fields);
                } else {
                    fields.push(path);
                }
            }
        }
        _ => fields.push(prefix.unwrap_or("$").to_owned()),
    }
}

async fn fetch_entity_payload(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    entity_id: &str,
) -> SyncResult<Option<Value>> {
    let (table_name, pk_column) = table_identity(transaction, entity_type).await?;
    let columns = table_columns(transaction, &table_name).await?;
    let sql = format!(
        "SELECT {json_expr} AS payload FROM \"{table}\" WHERE \"{pk}\" = ? LIMIT 1",
        json_expr = row_json_expression(&columns),
        table = quote_ident(&table_name),
        pk = quote_ident(&pk_column),
    );
    let payload: Option<String> = sqlx::query_scalar(&sql)
        .bind(entity_id)
        .fetch_optional(&mut **transaction)
        .await?;
    payload
        .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
        .transpose()
}

async fn upsert_row(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
    entity_id: &str,
    payload: &Value,
    clear_deleted: bool,
) -> SyncResult<()> {
    let (table_name, pk_column) = table_identity(transaction, entity_type).await?;
    let columns = table_columns(transaction, &table_name).await?;
    let mut payload_map = payload
        .as_object()
        .cloned()
        .ok_or_else(|| SyncError::Validation("entity payload must be a JSON object".into()))?;
    payload_map.insert(pk_column.clone(), Value::String(entity_id.to_owned()));
    if clear_deleted && columns.iter().any(|column| column.name == "deleted_at") {
        payload_map.insert("deleted_at".into(), Value::Null);
    }

    let write_columns: Vec<String> = columns
        .into_iter()
        .filter(|column| payload_map.contains_key(&column.name))
        .map(|column| column.name)
        .collect();
    let payload_json = serde_json::to_string(&Value::Object(payload_map))?;

    let columns_sql = write_columns
        .iter()
        .map(|column| format!("\"{}\"", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let values_sql = write_columns
        .iter()
        .map(|column| format!("json_extract(?, '$.{}')", column))
        .collect::<Vec<_>>()
        .join(", ");
    let updates_sql = write_columns
        .iter()
        .filter(|column| column.as_str() != pk_column)
        .map(|column| {
            format!(
                "\"{name}\" = excluded.\"{name}\"",
                name = quote_ident(column)
            )
        })
        .collect::<Vec<_>>();
    let updates_sql = if updates_sql.is_empty() {
        format!("\"{pk}\" = excluded.\"{pk}\"", pk = quote_ident(&pk_column))
    } else {
        updates_sql.join(", ")
    };

    let sql = format!(
        "INSERT INTO \"{table}\" ({columns}) VALUES ({values})
         ON CONFLICT(\"{pk}\") DO UPDATE SET {updates}",
        table = quote_ident(&table_name),
        columns = columns_sql,
        values = values_sql,
        pk = quote_ident(&pk_column),
        updates = updates_sql,
    );
    let mut query = sqlx::query(&sql);
    for _ in &write_columns {
        query = query.bind(&payload_json);
    }
    query.execute(&mut **transaction).await?;
    Ok(())
}

async fn ensure_deleted_at_column(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    table_name: &str,
) -> SyncResult<()> {
    let columns = table_columns(transaction, table_name).await?;
    if !columns.iter().any(|column| column.name == "deleted_at") {
        let sql = format!(
            "ALTER TABLE \"{}\" ADD COLUMN \"deleted_at\" TEXT",
            quote_ident(table_name)
        );
        sqlx::query(&sql).execute(&mut **transaction).await?;
    }
    Ok(())
}

async fn table_identity(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    entity_type: EntityType,
) -> SyncResult<(String, String)> {
    let table_name = entity_type.as_str().to_owned();
    let columns = table_columns(transaction, &table_name).await?;
    let pk_column = columns
        .iter()
        .find(|column| column.pk > 0)
        .map(|column| column.name.clone())
        .or_else(|| {
            columns
                .iter()
                .find(|column| column.name == "id")
                .map(|column| column.name.clone())
        })
        .ok_or_else(|| {
            SyncError::Validation(format!("table {} is missing a primary key", table_name))
        })?;
    Ok((table_name, pk_column))
}

async fn table_columns(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    table_name: &str,
) -> SyncResult<Vec<TableColumn>> {
    let sql = format!("PRAGMA table_info(\"{}\")", quote_ident(table_name));
    Ok(sqlx::query_as::<_, TableColumn>(&sql)
        .fetch_all(&mut **transaction)
        .await?)
}

fn row_json_expression(columns: &[TableColumn]) -> String {
    let mut parts = Vec::with_capacity(columns.len() * 2);
    for column in columns {
        parts.push(format!("'{}'", column.name));
        parts.push(format!("\"{}\"", quote_ident(&column.name)));
    }
    format!("json_object({})", parts.join(", "))
}

fn quote_ident(identifier: &str) -> String {
    identifier.replace('"', "\"\"")
}

fn apply_patch_field(target: &mut Value, field: &str, new_value: Value) -> bool {
    if field == "$" {
        *target = new_value;
        return true;
    }

    let parts: Vec<&str> = field.split('.').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() {
        return false;
    }

    let mut current = target;
    for part in &parts[..parts.len().saturating_sub(1)] {
        match current {
            Value::Object(map) => {
                current = map
                    .entry((*part).to_owned())
                    .or_insert_with(|| Value::Object(Map::new()));
            }
            _ => return false,
        }
    }

    match current {
        Value::Object(map) => {
            map.insert(parts[parts.len() - 1].to_owned(), new_value);
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use serde_json::json;
    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{detect_local_changes, DeltaApplier};
    use crate::{
        audit::log::AuditLog,
        crdt::{
            clock::HlcTimestamp,
            ops::{CrdtOp, FieldPatch},
            resolver::ConflictResolver,
        },
        crypto::keys::KeyStore,
        delta::extractor::{DeltaExtractor, DeltaSet, EntityType},
        proto::clawsync::v1::SyncCursor,
        sync::checkpoint::CheckpointStore,
    };

    async fn setup_pool() -> sqlx::SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite should connect");
        sqlx::query("CREATE TABLE memory_records (id TEXT PRIMARY KEY, title TEXT, body TEXT)")
            .execute(&pool)
            .await
            .expect("schema should be created");
        pool
    }

    async fn make_applier(pool: sqlx::SqlitePool) -> DeltaApplier {
        let dir = tempdir().expect("tempdir should exist");
        let dir_path = dir.keep();
        let key_store = Arc::new(
            KeyStore::load_or_create(Path::new(&dir_path), b"delta-applier-material")
                .expect("keystore should load"),
        );
        let audit = Arc::new(
            AuditLog::new(&dir_path, key_store)
                .await
                .expect("audit log should initialise"),
        );
        let checkpoint = Arc::new(
            CheckpointStore::new(Path::new(&dir_path)).expect("checkpoint store should initialise"),
        );
        DeltaApplier::new(
            pool,
            Arc::new(ConflictResolver::default()),
            audit,
            checkpoint,
            Uuid::from_bytes([1; 16]),
            Uuid::from_bytes([9; 16]),
        )
    }

    fn insert_op() -> CrdtOp {
        CrdtOp::Insert {
            entity_id: "m1".into(),
            payload: json!({ "title": "hello", "body": "world" }),
            hlc: HlcTimestamp {
                logical_ms: 1,
                counter: 0,
                node_id: Uuid::from_bytes([2; 16]),
            },
            device_id: Uuid::from_bytes([2; 16]),
        }
    }

    #[tokio::test]
    async fn apply_insert_update_and_delete_round_trip() {
        let pool = setup_pool().await;
        let applier = make_applier(pool.clone()).await;
        let extractor = DeltaExtractor::new(pool.clone(), Uuid::from_bytes([9; 16]));
        extractor
            .install_changelog_trigger()
            .await
            .expect("triggers should install");

        let result = applier
            .apply_delta_set(DeltaSet {
                entity_type: EntityType::MemoryRecords,
                ops: vec![insert_op()],
                sequences: vec![1],
                device_id: Uuid::from_bytes([2; 16]),
            })
            .await
            .expect("insert apply should succeed");
        assert_eq!(result.inserted, 1);

        let result = applier
            .apply_delta_set(DeltaSet {
                entity_type: EntityType::MemoryRecords,
                ops: vec![CrdtOp::Update {
                    entity_id: "m1".into(),
                    field_patches: vec![FieldPatch {
                        field: "body".into(),
                        old_value: json!("world"),
                        new_value: json!("updated"),
                    }],
                    hlc: HlcTimestamp {
                        logical_ms: 2,
                        counter: 0,
                        node_id: Uuid::from_bytes([2; 16]),
                    },
                    device_id: Uuid::from_bytes([2; 16]),
                }],
                sequences: vec![2],
                device_id: Uuid::from_bytes([2; 16]),
            })
            .await
            .expect("update apply should succeed");
        assert_eq!(result.updated, 1);

        let body: Option<String> =
            sqlx::query_scalar("SELECT body FROM memory_records WHERE id = 'm1'")
                .fetch_optional(&pool)
                .await
                .expect("row should query");
        assert_eq!(body.as_deref(), Some("updated"));

        let result = applier
            .apply_delta_set(DeltaSet {
                entity_type: EntityType::MemoryRecords,
                ops: vec![CrdtOp::Delete {
                    entity_id: "m1".into(),
                    hlc: HlcTimestamp {
                        logical_ms: 3,
                        counter: 0,
                        node_id: Uuid::from_bytes([2; 16]),
                    },
                    device_id: Uuid::from_bytes([2; 16]),
                }],
                sequences: vec![3],
                device_id: Uuid::from_bytes([2; 16]),
            })
            .await
            .expect("delete apply should succeed");
        assert_eq!(result.deleted, 1);

        let deleted_at: Option<String> =
            sqlx::query_scalar("SELECT deleted_at FROM memory_records WHERE id = 'm1'")
                .fetch_optional(&pool)
                .await
                .expect("row should query");
        assert!(deleted_at.is_some());
    }

    #[tokio::test]
    async fn detect_local_changes_returns_unsynced_changelog_op() {
        let pool = setup_pool().await;
        let extractor = DeltaExtractor::new(pool.clone(), Uuid::from_bytes([7; 16]));
        extractor
            .install_changelog_trigger()
            .await
            .expect("triggers should install");

        sqlx::query("INSERT INTO memory_records (id, title, body) VALUES ('m1', 'title', 'body')")
            .execute(&pool)
            .await
            .expect("insert should succeed");

        let change = detect_local_changes(
            &pool,
            "m1",
            &SyncCursor {
                workspace_id: Uuid::from_bytes([1; 16]).to_string(),
                device_id: Uuid::from_bytes([7; 16]).to_string(),
                lamport_clock: 0,
                hlc_timestamp: String::new(),
                state_hash: Vec::new(),
            },
        )
        .await
        .expect("detection should succeed");

        assert!(matches!(change, Some(CrdtOp::Insert { .. })));
    }
}

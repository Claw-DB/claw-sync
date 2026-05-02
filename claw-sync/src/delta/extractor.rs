//! extractor.rs — reads the WAL and change-log from claw-core to produce `DeltaSet`s.

use std::{collections::HashMap, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, QueryBuilder, Row, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::{
    crdt::{
        clock::{HlcTimestamp, HybridLogicalClock},
        ops::{CrdtOp, FieldPatch, OpKind},
    },
    delta::hasher::{content_hash, entity_hash, state_hash},
    error::{SyncError, SyncResult},
    proto::clawsync::v1::SyncCursor,
};

const DEFAULT_BATCH_LIMIT: usize = 1_000;

/// Entity tables tracked by claw-sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityType {
    /// Rows from the `memory_records` table.
    MemoryRecords,
    /// Rows from the `sessions` table.
    Sessions,
    /// Rows from the `tool_outputs` table.
    ToolOutputs,
}

impl EntityType {
    /// Returns the canonical SQLite table name for this entity type.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MemoryRecords => "memory_records",
            Self::Sessions => "sessions",
            Self::ToolOutputs => "tool_outputs",
        }
    }

    /// Returns all tracked entity types.
    pub fn all() -> [Self; 3] {
        [Self::MemoryRecords, Self::Sessions, Self::ToolOutputs]
    }
}

impl std::fmt::Display for EntityType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for EntityType {
    type Err = SyncError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "memory_records" => Ok(Self::MemoryRecords),
            "sessions" => Ok(Self::Sessions),
            "tool_outputs" => Ok(Self::ToolOutputs),
            other => Err(SyncError::Validation(format!(
                "unsupported entity type: {other}"
            ))),
        }
    }
}

/// An ordered group of changes extracted for a single entity type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSet {
    /// Entity type shared by every operation in this set.
    pub entity_type: EntityType,
    /// CRDT operations extracted from the changelog.
    pub ops: Vec<CrdtOp>,
    /// Changelog sequences represented by this set.
    pub sequences: Vec<i64>,
    /// Local device that produced the set.
    pub device_id: Uuid,
}

/// Reads pending row-level changes from the claw-core changelog.
#[derive(Clone)]
pub struct DeltaExtractor {
    pool: SqlitePool,
    device_id: Uuid,
    batch_limit: usize,
}

#[derive(Debug, FromRow)]
struct ChangeLogRow {
    entity_type: String,
    entity_id: String,
    operation: String,
    payload: String,
    device_id: String,
    hlc: String,
    sequence: i64,
}

#[derive(Debug, FromRow)]
struct TableColumn {
    name: String,
    pk: i64,
}

impl DeltaExtractor {
    /// Creates a new extractor using the supplied SQLite pool and local device id.
    pub fn new(pool: SqlitePool, device_id: Uuid) -> Self {
        Self {
            pool,
            device_id,
            batch_limit: DEFAULT_BATCH_LIMIT,
        }
    }

    /// Overrides the maximum number of changelog rows returned per extraction pass.
    pub fn with_batch_limit(mut self, batch_limit: usize) -> Self {
        self.batch_limit = batch_limit.max(1);
        self
    }

    /// Reads unsynchronised changelog rows and groups them into entity-scoped delta sets.
    pub async fn extract_pending(&self, entity_types: &[EntityType]) -> SyncResult<Vec<DeltaSet>> {
        let rows = self.fetch_pending_rows(entity_types).await?;
        let mut deltas = Vec::new();
        let mut positions = HashMap::new();

        for row in rows {
            let entity_type = EntityType::from_str(&row.entity_type)?;
            let op = change_log_row_to_crdt(&row)?;
            let position = match positions.get(&entity_type) {
                Some(position) => *position,
                None => {
                    let next = deltas.len();
                    deltas.push(DeltaSet {
                        entity_type,
                        ops: Vec::new(),
                        sequences: Vec::new(),
                        device_id: self.device_id,
                    });
                    positions.insert(entity_type, next);
                    next
                }
            };

            let delta = &mut deltas[position];
            delta.ops.push(op);
            delta.sequences.push(row.sequence);
        }

        Ok(deltas)
    }

    /// Marks successfully pushed changelog sequences as synced.
    pub async fn mark_synced(&self, sequences: &[i64]) -> SyncResult<()> {
        if sequences.is_empty() {
            return Ok(());
        }

        let mut builder =
            QueryBuilder::<Sqlite>::new("UPDATE sync_changelog SET synced = 1 WHERE sequence IN (");
        {
            let mut separated = builder.separated(", ");
            for sequence in sequences {
                separated.push_bind(sequence);
            }
        }
        builder.push(')');
        builder.build().execute(&self.pool).await?;
        Ok(())
    }

    /// Returns the number of unsynchronised changelog rows.
    pub async fn pending_count(&self) -> SyncResult<u64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_changelog WHERE synced = 0")
            .fetch_one(&self.pool)
            .await?;
        Ok(count as u64)
    }

    /// Installs the changelog table plus insert, update, and delete triggers for tracked tables.
    pub async fn install_changelog_trigger(&self) -> SyncResult<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS sync_changelog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
                payload TEXT NOT NULL,
                device_id TEXT NOT NULL,
                hlc TEXT NOT NULL,
                sequence INTEGER NOT NULL UNIQUE,
                synced INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS sync_runtime_context (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                default_device_id TEXT NOT NULL,
                current_device_id TEXT NOT NULL,
                current_synced INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO sync_runtime_context (id, default_device_id, current_device_id, current_synced)
             VALUES (1, ?, ?, 0)
             ON CONFLICT(id) DO UPDATE SET
                default_device_id = excluded.default_device_id,
                current_device_id = excluded.default_device_id,
                current_synced = 0",
        )
        .bind(self.device_id.to_string())
        .bind(self.device_id.to_string())
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_sync_changelog_pending ON sync_changelog (synced, sequence)",
        )
        .execute(&self.pool)
        .await?;

        for entity_type in EntityType::all() {
            let table_name = entity_type.as_str();
            if !table_exists(&self.pool, table_name).await? {
                continue;
            }

            let columns = table_columns(&self.pool, table_name).await?;
            if columns.is_empty() {
                continue;
            }

            let primary_key = columns
                .iter()
                .find(|column| column.pk > 0)
                .map(|column| column.name.as_str())
                .or_else(|| {
                    columns
                        .iter()
                        .find(|column| column.name == "id")
                        .map(|column| column.name.as_str())
                })
                .unwrap_or(columns[0].name.as_str());

            let escaped_table = quote_ident(table_name);
            let escaped_pk = quote_ident(primary_key);
            let insert_trigger = format!(
                "CREATE TRIGGER IF NOT EXISTS {name} AFTER INSERT ON \"{table}\" BEGIN
                    INSERT INTO sync_changelog (entity_type, entity_id, operation, payload, device_id, hlc, sequence)
                    VALUES (
                        '{entity_type}',
                        CAST(NEW.\"{pk}\" AS TEXT),
                        'INSERT',
                        json_object('after', {new_payload}),
                        (SELECT current_device_id FROM sync_runtime_context WHERE id = 1),
                        {hlc_sql},
                        (COALESCE((SELECT MAX(sequence) FROM sync_changelog), 0) + 1)
                    );
                    UPDATE sync_changelog
                    SET synced = (SELECT current_synced FROM sync_runtime_context WHERE id = 1)
                    WHERE id = last_insert_rowid();
                END;",
                name = trigger_name(table_name, "insert"),
                table = escaped_table,
                entity_type = table_name,
                pk = escaped_pk,
                new_payload = row_json_expression("NEW", &columns),
                hlc_sql = trigger_hlc_sql(),
            );
            let update_trigger = format!(
                "CREATE TRIGGER IF NOT EXISTS {name} AFTER UPDATE ON \"{table}\" BEGIN
                    INSERT INTO sync_changelog (entity_type, entity_id, operation, payload, device_id, hlc, sequence)
                    VALUES (
                        '{entity_type}',
                        CAST(NEW.\"{pk}\" AS TEXT),
                        'UPDATE',
                        json_object('before', {old_payload}, 'after', {new_payload}),
                        (SELECT current_device_id FROM sync_runtime_context WHERE id = 1),
                        {hlc_sql},
                        (COALESCE((SELECT MAX(sequence) FROM sync_changelog), 0) + 1)
                    );
                    UPDATE sync_changelog
                    SET synced = (SELECT current_synced FROM sync_runtime_context WHERE id = 1)
                    WHERE id = last_insert_rowid();
                END;",
                name = trigger_name(table_name, "update"),
                table = escaped_table,
                entity_type = table_name,
                pk = escaped_pk,
                old_payload = row_json_expression("OLD", &columns),
                new_payload = row_json_expression("NEW", &columns),
                hlc_sql = trigger_hlc_sql(),
            );
            let delete_trigger = format!(
                "CREATE TRIGGER IF NOT EXISTS {name} AFTER DELETE ON \"{table}\" BEGIN
                    INSERT INTO sync_changelog (entity_type, entity_id, operation, payload, device_id, hlc, sequence)
                    VALUES (
                        '{entity_type}',
                        CAST(OLD.\"{pk}\" AS TEXT),
                        'DELETE',
                        json_object('before', {old_payload}),
                        (SELECT current_device_id FROM sync_runtime_context WHERE id = 1),
                        {hlc_sql},
                        (COALESCE((SELECT MAX(sequence) FROM sync_changelog), 0) + 1)
                    );
                    UPDATE sync_changelog
                    SET synced = (SELECT current_synced FROM sync_runtime_context WHERE id = 1)
                    WHERE id = last_insert_rowid();
                END;",
                name = trigger_name(table_name, "delete"),
                table = escaped_table,
                entity_type = table_name,
                pk = escaped_pk,
                old_payload = row_json_expression("OLD", &columns),
                hlc_sql = trigger_hlc_sql(),
            );

            for suffix in ["insert", "update", "delete"] {
                sqlx::query(&format!(
                    "DROP TRIGGER IF EXISTS {}",
                    trigger_name(table_name, suffix)
                ))
                .execute(&self.pool)
                .await?;
            }

            sqlx::query(&insert_trigger).execute(&self.pool).await?;
            sqlx::query(&update_trigger).execute(&self.pool).await?;
            sqlx::query(&delete_trigger).execute(&self.pool).await?;
        }

        Ok(())
    }

    /// Returns the latest local cursor for this device using changelog sequence and entity hashes.
    pub async fn cursor_for_device(&self) -> SyncResult<SyncCursor> {
        let max_sequence: Option<i64> =
            sqlx::query_scalar("SELECT MAX(sequence) FROM sync_changelog")
                .fetch_one(&self.pool)
                .await?;
        let entity_rows = sqlx::query("SELECT DISTINCT entity_type, entity_id FROM sync_changelog")
            .fetch_all(&self.pool)
            .await?;

        let mut hashes = Vec::with_capacity(entity_rows.len());
        for row in entity_rows {
            let entity_type: String = row.try_get("entity_type")?;
            let entity_id: String = row.try_get("entity_id")?;
            hashes.push(entity_hash(
                &entity_type,
                &entity_id,
                &content_hash(entity_id.as_bytes()),
            ));
        }

        Ok(SyncCursor {
            workspace_id: String::new(),
            device_id: self.device_id.to_string(),
            lamport_clock: max_sequence.unwrap_or_default(),
            hlc_timestamp: build_hlc_for_row(self.device_id),
            state_hash: state_hash(&hashes).to_vec(),
        })
    }

    async fn fetch_pending_rows(
        &self,
        entity_types: &[EntityType],
    ) -> SyncResult<Vec<ChangeLogRow>> {
        let mut builder = QueryBuilder::<Sqlite>::new(
            "SELECT entity_type, entity_id, operation, payload, device_id, hlc, sequence FROM sync_changelog WHERE synced = 0",
        );

        if !entity_types.is_empty() {
            builder.push(" AND entity_type IN (");
            let mut separated = builder.separated(", ");
            for entity_type in entity_types {
                separated.push_bind(entity_type.as_str());
            }
            builder.push(')');
        }

        builder.push(" ORDER BY sequence ASC LIMIT ");
        builder.push_bind(self.batch_limit as i64);
        Ok(builder
            .build_query_as::<ChangeLogRow>()
            .fetch_all(&self.pool)
            .await?)
    }
}

/// Builds an HLC string suitable for changelog rows produced on the current device.
pub fn build_hlc_for_row(device_id: Uuid) -> String {
    HlcTimestamp {
        logical_ms: HybridLogicalClock::now(),
        counter: 0,
        node_id: device_id,
    }
    .to_string()
}

fn change_log_row_to_crdt(row: &ChangeLogRow) -> SyncResult<CrdtOp> {
    let operation = OpKind::from_str(&row.operation)?;
    let payload: Value = serde_json::from_str(&row.payload)?;
    let device_id = Uuid::parse_str(&row.device_id).map_err(|error| {
        SyncError::Validation(format!("invalid device id in changelog: {error}"))
    })?;
    let hlc = HlcTimestamp::from_str(&row.hlc)?;

    match operation {
        OpKind::Insert => Ok(CrdtOp::Insert {
            entity_id: row.entity_id.clone(),
            payload: payload.get("after").cloned().unwrap_or(payload),
            hlc,
            device_id,
        }),
        OpKind::Update => Ok(CrdtOp::Update {
            entity_id: row.entity_id.clone(),
            field_patches: field_patches_from_payload(&payload)?,
            hlc,
            device_id,
        }),
        OpKind::Delete => Ok(CrdtOp::Delete {
            entity_id: row.entity_id.clone(),
            hlc,
            device_id,
        }),
    }
}

fn field_patches_from_payload(payload: &Value) -> SyncResult<Vec<FieldPatch>> {
    if let Some(field_patches) = payload.get("field_patches") {
        return serde_json::from_value(field_patches.clone()).map_err(Into::into);
    }

    let before = payload.get("before").cloned().unwrap_or(Value::Null);
    let after = payload
        .get("after")
        .cloned()
        .unwrap_or_else(|| payload.clone());
    let mut patches = Vec::new();
    diff_values(None, &before, &after, &mut patches);
    Ok(patches)
}

fn diff_values(prefix: Option<&str>, before: &Value, after: &Value, patches: &mut Vec<FieldPatch>) {
    if before == after {
        return;
    }

    match (before, after) {
        (Value::Object(before_map), Value::Object(after_map)) => {
            for key in before_map.keys() {
                if let Some(after_value) = after_map.get(key) {
                    let path = join_field_path(prefix, key);
                    diff_values(
                        Some(&path),
                        before_map.get(key).unwrap_or(&Value::Null),
                        after_value,
                        patches,
                    );
                } else {
                    let path = join_field_path(prefix, key);
                    patches.push(FieldPatch {
                        field: path,
                        old_value: before_map.get(key).cloned().unwrap_or(Value::Null),
                        new_value: Value::Null,
                    });
                }
            }

            for (key, after_value) in after_map {
                if !before_map.contains_key(key) {
                    let path = join_field_path(prefix, key);
                    patches.push(FieldPatch {
                        field: path,
                        old_value: Value::Null,
                        new_value: after_value.clone(),
                    });
                }
            }
        }
        _ => {
            patches.push(FieldPatch {
                field: prefix.unwrap_or("$").to_owned(),
                old_value: before.clone(),
                new_value: after.clone(),
            });
        }
    }
}

fn join_field_path(prefix: Option<&str>, key: &str) -> String {
    match prefix {
        Some(existing) if existing != "$" => format!("{existing}.{key}"),
        _ => key.to_owned(),
    }
}

async fn table_exists(pool: &SqlitePool, table_name: &str) -> SyncResult<bool> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
    )
    .bind(table_name)
    .fetch_one(pool)
    .await?;
    Ok(exists > 0)
}

async fn table_columns(pool: &SqlitePool, table_name: &str) -> SyncResult<Vec<TableColumn>> {
    let sql = format!("PRAGMA table_info(\"{}\")", quote_ident(table_name));
    Ok(sqlx::query_as::<_, TableColumn>(&sql)
        .fetch_all(pool)
        .await?)
}

fn row_json_expression(alias: &str, columns: &[TableColumn]) -> String {
    let mut json_parts = Vec::with_capacity(columns.len() * 2);
    for column in columns {
        let escaped_name = quote_ident(&column.name);
        json_parts.push(format!("'{}'", column.name));
        json_parts.push(format!("{alias}.\"{escaped_name}\"",));
    }
    format!("json_object({})", json_parts.join(", "))
}

fn quote_ident(identifier: &str) -> String {
    identifier.replace('"', "\"\"")
}

fn trigger_name(table_name: &str, suffix: &str) -> String {
    format!("sync_changelog_{}_{}", table_name, suffix)
}

fn trigger_hlc_sql() -> String {
    "printf('%016x-%08x-%s', CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER), 0, (SELECT current_device_id FROM sync_runtime_context WHERE id = 1))".to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::sqlite::SqlitePoolOptions;
    use uuid::Uuid;

    use super::{build_hlc_for_row, DeltaExtractor, EntityType};
    use crate::crdt::ops::CrdtOp;

    async fn setup_pool() -> sqlx::SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite should connect");

        for statement in [
            "CREATE TABLE memory_records (id TEXT PRIMARY KEY, title TEXT, body TEXT)",
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, label TEXT)",
            "CREATE TABLE tool_outputs (id TEXT PRIMARY KEY, payload TEXT)",
        ] {
            sqlx::query(statement)
                .execute(&pool)
                .await
                .expect("schema setup should succeed");
        }

        pool
    }

    #[tokio::test]
    async fn install_trigger_and_extract_pending_groups_rows_by_entity_type() {
        let pool = setup_pool().await;
        let extractor = DeltaExtractor::new(pool.clone(), Uuid::from_bytes([7; 16]));
        extractor
            .install_changelog_trigger()
            .await
            .expect("trigger install should succeed");

        sqlx::query("INSERT INTO memory_records (id, title, body) VALUES ('m1', 'title', 'body')")
            .execute(&pool)
            .await
            .expect("insert should succeed");
        sqlx::query("INSERT INTO sessions (id, label) VALUES ('s1', 'session')")
            .execute(&pool)
            .await
            .expect("insert should succeed");
        sqlx::query("UPDATE memory_records SET body = 'body-2' WHERE id = 'm1'")
            .execute(&pool)
            .await
            .expect("update should succeed");

        let deltas = extractor
            .extract_pending(&[EntityType::MemoryRecords, EntityType::Sessions])
            .await
            .expect("extract should succeed");

        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].entity_type, EntityType::MemoryRecords);
        assert_eq!(deltas[0].ops.len(), 2);
        assert_eq!(deltas[1].entity_type, EntityType::Sessions);
        assert_eq!(deltas[1].ops.len(), 1);

        match &deltas[0].ops[0] {
            CrdtOp::Insert { payload, .. } => assert_eq!(payload["title"], json!("title")),
            other => panic!("expected insert op, got {other:?}"),
        }
        match &deltas[0].ops[1] {
            CrdtOp::Update { field_patches, .. } => {
                assert_eq!(field_patches.len(), 1);
                assert_eq!(field_patches[0].field, "body");
                assert_eq!(field_patches[0].new_value, json!("body-2"));
            }
            other => panic!("expected update op, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mark_synced_updates_pending_count() {
        let pool = setup_pool().await;
        let extractor = DeltaExtractor::new(pool.clone(), Uuid::from_bytes([8; 16]));
        extractor
            .install_changelog_trigger()
            .await
            .expect("trigger install should succeed");

        sqlx::query("INSERT INTO tool_outputs (id, payload) VALUES ('t1', 'value')")
            .execute(&pool)
            .await
            .expect("insert should succeed");

        let deltas = extractor
            .extract_pending(&[])
            .await
            .expect("extract should succeed");
        assert_eq!(
            extractor
                .pending_count()
                .await
                .expect("count should succeed"),
            1
        );
        extractor
            .mark_synced(&deltas[0].sequences)
            .await
            .expect("mark synced should succeed");
        assert_eq!(
            extractor
                .pending_count()
                .await
                .expect("count should succeed"),
            0
        );
    }

    #[test]
    fn build_hlc_for_row_emits_parseable_timestamp() {
        let hlc = build_hlc_for_row(Uuid::from_bytes([9; 16]));
        let parsed =
            crate::crdt::clock::HlcTimestamp::from_str(&hlc).expect("HLC string should parse");
        assert_eq!(parsed.node_id, Uuid::from_bytes([9; 16]));
    }
}

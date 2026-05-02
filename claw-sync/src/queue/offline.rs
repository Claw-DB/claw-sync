//! offline.rs — persistent SQLite-backed offline operation queue.

use std::path::Path;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    FromRow, QueryBuilder, Sqlite, SqlitePool,
};
use uuid::Uuid;

use crate::{
    crdt::ops::OpKind,
    delta::extractor::EntityType,
    error::{SyncError, SyncResult},
    queue::drain::DrainResult,
    transport::client::{SyncChunk, SyncHubClient, TransportFailureKind},
};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");
const MAX_ATTEMPTS: u32 = 5;

/// Status of an enqueued sync operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueStatus {
    /// Waiting to be sent.
    Pending,
    /// Currently assigned to a drainer batch.
    InFlight,
    /// Permanently failed after exhausting retries.
    Failed,
    /// Successfully delivered and acknowledged.
    Done,
}

impl QueueStatus {
    /// Returns the canonical lowercase SQLite string representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Failed => "failed",
            Self::Done => "done",
        }
    }
}

impl std::fmt::Display for QueueStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for QueueStatus {
    type Err = SyncError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "pending" => Ok(Self::Pending),
            "in_flight" => Ok(Self::InFlight),
            "failed" => Ok(Self::Failed),
            "done" => Ok(Self::Done),
            other => Err(SyncError::Validation(format!(
                "unsupported queue status: {other}"
            ))),
        }
    }
}

/// A sync operation persisted for offline replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedOp {
    /// Stable queue identifier.
    pub id: String,
    /// Workspace this operation belongs to.
    pub workspace_id: Uuid,
    /// Target entity type.
    pub entity_type: EntityType,
    /// Operation kind to replay.
    pub operation: OpKind,
    /// Target entity identifier.
    pub entity_id: String,
    /// JSON payload associated with the operation.
    pub payload: Value,
    /// Device that originated the operation.
    pub device_id: Uuid,
    /// HLC timestamp string associated with the operation.
    pub hlc: String,
    /// UTC time when the operation was queued.
    pub enqueued_at: DateTime<Utc>,
    /// Number of failed delivery attempts.
    pub attempt_count: u32,
    /// Current queue status.
    pub status: QueueStatus,
}

impl QueuedOp {
    /// Creates a new pending queue item with generated identifier and current timestamp.
    pub fn new(
        workspace_id: Uuid,
        entity_type: EntityType,
        operation: OpKind,
        entity_id: impl Into<String>,
        payload: Value,
        device_id: Uuid,
        hlc: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            workspace_id,
            entity_type,
            operation,
            entity_id: entity_id.into(),
            payload,
            device_id,
            hlc: hlc.into(),
            enqueued_at: Utc::now(),
            attempt_count: 0,
            status: QueueStatus::Pending,
        }
    }
}

/// A durable queue of pending sync operations stored in a local SQLite database.
#[derive(Clone)]
pub struct OfflineQueue {
    pub(crate) pool: SqlitePool,
}

#[derive(Debug, FromRow)]
struct QueueRow {
    id: String,
    workspace_id: String,
    entity_type: String,
    operation: String,
    entity_id: String,
    payload: String,
    device_id: String,
    hlc: String,
    enqueued_at: String,
    attempt_count: i64,
    last_attempt_at: Option<String>,
    status: String,
}

impl TryFrom<QueueRow> for QueuedOp {
    type Error = SyncError;

    fn try_from(row: QueueRow) -> Result<Self, Self::Error> {
        let _ = row.last_attempt_at;
        Ok(Self {
            id: row.id,
            workspace_id: Uuid::parse_str(&row.workspace_id)
                .map_err(|error| SyncError::Validation(format!("invalid workspace id: {error}")))?,
            entity_type: row.entity_type.parse()?,
            operation: row.operation.parse()?,
            entity_id: row.entity_id,
            payload: serde_json::from_str(&row.payload)?,
            device_id: Uuid::parse_str(&row.device_id)
                .map_err(|error| SyncError::Validation(format!("invalid device id: {error}")))?,
            hlc: row.hlc,
            enqueued_at: DateTime::parse_from_rfc3339(&row.enqueued_at)
                .map_err(|error| {
                    SyncError::Validation(format!("invalid enqueue timestamp: {error}"))
                })?
                .with_timezone(&Utc),
            attempt_count: row.attempt_count.max(0) as u32,
            status: row.status.parse()?,
        })
    }
}

impl OfflineQueue {
    /// Opens or creates `queue.db` inside `data_dir` and runs the queue migrations.
    pub async fn new(data_dir: &Path) -> SyncResult<Self> {
        tokio::fs::create_dir_all(data_dir).await?;

        let db_path = data_dir.join("queue.db");
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;

        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    /// Enqueues an operation for later replay.
    pub async fn enqueue(&self, op: &QueuedOp) -> SyncResult<()> {
        sqlx::query(
            "INSERT INTO sync_queue (
                id, workspace_id, entity_type, operation, entity_id, payload,
                device_id, hlc, enqueued_at, attempt_count, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(workspace_id, entity_type, operation, entity_id, hlc, device_id) DO UPDATE SET
                payload = excluded.payload,
                status = CASE WHEN sync_queue.status = 'done' THEN 'done' ELSE 'pending' END,
                attempt_count = CASE WHEN sync_queue.status = 'done' THEN sync_queue.attempt_count ELSE 0 END,
                last_attempt_at = CASE WHEN sync_queue.status = 'done' THEN sync_queue.last_attempt_at ELSE NULL END,
                error_message = CASE WHEN sync_queue.status = 'done' THEN sync_queue.error_message ELSE NULL END",
        )
        .bind(&op.id)
        .bind(op.workspace_id.to_string())
        .bind(op.entity_type.to_string())
        .bind(op.operation.as_str())
        .bind(&op.entity_id)
        .bind(serde_json::to_string(&op.payload)?)
        .bind(op.device_id.to_string())
        .bind(&op.hlc)
        .bind(op.enqueued_at.to_rfc3339())
        .bind(i64::from(op.attempt_count))
        .bind(op.status.as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Dequeues up to `limit` pending operations and atomically marks them as in flight.
    pub async fn dequeue_batch(&self, limit: u32) -> SyncResult<Vec<QueuedOp>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut transaction = self.pool.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM sync_queue WHERE status = 'pending' ORDER BY enqueued_at ASC LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await?;

        if ids.is_empty() {
            transaction.commit().await?;
            return Ok(Vec::new());
        }

        let attempt_time = Utc::now().to_rfc3339();
        let mut update = QueryBuilder::<Sqlite>::new(
            "UPDATE sync_queue SET status = 'in_flight', last_attempt_at = ",
        );
        update.push_bind(&attempt_time);
        update.push(" WHERE status = 'pending' AND id IN (");
        {
            let mut separated = update.separated(", ");
            for id in &ids {
                separated.push_bind(id);
            }
        }
        update.push(')');
        update.build().execute(&mut *transaction).await?;

        let rows = fetch_rows_by_ids(&mut transaction, &ids).await?;
        transaction.commit().await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// Marks delivered operations as done.
    pub async fn mark_done(&self, ids: &[String]) -> SyncResult<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let completed_at = Utc::now().to_rfc3339();
        let mut builder = QueryBuilder::<Sqlite>::new(
            "UPDATE sync_queue SET status = 'done', error_message = NULL, last_attempt_at = ",
        );
        builder.push_bind(&completed_at);
        builder.push(" WHERE id IN (");
        {
            let mut separated = builder.separated(", ");
            for id in ids {
                separated.push_bind(id);
            }
        }
        builder.push(')');
        builder.build().execute(&self.pool).await?;
        Ok(())
    }

    /// Marks an in-flight operation as failed and either retries it or finalises it as failed.
    pub async fn mark_failed(&self, id: &str, error: &str) -> SyncResult<()> {
        let attempted_at = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE sync_queue
             SET attempt_count = attempt_count + 1,
                 last_attempt_at = ?,
                 error_message = ?,
                 status = CASE WHEN attempt_count + 1 >= ? THEN 'failed' ELSE 'pending' END
             WHERE id = ?",
        )
        .bind(attempted_at)
        .bind(error)
        .bind(i64::from(MAX_ATTEMPTS))
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Requeues stale in-flight operations abandoned longer than `older_than_secs`.
    pub async fn requeue_stale_inflight(&self, older_than_secs: u64) -> SyncResult<u64> {
        let threshold = Utc::now() - ChronoDuration::seconds(older_than_secs as i64);
        let result = sqlx::query(
            "UPDATE sync_queue
             SET status = 'pending'
             WHERE status = 'in_flight' AND (last_attempt_at IS NULL OR last_attempt_at < ?)",
        )
        .bind(threshold.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Returns the number of currently pending operations.
    pub async fn pending_count(&self) -> SyncResult<u64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_queue WHERE status = 'pending'")
                .fetch_one(&self.pool)
                .await?;
        Ok(count as u64)
    }

    /// Returns every permanently failed operation.
    pub async fn failed_ops(&self) -> SyncResult<Vec<QueuedOp>> {
        let rows = sqlx::query_as::<_, QueueRow>(
            "SELECT id, workspace_id, entity_type, operation, entity_id, payload, device_id,
                    hlc, enqueued_at, attempt_count, last_attempt_at, status
             FROM sync_queue WHERE status = 'failed' ORDER BY enqueued_at ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// Deletes completed operations older than `older_than_days` days.
    pub async fn purge_done(&self, older_than_days: u32) -> SyncResult<u64> {
        let threshold = Utc::now() - ChronoDuration::days(i64::from(older_than_days));
        let result = sqlx::query(
            "DELETE FROM sync_queue
             WHERE status = 'done' AND COALESCE(last_attempt_at, enqueued_at) < ?",
        )
        .bind(threshold.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Enqueues a transport chunk into the lightweight offline chunk queue.
    pub async fn enqueue_chunk(&self, chunk: &SyncChunk) -> SyncResult<()> {
        let chunk_bytes = serde_json::to_vec(chunk)?;
        sqlx::query(
            "INSERT INTO offline_queue (chunk_bytes, enqueued_at, attempts, last_attempt, status)
             VALUES (?, ?, 0, NULL, 'pending')",
        )
        .bind(chunk_bytes)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Drains up to `max_batch` pending chunk rows and pushes them to the hub.
    pub async fn drain_chunks(
        &self,
        client: &mut SyncHubClient,
        max_batch: usize,
        max_retries: u32,
    ) -> SyncResult<DrainResult> {
        if max_batch == 0 {
            return Ok(DrainResult::default());
        }

        let rows = sqlx::query_as::<_, OfflineChunkRow>(
            "SELECT id, chunk_bytes, attempts
             FROM offline_queue
             WHERE status = 'pending'
             ORDER BY enqueued_at ASC
             LIMIT ?",
        )
        .bind(max_batch as i64)
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Ok(DrainResult {
                remaining_pending: self.pending_chunk_count().await?,
                ..DrainResult::default()
            });
        }

        let mut chunks = Vec::with_capacity(rows.len());
        for row in &rows {
            chunks.push(serde_json::from_slice::<SyncChunk>(&row.chunk_bytes)?);
        }

        let push_result = client.push_deltas(chunks).await?;
        let mut result = DrainResult::default();

        for (index, row) in rows.iter().enumerate() {
            let outcome = push_result.per_chunk.get(index);
            match outcome.map(|current| current.accepted).unwrap_or(false) {
                true => {
                    sqlx::query("DELETE FROM offline_queue WHERE id = ?")
                        .bind(row.id)
                        .execute(&self.pool)
                        .await?;
                    result.delivered = result.delivered.saturating_add(1);
                }
                false => {
                    let failure_kind = outcome
                        .and_then(|current| current.failure_kind)
                        .unwrap_or(TransportFailureKind::Transient);
                    let next_attempts = row.attempts.saturating_add(1);
                    let failed = next_attempts >= max_retries as i64
                        || failure_kind == TransportFailureKind::Permanent;
                    sqlx::query(
                        "UPDATE offline_queue
                         SET attempts = ?,
                             last_attempt = ?,
                             status = CASE WHEN ? THEN 'failed' ELSE 'pending' END
                         WHERE id = ?",
                    )
                    .bind(next_attempts)
                    .bind(Utc::now().timestamp_millis())
                    .bind(failed)
                    .bind(row.id)
                    .execute(&self.pool)
                    .await?;

                    if failed {
                        result.failed = result.failed.saturating_add(1);
                    } else {
                        result.retried = result.retried.saturating_add(1);
                    }
                }
            }
        }

        result.remaining_pending = self.pending_chunk_count().await?;
        Ok(result)
    }

    /// Returns the number of pending chunk rows in `offline_queue`.
    pub async fn pending_chunk_count(&self) -> SyncResult<u64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM offline_queue WHERE status = 'pending'")
                .fetch_one(&self.pool)
                .await?;
        Ok(count.max(0) as u64)
    }
}

#[derive(sqlx::FromRow)]
struct OfflineChunkRow {
    id: i64,
    chunk_bytes: Vec<u8>,
    attempts: i64,
}

async fn fetch_rows_by_ids(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    ids: &[String],
) -> SyncResult<Vec<QueueRow>> {
    let mut builder = QueryBuilder::<Sqlite>::new(
        "SELECT id, workspace_id, entity_type, operation, entity_id, payload, device_id,
                hlc, enqueued_at, attempt_count, last_attempt_at, status
         FROM sync_queue WHERE id IN (",
    );
    {
        let mut separated = builder.separated(", ");
        for id in ids {
            separated.push_bind(id);
        }
    }
    builder.push(") ORDER BY enqueued_at ASC");

    Ok(builder
        .build_query_as::<QueueRow>()
        .fetch_all(&mut **transaction)
        .await?)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::{OfflineQueue, QueueStatus, QueuedOp};
    use crate::{crdt::ops::OpKind, delta::extractor::EntityType};

    fn queued_op(id: &str) -> QueuedOp {
        QueuedOp {
            id: id.into(),
            workspace_id: Uuid::from_bytes([1; 16]),
            entity_type: EntityType::MemoryRecords,
            operation: OpKind::Insert,
            entity_id: format!("entity-{id}"),
            payload: json!({ "title": id }),
            device_id: Uuid::from_bytes([2; 16]),
            hlc: "0000000000000001-00000000-02020202-0202-0202-0202-020202020202".into(),
            enqueued_at: Utc::now(),
            attempt_count: 0,
            status: QueueStatus::Pending,
        }
    }

    #[tokio::test]
    async fn enqueue_dequeue_and_mark_done_round_trip() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let queue = OfflineQueue::new(temp_dir.path())
            .await
            .expect("queue should initialise");
        let op = queued_op("1");

        queue.enqueue(&op).await.expect("enqueue should succeed");
        assert_eq!(
            queue.pending_count().await.expect("count should succeed"),
            1
        );

        let batch = queue
            .dequeue_batch(10)
            .await
            .expect("dequeue should succeed");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].status, QueueStatus::InFlight);

        queue
            .mark_done(&[batch[0].id.clone()])
            .await
            .expect("mark done should succeed");
        assert_eq!(
            queue.pending_count().await.expect("count should succeed"),
            0
        );
    }

    #[tokio::test]
    async fn mark_failed_retries_until_max_attempts_then_fails() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let queue = OfflineQueue::new(temp_dir.path())
            .await
            .expect("queue should initialise");
        let op = queued_op("2");

        queue.enqueue(&op).await.expect("enqueue should succeed");
        for _ in 0..4 {
            let batch = queue
                .dequeue_batch(1)
                .await
                .expect("dequeue should succeed");
            queue
                .mark_failed(&batch[0].id, "temporary")
                .await
                .expect("mark failed should succeed");
        }

        let batch = queue
            .dequeue_batch(1)
            .await
            .expect("dequeue should succeed");
        queue
            .mark_failed(&batch[0].id, "permanent")
            .await
            .expect("mark failed should succeed");

        let failed = queue.failed_ops().await.expect("failed ops should load");
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].attempt_count, 5);
        assert_eq!(failed[0].status, QueueStatus::Failed);
    }

    #[tokio::test]
    async fn requeue_stale_inflight_resets_pending_state() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let queue = OfflineQueue::new(temp_dir.path())
            .await
            .expect("queue should initialise");
        let op = queued_op("3");

        queue.enqueue(&op).await.expect("enqueue should succeed");
        let batch = queue
            .dequeue_batch(1)
            .await
            .expect("dequeue should succeed");
        sqlx::query("UPDATE sync_queue SET last_attempt_at = ? WHERE id = ?")
            .bind((Utc::now() - chrono::Duration::hours(2)).to_rfc3339())
            .bind(&batch[0].id)
            .execute(&queue.pool)
            .await
            .expect("timestamp update should succeed");

        let requeued = queue
            .requeue_stale_inflight(60)
            .await
            .expect("requeue should succeed");
        assert_eq!(requeued, 1);
        assert_eq!(
            queue.pending_count().await.expect("count should succeed"),
            1
        );
    }

    #[tokio::test]
    async fn purge_done_removes_old_completed_rows() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let queue = OfflineQueue::new(temp_dir.path())
            .await
            .expect("queue should initialise");
        let op = queued_op("4");

        queue.enqueue(&op).await.expect("enqueue should succeed");
        queue
            .mark_done(&[op.id.clone()])
            .await
            .expect("mark done should succeed");
        sqlx::query("UPDATE sync_queue SET last_attempt_at = ? WHERE id = ?")
            .bind((Utc::now() - chrono::Duration::days(5)).to_rfc3339())
            .bind(&op.id)
            .execute(&queue.pool)
            .await
            .expect("timestamp update should succeed");

        let purged = queue.purge_done(1).await.expect("purge should succeed");
        assert_eq!(purged, 1);
    }

    #[tokio::test]
    async fn enqueue_deduplicates_same_business_operation() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let queue = OfflineQueue::new(temp_dir.path())
            .await
            .expect("queue should initialise");
        let op = queued_op("5");

        queue
            .enqueue(&op)
            .await
            .expect("first enqueue should succeed");
        queue
            .enqueue(&op)
            .await
            .expect("second enqueue should succeed");

        assert_eq!(
            queue.pending_count().await.expect("count should succeed"),
            1
        );
    }
}

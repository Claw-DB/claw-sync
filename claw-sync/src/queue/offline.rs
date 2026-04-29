//! offline.rs — persistent SQLite-backed offline operation queue.

use sqlx::SqlitePool;

use crate::error::SyncResult;

/// A durable queue of pending sync operations stored in a local SQLite database.
pub struct OfflineQueue {
    pool: SqlitePool,
}

impl OfflineQueue {
    /// Open (or create) the offline queue database at `db_url`.
    pub async fn open(db_url: &str) -> SyncResult<Self> {
        let pool = SqlitePool::connect(db_url).await?;
        Ok(Self { pool })
    }

    /// Enqueue a serialised operation payload for later delivery.
    pub async fn enqueue(&self, payload: &[u8]) -> SyncResult<()> {
        sqlx::query("INSERT INTO offline_queue (payload) VALUES (?)")
            .bind(payload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Fetch up to `limit` pending operations in insertion order.
    pub async fn peek(&self, limit: i64) -> SyncResult<Vec<Vec<u8>>> {
        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT payload FROM offline_queue ORDER BY id LIMIT ?")
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|(p,)| p).collect())
    }
}

//! reconcile.rs — full reconciliation pass using hash-set comparison via the Reconcile RPC.

use std::sync::Arc;

use sqlx::{FromRow, Row, SqlitePool};

use crate::{
    config::SyncConfig,
    delta::{extractor::EntityType, hasher::content_hash},
    engine::SyncEngine,
    error::SyncResult,
    proto::clawsync::v1::{EntityHashEntry, ReconcileRequest},
    sync::{pull::PullStats, push::PushStats},
    transport::reconnect::ReconnectingClient,
};

/// Summary of a reconciliation round.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReconcileStats {
    /// Number of local entity hashes sent to the hub.
    pub local_hashes: u32,
    /// Number of entities the hub reported missing on the client.
    pub missing_on_client: u32,
    /// Number of entities the hub reported missing on the server.
    pub missing_on_server: u32,
    /// Number of entities reported as conflicting.
    pub conflicted: u32,
    /// Number of entities pushed as a follow-up to reconciliation.
    pub pushed: u32,
    /// Number of entities pulled as a follow-up to reconciliation.
    pub pulled: u32,
    /// Follow-up push stats, when a push pass was triggered.
    pub push: Option<PushStats>,
    /// Follow-up pull stats, when a pull pass was triggered.
    pub pull: Option<PullStats>,
}

/// Runs a full reconciliation pass against the hub.
#[derive(Clone)]
pub struct ReconcileSession {
    config: Arc<SyncConfig>,
    pool: SqlitePool,
    transport: ReconnectingClient,
}

#[derive(Debug, FromRow)]
struct TableColumn {
    name: String,
    pk: i64,
}

impl ReconcileSession {
    /// Creates a new reconciliation session.
    pub fn new(config: Arc<SyncConfig>, pool: SqlitePool, transport: ReconnectingClient) -> Self {
        Self {
            config,
            pool,
            transport,
        }
    }

    /// Executes a full reconciliation round-trip.
    pub async fn reconcile(&self) -> SyncResult<ReconcileStats> {
        let entity_hashes = collect_local_hashes(&self.pool).await?;
        let mut client = self.transport.get_or_connect().await?;
        let response = client
            .reconcile(ReconcileRequest {
                workspace_id: self.config.workspace_id.to_string(),
                device_id: self.config.device_id.to_string(),
                entity_hashes: entity_hashes.clone(),
            })
            .await?;

        let stats = ReconcileStats {
            local_hashes: entity_hashes.len() as u32,
            missing_on_client: response.missing_on_client.len() as u32,
            missing_on_server: response.missing_on_server.len() as u32,
            conflicted: response.conflicted.len() as u32,
            pushed: 0,
            pulled: 0,
            push: None,
            pull: None,
        };

        Ok(stats)
    }
}

/// Performs a full reconciliation pass against the hub for the given workspace.
pub async fn reconcile(workspace_id: &str) -> SyncResult<()> {
    let engine = SyncEngine::from_env_for_workspace(workspace_id).await?;
    engine.reconcile_once().await?;
    Ok(())
}

async fn collect_local_hashes(pool: &SqlitePool) -> SyncResult<Vec<EntityHashEntry>> {
    let mut hashes = Vec::new();

    for entity_type in EntityType::all() {
        let table_name = entity_type.as_str();
        if !table_exists(pool, table_name).await? {
            continue;
        }

        let columns = table_columns(pool, table_name).await?;
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
        let sql = format!(
            "SELECT CAST(\"{pk}\" AS TEXT) AS entity_id, {payload} AS payload FROM \"{table}\"",
            pk = quote_ident(primary_key),
            payload = row_json_expression(&columns),
            table = quote_ident(table_name),
        );

        let rows = sqlx::query(&sql).fetch_all(pool).await?;
        for row in rows {
            let entity_id: String = row.try_get("entity_id")?;
            let payload: String = row.try_get("payload")?;
            hashes.push(EntityHashEntry {
                entity_id,
                content_hash: content_hash(payload.as_bytes()).to_vec(),
                entity_type: entity_type.to_string(),
            });
        }
    }

    Ok(hashes)
}

async fn table_exists(pool: &SqlitePool, table_name: &str) -> SyncResult<bool> {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table_name)
            .fetch_one(pool)
            .await?;
    Ok(count > 0)
}

async fn table_columns(pool: &SqlitePool, table_name: &str) -> SyncResult<Vec<TableColumn>> {
    let sql = format!("PRAGMA table_info(\"{}\")", quote_ident(table_name));
    Ok(sqlx::query_as::<_, TableColumn>(&sql)
        .fetch_all(pool)
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

#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;

    use super::collect_local_hashes;

    #[tokio::test]
    async fn collect_local_hashes_serialises_tracked_rows() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite should connect");
        sqlx::query("CREATE TABLE memory_records (id TEXT PRIMARY KEY, title TEXT)")
            .execute(&pool)
            .await
            .expect("table should create");
        sqlx::query("INSERT INTO memory_records (id, title) VALUES ('m1', 'hello')")
            .execute(&pool)
            .await
            .expect("row should insert");

        let hashes = collect_local_hashes(&pool)
            .await
            .expect("hash collection should succeed");
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].entity_id, "m1");
        assert_eq!(hashes[0].entity_type, "memory_records");
        assert_eq!(hashes[0].content_hash.len(), 32);
    }
}

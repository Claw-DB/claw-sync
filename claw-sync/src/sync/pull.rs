//! pull.rs — pull remote deltas from the hub and apply them to the local store.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use crate::{
    config::SyncConfig,
    crypto::keys::KeyStore,
    delta::{
        extractor::EntityType,
        unpacker::{chunks_from_proto, unpack},
    },
    engine::SyncEngine,
    error::{SyncError, SyncResult},
    proto::clawsync::v1::{DeltaChunk, PullResponse, SyncCursor},
    sync::{
        apply::{ApplyResult, DeltaApplier},
        checkpoint::CheckpointStore,
    },
    transport::reconnect::ReconnectingClient,
};

/// Summary of a completed pull pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PullStats {
    /// Number of payloads successfully received and applied.
    pub deltas_received: u32,
    /// Number of protobuf chunks received from the hub.
    pub chunks_received: u32,
    /// Number of entity operations applied locally.
    pub ops_applied: u32,
    /// Number of operations skipped by conflict resolution or version checks.
    pub ops_skipped: u32,
}

/// Detailed result of a completed pull pass.
#[derive(Debug, Clone)]
pub(crate) struct PullOutcome {
    /// Aggregate pull statistics.
    pub stats: PullStats,
    /// Cursor that should be persisted after the pull.
    pub cursor: SyncCursor,
    /// Affected entity type when only one type was seen.
    pub entity_type: Option<EntityType>,
    /// Unique entity ids represented in the applied deltas.
    pub entity_ids: Vec<String>,
}

/// Inbound pull session that streams payloads from the hub and applies them locally.
#[derive(Clone)]
pub struct PullSession {
    config: Arc<SyncConfig>,
    transport: ReconnectingClient,
    key_store: Arc<KeyStore>,
    applier: Arc<DeltaApplier>,
    checkpoint: Arc<CheckpointStore>,
}

impl PullSession {
    /// Creates a new pull session.
    pub fn new(
        config: Arc<SyncConfig>,
        transport: ReconnectingClient,
        key_store: Arc<KeyStore>,
        applier: Arc<DeltaApplier>,
        checkpoint: Arc<CheckpointStore>,
    ) -> Self {
        Self {
            config,
            transport,
            key_store,
            applier,
            checkpoint,
        }
    }

    /// Pulls remote deltas for the requested entity types.
    pub async fn pull_pending(&self, entity_types: &[EntityType]) -> SyncResult<PullStats> {
        Ok(self.pull_pending_detailed(entity_types).await?.stats)
    }

    /// Pulls remote deltas for the requested entity types, returning audit metadata.
    pub(crate) async fn pull_pending_detailed(
        &self,
        entity_types: &[EntityType],
    ) -> SyncResult<PullOutcome> {
        let current_cursor = self.current_cursor().await?;
        let mut client = self.transport.get_or_connect().await?;
        let mut stream = client
            .pull(current_cursor.clone(), entity_types.to_vec())
            .await?;

        let mut buffers: HashMap<String, Vec<DeltaChunk>> = HashMap::new();
        let mut latest_cursor = None;
        let mut stats = PullStats::default();
        let mut entity_type = None;
        let mut entity_ids = BTreeSet::new();

        while let Some(response) = stream
            .message()
            .await
            .map_err(|error| SyncError::Transport(format!("pull response failed: {error}")))?
        {
            stats.chunks_received += response.chunks.len() as u32;
            latest_cursor = response
                .server_cursor
                .clone()
                .map(|cursor| normalise_cursor(cursor, &self.config))
                .or(latest_cursor);

            let ready_payloads = buffer_payloads(&mut buffers, response)?;
            for payload_chunks in ready_payloads {
                let payload = chunks_from_proto(payload_chunks)?;
                let delta = unpack(&payload, self.key_store.workspace_key())?;
                for op in &delta.ops {
                    entity_ids.insert(op.entity_id().to_owned());
                }
                entity_type = match entity_type {
                    None => Some(delta.entity_type),
                    Some(existing) if existing == delta.entity_type => Some(existing),
                    _ => None,
                };
                let apply_result = self.applier.apply_delta_set(delta).await?;
                stats.deltas_received += 1;
                stats.ops_applied += applied_ops(&apply_result);
                stats.ops_skipped += apply_result.skipped;
            }
        }

        if !buffers.is_empty() {
            return Err(SyncError::Transport(
                "pull stream ended with incomplete payload chunks".into(),
            ));
        }

        let should_persist_cursor = latest_cursor.is_some();
        let cursor = latest_cursor.unwrap_or(current_cursor);
        if should_persist_cursor {
            self.checkpoint.save(&cursor).await?;
        }

        Ok(PullOutcome {
            stats,
            cursor,
            entity_type,
            entity_ids: entity_ids.into_iter().collect(),
        })
    }

    async fn current_cursor(&self) -> SyncResult<SyncCursor> {
        if let Some(checkpoint) = self
            .checkpoint
            .load(self.config.workspace_id, self.config.device_id)
            .await?
        {
            return Ok(normalise_cursor(checkpoint, &self.config));
        }

        Ok(SyncCursor {
            workspace_id: self.config.workspace_id.to_string(),
            device_id: self.config.device_id.to_string(),
            lamport_clock: 0,
            hlc_timestamp: String::new(),
            state_hash: Vec::new(),
        })
    }
}

/// Pulls remote deltas using environment-backed engine configuration.
pub async fn pull_deltas(workspace_id: &str) -> SyncResult<()> {
    let engine = SyncEngine::from_env_for_workspace(workspace_id).await?;
    engine.pull_once().await?;
    Ok(())
}

fn buffer_payloads(
    buffers: &mut HashMap<String, Vec<DeltaChunk>>,
    response: PullResponse,
) -> SyncResult<Vec<Vec<DeltaChunk>>> {
    let mut complete = Vec::new();

    for chunk in response.chunks {
        if chunk.total <= 0 {
            return Err(SyncError::Validation(
                "pull response chunk total must be greater than zero".into(),
            ));
        }

        let chunk_id = chunk.chunk_id.clone();
        let total = chunk.total as usize;
        let entry = buffers.entry(chunk_id.clone()).or_default();
        entry.push(chunk);
        if entry.len() == total {
            complete.push(
                buffers
                    .remove(&chunk_id)
                    .expect("payload buffer must exist"),
            );
        }
    }

    Ok(complete)
}

fn applied_ops(result: &ApplyResult) -> u32 {
    result.inserted + result.updated + result.deleted
}

fn normalise_cursor(mut cursor: SyncCursor, config: &SyncConfig) -> SyncCursor {
    if cursor.workspace_id.is_empty() {
        cursor.workspace_id = config.workspace_id.to_string();
    }
    if cursor.device_id.is_empty() {
        cursor.device_id = config.device_id.to_string();
    }
    cursor
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use uuid::Uuid;

    use super::{applied_ops, buffer_payloads, normalise_cursor};
    use crate::{
        config::SyncConfig,
        proto::clawsync::v1::{DeltaChunk, PullResponse, SyncCursor},
        sync::apply::ApplyResult,
    };

    fn config() -> Arc<SyncConfig> {
        Arc::new(SyncConfig {
            workspace_id: Uuid::from_bytes([1; 16]),
            device_id: Uuid::from_bytes([2; 16]),
            hub_endpoint: "http://localhost:50051".into(),
            data_dir: PathBuf::from(".claw-sync"),
            db_path: PathBuf::from("claw_sync.db"),
            tls_enabled: false,
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            sync_interval_secs: 15,
            heartbeat_interval_secs: 15,
            max_retries: 5,
            retry_base_ms: 100,
            max_delta_rows: 1_000,
            max_chunk_bytes: 64 * 1024,
            max_pull_chunks: 128,
            max_push_inflight: 4,
        })
    }

    #[test]
    fn buffer_payloads_waits_for_all_chunks() {
        let mut buffers = HashMap::new();
        let first = buffer_payloads(
            &mut buffers,
            PullResponse {
                chunks: vec![DeltaChunk {
                    chunk_id: "chunk-1".into(),
                    seq: 0,
                    total: 2,
                    encrypted_payload: vec![1],
                    payload_hash: vec![0; 32],
                    entity_type: "memory_records".into(),
                }],
                server_cursor: None,
                has_more: true,
            },
        )
        .expect("buffering should succeed");
        assert!(first.is_empty());

        let second = buffer_payloads(
            &mut buffers,
            PullResponse {
                chunks: vec![DeltaChunk {
                    chunk_id: "chunk-1".into(),
                    seq: 1,
                    total: 2,
                    encrypted_payload: vec![2],
                    payload_hash: vec![0; 32],
                    entity_type: "memory_records".into(),
                }],
                server_cursor: None,
                has_more: false,
            },
        )
        .expect("buffering should succeed");
        assert_eq!(second.len(), 1);
        assert!(buffers.is_empty());
    }

    #[test]
    fn applied_ops_counts_insert_update_and_delete() {
        let count = applied_ops(&ApplyResult {
            inserted: 1,
            updated: 2,
            deleted: 3,
            skipped: 4,
            conflicts_resolved: 0,
        });
        assert_eq!(count, 6);
    }

    #[test]
    fn normalise_cursor_fills_missing_ids() {
        let config = config();
        let cursor = normalise_cursor(
            SyncCursor {
                workspace_id: String::new(),
                device_id: String::new(),
                lamport_clock: 1,
                hlc_timestamp: String::new(),
                state_hash: Vec::new(),
            },
            &config,
        );
        assert_eq!(cursor.workspace_id, config.workspace_id.to_string());
        assert_eq!(cursor.device_id, config.device_id.to_string());
    }
}

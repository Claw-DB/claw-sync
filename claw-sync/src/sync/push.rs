//! push.rs — push local deltas to the sync hub using the gRPC streaming Push RPC.

use std::{collections::BTreeSet, sync::Arc};

use crate::{
    config::SyncConfig,
    crdt::ops::CrdtOp,
    crypto::keys::KeyStore,
    delta::{
        extractor::{DeltaExtractor, DeltaSet, EntityType},
        packer::{pack, payload_to_proto_chunks},
    },
    engine::SyncEngine,
    error::{SyncError, SyncResult},
    proto::clawsync::v1::{PushRequest, SyncCursor},
    queue::offline::{OfflineQueue, QueuedOp},
    sync::checkpoint::CheckpointStore,
    transport::reconnect::ReconnectingClient,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Summary of a completed push pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushStats {
    /// Number of delta sets successfully submitted.
    pub deltas_sent: u32,
    /// Number of individual CRDT operations represented by the push.
    pub ops_sent: u32,
    /// Number of protobuf chunks sent over the wire.
    pub chunks_sent: u32,
    /// Number of local changelog sequences marked synced.
    pub sequences_synced: u32,
    /// Number of transport retries needed before success.
    pub retries: u32,
}

impl PushStats {
    fn merge(&mut self, other: Self) {
        self.deltas_sent += other.deltas_sent;
        self.ops_sent += other.ops_sent;
        self.chunks_sent += other.chunks_sent;
        self.sequences_synced += other.sequences_synced;
        self.retries += other.retries;
    }
}

/// Detailed result of a completed push pass.
#[derive(Debug, Clone)]
pub(crate) struct PushOutcome {
    /// Aggregate push statistics.
    pub stats: PushStats,
    /// Cursor that should be persisted after the push.
    pub cursor: SyncCursor,
    /// Affected entity type when the pass only touched one kind.
    pub entity_type: Option<EntityType>,
    /// Unique entity ids represented by the push.
    pub entity_ids: Vec<String>,
}

/// Outbound push session for local delta transmission.
#[derive(Clone)]
pub struct PushSession {
    config: Arc<SyncConfig>,
    extractor: DeltaExtractor,
    queue: OfflineQueue,
    transport: ReconnectingClient,
    key_store: Arc<KeyStore>,
    checkpoint: Arc<CheckpointStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueEnvelope {
    op: CrdtOp,
    sequences: Vec<i64>,
}

impl PushSession {
    /// Creates a new push session using the shared runtime dependencies.
    pub fn new(
        config: Arc<SyncConfig>,
        extractor: DeltaExtractor,
        queue: OfflineQueue,
        transport: ReconnectingClient,
        key_store: Arc<KeyStore>,
        checkpoint: Arc<CheckpointStore>,
    ) -> Self {
        Self {
            config,
            extractor,
            queue,
            transport,
            key_store,
            checkpoint,
        }
    }

    /// Returns whether a transport client is currently connected.
    pub fn is_connected(&self) -> bool {
        self.transport.is_connected()
    }

    /// Extracts and pushes all currently pending local deltas.
    pub async fn push_pending(&self) -> SyncResult<PushStats> {
        Ok(self.push_pending_detailed().await?.stats)
    }

    /// Extracts and pushes all currently pending local deltas, returning audit metadata.
    pub(crate) async fn push_pending_detailed(&self) -> SyncResult<PushOutcome> {
        let deltas = self.extractor.extract_pending(&EntityType::all()).await?;
        let mut stats = PushStats::default();
        let mut latest_cursor = self.current_cursor().await?;
        let mut entity_type = None;
        let mut entity_ids = BTreeSet::new();

        if deltas.is_empty() {
            return Ok(PushOutcome {
                stats,
                cursor: latest_cursor,
                entity_type: None,
                entity_ids: Vec::new(),
            });
        }

        for delta in deltas {
            for op in &delta.ops {
                entity_ids.insert(op.entity_id().to_owned());
            }
            entity_type = match entity_type {
                None => Some(delta.entity_type),
                Some(existing) if existing == delta.entity_type => Some(existing),
                _ => None,
            };

            let (result, cursor) = self.push_one(&delta).await?;
            latest_cursor = cursor;
            stats.merge(result);
        }

        Ok(PushOutcome {
            stats,
            cursor: latest_cursor,
            entity_type,
            entity_ids: entity_ids.into_iter().collect(),
        })
    }

    /// Replays a previously queued operation using the same packing and transport path.
    pub async fn replay_queued_op(&self, queued: &QueuedOp) -> SyncResult<PushStats> {
        let envelope = decode_queue_envelope(&queued.payload)?;
        let delta = DeltaSet {
            entity_type: queued.entity_type,
            ops: vec![envelope.op],
            sequences: envelope.sequences,
            device_id: queued.device_id,
        };
        self.push_delta(&delta, false).await.map(|(stats, _)| stats)
    }

    /// Persists a delta set into the offline queue.
    pub async fn enqueue_delta_set(&self, delta: &DeltaSet) -> SyncResult<u32> {
        let mut enqueued = 0_u32;
        for (index, op) in delta.ops.iter().enumerate() {
            let queued = QueuedOp::new(
                self.config.workspace_id,
                delta.entity_type,
                op.kind(),
                op.entity_id(),
                serde_json::to_value(QueueEnvelope {
                    op: op.clone(),
                    sequences: delta.sequences.get(index).copied().into_iter().collect(),
                })?,
                op.device_id(),
                op.hlc().to_string(),
            );
            self.queue.enqueue(&queued).await?;
            enqueued += 1;
        }
        Ok(enqueued)
    }

    async fn push_one(&self, delta: &DeltaSet) -> SyncResult<(PushStats, SyncCursor)> {
        self.push_delta(delta, true).await
    }

    async fn push_delta(
        &self,
        delta: &DeltaSet,
        mark_synced: bool,
    ) -> SyncResult<(PushStats, SyncCursor)> {
        let payload = pack(
            delta,
            self.key_store.workspace_key(),
            self.config.max_chunk_bytes,
        )?;
        let chunk_count = payload.chunks.len() as u32;
        let requests = vec![PushRequest {
            cursor: Some(self.current_cursor().await?),
            chunks: payload_to_proto_chunks(&payload),
        }];

        let mut client = self.transport.get_or_connect().await?;
        let mut responses = client.push_stream(futures::stream::iter(requests)).await?;

        let mut accepted = false;
        let mut latest_cursor = None;
        while let Some(response) = responses
            .message()
            .await
            .map_err(|error| SyncError::Transport(format!("push response failed: {error}")))?
        {
            if !response.accepted {
                let reason = if response.reason.is_empty() {
                    "hub rejected pushed payload".to_owned()
                } else {
                    response.reason
                };
                return Err(SyncError::Transport(reason));
            }
            if !response.rejected_chunk_ids.is_empty() {
                return Err(SyncError::Transport(format!(
                    "hub rejected chunks: {}",
                    response.rejected_chunk_ids.join(",")
                )));
            }
            if let Some(server_cursor) = response.server_cursor {
                let cursor = normalise_cursor(server_cursor, &self.config);
                self.checkpoint.save(&cursor).await?;
                latest_cursor = Some(cursor);
            }
            accepted = true;
        }

        if !accepted {
            return Err(SyncError::Transport(
                "push stream completed without an acknowledgement".into(),
            ));
        }

        if mark_synced && !delta.sequences.is_empty() {
            self.extractor.mark_synced(&delta.sequences).await?;
        }

        Ok((
            PushStats {
                deltas_sent: 1,
                ops_sent: delta.ops.len() as u32,
                chunks_sent: chunk_count,
                sequences_synced: if mark_synced {
                    delta.sequences.len() as u32
                } else {
                    0
                },
                retries: 0,
            },
            latest_cursor.unwrap_or(normalise_cursor(
                self.extractor.cursor_for_device().await?,
                &self.config,
            )),
        ))
    }

    async fn current_cursor(&self) -> SyncResult<SyncCursor> {
        if let Some(checkpoint) = self
            .checkpoint
            .load(self.config.workspace_id, self.config.device_id)
            .await?
        {
            return Ok(normalise_cursor(checkpoint, &self.config));
        }

        let mut cursor = self.extractor.cursor_for_device().await?;
        cursor.workspace_id = self.config.workspace_id.to_string();
        cursor.device_id = self.config.device_id.to_string();
        Ok(cursor)
    }
}

/// Pushes all pending local deltas using environment-backed engine configuration.
pub async fn push_deltas(workspace_id: &str) -> SyncResult<()> {
    let engine = SyncEngine::from_env_for_workspace(workspace_id).await?;
    engine.push_once().await?;
    Ok(())
}

fn decode_queue_envelope(payload: &Value) -> SyncResult<QueueEnvelope> {
    Ok(serde_json::from_value(payload.clone())?)
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
    use std::{path::PathBuf, sync::Arc};

    use serde_json::json;
    use sqlx::sqlite::SqlitePoolOptions;
    use uuid::Uuid;

    use super::{decode_queue_envelope, normalise_cursor, PushSession};
    use crate::{
        config::SyncConfig,
        crdt::{clock::HlcTimestamp, ops::CrdtOp},
        crypto::keys::KeyStore,
        delta::extractor::{DeltaExtractor, DeltaSet, EntityType},
        proto::clawsync::v1::SyncCursor,
        queue::offline::OfflineQueue,
        sync::checkpoint::CheckpointStore,
        transport::reconnect::ReconnectingClient,
    };

    fn config(data_dir: PathBuf) -> Arc<SyncConfig> {
        Arc::new(SyncConfig {
            workspace_id: Uuid::from_bytes([1; 16]),
            device_id: Uuid::from_bytes([2; 16]),
            hub_endpoint: "http://localhost:50051".into(),
            data_dir,
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

    #[tokio::test]
    async fn enqueue_delta_set_persists_replayable_envelopes() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite should connect");
        let tempdir = tempfile::tempdir().expect("tempdir should exist");
        let config = config(tempdir.path().to_path_buf());
        let extractor = DeltaExtractor::new(pool.clone(), config.device_id);
        let queue = OfflineQueue::new(tempdir.path())
            .await
            .expect("offline queue should initialise");
        let key_store = Arc::new(
            KeyStore::load_or_create(tempdir.path(), b"push-session-test-material")
                .expect("keystore should load"),
        );
        let transport = ReconnectingClient::new(config.clone(), key_store.clone());
        let checkpoint = Arc::new(
            CheckpointStore::new(tempdir.path()).expect("checkpoint store should initialise"),
        );
        let session = PushSession::new(
            config,
            extractor,
            queue.clone(),
            transport,
            key_store,
            checkpoint,
        );

        let delta = DeltaSet {
            entity_type: EntityType::MemoryRecords,
            ops: vec![CrdtOp::Insert {
                entity_id: "m1".into(),
                payload: json!({ "title": "hello" }),
                hlc: HlcTimestamp {
                    logical_ms: 1,
                    counter: 0,
                    node_id: Uuid::from_bytes([2; 16]),
                },
                device_id: Uuid::from_bytes([2; 16]),
            }],
            sequences: vec![7],
            device_id: Uuid::from_bytes([2; 16]),
        };

        session
            .enqueue_delta_set(&delta)
            .await
            .expect("enqueue should succeed");

        let queued = queue
            .dequeue_batch(1)
            .await
            .expect("dequeue should succeed");
        assert_eq!(queued.len(), 1);
        let envelope = decode_queue_envelope(&queued[0].payload).expect("payload should decode");
        assert_eq!(envelope.sequences, vec![7]);
        assert!(matches!(envelope.op, CrdtOp::Insert { .. }));
    }

    #[test]
    fn normalise_cursor_backfills_workspace_and_device_ids() {
        let config = config(PathBuf::from(".claw-sync-test"));
        let cursor = normalise_cursor(
            SyncCursor {
                workspace_id: String::new(),
                device_id: String::new(),
                lamport_clock: 3,
                hlc_timestamp: "hlc".into(),
                state_hash: vec![1, 2, 3],
            },
            &config,
        );

        assert_eq!(cursor.workspace_id, config.workspace_id.to_string());
        assert_eq!(cursor.device_id, config.device_id.to_string());
    }
}

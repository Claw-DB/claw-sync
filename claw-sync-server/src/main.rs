//! main.rs — PostgreSQL-backed sync hub server with TLS.

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use base64::Engine as _;
use chrono::Utc;
use claw_sync::{
    crdt::{clock::HlcTimestamp, ops::CrdtOp},
    crypto::keys::KeyStore,
    delta::{
        extractor::{DeltaSet, EntityType},
        packer::{pack, payload_to_proto_chunks},
        unpacker::{chunks_from_proto, unpack},
    },
    proto::clawsync::v1::{
        sync_service_server::{SyncService, SyncServiceServer},
        DeltaChunk, HeartbeatRequest, HeartbeatResponse, PullRequest, PullResponse, PushRequest,
        PushResponse, ReconcileRequest, ReconcileResponse, RegisterDeviceRequest,
        RegisterDeviceResponse, ResolveConflictRequest, ResolveConflictResponse, SyncCursor,
    },
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    metadata::AsciiMetadataValue,
    transport::{Identity, Server, ServerTlsConfig},
    Code, Request, Response, Status, Streaming,
};
use tracing_subscriber::{fmt, EnvFilter};
use uuid::Uuid;

#[derive(Clone)]
struct HubService {
    pool: PgPool,
    key_store: Arc<KeyStore>,
}

#[derive(sqlx::FromRow)]
struct HubRow {
    seq: i64,
    entity_type: String,
    entity_id: String,
    op_kind: String,
    op_payload: serde_json::Value,
    hlc: String,
    device_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let addr = std::env::var("CLAW_LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_owned())
        .parse()?;
    let database_url = std::env::var("CLAW_SYNC_HUB_DATABASE_URL").map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "CLAW_SYNC_HUB_DATABASE_URL is required",
        )
    })?;

    let cert_path = std::env::var("CLAW_SYNC_TLS_CERT").map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "CLAW_SYNC_TLS_CERT is required",
        )
    })?;
    let key_path = std::env::var("CLAW_SYNC_TLS_KEY").map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "CLAW_SYNC_TLS_KEY is required",
        )
    })?;

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await?;
    ensure_hub_tables(&pool).await?;

    let key_seed = std::env::var("CLAW_SYNC_SERVER_KEY_SEED")
        .unwrap_or_else(|_| "claw-sync-server-default-key-seed".to_owned());
    let key_dir = PathBuf::from(
        std::env::var("CLAW_SYNC_SERVER_DATA_DIR")
            .unwrap_or_else(|_| ".claw-sync-server".to_owned()),
    );
    let key_store = Arc::new(KeyStore::load_or_create(&key_dir, key_seed.as_bytes())?);

    let cert_pem = tokio::fs::read(cert_path).await?;
    let key_pem = tokio::fs::read(key_path).await?;
    let identity = Identity::from_pem(cert_pem, key_pem);

    let service = HubService { pool, key_store };

    tracing::info!(%addr, "claw-sync-server starting");

    Server::builder()
        .tls_config(ServerTlsConfig::new().identity(identity))?
        .add_service(SyncServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

#[tonic::async_trait]
impl SyncService for HubService {
    async fn register_device(
        &self,
        request: Request<RegisterDeviceRequest>,
    ) -> Result<Response<RegisterDeviceResponse>, Status> {
        let request = request.into_inner();
        let device = request.device.ok_or_else(|| {
            status_with_detail(
                Code::InvalidArgument,
                "validation",
                "missing device metadata",
            )
        })?;

        let device_id = Uuid::parse_str(&device.device_id).map_err(|_| {
            status_with_detail(Code::InvalidArgument, "validation", "invalid device_id")
        })?;
        let workspace_id = Uuid::parse_str(&device.workspace_id).map_err(|_| {
            status_with_detail(Code::InvalidArgument, "validation", "invalid workspace_id")
        })?;

        let public_key = base64::engine::general_purpose::STANDARD
            .decode(device.public_key_b64.as_bytes())
            .map_err(|_| {
                status_with_detail(
                    Code::InvalidArgument,
                    "validation",
                    "public_key_b64 is not valid base64",
                )
            })?;

        sqlx::query(
            "INSERT INTO hub_devices (
                device_id, workspace_id, public_key_ed25519, registered_at, last_seen, last_pull_cursor
            ) VALUES ($1, $2, $3, NOW(), NOW(), 0)
            ON CONFLICT (device_id) DO UPDATE SET
                workspace_id = EXCLUDED.workspace_id,
                public_key_ed25519 = EXCLUDED.public_key_ed25519,
                last_seen = NOW()",
        )
        .bind(device_id)
        .bind(workspace_id)
        .bind(public_key)
        .execute(&self.pool)
        .await
        .map_err(database_status)?;

        Ok(Response::new(RegisterDeviceResponse {
            accepted: true,
            assigned_device_id: device.device_id,
            server_public_key: self.key_store.signing_keypair().public.0.to_vec(),
        }))
    }

    type PushStream = ReceiverStream<Result<PushResponse, Status>>;

    async fn push(
        &self,
        request: Request<Streaming<PushRequest>>,
    ) -> Result<Response<Self::PushStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(32);
        let pool = self.pool.clone();
        let key_store = self.key_store.clone();

        tokio::spawn(async move {
            let mut chunk_buffers: HashMap<String, Vec<DeltaChunk>> = HashMap::new();
            let mut max_seq = 0_i64;

            loop {
                let next = match stream.message().await {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = tx
                            .send(Err(status_with_detail(
                                Code::Unavailable,
                                "transport",
                                &format!("push stream error: {error}"),
                            )))
                            .await;
                        return;
                    }
                };

                let Some(push_request) = next else {
                    return;
                };

                let cursor = match push_request.cursor {
                    Some(cursor) => cursor,
                    None => {
                        let _ = tx
                            .send(Err(status_with_detail(
                                Code::InvalidArgument,
                                "validation",
                                "missing push cursor",
                            )))
                            .await;
                        return;
                    }
                };

                let workspace_id = match Uuid::parse_str(&cursor.workspace_id) {
                    Ok(id) => id,
                    Err(_) => {
                        let _ = tx
                            .send(Err(status_with_detail(
                                Code::InvalidArgument,
                                "validation",
                                "invalid cursor workspace_id",
                            )))
                            .await;
                        return;
                    }
                };

                let device_id = match Uuid::parse_str(&cursor.device_id) {
                    Ok(id) => id,
                    Err(_) => {
                        let _ = tx
                            .send(Err(status_with_detail(
                                Code::InvalidArgument,
                                "validation",
                                "invalid cursor device_id",
                            )))
                            .await;
                        return;
                    }
                };

                let mut rejected_chunk_ids = Vec::new();
                for chunk in push_request.chunks {
                    let computed_hash =
                        claw_sync::delta::hasher::content_hash(&chunk.encrypted_payload);
                    let expected_hash: [u8; 32] = match chunk.payload_hash.as_slice().try_into() {
                        Ok(hash) => hash,
                        Err(_) => {
                            rejected_chunk_ids.push(chunk.chunk_id.clone());
                            continue;
                        }
                    };
                    if computed_hash != expected_hash {
                        rejected_chunk_ids.push(chunk.chunk_id.clone());
                        continue;
                    }

                    let entry = chunk_buffers.entry(chunk.chunk_id.clone()).or_default();
                    entry.push(chunk.clone());
                    let expected_total = entry
                        .first()
                        .map(|current| current.total.max(0) as usize)
                        .unwrap_or(0);
                    if expected_total == 0 || entry.len() < expected_total {
                        continue;
                    }

                    let ready = chunk_buffers.remove(&chunk.chunk_id).unwrap_or_default();
                    let payload = match chunks_from_proto(ready) {
                        Ok(payload) => payload,
                        Err(_) => {
                            rejected_chunk_ids.push(chunk.chunk_id.clone());
                            continue;
                        }
                    };

                    let delta = match unpack(&payload, key_store.workspace_key()) {
                        Ok(delta) => delta,
                        Err(_) => {
                            rejected_chunk_ids.push(payload.chunk_id.clone());
                            continue;
                        }
                    };

                    for op in delta.ops {
                        let (op_kind, entity_id, op_payload, hlc) = op_to_hub_row(&op);
                        let seq = match sqlx::query(
                            "INSERT INTO hub_memory_log (
                                workspace_id, device_id, entity_type, entity_id, op_kind, op_payload, hlc, received_at
                            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, NOW())
                            RETURNING seq",
                        )
                        .bind(workspace_id)
                        .bind(device_id)
                        .bind(delta.entity_type.to_string())
                        .bind(entity_id)
                        .bind(op_kind)
                        .bind(op_payload.to_string())
                        .bind(hlc)
                        .fetch_one(&pool)
                        .await
                        {
                            Ok(row) => row.get::<i64, _>("seq"),
                            Err(_) => {
                                rejected_chunk_ids.push(payload.chunk_id.clone());
                                continue;
                            }
                        };
                        if seq > max_seq {
                            max_seq = seq;
                        }
                    }
                }

                let accepted = rejected_chunk_ids.is_empty();
                let reason = if accepted {
                    String::new()
                } else {
                    "one or more chunks failed verification or persistence".to_owned()
                };
                let response = PushResponse {
                    accepted,
                    server_cursor: Some(SyncCursor {
                        workspace_id: cursor.workspace_id,
                        device_id: cursor.device_id,
                        lamport_clock: max_seq,
                        hlc_timestamp: String::new(),
                        state_hash: Vec::new(),
                    }),
                    rejected_chunk_ids,
                    reason,
                };

                if tx.send(Ok(response)).await.is_err() {
                    return;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type PullStream = ReceiverStream<Result<PullResponse, Status>>;

    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let request = request.into_inner();
        let cursor = request.client_cursor.ok_or_else(|| {
            status_with_detail(Code::InvalidArgument, "validation", "missing pull cursor")
        })?;

        let workspace_id = if !request.workspace_id.is_empty() {
            request.workspace_id
        } else {
            cursor.workspace_id.clone()
        };
        let workspace_uuid = Uuid::parse_str(&workspace_id).map_err(|_| {
            status_with_detail(Code::InvalidArgument, "validation", "invalid workspace_id")
        })?;

        let max_rows = request.max_chunks.max(1) as i64;
        let since_seq = cursor.lamport_clock.max(0);

        let rows = sqlx::query_as::<_, HubRow>(
            "SELECT seq, entity_type, entity_id, op_kind, op_payload, hlc, device_id
             FROM hub_memory_log
             WHERE workspace_id = $1 AND seq > $2
             ORDER BY seq ASC
             LIMIT $3",
        )
        .bind(workspace_uuid)
        .bind(since_seq)
        .bind(max_rows)
        .fetch_all(&self.pool)
        .await
        .map_err(database_status)?;

        let mut outbound_chunks = Vec::new();
        let mut latest_seq = since_seq;
        for row in rows {
            let entity_type = row.entity_type.parse::<EntityType>().map_err(|_| {
                status_with_detail(
                    Code::Internal,
                    "storage",
                    "invalid entity_type in hub_memory_log",
                )
            })?;
            let op = row_to_op(&row).map_err(|error| {
                status_with_detail(
                    Code::Internal,
                    "storage",
                    &format!("failed to rehydrate op: {error}"),
                )
            })?;
            let device_id = Uuid::parse_str(&row.device_id).map_err(|_| {
                status_with_detail(
                    Code::Internal,
                    "storage",
                    "invalid device_id in hub_memory_log",
                )
            })?;
            let delta = DeltaSet {
                entity_type,
                ops: vec![op],
                sequences: vec![row.seq],
                device_id,
            };
            let payload =
                pack(&delta, self.key_store.workspace_key(), 64 * 1024).map_err(|error| {
                    status_with_detail(Code::Internal, "crypto", &format!("pack failed: {error}"))
                })?;
            outbound_chunks.extend(payload_to_proto_chunks(&payload));
            latest_seq = row.seq;
        }

        let has_more: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM hub_memory_log WHERE workspace_id = $1 AND seq > $2
            )",
        )
        .bind(workspace_uuid)
        .bind(latest_seq)
        .fetch_one(&self.pool)
        .await
        .map_err(database_status)?;

        let response = PullResponse {
            chunks: outbound_chunks,
            server_cursor: Some(SyncCursor {
                workspace_id,
                device_id: cursor.device_id,
                lamport_clock: latest_seq,
                hlc_timestamp: String::new(),
                state_hash: Vec::new(),
            }),
            has_more,
        };

        let (tx, rx) = mpsc::channel(1);
        let _ = tx.send(Ok(response)).await;
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn reconcile(
        &self,
        _request: Request<ReconcileRequest>,
    ) -> Result<Response<ReconcileResponse>, Status> {
        Ok(Response::new(ReconcileResponse {
            missing_on_client: Vec::new(),
            missing_on_server: Vec::new(),
            conflicted: Vec::new(),
        }))
    }

    async fn resolve_conflict(
        &self,
        _request: Request<ResolveConflictRequest>,
    ) -> Result<Response<ResolveConflictResponse>, Status> {
        Ok(Response::new(ResolveConflictResponse { ok: true }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let device_id = Uuid::parse_str(&request.device_id).map_err(|_| {
            status_with_detail(Code::InvalidArgument, "validation", "invalid device_id")
        })?;

        sqlx::query("UPDATE hub_devices SET last_seen = NOW() WHERE device_id = $1")
            .bind(device_id)
            .execute(&self.pool)
            .await
            .map_err(database_status)?;

        let row = sqlx::query(
            "SELECT COALESCE(last_pull_cursor, 0) AS last_pull_cursor
             FROM hub_devices WHERE device_id = $1",
        )
        .bind(device_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_status)?;
        let last_pull_cursor = row
            .as_ref()
            .map(|value| value.get::<i64, _>("last_pull_cursor"))
            .unwrap_or(0);

        let sync_required: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM hub_memory_log WHERE workspace_id = $1 AND seq > $2
            )",
        )
        .bind(Uuid::parse_str(&request.workspace_id).map_err(|_| {
            status_with_detail(Code::InvalidArgument, "validation", "invalid workspace_id")
        })?)
        .bind(last_pull_cursor)
        .fetch_one(&self.pool)
        .await
        .map_err(database_status)?;

        Ok(Response::new(HeartbeatResponse {
            server_time: Utc::now().timestamp_millis(),
            sync_required,
        }))
    }
}

fn op_to_hub_row(op: &CrdtOp) -> (&'static str, String, serde_json::Value, String) {
    match op {
        CrdtOp::Insert {
            entity_id,
            payload,
            hlc,
            ..
        } => (
            "INSERT",
            entity_id.clone(),
            payload.clone(),
            hlc.to_string(),
        ),
        CrdtOp::Update {
            entity_id,
            field_patches,
            hlc,
            ..
        } => (
            "UPDATE",
            entity_id.clone(),
            serde_json::to_value(field_patches).unwrap_or(serde_json::Value::Array(Vec::new())),
            hlc.to_string(),
        ),
        CrdtOp::Delete { entity_id, hlc, .. } => (
            "DELETE",
            entity_id.clone(),
            serde_json::Value::Null,
            hlc.to_string(),
        ),
        CrdtOp::Tombstone {
            entity_id,
            deleted_at,
        } => (
            "TOMBSTONE",
            entity_id.clone(),
            serde_json::Value::Null,
            deleted_at.to_string(),
        ),
    }
}

fn row_to_op(row: &HubRow) -> Result<CrdtOp, String> {
    let hlc = HlcTimestamp::from_str(&row.hlc).map_err(|error| error.to_string())?;
    let device_id = Uuid::parse_str(&row.device_id).map_err(|error| error.to_string())?;
    match row.op_kind.as_str() {
        "INSERT" => Ok(CrdtOp::Insert {
            entity_id: row.entity_id.clone(),
            payload: row.op_payload.clone(),
            hlc,
            device_id,
        }),
        "UPDATE" => Ok(CrdtOp::Update {
            entity_id: row.entity_id.clone(),
            field_patches: serde_json::from_value(row.op_payload.clone())
                .map_err(|error| error.to_string())?,
            hlc,
            device_id,
        }),
        "DELETE" => Ok(CrdtOp::Delete {
            entity_id: row.entity_id.clone(),
            hlc,
            device_id,
        }),
        "TOMBSTONE" => Ok(CrdtOp::Tombstone {
            entity_id: row.entity_id.clone(),
            deleted_at: hlc,
        }),
        other => Err(format!("unsupported op_kind: {other}")),
    }
}

async fn ensure_hub_tables(pool: &PgPool) -> Result<(), sqlx::Error> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS hub_memory_log (
            seq BIGSERIAL PRIMARY KEY,
            workspace_id UUID NOT NULL,
            device_id UUID NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            op_kind TEXT NOT NULL,
            op_payload JSONB NOT NULL,
            hlc TEXT NOT NULL,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        "CREATE INDEX IF NOT EXISTS idx_hub_memory_log_workspace_seq
         ON hub_memory_log (workspace_id, seq)",
        "CREATE TABLE IF NOT EXISTS hub_devices (
            device_id UUID PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            public_key_ed25519 BYTEA NOT NULL,
            registered_at TIMESTAMPTZ NOT NULL,
            last_seen TIMESTAMPTZ NOT NULL,
            last_pull_cursor BIGINT NOT NULL DEFAULT 0
        )",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

fn status_with_detail(code: Code, kind: &str, message: &str) -> Status {
    let mut status = Status::new(code, message.to_owned());
    if let Ok(value) = AsciiMetadataValue::try_from(kind) {
        status.metadata_mut().insert("error-kind", value);
    }
    status
}

fn database_status(error: sqlx::Error) -> Status {
    status_with_detail(
        Code::Internal,
        "database",
        &format!("database operation failed: {error}"),
    )
}

//! main.rs — PostgreSQL-backed sync hub server with optional TLS.

use std::{path::PathBuf, sync::Arc};

use base64::Engine as _;
use chrono::Utc;
use claw_sync::{
    crypto::keys::KeyStore,
    proto::clawsync::v1::{
        sync_service_server::{SyncService, SyncServiceServer},
        DeltaChunk, HeartbeatRequest, HeartbeatResponse, PullRequest, PullResponse, PushRequest,
        PushResponse, ReconcileRequest, ReconcileResponse, RegisterDeviceRequest,
        RegisterDeviceResponse, ResolveConflictRequest, ResolveConflictResponse, SyncCursor,
    },
};
use prost::Message;
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
struct HubChunkRow {
    seq: i64,
    chunk_bytes: Vec<u8>,
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

    let tls_cert_path = std::env::var("CLAW_SYNC_TLS_CERT").ok();
    let tls_key_path = std::env::var("CLAW_SYNC_TLS_KEY").ok();

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

    let service = HubService { pool, key_store };

    tracing::info!(%addr, "claw-sync-server starting");

    let server = Server::builder();
    let mut server = match (tls_cert_path, tls_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = tokio::fs::read(cert_path).await?;
            let key_pem = tokio::fs::read(key_path).await?;
            tracing::info!("claw-sync-server TLS enabled");
            server.tls_config(
                ServerTlsConfig::new().identity(Identity::from_pem(cert_pem, key_pem)),
            )?
        }
        _ => {
            tracing::warn!("CLAW_SYNC_TLS_CERT or CLAW_SYNC_TLS_KEY not set; starting without TLS");
            server
        }
    };

    server
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
                id, workspace_id, public_key_ed25519, registered_at, last_seen, last_pull_cursor
            ) VALUES ($1, $2, $3, NOW(), NOW(), 0)
            ON CONFLICT (id) DO UPDATE SET
                workspace_id = EXCLUDED.workspace_id,
                public_key_ed25519 = EXCLUDED.public_key_ed25519,
                last_seen = NOW()",
        )
        .bind(device_id.to_string())
        .bind(workspace_id.to_string())
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

        tokio::spawn(async move {
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
                    let chunk_bytes = chunk.encode_to_vec();
                    let expected_hash: [u8; 32] = match chunk.payload_hash.as_slice().try_into() {
                        Ok(hash) => hash,
                        Err(_) => {
                            rejected_chunk_ids.push(chunk.chunk_id.clone());
                            continue;
                        }
                    };
                    let computed_hash =
                        claw_sync::delta::hasher::content_hash(&chunk.encrypted_payload);
                    if computed_hash != expected_hash {
                        rejected_chunk_ids.push(chunk.chunk_id.clone());
                        continue;
                    }

                    let seq = match sqlx::query(
                        "INSERT INTO hub_memory_log (
                            workspace_id, device_id, chunk_bytes, payload_hash
                        ) VALUES ($1, $2, $3, $4)
                        RETURNING seq",
                    )
                    .bind(workspace_id.to_string())
                    .bind(device_id.to_string())
                    .bind(match chunk_bytes.is_empty() {
                        false => chunk_bytes,
                        true => {
                            rejected_chunk_ids.push(chunk.chunk_id.clone());
                            continue;
                        }
                    })
                    .bind(hex_hash(expected_hash))
                    .fetch_one(&pool)
                    .await
                    {
                        Ok(row) => row.get::<i64, _>("seq"),
                        Err(_) => {
                            rejected_chunk_ids.push(chunk.chunk_id.clone());
                            continue;
                        }
                    };

                    if seq > max_seq {
                        max_seq = seq;
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

        let rows = sqlx::query_as::<_, HubChunkRow>(
            "SELECT seq, chunk_bytes
             FROM hub_memory_log
             WHERE workspace_id = $1 AND seq > $2
             ORDER BY seq ASC
             LIMIT $3",
        )
        .bind(workspace_uuid.to_string())
        .bind(since_seq)
        .bind(max_rows)
        .fetch_all(&self.pool)
        .await
        .map_err(database_status)?;

        let mut outbound_chunks = Vec::new();
        let mut latest_seq = since_seq;
        for row in rows {
            let chunk = DeltaChunk::decode(row.chunk_bytes.as_slice()).map_err(|error| {
                status_with_detail(
                    Code::Internal,
                    "storage",
                    &format!("failed to decode persisted chunk: {error}"),
                )
            })?;
            outbound_chunks.push(chunk);
            latest_seq = row.seq;
        }

        sqlx::query(
            "UPDATE hub_devices SET last_pull_cursor = GREATEST(last_pull_cursor, $1), last_seen = NOW() WHERE id = $2",
        )
        .bind(latest_seq)
        .bind(&cursor.device_id)
        .execute(&self.pool)
        .await
        .map_err(database_status)?;

        let has_more: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM hub_memory_log WHERE workspace_id = $1 AND seq > $2
            )",
        )
        .bind(workspace_uuid.to_string())
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

        sqlx::query("UPDATE hub_devices SET last_seen = NOW() WHERE id = $1")
            .bind(device_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(database_status)?;

        let row = sqlx::query(
            "SELECT COALESCE(last_pull_cursor, 0) AS last_pull_cursor
               FROM hub_devices WHERE id = $1",
        )
        .bind(device_id.to_string())
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
        .bind(
            Uuid::parse_str(&request.workspace_id)
                .map_err(|_| {
                    status_with_detail(Code::InvalidArgument, "validation", "invalid workspace_id")
                })?
                .to_string(),
        )
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

async fn ensure_hub_tables(pool: &PgPool) -> Result<(), sqlx::Error> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS hub_memory_log (
            seq BIGSERIAL PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            device_id TEXT NOT NULL,
            chunk_bytes BYTEA NOT NULL,
            payload_hash TEXT NOT NULL,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        "CREATE INDEX IF NOT EXISTS idx_hub_memory_log_workspace_seq
         ON hub_memory_log (workspace_id, seq)",
        "CREATE TABLE IF NOT EXISTS hub_devices (
            id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            public_key_ed25519 BYTEA NOT NULL,
            registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen TIMESTAMPTZ,
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

fn hex_hash(bytes: [u8; 32]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

fn database_status(error: sqlx::Error) -> Status {
    status_with_detail(
        Code::Internal,
        "database",
        &format!("database operation failed: {error}"),
    )
}

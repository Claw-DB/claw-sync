//! main.rs — entry point for the `claw-sync-server` standalone sync hub binary.

use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let addr = std::env::var("CLAW_LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".into())
        .parse()?;

    tracing::info!(%addr, "claw-sync-server starting");

    // Build and start the gRPC server.
    tonic::transport::Server::builder()
        .add_service(
            claw_sync::proto::clawsync::v1::sync_service_server::SyncServiceServer::new(HubService),
        )
        .serve(addr)
        .await?;

    Ok(())
}

/// Minimal hub service implementation; extend with production logic as needed.
struct HubService;

use claw_sync::proto::clawsync::v1::{
    sync_service_server::SyncService, HeartbeatRequest, HeartbeatResponse, PullRequest,
    PullResponse, PushRequest, PushResponse, ReconcileRequest, ReconcileResponse,
    RegisterDeviceRequest, RegisterDeviceResponse, ResolveConflictRequest, ResolveConflictResponse,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

#[tonic::async_trait]
impl SyncService for HubService {
    async fn register_device(
        &self,
        _request: Request<RegisterDeviceRequest>,
    ) -> Result<Response<RegisterDeviceResponse>, Status> {
        Ok(Response::new(RegisterDeviceResponse {
            accepted: true,
            assigned_device_id: uuid::Uuid::new_v4().to_string(),
            server_public_key: vec![],
        }))
    }

    type PushStream = ReceiverStream<Result<PushResponse, Status>>;

    async fn push(
        &self,
        _request: Request<Streaming<PushRequest>>,
    ) -> Result<Response<Self::PushStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(PushResponse {
                    accepted: true,
                    server_cursor: None,
                    rejected_chunk_ids: vec![],
                    reason: String::new(),
                }))
                .await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type PullStream = ReceiverStream<Result<PullResponse, Status>>;

    async fn pull(
        &self,
        _request: Request<PullRequest>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let (_tx, rx) = tokio::sync::mpsc::channel::<Result<PullResponse, Status>>(4);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn reconcile(
        &self,
        _request: Request<ReconcileRequest>,
    ) -> Result<Response<ReconcileResponse>, Status> {
        Ok(Response::new(ReconcileResponse {
            missing_on_client: vec![],
            missing_on_server: vec![],
            conflicted: vec![],
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
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let server_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        Ok(Response::new(HeartbeatResponse {
            server_time,
            sync_required: false,
        }))
    }
}

//! client.rs — tonic gRPC client for communicating with the claw-sync hub.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures::Stream;
use tokio::{sync::mpsc, time::timeout};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    metadata::AsciiMetadataValue,
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};
use uuid::Uuid;

use crate::{
    config::SyncConfig,
    crypto::{
        cipher::{decrypt_chunk, EncryptedBlob},
        keys::KeyStore,
        signing::sign,
    },
    delta::{hasher::content_hash, unpacker::chunks_from_proto},
    error::{SyncError, SyncResult},
    identity::DeviceIdentity,
    proto::clawsync::v1::{
        sync_service_client::SyncServiceClient, DeltaChunk, DeviceInfo, HeartbeatRequest,
        PullRequest, PullResponse, PushRequest, PushResponse, ReconcileRequest, ReconcileResponse,
        RegisterDeviceRequest, SyncCursor,
    },
    transport::reconnect::transport_retry,
};

/// Public alias used by callers that interact with a hub endpoint.
pub type SyncHubClient = SyncClient;

/// Public alias for transport chunk payloads.
pub type SyncChunk = DeltaChunk;

/// Transport error classification for retry decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportFailureKind {
    /// Retry may succeed because failure is likely temporary.
    Transient,
    /// Retry is not expected to succeed without intervention.
    Permanent,
}

/// Per-chunk push acknowledgement outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushChunkResult {
    /// Stable payload id of the chunk.
    pub chunk_id: String,
    /// Sequence number inside the payload.
    pub seq: i32,
    /// Whether the chunk was accepted by the hub.
    pub accepted: bool,
    /// Error kind for rejected chunks.
    pub failure_kind: Option<TransportFailureKind>,
    /// Optional structured failure reason.
    pub reason: Option<String>,
}

/// Aggregate push result over a sequence of chunks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PushResult {
    /// Ordered per-chunk outcomes.
    pub per_chunk: Vec<PushChunkResult>,
    /// Number of accepted chunks.
    pub accepted: u32,
    /// Number of transient failures.
    pub transient_failures: u32,
    /// Number of permanent failures.
    pub permanent_failures: u32,
}

/// Pull result for heartbeat probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatResult {
    /// Hub wall-clock time in unix milliseconds.
    pub server_time: i64,
    /// Whether the hub indicates a pull is required.
    pub pull_required: bool,
}

/// gRPC interceptor that injects bearer auth on every outbound request.
#[derive(Clone)]
pub struct AuthInterceptor {
    authorization: AsciiMetadataValue,
}

impl AuthInterceptor {
    /// Creates a new auth interceptor for the supplied device and signed token.
    pub fn new(device_id: Uuid, signed_token: String) -> SyncResult<Self> {
        let raw_value = format!("Bearer {device_id}:{signed_token}");
        let authorization = AsciiMetadataValue::try_from(raw_value).map_err(|error| {
            SyncError::Transport(format!("invalid authorization metadata value: {error}"))
        })?;
        Ok(Self { authorization })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        Ok(request)
    }
}

/// Wrapper around the generated tonic gRPC client with auth interception and config.
#[derive(Clone)]
pub struct SyncClient {
    inner: SyncServiceClient<InterceptedService<Channel, AuthInterceptor>>,
    config: Arc<SyncConfig>,
    key_store: Arc<KeyStore>,
}

impl SyncClient {
    /// Connects to the sync hub and returns a ready client.
    pub async fn connect(config: Arc<SyncConfig>, key_store: Arc<KeyStore>) -> SyncResult<Self> {
        let mut endpoint = Endpoint::from_shared(config.hub_endpoint.clone())
            .map_err(|error| SyncError::Transport(format!("invalid hub endpoint: {error}")))?
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
            .timeout(Duration::from_secs(config.request_timeout_secs));
        if config.tls_enabled {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new())
                .map_err(|error| {
                    SyncError::Transport(format!("invalid TLS configuration: {error}"))
                })?;
        }

        let channel = endpoint
            .connect()
            .await
            .map_err(|error| SyncError::Transport(format!("gRPC connect failed: {error}")))?;

        let auth_message = format!("{}:{}", config.device_id, config.workspace_id);
        let signed_token =
            STANDARD.encode(sign(auth_message.as_bytes(), key_store.signing_keypair()));
        let interceptor = AuthInterceptor::new(config.device_id, signed_token)?;
        let inner = SyncServiceClient::with_interceptor(channel, interceptor);

        Ok(Self {
            inner,
            config,
            key_store,
        })
    }

    /// Returns the configured retry limit for transport operations.
    pub fn max_retries(&self) -> u32 {
        self.config.max_retries
    }

    /// Returns the configured retry base in milliseconds.
    pub fn retry_base_ms(&self) -> u64 {
        self.config.retry_base_ms
    }

    /// Registers the local device with the hub.
    pub async fn register_device(&mut self, identity: &DeviceIdentity) -> SyncResult<()> {
        let challenge = format!("{}:{}", identity.device_id, self.config.workspace_id);
        let request = RegisterDeviceRequest {
            device: Some(DeviceInfo {
                device_id: identity.device_id.to_string(),
                workspace_id: self.config.workspace_id.to_string(),
                public_key_b64: STANDARD.encode(&identity.public_key_bytes),
                registered_at: chrono::Utc::now().timestamp_millis(),
                client_version: env!("CARGO_PKG_VERSION").to_owned(),
            }),
            signed_challenge: identity.sign_challenge(challenge.as_bytes())?,
        };

        let response = self.inner.register_device(request).await.map_err(|error| {
            SyncError::Transport(format!("device registration failed: {error}"))
        })?;
        if response.into_inner().accepted {
            Ok(())
        } else {
            Err(SyncError::Transport(
                "device registration was rejected by the hub".into(),
            ))
        }
    }

    /// Opens the bidirectional push stream.
    pub async fn push_stream<S>(
        &mut self,
        requests: S,
    ) -> SyncResult<tonic::Streaming<PushResponse>>
    where
        S: Stream<Item = PushRequest> + Send + 'static,
    {
        let response = self
            .inner
            .push(Request::new(requests))
            .await
            .map_err(|error| SyncError::Transport(format!("push stream failed: {error}")))?;
        Ok(response.into_inner())
    }

    /// Opens the server-streaming pull session.
    pub async fn pull(
        &mut self,
        cursor: SyncCursor,
        entity_types: Vec<crate::delta::extractor::EntityType>,
    ) -> SyncResult<tonic::Streaming<PullResponse>> {
        let request = PullRequest {
            client_cursor: Some(cursor),
            workspace_id: self.config.workspace_id.to_string(),
            entity_types: entity_types
                .into_iter()
                .map(|entity_type| entity_type.to_string())
                .collect(),
            max_chunks: self.config.max_pull_chunks as i32,
        };

        let response = self
            .inner
            .pull(request)
            .await
            .map_err(|error| SyncError::Transport(format!("pull request failed: {error}")))?;
        Ok(response.into_inner())
    }

    /// Executes a reconciliation round-trip.
    pub async fn reconcile(&mut self, req: ReconcileRequest) -> SyncResult<ReconcileResponse> {
        let response =
            self.inner.reconcile(req).await.map_err(|error| {
                SyncError::Transport(format!("reconcile request failed: {error}"))
            })?;
        Ok(response.into_inner())
    }

    /// Sends a heartbeat request and returns whether the hub requires a pull pass.
    pub async fn heartbeat(
        &mut self,
        device_id: Uuid,
        workspace_id: Uuid,
    ) -> SyncResult<HeartbeatResult> {
        let response = self
            .inner
            .heartbeat(HeartbeatRequest {
                device_id: device_id.to_string(),
                workspace_id: workspace_id.to_string(),
                timestamp: chrono::Utc::now().timestamp_millis(),
            })
            .await
            .map_err(|error| SyncError::Transport(format!("heartbeat failed: {error}")))?;
        let inner = response.into_inner();
        Ok(HeartbeatResult {
            server_time: inner.server_time,
            pull_required: inner.sync_required,
        })
    }

    /// Pushes chunks with retry-aware transient/permanent classification.
    pub async fn push_deltas(&mut self, chunks: Vec<SyncChunk>) -> SyncResult<PushResult> {
        let mut result = PushResult::default();
        if chunks.is_empty() {
            return Ok(result);
        }

        let mut pending = VecDeque::from(chunks);
        while let Some(chunk) = pending.pop_front() {
            let mut attempt = 0_u32;
            let mut chunk_result = PushChunkResult {
                chunk_id: chunk.chunk_id.clone(),
                seq: chunk.seq,
                accepted: false,
                failure_kind: None,
                reason: None,
            };

            loop {
                let request = PushRequest {
                    cursor: Some(SyncCursor {
                        workspace_id: self.config.workspace_id.to_string(),
                        device_id: self.config.device_id.to_string(),
                        lamport_clock: 0,
                        hlc_timestamp: String::new(),
                        state_hash: Vec::new(),
                    }),
                    chunks: vec![chunk.clone()],
                };

                let (tx, rx) = mpsc::channel(1);
                let push_stream = self.push_stream(ReceiverStream::new(rx)).await;
                let mut responses = match push_stream {
                    Ok(stream) => stream,
                    Err(error) => {
                        let kind = classify_error(&error);
                        if kind == TransportFailureKind::Transient
                            && attempt < self.config.max_retries
                        {
                            tokio::time::sleep(transport_retry(self.config.retry_base_ms, attempt))
                                .await;
                            attempt = attempt.saturating_add(1);
                            continue;
                        }
                        chunk_result.failure_kind = Some(kind);
                        chunk_result.reason = Some(error.to_string());
                        break;
                    }
                };

                if timeout(Duration::from_secs(5), tx.send(request))
                    .await
                    .is_err()
                {
                    chunk_result.failure_kind = Some(TransportFailureKind::Transient);
                    chunk_result.reason =
                        Some("timed out while enqueueing push request".to_owned());
                    if attempt < self.config.max_retries {
                        tokio::time::sleep(transport_retry(self.config.retry_base_ms, attempt))
                            .await;
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                    break;
                }
                drop(tx);

                match responses.message().await {
                    Ok(Some(ack)) => {
                        let accepted = ack.accepted
                            && (ack.rejected_chunk_ids.is_empty()
                                || !ack.rejected_chunk_ids.contains(&chunk.chunk_id));
                        if accepted {
                            chunk_result.accepted = true;
                            result.accepted = result.accepted.saturating_add(1);
                        } else {
                            chunk_result.failure_kind = Some(TransportFailureKind::Permanent);
                            chunk_result.reason = Some(if ack.reason.is_empty() {
                                "hub rejected pushed chunk".to_owned()
                            } else {
                                ack.reason
                            });
                        }
                        break;
                    }
                    Ok(None) => {
                        chunk_result.failure_kind = Some(TransportFailureKind::Transient);
                        chunk_result.reason =
                            Some("push stream ended before chunk acknowledgement".to_owned());
                        if attempt < self.config.max_retries {
                            tokio::time::sleep(transport_retry(self.config.retry_base_ms, attempt))
                                .await;
                            attempt = attempt.saturating_add(1);
                            continue;
                        }
                        break;
                    }
                    Err(status) => {
                        let kind = classify_status(&status);
                        if kind == TransportFailureKind::Transient
                            && attempt < self.config.max_retries
                        {
                            tokio::time::sleep(transport_retry(self.config.retry_base_ms, attempt))
                                .await;
                            attempt = attempt.saturating_add(1);
                            continue;
                        }
                        chunk_result.failure_kind = Some(kind);
                        chunk_result.reason = Some(status.message().to_owned());
                        break;
                    }
                }
            }

            match chunk_result.failure_kind {
                Some(TransportFailureKind::Transient) => {
                    result.transient_failures = result.transient_failures.saturating_add(1)
                }
                Some(TransportFailureKind::Permanent) => {
                    result.permanent_failures = result.permanent_failures.saturating_add(1)
                }
                None => {}
            }
            result.per_chunk.push(chunk_result);
        }

        Ok(result)
    }

    /// Pulls chunks since `since_seq`, verifying per-chunk hash and decryption.
    pub async fn pull_deltas(
        &mut self,
        since_seq: u64,
        max_chunks: u32,
    ) -> SyncResult<Vec<SyncChunk>> {
        let mut stream = self
            .pull(
                SyncCursor {
                    workspace_id: self.config.workspace_id.to_string(),
                    device_id: self.config.device_id.to_string(),
                    lamport_clock: since_seq as i64,
                    hlc_timestamp: String::new(),
                    state_hash: Vec::new(),
                },
                crate::delta::extractor::EntityType::all().to_vec(),
            )
            .await?;

        let mut chunks = Vec::new();
        while let Some(response) = stream
            .message()
            .await
            .map_err(|error| SyncError::Transport(format!("pull stream failed: {error}")))?
        {
            chunks.extend(response.chunks);
            if chunks.len() as u32 >= max_chunks {
                break;
            }
        }
        chunks.truncate(max_chunks as usize);

        for chunk in &chunks {
            let payload_hash: [u8; 32] =
                chunk.payload_hash.as_slice().try_into().map_err(|_| {
                    SyncError::Validation("chunk payload hash must be 32 bytes".into())
                })?;
            let computed = content_hash(&chunk.encrypted_payload);
            if payload_hash != computed {
                return Err(SyncError::IntegrityError(format!(
                    "payload hash verification failed for chunk {}:{}",
                    chunk.chunk_id, chunk.seq
                )));
            }

            let blob = EncryptedBlob::from_bytes(&chunk.encrypted_payload)?;
            let _ = decrypt_chunk(&blob, self.key_store.workspace_key(), &chunk.chunk_id)?;
        }

        if !chunks.is_empty() {
            let _ = chunks_from_proto(chunks.clone())?;
        }
        Ok(chunks)
    }
}

fn classify_error(error: &SyncError) -> TransportFailureKind {
    match error {
        SyncError::Transport(message)
            if message.contains("unavailable")
                || message.contains("deadline")
                || message.contains("timeout")
                || message.contains("resource exhausted") =>
        {
            TransportFailureKind::Transient
        }
        SyncError::Transport(_) => TransportFailureKind::Permanent,
        _ => TransportFailureKind::Permanent,
    }
}

fn classify_status(status: &Status) -> TransportFailureKind {
    use tonic::Code;

    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted => {
            TransportFailureKind::Transient
        }
        _ => TransportFailureKind::Permanent,
    }
}

#[cfg(test)]
mod tests {
    use tonic::service::Interceptor;
    use tonic::Request;
    use uuid::Uuid;

    use super::AuthInterceptor;

    #[test]
    fn auth_interceptor_inserts_authorization_header() {
        let mut interceptor = AuthInterceptor::new(Uuid::from_bytes([1; 16]), "token".into())
            .expect("interceptor should be created");
        let request = interceptor
            .call(Request::new(()))
            .expect("request interception should succeed");

        let header = request
            .metadata()
            .get("authorization")
            .expect("authorization header should be present");
        assert_eq!(
            header.to_str().expect("metadata should be valid ASCII"),
            "Bearer 01010101-0101-0101-0101-010101010101:token"
        );
    }
}

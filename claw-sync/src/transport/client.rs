//! client.rs — tonic gRPC client for communicating with the claw-sync hub.

use std::{sync::Arc, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures::Stream;
use tonic::{
    metadata::AsciiMetadataValue,
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};
use uuid::Uuid;

use crate::{
    config::SyncConfig,
    crypto::{keys::KeyStore, signing::sign},
    error::{SyncError, SyncResult},
    identity::DeviceIdentity,
    proto::clawsync::v1::{
        sync_service_client::SyncServiceClient, DeviceInfo, HeartbeatRequest, PullRequest,
        PullResponse, PushRequest, PushResponse, ReconcileRequest, ReconcileResponse,
        RegisterDeviceRequest, SyncCursor,
    },
};

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

        Ok(Self { inner, config })
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
    pub async fn heartbeat(&mut self, device_id: Uuid, workspace_id: Uuid) -> SyncResult<bool> {
        let response = self
            .inner
            .heartbeat(HeartbeatRequest {
                device_id: device_id.to_string(),
                workspace_id: workspace_id.to_string(),
                timestamp: chrono::Utc::now().timestamp_millis(),
            })
            .await
            .map_err(|error| SyncError::Transport(format!("heartbeat failed: {error}")))?;
        Ok(response.into_inner().sync_required)
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

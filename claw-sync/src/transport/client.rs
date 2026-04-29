//! client.rs — tonic gRPC client for communicating with the claw-sync hub.

use tonic::transport::Channel;

use crate::{
    error::{SyncError, SyncResult},
    proto::clawsync::v1::sync_service_client::SyncServiceClient,
};

/// Wrapper around the generated tonic gRPC client with a connected `Channel`.
pub struct SyncClient {
    inner: SyncServiceClient<Channel>,
}

impl SyncClient {
    /// Connect to the sync hub at `endpoint` and return a ready `SyncClient`.
    pub async fn connect(endpoint: &str) -> SyncResult<Self> {
        let channel = Channel::from_shared(endpoint.to_owned())
            .map_err(|e| SyncError::Transport(e.to_string()))?
            .connect()
            .await
            .map_err(|e| SyncError::Transport(e.to_string()))?;
        Ok(Self { inner: SyncServiceClient::new(channel) })
    }

    /// Return a mutable reference to the underlying generated gRPC client.
    pub fn inner_mut(&mut self) -> &mut SyncServiceClient<Channel> {
        &mut self.inner
    }
}

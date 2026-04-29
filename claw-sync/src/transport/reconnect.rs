//! reconnect.rs — connection manager with automatic reconnection and exponential back-off.

use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::time::{sleep, Duration};

use crate::{
    error::{SyncError, SyncResult},
    transport::client::SyncClient,
};

/// Manages a lazily-reconnected gRPC connection to the sync hub.
pub struct ReconnectManager {
    endpoint: String,
    #[allow(dead_code)]
    client: Arc<ArcSwap<Option<SyncClient>>>,
}

impl ReconnectManager {
    /// Create a new `ReconnectManager` targeting the specified hub endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: Arc::new(ArcSwap::from_pointee(None)),
        }
    }

    /// Obtain a fresh gRPC client, retrying with back-off on transient failures.
    pub async fn connect_with_retry(&self, max_attempts: u32) -> SyncResult<SyncClient> {
        let mut delay = Duration::from_millis(200);
        for attempt in 1..=max_attempts {
            match SyncClient::connect(&self.endpoint).await {
                Ok(c) => return Ok(c),
                Err(e) if attempt == max_attempts => return Err(e),
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "reconnect attempt failed");
                    sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
        Err(SyncError::Transport("max reconnect attempts exceeded".into()))
    }
}

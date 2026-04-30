//! reconnect.rs — connection manager with automatic reconnection and heartbeat signaling.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::{
    sync::{broadcast, RwLock},
    time::{self, Duration},
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::SyncConfig, crypto::keys::KeyStore, error::SyncResult, transport::client::SyncClient,
};

/// Transport-level events emitted by the reconnecting client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncEvent {
    /// The hub asked the client to start a pull pass.
    PullRequired,
    /// A client connection was established.
    Connected,
    /// The cached client was dropped after a transport failure.
    Disconnected,
}

/// Lazily reconnecting gRPC client manager with heartbeat loop support.
#[derive(Clone)]
pub struct ReconnectingClient {
    /// Shared runtime configuration.
    pub config: Arc<SyncConfig>,
    /// Signing and encryption material used for connection setup.
    pub key_store: Arc<KeyStore>,
    /// Cached connected client.
    pub inner: Arc<RwLock<Option<SyncClient>>>,
    connected: Arc<AtomicBool>,
    events: broadcast::Sender<SyncEvent>,
}

impl ReconnectingClient {
    /// Creates a new reconnecting client wrapper.
    pub fn new(config: Arc<SyncConfig>, key_store: Arc<KeyStore>) -> Self {
        let (events, _) = broadcast::channel(32);
        Self {
            config,
            key_store,
            inner: Arc::new(RwLock::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            events,
        }
    }

    /// Subscribes to reconnect-client events.
    pub fn subscribe(&self) -> broadcast::Receiver<SyncEvent> {
        self.events.subscribe()
    }

    /// Returns a connected client, creating or recreating it on demand.
    pub async fn get_or_connect(&self) -> SyncResult<SyncClient> {
        if let Some(client) = self.inner.read().await.clone() {
            return Ok(client);
        }

        let client = SyncClient::connect(self.config.clone(), self.key_store.clone()).await?;
        *self.inner.write().await = Some(client.clone());
        if !self.connected.swap(true, Ordering::SeqCst) {
            let _ = self.events.send(SyncEvent::Connected);
        }
        Ok(client)
    }

    /// Drops the cached client and marks the connection as unavailable.
    pub async fn reset(&self) {
        *self.inner.write().await = None;
        if self.connected.swap(false, Ordering::SeqCst) {
            let _ = self.events.send(SyncEvent::Disconnected);
        }
    }

    /// Returns whether a connected client is currently cached.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// Runs the periodic heartbeat loop until cancelled.
    pub async fn run_heartbeat_loop(&self, interval_secs: u64, shutdown: CancellationToken) {
        let mut ticker = time::interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = ticker.tick() => {
                    match self.get_or_connect().await {
                        Ok(mut client) => {
                            match client.heartbeat(self.config.device_id, self.config.workspace_id).await {
                                Ok(sync_required) => {
                                    if !self.connected.swap(true, Ordering::SeqCst) {
                                        let _ = self.events.send(SyncEvent::Connected);
                                    }
                                    if sync_required {
                                        let _ = self.events.send(SyncEvent::PullRequired);
                                    }
                                }
                                Err(error) => {
                                    tracing::warn!(error = %error, "heartbeat failed; resetting transport client");
                                    self.reset().await;
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "heartbeat connect failed");
                            self.reset().await;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use super::{ReconnectingClient, SyncEvent};
    use crate::{config::SyncConfig, crypto::keys::KeyStore};

    #[test]
    fn subscribe_receives_event_enum_type() {
        let config = Arc::new(SyncConfig {
            workspace_id: uuid::Uuid::from_bytes([1; 16]),
            device_id: uuid::Uuid::from_bytes([2; 16]),
            hub_endpoint: "http://localhost:50051".into(),
            data_dir: std::path::PathBuf::from(".claw-sync"),
            db_path: std::path::PathBuf::from("claw_sync.db"),
            tls_enabled: false,
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            sync_interval_secs: 30,
            heartbeat_interval_secs: 15,
            max_retries: 5,
            retry_base_ms: 500,
            max_delta_rows: 1_000,
            max_chunk_bytes: 64 * 1024,
            max_pull_chunks: 128,
            max_push_inflight: 4,
        });
        let key_store = Arc::new(
            KeyStore::load_or_create(Path::new("/tmp/claw-sync-test-keystore"), b"test-material")
                .expect("keystore should load"),
        );
        let client = ReconnectingClient::new(config, key_store);
        let mut receiver = client.subscribe();
        let _ = client.events.send(SyncEvent::PullRequired);

        let event = receiver.try_recv().expect("event should be available");
        assert_eq!(event, SyncEvent::PullRequired);
    }
}

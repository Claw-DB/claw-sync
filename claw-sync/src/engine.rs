//! engine.rs — `SyncEngine`: top-level coordinator and lifecycle manager for claw-sync.

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Instant,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    SqlitePool,
};
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinHandle,
    time::{self, Duration},
};
use tokio_util::sync::CancellationToken;

use crate::{
    audit::{
        log::{AuditEvent, AuditEventType, AuditLog},
        verifier::{verify_chain, VerifyResult},
    },
    config::SyncConfig,
    crypto::keys::KeyStore,
    delta::extractor::{DeltaExtractor, EntityType},
    error::{SyncError, SyncResult},
    identity::device::DeviceIdentity,
    proto::clawsync::v1::ConflictRecord,
    queue::{
        drain::{DrainResult, QueueDrainer, QueueReplay},
        offline::OfflineQueue,
        scheduler::RetryScheduler,
    },
    sync::{
        apply::DeltaApplier,
        checkpoint::{atomic_write_json, CheckpointStore, CheckpointedState},
        pull::{PullOutcome, PullSession, PullStats},
        push::{PushOutcome, PushSession, PushStats},
        reconcile::{ReconcileSession, ReconcileStats},
    },
    transport::reconnect::{ReconnectingClient, SyncEvent as TransportEvent},
};

const REGISTRATION_FILE_NAME: &str = "device_registration.json";

/// Public engine events emitted during background and explicit sync activity.
#[derive(Debug, Clone)]
pub enum SyncEvent {
    /// A push pass completed successfully.
    PushCompleted(PushStats),
    /// A pull pass completed successfully.
    PullCompleted(PullStats),
    /// A conflict requires manual resolution.
    ConflictDetected(ConflictRecord),
    /// Pending operations were persisted to the offline queue.
    OfflineQueued(u32),
    /// An engine-visible error occurred.
    SyncError(String),
    /// The transport connected to the remote hub.
    Connected,
    /// The transport disconnected from the remote hub.
    Disconnected,
    /// A reconciliation pass completed successfully.
    ReconcileCompleted(ReconcileStats),
}

/// Snapshot of engine status suitable for UI display and health checks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncStatus {
    /// `true` when the gRPC client is currently connected.
    pub connected: bool,
    /// `true` when the device is registered with the hub.
    pub registered: bool,
    /// Last successful push statistics.
    pub last_push: Option<PushStats>,
    /// Last successful pull statistics.
    pub last_pull: Option<PullStats>,
    /// Last successful reconcile statistics.
    pub last_reconcile: Option<ReconcileStats>,
    /// Last persisted cursor.
    pub last_cursor: Option<crate::proto::clawsync::v1::SyncCursor>,
    /// Last recorded error message.
    pub last_error: Option<String>,
    /// Last time a sync-affecting operation completed.
    pub last_synced_at: Option<DateTime<Utc>>,
    /// Number of pending queued operations.
    pub queued_ops: u64,
    /// Duration of the last full `sync_now` round.
    pub last_round_duration_ms: Option<u64>,
}

/// Result of a push-then-pull sync round.
#[derive(Debug, Clone)]
pub struct SyncRoundResult {
    /// Push statistics for the round.
    pub push: PushStats,
    /// Pull statistics for the round.
    pub pull: PullStats,
    /// Cursor persisted after the round.
    pub cursor: crate::proto::clawsync::v1::SyncCursor,
    /// End-to-end duration in milliseconds.
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegistrationState {
    workspace_id: uuid::Uuid,
    device_id: uuid::Uuid,
    registered_at: DateTime<Utc>,
}

/// Top-level coordinator that starts and manages all sync subsystems.
#[derive(Clone)]
pub struct SyncEngine {
    /// Shared runtime configuration.
    pub config: Arc<SyncConfig>,
    /// Signing and encryption material.
    pub key_store: Arc<KeyStore>,
    /// Stable device identity.
    pub identity: Arc<DeviceIdentity>,
    /// Persistent offline operation queue.
    pub queue: Arc<OfflineQueue>,
    /// Background queue drainer.
    pub drainer: Arc<QueueDrainer>,
    /// Reconnecting gRPC client wrapper.
    pub client: Arc<ReconnectingClient>,
    /// Local delta extractor.
    pub extractor: Arc<DeltaExtractor>,
    /// Remote delta applier.
    pub applier: Arc<DeltaApplier>,
    /// Persistent sync checkpoint store.
    pub checkpoint: Arc<CheckpointStore>,
    /// Append-only signed audit log.
    pub audit: Arc<AuditLog>,
    /// Shared live status snapshot.
    pub status: Arc<RwLock<SyncStatus>>,
    /// Broadcast channel for engine events.
    pub event_tx: broadcast::Sender<SyncEvent>,
    core_pool: SqlitePool,
    push_session: Arc<PushSession>,
    pull_session: Arc<PullSession>,
    reconcile_session: Arc<ReconcileSession>,
    shutdown: CancellationToken,
    started: Arc<AtomicBool>,
    registered: Arc<AtomicBool>,
    handles: Arc<Mutex<Vec<JoinHandle<SyncResult<()>>>>>,
}

impl SyncEngine {
    /// Builds an engine from environment variables.
    pub async fn from_env() -> SyncResult<Self> {
        let config = SyncConfig::from_env()?;
        let pool = connect_pool(&config).await?;
        Self::new(config, pool).await
    }

    /// Builds an engine from environment variables while overriding the workspace id.
    pub async fn from_env_for_workspace(workspace_id: &str) -> SyncResult<Self> {
        let mut config = SyncConfig::from_env()?;
        config.workspace_id = uuid::Uuid::parse_str(workspace_id)
            .map_err(|error| SyncError::Config(format!("workspace_id must be a UUID: {error}")))?;
        let pool = connect_pool(&config).await?;
        Self::new(config, pool).await
    }

    /// Creates a new engine from the supplied configuration and core database pool.
    pub async fn new(config: SyncConfig, core_pool: SqlitePool) -> SyncResult<Self> {
        tokio::fs::create_dir_all(&config.data_dir).await?;

        let config = Arc::new(config);
        let key_store = Arc::new(KeyStore::load_or_create(
            &config.data_dir,
            config.workspace_id.as_bytes(),
        )?);
        let identity = Arc::new(DeviceIdentity::load_or_create(
            &config.data_dir,
            config.device_id,
            &key_store,
        )?);
        let checkpoint = Arc::new(CheckpointStore::new(&config.data_dir)?);
        let audit = Arc::new(AuditLog::new(&config.data_dir, key_store.clone()).await?);
        let extractor = Arc::new(
            DeltaExtractor::new(core_pool.clone(), config.device_id)
                .with_batch_limit(config.max_delta_rows),
        );
        extractor.install_changelog_trigger().await?;

        let queue = Arc::new(OfflineQueue::new(&config.data_dir).await?);
        let client = Arc::new(ReconnectingClient::new(config.clone(), key_store.clone()));
        let applier = Arc::new(DeltaApplier::new(
            core_pool.clone(),
            Arc::default(),
            audit.clone(),
            checkpoint.clone(),
            config.workspace_id,
            config.device_id,
        ));
        let push_session = Arc::new(PushSession::new(
            config.clone(),
            extractor.as_ref().clone(),
            queue.as_ref().clone(),
            client.as_ref().clone(),
            key_store.clone(),
            checkpoint.clone(),
        ));
        let pull_session = Arc::new(PullSession::new(
            config.clone(),
            client.as_ref().clone(),
            key_store.clone(),
            applier.clone(),
            checkpoint.clone(),
        ));
        let reconcile_session = Arc::new(ReconcileSession::new(
            config.clone(),
            core_pool.clone(),
            client.as_ref().clone(),
        ));

        let shutdown = CancellationToken::new();
        let drainer = Arc::new(QueueDrainer::new(
            queue.as_ref().clone(),
            RetryScheduler::new(config.clone()),
            push_session.clone() as Arc<dyn QueueReplay>,
            shutdown.child_token(),
            config.max_push_inflight as u32,
        ));

        let (event_tx, _) = broadcast::channel(128);
        let last_cursor = checkpoint
            .load(config.workspace_id, config.device_id)
            .await?
            .or(Some(extractor.cursor_for_device().await?));
        let status = Arc::new(RwLock::new(SyncStatus {
            connected: client.is_connected(),
            registered: registration_path(&config.data_dir).exists(),
            last_push: None,
            last_pull: None,
            last_reconcile: None,
            last_cursor,
            last_error: None,
            last_synced_at: None,
            queued_ops: queue.pending_count().await?,
            last_round_duration_ms: None,
        }));

        let engine = Self {
            config,
            key_store,
            identity,
            queue,
            drainer,
            client,
            extractor,
            applier,
            checkpoint,
            audit,
            status,
            event_tx,
            core_pool,
            push_session,
            pull_session,
            reconcile_session,
            shutdown,
            started: Arc::new(AtomicBool::new(false)),
            registered: Arc::new(AtomicBool::new(false)),
            handles: Arc::new(Mutex::new(Vec::new())),
        };

        if registration_path(&engine.config.data_dir).exists() {
            engine.registered.store(true, Ordering::SeqCst);
            engine.with_status_mut(|current| current.registered = true);
        } else if let Err(error) = engine.ensure_registered().await {
            tracing::warn!(error = %error, "device registration failed during initialisation; continuing offline");
            engine.publish_error(error.to_string());
        }

        Ok(engine)
    }

    /// Starts background queue draining, heartbeat, event bridging, and periodic sync.
    pub async fn start(&self, external_shutdown: CancellationToken) -> SyncResult<()> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        if let Err(error) = self.ensure_registered().await {
            tracing::warn!(error = %error, "device registration failed during start; continuing offline");
            self.publish_error(error.to_string());
        }

        let mut handles = self.handles.lock().await;
        handles.push(self.drainer.clone().start());

        let heartbeat_engine = self.clone();
        handles.push(tokio::spawn(async move {
            heartbeat_engine
                .client
                .run_heartbeat_loop(
                    heartbeat_engine.config.heartbeat_interval_secs,
                    heartbeat_engine.shutdown.child_token(),
                )
                .await;
            Ok(())
        }));

        let bridge_engine = self.clone();
        handles.push(tokio::spawn(async move {
            let mut events = bridge_engine.client.subscribe();
            loop {
                tokio::select! {
                    _ = bridge_engine.shutdown.cancelled() => break,
                    event = events.recv() => {
                        match event {
                            Ok(TransportEvent::PullRequired) => {
                                if let Err(error) = bridge_engine.pull_now().await {
                                    bridge_engine.publish_error(error.to_string());
                                }
                            }
                            Ok(TransportEvent::Connected) => bridge_engine.mark_connected(true),
                            Ok(TransportEvent::Disconnected) => bridge_engine.mark_connected(false),
                            Err(error) => tracing::debug!(error = %error, "transport event receiver lagged or closed"),
                        }
                    }
                }
            }
            Ok(())
        }));

        let periodic_engine = self.clone();
        handles.push(tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_secs(
                periodic_engine.config.sync_interval_secs.max(1),
            ));
            loop {
                tokio::select! {
                    _ = periodic_engine.shutdown.cancelled() => break,
                    _ = ticker.tick() => {
                        if let Err(error) = periodic_engine.sync_now().await {
                            periodic_engine.publish_error(error.to_string());
                        }
                    }
                }
            }
            Ok(())
        }));

        let audit_verify_engine = self.clone();
        handles.push(tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_secs(6 * 60 * 60));
            loop {
                tokio::select! {
                    _ = audit_verify_engine.shutdown.cancelled() => break,
                    _ = ticker.tick() => {
                        match audit_verify_engine.verify_audit_log().await {
                            Ok(result) if !result.ok => {
                                tracing::warn!(
                                    first_broken_at = ?result.first_broken_at,
                                    reason = ?result.broken_reason,
                                    "audit verification detected chain break"
                                );
                            }
                            Ok(_) => {}
                            Err(error) => tracing::warn!(error = %error, "audit verification task failed"),
                        }
                    }
                }
            }
            Ok(())
        }));

        let shutdown_engine = self.clone();
        handles.push(tokio::spawn(async move {
            tokio::select! {
                _ = external_shutdown.cancelled() => shutdown_engine.shutdown.cancel(),
                _ = shutdown_engine.shutdown.cancelled() => {}
            }
            Ok(())
        }));

        Ok(())
    }

    /// Executes a single outbound push pass immediately.
    pub async fn push_now(&self) -> SyncResult<PushStats> {
        self.ensure_registered().await?;
        let scheduler = RetryScheduler::new(self.config.clone());
        let mut retries = 0_u32;

        loop {
            match self.push_session.push_pending_detailed().await {
                Ok(mut outcome) => {
                    outcome.stats.retries = retries;
                    self.persist_checkpoint_state(&outcome.cursor, Some(outcome.stats), None)
                        .await?;
                    self.record_audit(
                        AuditEventType::Push,
                        outcome.entity_type,
                        outcome.entity_ids.clone(),
                        outcome.stats.ops_sent,
                        serde_json::json!({
                            "deltas_sent": outcome.stats.deltas_sent,
                            "ops_sent": outcome.stats.ops_sent,
                            "chunks_sent": outcome.stats.chunks_sent,
                            "sequences_synced": outcome.stats.sequences_synced,
                            "retries": outcome.stats.retries,
                        }),
                    )
                    .await?;
                    self.update_status_after_push(&outcome).await?;
                    let _ = self.event_tx.send(SyncEvent::PushCompleted(outcome.stats));
                    return Ok(outcome.stats);
                }
                Err(error)
                    if matches!(error, SyncError::Transport(_))
                        && scheduler.should_retry(retries + 1) =>
                {
                    let delay = scheduler.next_retry_delay(retries);
                    retries = retries.saturating_add(1);
                    tokio::time::sleep(delay).await;
                }
                Err(error) => {
                    if matches!(error, SyncError::Transport(_)) {
                        let queued = self.enqueue_pending_deltas().await?;
                        if queued > 0 {
                            self.update_queue_depth().await?;
                            let _ = self.event_tx.send(SyncEvent::OfflineQueued(queued));
                        }
                    }
                    self.publish_error(error.to_string());
                    return Err(error);
                }
            }
        }
    }

    /// Executes a single inbound pull pass immediately.
    pub async fn pull_now(&self) -> SyncResult<PullStats> {
        self.ensure_registered().await?;

        match self
            .pull_session
            .pull_pending_detailed(&EntityType::all())
            .await
        {
            Ok(outcome) => {
                self.persist_checkpoint_state(&outcome.cursor, None, Some(outcome.stats))
                    .await?;
                self.record_audit(
                    AuditEventType::Pull,
                    outcome.entity_type,
                    outcome.entity_ids.clone(),
                    outcome.stats.ops_applied,
                    serde_json::json!({
                        "deltas_received": outcome.stats.deltas_received,
                        "chunks_received": outcome.stats.chunks_received,
                        "ops_applied": outcome.stats.ops_applied,
                        "ops_skipped": outcome.stats.ops_skipped,
                    }),
                )
                .await?;
                self.update_status_after_pull(&outcome).await?;
                let _ = self.event_tx.send(SyncEvent::PullCompleted(outcome.stats));
                Ok(outcome.stats)
            }
            Err(SyncError::ConflictEscalation {
                entity_id: _,
                record,
            }) => {
                let record_for_event = (*record).clone();
                let _ = self
                    .event_tx
                    .send(SyncEvent::ConflictDetected(record_for_event));
                self.publish_error("manual conflict escalation required".to_owned());
                Err(SyncError::ConflictEscalation {
                    entity_id: record.entity_id.clone(),
                    record,
                })
            }
            Err(error) => {
                self.publish_error(error.to_string());
                Err(error)
            }
        }
    }

    /// Executes a full reconciliation pass and triggers follow-up push/pull work as needed.
    pub async fn reconcile(&self) -> SyncResult<ReconcileStats> {
        self.ensure_registered().await?;
        let mut stats = self.reconcile_session.reconcile().await?;

        if stats.missing_on_server > 0 || stats.conflicted > 0 {
            let push = self.push_now().await?;
            stats.pushed = push.ops_sent;
            stats.push = Some(push);
        }
        if stats.missing_on_client > 0 || stats.conflicted > 0 {
            let pull = self.pull_now().await?;
            stats.pulled = pull.ops_applied;
            stats.pull = Some(pull);
        }

        self.record_audit(
            AuditEventType::Reconcile,
            None,
            Vec::new(),
            stats.pulled.saturating_add(stats.pushed),
            serde_json::json!({
                "local_hashes": stats.local_hashes,
                "missing_on_client": stats.missing_on_client,
                "missing_on_server": stats.missing_on_server,
                "conflicted": stats.conflicted,
                "pushed": stats.pushed,
                "pulled": stats.pulled,
            }),
        )
        .await?;

        self.with_status_mut(|status| {
            status.last_reconcile = Some(stats);
            status.last_synced_at = Some(Utc::now());
        });
        let _ = self.event_tx.send(SyncEvent::ReconcileCompleted(stats));
        Ok(stats)
    }

    /// Executes push then pull and returns aggregate round information.
    pub async fn sync_now(&self) -> SyncResult<SyncRoundResult> {
        let started_at = Instant::now();
        let push = self.push_now().await?;
        let pull = self.pull_now().await?;
        let cursor = self.current_cursor().await?;
        let duration_ms = started_at.elapsed().as_millis() as u64;

        self.with_status_mut(|status| {
            status.last_cursor = Some(cursor.clone());
            status.last_round_duration_ms = Some(duration_ms);
            status.last_synced_at = Some(Utc::now());
        });

        Ok(SyncRoundResult {
            push,
            pull,
            cursor,
            duration_ms,
        })
    }

    /// Returns a point-in-time status snapshot.
    pub fn status(&self) -> SyncStatus {
        read_lock(&self.status).clone()
    }

    /// Subscribes to engine events.
    pub fn subscribe(&self) -> broadcast::Receiver<SyncEvent> {
        self.event_tx.subscribe()
    }

    /// Returns whether the transport currently has a cached connected client.
    pub fn is_connected(&self) -> bool {
        self.client.is_connected()
    }

    /// Verifies the full audit chain in sequence order.
    pub async fn verify_audit_log(&self) -> SyncResult<VerifyResult> {
        verify_chain(&self.audit.pool, &self.key_store).await
    }

    /// Drains a single batch from the offline queue.
    pub async fn drain_queue_once(&self) -> SyncResult<DrainResult> {
        let result = self.drainer.drain_once().await?;
        self.update_queue_depth().await?;
        Ok(result)
    }

    /// Compatibility alias for the old API.
    pub async fn push_once(&self) -> SyncResult<PushStats> {
        self.push_now().await
    }

    /// Compatibility alias for the old API.
    pub async fn pull_once(&self) -> SyncResult<PullStats> {
        self.pull_now().await
    }

    /// Compatibility alias for the old API.
    pub async fn reconcile_once(&self) -> SyncResult<ReconcileStats> {
        self.reconcile().await
    }

    /// Compatibility runner for the CLI entry point.
    pub async fn run(&self) -> SyncResult<()> {
        let shutdown = CancellationToken::new();
        self.start(shutdown.clone()).await?;
        tokio::signal::ctrl_c().await?;
        shutdown.cancel();
        self.close().await
    }

    /// Gracefully stops background work, drains the queue, persists the latest checkpoint, and resets the transport.
    pub async fn close(&self) -> SyncResult<()> {
        self.shutdown.cancel();

        let handles = {
            let mut guard = self.handles.lock().await;
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "background engine task exited with error")
                }
                Err(error) => tracing::warn!(error = %error, "background engine task join failed"),
            }
        }

        if self.is_connected() {
            loop {
                let result = self.drainer.drain_once().await?;
                if result.remaining_pending == 0 || result.delivered == 0 {
                    break;
                }
            }
        }

        let cursor = self.current_cursor().await?;
        let status = self.status();
        self.checkpoint
            .save_state(&CheckpointedState {
                cursor: cursor.clone(),
                saved_at: Utc::now(),
                push_stats: status.last_push,
                pull_stats: status.last_pull,
            })
            .await?;

        self.client.reset().await;
        self.mark_connected(false);
        self.started.store(false, Ordering::SeqCst);
        self.with_status_mut(|current| {
            current.last_cursor = Some(cursor);
            current.last_synced_at = Some(Utc::now());
        });
        Ok(())
    }

    /// Returns the local SQLite pool used by the engine.
    pub fn pool(&self) -> &SqlitePool {
        &self.core_pool
    }

    async fn ensure_registered(&self) -> SyncResult<()> {
        if self.registered.load(Ordering::SeqCst) {
            return Ok(());
        }

        let marker_path = registration_path(&self.config.data_dir);
        if tokio::fs::try_exists(&marker_path).await? {
            self.registered.store(true, Ordering::SeqCst);
            self.with_status_mut(|status| status.registered = true);
            return Ok(());
        }

        let mut client = self.client.get_or_connect().await?;
        client.register_device(&self.identity).await?;

        let registration = RegistrationState {
            workspace_id: self.config.workspace_id,
            device_id: self.config.device_id,
            registered_at: Utc::now(),
        };
        let marker_path_clone = marker_path.clone();
        tokio::task::spawn_blocking(move || atomic_write_json(&marker_path_clone, &registration))
            .await
            .map_err(|error| SyncError::Io(std::io::Error::other(error.to_string())))??;

        self.registered.store(true, Ordering::SeqCst);
        self.with_status_mut(|status| status.registered = true);
        self.record_audit(
            AuditEventType::DeviceRegistered,
            None,
            Vec::new(),
            0,
            serde_json::json!({
                "device_id": self.config.device_id,
                "workspace_id": self.config.workspace_id,
            }),
        )
        .await?;
        Ok(())
    }

    async fn persist_checkpoint_state(
        &self,
        cursor: &crate::proto::clawsync::v1::SyncCursor,
        push_stats: Option<PushStats>,
        pull_stats: Option<PullStats>,
    ) -> SyncResult<()> {
        self.checkpoint
            .save_state(&CheckpointedState {
                cursor: cursor.clone(),
                saved_at: Utc::now(),
                push_stats,
                pull_stats,
            })
            .await
    }

    async fn update_status_after_push(&self, outcome: &PushOutcome) -> SyncResult<()> {
        let queued_ops = self.queue.pending_count().await?;
        self.with_status_mut(|status| {
            status.last_push = Some(outcome.stats);
            status.last_cursor = Some(outcome.cursor.clone());
            status.last_error = None;
            status.last_synced_at = Some(Utc::now());
            status.queued_ops = queued_ops;
        });
        Ok(())
    }

    async fn update_status_after_pull(&self, outcome: &PullOutcome) -> SyncResult<()> {
        let queued_ops = self.queue.pending_count().await?;
        self.with_status_mut(|status| {
            status.last_pull = Some(outcome.stats);
            status.last_cursor = Some(outcome.cursor.clone());
            status.last_error = None;
            status.last_synced_at = Some(Utc::now());
            status.queued_ops = queued_ops;
        });
        Ok(())
    }

    async fn update_queue_depth(&self) -> SyncResult<()> {
        let queued_ops = self.queue.pending_count().await?;
        self.with_status_mut(|status| status.queued_ops = queued_ops);
        Ok(())
    }

    async fn enqueue_pending_deltas(&self) -> SyncResult<u32> {
        let deltas = self.extractor.extract_pending(&EntityType::all()).await?;
        let mut queued = 0_u32;
        for delta in deltas {
            queued = queued.saturating_add(self.push_session.enqueue_delta_set(&delta).await?);
        }
        Ok(queued)
    }

    async fn current_cursor(&self) -> SyncResult<crate::proto::clawsync::v1::SyncCursor> {
        if let Some(cursor) = self
            .checkpoint
            .load(self.config.workspace_id, self.config.device_id)
            .await?
        {
            return Ok(cursor);
        }

        let mut cursor = self.extractor.cursor_for_device().await?;
        cursor.workspace_id = self.config.workspace_id.to_string();
        cursor.device_id = self.config.device_id.to_string();
        Ok(cursor)
    }

    async fn record_audit(
        &self,
        event_type: AuditEventType,
        entity_type: Option<EntityType>,
        entity_ids: Vec<String>,
        ops_count: u32,
        payload: serde_json::Value,
    ) -> SyncResult<u64> {
        self.audit
            .record(AuditEvent {
                event_type,
                workspace_id: self.config.workspace_id,
                device_id: self.config.device_id,
                entity_type,
                entity_ids,
                ops_count,
                occurred_at: Utc::now(),
                payload,
            })
            .await
    }

    fn mark_connected(&self, connected: bool) {
        self.with_status_mut(|status| status.connected = connected);
        let _ = if connected {
            self.event_tx.send(SyncEvent::Connected)
        } else {
            self.event_tx.send(SyncEvent::Disconnected)
        };
    }

    fn publish_error(&self, message: impl Into<String>) {
        let message = message.into();
        self.with_status_mut(|status| status.last_error = Some(message.clone()));
        let _ = self.event_tx.send(SyncEvent::SyncError(message));
    }

    fn with_status_mut(&self, f: impl FnOnce(&mut SyncStatus)) {
        let mut status = write_lock(&self.status);
        f(&mut status);
    }
}

async fn connect_pool(config: &SyncConfig) -> SyncResult<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .filename(&config.db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true);
    Ok(SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?)
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn registration_path(data_dir: &Path) -> PathBuf {
    data_dir.join(REGISTRATION_FILE_NAME)
}

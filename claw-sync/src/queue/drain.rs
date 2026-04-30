//! drain.rs — queue drain loop that replays queued operations when connectivity resumes.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::{
    task::JoinHandle,
    time::{self, Duration},
};
use tokio_util::sync::CancellationToken;

use crate::{
    engine::SyncEngine,
    error::SyncResult,
    queue::{
        offline::{OfflineQueue, QueuedOp},
        scheduler::RetryScheduler,
    },
    sync::push::PushSession,
};

/// Outcome of a single queue drain pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DrainResult {
    /// Number of queued operations successfully delivered.
    pub delivered: u32,
    /// Number of operations that remain retryable after this pass.
    pub retried: u32,
    /// Number of operations that exhausted retries or failed permanently.
    pub failed: u32,
    /// Pending queue depth after the drain pass completed.
    pub remaining_pending: u64,
}

/// Abstract replay worker used by the queue drainer.
#[async_trait]
pub trait QueueReplay: Send + Sync {
    /// Replays a single queued operation.
    async fn replay(&self, op: &QueuedOp) -> SyncResult<()>;

    /// Returns whether the worker currently considers the upstream transport connected.
    fn is_connected(&self) -> bool;
}

#[async_trait]
impl QueueReplay for PushSession {
    async fn replay(&self, op: &QueuedOp) -> SyncResult<()> {
        self.replay_queued_op(op).await.map(|_| ())
    }

    fn is_connected(&self) -> bool {
        self.is_connected()
    }
}

/// Long-running offline queue drainer.
pub struct QueueDrainer {
    queue: OfflineQueue,
    scheduler: RetryScheduler,
    replay: Arc<dyn QueueReplay>,
    shutdown: CancellationToken,
    batch_size: u32,
    stale_inflight_secs: u64,
}

impl QueueDrainer {
    /// Creates a new queue drainer.
    pub fn new(
        queue: OfflineQueue,
        scheduler: RetryScheduler,
        replay: Arc<dyn QueueReplay>,
        shutdown: CancellationToken,
        batch_size: u32,
    ) -> Self {
        Self {
            queue,
            scheduler,
            replay,
            shutdown,
            batch_size: batch_size.max(1),
            stale_inflight_secs: 5 * 60,
        }
    }

    /// Starts the drainer in the background and returns the join handle.
    pub fn start(self: Arc<Self>) -> JoinHandle<SyncResult<()>> {
        tokio::spawn(async move { self.run().await })
    }

    /// Runs the drain loop until shutdown is requested.
    pub async fn run(&self) -> SyncResult<()> {
        let mut ticker = time::interval(Duration::from_secs(
            self.scheduler.config.sync_interval_secs.max(1),
        ));
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                _ = ticker.tick() => {
                    if let Err(error) = self.drain_once().await {
                        tracing::warn!(error = %error, "queue drain pass failed");
                    }
                }
            }
        }
        Ok(())
    }

    /// Drains one batch of queued operations.
    pub async fn drain_once(&self) -> SyncResult<DrainResult> {
        self.queue
            .requeue_stale_inflight(self.stale_inflight_secs)
            .await?;

        if !self.replay.is_connected() && self.queue.pending_count().await? == 0 {
            return Ok(DrainResult::default());
        }

        let batch = self.queue.dequeue_batch(self.batch_size).await?;
        if batch.is_empty() {
            return Ok(DrainResult {
                remaining_pending: self.queue.pending_count().await?,
                ..DrainResult::default()
            });
        }

        let mut delivered_ids = Vec::new();
        let mut result = DrainResult::default();

        for op in batch {
            match self.replay.replay(&op).await {
                Ok(()) => {
                    delivered_ids.push(op.id.clone());
                    result.delivered += 1;
                }
                Err(error) => {
                    self.queue.mark_failed(&op.id, &error.to_string()).await?;
                    if self
                        .scheduler
                        .should_retry(op.attempt_count.saturating_add(1))
                    {
                        result.retried += 1;
                        let delay = self.scheduler.next_retry_delay(op.attempt_count);
                        tokio::select! {
                            _ = self.shutdown.cancelled() => break,
                            _ = time::sleep(delay) => {}
                        }
                    } else {
                        result.failed += 1;
                    }
                }
            }
        }

        self.queue.mark_done(&delivered_ids).await?;
        result.remaining_pending = self.queue.pending_count().await?;
        Ok(result)
    }
}

/// Drains the offline queue until no pending work remains using environment-backed engine configuration.
pub async fn drain_loop() -> SyncResult<()> {
    let engine = SyncEngine::from_env().await?;
    loop {
        let result = engine.drain_queue_once().await?;
        if result.remaining_pending == 0 {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use serde_json::json;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{DrainResult, QueueDrainer, QueueReplay};
    use crate::{
        config::SyncConfig,
        crdt::ops::OpKind,
        delta::extractor::EntityType,
        error::{SyncError, SyncResult},
        queue::{
            offline::{OfflineQueue, QueueStatus, QueuedOp},
            scheduler::RetryScheduler,
        },
    };

    #[derive(Default)]
    struct FakeReplay {
        should_fail: bool,
        connected: bool,
        seen: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl QueueReplay for FakeReplay {
        async fn replay(&self, op: &QueuedOp) -> SyncResult<()> {
            self.seen
                .lock()
                .expect("lock should succeed")
                .push(op.id.clone());
            if self.should_fail {
                Err(SyncError::Transport("simulated failure".into()))
            } else {
                Ok(())
            }
        }

        fn is_connected(&self) -> bool {
            self.connected
        }
    }

    fn queued_op(id: &str) -> QueuedOp {
        QueuedOp {
            id: id.into(),
            workspace_id: Uuid::from_bytes([1; 16]),
            entity_type: EntityType::MemoryRecords,
            operation: OpKind::Insert,
            entity_id: format!("entity-{id}"),
            payload: json!({ "title": id }),
            device_id: Uuid::from_bytes([2; 16]),
            hlc: "0000000000000001-00000000-02020202-0202-0202-0202-020202020202".into(),
            enqueued_at: Utc::now(),
            attempt_count: 0,
            status: QueueStatus::Pending,
        }
    }

    fn scheduler() -> RetryScheduler {
        RetryScheduler::new(Arc::new(SyncConfig {
            workspace_id: Uuid::from_bytes([1; 16]),
            device_id: Uuid::from_bytes([2; 16]),
            hub_endpoint: "http://localhost:50051".into(),
            data_dir: ".claw-sync".into(),
            db_path: "claw_sync.db".into(),
            tls_enabled: false,
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            sync_interval_secs: 1,
            heartbeat_interval_secs: 1,
            max_retries: 2,
            retry_base_ms: 1,
            max_delta_rows: 1_000,
            max_chunk_bytes: 64 * 1024,
            max_pull_chunks: 128,
            max_push_inflight: 4,
        }))
    }

    async fn queue(temp_dir: &TempDir) -> OfflineQueue {
        OfflineQueue::new(temp_dir.path())
            .await
            .expect("queue should initialise")
    }

    #[tokio::test]
    async fn drain_once_marks_successful_items_done() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let queue = queue(&temp_dir).await;
        queue
            .enqueue(&queued_op("1"))
            .await
            .expect("enqueue should succeed");

        let drainer = QueueDrainer::new(
            queue.clone(),
            scheduler(),
            Arc::new(FakeReplay {
                should_fail: false,
                connected: true,
                seen: Mutex::new(Vec::new()),
            }),
            CancellationToken::new(),
            10,
        );

        let result = drainer.drain_once().await.expect("drain should succeed");
        assert_eq!(result.delivered, 1);
        assert_eq!(result.remaining_pending, 0);
    }

    #[tokio::test]
    async fn drain_once_marks_failed_items_retryable() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let queue = queue(&temp_dir).await;
        queue
            .enqueue(&queued_op("2"))
            .await
            .expect("enqueue should succeed");

        let drainer = QueueDrainer::new(
            queue.clone(),
            scheduler(),
            Arc::new(FakeReplay {
                should_fail: true,
                connected: true,
                seen: Mutex::new(Vec::new()),
            }),
            CancellationToken::new(),
            10,
        );

        let result: DrainResult = drainer.drain_once().await.expect("drain should succeed");
        assert_eq!(result.retried, 1);
        assert_eq!(
            queue.pending_count().await.expect("count should succeed"),
            1
        );
    }
}

//! scheduler.rs — retry scheduler with exponential backoff and jitter.

use std::{sync::Arc, time::Duration};

use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use rand::Rng;

use crate::{config::SyncConfig, queue::offline::QueuedOp};

/// Retry scheduler used by the offline queue drainer.
#[derive(Clone)]
pub struct RetryScheduler {
    /// Shared runtime configuration.
    pub config: Arc<SyncConfig>,
}

impl RetryScheduler {
    /// Creates a new scheduler from shared sync configuration.
    pub fn new(config: Arc<SyncConfig>) -> Self {
        Self { config }
    }

    /// Computes the next retry delay using exponential backoff plus bounded jitter.
    pub fn next_retry_delay(&self, attempt: u32) -> Duration {
        let base_ms = self.config.retry_base_ms.max(1);
        let exponential_ms = base_ms.saturating_mul(2_u64.saturating_pow(attempt.min(20)));
        let jitter_ms = rand::thread_rng().gen_range(0..=base_ms);
        Duration::from_millis((exponential_ms.saturating_add(jitter_ms)).min(300_000))
    }

    /// Returns `true` when another retry should be attempted.
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.config.max_retries
    }

    /// Builds a configured exponential backoff policy for an individual queued op.
    pub fn backoff_for_op(op: &QueuedOp, config: &SyncConfig) -> ExponentialBackoff {
        let initial = Duration::from_millis(
            config
                .retry_base_ms
                .max(1)
                .saturating_mul(2_u64.saturating_pow(op.attempt_count.min(20))),
        )
        .min(Duration::from_secs(300));
        let max_interval = Duration::from_secs(300);
        let max_elapsed_ms = config
            .retry_base_ms
            .max(1)
            .saturating_mul(2_u64.saturating_pow(config.max_retries.min(20)))
            .saturating_mul(u64::from(config.max_retries.max(1)))
            .min(300_000_u64.saturating_mul(u64::from(config.max_retries.max(1))));
        let max_elapsed = Some(Duration::from_millis(max_elapsed_ms));

        ExponentialBackoffBuilder::new()
            .with_initial_interval(initial)
            .with_max_interval(max_interval)
            .with_max_elapsed_time(max_elapsed)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc, time::Duration};

    use serde_json::json;
    use uuid::Uuid;

    use super::RetryScheduler;
    use crate::{
        config::SyncConfig,
        crdt::ops::OpKind,
        delta::extractor::EntityType,
        queue::offline::{QueueStatus, QueuedOp},
    };

    fn config() -> Arc<SyncConfig> {
        Arc::new(SyncConfig {
            workspace_id: Uuid::from_bytes([1; 16]),
            device_id: Uuid::from_bytes([2; 16]),
            hub_endpoint: "http://localhost:50051".into(),
            data_dir: PathBuf::from(".claw-sync"),
            db_path: PathBuf::from("claw_sync.db"),
            tls_enabled: false,
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            sync_interval_secs: 15,
            heartbeat_interval_secs: 15,
            max_retries: 5,
            retry_base_ms: 100,
            max_delta_rows: 1_000,
            max_chunk_bytes: 64 * 1024,
            max_pull_chunks: 128,
            max_push_inflight: 4,
        })
    }

    fn queued_op(attempt_count: u32) -> QueuedOp {
        QueuedOp {
            id: "queued-op".into(),
            workspace_id: Uuid::from_bytes([1; 16]),
            entity_type: EntityType::Sessions,
            operation: OpKind::Update,
            entity_id: "entity-1".into(),
            payload: json!({ "label": "retry" }),
            device_id: Uuid::from_bytes([2; 16]),
            hlc: "0000000000000001-00000000-02020202-0202-0202-0202-020202020202".into(),
            enqueued_at: chrono::Utc::now(),
            attempt_count,
            status: QueueStatus::Pending,
        }
    }

    #[test]
    fn next_retry_delay_grows_and_caps() {
        let scheduler = RetryScheduler::new(config());
        let first = scheduler.next_retry_delay(0);
        let later = scheduler.next_retry_delay(5);
        let capped = scheduler.next_retry_delay(32);

        assert!(later >= first);
        assert!(capped <= Duration::from_secs(300));
    }

    #[test]
    fn should_retry_respects_configured_limit() {
        let scheduler = RetryScheduler::new(config());
        assert!(scheduler.should_retry(0));
        assert!(scheduler.should_retry(4));
        assert!(!scheduler.should_retry(5));
    }

    #[test]
    fn backoff_for_op_uses_attempt_count() {
        let config = config();
        let op = queued_op(3);
        let backoff = RetryScheduler::backoff_for_op(&op, &config);
        assert!(backoff.initial_interval >= Duration::from_millis(800));
        assert!(backoff.max_interval <= Duration::from_secs(300));
    }
}

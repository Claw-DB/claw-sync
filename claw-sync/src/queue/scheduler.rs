//! scheduler.rs — retry scheduler with exponential back-off and jitter.

use std::time::Duration;

use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};

/// Build an [`ExponentialBackoff`] suitable for queue retry scheduling.
pub fn build_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_secs(1))
        .with_max_interval(Duration::from_secs(60))
        .with_max_elapsed_time(Some(Duration::from_secs(3600)))
        .build()
}

//! clock.rs — Hybrid Logical Clock (HLC) implementation for event ordering across devices.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// A Hybrid Logical Clock timestamp combining physical wall time with a logical counter.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HlcTimestamp {
    /// Wall-clock milliseconds since Unix epoch.
    pub wall_ms: u64,
    /// Logical counter incremented when physical time does not advance.
    pub counter: u16,
    /// Identifier of the node that generated this timestamp.
    pub node_id: u64,
}

impl HlcTimestamp {
    /// Generate a new HLC timestamp that is strictly greater than `last`.
    pub fn now(last: &HlcTimestamp, node_id: u64) -> Self {
        let wall_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(last.wall_ms);
        if wall_ms > last.wall_ms {
            Self { wall_ms, counter: 0, node_id }
        } else {
            Self { wall_ms: last.wall_ms, counter: last.counter + 1, node_id }
        }
    }

    /// Serialise this timestamp to a compact string (`{wall_ms}-{counter}-{node_id}`).
    pub fn to_string_repr(&self) -> String {
        format!("{}-{}-{}", self.wall_ms, self.counter, self.node_id)
    }
}

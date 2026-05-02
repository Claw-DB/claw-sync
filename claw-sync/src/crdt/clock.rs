//! clock.rs — Hybrid Logical Clock (HLC) implementation for event ordering across devices.

use std::{
    cmp::Ordering,
    fmt,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{SyncError, SyncResult};

/// Hybrid Logical Clock state for the local node.
#[derive(Debug, Clone)]
pub struct HybridLogicalClock {
    /// Current logical millisecond component tracked by the clock.
    pub logical: u64,
    /// Current logical counter for events sharing the same logical millisecond.
    pub counter: u32,
    node_id: Uuid,
}

impl HybridLogicalClock {
    /// Creates a new clock using a fresh node identifier.
    pub fn new() -> Self {
        Self::with_node_id(Uuid::new_v4())
    }

    /// Creates a new clock bound to a stable node identifier.
    pub fn with_node_id(node_id: Uuid) -> Self {
        Self {
            logical: Self::now(),
            counter: 0,
            node_id,
        }
    }

    /// Returns the node identifier attached to this clock.
    pub fn node_id(&self) -> Uuid {
        self.node_id
    }

    /// Advances the clock for a local event and returns the generated timestamp.
    pub fn tick(&mut self) -> HlcTimestamp {
        let physical_time = Self::now();
        let next_logical = self.logical.max(physical_time);
        let next_counter = if next_logical == self.logical {
            self.counter.saturating_add(1)
        } else {
            0
        };

        self.logical = next_logical;
        self.counter = next_counter;

        HlcTimestamp {
            logical_ms: next_logical,
            counter: next_counter,
            node_id: self.node_id,
        }
    }

    /// Incorporates a received remote timestamp and returns the next local timestamp.
    pub fn update(&mut self, received: &HlcTimestamp) -> HlcTimestamp {
        let physical_time = Self::now();
        let next_logical = self.logical.max(received.logical_ms).max(physical_time);
        let next_counter = if next_logical == self.logical && next_logical == received.logical_ms {
            self.counter.max(received.counter).saturating_add(1)
        } else if next_logical == self.logical {
            self.counter.saturating_add(1)
        } else if next_logical == received.logical_ms {
            received.counter.saturating_add(1)
        } else {
            0
        };

        self.logical = next_logical;
        self.counter = next_counter;

        HlcTimestamp {
            logical_ms: next_logical,
            counter: next_counter,
            node_id: self.node_id,
        }
    }

    /// Returns the current Unix time in milliseconds.
    pub fn now() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as u64,
            Err(_) => 0,
        }
    }
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

/// A Hybrid Logical Clock timestamp combining wall time, a logical counter, and node id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HlcTimestamp {
    /// Logical millisecond component.
    pub logical_ms: u64,
    /// Logical counter component.
    pub counter: u32,
    /// Stable node identifier that produced the timestamp.
    pub node_id: Uuid,
}

impl HlcTimestamp {
    /// Serialises the timestamp to `{logical_ms:016x}-{counter:08x}-{node_id}`.
    #[allow(clippy::inherent_to_string_shadow_display)]
    pub fn to_string(&self) -> String {
        format!(
            "{:016x}-{:08x}-{}",
            self.logical_ms, self.counter, self.node_id
        )
    }

    /// Parses a timestamp emitted by [`HlcTimestamp::to_string`].
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> SyncResult<Self> {
        <Self as FromStr>::from_str(value)
    }

    /// Returns `true` when this timestamp happened before `other`.
    pub fn happened_before(&self, other: &Self) -> bool {
        self < other
    }
}

impl fmt::Display for HlcTimestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_string())
    }
}

impl FromStr for HlcTimestamp {
    type Err = SyncError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.splitn(3, '-');
        let logical = parts.next().ok_or_else(|| {
            SyncError::Validation("HLC timestamp missing logical component".into())
        })?;
        let counter = parts.next().ok_or_else(|| {
            SyncError::Validation("HLC timestamp missing counter component".into())
        })?;
        let node_id = parts
            .next()
            .ok_or_else(|| SyncError::Validation("HLC timestamp missing node identifier".into()))?;

        Ok(Self {
            logical_ms: u64::from_str_radix(logical, 16).map_err(|error| {
                SyncError::Validation(format!("invalid HLC logical component: {error}"))
            })?,
            counter: u32::from_str_radix(counter, 16).map_err(|error| {
                SyncError::Validation(format!("invalid HLC counter component: {error}"))
            })?,
            node_id: Uuid::parse_str(node_id)
                .map_err(|error| SyncError::Validation(format!("invalid HLC node id: {error}")))?,
        })
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.logical_ms, self.counter, self.node_id).cmp(&(
            other.logical_ms,
            other.counter,
            other.node_id,
        ))
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::{HlcTimestamp, HybridLogicalClock};
    use uuid::Uuid;

    fn node(byte: u8) -> Uuid {
        Uuid::from_bytes([byte; 16])
    }

    fn timestamp(logical_ms: u64, counter: u32, byte: u8) -> HlcTimestamp {
        HlcTimestamp {
            logical_ms,
            counter,
            node_id: node(byte),
        }
    }

    #[test]
    fn new_initialises_with_current_time_and_zero_counter() {
        let now = HybridLogicalClock::now();
        let clock = HybridLogicalClock::new();
        assert!(clock.logical >= now);
        assert_eq!(clock.counter, 0);
    }

    #[test]
    fn tick_is_monotonic_when_wall_clock_does_not_advance() {
        let mut clock = HybridLogicalClock {
            logical: HybridLogicalClock::now().saturating_add(10_000),
            counter: 3,
            node_id: node(1),
        };

        let timestamp = clock.tick();
        assert_eq!(timestamp.logical_ms, clock.logical);
        assert_eq!(timestamp.counter, 4);
        assert_eq!(timestamp.node_id, node(1));
    }

    #[test]
    fn tick_resets_counter_when_physical_time_advances() {
        let mut clock = HybridLogicalClock {
            logical: 0,
            counter: 9,
            node_id: node(2),
        };

        let timestamp = clock.tick();
        assert!(timestamp.logical_ms >= HybridLogicalClock::now().saturating_sub(1));
        assert_eq!(timestamp.counter, 0);
    }

    #[test]
    fn receive_update_advances_to_remote_logical_time() {
        let base = HybridLogicalClock::now().saturating_add(10_000);
        let mut clock = HybridLogicalClock {
            logical: base,
            counter: 0,
            node_id: node(3),
        };

        let updated = clock.update(&timestamp(base.saturating_add(15), 4, 4));
        assert_eq!(updated.logical_ms, base.saturating_add(15));
        assert_eq!(updated.counter, 5);
    }

    #[test]
    fn receive_update_uses_max_counter_plus_one_when_logical_times_match() {
        let base = HybridLogicalClock::now().saturating_add(10_000);
        let mut clock = HybridLogicalClock {
            logical: base,
            counter: 6,
            node_id: node(5),
        };

        let updated = clock.update(&timestamp(base, 9, 6));
        assert_eq!(updated.logical_ms, base);
        assert_eq!(updated.counter, 10);
    }

    #[test]
    fn receive_update_increments_local_counter_when_local_clock_is_ahead() {
        let mut clock = HybridLogicalClock {
            logical: HybridLogicalClock::now().saturating_add(50_000),
            counter: 2,
            node_id: node(7),
        };

        let updated = clock.update(&timestamp(10, 1, 8));
        assert_eq!(updated.logical_ms, clock.logical);
        assert_eq!(updated.counter, 3);
    }

    #[test]
    fn receive_update_resets_counter_when_physical_clock_wins() {
        let mut clock = HybridLogicalClock {
            logical: 1,
            counter: 9,
            node_id: node(9),
        };

        let updated = clock.update(&timestamp(2, 7, 10));
        assert!(updated.logical_ms >= HybridLogicalClock::now().saturating_sub(1));
        assert_eq!(updated.counter, 0);
    }

    #[test]
    fn string_round_trip_preserves_timestamp() {
        let original = timestamp(0xfeed_beef, 0x42, 11);
        let reparsed = HlcTimestamp::from_str(&original.to_string())
            .expect("HLC timestamps should round-trip");
        assert_eq!(reparsed, original);
    }

    #[test]
    fn ordering_is_lexicographic() {
        let first = timestamp(10, 2, 1);
        let second = timestamp(10, 3, 1);
        let third = timestamp(11, 0, 1);
        assert!(first < second);
        assert!(second < third);
    }

    #[test]
    fn happened_before_matches_ordering() {
        let first = timestamp(100, 1, 1);
        let second = timestamp(100, 1, 2);
        assert!(first.happened_before(&second));
        assert!(!second.happened_before(&first));
    }

    #[test]
    fn concurrent_events_produce_unique_timestamps() {
        let mut left = HybridLogicalClock {
            logical: 1_000,
            counter: 0,
            node_id: node(12),
        };
        let mut right = HybridLogicalClock {
            logical: 1_000,
            counter: 0,
            node_id: node(13),
        };

        let left_timestamp = left.tick();
        let right_timestamp = right.tick();
        assert_ne!(left_timestamp, right_timestamp);
    }

    #[test]
    fn causally_ordered_sequence_is_correctly_sorted() {
        let mut sender = HybridLogicalClock {
            logical: 2_000,
            counter: 0,
            node_id: node(14),
        };
        let mut receiver = HybridLogicalClock {
            logical: 1_500,
            counter: 0,
            node_id: node(15),
        };

        let sent = sender.tick();
        let received = receiver.update(&sent);
        let reply = sender.update(&received);

        assert!(sent < received);
        assert!(received < reply);
    }
}

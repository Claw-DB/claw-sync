//! lww.rs — Last-Write-Wins register for simple scalar CRDT conflict resolution.

use serde::{Deserialize, Serialize};

use crate::crdt::clock::HlcTimestamp;

/// A Last-Write-Wins register holding a value and the timestamp of the last write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LwwRegister<T> {
    /// The current value of the register.
    pub value: T,
    /// HLC timestamp of the write that set the current value.
    pub timestamp: HlcTimestamp,
}

impl<T: Clone> LwwRegister<T> {
    /// Create a new register with an initial value at the given timestamp.
    pub fn new(value: T, timestamp: HlcTimestamp) -> Self {
        Self { value, timestamp }
    }

    /// Merge `other` into `self`, keeping the value with the higher timestamp.
    pub fn merge(&mut self, other: &LwwRegister<T>) {
        if other.timestamp > self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp.clone();
        }
    }
}

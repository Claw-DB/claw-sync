//! extractor.rs — reads the WAL and change-log from claw-core to produce a `DeltaSet`.

use serde::{Deserialize, Serialize};

/// An atomic set of changes extracted from the local claw-core database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSet {
    /// Ordered list of individual change records in this delta.
    pub records: Vec<DeltaRecord>,
    /// Lamport clock value at the point of extraction.
    pub lamport_clock: u64,
}

/// A single database row change captured from the WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaRecord {
    /// The table or entity type the change belongs to.
    pub entity_type: String,
    /// The primary key of the changed entity.
    pub entity_id: String,
    /// The serialised entity payload after the change.
    pub payload: Vec<u8>,
}

/// Extract a [`DeltaSet`] from the claw-core change log since `since_lamport`.
pub async fn extract(_since_lamport: u64) -> crate::error::SyncResult<DeltaSet> {
    Ok(DeltaSet { records: vec![], lamport_clock: 0 })
}

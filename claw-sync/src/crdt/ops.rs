//! ops.rs — `CrdtOp` enum representing Insert, Update, Delete, and Tombstone operations.

use serde::{Deserialize, Serialize};

use crate::crdt::clock::HlcTimestamp;

/// A single CRDT operation applied to an entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CrdtOp {
    /// Insert a new entity with the given payload.
    Insert {
        /// The entity's unique identifier.
        entity_id: String,
        /// The entity type (table name or collection).
        entity_type: String,
        /// Serialised entity payload.
        payload: Vec<u8>,
        /// HLC timestamp of this operation.
        timestamp: HlcTimestamp,
    },
    /// Update an existing entity's payload.
    Update {
        /// The entity's unique identifier.
        entity_id: String,
        /// The entity type.
        entity_type: String,
        /// New serialised entity payload.
        payload: Vec<u8>,
        /// HLC timestamp of this operation.
        timestamp: HlcTimestamp,
    },
    /// Soft-delete an entity; tombstones prevent phantom re-insertions.
    Delete {
        /// The entity's unique identifier.
        entity_id: String,
        /// The entity type.
        entity_type: String,
        /// HLC timestamp of this operation.
        timestamp: HlcTimestamp,
    },
    /// A permanent deletion marker that supersedes all other operations for this entity.
    Tombstone {
        /// The entity's unique identifier.
        entity_id: String,
        /// The entity type.
        entity_type: String,
        /// HLC timestamp of this operation.
        timestamp: HlcTimestamp,
    },
}

//! ops.rs — CRDT operations produced locally and applied remotely.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::crdt::clock::HlcTimestamp;

/// The durable operation kind recorded in the sync changelog and queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OpKind {
    /// Insert a new entity.
    Insert,
    /// Update an existing entity.
    Update,
    /// Delete or tombstone an entity.
    Delete,
}

impl OpKind {
    /// Returns the canonical uppercase string used in SQLite and on the wire.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

impl std::fmt::Display for OpKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for OpKind {
    type Err = crate::error::SyncError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "INSERT" => Ok(Self::Insert),
            "UPDATE" => Ok(Self::Update),
            "DELETE" => Ok(Self::Delete),
            other => Err(crate::error::SyncError::Validation(format!(
                "unsupported operation kind: {other}"
            ))),
        }
    }
}

/// A field-level patch used for partial updates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldPatch {
    /// Dotted field path relative to the entity root.
    pub field: String,
    /// Previous field value observed when producing the patch.
    pub old_value: Value,
    /// New field value to assign.
    pub new_value: Value,
}

/// A single CRDT operation applied to an entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CrdtOp {
    /// Inserts a new entity payload.
    Insert {
        /// The entity's unique identifier.
        entity_id: String,
        /// JSON payload to insert.
        payload: Value,
        /// HLC timestamp of this operation.
        hlc: HlcTimestamp,
        /// Device that produced the operation.
        device_id: Uuid,
    },
    /// Updates one or more fields on an existing entity.
    Update {
        /// The entity's unique identifier.
        entity_id: String,
        /// Field-level mutations to apply.
        field_patches: Vec<FieldPatch>,
        /// HLC timestamp of this operation.
        hlc: HlcTimestamp,
        /// Device that produced the operation.
        device_id: Uuid,
    },
    /// Soft-deletes an entity.
    Delete {
        /// The entity's unique identifier.
        entity_id: String,
        /// HLC timestamp of this operation.
        hlc: HlcTimestamp,
        /// Device that produced the operation.
        device_id: Uuid,
    },
    /// Permanent tombstone preventing stale resurrection.
    Tombstone {
        /// The entity's unique identifier.
        entity_id: String,
        /// Timestamp when the tombstone became authoritative.
        deleted_at: HlcTimestamp,
    },
}

impl CrdtOp {
    /// Returns the entity identifier targeted by this operation.
    pub fn entity_id(&self) -> &str {
        match self {
            Self::Insert { entity_id, .. }
            | Self::Update { entity_id, .. }
            | Self::Delete { entity_id, .. }
            | Self::Tombstone { entity_id, .. } => entity_id,
        }
    }

    /// Returns the HLC timestamp associated with this operation.
    pub fn hlc(&self) -> &HlcTimestamp {
        match self {
            Self::Insert { hlc, .. } | Self::Update { hlc, .. } | Self::Delete { hlc, .. } => hlc,
            Self::Tombstone { deleted_at, .. } => deleted_at,
        }
    }

    /// Returns the device that produced this operation.
    pub fn device_id(&self) -> Uuid {
        match self {
            Self::Insert { device_id, .. }
            | Self::Update { device_id, .. }
            | Self::Delete { device_id, .. } => *device_id,
            Self::Tombstone { deleted_at, .. } => deleted_at.node_id,
        }
    }

    /// Returns the durable operation kind corresponding to this CRDT operation.
    pub fn kind(&self) -> OpKind {
        match self {
            Self::Insert { .. } => OpKind::Insert,
            Self::Update { .. } => OpKind::Update,
            Self::Delete { .. } | Self::Tombstone { .. } => OpKind::Delete,
        }
    }

    /// Returns `true` when this operation deletes or tombstones the entity.
    pub fn is_delete_like(&self) -> bool {
        matches!(self, Self::Delete { .. } | Self::Tombstone { .. })
    }
}

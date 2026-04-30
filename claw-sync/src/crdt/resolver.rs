//! resolver.rs — conflict resolution strategies: LWW, merge, and manual escalation.

use serde_json::{Map, Value};

use crate::{
    crdt::{clock::HlcTimestamp, ops::CrdtOp},
    error::{SyncError, SyncResult},
    proto::clawsync::v1::ConflictRecord,
};

/// Supported conflict resolution strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictStrategy {
    /// Keep the operation with the newer HLC timestamp.
    LastWriteWins,
    /// Always prefer the remote server version.
    ServerWins,
    /// Always prefer the local client version.
    ClientWins,
    /// Escalate every conflict for external handling.
    ManualEscalate,
}

/// Outcome of resolving a conflict.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedOp {
    /// Keep the local operation.
    UseLocal,
    /// Apply the remote operation.
    UseRemote,
    /// Apply a merged JSON payload.
    Merge(Value),
    /// Escalate the conflict with full context.
    Escalate(ConflictRecord),
}

/// Conflict resolution engine for inbound vs outbound CRDT operations.
#[derive(Debug, Clone)]
pub struct ConflictResolver {
    /// Strategy used when resolving conflicts.
    pub strategy: ConflictStrategy,
}

impl ConflictResolver {
    /// Creates a new resolver with the supplied strategy.
    pub fn new(strategy: ConflictStrategy) -> Self {
        Self { strategy }
    }

    /// Resolves a single local/remote conflict pair.
    pub fn resolve(&self, local: &CrdtOp, remote: &CrdtOp) -> SyncResult<ResolvedOp> {
        if local.entity_id() != remote.entity_id() {
            return Err(SyncError::Validation(format!(
                "cannot resolve CRDT ops for different entities: {} vs {}",
                local.entity_id(),
                remote.entity_id()
            )));
        }

        match self.strategy {
            ConflictStrategy::ClientWins => Ok(ResolvedOp::UseLocal),
            ConflictStrategy::ServerWins => Ok(ResolvedOp::UseRemote),
            ConflictStrategy::ManualEscalate => {
                Ok(ResolvedOp::Escalate(build_conflict_record(local, remote)?))
            }
            ConflictStrategy::LastWriteWins => resolve_lww(local, remote),
        }
    }

    /// Resolves a batch of conflict pairs.
    pub fn resolve_batch(&self, conflicts: Vec<(CrdtOp, CrdtOp)>) -> SyncResult<Vec<ResolvedOp>> {
        conflicts
            .into_iter()
            .map(|(local, remote)| self.resolve(&local, &remote))
            .collect()
    }
}

impl Default for ConflictResolver {
    fn default() -> Self {
        Self::new(ConflictStrategy::LastWriteWins)
    }
}

/// Merges two JSON payloads using field-level last-write-wins semantics.
pub fn merge_json_objects(
    local: &Value,
    remote: &Value,
    local_hlc: &HlcTimestamp,
    remote_hlc: &HlcTimestamp,
) -> Value {
    match (local, remote) {
        (Value::Object(local_map), Value::Object(remote_map)) => {
            let mut merged = Map::new();

            for (key, value) in local_map {
                match remote_map.get(key) {
                    Some(remote_value) => {
                        merged.insert(
                            key.clone(),
                            merge_json_objects(value, remote_value, local_hlc, remote_hlc),
                        );
                    }
                    None => {
                        merged.insert(key.clone(), value.clone());
                    }
                }
            }

            for (key, value) in remote_map {
                if !merged.contains_key(key) {
                    merged.insert(key.clone(), value.clone());
                }
            }

            Value::Object(merged)
        }
        _ if remote_hlc > local_hlc => remote.clone(),
        _ => local.clone(),
    }
}

fn resolve_lww(local: &CrdtOp, remote: &CrdtOp) -> SyncResult<ResolvedOp> {
    if local.is_delete_like() || remote.is_delete_like() {
        return Ok(delete_resolution(local, remote));
    }

    if local.hlc() == remote.hlc() && has_field_collision(local, remote) {
        return Ok(ResolvedOp::Escalate(build_conflict_record(local, remote)?));
    }

    match (materialize_payload(local), materialize_payload(remote)) {
        (Some(local_value), Some(remote_value))
            if local_value.is_object() && remote_value.is_object() =>
        {
            Ok(ResolvedOp::Merge(merge_json_objects(
                &local_value,
                &remote_value,
                local.hlc(),
                remote.hlc(),
            )))
        }
        _ if remote.hlc() > local.hlc() => Ok(ResolvedOp::UseRemote),
        _ if remote.hlc() < local.hlc() => Ok(ResolvedOp::UseLocal),
        _ if remote.device_id() > local.device_id() => Ok(ResolvedOp::UseRemote),
        _ => Ok(ResolvedOp::UseLocal),
    }
}

fn has_field_collision(local: &CrdtOp, remote: &CrdtOp) -> bool {
    match (materialize_payload(local), materialize_payload(remote)) {
        (Some(Value::Object(local_map)), Some(Value::Object(remote_map))) => {
            local_map.keys().any(|key| remote_map.contains_key(key))
        }
        (Some(_), Some(_)) => true,
        _ => false,
    }
}

fn delete_resolution(local: &CrdtOp, remote: &CrdtOp) -> ResolvedOp {
    match (local.is_delete_like(), remote.is_delete_like()) {
        (true, false) => {
            if local.hlc() >= remote.hlc() {
                ResolvedOp::UseLocal
            } else {
                ResolvedOp::UseRemote
            }
        }
        (false, true) => {
            if remote.hlc() >= local.hlc() {
                ResolvedOp::UseRemote
            } else {
                ResolvedOp::UseLocal
            }
        }
        _ if remote.hlc() > local.hlc() => ResolvedOp::UseRemote,
        _ if remote.hlc() < local.hlc() => ResolvedOp::UseLocal,
        _ if remote.device_id() > local.device_id() => ResolvedOp::UseRemote,
        _ => ResolvedOp::UseLocal,
    }
}

fn materialize_payload(op: &CrdtOp) -> Option<Value> {
    match op {
        CrdtOp::Insert { payload, .. } => Some(payload.clone()),
        CrdtOp::Update { field_patches, .. } => {
            let mut object = Map::new();
            for patch in field_patches {
                object.insert(patch.field.clone(), patch.new_value.clone());
            }
            Some(Value::Object(object))
        }
        CrdtOp::Delete { .. } | CrdtOp::Tombstone { .. } => None,
    }
}

fn build_conflict_record(local: &CrdtOp, remote: &CrdtOp) -> SyncResult<ConflictRecord> {
    let local_value = materialize_payload(local).unwrap_or(Value::Null);
    let remote_value = materialize_payload(remote).unwrap_or(Value::Null);

    Ok(ConflictRecord {
        entity_id: local.entity_id().to_owned(),
        entity_type: String::new(),
        local_value: serde_json::to_vec(&local_value)?,
        remote_value: serde_json::to_vec(&remote_value)?,
        local_hlc: local.hlc().to_string(),
        remote_hlc: remote.hlc().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{ConflictResolver, ConflictStrategy, ResolvedOp};
    use crate::crdt::{
        clock::HlcTimestamp,
        ops::{CrdtOp, FieldPatch},
    };

    fn timestamp(logical: u64, counter: u32, node: u8) -> HlcTimestamp {
        HlcTimestamp {
            logical_ms: logical,
            counter,
            node_id: Uuid::from_bytes([node; 16]),
        }
    }

    fn insert(entity_id: &str, payload: serde_json::Value, logical: u64, node: u8) -> CrdtOp {
        CrdtOp::Insert {
            entity_id: entity_id.into(),
            payload,
            hlc: timestamp(logical, 0, node),
            device_id: Uuid::from_bytes([node; 16]),
        }
    }

    fn update(
        entity_id: &str,
        field: &str,
        value: serde_json::Value,
        logical: u64,
        node: u8,
    ) -> CrdtOp {
        CrdtOp::Update {
            entity_id: entity_id.into(),
            field_patches: vec![FieldPatch {
                field: field.into(),
                old_value: serde_json::Value::Null,
                new_value: value,
            }],
            hlc: timestamp(logical, 0, node),
            device_id: Uuid::from_bytes([node; 16]),
        }
    }

    #[test]
    fn concurrent_inserts_merge_object_payloads() {
        let resolver = ConflictResolver::default();
        let result = resolver
            .resolve(
                &insert("doc-1", json!({ "a": 1 }), 10, 1),
                &insert("doc-1", json!({ "b": 2 }), 11, 2),
            )
            .expect("resolver should succeed");

        assert_eq!(result, ResolvedOp::Merge(json!({ "a": 1, "b": 2 })));
    }

    #[test]
    fn concurrent_updates_same_field_use_lww() {
        let resolver = ConflictResolver::default();
        let result = resolver
            .resolve(
                &update("doc-1", "title", json!("left"), 10, 1),
                &update("doc-1", "title", json!("right"), 11, 2),
            )
            .expect("resolver should succeed");

        assert_eq!(result, ResolvedOp::Merge(json!({ "title": "right" })));
    }

    #[test]
    fn delete_wins_over_update() {
        let resolver = ConflictResolver::default();
        let result = resolver
            .resolve(
                &CrdtOp::Delete {
                    entity_id: "doc-1".into(),
                    hlc: timestamp(20, 0, 3),
                    device_id: Uuid::from_bytes([3; 16]),
                },
                &update("doc-1", "title", json!("right"), 19, 4),
            )
            .expect("resolver should succeed");

        assert_eq!(result, ResolvedOp::UseLocal);
    }

    #[test]
    fn merge_preserves_non_conflicting_fields() {
        let resolver = ConflictResolver::new(ConflictStrategy::LastWriteWins);
        let result = resolver
            .resolve(
                &insert("doc-1", json!({ "name": "left", "count": 1 }), 10, 1),
                &insert("doc-1", json!({ "count": 2, "updated": true }), 11, 2),
            )
            .expect("resolver should succeed");

        assert_eq!(
            result,
            ResolvedOp::Merge(json!({ "name": "left", "count": 2, "updated": true }))
        );
    }
}

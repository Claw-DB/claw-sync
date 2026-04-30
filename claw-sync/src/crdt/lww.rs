//! lww.rs — Last-Write-Wins register for deterministic conflict resolution.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::crdt::{clock::HlcTimestamp, ops::CrdtOp};

/// A Last-Write-Wins register holding a value and the metadata of the last write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LwwRegister<T: Clone> {
    /// The current value of the register.
    pub value: T,
    /// HLC timestamp of the write that set the current value.
    pub hlc: HlcTimestamp,
    /// Device that authored the current winning value.
    pub device_id: Uuid,
}

impl<T> LwwRegister<T>
where
    T: Clone + Serialize + DeserializeOwned,
{
    /// Creates a new register with an initial value.
    pub fn new(value: T, hlc: HlcTimestamp, device_id: Uuid) -> Self {
        Self {
            value,
            hlc,
            device_id,
        }
    }

    /// Merges another register into this one, keeping the deterministically newer value.
    pub fn merge(&mut self, other: LwwRegister<T>) {
        if other.hlc > self.hlc || (other.hlc == self.hlc && other.device_id > self.device_id) {
            *self = other;
        }
    }

    /// Applies a CRDT operation to the register when the incoming op wins.
    pub fn apply_op(&mut self, op: &CrdtOp) -> bool {
        let should_apply =
            op.hlc() > &self.hlc || (op.hlc() == &self.hlc && op.device_id() > self.device_id);
        if !should_apply {
            return false;
        }

        match op {
            CrdtOp::Insert {
                payload,
                hlc,
                device_id,
                ..
            } => match serde_json::from_value::<T>(payload.clone()) {
                Ok(value) => {
                    self.value = value;
                    self.hlc = hlc.clone();
                    self.device_id = *device_id;
                    true
                }
                Err(_) => false,
            },
            CrdtOp::Update {
                field_patches,
                hlc,
                device_id,
                ..
            } => {
                let mut json_value = match serde_json::to_value(&self.value) {
                    Ok(value) => value,
                    Err(_) => return false,
                };

                for patch in field_patches {
                    if !apply_patch_field(&mut json_value, &patch.field, patch.new_value.clone()) {
                        return false;
                    }
                }

                match serde_json::from_value::<T>(json_value) {
                    Ok(value) => {
                        self.value = value;
                        self.hlc = hlc.clone();
                        self.device_id = *device_id;
                        true
                    }
                    Err(_) => false,
                }
            }
            CrdtOp::Delete { .. } | CrdtOp::Tombstone { .. } => false,
        }
    }
}

fn apply_patch_field(target: &mut Value, field: &str, new_value: Value) -> bool {
    let parts: Vec<&str> = field.split('.').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() {
        return false;
    }

    let mut current = target;
    for part in &parts[..parts.len().saturating_sub(1)] {
        match current {
            Value::Object(map) => {
                current = map
                    .entry((*part).to_owned())
                    .or_insert_with(|| Value::Object(Map::new()));
            }
            _ => return false,
        }
    }

    match current {
        Value::Object(map) => {
            map.insert(parts[parts.len() - 1].to_owned(), new_value);
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use uuid::Uuid;

    use super::LwwRegister;
    use crate::crdt::{
        clock::HlcTimestamp,
        ops::{CrdtOp, FieldPatch},
    };

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct Doc {
        name: String,
        count: u32,
    }

    fn timestamp(logical: u64, counter: u32, node: u8) -> HlcTimestamp {
        HlcTimestamp {
            logical_ms: logical,
            counter,
            node_id: Uuid::from_bytes([node; 16]),
        }
    }

    #[test]
    fn merge_prefers_higher_hlc_then_device_id() {
        let mut register = LwwRegister::new(
            Doc {
                name: "left".into(),
                count: 1,
            },
            timestamp(10, 0, 1),
            Uuid::from_bytes([1; 16]),
        );
        register.merge(LwwRegister::new(
            Doc {
                name: "right".into(),
                count: 2,
            },
            timestamp(10, 0, 2),
            Uuid::from_bytes([2; 16]),
        ));

        assert_eq!(register.value.name, "right");
    }

    #[test]
    fn apply_update_mutates_json_backed_value() {
        let mut register = LwwRegister::new(
            Doc {
                name: "left".into(),
                count: 1,
            },
            timestamp(10, 0, 1),
            Uuid::from_bytes([1; 16]),
        );
        let op = CrdtOp::Update {
            entity_id: "doc-1".into(),
            field_patches: vec![FieldPatch {
                field: "count".into(),
                old_value: json!(1),
                new_value: json!(4),
            }],
            hlc: timestamp(11, 0, 2),
            device_id: Uuid::from_bytes([2; 16]),
        };

        assert!(register.apply_op(&op));
        assert_eq!(register.value.count, 4);
    }
}

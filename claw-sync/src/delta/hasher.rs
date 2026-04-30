//! hasher.rs — BLAKE3 content addressing for delta deduplication.

/// Computes a BLAKE3 hash of `data` and returns it as 32 raw bytes.
pub fn content_hash(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

/// Computes a stable entity hash from the entity type, id, and payload bytes.
pub fn entity_hash(entity_type: &str, entity_id: &str, payload: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(entity_type.as_bytes());
    hasher.update(&[0]);
    hasher.update(entity_id.as_bytes());
    hasher.update(&[0]);
    hasher.update(payload);
    *hasher.finalize().as_bytes()
}

/// Computes a state hash from a set of entity hashes after sorting them lexicographically.
pub fn state_hash(entity_hashes: &[[u8; 32]]) -> [u8; 32] {
    let mut sorted = entity_hashes.to_vec();
    sorted.sort_unstable();

    let mut hasher = blake3::Hasher::new();
    for entity_hash in sorted {
        hasher.update(&entity_hash);
    }
    *hasher.finalize().as_bytes()
}

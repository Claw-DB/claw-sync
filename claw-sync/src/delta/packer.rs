//! packer.rs — serialises, compresses, encrypts, and chunks `DeltaSet`s.

use lz4_flex::compress_prepend_size;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    crypto::{cipher::encrypt_chunk, keys::WorkspaceKey},
    delta::{
        extractor::{DeltaSet, EntityType},
        hasher::{content_hash, state_hash},
    },
    error::{SyncError, SyncResult},
    proto::clawsync::v1::DeltaChunk,
};

/// Configuration for outbound delta packing.
#[derive(Debug, Clone, Copy)]
pub struct PackerConfig {
    /// Maximum encoded bytes carried by any single chunk.
    pub max_chunk_bytes: usize,
}

impl Default for PackerConfig {
    fn default() -> Self {
        Self {
            max_chunk_bytes: 64 * 1024,
        }
    }
}

/// A single chunk of an encrypted payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadChunk {
    /// Zero-based sequence number for this chunk within the payload.
    pub seq: u32,
    /// Total number of chunks in the payload.
    pub total: u32,
    /// Encrypted chunk bytes.
    pub encrypted_payload: Vec<u8>,
    /// BLAKE3 hash of `encrypted_payload`.
    pub payload_hash: [u8; 32],
}

/// A fully packed delta payload ready to transmit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPayload {
    /// Stable identifier shared by every chunk in this payload.
    pub chunk_id: String,
    /// Entity type carried by the underlying `DeltaSet`.
    pub entity_type: EntityType,
    /// Ordered payload chunks.
    pub chunks: Vec<PayloadChunk>,
    /// Hash of all per-chunk hashes concatenated in sequence order.
    pub total_hash: [u8; 32],
    /// Local changelog sequences represented by this payload.
    pub sequences: Vec<i64>,
}

/// Packs a single delta set into an encrypted, chunked sync payload.
pub fn pack(
    delta: &DeltaSet,
    workspace_key: &WorkspaceKey,
    max_chunk_bytes: usize,
) -> SyncResult<SyncPayload> {
    if max_chunk_bytes == 0 {
        return Err(SyncError::Validation(
            "max_chunk_bytes must be greater than zero".into(),
        ));
    }

    let json = serde_json::to_vec(delta)?;
    let compressed = compress_prepend_size(&json);
    let chunk_id = Uuid::new_v4().to_string();
    let encrypted = encrypt_chunk(&compressed, workspace_key, &chunk_id)?;
    let encrypted_bytes = encrypted.to_bytes();

    let total_chunks = encrypted_bytes.len().div_ceil(max_chunk_bytes) as u32;
    let mut chunks = Vec::with_capacity(total_chunks as usize);
    let mut chunk_hashes = Vec::with_capacity(total_chunks as usize);

    for (index, bytes) in encrypted_bytes.chunks(max_chunk_bytes).enumerate() {
        let payload_hash = content_hash(bytes);
        chunk_hashes.push(payload_hash);
        chunks.push(PayloadChunk {
            seq: index as u32,
            total: total_chunks,
            encrypted_payload: bytes.to_vec(),
            payload_hash,
        });
    }

    Ok(SyncPayload {
        chunk_id,
        entity_type: delta.entity_type,
        chunks,
        total_hash: state_hash(&chunk_hashes),
        sequences: delta.sequences.clone(),
    })
}

/// Packs multiple delta sets independently.
pub fn pack_batch(
    deltas: Vec<DeltaSet>,
    workspace_key: &WorkspaceKey,
    max_chunk_bytes: usize,
) -> SyncResult<Vec<SyncPayload>> {
    deltas
        .iter()
        .map(|delta| pack(delta, workspace_key, max_chunk_bytes))
        .collect()
}

/// Converts a packed payload into protobuf chunk messages.
pub fn payload_to_proto_chunks(payload: &SyncPayload) -> Vec<DeltaChunk> {
    payload
        .chunks
        .iter()
        .map(|chunk| DeltaChunk {
            chunk_id: payload.chunk_id.clone(),
            seq: chunk.seq as i32,
            total: chunk.total as i32,
            encrypted_payload: chunk.encrypted_payload.clone(),
            payload_hash: chunk.payload_hash.to_vec(),
            entity_type: payload.entity_type.to_string(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{pack, payload_to_proto_chunks};
    use crate::{
        crdt::{clock::HlcTimestamp, ops::CrdtOp},
        delta::extractor::{DeltaSet, EntityType},
    };

    fn delta_set() -> DeltaSet {
        DeltaSet {
            entity_type: EntityType::MemoryRecords,
            ops: vec![CrdtOp::Insert {
                entity_id: "memory-1".into(),
                payload: json!({ "title": "hello" }),
                hlc: HlcTimestamp {
                    logical_ms: 1,
                    counter: 0,
                    node_id: Uuid::from_bytes([1; 16]),
                },
                device_id: Uuid::from_bytes([1; 16]),
            }],
            sequences: vec![1],
            device_id: Uuid::from_bytes([1; 16]),
        }
    }

    #[test]
    fn pack_splits_payload_into_multiple_chunks_when_requested() {
        let payload = pack(&delta_set(), &crate::crypto::keys::WorkspaceKey([5; 32]), 8)
            .expect("packing should succeed");
        assert!(payload.chunks.len() > 1);
        assert_eq!(
            payload_to_proto_chunks(&payload).len(),
            payload.chunks.len()
        );
    }
}

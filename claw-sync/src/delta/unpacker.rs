//! unpacker.rs — verifies, decrypts, decompresses, and deserialises inbound payloads.

use std::str::FromStr;

use lz4_flex::decompress_size_prepended;

use crate::{
    crypto::{cipher::decrypt_chunk, cipher::EncryptedBlob, keys::WorkspaceKey},
    delta::{
        extractor::{DeltaSet, EntityType},
        hasher::{content_hash, state_hash},
        packer::{PayloadChunk, SyncPayload},
    },
    error::{SyncError, SyncResult},
    proto::clawsync::v1::DeltaChunk,
};

/// Configuration for inbound delta unpacking.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnpackerConfig;

/// Unpacks an encrypted sync payload back into a delta set.
pub fn unpack(payload: &SyncPayload, workspace_key: &WorkspaceKey) -> SyncResult<DeltaSet> {
    let ordered_chunks = ordered_chunks(payload)?;
    let chunk_hashes: Vec<[u8; 32]> = ordered_chunks
        .iter()
        .map(|chunk| chunk.payload_hash)
        .collect();
    let computed_total_hash = state_hash(&chunk_hashes);
    if computed_total_hash != payload.total_hash {
        return Err(SyncError::Validation(
            "payload total hash verification failed".into(),
        ));
    }

    let mut encrypted_bytes = Vec::new();
    for chunk in ordered_chunks {
        encrypted_bytes.extend_from_slice(&chunk.encrypted_payload);
    }

    let blob = EncryptedBlob::from_bytes(&encrypted_bytes)?;
    let compressed = decrypt_chunk(&blob, workspace_key, &payload.chunk_id)?;
    let json = decompress_size_prepended(&compressed)
        .map_err(|error| SyncError::Transport(format!("lz4 decompression failed: {error}")))?;
    Ok(serde_json::from_slice(&json)?)
}

/// Converts protobuf chunks into a validated `SyncPayload`.
pub fn chunks_from_proto(chunks: Vec<DeltaChunk>) -> SyncResult<SyncPayload> {
    if chunks.is_empty() {
        return Err(SyncError::Validation(
            "cannot build a payload from zero chunks".into(),
        ));
    }

    let first_chunk_id = chunks[0].chunk_id.clone();
    let first_entity_type = chunks[0].entity_type.clone();
    let entity_type = EntityType::from_str(&first_entity_type)?;
    let total = chunks[0].total;
    if total <= 0 {
        return Err(SyncError::Validation(
            "payload chunk total must be greater than zero".into(),
        ));
    }

    let mut payload_chunks = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        if chunk.chunk_id != first_chunk_id {
            return Err(SyncError::Validation(
                "all proto chunks must share the same chunk_id".into(),
            ));
        }
        if chunk.entity_type != first_entity_type {
            return Err(SyncError::Validation(
                "all proto chunks must share the same entity_type".into(),
            ));
        }
        if chunk.total != total {
            return Err(SyncError::Validation(
                "all proto chunks must agree on total chunk count".into(),
            ));
        }
        let payload_hash: [u8; 32] = chunk
            .payload_hash
            .as_slice()
            .try_into()
            .map_err(|_| SyncError::Validation("chunk payload hash must be 32 bytes".into()))?;
        payload_chunks.push(PayloadChunk {
            seq: chunk.seq as u32,
            total: chunk.total as u32,
            encrypted_payload: chunk.encrypted_payload,
            payload_hash,
        });
    }

    let ordered = ordered_chunks(&SyncPayload {
        chunk_id: first_chunk_id.clone(),
        entity_type,
        chunks: payload_chunks.clone(),
        total_hash: [0; 32],
        sequences: Vec::new(),
    })?;
    let hashes: Vec<[u8; 32]> = ordered.iter().map(|chunk| chunk.payload_hash).collect();

    Ok(SyncPayload {
        chunk_id: first_chunk_id,
        entity_type,
        chunks: payload_chunks,
        total_hash: state_hash(&hashes),
        sequences: Vec::new(),
    })
}

fn ordered_chunks(payload: &SyncPayload) -> SyncResult<Vec<PayloadChunk>> {
    if payload.chunks.is_empty() {
        return Err(SyncError::Validation(
            "payload must contain at least one chunk".into(),
        ));
    }

    let mut chunks = payload.chunks.clone();
    chunks.sort_by_key(|chunk| chunk.seq);
    let expected_total = chunks[0].total;
    if expected_total == 0 || expected_total as usize != chunks.len() {
        return Err(SyncError::Validation(
            "payload chunk total does not match actual chunk count".into(),
        ));
    }

    for (index, chunk) in chunks.iter().enumerate() {
        if chunk.total != expected_total {
            return Err(SyncError::Validation(
                "payload chunks disagree on total count".into(),
            ));
        }
        if chunk.seq != index as u32 {
            return Err(SyncError::Validation(
                "payload chunk sequence ordering is invalid".into(),
            ));
        }
        if content_hash(&chunk.encrypted_payload) != chunk.payload_hash {
            return Err(SyncError::Validation(
                "payload chunk hash verification failed".into(),
            ));
        }
    }

    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{chunks_from_proto, unpack};
    use crate::{
        crdt::{clock::HlcTimestamp, ops::CrdtOp},
        delta::{
            extractor::{DeltaSet, EntityType},
            packer::{pack, payload_to_proto_chunks},
        },
    };

    fn delta_set() -> DeltaSet {
        DeltaSet {
            entity_type: EntityType::Sessions,
            ops: vec![CrdtOp::Insert {
                entity_id: "session-1".into(),
                payload: json!({ "label": "alpha" }),
                hlc: HlcTimestamp {
                    logical_ms: 1,
                    counter: 0,
                    node_id: Uuid::from_bytes([2; 16]),
                },
                device_id: Uuid::from_bytes([2; 16]),
            }],
            sequences: vec![11],
            device_id: Uuid::from_bytes([2; 16]),
        }
    }

    #[test]
    fn packed_payload_round_trips_through_proto_and_unpack() {
        let key = crate::crypto::keys::WorkspaceKey([9; 32]);
        let payload = pack(&delta_set(), &key, 16).expect("packing should succeed");
        let proto_chunks = payload_to_proto_chunks(&payload);
        let reparsed = chunks_from_proto(proto_chunks).expect("proto chunks should parse");

        let delta = unpack(&reparsed, &key).expect("unpack should succeed");
        assert_eq!(delta.entity_type, EntityType::Sessions);
        assert_eq!(delta.sequences, vec![11]);
        assert_eq!(delta.ops.len(), 1);
    }

    #[test]
    fn unpack_rejects_tampered_chunk_hash() {
        let key = crate::crypto::keys::WorkspaceKey([10; 32]);
        let mut payload = pack(&delta_set(), &key, 16).expect("packing should succeed");
        payload.chunks[0].encrypted_payload[0] ^= 0xAA;

        let error = unpack(&payload, &key).expect_err("tampered payload must fail");
        assert!(
            matches!(error, crate::error::SyncError::Validation(message) if message.contains("chunk hash"))
        );
    }
}

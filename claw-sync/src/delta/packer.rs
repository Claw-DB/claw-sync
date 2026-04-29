//! packer.rs — serialises and LZ4-compresses a `DeltaSet` into a `SyncPayload`.

use lz4_flex::compress_prepend_size;

use crate::{delta::extractor::DeltaSet, error::SyncResult};

/// A compressed, serialised representation of a `DeltaSet` ready for encryption.
pub struct SyncPayload {
    /// Raw compressed bytes of the serialised delta.
    pub compressed: Vec<u8>,
}

/// Pack a [`DeltaSet`] into a [`SyncPayload`] via JSON serialisation and LZ4 compression.
pub fn pack(delta: &DeltaSet) -> SyncResult<SyncPayload> {
    let json = serde_json::to_vec(delta)?;
    let compressed = compress_prepend_size(&json);
    Ok(SyncPayload { compressed })
}

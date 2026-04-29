//! unpacker.rs — decompresses and deserialises an incoming `SyncPayload` into a `DeltaSet`.

use lz4_flex::decompress_size_prepended;

use crate::{delta::extractor::DeltaSet, error::{SyncError, SyncResult}};

/// Decompress and deserialise raw bytes from the wire into a [`DeltaSet`].
pub fn unpack(compressed: &[u8]) -> SyncResult<DeltaSet> {
    let json = decompress_size_prepended(compressed)
        .map_err(|e| SyncError::Crypto(format!("lz4 decompression failed: {e}")))?;
    let delta = serde_json::from_slice(&json)?;
    Ok(delta)
}

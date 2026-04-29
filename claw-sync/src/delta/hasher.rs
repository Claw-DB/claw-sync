//! hasher.rs — BLAKE3 content addressing for delta deduplication.

/// Compute a BLAKE3 hash of `data` and return it as 32 raw bytes.
pub fn content_hash(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

/// Return the hex-encoded BLAKE3 hash of `data`.
pub fn content_hash_hex(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

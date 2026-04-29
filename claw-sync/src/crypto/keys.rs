//! keys.rs — key generation, BLAKE3-based KDF derivation, and key rotation helpers.

/// Derive a 32-byte subkey using BLAKE3's key-derivation function.
///
/// `context` must be a globally unique, hardcoded string for each key purpose.
pub fn derive_subkey(master: &[u8], context: &str) -> [u8; 32] {
    blake3::derive_key(context, master)
}

/// Generate a random 32-byte symmetric key suitable for XSalsa20-Poly1305.
pub fn generate_symmetric_key() -> crate::error::SyncResult<[u8; 32]> {
    sodiumoxide::init()
        .map_err(|_| crate::error::SyncError::Crypto("sodiumoxide init failed".into()))?;
    let key = sodiumoxide::crypto::secretbox::gen_key();
    Ok(key.0)
}

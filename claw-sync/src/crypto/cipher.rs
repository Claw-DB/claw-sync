//! cipher.rs — XSalsa20-Poly1305 authenticated encryption and decryption helpers.

use sodiumoxide::crypto::secretbox;

use crate::error::{SyncError, SyncResult};

/// Encrypt `plaintext` with `key` using XSalsa20-Poly1305; prepends the random nonce.
pub fn encrypt(plaintext: &[u8], key: &[u8; 32]) -> SyncResult<Vec<u8>> {
    sodiumoxide::init().map_err(|_| SyncError::Crypto("sodiumoxide init failed".into()))?;
    let key = secretbox::Key(*key);
    let nonce = secretbox::gen_nonce();
    let ciphertext = secretbox::seal(plaintext, &nonce, &key);
    let mut out = nonce.0.to_vec();
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// Decrypt a nonce-prefixed ciphertext produced by [`encrypt`].
pub fn decrypt(data: &[u8], key: &[u8; 32]) -> SyncResult<Vec<u8>> {
    const NONCE_LEN: usize = secretbox::NONCEBYTES;
    if data.len() < NONCE_LEN {
        return Err(SyncError::Crypto("ciphertext too short".into()));
    }
    let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
    let nonce = secretbox::Nonce::from_slice(nonce_bytes)
        .ok_or_else(|| SyncError::Crypto("invalid nonce".into()))?;
    let key = secretbox::Key(*key);
    secretbox::open(ciphertext, &nonce, &key)
        .map_err(|_| SyncError::Crypto("decryption failed".into()))
}

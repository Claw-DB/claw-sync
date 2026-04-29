//! signing.rs — Ed25519 sign and verify helpers for operation log entries.

use sodiumoxide::crypto::sign;

use crate::error::{SyncError, SyncResult};

/// Sign `message` with the provided Ed25519 secret key bytes; returns a detached signature.
pub fn sign_detached(message: &[u8], secret_key_bytes: &[u8]) -> SyncResult<Vec<u8>> {
    let sk = sign::SecretKey::from_slice(secret_key_bytes)
        .ok_or_else(|| SyncError::Crypto("invalid secret key".into()))?;
    Ok(sign::sign_detached(message, &sk).as_ref().to_vec())
}

/// Verify a detached Ed25519 `signature` over `message` using the provided public key bytes.
pub fn verify_detached(
    message: &[u8],
    signature_bytes: &[u8],
    public_key_bytes: &[u8],
) -> SyncResult<bool> {
    let pk = sign::PublicKey::from_slice(public_key_bytes)
        .ok_or_else(|| SyncError::Crypto("invalid public key".into()))?;
    let sig = sign::Signature::from_bytes(signature_bytes)
        .map_err(|_| SyncError::Crypto("invalid signature bytes".into()))?;
    Ok(sign::verify_detached(&sig, message, &pk))
}

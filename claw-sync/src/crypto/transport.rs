//! transport.rs — X25519 Diffie-Hellman key exchange for per-session encryption.

use sodiumoxide::crypto::box_;

use crate::error::{SyncError, SyncResult};

/// An ephemeral X25519 keypair used for one session key exchange.
pub struct SessionKeypair {
    /// Public half of the ephemeral keypair; share this with the peer.
    pub public_key: Vec<u8>,
    secret_key: box_::SecretKey,
}

impl SessionKeypair {
    /// Generate a fresh ephemeral X25519 keypair.
    pub fn generate() -> SyncResult<Self> {
        sodiumoxide::init()
            .map_err(|_| SyncError::Crypto("sodiumoxide init failed".into()))?;
        let (pk, sk) = box_::gen_keypair();
        Ok(Self { public_key: pk.as_ref().to_vec(), secret_key: sk })
    }

    /// Derive a 32-byte shared session key from this keypair and a peer public key.
    pub fn derive_shared_key(&self, peer_public_key_bytes: &[u8]) -> SyncResult<[u8; 32]> {
        let peer_pk = box_::PublicKey::from_slice(peer_public_key_bytes)
            .ok_or_else(|| SyncError::Crypto("invalid peer public key".into()))?;
        let precomputed = box_::precompute(&peer_pk, &self.secret_key);
        Ok(precomputed.0)
    }
}

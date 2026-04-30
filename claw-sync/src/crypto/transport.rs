//! transport.rs — X25519 session key exchange and peer-to-peer payload encryption.

use sodiumoxide::{crypto::box_::curve25519xsalsa20poly1305 as x25519, randombytes::randombytes};

pub use sodiumoxide::crypto::box_::curve25519xsalsa20poly1305::PrecomputedKey;

use crate::{
    crypto::{
        cipher::EncryptedBlob,
        ensure_sodium_initialized,
        keys::{SessionKey, SESSION_KEY_LEN},
    },
    error::{SyncError, SyncResult},
};

/// X25519 keypair used for transport-level shared-secret derivation.
pub struct X25519Keypair {
    /// Public X25519 key shared with a remote peer.
    pub public: x25519::PublicKey,
    /// Secret X25519 key held locally.
    pub secret: x25519::SecretKey,
}

/// Backward-compatible alias for the older transport keypair name.
pub type SessionKeypair = X25519Keypair;

impl X25519Keypair {
    /// Generates a fresh transport keypair.
    pub fn generate() -> SyncResult<Self> {
        ensure_sodium_initialized()?;
        Ok(generate_transport_keypair())
    }

    /// Derives the shared key bytes with a peer public key.
    pub fn derive_shared_key(
        &self,
        peer_public_key_bytes: &[u8],
    ) -> SyncResult<[u8; SESSION_KEY_LEN]> {
        let peer = x25519::PublicKey::from_slice(peer_public_key_bytes)
            .ok_or_else(|| SyncError::Crypto("invalid peer public key".into()))?;
        Ok(*derive_session_key(&self.secret, &peer).as_bytes())
    }
}

fn split_box_ciphertext(ciphertext: Vec<u8>) -> EncryptedBlob {
    let (tag, body) = ciphertext.split_at(x25519::MACBYTES);
    EncryptedBlob {
        nonce: [0_u8; x25519::NONCEBYTES],
        ciphertext: body.to_vec(),
        tag: tag.try_into().expect("crypto_box tag has fixed size"),
    }
}

fn nonce_from_bytes(bytes: [u8; x25519::NONCEBYTES]) -> SyncResult<x25519::Nonce> {
    x25519::Nonce::from_slice(&bytes)
        .ok_or_else(|| SyncError::Crypto("invalid transport nonce".into()))
}

/// Generates a fresh transport X25519 keypair.
pub fn generate_transport_keypair() -> X25519Keypair {
    let _ = sodiumoxide::init();
    let (public, secret) = x25519::gen_keypair();
    X25519Keypair { public, secret }
}

/// Derives a 32-byte session key from an X25519 secret key and a peer public key.
pub fn derive_session_key(
    our_secret: &x25519::SecretKey,
    their_public: &x25519::PublicKey,
) -> SessionKey {
    let precomputed = x25519::precompute(their_public, our_secret);
    SessionKey::from_bytes(precomputed.0)
}

/// Encrypts a payload for a peer using a precomputed X25519 shared key.
pub fn encrypt_for_peer(
    plaintext: &[u8],
    precomputed: &PrecomputedKey,
) -> SyncResult<EncryptedBlob> {
    ensure_sodium_initialized()?;

    let nonce_bytes: [u8; x25519::NONCEBYTES] = randombytes(x25519::NONCEBYTES)
        .try_into()
        .expect("randombytes returned unexpected transport nonce length");
    let nonce = nonce_from_bytes(nonce_bytes)?;
    let mut blob = split_box_ciphertext(x25519::seal_precomputed(plaintext, &nonce, precomputed));
    blob.nonce = nonce.0;
    Ok(blob)
}

/// Decrypts a payload from a peer using a precomputed X25519 shared key.
pub fn decrypt_from_peer(
    blob: &EncryptedBlob,
    precomputed: &PrecomputedKey,
) -> SyncResult<Vec<u8>> {
    ensure_sodium_initialized()?;

    let nonce = nonce_from_bytes(blob.nonce)?;
    let mut combined = Vec::with_capacity(x25519::MACBYTES + blob.ciphertext.len());
    combined.extend_from_slice(&blob.tag);
    combined.extend_from_slice(&blob.ciphertext);
    x25519::open_precomputed(&combined, &nonce, precomputed)
        .map_err(|_| SyncError::Crypto("decryption failed: bad key or tampered payload".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_secret_is_symmetric() {
        let alice = generate_transport_keypair();
        let bob = generate_transport_keypair();

        let alice_key = derive_session_key(&alice.secret, &bob.public);
        let bob_key = derive_session_key(&bob.secret, &alice.public);
        assert_eq!(alice_key.as_bytes(), bob_key.as_bytes());
    }

    #[test]
    fn encrypt_for_peer_round_trip() {
        let alice = generate_transport_keypair();
        let bob = generate_transport_keypair();
        let alice_precomputed = x25519::precompute(&bob.public, &alice.secret);
        let bob_precomputed = x25519::precompute(&alice.public, &bob.secret);

        let blob = encrypt_for_peer(b"transport payload", &alice_precomputed)
            .expect("transport encryption should succeed");
        let plaintext = decrypt_from_peer(&blob, &bob_precomputed)
            .expect("transport decryption should succeed");
        assert_eq!(plaintext, b"transport payload");
    }
}

//! cipher.rs — authenticated XSalsa20-Poly1305 encryption for sync payloads.

use sodiumoxide::{crypto::secretbox, randombytes::randombytes};

use crate::{
    crypto::{
        ensure_sodium_initialized,
        keys::{derive_chunk_key, SessionKey, WorkspaceKey},
    },
    error::{SyncError, SyncResult},
};

const CIPHERTEXT_LEN_PREFIX_LEN: usize = 4;
const CHUNK_KEY_CONTEXT: &[u8] = b"claw-sync chunk encryption v1";

/// A serialized secretbox payload split into nonce, ciphertext body, and tag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedBlob {
    /// Random XSalsa20 nonce.
    pub nonce: [u8; secretbox::NONCEBYTES],
    /// Ciphertext bytes without the detached authenticator.
    pub ciphertext: Vec<u8>,
    /// Detached Poly1305 authenticator.
    pub tag: [u8; secretbox::MACBYTES],
}

impl EncryptedBlob {
    /// Serializes the blob as `ciphertext_len || nonce || ciphertext || tag`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            CIPHERTEXT_LEN_PREFIX_LEN
                + secretbox::NONCEBYTES
                + self.ciphertext.len()
                + secretbox::MACBYTES,
        );
        out.extend_from_slice(&(self.ciphertext.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.nonce);
        out.extend_from_slice(&self.ciphertext);
        out.extend_from_slice(&self.tag);
        out
    }

    /// Parses the `ciphertext_len || nonce || ciphertext || tag` blob format.
    pub fn from_bytes(raw: &[u8]) -> SyncResult<Self> {
        let minimum_len = CIPHERTEXT_LEN_PREFIX_LEN + secretbox::NONCEBYTES + secretbox::MACBYTES;
        if raw.len() < minimum_len {
            return Err(SyncError::Crypto("encrypted blob is too short".into()));
        }

        let ciphertext_len = u32::from_le_bytes(
            raw[..CIPHERTEXT_LEN_PREFIX_LEN]
                .try_into()
                .expect("ciphertext length prefix slice has fixed size"),
        ) as usize;
        let expected_len = minimum_len + ciphertext_len;
        if raw.len() != expected_len {
            return Err(SyncError::Crypto(
                "encrypted blob length prefix does not match payload size".into(),
            ));
        }

        let nonce_start = CIPHERTEXT_LEN_PREFIX_LEN;
        let nonce_end = nonce_start + secretbox::NONCEBYTES;
        let ciphertext_end = nonce_end + ciphertext_len;
        let nonce = raw[nonce_start..nonce_end]
            .try_into()
            .expect("nonce slice has fixed size");
        let ciphertext = raw[nonce_end..ciphertext_end].to_vec();
        let tag = raw[ciphertext_end..]
            .try_into()
            .expect("tag slice has fixed size");

        Ok(Self {
            nonce,
            ciphertext,
            tag,
        })
    }

    fn combined_ciphertext(&self) -> Vec<u8> {
        let mut combined = Vec::with_capacity(secretbox::MACBYTES + self.ciphertext.len());
        combined.extend_from_slice(&self.tag);
        combined.extend_from_slice(&self.ciphertext);
        combined
    }
}

fn as_secretbox_key(key: &SessionKey) -> SyncResult<secretbox::Key> {
    secretbox::Key::from_slice(key.as_bytes())
        .ok_or_else(|| SyncError::Crypto("invalid session key length".into()))
}

/// Encrypts plaintext with XSalsa20-Poly1305 and returns a detached blob.
pub fn encrypt(plaintext: &[u8], key: &SessionKey) -> SyncResult<EncryptedBlob> {
    ensure_sodium_initialized()?;

    let nonce_bytes = randombytes(secretbox::NONCEBYTES);
    let nonce = secretbox::Nonce::from_slice(&nonce_bytes)
        .ok_or_else(|| SyncError::Crypto("failed to construct encryption nonce".into()))?;
    let sealed = secretbox::seal(plaintext, &nonce, &as_secretbox_key(key)?);
    let (tag, ciphertext) = sealed.split_at(secretbox::MACBYTES);

    Ok(EncryptedBlob {
        nonce: nonce.0,
        ciphertext: ciphertext.to_vec(),
        tag: tag.try_into().expect("secretbox tag has fixed size"),
    })
}

/// Decrypts an authenticated blob with XSalsa20-Poly1305.
pub fn decrypt(blob: &EncryptedBlob, key: &SessionKey) -> SyncResult<Vec<u8>> {
    ensure_sodium_initialized()?;

    let nonce = secretbox::Nonce::from_slice(&blob.nonce)
        .ok_or_else(|| SyncError::Crypto("invalid encrypted blob nonce".into()))?;
    secretbox::open(&blob.combined_ciphertext(), &nonce, &as_secretbox_key(key)?)
        .map_err(|_| SyncError::Crypto("decryption failed: bad key or tampered payload".into()))
}

/// Encrypts a chunk using a deterministic key derived from the workspace key and chunk id.
pub fn encrypt_chunk(
    chunk_data: &[u8],
    workspace_key: &WorkspaceKey,
    chunk_id: &str,
) -> SyncResult<EncryptedBlob> {
    let chunk_key = derive_chunk_key(workspace_key, chunk_id, CHUNK_KEY_CONTEXT)?;
    encrypt(chunk_data, &chunk_key)
}

/// Decrypts a chunk encrypted with [`encrypt_chunk`].
pub fn decrypt_chunk(
    blob: &EncryptedBlob,
    workspace_key: &WorkspaceKey,
    chunk_id: &str,
) -> SyncResult<Vec<u8>> {
    let chunk_key = derive_chunk_key(workspace_key, chunk_id, CHUNK_KEY_CONTEXT)?;
    decrypt(blob, &chunk_key)
}

/// Computes a BLAKE3 keyed hash for payload integrity checks.
pub fn compute_hmac(data: &[u8], key: &WorkspaceKey) -> [u8; 32] {
    *blake3::keyed_hash(key.as_bytes(), data).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::keys::{derive_chunk_key, SessionKey, WorkspaceKey};

    fn session_key(byte: u8) -> SessionKey {
        SessionKey::from_bytes([byte; 32])
    }

    fn workspace_key(byte: u8) -> WorkspaceKey {
        WorkspaceKey([byte; 32])
    }

    #[test]
    fn encrypt_decrypt_round_trip() {
        let blob = encrypt(b"sync payload", &session_key(7)).expect("encryption should succeed");
        let plaintext = decrypt(&blob, &session_key(7)).expect("decryption should succeed");
        assert_eq!(plaintext, b"sync payload");
    }

    #[test]
    fn encrypt_decrypt_empty_payload() {
        let blob = encrypt(&[], &session_key(8)).expect("empty encryption should succeed");
        let plaintext = decrypt(&blob, &session_key(8)).expect("empty decryption should succeed");
        assert!(plaintext.is_empty());
    }

    #[test]
    fn tampered_ciphertext_returns_error() {
        let mut blob = encrypt(b"chunk bytes", &session_key(9)).expect("encryption should succeed");
        blob.ciphertext[0] ^= 0x55;

        let error = decrypt(&blob, &session_key(9)).expect_err("tampered ciphertext must fail");
        assert!(
            matches!(error, SyncError::Crypto(message) if message.contains("decryption failed"))
        );
    }

    #[test]
    fn tampered_tag_returns_error() {
        let mut blob =
            encrypt(b"chunk bytes", &session_key(10)).expect("encryption should succeed");
        blob.tag[0] ^= 0x10;

        let error = decrypt(&blob, &session_key(10)).expect_err("tampered tag must fail");
        assert!(
            matches!(error, SyncError::Crypto(message) if message.contains("decryption failed"))
        );
    }

    #[test]
    fn wrong_key_returns_error() {
        let blob = encrypt(b"chunk bytes", &session_key(11)).expect("encryption should succeed");
        let error = decrypt(&blob, &session_key(12)).expect_err("wrong key must fail");
        assert!(
            matches!(error, SyncError::Crypto(message) if message.contains("decryption failed"))
        );
    }

    #[test]
    fn blob_serialization_round_trip() {
        let blob =
            encrypt(b"serialized payload", &session_key(13)).expect("encryption should succeed");
        let reparsed =
            EncryptedBlob::from_bytes(&blob.to_bytes()).expect("blob bytes should parse");
        assert_eq!(reparsed, blob);
    }

    #[test]
    fn blob_from_bytes_rejects_short_input() {
        let error = EncryptedBlob::from_bytes(&[0_u8; 8]).expect_err("short payload must fail");
        assert!(matches!(error, SyncError::Crypto(message) if message.contains("too short")));
    }

    #[test]
    fn blob_from_bytes_rejects_length_mismatch() {
        let mut raw = encrypt(b"abcd", &session_key(14))
            .expect("encryption should succeed")
            .to_bytes();
        raw[0..4].copy_from_slice(&99_u32.to_le_bytes());

        let error = EncryptedBlob::from_bytes(&raw).expect_err("bad length prefix must fail");
        assert!(matches!(error, SyncError::Crypto(message) if message.contains("length prefix")));
    }

    #[test]
    fn chunk_key_derivation_is_deterministic() {
        let key_a = derive_chunk_key(&workspace_key(15), "chunk-42", CHUNK_KEY_CONTEXT)
            .expect("chunk key derivation should succeed");
        let key_b = derive_chunk_key(&workspace_key(15), "chunk-42", CHUNK_KEY_CONTEXT)
            .expect("chunk key derivation should succeed");
        assert_eq!(key_a.as_bytes(), key_b.as_bytes());
    }

    #[test]
    fn encrypt_chunk_decrypt_chunk_round_trip() {
        let workspace_key = workspace_key(16);
        let blob = encrypt_chunk(b"chunk payload", &workspace_key, "chunk-a")
            .expect("chunk encryption should succeed");
        let plaintext = decrypt_chunk(&blob, &workspace_key, "chunk-a")
            .expect("chunk decryption should succeed");
        assert_eq!(plaintext, b"chunk payload");
    }
}

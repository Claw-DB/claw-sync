//! keys.rs — sodiumoxide-backed key generation, derivation, storage, and rotation.

use std::{
    fmt, fs,
    path::{Path, PathBuf},
    str,
};

use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::sign::ed25519;
use sodiumoxide::randombytes::randombytes;
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

use crate::{
    crypto::{cipher::EncryptedBlob, ensure_sodium_initialized},
    error::{SyncError, SyncResult},
};

const WORKSPACE_KEY_MATERIAL_CONTEXT: &str = "claw-sync workspace key material v1";
const DEVICE_KEY_CONTEXT: &str = "claw-sync device key v1";
const KEY_ROTATION_CONTEXT: &str = "claw-sync key rotation v1";
const KEYSTORE_FILE_NAME: &str = "keystore.bin";

/// Number of bytes in a workspace encryption key.
pub const WORKSPACE_KEY_LEN: usize = 32;

/// Number of bytes in an Ed25519 secret signing key.
pub const SIGNING_KEY_LEN: usize = 64;

/// Number of bytes in a derived session key.
pub const SESSION_KEY_LEN: usize = 32;

/// Symmetric key used for workspace encryption.
#[derive(Clone, PartialEq, Eq, Zeroize)]
#[zeroize(drop)]
pub struct WorkspaceKey(
    /// Raw 32-byte key material.
    pub [u8; WORKSPACE_KEY_LEN],
);

impl WorkspaceKey {
    /// Returns the raw key bytes.
    pub fn as_bytes(&self) -> &[u8; WORKSPACE_KEY_LEN] {
        &self.0
    }
}

impl fmt::Debug for WorkspaceKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkspaceKey([REDACTED])")
    }
}

/// Ed25519 signing keypair used for audit and transport signatures.
pub struct SigningKeypair {
    /// Public Ed25519 verification key.
    pub public: ed25519::PublicKey,
    /// Secret Ed25519 signing key.
    pub secret: ed25519::SecretKey,
}

impl Zeroize for SigningKeypair {
    fn zeroize(&mut self) {
        self.secret.0.zeroize();
    }
}

impl Drop for SigningKeypair {
    fn drop(&mut self) {
        self.zeroize();
    }
}

impl fmt::Debug for SigningKeypair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SigningKeypair")
            .field("public", &self.public)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

/// Ephemeral per-session symmetric key derived from X25519 or BLAKE3 KDFs.
#[derive(Clone, PartialEq, Eq, Zeroize)]
#[zeroize(drop)]
pub struct SessionKey([u8; SESSION_KEY_LEN]);

impl SessionKey {
    /// Returns the raw key bytes.
    pub fn as_bytes(&self) -> &[u8; SESSION_KEY_LEN] {
        &self.0
    }

    pub(crate) fn from_bytes(bytes: [u8; SESSION_KEY_LEN]) -> Self {
        Self(bytes)
    }
}

impl fmt::Debug for SessionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionKey([REDACTED])")
    }
}

/// On-disk keystore for workspace and signing keys.
pub struct KeyStore {
    workspace_key: WorkspaceKey,
    signing_keypair: SigningKeypair,
    data_dir: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct StoredSigningKeypair {
    public_key: Vec<u8>,
    secret_key: Vec<u8>,
}

/// Generates a random workspace encryption key.
pub fn generate_workspace_key() -> WorkspaceKey {
    let _ = ensure_sodium_initialized();
    let raw = randombytes(WORKSPACE_KEY_LEN)
        .try_into()
        .expect("randombytes returned unexpected workspace key length");
    WorkspaceKey(raw)
}

/// Derives a deterministic per-chunk session key from a workspace key and chunk identifier.
pub fn derive_chunk_key(
    workspace_key: &WorkspaceKey,
    chunk_id: &str,
    context: &[u8],
) -> SyncResult<SessionKey> {
    let context = str::from_utf8(context).map_err(|_| {
        SyncError::Crypto("chunk key derivation context must be valid UTF-8".into())
    })?;
    let mut material = Zeroizing::new(Vec::with_capacity(WORKSPACE_KEY_LEN + chunk_id.len()));
    material.extend_from_slice(workspace_key.as_bytes());
    material.extend_from_slice(chunk_id.as_bytes());
    Ok(SessionKey::from_bytes(blake3::derive_key(
        context,
        material.as_slice(),
    )))
}

/// Derives a deterministic per-device session key from a workspace key and device id.
pub fn derive_device_key(workspace_key: &WorkspaceKey, device_id: Uuid) -> SessionKey {
    let mut material = Zeroizing::new(Vec::with_capacity(WORKSPACE_KEY_LEN + 16));
    material.extend_from_slice(workspace_key.as_bytes());
    material.extend_from_slice(device_id.as_bytes());
    SessionKey::from_bytes(blake3::derive_key(DEVICE_KEY_CONTEXT, material.as_slice()))
}

/// Generates a fresh Ed25519 signing keypair.
pub fn generate_signing_keypair() -> SyncResult<SigningKeypair> {
    ensure_sodium_initialized()?;
    let (public, secret) = ed25519::gen_keypair();
    Ok(SigningKeypair { public, secret })
}

/// Rotates a workspace key using the previous key and a caller-provided rotation nonce.
pub fn rotate_workspace_key(old_key: &WorkspaceKey, rotation_nonce: &[u8]) -> WorkspaceKey {
    let mut material = Zeroizing::new(Vec::with_capacity(WORKSPACE_KEY_LEN + rotation_nonce.len()));
    material.extend_from_slice(old_key.as_bytes());
    material.extend_from_slice(rotation_nonce);
    WorkspaceKey(blake3::derive_key(
        KEY_ROTATION_CONTEXT,
        material.as_slice(),
    ))
}

fn derive_workspace_key_from_material(workspace_key_material: &[u8]) -> WorkspaceKey {
    WorkspaceKey(blake3::derive_key(
        WORKSPACE_KEY_MATERIAL_CONTEXT,
        workspace_key_material,
    ))
}

fn keystore_path(data_dir: &Path) -> PathBuf {
    data_dir.join(KEYSTORE_FILE_NAME)
}

impl KeyStore {
    /// Loads an encrypted keystore from disk or creates a new one when absent.
    pub fn load_or_create(data_dir: &Path, workspace_key_material: &[u8]) -> SyncResult<Self> {
        ensure_sodium_initialized()?;

        let workspace_key = derive_workspace_key_from_material(workspace_key_material);
        let data_dir = data_dir.to_path_buf();
        let path = keystore_path(&data_dir);

        if path.exists() {
            let raw = fs::read(&path)?;
            let blob = EncryptedBlob::from_bytes(&raw)?;
            let encryption_key = SessionKey::from_bytes(*workspace_key.as_bytes());
            let plaintext = Zeroizing::new(crate::crypto::cipher::decrypt(&blob, &encryption_key)?);
            let stored: StoredSigningKeypair = serde_json::from_slice(plaintext.as_slice())?;
            let public = ed25519::PublicKey::from_slice(&stored.public_key)
                .ok_or_else(|| SyncError::Crypto("stored keystore public key is invalid".into()))?;
            let secret = ed25519::SecretKey::from_slice(&stored.secret_key)
                .ok_or_else(|| SyncError::Crypto("stored keystore secret key is invalid".into()))?;

            return Ok(Self {
                workspace_key,
                signing_keypair: SigningKeypair { public, secret },
                data_dir,
            });
        }

        fs::create_dir_all(&data_dir)?;
        let keystore = Self {
            workspace_key,
            signing_keypair: generate_signing_keypair()?,
            data_dir,
        };
        keystore.save()?;
        Ok(keystore)
    }

    /// Persists the signing keypair encrypted with the workspace key.
    pub fn save(&self) -> SyncResult<()> {
        fs::create_dir_all(&self.data_dir)?;

        let payload = StoredSigningKeypair {
            public_key: self.signing_keypair.public.0.to_vec(),
            secret_key: self.signing_keypair.secret.0.to_vec(),
        };
        let plaintext = Zeroizing::new(serde_json::to_vec(&payload)?);
        let encryption_key = SessionKey::from_bytes(*self.workspace_key.as_bytes());
        let blob = crate::crypto::cipher::encrypt(plaintext.as_slice(), &encryption_key)?;

        fs::write(keystore_path(&self.data_dir), blob.to_bytes())?;
        Ok(())
    }

    /// Returns the active workspace key.
    pub fn workspace_key(&self) -> &WorkspaceKey {
        &self.workspace_key
    }

    /// Returns the active signing keypair.
    pub fn signing_keypair(&self) -> &SigningKeypair {
        &self.signing_keypair
    }
}

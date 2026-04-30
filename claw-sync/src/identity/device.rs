//! device.rs — `DeviceIdentity`: device identifier, Ed25519 keypair, and hub registration.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    crypto::{keys::KeyStore, signing::sign_detached},
    error::SyncResult,
};

const DEVICE_IDENTITY_FILE_NAME: &str = "device_identity.json";

#[derive(Debug, Serialize, Deserialize)]
struct StoredDeviceIdentity {
    device_id: Uuid,
    public_key_bytes: Vec<u8>,
}

/// Holds the cryptographic identity and metadata for a single sync device.
#[derive(Debug, Clone)]
pub struct DeviceIdentity {
    /// Stable unique identifier assigned to this device.
    pub device_id: Uuid,
    /// Raw Ed25519 public key bytes used to verify signed messages from this device.
    pub public_key_bytes: Vec<u8>,
    secret_key_bytes: Vec<u8>,
}

impl DeviceIdentity {
    /// Loads the persisted device identity for `data_dir`, or creates one from the keystore.
    pub fn load_or_create(
        data_dir: &Path,
        device_id: Uuid,
        key_store: &KeyStore,
    ) -> SyncResult<Self> {
        fs::create_dir_all(data_dir)?;
        let path = identity_path(data_dir);
        if path.exists() {
            let stored: StoredDeviceIdentity = serde_json::from_slice(&fs::read(path)?)?;
            return Ok(Self::from_parts(
                stored.device_id,
                stored.public_key_bytes,
                key_store.signing_keypair().secret.0.to_vec(),
            ));
        }

        let identity = Self::from_parts(
            device_id,
            key_store.signing_keypair().public.0.to_vec(),
            key_store.signing_keypair().secret.0.to_vec(),
        );
        identity.save(data_dir)?;
        Ok(identity)
    }

    /// Builds a `DeviceIdentity` from persisted key material.
    pub fn from_parts(
        device_id: Uuid,
        public_key_bytes: Vec<u8>,
        secret_key_bytes: Vec<u8>,
    ) -> Self {
        Self {
            device_id,
            public_key_bytes,
            secret_key_bytes,
        }
    }

    /// Persists the stable public identity information to disk.
    pub fn save(&self, data_dir: &Path) -> SyncResult<()> {
        fs::create_dir_all(data_dir)?;
        fs::write(
            identity_path(data_dir),
            serde_json::to_vec_pretty(&StoredDeviceIdentity {
                device_id: self.device_id,
                public_key_bytes: self.public_key_bytes.clone(),
            })?,
        )?;
        Ok(())
    }

    /// Generate a new `DeviceIdentity` with a fresh random device ID and keypair.
    pub fn generate() -> SyncResult<Self> {
        sodiumoxide::init().map_err(|_| {
            crate::error::SyncError::Crypto("sodiumoxide initialisation failed".into())
        })?;
        let (pk, sk) = sodiumoxide::crypto::sign::gen_keypair();
        Ok(Self {
            device_id: Uuid::new_v4(),
            public_key_bytes: pk.as_ref().to_vec(),
            secret_key_bytes: sk.as_ref().to_vec(),
        })
    }

    /// Produces a detached Ed25519 signature over a registration challenge.
    pub fn sign_challenge(&self, challenge: &[u8]) -> SyncResult<Vec<u8>> {
        sign_detached(challenge, &self.secret_key_bytes)
    }
}

fn identity_path(data_dir: &Path) -> PathBuf {
    data_dir.join(DEVICE_IDENTITY_FILE_NAME)
}

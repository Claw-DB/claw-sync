//! device.rs — `DeviceIdentity`: device identifier, Ed25519 keypair, and hub registration.

use uuid::Uuid;

/// Holds the cryptographic identity and metadata for a single sync device.
#[derive(Debug, Clone)]
pub struct DeviceIdentity {
    /// Stable unique identifier assigned to this device.
    pub device_id: Uuid,
    /// Raw Ed25519 public key bytes used to verify signed messages from this device.
    pub public_key_bytes: Vec<u8>,
}

impl DeviceIdentity {
    /// Generate a new `DeviceIdentity` with a fresh random device ID and keypair.
    pub fn generate() -> crate::error::SyncResult<Self> {
        sodiumoxide::init().map_err(|_| {
            crate::error::SyncError::Crypto("sodiumoxide initialisation failed".into())
        })?;
        let (pk, _sk) = sodiumoxide::crypto::sign::gen_keypair();
        Ok(Self {
            device_id: Uuid::new_v4(),
            public_key_bytes: pk.as_ref().to_vec(),
        })
    }
}

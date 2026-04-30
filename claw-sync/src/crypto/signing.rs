//! signing.rs — detached Ed25519 signing and signed JSON envelopes.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sodiumoxide::crypto::sign::ed25519;

use crate::{
    crypto::{ensure_sodium_initialized, keys::SigningKeypair},
    error::{SyncError, SyncResult},
};

/// Detached signature envelope for JSON payloads.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedEnvelope {
    /// Canonical JSON payload that was signed.
    pub payload_json: String,
    /// Base64-encoded detached Ed25519 signature.
    pub signature_b64: String,
    /// Base64-encoded Ed25519 public key used to produce the signature.
    pub signer_pub_key_b64: String,
    /// UTC timestamp when the payload was signed.
    pub signed_at: DateTime<Utc>,
}

fn decode_signature(signature: &[u8]) -> SyncResult<ed25519::Signature> {
    ed25519::Signature::from_bytes(signature)
        .map_err(|_| SyncError::Crypto("invalid signature bytes".into()))
}

/// Produces a detached Ed25519 signature for `message`.
pub fn sign(message: &[u8], keypair: &SigningKeypair) -> Vec<u8> {
    let _ = sodiumoxide::init();
    ed25519::sign_detached(message, &keypair.secret)
        .to_bytes()
        .to_vec()
}

/// Verifies a detached Ed25519 signature.
pub fn verify(message: &[u8], signature: &[u8], public_key: &ed25519::PublicKey) -> SyncResult<()> {
    ensure_sodium_initialized()?;

    let signature = decode_signature(signature)?;
    if ed25519::verify_detached(&signature, message, public_key) {
        Ok(())
    } else {
        Err(SyncError::Crypto("signature verification failed".into()))
    }
}

/// Serializes a payload to JSON and signs it into a detached envelope.
pub fn sign_json<T: Serialize>(
    payload: &T,
    keypair: &SigningKeypair,
) -> SyncResult<SignedEnvelope> {
    let payload_json = serde_json::to_string(payload)?;
    let signature = sign(payload_json.as_bytes(), keypair);

    Ok(SignedEnvelope {
        payload_json,
        signature_b64: STANDARD.encode(signature),
        signer_pub_key_b64: STANDARD.encode(keypair.public.0),
        signed_at: Utc::now(),
    })
}

impl SignedEnvelope {
    /// Verifies the embedded signature and decodes the payload.
    pub fn verify_and_decode<T: DeserializeOwned>(&self) -> SyncResult<T> {
        let signature = STANDARD
            .decode(&self.signature_b64)
            .map_err(|error| SyncError::Crypto(format!("invalid base64 signature: {error}")))?;
        let public_key_bytes = STANDARD
            .decode(&self.signer_pub_key_b64)
            .map_err(|error| SyncError::Crypto(format!("invalid base64 public key: {error}")))?;
        let public_key = ed25519::PublicKey::from_slice(&public_key_bytes)
            .ok_or_else(|| SyncError::Crypto("invalid signer public key".into()))?;

        verify(self.payload_json.as_bytes(), &signature, &public_key)?;
        Ok(serde_json::from_str(&self.payload_json)?)
    }
}

/// Backward-compatible detached signing helper for raw secret key bytes.
pub fn sign_detached(message: &[u8], secret_key_bytes: &[u8]) -> SyncResult<Vec<u8>> {
    ensure_sodium_initialized()?;
    let secret = ed25519::SecretKey::from_slice(secret_key_bytes)
        .ok_or_else(|| SyncError::Crypto("invalid secret key".into()))?;
    Ok(ed25519::sign_detached(message, &secret).to_bytes().to_vec())
}

/// Backward-compatible detached verification helper for raw public key bytes.
pub fn verify_detached(
    message: &[u8],
    signature_bytes: &[u8],
    public_key_bytes: &[u8],
) -> SyncResult<bool> {
    ensure_sodium_initialized()?;
    let public = ed25519::PublicKey::from_slice(public_key_bytes)
        .ok_or_else(|| SyncError::Crypto("invalid public key".into()))?;
    let signature = decode_signature(signature_bytes)?;
    Ok(ed25519::verify_detached(&signature, message, &public))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::keys::generate_signing_keypair;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct Payload {
        id: u32,
        name: String,
    }

    #[test]
    fn sign_verify_round_trip() {
        let keypair = generate_signing_keypair().expect("key generation should succeed");
        let signature = sign(b"audit-entry", &keypair);
        verify(b"audit-entry", &signature, &keypair.public).expect("signature should verify");
    }

    #[test]
    fn tampered_payload_fails_verify() {
        let keypair = generate_signing_keypair().expect("key generation should succeed");
        let signature = sign(b"audit-entry", &keypair);
        let error = verify(b"audit-entry-tampered", &signature, &keypair.public)
            .expect_err("tampered payload must not verify");
        assert!(
            matches!(error, SyncError::Crypto(message) if message.contains("verification failed"))
        );
    }

    #[test]
    fn signed_envelope_verify_and_decode_round_trip() {
        let keypair = generate_signing_keypair().expect("key generation should succeed");
        let payload = Payload {
            id: 7,
            name: "claw".into(),
        };

        let envelope = sign_json(&payload, &keypair).expect("envelope signing should succeed");
        let decoded: Payload = envelope
            .verify_and_decode()
            .expect("signed envelope should decode after verification");
        assert_eq!(decoded, payload);
    }
}

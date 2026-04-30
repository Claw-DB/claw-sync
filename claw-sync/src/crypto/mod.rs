//! crypto/mod.rs — re-exports for the crypto subsystem (keys, cipher, signing, transport).

pub mod cipher;
pub mod keys;
pub mod signing;
pub mod transport;

use crate::error::{SyncError, SyncResult};

pub(crate) fn ensure_sodium_initialized() -> SyncResult<()> {
    sodiumoxide::init().map_err(|_| SyncError::Crypto("sodiumoxide init failed".into()))
}

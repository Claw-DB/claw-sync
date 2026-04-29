//! error.rs — `SyncError` enum and `SyncResult<T>` type alias.

use thiserror::Error;

/// All error conditions that can arise within claw-sync.
#[derive(Debug, Error)]
pub enum SyncError {
    /// A required configuration value was missing or invalid.
    #[error("configuration error: {0}")]
    Config(String),

    /// A transport-layer error (e.g. gRPC or network failure).
    #[error("transport error: {0}")]
    Transport(String),

    /// An encryption or decryption failure.
    #[error("crypto error: {0}")]
    Crypto(String),

    /// A database operation failed.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// A serialisation or deserialisation failure.
    #[error("serialisation error: {0}")]
    Serialisation(#[from] serde_json::Error),

    /// An I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// An irreconcilable conflict was detected and escalated for manual resolution.
    #[error("conflict escalation: entity_id={entity_id}")]
    ConflictEscalation {
        /// The entity whose conflict could not be resolved automatically.
        entity_id: String,
    },
}

/// Convenience alias for `Result<T, SyncError>`.
pub type SyncResult<T> = Result<T, SyncError>;

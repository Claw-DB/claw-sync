//! error.rs — `SyncError` enum and `SyncResult<T>` type alias.

use thiserror::Error;

use crate::proto::clawsync::v1::ConflictRecord;

/// All error conditions that can arise within claw-sync.
#[derive(Debug, Error)]
pub enum SyncError {
    /// A required configuration value was missing or invalid.
    #[error("configuration error: {0}")]
    Config(String),

    /// Input data or persisted state was malformed or semantically invalid.
    #[error("validation error: {0}")]
    Validation(String),

    /// A transport-layer error (e.g. gRPC or network failure).
    #[error("transport error: {0}")]
    Transport(String),

    /// An encryption or decryption failure.
    #[error("crypto error: {0}")]
    Crypto(String),

    /// A database operation failed.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// A SQL migration failed.
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

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
        /// Full conflict details for subscribers and manual resolution.
        record: Box<ConflictRecord>,
    },
}

/// Convenience alias for `Result<T, SyncError>`.
pub type SyncResult<T> = Result<T, SyncError>;

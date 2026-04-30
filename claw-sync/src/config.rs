//! config.rs — `SyncConfig` builder with environment-variable fallback.

use std::path::PathBuf;

use uuid::Uuid;

use crate::error::{SyncError, SyncResult};

/// Runtime configuration for the claw-sync engine.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Identifier of the workspace being synchronised.
    pub workspace_id: Uuid,
    /// Stable device identifier used for auth and HLC timestamps.
    pub device_id: Uuid,
    /// gRPC endpoint of the remote sync hub.
    pub hub_endpoint: String,
    /// Root directory for local sync artefacts such as the queue and keystore.
    pub data_dir: PathBuf,
    /// Path to the local claw-core SQLite database.
    pub db_path: PathBuf,
    /// Whether the gRPC client should negotiate TLS.
    pub tls_enabled: bool,
    /// Channel connect timeout in seconds.
    pub connect_timeout_secs: u64,
    /// Per-request timeout in seconds.
    pub request_timeout_secs: u64,
    /// Background sync interval when no work is immediately pending.
    pub sync_interval_secs: u64,
    /// Heartbeat interval used by the reconnecting client.
    pub heartbeat_interval_secs: u64,
    /// Maximum retry attempts for transient failures.
    pub max_retries: u32,
    /// Base delay for retry scheduling in milliseconds.
    pub retry_base_ms: u64,
    /// Maximum number of delta rows to extract in one pass.
    pub max_delta_rows: usize,
    /// Maximum encoded bytes per outbound chunk.
    pub max_chunk_bytes: usize,
    /// Maximum pull chunks requested in one RPC.
    pub max_pull_chunks: u32,
    /// Maximum in-flight push chunks awaiting acknowledgement.
    pub max_push_inflight: usize,
}

impl SyncConfig {
    /// Constructs `SyncConfig` by reading well-known environment variables.
    pub fn from_env() -> SyncResult<Self> {
        Ok(Self {
            workspace_id: parse_uuid_env("CLAW_WORKSPACE_ID")?,
            device_id: optional_uuid_env("CLAW_DEVICE_ID")?.unwrap_or_else(Uuid::new_v4),
            hub_endpoint: std::env::var("CLAW_HUB_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:50051".into()),
            data_dir: PathBuf::from(
                std::env::var("CLAW_DATA_DIR").unwrap_or_else(|_| ".claw-sync".into()),
            ),
            db_path: PathBuf::from(
                std::env::var("CLAW_DB_PATH").unwrap_or_else(|_| "claw_sync.db".into()),
            ),
            tls_enabled: parse_bool_env("CLAW_TLS_ENABLED", false)?,
            connect_timeout_secs: parse_u64_env("CLAW_CONNECT_TIMEOUT_SECS", 10)?,
            request_timeout_secs: parse_u64_env("CLAW_REQUEST_TIMEOUT_SECS", 30)?,
            sync_interval_secs: parse_u64_env("CLAW_SYNC_INTERVAL_SECS", 30)?,
            heartbeat_interval_secs: parse_u64_env("CLAW_HEARTBEAT_INTERVAL_SECS", 15)?,
            max_retries: parse_u32_env("CLAW_MAX_RETRIES", 5)?,
            retry_base_ms: parse_u64_env("CLAW_RETRY_BASE_MS", 500)?,
            max_delta_rows: parse_usize_env("CLAW_MAX_DELTA_ROWS", 1_000)?,
            max_chunk_bytes: parse_usize_env("CLAW_MAX_CHUNK_BYTES", 64 * 1024)?,
            max_pull_chunks: parse_u32_env("CLAW_MAX_PULL_CHUNKS", 128)?,
            max_push_inflight: parse_usize_env("CLAW_MAX_PUSH_INFLIGHT", 4)?,
        })
    }
}

fn parse_uuid_env(name: &str) -> SyncResult<Uuid> {
    let raw = std::env::var(name).map_err(|_| SyncError::Config(format!("{name} not set")))?;
    Uuid::parse_str(&raw)
        .map_err(|error| SyncError::Config(format!("{name} must be a UUID: {error}")))
}

fn optional_uuid_env(name: &str) -> SyncResult<Option<Uuid>> {
    match std::env::var(name) {
        Ok(raw) => Uuid::parse_str(&raw)
            .map(Some)
            .map_err(|error| SyncError::Config(format!("{name} must be a UUID: {error}"))),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(SyncError::Config(format!("failed to read {name}: {error}"))),
    }
}

fn parse_bool_env(name: &str, default: bool) -> SyncResult<bool> {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(SyncError::Config(format!(
                "{name} must be one of true/false/1/0/yes/no/on/off"
            ))),
        },
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(SyncError::Config(format!("failed to read {name}: {error}"))),
    }
}

fn parse_u64_env(name: &str, default: u64) -> SyncResult<u64> {
    match std::env::var(name) {
        Ok(raw) => raw.parse::<u64>().map_err(|error| {
            SyncError::Config(format!("{name} must be an unsigned integer: {error}"))
        }),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(SyncError::Config(format!("failed to read {name}: {error}"))),
    }
}

fn parse_u32_env(name: &str, default: u32) -> SyncResult<u32> {
    match std::env::var(name) {
        Ok(raw) => raw.parse::<u32>().map_err(|error| {
            SyncError::Config(format!("{name} must be an unsigned integer: {error}"))
        }),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(SyncError::Config(format!("failed to read {name}: {error}"))),
    }
}

fn parse_usize_env(name: &str, default: usize) -> SyncResult<usize> {
    match std::env::var(name) {
        Ok(raw) => raw.parse::<usize>().map_err(|error| {
            SyncError::Config(format!("{name} must be an unsigned integer: {error}"))
        }),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(SyncError::Config(format!("failed to read {name}: {error}"))),
    }
}

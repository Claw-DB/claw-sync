//! main.rs — entry point for the `claw-sync` binary.

use claw_sync::config::SyncConfig;
use claw_sync::engine::SyncEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let _config = SyncConfig::from_env()?;
    let engine = SyncEngine::from_env().await?;
    engine.run().await?;
    Ok(())
}

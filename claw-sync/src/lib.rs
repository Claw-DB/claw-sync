//! claw-sync — public re-exports and crate root for the synchronisation engine.
//!
//! # Overview
//! This library provides encrypted delta replication, CRDT conflict resolution,
//! offline queueing, and resumable chunked sync for ClawDB workspaces.

#![deny(missing_docs, clippy::unwrap_used)]

pub mod audit;
pub mod config;
pub mod crdt;
pub mod crypto;
pub mod delta;
pub mod engine;
pub mod error;
pub mod identity;
pub mod queue;
pub mod sync;
pub mod transport;

/// Generated protobuf / gRPC types for the `clawsync.v1` package.
pub mod proto {
    /// Re-export of the generated `clawsync.v1` module.
    pub mod clawsync {
        /// Version 1 of the claw-sync protobuf API.
        pub mod v1 {
            // Generated code does not carry doc-comments; allow the lint here.
            #![allow(missing_docs, clippy::unwrap_used)]
            tonic::include_proto!("clawsync.v1");
        }
    }
}

pub use config::SyncConfig;
pub use engine::SyncEngine;
pub use error::{SyncError, SyncResult};

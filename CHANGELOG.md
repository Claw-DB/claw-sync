# Changelog

## 2026-05-01

### Workspace
- Removed duplicated in-repo claw-core copy and normalized workspace membership.

### Transport
- Added high-level hub client operations:
  - push_deltas with per-chunk result reporting.
  - pull_deltas with payload hash + decryption verification.
  - heartbeat_result with pull-required flag.
- Added transient/permanent transport error classification helpers.
- Added transport retry helper implementing capped exponential backoff + jitter.

### Queue
- Updated retry scheduler backoff to cap at 30s and use rand(0..base_ms) jitter.

### Conflict Handling
- Added conflict_id to protocol conflict records.
- Added conflict persistence in local metadata tables during escalation.
- Added manual conflict resolution API that updates conflict state and applies chosen values.

### Sync Hub Server
- Replaced no-op hub handlers with PostgreSQL-backed implementations for:
  - RegisterDevice
  - Push
  - Pull
  - Heartbeat
- Added durable hub tables:
  - hub_memory_log
  - hub_devices
- Added TLS support using CLAW_SYNC_TLS_CERT and CLAW_SYNC_TLS_KEY.
- Added structured tonic::Status metadata (error-kind) for error responses.

### Audit Integrity
- Added background audit-chain verification task every 6 hours with warning logs on failure.

### Repo Hygiene
- Expanded .gitignore for database files and non-Rust lock files while preserving Cargo.lock.
- Added Dependabot configuration for weekly Cargo updates.

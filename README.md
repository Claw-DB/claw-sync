# claw-sync

`claw-sync` is the replication and recovery layer for ClawDB: it observes local `claw-core` mutations, converts them into encrypted CRDT deltas, transports them to a sync hub, applies remote deltas back into local state, and preserves resumable checkpoints plus a signed audit chain so synchronization can recover cleanly after transport failure, process restart, or conflict escalation.

This repository does not vendor `claw-core`; use the published crate from crates.io (`claw-core = "0.1.0"`).

## Architecture

```text
[Device A: claw-core]
	-> DeltaExtractor
	-> Packer / Encryptor
	-> gRPC Push Stream
	-> [Sync Hub]

[Sync Hub]
	-> gRPC Pull Stream
	-> Unpacker / Decryptor
	-> DeltaApplier
	-> [Device B: claw-core]
```

## Security Model

`claw-sync` is designed so synchronization metadata and payload integrity are explicit rather than implicit.

- End-to-end payload confidentiality uses XSalsa20-Poly1305 for chunk encryption.
- Devices sign audit entries and transport challenges with Ed25519 keys.
- Session and peer transport material is derived through X25519-based exchange and per-context derivation helpers.
- BLAKE3 is used for content addressing, payload hashing, and audit-chain row linkage.
- Audit records are chained by previous-entry hashes and signed per entry so tampering is detectable after the fact.

## CRDT Conflict Resolution

`claw-sync` resolves ordinary concurrency automatically and escalates only when deterministic merge safety runs out.

- HLC ordering establishes cross-device causal precedence.
- Last-write-wins fields apply the newer HLC, then use device id as a deterministic tie-breaker.
- Tombstones propagate delete intent so stale inserts do not resurrect removed entities.
- Identical HLC collisions on the same field escalate as manual conflicts and are emitted through `SyncEvent::ConflictDetected`.

## Quick Start

```rust
use claw_sync::{SyncConfig, SyncEngine};
use tokio_util::sync::CancellationToken;

# async fn example(pool: sqlx::SqlitePool) -> claw_sync::SyncResult<()> {
let config = SyncConfig::from_env()?;
let engine = SyncEngine::new(config, pool).await?;

let shutdown_token = CancellationToken::new();
engine.start(shutdown_token.clone()).await?;

let round = engine.sync_now().await?;
println!("pushed={}, pulled={}", round.push.deltas_sent, round.pull.deltas_received);

engine.close().await?;
# Ok(()) }
```

## SyncConfig Reference

| Field | Purpose |
| --- | --- |
| `workspace_id` | Stable workspace UUID used for audit, checkpoint, and transport scoping. |
| `device_id` | Stable device UUID used for HLC ordering and registration identity. |
| `hub_endpoint` | Sync hub gRPC endpoint. |
| `data_dir` | Root directory for queue state, checkpoints, audit database, key material, and device identity files. |
| `db_path` | SQLite path for the local `claw-core` database being synchronized. |
| `tls_enabled` | Enables TLS when connecting to the sync hub. |
| `connect_timeout_secs` | Connection establishment timeout for the transport client. |
| `request_timeout_secs` | Per-request timeout for RPC operations. |
| `sync_interval_secs` | Background sync cadence used by `SyncEngine::start`. |
| `heartbeat_interval_secs` | Interval for heartbeat-based liveness and pull-required notifications. |
| `max_retries` | Number of transport retry attempts before a push falls back to the offline queue. |
| `retry_base_ms` | Base backoff duration for transient transport retries. |
| `max_delta_rows` | Maximum pending changelog rows extracted in a single pass. |
| `max_chunk_bytes` | Maximum encrypted bytes per outbound sync chunk. |
| `max_pull_chunks` | Maximum inbound chunks requested per pull RPC. |
| `max_push_inflight` | Maximum queue-drain replay batch size for pending offline operations. |

## Offline Behavior

When the hub is unreachable, `claw-sync` does not discard work.

- Failed push attempts are retried with exponential backoff.
- Once retries are exhausted, extracted operations are persisted into the local SQLite offline queue.
- Background draining resumes automatically when transport connectivity returns.
- Checkpoints are stored under `data_dir/checkpoints/` so restart does not duplicate already-sent work.
- `SyncEngine::close()` persists the latest cursor and attempts a final queue drain when connected.

## Audit Log Integrity

The audit database lives at `data_dir/audit.db` in the `sync_audit_log` table. Every row stores the signed event payload hash plus the BLAKE3 hash of the previous row so the log forms an append-only verification chain.

Use the engine API to verify integrity:

```rust
# async fn example(engine: &claw_sync::SyncEngine) -> claw_sync::SyncResult<()> {
let result = engine.verify_audit_log().await?;
assert!(result.ok, "audit chain broken at {:?}: {:?}", result.first_broken_at, result.broken_reason);
# Ok(()) }
```

Tampering with any signed row, previous-entry hash, or signer metadata causes verification to fail with the first broken sequence number.

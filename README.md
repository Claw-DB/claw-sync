# claw-sync

`claw-sync` is the replication and recovery layer for ClawDB. It observes local `claw-core` mutations, converts them into encrypted CRDT deltas, transports them to a sync hub, applies remote deltas back into local state, and preserves resumable checkpoints plus a signed audit chain so synchronization can recover cleanly after transport failure, process restart, or conflict escalation.

This repository contains two Rust crates:

- `claw-sync`: the library and client binary for embedding sync into a local ClawDB application.
- `claw-sync-server`: a standalone PostgreSQL-backed gRPC sync hub.

The workspace does not vendor `claw-core`; it depends on the published crate from crates.io.

## Features

- Encrypted, compressed delta chunks for local-to-hub and hub-to-local replication.
- Hybrid logical clock ordering for deterministic cross-device causality.
- CRDT merge helpers for inserts, updates, deletes, tombstones, and conflict escalation.
- Offline queueing with exponential retry, resumable replay, and persisted checkpoints.
- Device identity, workspace identity, signing keys, transport key exchange, and audit verification.
- gRPC transport generated from `clawsync.v1` protobuf definitions.
- A sync hub binary with durable device registration, push, pull, heartbeat, and reconciliation endpoints.

## Installation

Add the library crate to an application that already uses `claw-core`:

```toml
[dependencies]
claw-sync = "0.1.1"
```

Install the client binary from crates.io:

```sh
cargo install claw-sync
```

The server crate is published separately:

```sh
cargo install claw-sync-server
```

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

At runtime, `SyncEngine` coordinates five main responsibilities:

- Extract unsynced local changelog rows from the `claw-core` database.
- Pack rows into compressed, encrypted `DeltaChunk` payloads.
- Push outbound chunks to the hub and persist failed sends into the offline queue.
- Pull inbound chunks from the hub, verify hashes, decrypt payloads, and apply operations locally.
- Persist checkpoints and audit entries so later sync rounds can resume from a known cursor.

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

For simple binary usage, provide the required environment and run:

```sh
export CLAW_WORKSPACE_ID=00000000-0000-0000-0000-000000000001
export CLAW_HUB_ENDPOINT=http://localhost:50051
export CLAW_DB_PATH=claw_sync.db
cargo run -p claw-sync
```

## SyncConfig Reference

| Field | Environment variable | Default | Purpose |
| --- | --- | --- | --- |
| `workspace_id` | `CLAW_WORKSPACE_ID` | Required | Stable workspace UUID used for audit, checkpoint, and transport scoping. |
| `device_id` | `CLAW_DEVICE_ID` | Random UUID | Stable device UUID used for HLC ordering and registration identity. |
| `hub_endpoint` | `CLAW_HUB_ENDPOINT` | `http://localhost:50051` | Sync hub gRPC endpoint. |
| `data_dir` | `CLAW_DATA_DIR` | `.claw-sync` | Root directory for queue state, checkpoints, audit database, key material, and device identity files. |
| `db_path` | `CLAW_DB_PATH` | `claw_sync.db` | SQLite path for the local `claw-core` database being synchronized. |
| `tls_enabled` | `CLAW_TLS_ENABLED` | `false` | Enables TLS when connecting to the sync hub. |
| `connect_timeout_secs` | `CLAW_CONNECT_TIMEOUT_SECS` | `10` | Connection establishment timeout for the transport client. |
| `request_timeout_secs` | `CLAW_REQUEST_TIMEOUT_SECS` | `30` | Per-request timeout for RPC operations. |
| `sync_interval_secs` | `CLAW_SYNC_INTERVAL_SECS` | `30` | Background sync cadence used by `SyncEngine::start`. |
| `heartbeat_interval_secs` | `CLAW_HEARTBEAT_INTERVAL_SECS` | `15` | Interval for heartbeat-based liveness and pull-required notifications. |
| `max_retries` | `CLAW_MAX_RETRIES` | `5` | Number of transport retry attempts before a push falls back to the offline queue. |
| `retry_base_ms` | `CLAW_RETRY_BASE_MS` | `500` | Base backoff duration for transient transport retries. |
| `max_delta_rows` | `CLAW_MAX_DELTA_ROWS` | `1000` | Maximum pending changelog rows extracted in a single pass. |
| `max_chunk_bytes` | `CLAW_MAX_CHUNK_BYTES` | `65536` | Maximum encrypted bytes per outbound sync chunk. |
| `max_pull_chunks` | `CLAW_MAX_PULL_CHUNKS` | `128` | Maximum inbound chunks requested per pull RPC. |
| `max_push_inflight` | `CLAW_MAX_PUSH_INFLIGHT` | `4` | Maximum queue-drain replay batch size for pending offline operations. |

## Server

`claw-sync-server` runs the sync hub API over TLS and stores hub state in PostgreSQL. It creates the required hub tables at startup.

Required environment:

| Variable | Purpose |
| --- | --- |
| `CLAW_SYNC_HUB_DATABASE_URL` | PostgreSQL connection string for hub metadata and delta storage. |
| `CLAW_SYNC_TLS_CERT` | Path to the PEM certificate served by tonic. |
| `CLAW_SYNC_TLS_KEY` | Path to the PEM private key served by tonic. |

Optional environment:

| Variable | Default | Purpose |
| --- | --- | --- |
| `CLAW_LISTEN_ADDR` | `0.0.0.0:50051` | Socket address for the gRPC server. |
| `CLAW_SYNC_SERVER_KEY_SEED` | `claw-sync-server-default-key-seed` | Seed material used when creating server signing keys. |
| `CLAW_SYNC_SERVER_DATA_DIR` | `.claw-sync-server` | Directory for server identity and key material. |

Run the server locally:

```sh
cargo run -p claw-sync-server
```

## Protocol

The wire protocol is defined in `claw-sync/proto/sync.proto` under the `clawsync.v1` package. The generated API exposes:

- `RegisterDevice` for registering device metadata and public keys.
- `Push` for streaming local delta chunks to the hub.
- `Pull` for streaming remote delta chunks back to a client.
- `Reconcile` for comparing entity hash sets.
- `ResolveConflict` for submitting manually resolved conflict records.
- `Heartbeat` for liveness checks and pull-required notifications.

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

## Offline Behavior

When the hub is unreachable, `claw-sync` does not discard work.

- Failed push attempts are retried with exponential backoff.
- Once retries are exhausted, extracted operations are persisted into the local SQLite offline queue.
- Background draining resumes automatically when transport connectivity returns.
- Checkpoints are stored under `data_dir/checkpoints/` so restart does not duplicate already-sent work.
- `SyncEngine::close()` persists the latest cursor and attempts a final queue drain when connected.

## Runtime Files

The client stores local sync state under `SyncConfig::data_dir`:

| Path | Purpose |
| --- | --- |
| `audit.db` | Signed append-only audit log. |
| `checkpoints/` | Per-workspace and per-device sync cursor state. |
| `queue.db` | Offline queue and replay metadata. |
| `identity/` and key material files | Device identity and cryptographic material managed by the crate. |

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

## Development

Install `protoc` before building because the gRPC bindings are generated during the build.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace --all-features
cargo bench --workspace --no-run
```

The GitHub CI workflow runs the same formatting, lint, test, and benchmark compile checks.

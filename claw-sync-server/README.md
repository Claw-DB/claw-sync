# claw-sync-server

`claw-sync-server` is the stateless gRPC hub for `claw-sync`.

## What it provides

- PostgreSQL-backed device registration and heartbeat tracking.
- Raw encrypted chunk ingestion with BLAKE3 payload-hash verification.
- Pull replay directly from durable hub storage for horizontally scalable hub nodes.
- Optional TLS using `CLAW_SYNC_TLS_CERT` and `CLAW_SYNC_TLS_KEY`.

## Install

```sh
cargo install claw-sync-server --version 0.1.2
```

If the TLS certificate or key environment variable is absent, the server starts without TLS and emits a warning.

See the workspace [README](../README.md) for the full protocol and deployment guidance.
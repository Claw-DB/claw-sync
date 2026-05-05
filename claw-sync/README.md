# claw-sync

`claw-sync` is the client library and binary for encrypted ClawDB replication.

## What it provides

- `SyncEngine` orchestration for push, pull, reconcile, checkpoints, and audit verification.
- Encrypted CRDT delta extraction, packing, transport, and application.
- Offline replay queues with capped exponential retry and durable checkpoints.
- Audit-chain signing with periodic background verification.

## Install

```toml
[dependencies]
claw-sync = "0.1.2"
claw-core = "0.1.2"
```

## Binary

```sh
cargo install claw-sync --version 0.1.2
```

See the workspace [README](../README.md) for protocol, security model, and server setup.
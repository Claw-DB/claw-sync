# Security

## Key Hierarchy

```text
workspace key material
    -> workspace key
        -> per-chunk encryption keys
        -> transport/session derivation inputs
    -> device signing identity (Ed25519)
```

`claw-sync` separates long-lived workspace protection from per-operation material.

- The workspace key anchors encrypted payload handling for a workspace.
- Chunk keys are derived per chunk id so encrypted payloads do not reuse raw symmetric keys directly.
- Device signing keys authenticate registrations, audit rows, and signed transport material.

## Key Rotation Procedure

1. Generate fresh workspace rotation input and derive the replacement workspace key.
2. Persist the rotated key material through the local keystore update path.
3. Emit a `KEY_ROTATED` audit event so the rotation is captured in the signed audit chain.
4. Restart or reconnect active sync clients so new transport sessions derive fresh material.
5. Verify the audit chain after rotation to confirm the event was recorded intact.

## Threat Model

### Compromised device key

If a device signing key is compromised, an attacker may impersonate that device for challenge signing and audit entry creation until the key is rotated and the device is revoked. Integrity review should assume any signatures from that device after compromise may be malicious.

Recommended response:

- Rotate the compromised device key immediately.
- Re-register the device with the sync hub.
- Review `DEVICE_REGISTERED`, `KEY_ROTATED`, `PUSH`, and `PULL` audit rows for anomalous sequences.

### Compromised workspace key

If the workspace key is compromised, encrypted payload confidentiality for that workspace should be treated as lost for any data protected by that key. Audit integrity still depends on Ed25519 signing keys, but payload secrecy does not.

Recommended response:

- Rotate the workspace key.
- Re-establish transport sessions.
- Reconcile devices so all peers converge on material encrypted with the replacement key.
- Re-verify the audit chain after the rotation event is written.

## Audit Log Verification Procedure

1. Open the local engine for the target workspace.
2. Call `SyncEngine::verify_audit_log().await?`.
3. Check `VerifyResult.ok`.
4. If verification fails, inspect `first_broken_at` and `broken_reason`.
5. Treat the log as tampered or corrupted until the affected device and storage path are investigated.

The audit database is stored at `data_dir/audit.db`, and the chain is formed by signed rows in `sync_audit_log` linked through `prev_entry_hash`.

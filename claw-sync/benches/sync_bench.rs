use std::path::Path;

use claw_sync::{
    audit::log::AuditLog,
    crdt::{
        clock::{HlcTimestamp, HybridLogicalClock},
        lww::LwwRegister,
        ops::{CrdtOp, OpKind},
        resolver::ConflictResolver,
    },
    crypto::{
        cipher::{decrypt_chunk, encrypt_chunk},
        keys::KeyStore,
    },
    delta::{
        extractor::{DeltaExtractor, DeltaSet, EntityType},
        hasher::content_hash,
        packer::pack,
        unpacker::unpack,
    },
    queue::offline::{OfflineQueue, QueueStatus, QueuedOp},
    sync::{apply::DeltaApplier, checkpoint::CheckpointStore},
};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    SqlitePool,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use uuid::Uuid;

// Performance targets:
// - Pack 100 ops: < 5ms
// - Encrypt 512KB: < 2ms
// - Queue enqueue 100: < 10ms
// - BLAKE3 1MB: < 1ms
// - HLC 1000 ticks: < 100µs

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BenchDoc {
    title: String,
    body: String,
    count: u32,
}

fn runtime() -> Runtime {
    Runtime::new().expect("runtime should initialise")
}

fn timestamp(logical_ms: u64, counter: u32, node_id: Uuid) -> HlcTimestamp {
    HlcTimestamp {
        logical_ms,
        counter,
        node_id,
    }
}

fn delta_set_with_ops(op_count: usize) -> DeltaSet {
    let device_id = Uuid::from_bytes([1; 16]);
    DeltaSet {
        entity_type: EntityType::MemoryRecords,
        ops: (0..op_count)
            .map(|index| CrdtOp::Insert {
                entity_id: format!("memory-{index}"),
                payload: json!({
                    "title": format!("title-{index}"),
                    "body": "x".repeat(64),
                    "count": index,
                }),
                hlc: timestamp(index as u64 + 1, 0, device_id),
                device_id,
            })
            .collect(),
        sequences: (0..op_count as i64).collect(),
        device_id,
    }
}

fn queued_ops(count: usize) -> Vec<QueuedOp> {
    let workspace_id = Uuid::from_bytes([3; 16]);
    let device_id = Uuid::from_bytes([4; 16]);
    (0..count)
        .map(|index| QueuedOp {
            id: format!("queue-{index}"),
            workspace_id,
            entity_type: EntityType::MemoryRecords,
            operation: OpKind::Insert,
            entity_id: format!("entity-{index}"),
            payload: json!({ "title": format!("entity-{index}") }),
            device_id,
            hlc: timestamp(index as u64 + 1, 0, device_id).to_string(),
            enqueued_at: chrono::Utc::now(),
            attempt_count: 0,
            status: QueueStatus::Pending,
        })
        .collect()
}

async fn open_pool(db_path: &Path) -> SqlitePool {
    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true);
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("sqlite pool should connect")
}

async fn init_schema(pool: &SqlitePool) {
    for statement in [
        "CREATE TABLE IF NOT EXISTS memory_records (id TEXT PRIMARY KEY, title TEXT, body TEXT, deleted_at TEXT)",
        "CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, label TEXT, deleted_at TEXT)",
        "CREATE TABLE IF NOT EXISTS tool_outputs (id TEXT PRIMARY KEY, payload TEXT, deleted_at TEXT)",
    ] {
        sqlx::query(statement)
            .execute(pool)
            .await
            .expect("schema statement should succeed");
    }
}

async fn make_applier_fixture() -> (DeltaApplier, DeltaSet, TempDir) {
    let temp_dir = TempDir::new().expect("temp dir should exist");
    let pool = open_pool(&temp_dir.path().join("core.db")).await;
    init_schema(&pool).await;
    let key_store = std::sync::Arc::new(
        KeyStore::load_or_create(temp_dir.path(), b"bench-applier").expect("keystore should load"),
    );
    let audit = std::sync::Arc::new(
        AuditLog::new(temp_dir.path(), key_store)
            .await
            .expect("audit log should initialise"),
    );
    let checkpoint = std::sync::Arc::new(
        CheckpointStore::new(temp_dir.path()).expect("checkpoint store should initialise"),
    );
    let applier = DeltaApplier::new(
        pool,
        std::sync::Arc::new(ConflictResolver::default()),
        audit,
        checkpoint,
        Uuid::from_bytes([1; 16]),
        Uuid::from_bytes([2; 16]),
    );
    (applier, delta_set_with_ops(100), temp_dir)
}

async fn make_extractor_fixture() -> (DeltaExtractor, TempDir) {
    let temp_dir = TempDir::new().expect("temp dir should exist");
    let pool = open_pool(&temp_dir.path().join("core.db")).await;
    init_schema(&pool).await;
    let extractor = DeltaExtractor::new(pool.clone(), Uuid::from_bytes([7; 16]));
    extractor
        .install_changelog_trigger()
        .await
        .expect("triggers should install");
    for index in 0..1_000 {
        sqlx::query("INSERT INTO memory_records (id, title, body) VALUES (?, ?, ?)")
            .bind(format!("m-{index}"))
            .bind(format!("title-{index}"))
            .bind("body")
            .execute(&pool)
            .await
            .expect("seed insert should succeed");
    }
    (extractor, temp_dir)
}

async fn make_queue_fixture() -> (OfflineQueue, Vec<QueuedOp>, TempDir) {
    let temp_dir = TempDir::new().expect("temp dir should exist");
    let queue = OfflineQueue::new(temp_dir.path())
        .await
        .expect("offline queue should initialise");
    (queue, queued_ops(100), temp_dir)
}

fn bench_pack_delta_100_ops(c: &mut Criterion) {
    let workspace_key =
        KeyStore::load_or_create(Path::new("/tmp/claw-sync-bench-pack"), b"bench-pack")
            .expect("keystore should load")
            .workspace_key()
            .clone();
    let delta = delta_set_with_ops(100);
    c.bench_function("bench_pack_delta_100_ops", |b| {
        b.iter(|| pack(&delta, &workspace_key, 64 * 1024).expect("pack should succeed"))
    });
}

fn bench_unpack_delta_100_ops(c: &mut Criterion) {
    let workspace_key =
        KeyStore::load_or_create(Path::new("/tmp/claw-sync-bench-unpack"), b"bench-unpack")
            .expect("keystore should load")
            .workspace_key()
            .clone();
    let delta = delta_set_with_ops(100);
    let payload = pack(&delta, &workspace_key, 64 * 1024).expect("pack should succeed");
    c.bench_function("bench_unpack_delta_100_ops", |b| {
        b.iter(|| unpack(&payload, &workspace_key).expect("unpack should succeed"))
    });
}

fn bench_content_hash_1kb(c: &mut Criterion) {
    let payload = vec![0x55_u8; 1_024];
    c.bench_function("bench_content_hash_1kb", |b| {
        b.iter(|| content_hash(&payload))
    });
}

fn bench_content_hash_1mb(c: &mut Criterion) {
    let payload = vec![0x55_u8; 1_024 * 1_024];
    c.bench_function("bench_content_hash_1mb", |b| {
        b.iter(|| content_hash(&payload))
    });
}

fn bench_encrypt_chunk_512kb(c: &mut Criterion) {
    let workspace_key =
        KeyStore::load_or_create(Path::new("/tmp/claw-sync-bench-encrypt"), b"bench-encrypt")
            .expect("keystore should load")
            .workspace_key()
            .clone();
    let payload = vec![0xAB_u8; 512 * 1_024];
    c.bench_function("bench_encrypt_chunk_512kb", |b| {
        b.iter(|| {
            encrypt_chunk(&payload, &workspace_key, "bench-chunk").expect("encrypt should succeed")
        })
    });
}

fn bench_decrypt_chunk_512kb(c: &mut Criterion) {
    let workspace_key =
        KeyStore::load_or_create(Path::new("/tmp/claw-sync-bench-decrypt"), b"bench-decrypt")
            .expect("keystore should load")
            .workspace_key()
            .clone();
    let payload = vec![0xAB_u8; 512 * 1_024];
    let blob =
        encrypt_chunk(&payload, &workspace_key, "bench-chunk").expect("encrypt should succeed");
    c.bench_function("bench_decrypt_chunk_512kb", |b| {
        b.iter(|| {
            decrypt_chunk(&blob, &workspace_key, "bench-chunk").expect("decrypt should succeed")
        })
    });
}

fn bench_queue_enqueue_100(c: &mut Criterion) {
    let rt = runtime();
    c.bench_function("bench_queue_enqueue_100", |b| {
        b.iter_batched(
            || rt.block_on(make_queue_fixture()),
            |(queue, ops, _temp_dir)| {
                rt.block_on(async move {
                    for op in &ops {
                        queue.enqueue(op).await.expect("enqueue should succeed");
                    }
                })
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_queue_dequeue_100(c: &mut Criterion) {
    let rt = runtime();
    c.bench_function("bench_queue_dequeue_100", |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let (queue, ops, temp_dir) = make_queue_fixture().await;
                    for op in &ops {
                        queue.enqueue(op).await.expect("enqueue should succeed");
                    }
                    (queue, temp_dir)
                })
            },
            |(queue, _temp_dir)| {
                rt.block_on(async move {
                    queue
                        .dequeue_batch(100)
                        .await
                        .expect("dequeue should succeed")
                })
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_hlc_tick_1000(c: &mut Criterion) {
    c.bench_function("bench_hlc_tick_1000", |b| {
        b.iter(|| {
            let mut clock = HybridLogicalClock::with_node_id(Uuid::from_bytes([6; 16]));
            for _ in 0..1_000 {
                let _ = clock.tick();
            }
        })
    });
}

fn bench_lww_merge_1000(c: &mut Criterion) {
    c.bench_function("bench_lww_merge_1000", |b| {
        b.iter(|| {
            let mut register = LwwRegister::new(
                BenchDoc {
                    title: "left".into(),
                    body: "body".into(),
                    count: 0,
                },
                timestamp(1, 0, Uuid::from_bytes([7; 16])),
                Uuid::from_bytes([7; 16]),
            );
            for index in 0..1_000 {
                register.merge(LwwRegister::new(
                    BenchDoc {
                        title: format!("doc-{index}"),
                        body: "body".into(),
                        count: index,
                    },
                    timestamp(index as u64 + 2, 0, Uuid::from_bytes([8; 16])),
                    Uuid::from_bytes([8; 16]),
                ));
            }
        })
    });
}

fn bench_extractor_1000_rows(c: &mut Criterion) {
    let rt = runtime();
    c.bench_function("bench_extractor_1000_rows", |b| {
        b.iter_batched(
            || rt.block_on(make_extractor_fixture()),
            |(extractor, _temp_dir)| {
                rt.block_on(async move {
                    extractor
                        .extract_pending(&[EntityType::MemoryRecords])
                        .await
                        .expect("extract should succeed")
                })
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_applier_100_inserts(c: &mut Criterion) {
    let rt = runtime();
    c.bench_function("bench_applier_100_inserts", |b| {
        b.iter_batched(
            || rt.block_on(make_applier_fixture()),
            |(applier, delta, _temp_dir)| {
                rt.block_on(async move {
                    applier
                        .apply_delta_set(delta)
                        .await
                        .expect("apply should succeed")
                })
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    sync_benches,
    bench_pack_delta_100_ops,
    bench_unpack_delta_100_ops,
    bench_content_hash_1kb,
    bench_content_hash_1mb,
    bench_encrypt_chunk_512kb,
    bench_decrypt_chunk_512kb,
    bench_queue_enqueue_100,
    bench_queue_dequeue_100,
    bench_hlc_tick_1000,
    bench_lww_merge_1000,
    bench_extractor_1000_rows,
    bench_applier_100_inserts,
);
criterion_main!(sync_benches);

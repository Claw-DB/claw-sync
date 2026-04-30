use std::{
    net::SocketAddr,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use claw_sync::{
    config::SyncConfig,
    crdt::{
        clock::HlcTimestamp,
        ops::{CrdtOp, FieldPatch},
    },
    crypto::keys::KeyStore,
    delta::{
        extractor::{DeltaSet, EntityType},
        packer::{pack, payload_to_proto_chunks},
    },
    engine::{SyncEngine, SyncEvent},
    proto::clawsync::v1::{
        sync_service_server::{SyncService, SyncServiceServer},
        DeltaChunk, HeartbeatRequest, HeartbeatResponse, PullRequest, PullResponse, PushRequest,
        PushResponse, ReconcileRequest, ReconcileResponse, RegisterDeviceRequest,
        RegisterDeviceResponse, ResolveConflictRequest, ResolveConflictResponse, SyncCursor,
    },
};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    SqlitePool,
};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Code, Request, Response, Status, Streaming};
use uuid::Uuid;

#[derive(Debug, Clone)]
enum PushBehavior {
    AlwaysOk,
    FailTimes { remaining: u32, code: Code },
    AlwaysFail { code: Code },
}

impl Default for PushBehavior {
    fn default() -> Self {
        Self::AlwaysOk
    }
}

impl PushBehavior {
    fn on_call(&mut self) -> Result<(), Status> {
        match self {
            Self::AlwaysOk => Ok(()),
            Self::FailTimes { remaining, code } => {
                if *remaining > 0 {
                    *remaining -= 1;
                    Err(Status::new(*code, "mock push failure"))
                } else {
                    Ok(())
                }
            }
            Self::AlwaysFail { code } => Err(Status::new(*code, "mock push failure")),
        }
    }
}

#[derive(Default)]
struct MockState {
    register_calls: AtomicU32,
    push_calls: AtomicU32,
    pull_calls: AtomicU32,
    reconcile_calls: AtomicU32,
    heartbeat_calls: AtomicU32,
    received_push_requests: Mutex<Vec<PushRequest>>,
    received_push_chunks: Mutex<Vec<DeltaChunk>>,
    push_behavior: Mutex<PushBehavior>,
    pull_responses: Mutex<Vec<PullResponse>>,
    reconcile_response: Mutex<ReconcileResponse>,
    heartbeat_sync_required: AtomicBool,
}

#[derive(Clone, Default)]
struct MockSyncService {
    state: Arc<MockState>,
}

#[tonic::async_trait]
impl SyncService for MockSyncService {
    async fn register_device(
        &self,
        _request: Request<RegisterDeviceRequest>,
    ) -> Result<Response<RegisterDeviceResponse>, Status> {
        self.state.register_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(RegisterDeviceResponse {
            accepted: true,
            assigned_device_id: Uuid::new_v4().to_string(),
            server_public_key: vec![],
        }))
    }

    type PushStream = ReceiverStream<Result<PushResponse, Status>>;

    async fn push(
        &self,
        request: Request<Streaming<PushRequest>>,
    ) -> Result<Response<Self::PushStream>, Status> {
        self.state.push_calls.fetch_add(1, Ordering::SeqCst);
        self.state
            .push_behavior
            .lock()
            .expect("push behavior lock should succeed")
            .on_call()?;

        let mut stream = request.into_inner();
        let mut requests = Vec::new();
        let mut chunks = Vec::new();
        while let Some(push_request) = stream.message().await? {
            chunks.extend(push_request.chunks.clone());
            requests.push(push_request);
        }
        self.state
            .received_push_chunks
            .lock()
            .expect("push chunks lock should succeed")
            .extend(chunks);
        self.state
            .received_push_requests
            .lock()
            .expect("push requests lock should succeed")
            .extend(requests.clone());

        let server_cursor = requests.last().and_then(|request| request.cursor.clone());
        let (tx, rx) = mpsc::channel(1);
        let _ = tx
            .send(Ok(PushResponse {
                accepted: true,
                server_cursor,
                rejected_chunk_ids: vec![],
                reason: String::new(),
            }))
            .await;
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type PullStream = ReceiverStream<Result<PullResponse, Status>>;

    async fn pull(
        &self,
        _request: Request<PullRequest>,
    ) -> Result<Response<Self::PullStream>, Status> {
        self.state.pull_calls.fetch_add(1, Ordering::SeqCst);
        let responses = self
            .state
            .pull_responses
            .lock()
            .expect("pull responses lock should succeed")
            .clone();
        let (tx, rx) = mpsc::channel(responses.len().max(1));
        tokio::spawn(async move {
            for response in responses {
                let _ = tx.send(Ok(response)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn reconcile(
        &self,
        _request: Request<ReconcileRequest>,
    ) -> Result<Response<ReconcileResponse>, Status> {
        self.state.reconcile_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(
            self.state
                .reconcile_response
                .lock()
                .expect("reconcile response lock should succeed")
                .clone(),
        ))
    }

    async fn resolve_conflict(
        &self,
        _request: Request<ResolveConflictRequest>,
    ) -> Result<Response<ResolveConflictResponse>, Status> {
        Ok(Response::new(ResolveConflictResponse { ok: true }))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        self.state.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(HeartbeatResponse {
            server_time: chrono::Utc::now().timestamp_millis(),
            sync_required: self.state.heartbeat_sync_required.load(Ordering::SeqCst),
        }))
    }
}

struct MockSyncServer {
    state: Arc<MockState>,
    address: SocketAddr,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl MockSyncServer {
    async fn start() -> Self {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have a local addr");
        drop(listener);

        let state = Arc::new(MockState {
            reconcile_response: Mutex::new(ReconcileResponse {
                missing_on_client: vec![],
                missing_on_server: vec![],
                conflicted: vec![],
            }),
            ..MockState::default()
        });
        let shutdown = CancellationToken::new();
        let shutdown_signal = shutdown.clone();
        let service = MockSyncService {
            state: state.clone(),
        };
        let task = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(SyncServiceServer::new(service))
                .serve_with_shutdown(address, async move {
                    shutdown_signal.cancelled().await;
                })
                .await;
        });

        wait_for_server(address).await;

        Self {
            state,
            address,
            shutdown,
            task,
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    fn set_push_behavior(&self, behavior: PushBehavior) {
        *self
            .state
            .push_behavior
            .lock()
            .expect("push behavior lock should succeed") = behavior;
    }

    fn set_pull_responses(&self, responses: Vec<PullResponse>) {
        *self
            .state
            .pull_responses
            .lock()
            .expect("pull responses lock should succeed") = responses;
    }

    fn set_reconcile_response(&self, response: ReconcileResponse) {
        *self
            .state
            .reconcile_response
            .lock()
            .expect("reconcile response lock should succeed") = response;
    }

    fn register_calls(&self) -> u32 {
        self.state.register_calls.load(Ordering::SeqCst)
    }

    fn push_calls(&self) -> u32 {
        self.state.push_calls.load(Ordering::SeqCst)
    }

    fn received_push_chunk_count(&self) -> usize {
        self.state
            .received_push_chunks
            .lock()
            .expect("push chunks lock should succeed")
            .len()
    }
}

impl Drop for MockSyncServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

struct TestHarness {
    temp_dir: TempDir,
    server: MockSyncServer,
    config: SyncConfig,
    pool: SqlitePool,
}

impl TestHarness {
    async fn new() -> Self {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let server = MockSyncServer::start().await;
        let config = test_config(temp_dir.path(), server.endpoint());
        let pool = open_pool(&config.db_path).await;
        init_core_schema(&pool).await;

        Self {
            temp_dir,
            server,
            config,
            pool,
        }
    }

    async fn engine(&self) -> SyncEngine {
        SyncEngine::new(self.config.clone(), self.pool.clone())
            .await
            .expect("engine should initialise")
    }

    fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    fn server_workspace_key(&self) -> claw_sync::crypto::keys::WorkspaceKey {
        let key_store = KeyStore::load_or_create(
            &self.temp_dir.path().join("server-keys"),
            self.config.workspace_id.as_bytes(),
        )
        .expect("server keystore should load");
        key_store.workspace_key().clone()
    }
}

#[tokio::test]
async fn engine_new_and_close() {
    let harness = TestHarness::new().await;
    let engine = harness.engine().await;
    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn device_registration() {
    let harness = TestHarness::new().await;
    let engine = harness.engine().await;

    assert_eq!(harness.server.register_calls(), 1);
    assert!(harness.data_dir().join("device_identity.json").exists());
    assert!(harness.data_dir().join("device_registration.json").exists());

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn push_pending_empty() {
    let harness = TestHarness::new().await;
    let engine = harness.engine().await;

    let stats = engine.push_now().await.expect("push should succeed");
    assert_eq!(stats.deltas_sent, 0);

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn push_single_insert() {
    let harness = TestHarness::new().await;
    let engine = harness.engine().await;
    insert_memory_record(&harness.pool, "m1", "title", "body").await;

    let stats = engine.push_now().await.expect("push should succeed");

    assert_eq!(stats.deltas_sent, 1);
    assert_eq!(harness.server.received_push_chunk_count(), 1);
    let synced: i64 =
        sqlx::query_scalar("SELECT synced FROM sync_changelog WHERE entity_id = 'm1' LIMIT 1")
            .fetch_one(&harness.pool)
            .await
            .expect("changelog row should exist");
    assert_eq!(synced, 1);

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn push_retries_on_transport_error() {
    let harness = TestHarness::new().await;
    harness.server.set_push_behavior(PushBehavior::FailTimes {
        remaining: 2,
        code: Code::Unavailable,
    });
    let engine = harness.engine().await;
    insert_memory_record(&harness.pool, "m1", "title", "body").await;

    let stats = engine
        .push_now()
        .await
        .expect("push should succeed after retries");

    assert_eq!(stats.deltas_sent, 1);
    assert_eq!(stats.retries, 2);
    assert_eq!(harness.server.push_calls(), 3);

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn push_enqueues_on_persistent_error() {
    let harness = TestHarness::new().await;
    harness.server.set_push_behavior(PushBehavior::AlwaysFail {
        code: Code::Unavailable,
    });
    let engine = harness.engine().await;
    insert_memory_record(&harness.pool, "m1", "title", "body").await;

    let error = engine.push_now().await.expect_err("push should fail");
    assert!(matches!(error, claw_sync::SyncError::Transport(_)));
    assert!(
        engine
            .queue
            .pending_count()
            .await
            .expect("queue count should succeed")
            >= 1
    );

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn pull_applies_remote_insert() {
    let harness = TestHarness::new().await;
    let engine = harness.engine().await;
    harness
        .server
        .set_pull_responses(vec![pull_response_from_delta(
            &harness.config,
            &harness.server_workspace_key(),
            &remote_insert_delta("m1", "remote", "body", 10, remote_device()),
        )]);

    let stats = engine.pull_now().await.expect("pull should succeed");

    assert_eq!(stats.ops_applied, 1);
    assert_eq!(
        memory_body(&harness.pool, "m1").await.as_deref(),
        Some("body")
    );

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn pull_lww_remote_wins() {
    let harness = TestHarness::new().await;
    insert_memory_record(&harness.pool, "m1", "title", "base").await;
    let engine = harness.engine().await;
    engine
        .sync_now()
        .await
        .expect("baseline sync should succeed");
    update_memory_body(&harness.pool, "m1", "local").await;
    let local_hlc = HlcTimestamp {
        logical_ms: 10,
        counter: 0,
        node_id: harness.config.device_id,
    };
    sqlx::query("UPDATE sync_changelog SET hlc = ? WHERE entity_id = 'm1' AND synced = 0")
        .bind(local_hlc.to_string())
        .execute(&harness.pool)
        .await
        .expect("should be able to lower the local hlc");
    harness
        .server
        .set_pull_responses(vec![pull_response_from_delta(
            &harness.config,
            &harness.server_workspace_key(),
            &remote_update_delta("m1", "body", "remote", 20, remote_device()),
        )]);

    engine.pull_now().await.expect("pull should succeed");
    assert_eq!(
        memory_body(&harness.pool, "m1").await.as_deref(),
        Some("remote")
    );

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn pull_lww_local_wins() {
    let harness = TestHarness::new().await;
    insert_memory_record(&harness.pool, "m1", "title", "base").await;
    let engine = harness.engine().await;
    engine
        .sync_now()
        .await
        .expect("baseline sync should succeed");
    update_memory_body(&harness.pool, "m1", "local").await;
    harness
        .server
        .set_pull_responses(vec![pull_response_from_delta(
            &harness.config,
            &harness.server_workspace_key(),
            &remote_update_delta("m1", "body", "remote", 1, remote_device()),
        )]);

    engine.pull_now().await.expect("pull should succeed");
    assert_eq!(
        memory_body(&harness.pool, "m1").await.as_deref(),
        Some("local")
    );

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn crdt_conflict_escalation() {
    let harness = TestHarness::new().await;
    insert_memory_record(&harness.pool, "m1", "title", "base").await;
    let engine = harness.engine().await;
    engine
        .sync_now()
        .await
        .expect("baseline sync should succeed");
    update_memory_body(&harness.pool, "m1", "local").await;

    let identical_hlc = HlcTimestamp {
        logical_ms: 50,
        counter: 0,
        node_id: remote_device(),
    };
    sqlx::query("UPDATE sync_changelog SET hlc = ? WHERE entity_id = 'm1' AND synced = 0")
        .bind(identical_hlc.to_string())
        .execute(&harness.pool)
        .await
        .expect("should be able to align local hlc");

    harness
        .server
        .set_pull_responses(vec![pull_response_from_delta(
            &harness.config,
            &harness.server_workspace_key(),
            &DeltaSet {
                entity_type: EntityType::MemoryRecords,
                ops: vec![CrdtOp::Update {
                    entity_id: "m1".into(),
                    field_patches: vec![FieldPatch {
                        field: "body".into(),
                        old_value: serde_json::json!("base"),
                        new_value: serde_json::json!("remote"),
                    }],
                    hlc: identical_hlc,
                    device_id: remote_device(),
                }],
                sequences: vec![],
                device_id: remote_device(),
            },
        )]);
    let mut events = engine.subscribe();

    let error = engine
        .pull_now()
        .await
        .expect_err("pull should escalate conflict");
    assert!(matches!(
        error,
        claw_sync::SyncError::ConflictEscalation { .. }
    ));

    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("conflict event should arrive")
        .expect("conflict event should be readable");
    match event {
        SyncEvent::ConflictDetected(record) => {
            assert_eq!(record.entity_id, "m1");
            assert_eq!(record.entity_type, "memory_records");
        }
        other => panic!("unexpected event: {other:?}"),
    }

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn offline_queue_drain_on_reconnect() {
    let harness = TestHarness::new().await;
    harness.server.set_push_behavior(PushBehavior::AlwaysFail {
        code: Code::Unavailable,
    });
    let engine = harness.engine().await;

    for index in 0..5 {
        insert_memory_record(
            &harness.pool,
            &format!("m{index}"),
            &format!("title-{index}"),
            &format!("body-{index}"),
        )
        .await;
    }

    let _ = engine
        .push_now()
        .await
        .expect_err("initial push should fail");
    assert_eq!(
        engine
            .queue
            .pending_count()
            .await
            .expect("queue count should succeed"),
        5
    );

    let shutdown = CancellationToken::new();
    engine
        .start(shutdown.clone())
        .await
        .expect("start should succeed");
    harness.server.set_push_behavior(PushBehavior::AlwaysOk);

    wait_for_condition(Duration::from_secs(4), || async {
        engine
            .queue
            .pending_count()
            .await
            .expect("queue count should succeed")
            == 0
    })
    .await;

    assert!(harness.server.received_push_chunk_count() >= 5);

    shutdown.cancel();
    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn checkpoint_resume() {
    let harness = TestHarness::new().await;
    let engine = harness.engine().await;
    insert_memory_record(&harness.pool, "m1", "title", "body").await;

    let first = engine.push_now().await.expect("first push should succeed");
    assert_eq!(first.deltas_sent, 1);
    assert_eq!(harness.server.received_push_chunk_count(), 1);

    engine.close().await.expect("close should succeed");

    let reopened = harness.engine().await;
    let second = reopened
        .push_now()
        .await
        .expect("second push should succeed");
    assert_eq!(second.deltas_sent, 0);
    assert_eq!(harness.server.received_push_chunk_count(), 1);

    reopened.close().await.expect("close should succeed");
}

#[tokio::test]
async fn audit_log_chain_integrity() {
    let harness = TestHarness::new().await;
    let engine = harness.engine().await;

    for _ in 0..10 {
        engine.push_now().await.expect("push should succeed");
    }

    let result = engine
        .verify_audit_log()
        .await
        .expect("audit verification should succeed");
    assert!(result.ok);
    assert!(result.checked >= 10);

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn audit_log_tamper_detected() {
    let harness = TestHarness::new().await;
    let engine = harness.engine().await;
    engine.push_now().await.expect("push should succeed");

    let latest_sequence: i64 = sqlx::query_scalar("SELECT MAX(sequence) FROM sync_audit_log")
        .fetch_one(&engine.audit.pool)
        .await
        .expect("audit sequence should query");
    sqlx::query("UPDATE sync_audit_log SET payload_hash = 'tampered' WHERE sequence = ?")
        .bind(latest_sequence)
        .execute(&engine.audit.pool)
        .await
        .expect("tamper update should succeed");

    let result = engine
        .verify_audit_log()
        .await
        .expect("audit verification should complete");
    assert!(!result.ok);
    assert_eq!(result.first_broken_at, Some(latest_sequence as u64));

    engine.close().await.expect("close should succeed");
}

#[tokio::test]
async fn reconcile_detects_missing() {
    let harness = TestHarness::new().await;
    for index in 0..10 {
        insert_memory_record(
            &harness.pool,
            &format!("m{index}"),
            &format!("title-{index}"),
            &format!("body-{index}"),
        )
        .await;
    }
    let engine = harness.engine().await;

    let remote_delta = DeltaSet {
        entity_type: EntityType::MemoryRecords,
        ops: vec![
            remote_insert_op("remote-10", "title-10", "body-10", 100),
            remote_insert_op("remote-11", "title-11", "body-11", 101),
        ],
        sequences: vec![],
        device_id: remote_device(),
    };
    harness.server.set_reconcile_response(ReconcileResponse {
        missing_on_client: vec!["remote-10".into(), "remote-11".into()],
        missing_on_server: vec![],
        conflicted: vec![],
    });
    harness
        .server
        .set_pull_responses(vec![pull_response_from_delta(
            &harness.config,
            &harness.server_workspace_key(),
            &remote_delta,
        )]);

    let stats = engine.reconcile().await.expect("reconcile should succeed");
    assert_eq!(stats.pulled, 2);

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memory_records")
        .fetch_one(&harness.pool)
        .await
        .expect("row count should query");
    assert_eq!(count, 12);

    engine.close().await.expect("close should succeed");
}

fn test_config(base_dir: &Path, endpoint: String) -> SyncConfig {
    SyncConfig {
        workspace_id: Uuid::from_bytes([1; 16]),
        device_id: Uuid::from_bytes([2; 16]),
        hub_endpoint: endpoint,
        data_dir: base_dir.join("data"),
        db_path: base_dir.join("core.db"),
        tls_enabled: false,
        connect_timeout_secs: 5,
        request_timeout_secs: 5,
        sync_interval_secs: 1,
        heartbeat_interval_secs: 1,
        max_retries: 3,
        retry_base_ms: 5,
        max_delta_rows: 1_000,
        max_chunk_bytes: 64 * 1024,
        max_pull_chunks: 128,
        max_push_inflight: 8,
    }
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
        .expect("pool should connect")
}

async fn init_core_schema(pool: &SqlitePool) {
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

async fn insert_memory_record(pool: &SqlitePool, id: &str, title: &str, body: &str) {
    sqlx::query("INSERT INTO memory_records (id, title, body) VALUES (?, ?, ?)")
        .bind(id)
        .bind(title)
        .bind(body)
        .execute(pool)
        .await
        .expect("insert should succeed");
}

async fn update_memory_body(pool: &SqlitePool, id: &str, body: &str) {
    sqlx::query("UPDATE memory_records SET body = ? WHERE id = ?")
        .bind(body)
        .bind(id)
        .execute(pool)
        .await
        .expect("update should succeed");
}

async fn memory_body(pool: &SqlitePool, id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT body FROM memory_records WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("body query should succeed")
}

fn remote_device() -> Uuid {
    Uuid::from_bytes([9; 16])
}

fn remote_insert_op(entity_id: &str, title: &str, body: &str, logical_ms: u64) -> CrdtOp {
    CrdtOp::Insert {
        entity_id: entity_id.into(),
        payload: serde_json::json!({ "title": title, "body": body }),
        hlc: HlcTimestamp {
            logical_ms,
            counter: 0,
            node_id: remote_device(),
        },
        device_id: remote_device(),
    }
}

fn remote_insert_delta(
    entity_id: &str,
    title: &str,
    body: &str,
    logical_ms: u64,
    device_id: Uuid,
) -> DeltaSet {
    DeltaSet {
        entity_type: EntityType::MemoryRecords,
        ops: vec![CrdtOp::Insert {
            entity_id: entity_id.into(),
            payload: serde_json::json!({ "title": title, "body": body }),
            hlc: HlcTimestamp {
                logical_ms,
                counter: 0,
                node_id: device_id,
            },
            device_id,
        }],
        sequences: vec![],
        device_id,
    }
}

fn remote_update_delta(
    entity_id: &str,
    field: &str,
    value: &str,
    logical_ms: u64,
    device_id: Uuid,
) -> DeltaSet {
    DeltaSet {
        entity_type: EntityType::MemoryRecords,
        ops: vec![CrdtOp::Update {
            entity_id: entity_id.into(),
            field_patches: vec![FieldPatch {
                field: field.into(),
                old_value: serde_json::Value::Null,
                new_value: serde_json::json!(value),
            }],
            hlc: HlcTimestamp {
                logical_ms,
                counter: 0,
                node_id: device_id,
            },
            device_id,
        }],
        sequences: vec![],
        device_id,
    }
}

fn pull_response_from_delta(
    config: &SyncConfig,
    workspace_key: &claw_sync::crypto::keys::WorkspaceKey,
    delta: &DeltaSet,
) -> PullResponse {
    let payload =
        pack(delta, workspace_key, config.max_chunk_bytes).expect("delta packing should succeed");
    let hlc_timestamp = delta
        .ops
        .last()
        .map(|op| op.hlc().to_string())
        .unwrap_or_default();

    PullResponse {
        chunks: payload_to_proto_chunks(&payload),
        server_cursor: Some(SyncCursor {
            workspace_id: config.workspace_id.to_string(),
            device_id: delta.device_id.to_string(),
            lamport_clock: delta.ops.len() as i64,
            hlc_timestamp,
            state_hash: vec![],
        }),
        has_more: false,
    }
}

async fn wait_for_server(address: SocketAddr) {
    wait_for_condition(Duration::from_secs(2), || async move {
        tokio::net::TcpStream::connect(address).await.is_ok()
    })
    .await;
}

async fn wait_for_condition<F, Fut>(timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("condition was not met before timeout");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

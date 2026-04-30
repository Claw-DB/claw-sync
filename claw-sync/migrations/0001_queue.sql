CREATE TABLE IF NOT EXISTS sync_queue (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    entity_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    device_id TEXT NOT NULL,
    hlc TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'in_flight', 'failed', 'done')),
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_queue_pending
    ON sync_queue (status, enqueued_at);

CREATE INDEX IF NOT EXISTS idx_sync_queue_workspace
    ON sync_queue (workspace_id, status);

CREATE INDEX IF NOT EXISTS idx_sync_queue_last_attempt
    ON sync_queue (status, last_attempt_at);CREATE TABLE IF NOT EXISTS sync_queue (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    entity_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    device_id TEXT NOT NULL,
    hlc TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'in_flight', 'failed', 'done')),
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_queue_pending
    ON sync_queue (status, enqueued_at);

CREATE INDEX IF NOT EXISTS idx_sync_queue_workspace_status
    ON sync_queue (workspace_id, status);

CREATE INDEX IF NOT EXISTS idx_sync_queue_last_attempt
    ON sync_queue (status, last_attempt_at);
CREATE TABLE IF NOT EXISTS offline_queue (
    id INTEGER PRIMARY KEY,
    chunk_bytes BLOB NOT NULL,
    enqueued_at INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt INTEGER,
    status TEXT NOT NULL CHECK (status IN ('pending', 'failed')) DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS idx_offline_queue_pending
    ON offline_queue (status, enqueued_at);

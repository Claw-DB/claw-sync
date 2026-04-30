CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_queue_dedupe
    ON sync_queue (workspace_id, entity_type, operation, entity_id, hlc, device_id);
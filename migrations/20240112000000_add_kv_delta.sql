-- KV delta table: captures KV mutations from the current execution only.
-- Values are merged into kv_store on execution completion (Completed, CAN, Failed).
-- Snapshot seeding reads from kv_store only (prior-execution state).
-- Client reads merge kv_store + kv_delta for a live view.
-- NULL value = tombstone (key was explicitly cleared in current execution).

CREATE TABLE IF NOT EXISTS kv_delta (
    instance_id TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       TEXT,
    last_updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (instance_id, key)
);

-- Drop execution_id from kv_store (never read, only written).
-- SQLite requires table recreation for older versions.
CREATE TABLE IF NOT EXISTS kv_store_new (
    instance_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    last_updated_at_ms INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (instance_id, key)
);

INSERT OR IGNORE INTO kv_store_new (instance_id, key, value, last_updated_at_ms)
    SELECT instance_id, key, value, last_updated_at_ms FROM kv_store;

DROP TABLE IF EXISTS kv_store;
ALTER TABLE kv_store_new RENAME TO kv_store;

DROP INDEX IF EXISTS idx_kv_store_execution;

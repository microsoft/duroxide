-- KV store: instance-scoped key-value pairs, materialized from history events.
-- Primary key is (instance_id, key) — only the latest value per key is stored.
-- execution_id tracks which execution last wrote this key (for pruning).

CREATE TABLE IF NOT EXISTS kv_store (
    instance_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    PRIMARY KEY (instance_id, key)
);

-- For pruning: find keys last-written by a specific execution
CREATE INDEX IF NOT EXISTS idx_kv_store_execution ON kv_store(instance_id, execution_id);

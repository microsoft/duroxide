-- Add last_updated_at_ms timestamp to kv_store entries.
-- Set by the runtime at set_kv_value() time, persisted by the provider.
-- Pre-existing rows default to 0 (unknown update time until rewritten).

ALTER TABLE kv_store ADD COLUMN last_updated_at_ms INTEGER NOT NULL DEFAULT 0;

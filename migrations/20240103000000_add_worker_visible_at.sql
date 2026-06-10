-- Add visible_at column to worker_queue for proper visibility control
-- This mirrors the orchestrator_queue pattern where visibility and locking are separate concerns

-- Add visible_at column (default to 0 which is always visible)
-- Using INTEGER for milliseconds since epoch, matching how the code stores timestamps
ALTER TABLE worker_queue ADD COLUMN visible_at INTEGER NOT NULL DEFAULT 0;

-- Note: Existing rows will have visible_at = 0 (always visible since 0 <= now_ms)

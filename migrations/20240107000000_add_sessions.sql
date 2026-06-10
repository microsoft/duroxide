-- Add session_id column to worker_queue for session-based worker affinity routing
ALTER TABLE worker_queue ADD COLUMN session_id TEXT;
CREATE INDEX idx_worker_queue_session ON worker_queue(session_id);

-- Sessions table tracks which worker owns which session
CREATE TABLE sessions (
    session_id     TEXT PRIMARY KEY,
    worker_id      TEXT NOT NULL,
    locked_until   INTEGER NOT NULL,    -- ms since epoch (heartbeat lease)
    last_activity_at INTEGER NOT NULL   -- ms since epoch (last work flow)
);

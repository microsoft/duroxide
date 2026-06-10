-- Initial schema for Duroxide SQLite provider

-- Instance metadata
CREATE TABLE IF NOT EXISTS instances (
    instance_id TEXT PRIMARY KEY,
    orchestration_name TEXT NOT NULL,
    orchestration_version TEXT NOT NULL,
    current_execution_id INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Multi-execution support
CREATE TABLE IF NOT EXISTS executions (
    instance_id TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'Running',
    output TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    PRIMARY KEY (instance_id, execution_id)
);

-- Event history (append-only)
CREATE TABLE IF NOT EXISTS history (
    instance_id TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_data TEXT NOT NULL, -- JSON serialized Event
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instance_id, execution_id, event_id)
);

-- Orchestrator queue
CREATE TABLE IF NOT EXISTS orchestrator_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    work_item TEXT NOT NULL, -- JSON serialized WorkItem
    visible_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    lock_token TEXT,
    locked_until TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for orchestrator queue
CREATE INDEX IF NOT EXISTS idx_orch_visible ON orchestrator_queue(visible_at, lock_token);
CREATE INDEX IF NOT EXISTS idx_orch_instance ON orchestrator_queue(instance_id);
CREATE INDEX IF NOT EXISTS idx_orch_lock ON orchestrator_queue(lock_token);

-- Worker queue
CREATE TABLE IF NOT EXISTS worker_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_item TEXT NOT NULL, -- JSON serialized WorkItem
    lock_token TEXT,
    locked_until TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for worker queue
CREATE INDEX IF NOT EXISTS idx_worker_available ON worker_queue(lock_token, id);

-- Timer queue
CREATE TABLE IF NOT EXISTS timer_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_item TEXT NOT NULL, -- JSON serialized WorkItem
    fire_at TIMESTAMP NOT NULL,
    lock_token TEXT,
    locked_until TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for timer queue
CREATE INDEX IF NOT EXISTS idx_timer_fire ON timer_queue(fire_at, lock_token);

-- Instance-level locks for concurrent dispatcher coordination
CREATE TABLE IF NOT EXISTS instance_locks (
    instance_id TEXT PRIMARY KEY,
    lock_token TEXT NOT NULL,
    locked_until INTEGER NOT NULL,
    locked_at INTEGER NOT NULL
);

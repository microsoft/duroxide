-- Add attempt_count column for poison message detection

-- Add attempt_count to orchestrator_queue (non-negative integer)
ALTER TABLE orchestrator_queue ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0);

-- Add attempt_count to worker_queue (non-negative integer)
ALTER TABLE worker_queue ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0);


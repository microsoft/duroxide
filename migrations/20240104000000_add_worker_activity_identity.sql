-- Add activity identity columns to worker_queue for efficient cancellation lookups
-- These columns denormalize the activity identity from the work_item JSON

ALTER TABLE worker_queue ADD COLUMN instance_id TEXT;
ALTER TABLE worker_queue ADD COLUMN execution_id INTEGER;
ALTER TABLE worker_queue ADD COLUMN activity_id INTEGER;

-- Index for efficient cancellation queries
CREATE INDEX IF NOT EXISTS idx_worker_activity ON worker_queue(instance_id, execution_id, activity_id);

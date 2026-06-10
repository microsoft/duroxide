-- Add tag column to worker_queue for activity routing to specialized workers
ALTER TABLE worker_queue ADD COLUMN tag TEXT;
CREATE INDEX idx_worker_queue_tag ON worker_queue(tag);

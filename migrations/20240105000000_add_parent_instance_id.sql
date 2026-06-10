-- Add parent_instance_id to instances table for cascading delete support
-- NULL means this is a root orchestration, non-NULL means it's a sub-orchestration

ALTER TABLE instances ADD COLUMN parent_instance_id TEXT REFERENCES instances(instance_id);

-- Index for efficient parent-child lookups during cascading delete
CREATE INDEX IF NOT EXISTS idx_instances_parent ON instances(parent_instance_id);

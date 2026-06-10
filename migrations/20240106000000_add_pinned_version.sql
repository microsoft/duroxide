-- Add pinned duroxide version columns to executions table for capability filtering.
-- These columns store the semver version of the runtime that created this execution,
-- allowing the provider to filter work items by replay engine compatibility.
-- NULL values mean "always compatible" (pre-migration data, safe default).

ALTER TABLE executions ADD COLUMN duroxide_version_major INTEGER;
ALTER TABLE executions ADD COLUMN duroxide_version_minor INTEGER;
ALTER TABLE executions ADD COLUMN duroxide_version_patch INTEGER;

-- Add custom_status columns to instances table (survives continue_as_new)
ALTER TABLE instances ADD COLUMN custom_status TEXT;
ALTER TABLE instances ADD COLUMN custom_status_version INTEGER NOT NULL DEFAULT 0;

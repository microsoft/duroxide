-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- Support visibility-ordered worker fetches on file-backed databases.
CREATE INDEX IF NOT EXISTS idx_worker_ready_order ON worker_queue(visible_at, id);

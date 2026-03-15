# KV Delta Table: Fixing Read-Modify-Write Replay Poisoning

## Summary

The current KV store materializes every `set_kv_value` / `clear_kv_value` into the `kv_store` table on every orchestrator ack (every turn). This creates a replay correctness bug: when the provider snapshot is seeded into `kv_state` before replay, the orchestration sees the **latest accumulated** value instead of the value as it existed at each point in history. Read-modify-write patterns produce nondeterminism errors.

The fix introduces a `kv_delta` table that captures mutations from the **current execution only**. The existing `kv_store` table is only written at execution completion boundaries (completion, CAN, failure). Replay seeding reads from `kv_store` only (prior-execution state), and client reads merge `kv_store + kv_delta` for a live view.

Additionally, the unused `execution_id` column is dropped from `kv_store`.

---

## The Bug

### Reproduction

An orchestration that does read-modify-write on every turn:

```rust
for _ in 0..5 {
    let val = ctx.get_kv_value("counter").unwrap_or("0".to_string());
    let n: u32 = val.parse().unwrap();
    ctx.set_kv_value("counter", (n + 1).to_string());
    ctx.schedule_activity("Noop", "").await?;
}
```

### What happens

**Turn 1** (first execution, no history): `kv_state` is empty → `get` returns `None` → defaults to `"0"` → `set("counter", "1")`. History records `KeyValueSet { counter, "1" }`. Provider materializes `kv_store: counter = "1"`.

**Turn 2** (handler replays from the start): Provider loads snapshot → seeds `kv_state` with `counter = "1"`. Handler re-runs turn 1's code:
- `get("counter")` → returns `"1"` (from snapshot — **should** be `None`)
- Computes `1 + 1 = 2`
- `set("counter", "2")` → emits `Action::SetKeyValue { counter, "2" }`

Replay engine matches against history event `KeyValueSet { counter, "1" }` → **value mismatch → nondeterminism error**.

### Root cause

`kv_store` is written every turn, so the snapshot fed to the replay engine always contains the **end-state** of all prior turns in the current execution. But replay re-runs the handler from the beginning, so turn 1's code sees turn N-1's accumulated state instead of seeing what it originally saw.

---

## Design

### Two-table model

| Table | Written when | Contains |
|-------|-------------|----------|
| `kv_store` | Execution completion (Completed, CAN, Failed) | Fully merged state at last execution boundary |
| `kv_delta` | Every ack (every turn) | KV mutations from the current execution only |

### Schema changes

**New table:**

```sql
CREATE TABLE IF NOT EXISTS kv_delta (
    instance_id TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       TEXT,                    -- NULL = tombstone (key was cleared)
    last_updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (instance_id, key)
);
```

**Drop column from `kv_store`:**

```sql
-- execution_id is never read, only written. Remove it.
ALTER TABLE kv_store DROP COLUMN execution_id;
-- Drop the associated index
DROP INDEX IF EXISTS idx_kv_store_execution;
```

Final `kv_store` schema:

```sql
CREATE TABLE IF NOT EXISTS kv_store (
    instance_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    last_updated_at_ms INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (instance_id, key)
);
```

### Write path — ack_work_item (every turn)

Today, KV mutations are materialized into `kv_store` during every orchestrator ack. Change this to write to `kv_delta` instead:

```
For each event in history_delta:
    KeyValueSet { key, value, ts }:
        UPSERT into kv_delta (instance_id, key, value, last_updated_at_ms)
    KeyValueCleared { key }:
        UPSERT into kv_delta (instance_id, key, NULL, ts)  -- tombstone
    KeyValuesCleared:
        -- Tombstone everything currently in delta
        UPDATE kv_delta SET value = NULL, last_updated_at_ms = ts
            WHERE instance_id = ?
        -- Tombstone kv_store keys not already represented in delta
        INSERT OR IGNORE INTO kv_delta (instance_id, key, value, last_updated_at_ms)
            SELECT instance_id, key, NULL, ts FROM kv_store WHERE instance_id = ?
```

### Write path — execution completion

When the turn includes a terminal action (OrchestrationCompleted, ContinueAsNew, OrchestrationFailed):

```
1. Merge kv_delta into kv_store:
    -- Apply non-tombstone values
    INSERT INTO kv_store (instance_id, key, value, last_updated_at_ms)
        SELECT instance_id, key, value, last_updated_at_ms
        FROM kv_delta
        WHERE instance_id = ? AND value IS NOT NULL
    ON CONFLICT(instance_id, key)
        DO UPDATE SET value = excluded.value,
                      last_updated_at_ms = excluded.last_updated_at_ms

    -- Apply tombstones (delete from kv_store)
    DELETE FROM kv_store
        WHERE instance_id = ?
        AND key IN (SELECT key FROM kv_delta WHERE instance_id = ? AND value IS NULL)

2. Clear kv_delta for this instance:
    DELETE FROM kv_delta WHERE instance_id = ?
```

### Read path — replay engine (fetch_work_item snapshot)

The snapshot loaded before the handler runs comes from `kv_store` **only**. This gives the handler the prior-execution carry-over state (e.g., values set before a CAN boundary). Current-execution mutations are rebuilt organically as `set_kv_value` calls replay.

```sql
SELECT key, value, last_updated_at_ms FROM kv_store WHERE instance_id = ?
```

No change to the replay engine code itself — only the provider query changes (exclude `kv_delta`).

### Read path — client / cross-instance reads

`get_kv_value(instance, key)` merges both tables:

```sql
-- Check delta first (current execution mutations)
SELECT value FROM kv_delta WHERE instance_id = ? AND key = ?
```

- Row found, `value IS NOT NULL` → return value
- Row found, `value IS NULL` → return `None` (tombstone)
- No row → fall through to:

```sql
SELECT value FROM kv_store WHERE instance_id = ? AND key = ?
```

`get_all_kv_values(instance)` merges both tables:

```sql
SELECT
    COALESCE(d.key, s.key) AS key,
    CASE
        WHEN d.key IS NOT NULL THEN d.value  -- delta wins (NULL = tombstone)
        ELSE s.value
    END AS value
FROM kv_store s
LEFT JOIN kv_delta d ON s.instance_id = d.instance_id AND s.key = d.key
WHERE s.instance_id = ?
UNION
SELECT key, value FROM kv_delta
WHERE instance_id = ? AND value IS NOT NULL
  AND key NOT IN (SELECT key FROM kv_store WHERE instance_id = ?)
```

(Or do it in two queries and merge in Rust — simpler and avoids complex SQL.)

---

## Why this fixes the bug

**Turn 1** (first execution): `kv_store` is empty (no prior execution), `kv_delta` is empty. Handler reads `get("counter")` → `None` → `"0"`. Sets `"1"`. Ack writes to `kv_delta: counter = "1"`.

**Turn 2** (replay of turn 1): Snapshot from `kv_store` only → empty. Handler replays: `get("counter")` → `None` → `"0"`. Sets `"1"`. Matches history. ✓

**CAN boundary**: `kv_delta` merged into `kv_store`. New execution starts with clean delta, sees prior-execution values via `kv_store`. ✓

---

## Impact on existing operations

### Instance deletion (`delete_instances_atomic`)

Add `DELETE FROM kv_delta WHERE instance_id IN (...)` alongside existing `DELETE FROM kv_store`. Trivial.

### Execution pruning (`prune_executions`)

**No change needed.** Prune deletes old executions and their history. It never touches `kv_store` today, and `kv_delta` belongs to the current execution (always protected). `kv_store` is a self-contained snapshot that doesn't depend on history events being present.

### Provider trait

No changes to the `Provider` trait. The delta table is an internal implementation detail of the SQLite provider. Other provider implementations would adopt a similar strategy.

---

## Migration

```sql
-- Migration: add kv_delta table + drop execution_id from kv_store

CREATE TABLE IF NOT EXISTS kv_delta (
    instance_id TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       TEXT,
    last_updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (instance_id, key)
);

-- SQLite doesn't support DROP COLUMN before 3.35.0.
-- Recreate kv_store without execution_id.
CREATE TABLE kv_store_new (
    instance_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    last_updated_at_ms INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (instance_id, key)
);

INSERT INTO kv_store_new (instance_id, key, value, last_updated_at_ms)
    SELECT instance_id, key, value, last_updated_at_ms FROM kv_store;

DROP TABLE kv_store;
ALTER TABLE kv_store_new RENAME TO kv_store;

DROP INDEX IF EXISTS idx_kv_store_execution;
```

### Rolling upgrade safety

During a rolling upgrade, old nodes still write KV to `kv_store` on every ack (old behavior). New nodes write to `kv_delta` and only merge to `kv_store` on completion.

If a new node picks up an instance that was mid-execution on an old node:
- `kv_store` has the accumulated state from old node's acks (stale for new node's model)
- `kv_delta` is empty

The new node seeds `kv_state` from `kv_store` (which has accumulated values from old acks). Replay replays `set_kv_value` calls which overwrite `kv_state` entries. **This is the same bug scenario** — but only for instances that were mid-execution when the upgrade happened. They'll hit the nondeterminism error and fail.

**Mitigation**: Instances that were idle at upgrade time are unaffected. Instances mid-execution will fail and can be restarted (which creates a new execution with a clean state). This is acceptable since:
1. The bug already exists for these instances on old nodes
2. After restart, the new execution starts clean with correct behavior

---

## Test plan

### Existing tests that should continue passing

All existing KV tests in `tests/kv_store_tests.rs` and `tests/replay_engine/kv.rs`.

### New tests (already written, currently failing)

- `kv_read_modify_write_every_turn` (E2E) — RMW on every turn, 5 iterations
- `kv_read_modify_write_every_turn_snapshot_poisons_replay` (replay engine) — Direct replay engine test demonstrating the snapshot poisoning

### Additional tests to add during implementation

- `kv_delta_clear_all_then_set` — `clear_all` followed by new sets in subsequent turns
- `kv_delta_clear_single_with_prior_execution_value` — tombstone overrides `kv_store` value
- `kv_delta_merged_on_completion` — after orchestration completes, `kv_store` reflects merged state and `kv_delta` is empty
- `kv_delta_merged_on_can` — same for continue_as_new
- `kv_delta_client_reads_live_state` — client reads see delta + store merged
- Provider validation tests for the new contract

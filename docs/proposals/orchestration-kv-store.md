# Orchestration KV Store

## Summary

Instance-scoped key-value storage accessible from orchestration context and client. KV mutations are recorded as history events (deterministic, replayable). The provider maintains a materialized KV table indexed on `(instance_id, execution_id, key)` for efficient point lookups. Pruning is execution-aware: when an execution is pruned, any keys whose last-writing execution matches the pruned one are also removed.

---

## Motivation

Orchestrations need lightweight state that:
- Survives across turns and `continue_as_new` boundaries
- Is queryable externally (client, dashboards, other services)
- Is more structured than `custom_status` (which is a single opaque string)
- Follows all determinism rules (replayable, no side effects during replay)

Examples: progress counters, checkpoint markers, computed aggregates, status fields broken out by key.

---

## API Surface

### OrchestrationContext

```rust
impl OrchestrationContext {
    /// Set a key-value pair scoped to this orchestration instance.
    /// Overwrites if the key already exists. Emits a history event.
    /// All determinism rules apply — the value is recorded in history
    /// and replayed on subsequent turns.
    ///
    /// **Limits:** Max 10 keys per instance, max 16 KiB per value.
    /// Exceeding either limit fails the orchestration (same as custom_status
    /// size violations) — checked at ack time in `validate_limits()`.
    pub fn set_value(&self, key: impl Into<String>, value: impl Into<String>);

    /// Typed variant: serializes `value` as JSON before storing.
    pub fn set_value_typed<T: Serialize>(&self, key: impl Into<String>, value: &T) -> Result<(), serde_json::Error>;

    /// Get the current value for a key. Returns `None` if the key
    /// was never set or was removed by a clear operation.
    /// Reads from in-memory state (seeded from provider snapshot at fetch time,
    /// kept up-to-date by the orchestration code's own set/clear calls during replay).
    pub fn get_value(&self, key: &str) -> Option<String>;

    /// Typed variant: deserializes the stored JSON string into `T`.
    /// Returns `None` if the key doesn't exist.
    /// Returns `Err` if the key exists but deserialization fails.
    pub fn get_value_typed<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, serde_json::Error>;

    /// Remove a single key from the KV store.
    /// Emits a history event. After this call, `get_value(key)` returns `None`.
    /// No-op in terms of history if the key doesn't exist — the event is still
    /// recorded for determinism (just like set_value to a key that already has that value).
    pub fn clear_value(&self, key: impl Into<String>);

    /// Clear ALL key-value pairs for this orchestration instance.
    /// Emits a history event. After this call, `get_value` returns `None` for all keys.
    pub fn clear_all_values(&self);

    /// Read a KV entry from another orchestration instance.
    /// This is modeled as a system activity — under the covers it schedules
    /// `__duroxide_syscall:get_kv_value` which calls `client.get_value()`.
    /// Returns the value at the time the activity executes (not replay-cached).
    /// All determinism rules apply: the result is recorded in history as an
    /// activity completion and replayed on subsequent turns.
    pub fn get_value_from_instance(
        &self,
        instance_id: impl Into<String>,
        key: impl Into<String>,
    ) -> impl Future<Output = Result<Option<String>, String>>;

    /// Typed variant: deserializes the returned JSON string into `T`.
    /// Returns `Ok(None)` if the key doesn't exist in the remote instance.
    /// Returns `Err` if the key exists but deserialization fails, or if the
    /// system activity itself fails.
    pub fn get_value_from_instance_typed<T: DeserializeOwned>(
        &self,
        instance_id: impl Into<String>,
        key: impl Into<String>,
    ) -> impl Future<Output = Result<Option<T>, String>>;
}
```

### Client

```rust
impl Client {
    /// Read a single KV entry for the given instance. Reads directly from
    /// the provider's materialized KV table (not from history).
    /// Returns `None` if the key doesn't exist or the instance is unknown.
    pub async fn get_value(&self, instance_id: &str, key: &str) -> Result<Option<String>, ProviderError>;

    /// Typed variant: deserializes the stored JSON string into `T`.
    pub async fn get_value_typed<T: DeserializeOwned>(
        &self, instance_id: &str, key: &str,
    ) -> Result<Option<T>, ProviderError>;
}
```

---

## Event & Action Design

### New Action Variants

```rust
pub enum Action {
    // ... existing variants ...

    /// Set a key-value pair in the orchestration's KV store.
    SetKeyValue { key: String, value: String },

    /// Remove a single key from the orchestration's KV store.
    ClearKeyValue { key: String },

    /// Clear all key-value pairs for this orchestration instance.
    ClearKeyValues,
}
```

### New EventKind Variants

```rust
pub enum EventKind {
    // ... existing variants ...

    /// A key-value pair was set via `ctx.set_value()`.
    #[serde(rename = "KeyValueSet")]
    KeyValueSet { key: String, value: String },

    /// A single key was removed via `ctx.clear_value(key)`.
    #[serde(rename = "KeyValueCleared")]
    KeyValueCleared { key: String },

    /// All key-value pairs were cleared via `ctx.clear_all_values()`.
    #[serde(rename = "KeyValuesCleared")]
    KeyValuesCleared,
}
```

### action_to_event Mapping

```rust
Action::SetKeyValue { key, value } => EventKind::KeyValueSet { key, value },
Action::ClearKeyValue { key } => EventKind::KeyValueCleared { key },
Action::ClearKeyValues => EventKind::KeyValuesCleared,
```

`SetKeyValue`, `ClearKeyValue`, and `ClearKeyValues` do NOT have a `scheduling_event_id` — they are fire-and-forget metadata actions (like `UpdateCustomStatus`), not schedule/completion pairs. They don't produce tokens or futures.

`get_value_from_instance` is NOT a metadata action — it uses `schedule_activity` with the system activity name, so it flows through the normal activity schedule/completion path and produces a `DurableFuture`.

---

## Replay Behavior

### In-Memory KV State

`CtxInner` gains a new field:

```rust
struct CtxInner {
    // ... existing fields ...

    /// Accumulated KV state, seeded from the provider's materialized kv_store
    /// table at fetch time. Updated by the orchestration code's own
    /// set_value()/clear_value()/clear_all_values() calls during both replay and
    /// new execution. NOT reconstructed from history events.
    kv_state: HashMap<String, String>,
}
```

### Why Snapshot-Only (Not History Reconstruction)

`fetch_orchestration_item` only loads history for the **current execution** — events from prior executions (before `continue_as_new`) are not in the baseline history. That means replaying history events to reconstruct KV state would miss writes from prior executions. The provider's materialized `kv_store` table already reflects all writes from all executions, so loading the snapshot is the correct and complete approach.

### How kv_state Stays Current During Replay

The orchestration code re-executes during replay. When it calls `set_value("A", "1")`, the method:
1. Inserts `"A" → "1"` into `kv_state` (in-memory update)
2. Emits `Action::SetKeyValue { key: "A", value: "1" }`

The replay engine then matches that action against the corresponding `KeyValueSet` history event for nondeterminism detection — but it does **not** separately update `kv_state` from the event. The code's own call already did that.

Same pattern for `clear_value` and `clear_all_values` — the method updates `kv_state` and emits the action. The replay engine validates the action against history.

### Action Validation During Replay

```rust
// KeyValueSet, KeyValueCleared, and KeyValuesCleared are metadata events —
// validated the same way as CustomStatusUpdated: the action is popped and
// matched to the event kind, but no token is bound.
```

### get_value is Pure Read

`get_value()` reads from `kv_state` — no provider call, no event emitted, fully deterministic. The in-memory map is the source of truth during orchestration execution.

### get_value_from_instance is a System Activity

`get_value_from_instance(instance_id, key)` follows the existing syscall activity pattern (`new_guid`, `utc_now_ms`). It schedules an activity named `__duroxide_syscall:get_kv_value` with a JSON-serialized input `{"instance_id": "...", "key": "..."}`. The runtime's builtin activity registry handles this by calling `client.get_value(instance_id, key)` and returning the result (or `None` serialized as JSON).

Because it's a normal activity:
- The result is recorded as `ActivityCompleted`/`ActivityFailed` in history
- On replay, the recorded result is returned — no provider call
- It returns a `DurableFuture` that must be `.await`ed
- It can be used in `select2`, `join`, etc.
- It's subject to activity worker execution (dispatched via WorkDispatcher)

```rust
// Constant
pub(crate) const SYSCALL_ACTIVITY_GET_KV_VALUE: &str = "__duroxide_syscall:get_kv_value";

// Registration in runtime setup
.register_builtin(
    SYSCALL_ACTIVITY_GET_KV_VALUE,
    |ctx: ActivityContext, input: String| async move {
        #[derive(Deserialize)]
        struct GetKvInput { instance_id: String, key: String }
        let req: GetKvInput = serde_json::from_str(&input)
            .map_err(|e| format!("Invalid get_kv_value input: {e}"))?;
        // ActivityContext::get_client() returns a Client backed by the provider
        let client = ctx.get_client();
        let value = client.get_value(&req.instance_id, &req.key).await
            .map_err(|e| format!("Client error: {e}"))?;
        Ok(serde_json::to_string(&value).unwrap())
    },
)

// OrchestrationContext implementation
pub fn get_value_from_instance(
    &self,
    instance_id: impl Into<String>,
    key: impl Into<String>,
) -> impl Future<Output = Result<Option<String>, String>> {
    let input = serde_json::json!({
        "instance_id": instance_id.into(),
        "key": key.into(),
    }).to_string();
    let fut = self.schedule_activity(SYSCALL_ACTIVITY_GET_KV_VALUE, input);
    async move {
        let result = fut.await?;
        serde_json::from_str(&result).map_err(|e| format!("Deserialization error: {e}"))
    }
}

// Typed variant
pub fn get_value_from_instance_typed<T: DeserializeOwned>(
    &self,
    instance_id: impl Into<String>,
    key: impl Into<String>,
) -> impl Future<Output = Result<Option<T>, String>> {
    let fut = self.get_value_from_instance(instance_id, key);
    async move {
        match fut.await? {
            None => Ok(None),
            Some(json_str) => serde_json::from_str(&json_str)
                .map(Some)
                .map_err(|e| format!("Typed deserialization error: {e}")),
        }
    }
}
```

---

## Provider Contract

### New Provider Trait Methods

```rust
pub trait Provider: Send + Sync {
    // ... existing methods ...

    /// Read a single KV entry for the given instance.
    /// Returns `None` if the key doesn't exist.
    /// This is used by the Client API for external reads.
    async fn get_kv_value(
        &self,
        instance_id: &str,
        key: &str,
    ) -> Result<Option<String>, ProviderError>;

    /// Load the full KV snapshot for an instance.
    /// Called during fetch_orchestration_item to seed the in-memory KV state.
    /// Returns all current key-value pairs for the instance.
    async fn get_kv_snapshot(
        &self,
        instance_id: &str,
    ) -> Result<HashMap<String, String>, ProviderError>;
}
```

### KV Materialization in ack_orchestration_item

The provider extracts KV mutations from `history_delta` during ack, similar to how `CustomStatusUpdated` is extracted today:

```rust
// Inside ack_orchestration_item transaction:
for event in &history_delta {
    match &event.kind {
        EventKind::KeyValueSet { key, value } => {
            // UPSERT into kv_store table
            // instance_id = instance, execution_id = event.execution_id, key = key, value = value
            sqlx::query(
                "INSERT INTO kv_store (instance_id, execution_id, key, value)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(instance_id, key) DO UPDATE
                 SET value = excluded.value, execution_id = excluded.execution_id"
            )
            .bind(&instance_id)
            .bind(event.execution_id as i64)
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await?;
        }
        EventKind::KeyValueCleared { key } => {
            // Delete a single KV entry
            sqlx::query("DELETE FROM kv_store WHERE instance_id = ? AND key = ?")
                .bind(&instance_id)
                .bind(key)
                .execute(&mut *tx)
                .await?;
        }
        EventKind::KeyValuesCleared => {
            // Delete all KV entries for this instance
            sqlx::query("DELETE FROM kv_store WHERE instance_id = ?")
                .bind(&instance_id)
                .execute(&mut *tx)
                .await?;
        }
        _ => {}
    }
}
```

### Provider Storage Schema (SQLite Reference)

```sql
CREATE TABLE IF NOT EXISTS kv_store (
    instance_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    PRIMARY KEY (instance_id, key)
);

-- For pruning: find keys last-written by a specific execution
CREATE INDEX idx_kv_store_execution ON kv_store(instance_id, execution_id);
```

Key design points:
- **Primary key is `(instance_id, key)`** — only the latest value per key is stored
- **`execution_id`** tracks which execution last wrote this key (for pruning)
- No `created_at` column needed — history events have timestamps for audit trail

---

## KV Loading Strategy

The KV snapshot is loaded as part of `fetch_orchestration_item`. This is the only correct approach because:

1. **History only covers the current execution** — `fetch_orchestration_item` loads history for the current `execution_id` only. KV writes from prior executions (before `continue_as_new`) are not in the baseline history and cannot be reconstructed from it.
2. **The provider table is the source of truth** — the `kv_store` table reflects all writes from all executions, materialized during `ack_orchestration_item`. Loading the snapshot gives the complete, correct initial state.
3. **Atomicity** — the snapshot is loaded under the same instance lock as the history and messages, ensuring a consistent view.

```rust
pub struct OrchestrationItem {
    pub messages: Vec<WorkItem>,
    pub history: Vec<Event>,
    pub execution_id: u64,
    pub kv_snapshot: HashMap<String, String>,  // NEW
}
```

**How it works:**
1. `fetch_orchestration_item` loads `kv_snapshot` from the `kv_store` table (all rows for the instance)
2. Runtime seeds `ctx.kv_state = kv_snapshot`
3. Orchestration code re-executes — its own calls to `set_value`/`clear_value`/`clear_all_values` update `kv_state` directly
4. Replay engine validates KV actions against history events (nondeterminism detection) but does not update `kv_state` — the code's calls already did that
5. `get_value` reads from `kv_state` at any point — correct and deterministic

The KV table is capped at 10 keys per instance (enforced at ack time), so the snapshot payload is bounded and small.

---

## ContinueAsNew Behavior

When `continue_as_new()` is called:

1. Current execution emits `OrchestrationContinuedAsNew` terminal event
2. Provider acks this turn — KV table retains all keys written by *any* execution (including current)
3. New execution starts with `execution_id + 1`
4. On first turn, `fetch_orchestration_item` loads the KV snapshot → the new execution sees all keys from previous executions
5. If the new execution calls `set_value("key", "new_val")`, the provider UPSERTs with the new execution_id
6. If the new execution calls `clear_all_values()`, all keys are deleted

**No explicit carry-forward needed** — unlike `initial_custom_status` which is carried via `OrchestrationStarted`, KV state persists in the provider table across executions naturally.

The execution_id stamp on each key allows pruning to know which execution "owns" each key's current value.

---

## Pruning Behavior

### Execution Pruning (continue_as_new chains)

When `prune_executions` removes old executions:

```sql
-- For each pruned execution_id:
-- Delete KV entries whose last-writing execution matches the pruned one.
-- This means: if exec 1 set key "A" and exec 3 later set key "A" to a new value,
-- pruning exec 1 does NOT delete key "A" (its execution_id is now 3).
-- Only keys that were LAST written by the pruned execution are removed.
DELETE FROM kv_store 
WHERE instance_id = ? AND execution_id = ?;
```

This is safe because:
- If a later execution overwrote the key, the execution_id was updated → the key survives
- If the key was only ever set by the pruned execution, it's stale data from a pruned execution → safe to delete

### Instance Deletion

When `delete_instance` removes an instance entirely:

```sql
DELETE FROM kv_store WHERE instance_id = ?;
```

Added to the existing cascading delete transaction.

### Bulk Pruning

`prune_executions_bulk` iterates instances and applies the same per-execution logic.

---

## Provider Validation Tests

New validation tests in `src/provider_validation/`:

```rust
/// Provider must store and retrieve KV values
async fn kv_set_and_get(provider: &impl Provider) { ... }

/// Provider must overwrite existing keys
async fn kv_overwrite(provider: &impl Provider) { ... }

/// Provider must clear all KV entries on KeyValuesCleared
async fn kv_clear(provider: &impl Provider) { ... }

/// Provider must prune KV entries when execution is pruned
async fn kv_prune_with_execution(provider: &impl ProviderAdmin) { ... }

/// Provider must delete KV entries when instance is deleted
async fn kv_delete_with_instance(provider: &impl ProviderAdmin) { ... }

/// Provider must track execution_id per key (last-writer-wins)
async fn kv_execution_id_tracking(provider: &impl Provider) { ... }

/// get_kv_snapshot returns all current keys for an instance
async fn kv_snapshot_complete(provider: &impl Provider) { ... }

/// get_kv_value returns None for non-existent key
async fn kv_get_nonexistent(provider: &impl Provider) { ... }
```

---

## Observability

### Tracing Spans

```rust
// In set_value:
trace::info!(key = %key, value_len = value.len(), "kv.set");

// In clear_all_values:
trace::info!("kv.clear");

// In get_value:
trace::debug!(key = %key, found = kv_state.contains_key(key), "kv.get");
```

KV operations are replay-guarded (like `trace_info`): only emit telemetry on first execution, not during replay.

---

## Size Limits

### Constants

Added to `src/runtime/limits.rs` alongside `MAX_CUSTOM_STATUS_BYTES` and `MAX_TAG_NAME_BYTES`:

```rust
/// Maximum number of KV keys per orchestration instance.
pub const MAX_KV_KEYS: usize = 10;

/// Maximum size of a single KV value in bytes (16 KiB).
pub const MAX_KV_VALUE_BYTES: usize = 16 * 1024;
```

### Enforcement

Limits are checked in the **`validate_limits()`** function in the orchestration dispatcher (`src/runtime/dispatchers/orchestration.rs`), the same place that enforces `MAX_CUSTOM_STATUS_BYTES` and `MAX_TAG_NAME_BYTES`. Validation runs **after** the orchestration code has executed and produced a `history_delta`, but **before** the delta is committed to the provider via `ack_orchestration_item`.

#### Key Count Check

Compute the effective KV key count by applying the turn's `KeyValueSet`, `KeyValueCleared`, and `KeyValuesCleared` events to the snapshot loaded at fetch time:

```rust
// Start from snapshot key set, apply delta events
let mut effective_keys: HashSet<String> = kv_snapshot.keys().cloned().collect();
for event in &history_delta {
    match &event.kind {
        EventKind::KeyValueSet { key, .. } => { effective_keys.insert(key.clone()); }
        EventKind::KeyValueCleared { key } => { effective_keys.remove(key); }
        EventKind::KeyValuesCleared => { effective_keys.clear(); }
        _ => {}
    }
}

if effective_keys.len() > MAX_KV_KEYS {
    tracing::warn!(
        target: "duroxide::runtime",
        instance_id = %instance,
        execution_id = %execution_id,
        key_count = effective_keys.len(),
        max_keys = MAX_KV_KEYS,
        "KV key count exceeds limit"
    );
    tracing::error!(
        target: "duroxide::runtime",
        instance_id = %instance,
        execution_id = %execution_id,
        key_count = effective_keys.len(),
        max_keys = MAX_KV_KEYS,
        "KV key count exceeds limit, failing orchestration"
    );
    // increment duroxide.kv.limit_exceeded.key_count counter
    fail_orchestration_for_limit(
        format!("KV key count ({}) exceeds limit ({})", effective_keys.len(), MAX_KV_KEYS),
        ...
    );
    return true;
}
```

#### Value Size Check

Scan `history_delta` for `KeyValueSet` events with oversized values:

```rust
let oversized_kv = history_delta.iter().find_map(|e| {
    if let EventKind::KeyValueSet { key, value } = &e.kind {
        if value.len() > MAX_KV_VALUE_BYTES {
            Some((key.clone(), value.len()))
        } else {
            None
        }
    } else {
        None
    }
});

if let Some((key, value_len)) = oversized_kv {
    tracing::warn!(
        target: "duroxide::runtime",
        instance_id = %instance,
        execution_id = %execution_id,
        key = %key,
        value_bytes = value_len,
        max_bytes = MAX_KV_VALUE_BYTES,
        "KV value exceeds size limit"
    );
    tracing::error!(
        target: "duroxide::runtime",
        instance_id = %instance,
        execution_id = %execution_id,
        key = %key,
        value_bytes = value_len,
        max_bytes = MAX_KV_VALUE_BYTES,
        "KV value exceeds size limit, failing orchestration"
    );
    // increment duroxide.kv.limit_exceeded.value_size counter
    fail_orchestration_for_limit(
        format!("KV value for key '{}' ({} bytes) exceeds limit ({} bytes)", key, value_len, MAX_KV_VALUE_BYTES),
        ...
    );
    return true;
}
```

### Failure Mode

Same as custom_status and tag violations: the orchestration is failed with an `Application` error (`AppErrorKind::OrchestrationFailed`, `retryable: false`). An `OrchestrationFailed` event is appended to the history delta. Worker and orchestrator items are cleared (no activities dispatched). The provider sees the failure on ack.

### Metrics

**Operation counters (replay-guarded):**
- `duroxide.kv.set` — counter of set_value calls (non-replay only)
- `duroxide.kv.clear_value` — counter of clear_value calls (non-replay only)
- `duroxide.kv.clear_all` — counter of clear_all_values calls (non-replay only)
- `duroxide.kv.get` — counter of get_value calls
- `duroxide.kv.get_remote` — counter of get_value_from_instance calls (non-replay only)

**Size / capacity gauges (emitted per-turn, stamped with instance_id):**
- `duroxide.kv.key_count` — gauge: number of keys in the in-memory KV state after the turn, tagged with `instance_id`
- `duroxide.kv.largest_value_bytes` — gauge: size in bytes of the largest value in the KV state after the turn, tagged with `instance_id`

**Limit violation counters:**
- `duroxide.kv.limit_exceeded.key_count` — counter: incremented when a turn is rejected because the key count exceeds `MAX_KV_KEYS` (tagged with `instance_id`)
- `duroxide.kv.limit_exceeded.value_size` — counter: incremented when a turn is rejected because a value exceeds `MAX_KV_VALUE_BYTES` (tagged with `instance_id`, `key`)

---

## Migration

### SQLite Migration

```sql
-- migrations/20240110000000_add_kv_store.sql
CREATE TABLE IF NOT EXISTS kv_store (
    instance_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    PRIMARY KEY (instance_id, key)
);

CREATE INDEX IF NOT EXISTS idx_kv_store_execution 
ON kv_store(instance_id, execution_id);
```

### Rolling Upgrade Safety

- New event variants (`KeyValueSet`, `KeyValueCleared`, `KeyValuesCleared`) are additive — old runtimes skip unknown events during replay (they won't process KV actions but won't crash)
- New provider methods (`get_kv_value`, `get_kv_snapshot`) have default implementations returning empty results for providers that haven't implemented KV yet
- The KV table migration is additive — no existing tables are modified
- Old ack_orchestration_item implementations will ignore KV events in history_delta (they only scan for `CustomStatusUpdated` and terminal events)

---

## Implementation Sequence

1. **Schema & Types** — Add `EventKind::KeyValueSet`/`KeyValueCleared`/`KeyValuesCleared`, `Action::SetKeyValue`/`ClearKeyValue`/`ClearKeyValues`, migration SQL
2. **Limits** — Add `MAX_KV_KEYS` and `MAX_KV_VALUE_BYTES` constants to `src/runtime/limits.rs`
3. **action_to_event** — Map new actions to events in replay engine
4. **Replay Engine** — Validate KV actions against history events during replay (nondeterminism detection)
5. **OrchestrationContext** — Implement `set_value`, `get_value`, `clear_value`, `clear_all_values`, typed variants
6. **Provider trait** — Add `get_kv_value`, `get_kv_snapshot` with default impls
7. **SQLite provider** — Implement KV materialization in `ack_orchestration_item`, snapshot loading in `fetch_orchestration_item`, new methods, pruning integration
8. **KV Loading** — Pass KV snapshot from fetch into CtxInner to seed `kv_state`
9. **Limit Validation** — Add KV checks to `validate_limits()` in orchestration dispatcher (key count, value size), emit warn/error logs and metrics
10. **Client** — Add `get_value`, `get_value_typed`
11. **System Activity** — Register `__duroxide_syscall:get_kv_value` builtin, implement `get_value_from_instance` + typed variant
12. **Pruning** — Add KV cleanup to `prune_executions` and `delete_instance`
13. **Provider validation tests** — New test module
14. **Scenario tests** — E2E patterns (including limit enforcement tests)

---

## Open Questions

1. **Can activities read KV values?** Yes — `Client` is available in `ActivityContext`, so activities can call `client.get_value(instance_id, key)` directly. This is a normal provider read, not a durable operation, so it doesn't go through history. The orchestration's `get_value()` (in-memory, deterministic) is separate from the client's `get_value()` (provider read, non-deterministic).

2. **Should `get_value_from_instance` support reading multiple keys at once?** For v1, single key only. A `get_values_from_instance(instance_id, keys: Vec<&str>)` batch variant could be added later as a separate system activity to reduce round-trips.

---

## Related Documents

- [Test Plan](orchestration-kv-store-test-plan.md)

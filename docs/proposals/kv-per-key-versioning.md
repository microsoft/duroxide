# KV Per-Key Versioning

## Summary

Add a monotonically increasing per-instance version counter that stamps every KV mutation (`set`, `clear`, `clear_all`). This mirrors `custom_status_version` semantics — enabling efficient change detection and polling without re-reading unchanged values.

---

## Motivation

`custom_status` has a `custom_status_version` counter that enables efficient change detection: clients call `get_custom_status(instance, last_seen_version)` and get `None` (no change) or `Some((value, new_version))`. This avoids wasteful polling.

KV store entries lack this. `wait_for_kv_value` polls with `get_kv_value` every iteration, comparing the full value. There's no way to detect changes without reading the value, and no way to efficiently *wait for any key to change*.

Adding a per-key version counter enables:
1. **Change detection**: `get_kv_value_if_changed(instance, key, last_seen_version)` returns only if the version advanced
2. **Efficient polling**: `wait_for_kv_value_change` can skip no-op polls by passing last-seen version
3. **Consistency**: Same pattern as `custom_status_version` — monotonically increasing instance-scoped counter

---

## Design

### Per-instance version counter

A new column on the `instances` table:

```sql
ALTER TABLE instances ADD COLUMN kv_version_counter INTEGER NOT NULL DEFAULT 0;
```

This is a monotonically increasing instance-scoped counter. Every `KeyValueSet`, `KeyValueCleared`, or `KeyValuesCleared` event in a history delta increments it and stamps the corresponding row(s) in `kv_delta`.

### Per-key version stamp

New column on both KV tables:

```sql
ALTER TABLE kv_store ADD COLUMN version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE kv_delta ADD COLUMN version INTEGER NOT NULL DEFAULT 0;
```

Each KV mutation stamps its row with the incremented counter value. On merge (completion/CAN), the version carries from `kv_delta` → `kv_store`.

### Write path — `ack_orchestration_item`

For each KV mutation event in `history_delta`:

1. Increment counter: `UPDATE instances SET kv_version_counter = kv_version_counter + 1 WHERE instance_id = ?` and read back the new value
2. Stamp the `kv_delta` row with that version
3. On merge (terminal turn), copy version from `kv_delta` → `kv_store`

Since multiple KV events can appear in a single delta, we increment once per event (not once per ack). This matches `custom_status_version` behavior — each `set_custom_status` call produces one `CustomStatusUpdated` event and one version increment.

For `KeyValuesCleared`: each tombstone gets its own version increment (same as if you cleared them one by one). This is consistent — every mutation bumps the counter.

### Read path — Provider trait additions

```rust
/// Read a single KV entry with version.
/// Returns Ok(Some((value, version))) if the key exists.
async fn get_kv_value_versioned(
    &self, instance: &str, key: &str,
) -> Result<Option<(String, u64)>, ProviderError>;

/// Read a single KV entry only if its version > last_seen_version.
/// Returns None if unchanged. Enables efficient polling.
async fn get_kv_value_if_changed(
    &self, instance: &str, key: &str, last_seen_version: u64,
) -> Result<Option<(String, u64)>, ProviderError>;
```

### Client API additions

```rust
/// Read a single KV entry with its version.
pub async fn get_kv_value_versioned(
    &self, instance: &str, key: &str,
) -> Result<Option<(String, u64)>, ClientError>;

/// Poll until a KV key changes from the given version.
pub async fn wait_for_kv_value_change(
    &self, instance: &str, key: &str,
    last_seen_version: u64, timeout: Duration,
) -> Result<(String, u64), ClientError>;
```

### Snapshot carries version

`KvEntry` already has `last_updated_at_ms`. Add `version: u64`:

```rust
pub struct KvEntry {
    pub value: String,
    pub last_updated_at_ms: u64,
    pub version: u64,  // NEW
}
```

The replay engine seeds versions from the snapshot, allowing the orchestration context to read versions internally if needed (future use).

---

## What stays the same

- `get_kv_value(instance, key)` — unchanged, returns `Option<String>`
- `get_kv_all_values(instance)` — unchanged, returns `HashMap<String, String>`
- Orchestration context `set_kv_value` / `get_kv_value` — unchanged
- `KeyValueSet` event kind — unchanged (version is a provider-only concept, not in events)

---

## Migration

```sql
ALTER TABLE instances ADD COLUMN kv_version_counter INTEGER NOT NULL DEFAULT 0;
ALTER TABLE kv_store ADD COLUMN version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE kv_delta ADD COLUMN version INTEGER NOT NULL DEFAULT 0;
```

Pre-existing KV entries get version 0. First mutation after upgrade bumps to 1+. Clients calling `get_kv_value_if_changed(instance, key, 0)` will see 0 as "never seen" and get the current value.

---

## Rolling upgrade safety

Old nodes don't write the `version` column — it stays at the DEFAULT 0. New nodes read version 0 as "unknown" (same as never-incremented). After full rollout, all new writes get proper versions. No mixed-cluster inconsistency — worst case is a stale version=0 that triggers a redundant read on the first poll.

---

## Test plan

### Provider validation tests (`src/provider_validation/kv_store.rs`)

| Test | Description |
|------|-------------|
| `test_kv_version_increments_on_set` | Set a key 3 times across 3 acks. Read with `get_kv_value_versioned`. Version must be ≥3 and monotonically increasing. |
| `test_kv_version_increments_on_clear` | Set a key, then clear it. The version for that key should have advanced. |
| `test_kv_version_no_change_returns_none` | Set a key (version=V). Call `get_kv_value_if_changed(key, V)`. Must return `None` (unchanged). |
| `test_kv_version_cross_key_independence` | Set key "a" (version=1), set key "b" (version=2). Versions are instance-scoped — "a" has version 1, "b" has version 2. They're NOT per-key counters, they're stamped from a shared instance counter. |
| `test_kv_version_survives_can` | Set key in exec 1, CAN. Key version in kv_store matches what was written. New execution reads via snapshot — version preserved. |
| `test_kv_version_clear_all_bumps_each_key` | Set 3 keys, then `KeyValuesCleared`. Each tombstone gets a distinct version (3 increments). |
| `test_kv_version_merged_on_completion` | Set key in delta (version=V). Complete. Read from kv_store — version=V persisted. |
| `test_kv_version_in_snapshot` | Set key, CAN. Fetch snapshot for new execution. `KvEntry.version` field populated. |

### E2E runtime tests (`tests/kv_store_tests.rs`)

| Test | Description |
|------|-------------|
| `kv_version_increments_e2e` | Orchestration sets key 3 times across turns. Client reads version after completion. Version ≥ 3. |
| `kv_wait_for_change_e2e` | Orchestration sets key "progress" to "50%" then "100%" across turns. Client calls `wait_for_kv_value_change("progress", initial_version, 5s)`. Returns "100%" with version > initial. |

### Client sample tests (`tests/e2e_samples.rs`)

| Test | Description |
|------|-------------|
| `sample_kv_version_polling` | Demonstrates the polling pattern: client starts orchestration, polls with `wait_for_kv_value_change` in a loop, accumulates version history, asserts monotonicity. |

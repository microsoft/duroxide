# Orchestration KV Store — Test Plan

Cross-reference: [Design Document](orchestration-kv-store.md)

> **Collapsed revision.** Reduced from ~130 to ~87 tests (~33%). Section 3 (OrchestrationContext
> unit tests) eliminated — `OrchestrationContext` is tightly coupled to the replay engine and
> cannot be meaningfully unit-tested in isolation; all its behaviors are covered by E2E tests.
> Additional merges within Sections 1, 2, 4, and 5 removed duplicative coverage. See per-section
> notes for specific collapses.

---

## 1. Provider Validation Tests (22 tests)

> Location: `src/provider_validation/kv.rs` + registration in `src/provider_validations.rs`
> Applied to: SQLite provider (and any future providers)

### 1.1 Basic CRUD

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| PV-KV-01 | `kv_set_and_get` | Set a key via `KeyValueSet` event in ack, then `get_kv_value` | Returns `Some(value)` |
| PV-KV-02 | `kv_get_nonexistent` | `get_kv_value` for a key that was never set | Returns `None` |
| PV-KV-03 | `kv_get_unknown_instance` | `get_kv_value` for an instance that doesn't exist | Returns `None` (not error) |
| PV-KV-04 | `kv_overwrite` | Set key "A" to "1", then set "A" to "2" in a subsequent ack | `get_kv_value("A")` returns `"2"` |
| PV-KV-05 | `kv_clear_all` | Set keys "A", "B", then ack with `KeyValuesCleared` event | Both keys return `None` |
| PV-KV-05a | `kv_clear_single_key` | Set keys "A", "B", then ack with `KeyValueCleared { key: "A" }` event | "A" returns `None`, "B" still returns its value |
| PV-KV-05b | `kv_clear_nonexistent_key` | Ack with `KeyValueCleared { key: "missing" }` (key never set) | No error — operation is idempotent |
| PV-KV-06 | `kv_set_after_clear` | Clear all, then set "X" in same ack (after the clear event) | "X" exists, old keys don't |
| PV-KV-07 | `kv_empty_value` | Set key "A" to `""` (empty string) | Returns `Some("")` — not `None` |
| PV-KV-08 | `kv_large_value` | Set a key with a 64KB value | Stored and retrieved correctly |
| PV-KV-09 | `kv_special_chars_in_key` | Keys with spaces, unicode, dots, slashes | All stored/retrieved correctly |

> Removed: `kv_multiple_keys` — trivially covered by PV-KV-12 (`kv_snapshot_complete`) which sets multiple keys across acks.

### 1.2 Execution ID Tracking

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| PV-KV-10 | `kv_execution_id_lifecycle` | Set "A" in exec 1 (verify exec_id=1), overwrite "A" in exec 2 (verify exec_id=2). Set "B" only in exec 1, verify exec_id=1. | Execution IDs are stamped on set and updated on overwrite. Mixed IDs coexist correctly. |

> Collapsed: Former PV-KV-11/12/13 — all test the same execution_id stamping mechanism at different stages. One test covers the full lifecycle.

### 1.3 Snapshot

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| PV-KV-11 | `kv_snapshot_empty` | `get_kv_snapshot` for instance with no KV | Returns empty HashMap |
| PV-KV-12 | `kv_snapshot_complete` | Set "A", "B", "C" across multiple acks | Snapshot contains all three |
| PV-KV-13 | `kv_snapshot_after_clear` | Set keys, clear, set new keys | Snapshot only has keys set after clear |
| PV-KV-13a | `kv_snapshot_after_clear_single` | Set "A", "B", clear "A" only | Snapshot has "B" but not "A" |
| PV-KV-14 | `kv_snapshot_cross_execution` | Set "A" in exec 1, "B" in exec 2, no overwrites | Snapshot contains both |

### 1.4 Pruning Integration

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| PV-KV-15 | `kv_prune_execution_removes_keys` | Set "A" in exec 1 (only), prune exec 1 | "A" returns `None` |
| PV-KV-16 | `kv_prune_execution_preserves_overwritten` | Set "A" in exec 1, overwrite "A" in exec 2, prune exec 1 | "A" still returns exec 2's value |
| PV-KV-17 | `kv_prune_current_execution_protected` | Attempt to prune current execution | Current execution is never pruned (standard prune rules) — KV survives |
| PV-KV-18 | `kv_delete_instance_cascades` | Set keys, delete entire instance | All KV entries removed |
| PV-KV-19 | `kv_delete_instance_with_children` | Parent + child orchestrations with KV, delete parent tree | Both parent and child KV removed |

> Removed: `kv_prune_execution_partial` — is exactly PV-KV-15 + PV-KV-16 combined. No novel behavior tested.

### 1.5 Isolation

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| PV-KV-20 | `kv_instance_isolation` | Two instances set same key "A" to different values | `get_kv_value(inst1, "A")` ≠ `get_kv_value(inst2, "A")` |
| PV-KV-21 | `kv_clear_isolation` | Clear KV on inst1 | inst2's KV is unaffected |

---

## 2. Replay Engine Unit Tests (15 tests)

> Location: `src/runtime/replay_engine.rs` (or `tests/replay_kv.rs`)

### 2.1 Action/Event Conversion

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| RE-KV-01 | `action_to_event_kv_variants` | All KV action→event conversions: `SetKeyValue{k,v}` → `KeyValueSet{k,v}`, `ClearKeyValues` → `KeyValuesCleared`, `ClearKeyValue{k}` → `KeyValueCleared{k}` | Correct event kind for each variant. All produce `source_event_id = None`. |

> Collapsed: Former RE-KV-01/02/02a/03 — all trivially different conversions with identical structure.

### 2.2 Replay Determinism

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| RE-KV-02 | `replay_set_value_reconstructs_state` | Snapshot has no keys. Orch code calls `set_value("A","1")`. History has `KeyValueSet{A=1}`. | `ctx.get_value("A")` returns `Some("1")` during replay (from code's own call, not history scanning) |
| RE-KV-03 | `replay_multiple_sets_last_wins` | History: [KeyValueSet{A=1}, KeyValueSet{A=2}]. | `ctx.get_value("A")` returns `Some("2")` |
| RE-KV-04 | `replay_clear_then_set` | History: [KeyValueSet{A=1}, KeyValuesCleared, KeyValueSet{B=2}]. | "A" is `None`, "B" is `Some("2")` |
| RE-KV-04a | `replay_clear_single_key` | History: [KeyValueSet{A=1}, KeyValueSet{B=2}, KeyValueCleared{A}]. | "A" is `None`, "B" is `Some("2")` |
| RE-KV-05 | `replay_set_value_matches_action` | Orchestration calls `set_value("A", "1")`. History has `KeyValueSet{A=1}`. | No nondeterminism error. Action matched to event. |
| RE-KV-06 | `replay_set_value_mismatch_nondeterminism` | Orchestration calls `set_value("A", "1")`. History has `KeyValueSet{A=2}`. | Nondeterminism error (action doesn't match event) |
| RE-KV-07 | `replay_clear_all_values_matches_action` | Orchestration calls `clear_all_values()`. History has `KeyValuesCleared`. | No nondeterminism error |
| RE-KV-07a | `replay_clear_value_matches_action` | Orchestration calls `clear_value("A")`. History has `KeyValueCleared{A}`. | No nondeterminism error |
| RE-KV-07b | `replay_clear_value_mismatch_nondeterminism` | Orchestration calls `clear_value("A")`. History has `KeyValueCleared{B}`. | Nondeterminism error (key mismatch) |
| RE-KV-08 | `replay_kv_interleaved_with_activities` | History: [ActivityScheduled, KeyValueSet, ActivityCompleted]. | KV state correct. Activity result correct. Event ordering preserved. |
| RE-KV-09 | `replay_kv_no_token_binding` | Set value during replay | No token is bound (unlike schedule actions). No future created. |

### 2.3 New Events & Snapshot Seeding

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| RE-KV-10 | `new_kv_events_after_replay` | After replay completes: `set_value("X","Y")`, `clear_all_values()`, `clear_value("Z")`. Also: `set_value("A","1")` then `get_value("A")`. | `history_delta` contains all three event types. `get_value("A")` returns `Some("1")` immediately (not just after ack). |
| RE-KV-11 | `kv_state_seeded_from_snapshot` | Snapshot has {"X": "from_prev_exec"}. Current exec history has no KV events. Code calls `get_value("X")`. | Returns `Some("from_prev_exec")` — snapshot is the initial state, no history reconstruction needed |
| RE-KV-12 | `kv_state_snapshot_overridden_by_code` | Snapshot has {"X": "old"}. Code calls `set_value("X", "new")` during replay. | `get_value("X")` returns `Some("new")` — the code's own call updates kv_state |

> Collapsed: Former RE-KV-12/13/13a/14 into RE-KV-10 — all test "what happens after replay completes" for trivially different event variants. Combined into one test with multiple assertions.

---

## ~~3. OrchestrationContext Unit Tests~~ — ELIMINATED

> **Rationale:** `OrchestrationContext` is tightly coupled to the replay engine and cannot be
> unit-tested in isolation — any test setup requires a replay engine, making it an integration
> test indistinguishable from E2E. All behaviors are already covered:
>
> - **Basic set/get/clear** (CTX-KV-01..07) → E2E-KV-01..04b
> - **Action emission** (CTX-KV-04/06/06a) → internal implementation detail verified transitively by all E2E tests that set/clear values
> - **Typed variants** (CTX-KV-08/09/11/12) → E2E-KV-15 (`client_get_value_typed`)
> - **Typed wrong-type error** (CTX-KV-10) → migrated to E2E edge cases as E2E-KV-27a
> - **get_value_from_instance** (CTX-KV-13..20) → E2E-KV-CI-01..05, CI-07

---

## 4. End-to-End Scenario Tests (40 tests)

> Location: `tests/scenarios/kv_store.rs`

### 4.1 Single Execution

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-01 | `kv_basic_set_get_single_turn` | Orchestration: set "progress" → schedule activity → get "progress" → complete | Value visible within same turn. Client `get_value` returns value after completion. |
| E2E-KV-02 | `kv_survives_multiple_turns` | Turn 1: set "state"="init", schedule activity. Turn 2 (after activity completes): get "state" | Returns "init" — survived replay |
| E2E-KV-03 | `kv_update_across_turns` | Turn 1: set "counter"="1". Turn 2: get "counter", set "counter"="2". Turn 3: verify "counter"="2" | Progressive updates work |
| E2E-KV-04 | `kv_clear_all_e2e` | Set several keys, clear, verify via client | Client `get_value` returns `None` for all |
| E2E-KV-04a | `kv_clear_single_key_e2e` | Set "A", "B". Clear "A" only. Verify via client. | "A" is `None`, "B" is returned |
| E2E-KV-04b | `kv_clear_value_then_reset` | Set "A", clear "A", set "A" to new value | Client sees new value |

### 4.2 ContinueAsNew

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-05 | `kv_survives_continue_as_new` | Exec 1: set "total"="100", CAN. Exec 2: get "total" | Returns "100" — KV persists across CAN |
| E2E-KV-06 | `kv_overwrite_after_can` | Exec 1: set "A"="old", CAN. Exec 2: set "A"="new" | Exec 2 overwrites. Client sees "new". execution_id updated. |
| E2E-KV-07 | `kv_clear_after_can` | Exec 1: set "A", "B", CAN. Exec 2: clear_all_values | All keys gone. Client confirms. |
| E2E-KV-08 | `kv_can_chain_accumulation` | Exec 1: set "count"="1", CAN. Exec 2: get "count", set "count"="2", CAN. Exec 3: get "count" | Returns "2" — values accumulate across CAN chain |

### 4.3 Pruning Integration

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-09 | `kv_prune_old_execution` | Exec 1: set "temp"="x", set "shared"="1". Exec 2 (CAN): set "shared"="2" (overwrites), set "total"="y" (new key). Prune exec 1. | "temp" removed (orphan, last-writer exec 1). "shared" preserved with value "2" (last-writer exec 2). "total" preserved (last-writer exec 2). |
| E2E-KV-10 | `kv_delete_instance_removes_all_kv` | Set KV, delete instance (force=true), client reads | All KV returns `None` |
| E2E-KV-11 | `kv_bulk_prune` | Multiple instances with CAN chains and KV, bulk prune | Correct per-instance pruning |

> Collapsed: Former E2E-KV-09 + E2E-KV-10 → E2E-KV-09 (prune test now covers both orphan removal AND overwrite preservation in one scenario). Former E2E-KV-11 + E2E-KV-16 → E2E-KV-10 (both tested "delete instance removes KV" from different perspectives — combined).

### 4.4 Client API

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-12 | `client_get_value_running` | Start orchestration, set KV, wait for activity. Client reads KV while orch is running. | Returns current value |
| E2E-KV-13 | `client_get_value_completed` | Run orchestration to completion with KV. Read KV after completion. | Still accessible |
| E2E-KV-14 | `client_get_value_typed` | Set typed value from orchestration, read typed from client | Deserializes correctly |

### 4.5 Cross-Instance Reads (get_value_from_instance)

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-CI-01 | `get_value_from_other_instance` | Orch A sets "data"="hello". Orch B calls `get_value_from_instance(A, "data")`. | Returns `Ok(Some("hello"))` |
| E2E-KV-CI-02 | `get_value_from_instance_key_missing` | Orch B calls `get_value_from_instance(A, "nonexistent")` | Returns `Ok(None)` |
| E2E-KV-CI-03 | `get_value_from_instance_unknown_instance` | Orch B calls `get_value_from_instance("no-such-inst", "key")` | Returns `Ok(None)` (provider returns None for unknown instance) |
| E2E-KV-CI-04 | `get_value_from_instance_typed_e2e` | Orch A sets typed value. Orch B reads typed. | Deserializes correctly |
| E2E-KV-CI-05 | `get_value_from_instance_replay_safe` | Orch B reads from A, then gets replayed. | Recorded ActivityCompleted result used — not a live read on replay |
| E2E-KV-CI-06 | `get_value_from_instance_after_source_update` | Orch A sets "v"="1". Orch B reads (gets "1"). Orch A updates to "2". Orch B reads again (new system activity). | Second read gets "2" (fresh activity execution) |

> Removed: `get_value_from_instance_concurrent` — tests runtime concurrency, not KV-specific behavior. `get_value_from_instance_in_select2` — `get_value_from_instance` is a normal activity; select2 with activities is tested elsewhere. `get_value_from_instance_single_thread` — merged into E2E-KV-26.

### 4.6 Sub-Orchestrations

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-15 | `kv_parent_child_isolation` | Parent sets "role"="parent". Child calls `get_value("role")` (gets `None`), then sets "role"="child". | `client.get_value(parent_id, "role")` = "parent". `client.get_value(child_id, "role")` = "child". KV is instance-scoped. |
| E2E-KV-16 | `kv_delete_parent_cascades_child_kv` | Parent + child both set KV. Delete parent. | Both parent and child KV removed |

> Collapsed: Former E2E-KV-17 + E2E-KV-18 → E2E-KV-15 (both test parent-child isolation — combined into one test that verifies both directions).

### 4.7 Determinism & Replay Safety

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-17 | `kv_replay_determinism` | Orchestration: set "step"="1", activity, set "step"="2", activity. Kill and replay. | Same KV state after replay. No nondeterminism errors. |
| E2E-KV-18 | `kv_get_value_safe_during_replay` | Orchestration: set "A"="1", get "A", use result in activity input. Replay. | Deterministic — same activity scheduled with same input |
| E2E-KV-19 | `kv_no_side_effects_during_replay` | Orchestration: set value. Check provider table during replay vs after. | Provider table not updated during replay — only on ack |

### 4.8 Edge Cases

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-20 | `kv_set_same_key_multiple_times_one_turn` | `set_value("k","1"); set_value("k","2"); set_value("k","3");` in one turn | History has three KeyValueSet events. Final value is "3". On replay, same result. |
| E2E-KV-21 | `kv_interleaved_with_activities_and_timers` | set_value → schedule_activity → set_value → schedule_timer → clear_all_values → set_value → complete | All events in correct order. KV state correct at each point. |
| E2E-KV-22 | `kv_in_select2_winner_branch` | `select2(timer, activity)` — winning branch sets KV, losing branch doesn't | Only winner's KV persists. No nondeterminism on replay. |
| E2E-KV-23 | `kv_in_fan_out_fan_in` | Fan out 5 activities, each returns data. After join, set KV for each result. | All 5 KV entries present. |
| E2E-KV-24 | `kv_json_value` | `set_value("data", r#"{"nested": [1,2,3]}"#)` | Stored and retrieved as-is (raw JSON string) |
| E2E-KV-25 | `kv_empty_key` | `set_value("", "value")` | Works — empty string is a valid key |
| E2E-KV-25a | `kv_clear_value_then_set_same_key` | `set_value("A","1"); clear_value("A"); set_value("A","2");` in one turn | History has set, clear, set. Final value is "2". Replay produces same result. |
| E2E-KV-25b | `kv_get_value_from_instance_self` | Orch calls `get_value_from_instance(own_instance_id, "key")` | Works — reads from own materialized KV (may differ from in-memory if current turn has uncommitted writes) |
| E2E-KV-25c | `kv_get_value_typed_wrong_type` | Orchestration: `set_value_typed("k", &vec![1,2,3])`. Client: `get_value_typed::<HashMap<String,String>>("k")` | Returns `Err(...)` — deserialization fails gracefully |

> Migrated: CTX-KV-10 → E2E-KV-25c (only unique test from eliminated Section 3).

### 4.9 Single-Threaded Runtime

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-26 | `kv_single_thread_basic` | Basic KV operations + cross-instance read under `current_thread` tokio runtime with 1x1 concurrency | All operations work correctly |
| E2E-KV-27 | `kv_single_thread_can_with_kv` | CAN chain with KV under single-threaded runtime | KV persists across CAN |

> Collapsed: Former E2E-KV-CI-09 merged into E2E-KV-26 (both test single-thread KV behavior).

### 4.10 Request/Response Patterns

> Location: `tests/e2e_samples.rs`
> These tests demonstrate using KV + external events as a request/response mechanism.

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-RR-01 | `kv_client_request_response` | **Client → Orchestration.** Orchestration waits on `schedule_wait("request")`. Client sends an external event with JSON payload `{"op_id": "op-123", "command": "compute", "args": "..."}`. Orchestration receives event, processes it, writes response to KV: `set_value("response:op-123", result)`. Client polls `client.get_value(instance_id, "response:op-123")` with exponential backoff until it gets `Some(result)`. | Client eventually receives the response. KV key contains expected result. Polling returns `None` before the orchestration processes the event, then `Some(...)` after. |
| E2E-KV-RR-02 | `kv_client_request_response_multiple_with_delay` | **Multiple concurrent requests, one delayed.** Client sends 3 events with different `op_id`s. Orchestration processes each via `schedule_wait` loop — one request includes a `schedule_timer` delay before responding. Client polls all 3 keys with bounded backoff. | All 3 responses eventually appear. Delayed response shows `None` during timer, then `Some(...)`. |
| E2E-KV-RR-03 | `kv_orch_to_orch_request_response` | **Orchestration → Orchestration (typed).** Orch A sends external event to Orch B. Orch A polls via `get_value_from_instance_typed::<ResponseStruct>(B_id, "response:req-456")` in a loop with `schedule_timer` backoff. Orch B receives event, processes it, writes `set_value_typed("response:req-456", &ResponseStruct{...})`. | Orch A receives correctly deserialized response. Each poll is a separate system activity. On replay, recorded results are used. |

> Collapsed: Former RR-02 + RR-03 → RR-02 (multiple requests with delay is a superset of both scenarios). Former RR-04 + RR-05 → RR-03 (typed variant is trivial overlay on untyped — combined).

---

## 5. Serde / Versioning Tests (2 tests)

> Location: `tests/serde_kv.rs` or within existing serde tests

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| SER-KV-01 | `roundtrip_kv_events` | Serialize → deserialize for all KV event variants: `KeyValueSet { key, value }`, `KeyValuesCleared`, `KeyValueCleared { key }` | Identity roundtrip for each. JSON shapes: `{"KeyValueSet": {"key": "k", "value": "v"}}`, `"KeyValuesCleared"`, `{"KeyValueCleared": {"key": "k"}}`. |
| SER-KV-02 | `unknown_event_skipped_by_old_runtime` | Old runtime (without KV support) encounters `KeyValueSet` in history | Skipped gracefully (forward compatibility) |

> Collapsed: Former SER-KV-01/02/03/03a/04 → SER-KV-01. Individual serialize/deserialize tests are trivially covered by the roundtrip test, which asserts both directions and verifies JSON shapes.

---

## 6. Stress Tests (4 tests)

> Location: `src/provider_stress_test/kv.rs` or `tests/stress_kv.rs`

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| STRESS-KV-01 | `kv_high_key_count` | Single instance sets 10 keys (at limit) across 100 turns, overwriting each turn | All 10 keys correct after each turn. No limit violations since count stays at 10. |
| STRESS-KV-02 | `kv_concurrent_instances_with_kv` | 100 instances each setting 10 keys | Full isolation. No cross-contamination. All within limits. |
| STRESS-KV-03 | `kv_can_chain_with_prune` | 50-execution CAN chain, each sets 5 keys (different from prior). Prune keep_last=5. | Only keys from last 5 executions remain (up to 10 per instance if unique keys across execs). |
| STRESS-KV-04 | `kv_rapid_overwrite` | Single key written 1000 times across 100 turns | Final value correct. History has 1000 KeyValueSet events. Key count stays at 1. |

---

## Test Infrastructure Helpers

### Seed Helpers (in `tests/common/mod.rs`)

```rust
/// Seed a KV entry directly via provider for test setup.
pub async fn seed_kv_entry(
    provider: &impl Provider,
    instance_id: &str,
    execution_id: u64,
    key: &str,
    value: &str,
);

/// Build a KeyValueSet event for use in seed_history_turn.
pub fn make_kv_set_event(
    instance_id: &str,
    execution_id: u64,
    event_id: u64,
    key: &str,
    value: &str,
) -> Event;

/// Build a KeyValuesCleared event for use in seed_history_turn.
pub fn make_kv_clear_event(
    instance_id: &str,
    execution_id: u64,
    event_id: u64,
) -> Event;

/// Build a KeyValueCleared (single key) event for use in seed_history_turn.
pub fn make_kv_clear_key_event(
    instance_id: &str,
    execution_id: u64,
    event_id: u64,
    key: &str,
) -> Event;
```

### Test Orchestrations

```rust
// Reusable test orchestration that uses KV
fn kv_test_orchestration() -> impl Fn(OrchestrationContext, String) -> Pin<...> {
    |ctx, input| Box::pin(async move {
        ctx.set_value("step", "started");
        let result = ctx.schedule_activity("DoWork", input).await?;
        ctx.set_value("step", "completed");
        ctx.set_value("result", &result);
        Ok(result)
    })
}
```

---

## Coverage Targets

| Component | Target | Notes |
|-----------|--------|-------|
| OrchestrationContext KV methods | 100% | All branches: set, get, clear_value, clear_all_values, typed variants, None cases |
| OrchestrationContext cross-instance | 100% | get_value_from_instance, typed variant, None/Some/Err paths |
| Replay engine KV processing | 100% | Replay match, nondeterminism, snapshot seeding, code-driven state updates |
| Provider KV methods | 100% | All CRUD paths, error cases, snapshot loading |
| Pruning KV integration | 100% | Execution prune, instance delete, bulk |
| Client KV methods | 100% | get_value, get_value_typed, None/Some |
| System activity (get_kv_value) | 100% | Registration, input parsing, provider call, serialization |
| Limit enforcement | 100% | Key count, value size, snapshot+delta calculation, error messages, metrics, warn/error logs |
| Migration | Tested via provider init | Table creation idempotent |

---

## Collapse Summary

| Section | Before | After | Reduction | Key Changes |
|---------|--------|-------|-----------|-------------|
| 1. Provider Validation | 28 | 22 | -6 | Removed `multiple_keys`, collapsed exec ID tests (3→1), removed `prune_partial` |
| 2. Replay Engine | 21 | 15 | -6 | Action conversion (4→1), new events after replay (4→1) |
| 3. OrchestrationContext | 23 | 0 | -23 | Eliminated — all covered by E2E; one test migrated to E2E-KV-25c |
| 4. E2E Scenarios | 48 | 40 | -8 | Merged prune tests, merged sub-orch isolation, removed non-KV-specific CI tests, merged RR variants |
| 5. Serde / Versioning | 6 | 2 | -4 | Individual serde tests subsumed by roundtrip |
| 6. Stress | 4 | 4 | 0 | Unchanged |
| **Total** | **130** | **83** | **-47 (36%)** | |

# Orchestration KV Store — Test Plan

Cross-reference: [Design Document](orchestration-kv-store.md)

---

## 1. Provider Validation Tests

> Location: `src/provider_validation/kv.rs` + registration in `src/provider_validations.rs`
> Applied to: SQLite provider (and any future providers)

### 1.1 Basic CRUD

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| PV-KV-01 | `kv_set_and_get` | Set a key via `KeyValueSet` event in ack, then `get_kv_value` | Returns `Some(value)` |
| PV-KV-02 | `kv_get_nonexistent` | `get_kv_value` for a key that was never set | Returns `None` |
| PV-KV-03 | `kv_get_unknown_instance` | `get_kv_value` for an instance that doesn't exist | Returns `None` (not error) |
| PV-KV-04 | `kv_overwrite` | Set key "A" to "1", then set "A" to "2" in a subsequent ack | `get_kv_value("A")` returns `"2"` |
| PV-KV-05 | `kv_multiple_keys` | Set keys "A", "B", "C" in one ack | All three retrievable independently |
| PV-KV-06 | `kv_clear_all` | Set keys "A", "B", then ack with `KeyValuesCleared` event | Both keys return `None` |
| PV-KV-06a | `kv_clear_single_key` | Set keys "A", "B", then ack with `KeyValueCleared { key: "A" }` event | "A" returns `None`, "B" still returns its value |
| PV-KV-06b | `kv_clear_nonexistent_key` | Ack with `KeyValueCleared { key: "missing" }` (key never set) | No error — operation is idempotent |
| PV-KV-07 | `kv_set_after_clear` | Clear all, then set "X" in same ack (after the clear event) | "X" exists, old keys don't |
| PV-KV-08 | `kv_empty_value` | Set key "A" to `""` (empty string) | Returns `Some("")` — not `None` |
| PV-KV-09 | `kv_large_value` | Set a key with a 64KB value | Stored and retrieved correctly |
| PV-KV-10 | `kv_special_chars_in_key` | Keys with spaces, unicode, dots, slashes | All stored/retrieved correctly |

### 1.2 Execution ID Tracking

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| PV-KV-11 | `kv_execution_id_stamped` | Set key "A" in exec 1, verify execution_id is 1 | Provider stores execution_id=1 |
| PV-KV-12 | `kv_execution_id_updated_on_overwrite` | Set "A" in exec 1, overwrite "A" in exec 2 | execution_id updated to 2 |
| PV-KV-13 | `kv_mixed_execution_ids` | Set "A" in exec 1, "B" in exec 2 | "A" has exec_id=1, "B" has exec_id=2 |

### 1.3 Snapshot

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| PV-KV-14 | `kv_snapshot_empty` | `get_kv_snapshot` for instance with no KV | Returns empty HashMap |
| PV-KV-15 | `kv_snapshot_complete` | Set "A", "B", "C" across multiple acks | Snapshot contains all three |
| PV-KV-16 | `kv_snapshot_after_clear` | Set keys, clear, set new keys | Snapshot only has keys set after clear |
| PV-KV-16a | `kv_snapshot_after_clear_single` | Set "A", "B", clear "A" only | Snapshot has "B" but not "A" |
| PV-KV-17 | `kv_snapshot_cross_execution` | Set "A" in exec 1, "B" in exec 2, no overwrites | Snapshot contains both |

### 1.4 Pruning Integration

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| PV-KV-18 | `kv_prune_execution_removes_keys` | Set "A" in exec 1 (only), prune exec 1 | "A" returns `None` |
| PV-KV-19 | `kv_prune_execution_preserves_overwritten` | Set "A" in exec 1, overwrite "A" in exec 2, prune exec 1 | "A" still returns exec 2's value |
| PV-KV-20 | `kv_prune_execution_partial` | "A" from exec 1, "B" from exec 2, prune exec 1 | "A" gone, "B" preserved |
| PV-KV-21 | `kv_prune_current_execution_protected` | Attempt to prune current execution | Current execution is never pruned (standard prune rules) — KV survives |
| PV-KV-22 | `kv_delete_instance_cascades` | Set keys, delete entire instance | All KV entries removed |
| PV-KV-23 | `kv_delete_instance_with_children` | Parent + child orchestrations with KV, delete parent tree | Both parent and child KV removed |

### 1.5 Isolation

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| PV-KV-24 | `kv_instance_isolation` | Two instances set same key "A" to different values | `get_kv_value(inst1, "A")` ≠ `get_kv_value(inst2, "A")` |
| PV-KV-25 | `kv_clear_isolation` | Clear KV on inst1 | inst2's KV is unaffected |

---

## 2. Replay Engine Unit Tests

> Location: `src/runtime/replay_engine.rs` (or `tests/replay_kv.rs`)

### 2.1 Action/Event Conversion

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| RE-KV-01 | `action_to_event_set_key_value` | `Action::SetKeyValue { key, value }` → `EventKind::KeyValueSet { key, value }` | Correct event kind, no source_event_id |
| RE-KV-02 | `action_to_event_clear_key_values` | `Action::ClearKeyValues` → `EventKind::KeyValuesCleared` | Correct event kind |
| RE-KV-02a | `action_to_event_clear_key_value` | `Action::ClearKeyValue { key }` → `EventKind::KeyValueCleared { key }` | Correct event kind, key preserved |
| RE-KV-03 | `set_key_value_no_scheduling_event_id` | SetKeyValue action produces event with event_id but no scheduling_event_id (it's not a schedule) | `source_event_id` is `None` |

### 2.2 Replay Determinism

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| RE-KV-04 | `replay_set_value_reconstructs_state` | Snapshot has no keys. Orch code calls `set_value("A","1")`. History has `KeyValueSet{A=1}`. | `ctx.get_value("A")` returns `Some("1")` during replay (from code's own call, not history scanning) |
| RE-KV-05 | `replay_multiple_sets_last_wins` | History: [KeyValueSet{A=1}, KeyValueSet{A=2}]. | `ctx.get_value("A")` returns `Some("2")` |
| RE-KV-06 | `replay_clear_then_set` | History: [KeyValueSet{A=1}, KeyValuesCleared, KeyValueSet{B=2}]. | "A" is `None`, "B" is `Some("2")` |
| RE-KV-06a | `replay_clear_single_key` | History: [KeyValueSet{A=1}, KeyValueSet{B=2}, KeyValueCleared{A}]. | "A" is `None`, "B" is `Some("2")` |
| RE-KV-07 | `replay_set_value_matches_action` | Orchestration calls `set_value("A", "1")`. History has `KeyValueSet{A=1}`. | No nondeterminism error. Action matched to event. |
| RE-KV-08 | `replay_set_value_mismatch_nondeterminism` | Orchestration calls `set_value("A", "1")`. History has `KeyValueSet{A=2}`. | Nondeterminism error (action doesn't match event) |
| RE-KV-09 | `replay_clear_all_values_matches_action` | Orchestration calls `clear_all_values()`. History has `KeyValuesCleared`. | No nondeterminism error |
| RE-KV-09a | `replay_clear_value_matches_action` | Orchestration calls `clear_value("A")`. History has `KeyValueCleared{A}`. | No nondeterminism error |
| RE-KV-09b | `replay_clear_value_mismatch_nondeterminism` | Orchestration calls `clear_value("A")`. History has `KeyValueCleared{B}`. | Nondeterminism error (key mismatch) |
| RE-KV-10 | `replay_kv_interleaved_with_activities` | History: [ActivityScheduled, KeyValueSet, ActivityCompleted]. | KV state correct. Activity result correct. Event ordering preserved. |
| RE-KV-11 | `replay_kv_no_token_binding` | Set value during replay | No token is bound (unlike schedule actions). No future created. |

### 2.3 New Events After Replay

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| RE-KV-12 | `new_set_value_emits_event` | After replay completes, call `set_value("X", "Y")` in new logic | `history_delta` contains `KeyValueSet{X=Y}` event |
| RE-KV-13 | `new_clear_all_values_emits_event` | After replay, call `clear_all_values()` | `history_delta` contains `KeyValuesCleared` event |
| RE-KV-13a | `new_clear_value_emits_event` | After replay, call `clear_value("X")` | `history_delta` contains `KeyValueCleared{X}` event |
| RE-KV-14 | `new_set_value_updates_in_memory` | After replay, `set_value("A", "1")` then `get_value("A")` | Returns `Some("1")` immediately (not just after ack) |
| RE-KV-15 | `kv_state_seeded_from_snapshot` | Snapshot has {"X": "from_prev_exec"}. Current exec history has no KV events. Code calls `get_value("X")`. | Returns `Some("from_prev_exec")` — snapshot is the initial state, no history reconstruction needed |
| RE-KV-16 | `kv_state_snapshot_overridden_by_code` | Snapshot has {"X": "old"}. Code calls `set_value("X", "new")` during replay. | `get_value("X")` returns `Some("new")` — the code's own call updates kv_state |

---

## 3. OrchestrationContext Unit Tests

> Location: tests within `src/lib.rs` or a dedicated `tests/kv_context.rs`

### 3.1 Basic Set/Get

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| CTX-KV-01 | `set_value_get_value` | `set_value("k", "v")` then `get_value("k")` | Returns `Some("v")` |
| CTX-KV-02 | `get_value_nonexistent` | `get_value("missing")` without any set | Returns `None` |
| CTX-KV-03 | `set_value_overwrite` | `set_value("k", "1")` then `set_value("k", "2")` | `get_value("k")` returns `Some("2")` |
| CTX-KV-04 | `set_value_emits_action` | `set_value("k", "v")` | Action list contains `SetKeyValue { key: "k", value: "v" }` |
| CTX-KV-05 | `clear_all_values_empties_all` | Set three keys, call `clear_all_values()` | All three return `None` |
| CTX-KV-05a | `clear_value_single_key` | Set "A", "B", call `clear_value("A")` | "A" returns `None`, "B" still returns value |
| CTX-KV-05b | `clear_value_nonexistent_key` | `clear_value("missing")` with no keys set | Emits `ClearKeyValue` action, no panic. `get_value("missing")` returns `None`. |
| CTX-KV-06 | `clear_all_values_emits_action` | `clear_all_values()` | Action list contains `ClearKeyValues` |
| CTX-KV-06a | `clear_value_emits_action` | `clear_value("k")` | Action list contains `ClearKeyValue { key: "k" }` |
| CTX-KV-07 | `set_after_clear` | Set "A", clear, set "B" | "A" is `None`, "B" is `Some(...)` |

### 3.2 Typed Variants

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| CTX-KV-08 | `set_value_typed_struct` | `set_value_typed("config", &MyStruct{...})` | Serializes to JSON. `get_value("config")` returns JSON string. |
| CTX-KV-09 | `get_value_typed_struct` | Set typed, then `get_value_typed::<MyStruct>("config")` | Returns deserialized struct |
| CTX-KV-10 | `get_value_typed_wrong_type` | Set as `Vec<i32>`, get as `HashMap<String, String>` | Returns `Err(...)` |
| CTX-KV-11 | `get_value_typed_nonexistent` | `get_value_typed::<i32>("missing")` | Returns `Ok(None)` |
| CTX-KV-12 | `set_value_typed_primitives` | `set_value_typed("count", &42i64)` | `get_value_typed::<i64>("count")` returns `Ok(Some(42))` |

### 3.3 get_value_from_instance (System Activity)

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| CTX-KV-13 | `get_value_from_instance_schedules_syscall` | `get_value_from_instance("other", "key")` | Emits `CallActivity` action with name `__duroxide_syscall:get_kv_value` and JSON input `{"instance_id": "other", "key": "key"}` |
| CTX-KV-14 | `get_value_from_instance_returns_value` | System activity completes with `Some("val")` serialized | Future resolves to `Ok(Some("val"))` |
| CTX-KV-15 | `get_value_from_instance_returns_none` | System activity completes with `null` serialized (key not found) | Future resolves to `Ok(None)` |
| CTX-KV-16 | `get_value_from_instance_typed_deserializes` | System activity returns `Some("{\"x\": 1}")`, typed get as struct | Deserializes correctly |
| CTX-KV-17 | `get_value_from_instance_typed_none` | System activity returns `None` | `Ok(None)` — no deserialization attempted |
| CTX-KV-18 | `get_value_from_instance_typed_bad_json` | System activity returns `Some("not json")`, typed get as struct | Returns `Err(...)` |
| CTX-KV-19 | `get_value_from_instance_provider_error` | Provider returns error from `get_kv_value` | System activity fails, future resolves to `Err(...)` |
| CTX-KV-20 | `get_value_from_instance_replay_determinism` | Call, replay — second execution uses recorded ActivityCompleted result | No provider call on replay. Same result. |

---

## 4. End-to-End Scenario Tests

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
| E2E-KV-09 | `kv_prune_old_execution_removes_orphan_keys` | Exec 1: set "temp"="x". Exec 2 (CAN): set "total"="y" (doesn't touch "temp"). Prune exec 1. | "temp" removed (last-writer exec 1). "total" preserved (last-writer exec 2). |
| E2E-KV-10 | `kv_prune_preserves_overwritten_keys` | Exec 1: set "A"="1". Exec 2: set "A"="2". Prune exec 1. | "A" preserved with value "2" (last-writer is exec 2) |
| E2E-KV-11 | `kv_delete_instance_removes_all_kv` | Set KV, delete instance (force=true) | All KV gone |
| E2E-KV-12 | `kv_bulk_prune` | Multiple instances with CAN chains and KV, bulk prune | Correct per-instance pruning |

### 4.4 Client API

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-13 | `client_get_value_running` | Start orchestration, set KV, wait for activity. Client reads KV while orch is running. | Returns current value |
| E2E-KV-14 | `client_get_value_completed` | Run orchestration to completion with KV. Read KV after completion. | Still accessible |
| E2E-KV-15 | `client_get_value_typed` | Set typed value from orchestration, read typed from client | Deserializes correctly |
| E2E-KV-16 | `client_get_value_after_delete` | Set KV, delete instance, try to read | Returns `None` |

### 4.5 Cross-Instance Reads (get_value_from_instance)

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-CI-01 | `get_value_from_other_instance` | Orch A sets "data"="hello". Orch B calls `get_value_from_instance(A, "data")`. | Returns `Ok(Some("hello"))` |
| E2E-KV-CI-02 | `get_value_from_instance_key_missing` | Orch B calls `get_value_from_instance(A, "nonexistent")` | Returns `Ok(None)` |
| E2E-KV-CI-03 | `get_value_from_instance_unknown_instance` | Orch B calls `get_value_from_instance("no-such-inst", "key")` | Returns `Ok(None)` (provider returns None for unknown instance) |
| E2E-KV-CI-04 | `get_value_from_instance_typed_e2e` | Orch A sets typed value. Orch B reads typed. | Deserializes correctly |
| E2E-KV-CI-05 | `get_value_from_instance_replay_safe` | Orch B reads from A, then gets replayed. | Recorded ActivityCompleted result used — not a live read on replay |
| E2E-KV-CI-06 | `get_value_from_instance_concurrent` | Orch A sets KV. Orch B and C both read from A concurrently. | Both get correct values. No contention. |
| E2E-KV-CI-07 | `get_value_from_instance_after_source_update` | Orch A sets "v"="1". Orch B reads (gets "1"). Orch A updates to "2". Orch B reads again (new system activity). | Second read gets "2" (fresh activity execution) |
| E2E-KV-CI-08 | `get_value_from_instance_in_select2` | `select2(timer, get_value_from_instance(A, "k"))` | Works with select2 — it's a normal activity future |
| E2E-KV-CI-09 | `get_value_from_instance_single_thread` | Cross-instance read under `current_thread` runtime with 1x1 concurrency | Works correctly |

### 4.6 Sub-Orchestrations

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-17 | `kv_parent_child_isolation` | Parent sets "role"="parent". Child sets "role"="child". | `client.get_value(parent_id, "role")` = "parent". `client.get_value(child_id, "role")` = "child". |
| E2E-KV-18 | `kv_child_cannot_see_parent_kv` | Parent sets "secret"="42". Child calls `get_value("secret")`. | Returns `None` — KV is instance-scoped |
| E2E-KV-19 | `kv_delete_parent_cascades_child_kv` | Parent + child both set KV. Delete parent. | Both parent and child KV removed |

### 4.7 Determinism & Replay Safety

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-20 | `kv_replay_determinism` | Orchestration: set "step"="1", activity, set "step"="2", activity. Kill and replay. | Same KV state after replay. No nondeterminism errors. |
| E2E-KV-21 | `kv_get_value_safe_during_replay` | Orchestration: set "A"="1", get "A", use result in activity input. Replay. | Deterministic — same activity scheduled with same input |
| E2E-KV-22 | `kv_no_side_effects_during_replay` | Orchestration: set value. Check provider table during replay vs after. | Provider table not updated during replay — only on ack |

### 4.9 Edge Cases

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-23 | `kv_set_same_key_multiple_times_one_turn` | `set_value("k","1"); set_value("k","2"); set_value("k","3");` in one turn | History has three KeyValueSet events. Final value is "3". On replay, same result. |
| E2E-KV-24 | `kv_interleaved_with_activities_and_timers` | set_value → schedule_activity → set_value → schedule_timer → clear_all_values → set_value → complete | All events in correct order. KV state correct at each point. |
| E2E-KV-25 | `kv_in_select2_winner_branch` | `select2(timer, activity)` — winning branch sets KV, losing branch doesn't | Only winner's KV persists. No nondeterminism on replay. |
| E2E-KV-26 | `kv_in_fan_out_fan_in` | Fan out 5 activities, each returns data. After join, set KV for each result. | All 5 KV entries present. |
| E2E-KV-27 | `kv_json_value` | `set_value("data", r#"{"nested": [1,2,3]}"#)` | Stored and retrieved as-is (raw JSON string) |
| E2E-KV-28 | `kv_empty_key` | `set_value("", "value")` | Works — empty string is a valid key |
| E2E-KV-28a | `kv_clear_value_then_set_same_key` | `set_value("A","1"); clear_value("A"); set_value("A","2");` in one turn | History has set, clear, set. Final value is "2". Replay produces same result. |
| E2E-KV-28b | `kv_get_value_from_instance_self` | Orch calls `get_value_from_instance(own_instance_id, "key")` | Works — reads from own materialized KV (may differ from in-memory if current turn has uncommitted writes) |

### 4.10 Single-Threaded Runtime

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-29 | `kv_single_thread_basic` | Basic KV operations under `current_thread` tokio runtime with 1x1 concurrency | All operations work correctly |
| E2E-KV-30 | `kv_single_thread_can_with_kv` | CAN chain with KV under single-threaded runtime | KV persists across CAN |

### 4.11 Request/Response Patterns

> Location: `tests/e2e_samples.rs`
> These tests demonstrate using KV + external events as a request/response mechanism.

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| E2E-KV-RR-01 | `kv_client_request_response` | **Client → Orchestration.** Orchestration waits on `schedule_wait("request")`. Client sends an external event with JSON payload `{"op_id": "op-123", "command": "compute", "args": "..."}`. Orchestration receives event, processes it, writes response to KV: `set_value("response:op-123", result)`. Client polls `client.get_value(instance_id, "response:op-123")` with exponential backoff until it gets `Some(result)`. | Client eventually receives the response. KV key contains expected result. Polling returns `None` before the orchestration processes the event, then `Some(...)` after. |
| E2E-KV-RR-02 | `kv_client_request_response_multiple` | **Multiple concurrent requests.** Client sends 3 events with different `op_id`s in quick succession. Orchestration processes each (via a loop with `schedule_wait`) and writes `response:{op_id}` for each. Client polls all 3 keys. | All 3 responses eventually appear. Each key has the correct response for its op_id. Order of response availability may vary. |
| E2E-KV-RR-03 | `kv_client_request_response_timeout` | **Client polls with bounded retries.** Client sends event, orchestration takes a while (schedules a timer before responding). Client polls with backoff up to a reasonable limit. | Response appears after the timer fires. Client sees `None` during the delay, then `Some(...)`. |
| E2E-KV-RR-04 | `kv_orch_to_orch_request_response` | **Orchestration → Orchestration.** Orch A wants a result from Orch B. Orch A sends an external event to Orch B with `{"op_id": "req-456", "query": "..."}`. Orch A then polls via `get_value_from_instance(B_id, "response:req-456")` in a loop with `schedule_timer` backoff. Orch B receives the event via `schedule_wait("request")`, processes it, and writes `set_value("response:req-456", answer)`. Orch A's poll eventually returns `Some(answer)`. | Orch A receives the correct answer. Each `get_value_from_instance` call is a separate system activity (recorded in history). On replay, the recorded results are used — no live polling. |
| E2E-KV-RR-05 | `kv_orch_to_orch_request_response_typed` | Same as RR-04 but using typed variants. Orch B writes `set_value_typed("response:req-789", &ResponseStruct{...})`. Orch A reads via `get_value_from_instance_typed::<ResponseStruct>(B_id, "response:req-789")`. | Typed deserialization works end-to-end across instances. |

---

## 5. Serde / Versioning Tests

> Location: `tests/serde_kv.rs` or within existing serde tests

| # | Test | Description | Assertions |
|---|------|-------------|------------|
| SER-KV-01 | `serialize_key_value_set_event` | Serialize `KeyValueSet { key: "k", value: "v" }` to JSON | Produces `{"KeyValueSet": {"key": "k", "value": "v"}}` |
| SER-KV-02 | `deserialize_key_value_set_event` | Deserialize from JSON | Produces correct EventKind |
| SER-KV-03 | `serialize_key_values_cleared_event` | Serialize `KeyValuesCleared` | Produces `"KeyValuesCleared"` |
| SER-KV-03a | `serialize_key_value_cleared_event` | Serialize `KeyValueCleared { key: "k" }` to JSON | Produces `{"KeyValueCleared": {"key": "k"}}` |
| SER-KV-04 | `roundtrip_kv_events` | Serialize → deserialize for all KV event variants | Identity roundtrip |
| SER-KV-05 | `unknown_event_skipped_by_old_runtime` | Old runtime (without KV support) encounters `KeyValueSet` in history | Skipped gracefully (forward compatibility) |

---

## 6. Stress Tests

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

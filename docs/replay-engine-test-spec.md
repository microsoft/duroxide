# Replay Engine Test Specification

This document specifies the unit tests for `ReplayEngine` — the core component that processes history events, matches them against orchestration actions, delivers completions, and emits new actions.

## Test Philosophy

The replay engine is tested in isolation by:
1. Constructing a **baseline history** (events from the database)
2. Providing **completion messages** (WorkItems arriving this turn)
3. Supplying a **mock orchestration handler** that emits specific actions
4. Verifying the **outputs**: `TurnResult`, `history_delta`, `pending_actions`

This approach tests the engine's deterministic replay logic without needing a full runtime or provider.

---

## Test Sections

### 1. Fresh Execution (No Prior Schedule History)

Tests where the orchestration starts from `OrchestrationStarted` with no schedule events in history.

| Test | Baseline | Handler Behavior | Expected Result |
|------|----------|------------------|-----------------|
| `immediate_return_ok` | `[Started]` | Returns `Ok("success")` | `Completed("success")`, no actions |
| `immediate_return_err` | `[Started]` | Returns `Err("failure")` | `Failed(Application)`, no actions |
| `schedule_activity_pending` | `[Started]` | Schedules "Greet"/"Alice", awaits | `Continue`, `pending_actions=[CallActivity]`, `history_delta=[ActivityScheduled]` |
| `schedule_timer_pending` | `[Started]` | Schedules timer(60s), awaits | `Continue`, `pending_actions=[CreateTimer]`, `history_delta=[TimerCreated]` |
| `schedule_external_pending` | `[Started]` | Waits for "Approval" event | `Continue`, `pending_actions=[WaitExternal]`, `history_delta=[ExternalSubscribed]` |
| `schedule_sub_orch_pending` | `[Started]` | Schedules sub-orchestration | `Continue`, `pending_actions=[StartSubOrchestration]` |
| `continue_as_new` | `[Started]` | Calls `continue_as_new("new")` | `ContinueAsNew{input:"new"}` |
| `multiple_schedules_no_await` | `[Started]` | Schedules A, B, C but returns immediately | `Completed`, 3 pending actions, 3 history delta events |

---

### 2. Replay with Completions in History

Tests where the baseline history already contains schedule AND completion events.

| Test | Baseline | Handler Behavior | Expected Result |
|------|----------|------------------|-----------------|
| `activity_completed_in_history` | `[Started, ActivityScheduled, ActivityCompleted]` | Schedules same activity | `Completed`, no new actions |
| `activity_failed_in_history` | `[Started, ActivityScheduled, ActivityFailed]` | Schedules same activity | `Failed(Application)` (handler propagates error) |
| `timer_fired_in_history` | `[Started, TimerCreated, TimerFired]` | Schedules same timer | `Completed` |
| `external_received_in_history` | `[Started, ExternalSubscribed, ExternalEvent]` | Waits for same event | `Completed` |
| `sub_orch_completed_in_history` | `[Started, SubOrchScheduled, SubOrchCompleted]` | Schedules same sub-orch | `Completed` |
| `system_call_in_history` | `[Started, SystemCall{op,value}]` | Calls same system op | `Completed` with replayed value |

---

### 3. Replay with Partial Completion (Schedule in History, No Completion)

Tests where history has schedule events but completions haven't arrived yet.

| Test | Baseline | Handler Behavior | Expected Result |
|------|----------|------------------|-----------------|
| `activity_scheduled_no_completion` | `[Started, ActivityScheduled]` | Schedules same activity | `Continue`, no new actions |
| `timer_created_no_fire` | `[Started, TimerCreated]` | Schedules same timer | `Continue`, no new actions |
| `external_subscribed_no_event` | `[Started, ExternalSubscribed]` | Waits for same event | `Continue`, no new actions |
| `sub_orch_scheduled_no_completion` | `[Started, SubOrchScheduled]` | Schedules same sub-orch | `Continue`, no new actions |

---

### 4. Sequential Progress

Tests verifying multi-step orchestration replay.

| Test | Baseline | Handler Behavior | Expected Result |
|------|----------|------------------|-----------------|
| `two_activities_first_done` | `[Started, A1-Sched, A1-Comp]` | Schedules A1, then A2 | `Continue`, A2 scheduled as new action |
| `two_activities_both_done` | `[Started, A1-Sched, A1-Comp, A2-Sched, A2-Comp]` | Schedules A1, then A2 | `Completed` |
| `activity_then_timer` | `[Started, Activity-Sched, Activity-Comp]` | Activity then timer | `Continue`, timer scheduled |
| `timer_then_activity` | `[Started, Timer-Created, Timer-Fired]` | Timer then activity | `Continue`, activity scheduled |
| `many_sequential_activities` | 10 schedule+complete pairs | Schedules 10 activities | `Completed` with all results |

---

### 5. Completion Message Processing (`prep_completions`)

Tests for WorkItem → Event conversion and filtering.

| Test | Baseline | Messages | Expected |
|------|----------|----------|----------|
| `activity_completed_message` | `[Started, ActivityScheduled(id=2)]` | `ActivityCompleted(id=2)` | Completion in `history_delta` |
| `timer_fired_message` | `[Started, TimerCreated(id=2)]` | `TimerFired(id=2)` | Completion in `history_delta` |
| `external_raised_message` | `[Started, ExternalSubscribed("E")]` | `ExternalRaised("E", data)` | Event in `history_delta` |
| `sub_orch_completed_message` | `[Started, SubOrchScheduled(id=2)]` | `SubOrchCompleted(parent=2)` | Completion in `history_delta` |
| `duplicate_completion_filtered` | `[Started, Scheduled, Completed]` | Same completion again | `history_delta` empty (filtered) |
| `duplicate_in_same_batch` | `[Started, Scheduled]` | Two identical completions | Only first added |
| `wrong_execution_filtered` | `[Started, Scheduled]` | Completion with wrong `execution_id` | Filtered, no delta |
| `external_without_subscription` | `[Started]` | `ExternalRaised("X")` | Filtered (no subscription) |
| `multiple_completions_batch` | `[Started, A-Sched, B-Sched]` | Completions for A and B | Both in delta |

---

### 6. Cancellation

Tests for `OrchestrationCancelRequested` handling.

| Test | Baseline | Messages | Expected |
|------|----------|----------|----------|
| `cancel_via_message` | `[Started]` | `CancelInstance` | `Cancelled(reason)` |
| `cancel_in_history` | `[Started, CancelRequested]` | none | `Cancelled(reason)` |
| `cancel_precedence_over_completion` | `[Started]` | `CancelInstance` | `Cancelled` (even if handler returns Ok) |
| `cancel_with_pending_activity` | `[Started, ActivityScheduled]` | `CancelInstance` | `Cancelled` |
| `cancel_duplicate_filtered` | `[Started, CancelRequested]` | Another `CancelInstance` | Only one cancel event |

---

### 7. Nondeterminism Detection

Tests verifying the engine detects replay mismatches.

| Test | Baseline | Handler Behavior | Expected |
|------|----------|------------------|----------|
| `schedule_mismatch_activity_vs_timer` | `[Started, ActivityScheduled]` | Schedules timer | `Failed(Nondeterminism)` |
| `schedule_mismatch_timer_vs_activity` | `[Started, TimerCreated]` | Schedules activity | `Failed(Nondeterminism)` |
| `schedule_mismatch_wrong_activity_name` | `[Started, ActivityScheduled("A")]` | Schedules activity "B" | `Failed(Nondeterminism)` |
| `schedule_mismatch_wrong_input` | `[Started, ActivityScheduled("A", "x")]` | Schedules "A" with "y" | `Failed(Nondeterminism)` |
| `schedule_mismatch_wrong_external_name` | `[Started, ExternalSubscribed("X")]` | Waits for "Y" | `Failed(Nondeterminism)` |
| `history_schedule_no_emitted_action` | `[Started, ActivityScheduled]` | Returns immediately | Engine processes as complete (extra history ignored) |
| `completion_without_schedule` | `[Started]` | — | Completion message for id=999 → `Failed(Nondeterminism)` in prep |
| `completion_kind_mismatch` | `[Started, ActivityScheduled(id=2)]` | — | TimerFired for id=2 → `Failed(Nondeterminism)` |

---

### 8. Corrupted History

Tests for invalid history detection and graceful handling.

#### 8.1 Structural Corruption

| Test | Baseline | Expected |
|------|----------|----------|
| `empty_history` | `[]` | `Failed(corrupted: empty)` |
| `missing_started_event` | `[ActivityScheduled]` | `Failed(first event must be OrchestrationStarted)` |
| `started_not_first` | `[ActivityScheduled, Started]` | `Failed(first event must be OrchestrationStarted)` |
| `duplicate_event_ids` | `[Started(1), Scheduled(2), Scheduled(2)]` | Engine should handle (last wins or error) |
| `non_monotonic_event_ids` | `[Started(1), Scheduled(5), Scheduled(3)]` | Engine allocates next_id from max |
| `event_id_zero` | `[Started(0)]` | Should work (0 is valid) |
| `event_id_gap` | `[Started(1), Scheduled(100)]` | Should work (gaps allowed) |

#### 8.2 Completion Linkage Corruption

| Test | Baseline | Expected |
|------|----------|----------|
| `completion_missing_source_id` | `[Started, Scheduled(2), Completed(source=None)]` | Completion ignored or error |
| `completion_source_not_found` | `[Started, Scheduled(2), Completed(source=99)]` | `Failed(Nondeterminism)` |
| `completion_source_wrong_type` | `[Started, Timer(2), ActivityCompleted(source=2)]` | `Failed(Nondeterminism)` |
| `completion_before_schedule` | `[Started, Completed(source=2), Scheduled(2)]` | `Failed(Nondeterminism)` or handled |
| `multiple_completions_same_source` | `[Started, Scheduled(2), Completed(src=2), Completed(src=2)]` | Second ignored or error |
| `completion_for_different_instance` | Completion with wrong instance_id | Filtered in prep_completions |

#### 8.3 Terminal Event Corruption

| Test | Baseline | Expected |
|------|----------|----------|
| `terminal_completed` | `[Started, Completed]` | `Continue` (no-op, already terminal) |
| `terminal_failed` | `[Started, Failed]` | `Continue` (no-op) |
| `terminal_continued_as_new` | `[Started, ContinuedAsNew]` | `Continue` (no-op) |
| `multiple_terminal_events` | `[Started, Completed, Failed]` | First terminal wins, `Continue` |
| `events_after_terminal` | `[Started, Completed, ActivityScheduled]` | `Continue` (terminal already reached) |
| `terminal_not_last` | `[Started, Completed, Timer]` | `Continue` (terminal takes precedence) |
| `cancel_after_completed` | `[Started, Completed, CancelRequested]` | `Continue` (already terminal) |

#### 8.4 Execution ID Corruption

| Test | Baseline | Expected |
|------|----------|----------|
| `wrong_execution_id_in_event` | `[Started(exec=1), Scheduled(exec=2)]` | May cause issues, depends on validation |
| `mixed_execution_ids` | Events from exec 1 and 2 interleaved | Engine should validate or handle |
| `completion_wrong_execution` | Completion message with wrong exec_id | Filtered in prep_completions |

#### 8.5 Field Value Corruption

| Test | Baseline | Expected |
|------|----------|----------|
| `empty_activity_name` | `[Started, ActivityScheduled(name="")]` | Should work (empty is valid) |
| `empty_orchestration_name` | `[Started(name="")]` | Should work |
| `null_equivalent_strings` | Various null-like values | Should handle gracefully |
| `very_long_strings` | 10MB activity name | Should work (no arbitrary limits) |
| `special_characters` | Unicode, newlines, null bytes | Should work |

#### 8.6 Event Order Corruption  

| Test | Baseline | Expected |
|------|----------|----------|
| `timer_fired_before_created` | `[Started, TimerFired(src=2), TimerCreated(2)]` | `Failed(Nondeterminism)` |
| `external_event_before_subscription` | `[Started, ExternalEvent, ExternalSubscribed]` | Event ignored or error |
| `sub_orch_completed_before_scheduled` | `[Started, SubOrchCompleted(src=2), SubOrchScheduled(2)]` | `Failed(Nondeterminism)` |

#### 8.7 Orphaned Events

| Test | Baseline | Expected |
|------|----------|----------|
| `orphan_completion_no_schedule` | `[Started, ActivityCompleted(src=99)]` | Ignored or `Failed` |
| `orphan_timer_fired` | `[Started, TimerFired(src=99)]` | Ignored or `Failed` |
| `orphan_external_event` | `[Started, ExternalEvent("X")]` | Ignored (no subscription) |
| `orphan_sub_orch_completed` | `[Started, SubOrchCompleted(src=99)]` | Ignored or `Failed` |

#### 8.8 Duplicate Schedule Events

| Test | Baseline | Handler | Expected |
|------|----------|---------|----------|
| `duplicate_activity_schedule` | `[Started, Scheduled("A"), Scheduled("A")]` | Schedules "A" once | First matched, second causes mismatch |
| `duplicate_timer_schedule` | `[Started, Timer(t1), Timer(t1)]` | Schedules one timer | Similar behavior |
| `duplicate_external_subscription` | `[Started, ExtSub("E"), ExtSub("E")]` | Waits for "E" once | First matched |

---

### 9. Select/Join Composition

Tests for aggregate future behavior.

| Test | Baseline | Handler Behavior | Expected |
|------|----------|------------------|----------|
| `select2_activity_wins` | `[Started, Activity-Sched, Timer-Created, Activity-Comp]` | `select2(activity, timer)` | `Completed` with activity result |
| `select2_timer_wins` | `[Started, Activity-Sched, Timer-Created, Timer-Fired]` | `select2(activity, timer)` | `Completed("timeout")` |
| `select2_both_pending` | `[Started, Activity-Sched, Timer-Created]` | `select2(activity, timer)` | `Continue` |
| `join_all_complete` | `[Started, A-Sched, B-Sched, A-Comp, B-Comp]` | `join([a, b])` | `Completed` with both results |
| `join_partial_complete` | `[Started, A-Sched, B-Sched, A-Comp]` | `join([a, b])` | `Continue` (waiting on B) |
| `join_fresh_schedule` | `[Started]` | `join([a, b, c])` | `Continue`, 3 pending actions |

---

### 10. Event ID Allocation

Tests verifying correct event ID assignment for new events.

| Test | Baseline | Handler | Verify |
|------|----------|---------|--------|
| `sequential_allocation` | `[Started(id=1)]` | Schedules 3 things | Delta has ids 2, 3, 4 |
| `allocation_after_replay` | `[Started(1), Scheduled(2), Completed(3)]` | Schedules new activity | New event gets id=4 |
| `allocation_with_gaps` | `[Started(1), Scheduled(5)]` | Schedules new | New event id = max + 1 = 6 |

---

### 11. `is_replaying` State

Tests verifying the context's replay state tracking.

| Test | Baseline | When Checked | Expected |
|------|----------|--------------|----------|
| `fresh_execution_not_replaying` | `[Started]` | At start | `is_replaying() == false` |
| `replay_is_replaying` | `[Started, Scheduled, Completed]` | At start | `is_replaying() == true` |
| `transitions_after_history` | `[Started, Scheduled]` | Before/after history | Starts true, becomes false |

---

### 12. Sub-orchestration Instance ID Generation

Tests for deterministic child instance ID.

| Test | Baseline | Handler | Expected |
|------|----------|---------|----------|
| `auto_instance_id` | `[Started]` | `schedule_sub_orchestration("Child", input)` | Instance = `sub::{event_id}` (execution 1) |
| `auto_instance_id_includes_execution_id_for_later_generations` | `[Started(execution=2)]` | `schedule_sub_orchestration("Child", input)` | Instance = `sub::2.{event_id}` |
| `explicit_instance_preserved` | `[Started]` | `schedule_sub_orchestration_with_instance("my-id", ...)` | Instance = "my-id" |
| `replay_uses_history_instance` | `[Started, SubOrchScheduled{instance:"sub::2"}]` | Same schedule | Binds to same instance |

---

### 13. Activity Failure Handling

Tests for different error categories.

| Test | Baseline | Messages | Expected |
|------|----------|----------|----------|
| `application_error_propagates` | `[Started, Scheduled]` | `ActivityFailed(Application)` | Handler sees error, may return `Failed(App)` |
| `infrastructure_error_aborts` | `[Started, Scheduled]` | `ActivityFailed(Infrastructure)` | `Failed(Infrastructure)` immediately |
| `configuration_error_aborts` | `[Started, Scheduled]` | `ActivityFailed(Configuration)` | `Failed(Configuration)` immediately |

---

### 14. Handler Panic Handling

Tests for orchestration panics.

| Test | Handler | Expected |
|------|---------|----------|
| `panic_string` | `panic!("oops")` | `Failed(Nondeterminism, message="oops")` |
| `panic_other` | `panic!(42)` | `Failed(Nondeterminism, message="orchestration panicked")` |

---

### 15. Edge Cases

| Test | Description | Expected |
|------|-------------|----------|
| `zero_delay_timer` | Timer with Duration::ZERO | Works normally |
| `empty_activity_input` | Activity with "" input | Works normally |
| `large_result_string` | Activity returns 1MB string | Works normally |
| `unicode_in_names` | Activity name with emoji | Works normally |
| `handler_invoked_once` | Count handler invocations | Exactly 1 per turn |

---

## Implementation Notes

### Mock Handler Types Needed

```rust
// Returns immediately
struct ImmediateOkHandler(String);
struct ImmediateErrHandler(String);

// Schedules single operation
struct SingleActivityHandler { name: String, input: String }
struct SingleTimerHandler { delay: Duration }
struct WaitExternalHandler { event_name: String }
struct SubOrchHandler { name: String, input: String }

// Schedules multiple operations
struct TwoSequentialActivitiesHandler;
struct Select2Handler;
struct JoinHandler { count: usize }

// Special behaviors
struct ContinueAsNewHandler { input: String }
struct SystemCallHandler { op: String }
struct PanicHandler { message: String }
struct CountingHandler { count: Arc<AtomicUsize> }
struct ReplayAwareHandler { states: Arc<Mutex<Vec<bool>>> }

// Nondeterminism triggers
struct WrongActionHandler; // Schedules different type than expected
```

### Helper Functions Needed

```rust
// Event constructors
fn started_event(id, name, input) -> Event
fn activity_scheduled(id, name, input) -> Event
fn activity_completed(id, source_id, result) -> Event
fn activity_failed(id, source_id, message) -> Event
fn timer_created(id, fire_at_ms) -> Event
fn timer_fired(id, source_id, fire_at_ms) -> Event
fn external_subscribed(id, name) -> Event
fn external_event(id, name, data) -> Event
fn sub_orch_scheduled(id, name, instance, input) -> Event
fn sub_orch_completed(id, source_id, result) -> Event
fn system_call(id, op, value) -> Event
fn cancel_requested(id, reason) -> Event
fn orchestration_completed(id, output) -> Event

// WorkItem constructors
fn wi_activity_completed(id, result) -> WorkItem
fn wi_activity_failed(id, message) -> WorkItem
fn wi_timer_fired(id, fire_at_ms) -> WorkItem
fn wi_external_raised(name, data) -> WorkItem
fn wi_sub_orch_completed(parent_id, result) -> WorkItem
fn wi_cancel(reason) -> WorkItem

// Test runner
fn run_engine(baseline, messages, handler) -> (TurnResult, ReplayEngine)
```

---

## Success Criteria

All tests should:
1. Run without a full runtime (unit test isolation)
2. Be deterministic (no timing dependencies)
3. Cover both happy path and error cases
4. Verify specific output values, not just variant matching
5. Be fast (< 100ms each)

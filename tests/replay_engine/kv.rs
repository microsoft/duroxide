//! KV Store Replay Engine Tests
//!
//! Tests verifying:
//! - Action→Event conversion for KV actions
//! - Replay determinism for KV events (match, mismatch, interleave)
//! - New KV events emitted after replay
//! - KV snapshot seeding from provider

use super::helpers::*;
use async_trait::async_trait;
use duroxide::{Event, EventKind, OrchestrationContext, OrchestrationHandler};
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// KV Handlers
// ============================================================================

/// Handler that sets a key-value pair.
struct SetKeyValueHandler {
    key: String,
    value: String,
}

impl SetKeyValueHandler {
    fn new(key: &str, value: &str) -> Arc<Self> {
        Arc::new(Self {
            key: key.to_string(),
            value: value.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for SetKeyValueHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.set_value(&self.key, &self.value);
        Ok("done".to_string())
    }
}

/// Handler that sets value, then reads it back and returns the read result.
struct SetThenGetHandler {
    key: String,
    value: String,
}

impl SetThenGetHandler {
    fn new(key: &str, value: &str) -> Arc<Self> {
        Arc::new(Self {
            key: key.to_string(),
            value: value.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for SetThenGetHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.set_value(&self.key, &self.value);
        let got = ctx.get_value(&self.key).unwrap_or_default();
        Ok(got)
    }
}

/// Handler that clears all values.
struct ClearAllValuesHandler;

#[async_trait]
impl OrchestrationHandler for ClearAllValuesHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.clear_all_values();
        Ok("cleared".to_string())
    }
}

/// Handler that clears a single key.
struct ClearValueHandler {
    key: String,
}

impl ClearValueHandler {
    fn new(key: &str) -> Arc<Self> {
        Arc::new(Self { key: key.to_string() })
    }
}

#[async_trait]
impl OrchestrationHandler for ClearValueHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.clear_value(&self.key);
        Ok("cleared".to_string())
    }
}

/// Handler that sets multiple keys, clears all, then sets new ones.
struct SetClearSetHandler;

#[async_trait]
impl OrchestrationHandler for SetClearSetHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.set_value("A", "1");
        ctx.clear_all_values();
        ctx.set_value("B", "2");
        let a = ctx.get_value("A").unwrap_or("none".to_string());
        let b = ctx.get_value("B").unwrap_or("none".to_string());
        Ok(format!("A={a},B={b}"))
    }
}

/// Handler that sets A then clears only A (single-key clear).
struct SetThenClearSingleHandler;

#[async_trait]
impl OrchestrationHandler for SetThenClearSingleHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.set_value("A", "1");
        ctx.set_value("B", "2");
        ctx.clear_value("A");
        let a = ctx.get_value("A").unwrap_or("none".to_string());
        let b = ctx.get_value("B").unwrap_or("none".to_string());
        Ok(format!("A={a},B={b}"))
    }
}

/// Handler that sets a value then schedules an activity.
struct SetKvThenActivityHandler {
    key: String,
    value: String,
    activity_name: String,
    activity_input: String,
}

impl SetKvThenActivityHandler {
    fn new(key: &str, value: &str, activity: &str, input: &str) -> Arc<Self> {
        Arc::new(Self {
            key: key.to_string(),
            value: value.to_string(),
            activity_name: activity.to_string(),
            activity_input: input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for SetKvThenActivityHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.set_value(&self.key, &self.value);
        let result = ctx.schedule_activity(&self.activity_name, &self.activity_input).await?;
        Ok(result)
    }
}

/// Handler that does all three KV operations (set, clear_value, clear_all) then returns.
struct AllKvOpsHandler;

#[async_trait]
impl OrchestrationHandler for AllKvOpsHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.set_value("X", "Y");
        ctx.clear_value("Z");
        ctx.clear_all_values();
        ctx.set_value("A", "1");
        let val = ctx.get_value("A").unwrap_or_default();
        Ok(val)
    }
}

/// Handler that reads a value from kv_state (seeded via snapshot).
struct ReadSnapshotHandler {
    key: String,
}

impl ReadSnapshotHandler {
    fn new(key: &str) -> Arc<Self> {
        Arc::new(Self { key: key.to_string() })
    }
}

#[async_trait]
impl OrchestrationHandler for ReadSnapshotHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let val = ctx.get_value(&self.key).unwrap_or("missing".to_string());
        Ok(val)
    }
}

/// Handler that reads a snapshot value then overwrites it.
struct ReadThenOverwriteSnapshotHandler {
    key: String,
    new_value: String,
}

impl ReadThenOverwriteSnapshotHandler {
    fn new(key: &str, new_value: &str) -> Arc<Self> {
        Arc::new(Self {
            key: key.to_string(),
            new_value: new_value.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for ReadThenOverwriteSnapshotHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.set_value(&self.key, &self.new_value);
        let val = ctx.get_value(&self.key).unwrap_or("missing".to_string());
        Ok(val)
    }
}

// ============================================================================
// Section 2.1: Action/Event Conversion
// ============================================================================

/// RE-KV-01: All KV action→event conversions produce correct event kinds
/// and None source_event_id.
#[test]
fn action_to_event_kv_variants() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let handler: Arc<dyn OrchestrationHandler> = Arc::new(AllKvOpsHandler);
    let result = execute(&mut engine, handler);

    assert_completed(&result, "1");

    let delta = engine.history_delta();
    // Should contain: KeyValueSet(X,Y), KeyValueCleared(Z), KeyValuesCleared, KeyValueSet(A,1)
    let kv_events: Vec<_> = delta
        .iter()
        .filter(|e| {
            matches!(
                &e.kind,
                EventKind::KeyValueSet { .. } | EventKind::KeyValueCleared { .. } | EventKind::KeyValuesCleared
            )
        })
        .collect();

    assert_eq!(kv_events.len(), 4, "should have 4 KV events in delta");

    // All KV events have source_event_id = None
    for ev in &kv_events {
        assert_eq!(ev.source_event_id, None, "KV events should not have source_event_id");
    }

    // Check specific types
    assert!(matches!(&kv_events[0].kind, EventKind::KeyValueSet { key, value } if key == "X" && value == "Y"));
    assert!(matches!(&kv_events[1].kind, EventKind::KeyValueCleared { key } if key == "Z"));
    assert!(matches!(&kv_events[2].kind, EventKind::KeyValuesCleared));
    assert!(matches!(&kv_events[3].kind, EventKind::KeyValueSet { key, value } if key == "A" && value == "1"));
}

// ============================================================================
// Section 2.2: Replay Determinism
// ============================================================================

/// RE-KV-02: Replaying with KeyValueSet in history reconstructs kv_state.
#[test]
fn replay_set_value_reconstructs_state() {
    let history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "A".to_string(),
                value: "1".to_string(),
            },
        ),
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, SetThenGetHandler::new("A", "1"));

    assert_completed(&result, "1");
}

/// RE-KV-03: Multiple sets of same key — last wins during replay.
#[test]
fn replay_multiple_sets_last_wins() {
    let history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "A".to_string(),
                value: "1".to_string(),
            },
        ),
        Event::with_event_id(
            3,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "A".to_string(),
                value: "2".to_string(),
            },
        ),
    ];
    let mut engine = create_engine(history);

    // Handler sets A to 1 then 2, reads back — should get "2"
    struct SetTwiceHandler;
    #[async_trait]
    impl OrchestrationHandler for SetTwiceHandler {
        async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
            ctx.set_value("A", "1");
            ctx.set_value("A", "2");
            Ok(ctx.get_value("A").unwrap_or_default())
        }
    }

    let result = execute(&mut engine, Arc::new(SetTwiceHandler));
    assert_completed(&result, "2");
}

/// RE-KV-04: Clear all then set — only post-clear key exists.
#[test]
fn replay_clear_then_set() {
    let history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "A".to_string(),
                value: "1".to_string(),
            },
        ),
        Event::with_event_id(3, TEST_INSTANCE, TEST_EXECUTION_ID, None, EventKind::KeyValuesCleared),
        Event::with_event_id(
            4,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "B".to_string(),
                value: "2".to_string(),
            },
        ),
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, Arc::new(SetClearSetHandler));
    assert_completed(&result, "A=none,B=2");
}

/// RE-KV-04a: Clear single key — only that key removed.
#[test]
fn replay_clear_single_key() {
    let history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "A".to_string(),
                value: "1".to_string(),
            },
        ),
        Event::with_event_id(
            3,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "B".to_string(),
                value: "2".to_string(),
            },
        ),
        Event::with_event_id(
            4,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueCleared { key: "A".to_string() },
        ),
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, Arc::new(SetThenClearSingleHandler));
    assert_completed(&result, "A=none,B=2");
}

/// RE-KV-05: Replay set_value matches history — no nondeterminism.
#[test]
fn replay_set_value_matches_action() {
    let history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "A".to_string(),
                value: "1".to_string(),
            },
        ),
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, SetKeyValueHandler::new("A", "1"));
    // Should complete successfully — action matches event
    assert_completed(&result, "done");
}

/// RE-KV-06: Nondeterminism when history has KeyValueSet but handler emits
/// a schedule action instead.
#[test]
fn replay_set_value_mismatch_nondeterminism() {
    // History has KeyValueSet — but handler will schedule an activity instead
    let history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "A".to_string(),
                value: "1".to_string(),
            },
        ),
        activity_scheduled(3, "Task", "input"),
        activity_completed(4, 3, "result"),
    ];
    let mut engine = create_engine(history);

    // Handler schedules an activity instead of setting KV
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));
    assert_nondeterminism(&result);
}

/// RE-KV-07: Replay clear_all_values matches history.
#[test]
fn replay_clear_all_values_matches_action() {
    let history = vec![
        started_event(1),
        Event::with_event_id(2, TEST_INSTANCE, TEST_EXECUTION_ID, None, EventKind::KeyValuesCleared),
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, Arc::new(ClearAllValuesHandler));
    assert_completed(&result, "cleared");
}

/// RE-KV-07a: Replay clear_value matches history.
#[test]
fn replay_clear_value_matches_action() {
    let history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueCleared { key: "A".to_string() },
        ),
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, ClearValueHandler::new("A"));
    assert_completed(&result, "cleared");
}

/// RE-KV-07b: Nondeterminism when history has KeyValueCleared but handler
/// emits a schedule action instead.
#[test]
fn replay_clear_value_mismatch_nondeterminism() {
    // History has KeyValueCleared — but handler will schedule an activity instead
    let history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueCleared { key: "A".to_string() },
        ),
        activity_scheduled(3, "Task", "input"),
        activity_completed(4, 3, "result"),
    ];
    let mut engine = create_engine(history);

    // Handler schedules an activity instead of clearing KV
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));
    assert_nondeterminism(&result);
}

/// RE-KV-08: KV interleaved with activities — both work correctly.
#[test]
fn replay_kv_interleaved_with_activities() {
    let history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "progress".to_string(),
                value: "started".to_string(),
            },
        ),
        activity_scheduled(3, "DoWork", "work-input"),
        activity_completed(4, 3, "work-result"),
    ];
    let mut engine = create_engine(history);

    let result = execute(
        &mut engine,
        SetKvThenActivityHandler::new("progress", "started", "DoWork", "work-input"),
    );
    assert_completed(&result, "work-result");
}

/// RE-KV-09: Setting a value during replay does not create a pending future/token.
/// The orchestration handler completes immediately — KV set is fire-and-forget.
#[test]
fn replay_kv_no_token_binding() {
    let history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::KeyValueSet {
                key: "k".to_string(),
                value: "v".to_string(),
            },
        ),
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, SetKeyValueHandler::new("k", "v"));

    // Should complete immediately — KV set does not suspend the orchestration
    // (unlike schedule_activity which creates a future/token that blocks progress).
    assert_completed(&result, "done");
}

// ============================================================================
// Section 2.3: New Events & Snapshot Seeding
// ============================================================================

/// RE-KV-10: After replay, new KV operations emit events in history_delta
/// and get_value returns the value immediately.
#[test]
fn new_kv_events_after_replay() {
    // Only the started event — everything after is "new"
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, Arc::new(AllKvOpsHandler));
    assert_completed(&result, "1");

    let delta = engine.history_delta();
    let kv_events: Vec<_> = delta
        .iter()
        .filter(|e| {
            matches!(
                &e.kind,
                EventKind::KeyValueSet { .. } | EventKind::KeyValueCleared { .. } | EventKind::KeyValuesCleared
            )
        })
        .collect();

    assert!(
        kv_events.len() >= 3,
        "should have at least set, clear_value, clear_all in delta"
    );
}

/// RE-KV-11: Snapshot seeds initial kv_state — get_value returns snapshot value.
#[test]
fn kv_state_seeded_from_snapshot() {
    let history = vec![started_event(1)];
    let mut snapshot = HashMap::new();
    snapshot.insert("X".to_string(), "from_prev_exec".to_string());

    let mut engine = create_engine(history).with_kv_snapshot(snapshot);

    let result = execute(&mut engine, ReadSnapshotHandler::new("X"));
    assert_completed(&result, "from_prev_exec");
}

/// RE-KV-12: Snapshot value overridden by code's own set_value.
#[test]
fn kv_state_snapshot_overridden_by_code() {
    let history = vec![started_event(1)];
    let mut snapshot = HashMap::new();
    snapshot.insert("X".to_string(), "old".to_string());

    let mut engine = create_engine(history).with_kv_snapshot(snapshot);

    let result = execute(&mut engine, ReadThenOverwriteSnapshotHandler::new("X", "new"));
    assert_completed(&result, "new");
}

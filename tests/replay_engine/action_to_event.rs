//! Action to Event Recording Tests
//!
//! Tests verifying that each orchestration action type correctly
//! creates a corresponding event in the history delta.
//!
//! Each scheduling action should result in a matching event being recorded
//! to ensure determinism can be enforced on replay.

use super::helpers::*;
use async_trait::async_trait;
use duroxide::{EventKind, OrchestrationContext, OrchestrationHandler};
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// Helper Handlers for Action-to-Event Tests
// ============================================================================

/// Handler that schedules a detached orchestration (fire-and-forget)
pub struct DetachedOrchHandler {
    orch_name: String,
    instance_id: String,
    orch_input: String,
}

impl DetachedOrchHandler {
    pub fn new(name: &str, instance: &str, input: &str) -> Arc<Self> {
        Arc::new(Self {
            orch_name: name.to_string(),
            instance_id: instance.to_string(),
            orch_input: input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for DetachedOrchHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.schedule_orchestration(&self.orch_name, &self.instance_id, &self.orch_input);
        Ok("scheduled".to_string())
    }
}

/// Handler that schedules a sub-orchestration without awaiting (for delta check)
pub struct SubOrchNoAwaitHandler {
    sub_name: String,
    sub_input: String,
}

impl SubOrchNoAwaitHandler {
    pub fn new(name: &str, input: &str) -> Arc<Self> {
        Arc::new(Self {
            sub_name: name.to_string(),
            sub_input: input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for SubOrchNoAwaitHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        // Schedule but don't await - just drop the future
        drop(ctx.schedule_sub_orchestration(&self.sub_name, &self.sub_input));
        Ok("scheduled".to_string())
    }
}

/// Handler that schedules a session-bound activity and awaits it.
pub struct SingleSessionActivityHandler {
    activity_name: String,
    activity_input: String,
    session_id: String,
}

impl SingleSessionActivityHandler {
    pub fn new(name: &str, input: &str, session_id: &str) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
            session_id: session_id.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for SingleSessionActivityHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let result = ctx
            .schedule_activity_on_session(&self.activity_name, &self.activity_input, &self.session_id)
            .await?;
        Ok(result)
    }
}

// ============================================================================
// Tests: Each Action Creates Corresponding Event in History Delta
// ============================================================================

/// Scheduling an activity creates an ActivityScheduled event in history delta.
#[test]
fn schedule_activity_creates_activity_scheduled_event() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let handler = SingleActivityHandler::new("MyActivity", "my-input");
    let result = execute(&mut engine, handler);

    // Should be waiting for the activity to complete
    assert_continue(&result);

    // Verify ActivityScheduled event was created in delta
    let delta = engine.history_delta();
    let activity_events: Vec<_> = delta
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ActivityScheduled { .. }))
        .collect();

    assert_eq!(
        activity_events.len(),
        1,
        "Expected exactly one ActivityScheduled event in delta"
    );

    match &activity_events[0].kind {
        EventKind::ActivityScheduled { name, input, .. } => {
            assert_eq!(name, "MyActivity");
            assert_eq!(input, "my-input");
        }
        _ => panic!("Expected ActivityScheduled event"),
    }
}

/// Scheduling a session-bound activity records session_id in ActivityScheduled.
#[test]
fn schedule_activity_on_session_creates_activity_scheduled_event_with_session_id() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let handler = SingleSessionActivityHandler::new("MyActivity", "my-input", "sess-1");
    let result = execute(&mut engine, handler);

    // Should be waiting for the activity to complete
    assert_continue(&result);

    // Verify ActivityScheduled event was created in delta with session_id
    let delta = engine.history_delta();
    let activity_events: Vec<_> = delta
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ActivityScheduled { .. }))
        .collect();

    assert_eq!(
        activity_events.len(),
        1,
        "Expected exactly one ActivityScheduled event in delta"
    );

    match &activity_events[0].kind {
        EventKind::ActivityScheduled {
            name,
            input,
            session_id,
            ..
        } => {
            assert_eq!(name, "MyActivity");
            assert_eq!(input, "my-input");
            assert_eq!(session_id.as_deref(), Some("sess-1"));
        }
        _ => panic!("Expected ActivityScheduled event"),
    }
}

/// On replay, a session_id mismatch between an existing ActivityScheduled event and a new action
/// must surface as nondeterminism.
#[test]
fn schedule_activity_on_session_session_id_mismatch_causes_nondeterminism() {
    let history = vec![
        started_event(1),
        // Existing schedule has no session_id
        duroxide::Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::ActivityScheduled {
                name: "MyActivity".to_string(),
                input: "my-input".to_string(),
                session_id: None,
                tag: None,
            },
        ),
    ];
    let mut engine = create_engine(history);

    // Handler schedules the same activity but with a session_id.
    // This must fail determinism checks.
    let handler = SingleSessionActivityHandler::new("MyActivity", "my-input", "sess-1");
    let result = execute(&mut engine, handler);

    assert_nondeterminism(&result);
}

/// Scheduling a timer creates a TimerCreated event in history delta.
#[test]
fn schedule_timer_creates_timer_created_event() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let handler = SingleTimerHandler::new(Duration::from_secs(10));
    let result = execute(&mut engine, handler);

    // Should be waiting for the timer to fire
    assert_continue(&result);

    // Verify TimerCreated event was created in delta
    let delta = engine.history_delta();
    let timer_events: Vec<_> = delta
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::TimerCreated { .. }))
        .collect();

    assert_eq!(
        timer_events.len(),
        1,
        "Expected exactly one TimerCreated event in delta"
    );

    match &timer_events[0].kind {
        EventKind::TimerCreated { fire_at_ms } => {
            // fire_at_ms should be non-zero (based on current time + duration)
            assert!(*fire_at_ms > 0, "Expected fire_at_ms to be set");
        }
        _ => panic!("Expected TimerCreated event"),
    }
}

/// Scheduling an external event wait creates an ExternalSubscribed event in history delta.
#[test]
fn schedule_wait_creates_external_subscribed_event() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let handler = WaitExternalHandler::new("MyEvent");
    let result = execute(&mut engine, handler);

    // Should be waiting for the external event
    assert_continue(&result);

    // Verify ExternalSubscribed event was created in delta
    let delta = engine.history_delta();
    let external_events: Vec<_> = delta
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ExternalSubscribed { .. }))
        .collect();

    assert_eq!(
        external_events.len(),
        1,
        "Expected exactly one ExternalSubscribed event in delta"
    );

    match &external_events[0].kind {
        EventKind::ExternalSubscribed { name } => {
            assert_eq!(name, "MyEvent");
        }
        _ => panic!("Expected ExternalSubscribed event"),
    }
}

/// Scheduling a sub-orchestration creates a SubOrchestrationScheduled event in history delta.
#[test]
fn schedule_sub_orchestration_creates_sub_orchestration_scheduled_event() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let handler = SubOrchNoAwaitHandler::new("ChildOrch", "child-input");
    let result = execute(&mut engine, handler);

    // Completes immediately since we don't await
    assert_completed(&result, "scheduled");

    // Verify SubOrchestrationScheduled event was created in delta
    let delta = engine.history_delta();
    let sub_orch_events: Vec<_> = delta
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::SubOrchestrationScheduled { .. }))
        .collect();

    assert_eq!(
        sub_orch_events.len(),
        1,
        "Expected exactly one SubOrchestrationScheduled event in delta"
    );

    match &sub_orch_events[0].kind {
        EventKind::SubOrchestrationScheduled { name, input, .. } => {
            assert_eq!(name, "ChildOrch");
            assert_eq!(input, "child-input");
        }
        _ => panic!("Expected SubOrchestrationScheduled event"),
    }
}

/// Scheduling a detached orchestration (fire-and-forget) creates an OrchestrationChained event
/// in history delta.
///
/// This test documents the expected behavior for determinism enforcement.
/// Fire-and-forget orchestration starts must be recorded so that replay can detect
/// nondeterminism if the scheduling call moves or becomes conditional.
#[test]
fn schedule_orchestration_creates_orchestration_chained_event() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let handler = DetachedOrchHandler::new("DetachedChild", "child-instance", "child-input");
    let result = execute(&mut engine, handler);

    // Completes immediately since fire-and-forget doesn't await
    assert_completed(&result, "scheduled");

    // Verify OrchestrationChained event was created in delta
    let delta = engine.history_delta();
    let chained_events: Vec<_> = delta
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::OrchestrationChained { .. }))
        .collect();

    assert_eq!(
        chained_events.len(),
        1,
        "Expected exactly one OrchestrationChained event in delta. \
         Bug: StartOrchestrationDetached action is not being recorded as an event!"
    );

    match &chained_events[0].kind {
        EventKind::OrchestrationChained { name, instance, input } => {
            assert_eq!(name, "DetachedChild");
            assert_eq!(instance, "child-instance");
            assert_eq!(input, "child-input");
        }
        _ => panic!("Expected OrchestrationChained event"),
    }
}

/// Multiple different action types in one orchestration should each create their own events.
#[test]
fn multiple_action_types_create_corresponding_events() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    // Use MultiScheduleNoAwaitHandler for activities
    let handler = MultiScheduleNoAwaitHandler::new(vec![("Task1", "input1"), ("Task2", "input2")]);
    let result = execute(&mut engine, handler);

    assert_completed(&result, "done");

    // Verify both ActivityScheduled events were created
    let delta = engine.history_delta();
    let activity_events: Vec<_> = delta
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ActivityScheduled { .. }))
        .collect();

    assert_eq!(
        activity_events.len(),
        2,
        "Expected two ActivityScheduled events in delta"
    );
}

// ============================================================================
// Nondeterminism Detection Tests
// ============================================================================

/// Handler that schedules a detached orchestration then an activity
pub struct DetachedThenActivityHandler {
    orch_name: String,
    instance_id: String,
    orch_input: String,
    activity_name: String,
    activity_input: String,
}

impl DetachedThenActivityHandler {
    pub fn new(
        orch_name: &str,
        instance: &str,
        orch_input: &str,
        activity_name: &str,
        activity_input: &str,
    ) -> Arc<Self> {
        Arc::new(Self {
            orch_name: orch_name.to_string(),
            instance_id: instance.to_string(),
            orch_input: orch_input.to_string(),
            activity_name: activity_name.to_string(),
            activity_input: activity_input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for DetachedThenActivityHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        // First: fire-and-forget a detached orchestration
        ctx.schedule_orchestration(&self.orch_name, &self.instance_id, &self.orch_input);
        // Then: schedule and await an activity
        let result = ctx.schedule_activity(&self.activity_name, &self.activity_input).await?;
        Ok(result)
    }
}

/// This test demonstrates the nondeterminism bug when OrchestrationChained events are not recorded.
///
/// Scenario:
/// 1. Fresh execution: orchestration schedules detached orch, then activity
/// 2. History is persisted (but OrchestrationChained is missing due to bug!)
/// 3. Replay with activity completion: engine tries to match StartOrchestrationDetached
///    action against ActivityScheduled event -> NONDETERMINISM ERROR
///
/// With the fix, OrchestrationChained is recorded, so replay correctly matches:
/// - OrchestrationChained event <- StartOrchestrationDetached action
/// - ActivityScheduled event <- CallActivity action
#[test]
fn detached_orchestration_then_activity_replays_correctly() {
    // === PHASE 1: Fresh execution ===
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let handler = DetachedThenActivityHandler::new(
        "DetachedChild",
        "child-1",
        "child-input",
        "MyActivity",
        "activity-input",
    );
    let result = execute(&mut engine, handler.clone());

    // Should be waiting for activity to complete
    assert_continue(&result);

    // Collect the delta (what would be persisted)
    let delta = engine.history_delta().to_vec();

    // === PHASE 2: Replay with activity completion ===
    // Simulate: history from phase 1 + activity completion message
    let mut replay_history = vec![started_event(1)];
    replay_history.extend(delta);

    let mut replay_engine = create_engine(replay_history);

    // Inject activity completion (id=2 if OrchestrationChained missing, id=3 if present)
    // We need to figure out which ID the activity got
    let activity_id = replay_engine
        .final_history()
        .iter()
        .find(|e| matches!(&e.kind, EventKind::ActivityScheduled { .. }))
        .map(|e: &duroxide::Event| e.event_id())
        .expect("ActivityScheduled should exist in history");

    replay_engine.prep_completions(vec![activity_completed_msg(activity_id, "activity-result")]);

    let replay_result = execute(&mut replay_engine, handler);

    // With the bug: this fails with nondeterminism because:
    // - History has: OrchestrationStarted(1), ActivityScheduled(2)
    // - Orchestration emits: StartOrchestrationDetached, then CallActivity
    // - Engine tries to match StartOrchestrationDetached with ActivityScheduled -> MISMATCH
    //
    // With the fix: this succeeds because:
    // - History has: OrchestrationStarted(1), OrchestrationChained(2), ActivityScheduled(3)
    // - Orchestration emits: StartOrchestrationDetached, then CallActivity
    // - Engine correctly matches both
    assert_completed(&replay_result, "activity-result");
}

// ============================================================================
// Tests: Activity Tags via with_tag()
// ============================================================================

/// Handler that schedules a tagged activity.
pub struct TaggedActivityHandler {
    activity_name: String,
    activity_input: String,
    tag: String,
}

impl TaggedActivityHandler {
    pub fn new(name: &str, input: &str, tag: &str) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
            tag: tag.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for TaggedActivityHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let result = ctx
            .schedule_activity(&self.activity_name, &self.activity_input)
            .with_tag(&self.tag)
            .await?;
        Ok(result)
    }
}

/// Scheduling an activity with .with_tag() creates an ActivityScheduled event with the tag.
#[test]
fn with_tag_sets_tag_on_emitted_event() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let handler = TaggedActivityHandler::new("GpuWork", "data", "gpu");
    let result = execute(&mut engine, handler);
    assert_continue(&result);

    let activity_events: Vec<_> = engine
        .history_delta()
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ActivityScheduled { .. }))
        .collect();

    assert_eq!(activity_events.len(), 1);

    match &activity_events[0].kind {
        EventKind::ActivityScheduled { name, tag, .. } => {
            assert_eq!(name, "GpuWork");
            assert_eq!(tag.as_deref(), Some("gpu"));
        }
        _ => panic!("Expected ActivityScheduled event"),
    }
}

/// Scheduling an activity without .with_tag() produces tag: None.
#[test]
fn no_with_tag_leaves_none() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let handler = SingleActivityHandler::new("PlainWork", "data");
    let result = execute(&mut engine, handler);
    assert_continue(&result);

    let activity_events: Vec<_> = engine
        .history_delta()
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ActivityScheduled { .. }))
        .collect();

    assert_eq!(activity_events.len(), 1);

    match &activity_events[0].kind {
        EventKind::ActivityScheduled { tag, .. } => {
            assert_eq!(tag, &None);
        }
        _ => panic!("Expected ActivityScheduled event"),
    }
}

/// On replay, a tag mismatch between an existing ActivityScheduled event and a new action
/// must surface as nondeterminism.
#[test]
fn tag_mismatch_on_replay_causes_nondeterminism() {
    let history = vec![
        started_event(1),
        // Existing schedule has no tag
        duroxide::Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::ActivityScheduled {
                name: "GpuWork".to_string(),
                input: "data".to_string(),
                session_id: None,
                tag: None,
            },
        ),
    ];
    let mut engine = create_engine(history);

    // Handler schedules the same activity but WITH a tag.
    // This must fail determinism checks.
    let handler = TaggedActivityHandler::new("GpuWork", "data", "gpu");
    let result = execute(&mut engine, handler);

    assert_nondeterminism(&result);
}

/// Changing a tag value between deployments (e.g. "gpu" → "cpu") also causes nondeterminism.
#[test]
fn tag_value_change_on_replay_causes_nondeterminism() {
    let history = vec![
        started_event(1),
        // Existing schedule has tag "gpu"
        duroxide::Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::ActivityScheduled {
                name: "Render".to_string(),
                input: "data".to_string(),
                session_id: None,
                tag: Some("gpu".to_string()),
            },
        ),
    ];
    let mut engine = create_engine(history);

    // New code changed the tag from "gpu" to "cpu"
    let handler = TaggedActivityHandler::new("Render", "data", "cpu");
    let result = execute(&mut engine, handler);

    assert_nondeterminism(&result);
}

/// Removing a tag (Some("gpu") → None) on replay causes nondeterminism.
#[test]
fn tag_removal_on_replay_causes_nondeterminism() {
    let history = vec![
        started_event(1),
        // Existing schedule has tag "gpu"
        duroxide::Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::ActivityScheduled {
                name: "Render".to_string(),
                input: "data".to_string(),
                session_id: None,
                tag: Some("gpu".to_string()),
            },
        ),
    ];
    let mut engine = create_engine(history);

    // New code removed the tag entirely
    let handler = SingleActivityHandler::new("Render", "data");
    let result = execute(&mut engine, handler);

    assert_nondeterminism(&result);
}

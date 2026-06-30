// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Unobserved Future Cancellation Tests
//!
//! Tests for cancellation of DurableFuture when dropped without completion.
//!
//! Key design principle: Actions ARE emitted at schedule time (not poll time)
//! because this is a durable orchestration framework. When a future is dropped,
//! the cancellation mechanism kicks in to clean up the in-flight work.
//!
//! Covers three scenarios:
//! 1. Select losers - future loses a race and is dropped
//! 2. Explicitly dropped futures - future dropped before completion
//! 3. Abandoned futures - future polled but dropped before completion

use async_trait::async_trait;
use duroxide::{Either2, Either3, EventKind, OrchestrationContext, OrchestrationHandler};
use std::sync::Arc;
use std::time::Duration;

use super::helpers::*;

// ============================================================================
// Select Loser Tests
// ============================================================================

/// Handler that does select2 with activity vs timer, timer wins
pub struct Select2TimerWinsHandler {
    activity_name: String,
    activity_input: String,
    timer_duration: Duration,
}

impl Select2TimerWinsHandler {
    pub fn new(name: &str, input: &str, duration: Duration) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
            timer_duration: duration,
        })
    }
}

#[async_trait]
impl OrchestrationHandler for Select2TimerWinsHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let activity = ctx.schedule_activity(&self.activity_name, &self.activity_input);
        let timer = ctx.schedule_timer(self.timer_duration);
        // Timer wins, activity loser should be cancelled
        match ctx.select2(activity, timer).await {
            Either2::First(result) => result.map(|r| format!("activity_won:{r}")),
            Either2::Second(()) => Ok("timer_won".to_string()),
        }
    }
}

/// Handler that schedules activity + timer, but does NOT drop the activity (awaits it instead).
/// Used to simulate a code change where a previously-dropped activity is no longer dropped.
pub struct ScheduleActivityAndTimerThenAwaitActivityHandler {
    activity_name: String,
    activity_input: String,
    timer_duration: Duration,
}

impl ScheduleActivityAndTimerThenAwaitActivityHandler {
    pub fn new(name: &str, input: &str, duration: Duration) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
            timer_duration: duration,
        })
    }
}

#[async_trait]
impl OrchestrationHandler for ScheduleActivityAndTimerThenAwaitActivityHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let activity = ctx.schedule_activity(&self.activity_name, &self.activity_input);
        let _timer = ctx.schedule_timer(self.timer_duration);

        // The activity is no longer dropped/cancelled; it is awaited.
        // In the test we intentionally never complete it, so the turn would Continue if
        // nondeterminism isn't detected first.
        let r = activity.await?;
        Ok(format!("activity_awaited:{r}"))
    }
}

/// Handler that replays a select2 timer win (dropping the activity loser), then schedules and
/// explicitly drops a NEW activity in the current turn.
pub struct Select2TimerWinsThenDropNewActivityHandler {
    activity_name: String,
    activity_input: String,
    timer_duration: Duration,
    extra_activity_name: String,
    extra_activity_input: String,
}

impl Select2TimerWinsThenDropNewActivityHandler {
    pub fn new(name: &str, input: &str, duration: Duration, extra_name: &str, extra_input: &str) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
            timer_duration: duration,
            extra_activity_name: extra_name.to_string(),
            extra_activity_input: extra_input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for Select2TimerWinsThenDropNewActivityHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let activity = ctx.schedule_activity(&self.activity_name, &self.activity_input);
        let timer = ctx.schedule_timer(self.timer_duration);

        // Timer wins, activity loser is dropped by select2.
        let _ = ctx.select2(activity, timer).await;

        // Now schedule a NEW activity and explicitly drop it in the current turn.
        let extra = ctx.schedule_activity(&self.extra_activity_name, &self.extra_activity_input);
        drop(extra);

        Ok("timer_won_plus_extra_drop".to_string())
    }
}

/// select2 timer wins - activity should be in cancelled_activity_ids
///
/// Scenario: `ctx.select2(activity, timer)` where timer completes first.
/// The activity future is dropped by select2, triggering cancellation.
/// Note: Activity is scheduled first (emits action), then timer.
#[test]
fn select2_timer_wins_activity_cancelled() {
    // History must match handler's schedule order: activity first, then timer
    let history = vec![
        started_event(1),
        activity_scheduled(2, "LongTask", "input"), // Activity scheduled first
        timer_created(3, 1000),                     // Timer scheduled second
    ];
    let mut engine = create_engine(history);

    // Timer fires (wins the race)
    engine.prep_completions(vec![timer_fired_msg(3, 1000)]);

    let result = execute(
        &mut engine,
        Select2TimerWinsHandler::new("LongTask", "input", Duration::from_millis(1)),
    );

    assert_completed(&result, "timer_won");

    // The activity should be marked as cancelled
    let cancelled = engine.cancelled_activity_ids();
    assert!(
        cancelled.contains(&2),
        "Activity schedule_id 2 should be in cancelled list, got {cancelled:?}"
    );
}

/// Handler that does select2 with activity vs timer, activity wins
pub struct Select2ActivityWinsHandler {
    activity_name: String,
    activity_input: String,
    timer_duration: Duration,
}

impl Select2ActivityWinsHandler {
    pub fn new(name: &str, input: &str, duration: Duration) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
            timer_duration: duration,
        })
    }
}

#[async_trait]
impl OrchestrationHandler for Select2ActivityWinsHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let activity = ctx.schedule_activity(&self.activity_name, &self.activity_input);
        let timer = ctx.schedule_timer(self.timer_duration);
        // Activity wins, timer is dropped (but timers don't need explicit cancellation)
        match ctx.select2(activity, timer).await {
            Either2::First(result) => result.map(|r| format!("activity_won:{r}")),
            Either2::Second(()) => Ok("timer_won".to_string()),
        }
    }
}

/// select2 activity wins - timer should NOT be in cancelled list
///
/// Timers are virtual constructs and don't need explicit cancellation.
#[test]
fn select2_activity_wins_timer_not_cancelled() {
    let history = vec![
        started_event(1),
        activity_scheduled(2, "FastTask", "input"), // Activity scheduled first
        timer_created(3, 5000),                     // Timer scheduled second
    ];
    let mut engine = create_engine(history);

    // Activity completes first
    engine.prep_completions(vec![activity_completed_msg(2, "done")]);

    let result = execute(
        &mut engine,
        Select2ActivityWinsHandler::new("FastTask", "input", Duration::from_millis(5)),
    );

    assert_completed(&result, "activity_won:done");

    // Timer is dropped but timers don't go to cancelled_activity_ids
    let cancelled = engine.cancelled_activity_ids();
    assert!(
        cancelled.is_empty(),
        "No activities should be cancelled (timer dropped, but timers don't need cancel), got {cancelled:?}"
    );
}

/// select3 with one winner and two losers
pub struct Select3Handler;

#[async_trait]
impl OrchestrationHandler for Select3Handler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let a1 = ctx.schedule_activity("Task1", "input1");
        let a2 = ctx.schedule_activity("Task2", "input2");
        let timer = ctx.schedule_timer(Duration::from_millis(1));
        // Timer wins, both activities dropped
        match ctx.select3(a1, a2, timer).await {
            Either3::First(r) => r,
            Either3::Second(r) => r,
            Either3::Third(()) => Ok("timer_won".to_string()),
        }
    }
}

/// select3 timer wins - both activities should be cancelled
#[test]
fn select3_two_activities_cancelled() {
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task1", "input1"),
        activity_scheduled(3, "Task2", "input2"),
        timer_created(4, 1000),
    ];
    let mut engine = create_engine(history);

    // Timer fires first
    engine.prep_completions(vec![timer_fired_msg(4, 1000)]);

    let result = execute(&mut engine, Arc::new(Select3Handler));

    assert_completed(&result, "timer_won");

    // Both activities should be cancelled
    let cancelled = engine.cancelled_activity_ids();
    assert!(
        cancelled.contains(&2) && cancelled.contains(&3),
        "Both activities should be cancelled, got {cancelled:?}"
    );
}

// ============================================================================
// Completed Future Tests
// ============================================================================

/// Completed future should NOT be in cancelled list
#[test]
fn completed_future_not_in_cancelled_list() {
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        activity_completed(3, 2, "result"),
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    assert_completed(&result, "result");

    // Activity completed normally, should NOT be cancelled
    let cancelled = engine.cancelled_activity_ids();
    assert!(
        cancelled.is_empty(),
        "Completed activity should NOT be in cancelled list, got {cancelled:?}"
    );
}

// ============================================================================
// Explicit Drop Tests (After Schedule)
// ============================================================================

/// Handler that schedules an activity then immediately drops the future
pub struct DropActivityAfterScheduleHandler {
    activity_name: String,
    activity_input: String,
}

impl DropActivityAfterScheduleHandler {
    pub fn new(name: &str, input: &str) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for DropActivityAfterScheduleHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        // Schedule activity (action is emitted)
        let fut = ctx.schedule_activity(&self.activity_name, &self.activity_input);
        // Explicitly drop before awaiting completion
        drop(fut);
        // Complete the orchestration via a different path
        Ok("completed_after_dropping_activity".to_string())
    }
}

/// Explicitly dropped activity - should be in cancelled list
///
/// Actions ARE emitted at schedule time, so the activity will be in history.
/// Dropping the future should add it to cancelled_activity_ids.
#[test]
fn explicit_drop_activity_gets_cancelled() {
    // Fresh execution - activity will be scheduled then dropped
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let result = execute(
        &mut engine,
        DropActivityAfterScheduleHandler::new("DroppedTask", "input"),
    );

    assert_completed(&result, "completed_after_dropping_activity");

    // Activity was scheduled (check history delta)
    assert!(
        has_activity_scheduled_delta(&engine, "DroppedTask"),
        "Activity should be in history delta (emitted at schedule time)"
    );

    // Activity should be in cancelled list
    let cancelled = engine.cancelled_activity_ids();
    assert!(
        cancelled.contains(&2), // event_id 2 for the scheduled activity
        "Dropped activity should be in cancelled list, got {cancelled:?}"
    );

    // Cancellation request should be recorded in history delta
    let has_cancel_event = engine.history_delta().iter().any(|e| {
        matches!(&e.kind, EventKind::ActivityCancelRequested { reason } if reason == "dropped_future")
            && e.source_event_id == Some(2)
    });
    assert!(
        has_cancel_event,
        "Expected ActivityCancelRequested(source_event_id=2, reason=dropped_future) in history delta"
    );
}

/// Handler that explicitly drops a timer
pub struct ExplicitDropTimerHandler;

#[async_trait]
impl OrchestrationHandler for ExplicitDropTimerHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let timer = ctx.schedule_timer(Duration::from_secs(60));
        drop(timer); // Explicitly drop timer
        Ok("timer_dropped".to_string())
    }
}

/// Explicit drop of timer - no cancellation needed (timers are virtual)
#[test]
fn explicit_drop_timer_no_cancel_needed() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, Arc::new(ExplicitDropTimerHandler));

    assert_completed(&result, "timer_dropped");

    // Timer should not appear in cancelled_activity_ids (timers are virtual)
    let cancelled = engine.cancelled_activity_ids();
    assert!(cancelled.is_empty(), "No activities cancelled for timer drop");
}

/// Handler that explicitly drops an external wait
pub struct ExplicitDropExternalWaitHandler {
    event_name: String,
}

impl ExplicitDropExternalWaitHandler {
    pub fn new(name: &str) -> Arc<Self> {
        Arc::new(Self {
            event_name: name.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for ExplicitDropExternalWaitHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let wait = ctx.schedule_wait(&self.event_name);
        drop(wait); // Explicitly drop wait
        Ok("wait_dropped".to_string())
    }
}

/// Explicit drop of external wait - should be cleaned up
#[test]
fn explicit_drop_external_wait() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, ExplicitDropExternalWaitHandler::new("MyEvent"));

    assert_completed(&result, "wait_dropped");

    // External wait should not appear in activity cancel list (it's not an activity)
    let cancelled = engine.cancelled_activity_ids();
    assert!(cancelled.is_empty(), "External waits don't go to activity cancel list");
}

// ============================================================================
// Multiple Dropped Futures Tests
// ============================================================================

/// Handler that creates multiple futures and drops all of them
pub struct MultipleDroppedActivitiesHandler;

#[async_trait]
impl OrchestrationHandler for MultipleDroppedActivitiesHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        // Create several futures (actions emitted at schedule time)
        let _a = ctx.schedule_activity("Task1", "input1");
        let _b = ctx.schedule_activity("Task2", "input2");
        let _c = ctx.schedule_timer(Duration::from_secs(10));
        // All dropped here without await
        Ok("all_dropped".to_string())
    }
}

/// Multiple dropped activities - all activities should be cancelled (not timer)
#[test]
fn multiple_dropped_activities_all_cancelled() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, Arc::new(MultipleDroppedActivitiesHandler));

    assert_completed(&result, "all_dropped");

    // Both activities should be in cancelled list (but not the timer)
    let cancelled = engine.cancelled_activity_ids();
    assert!(
        cancelled.contains(&2) && cancelled.contains(&3),
        "Both activities should be cancelled, got {cancelled:?}"
    );
    // Timer (event_id 4) should NOT be in cancelled list
    assert!(!cancelled.contains(&4), "Timer should NOT be in cancelled list");
}

// ============================================================================
// Sub-Orchestration Cancellation Tests
// ============================================================================

/// Handler that drops a sub-orchestration future after scheduling
pub struct DropSubOrchAfterScheduleHandler {
    sub_name: String,
    sub_input: String,
}

impl DropSubOrchAfterScheduleHandler {
    pub fn new(name: &str, input: &str) -> Arc<Self> {
        Arc::new(Self {
            sub_name: name.to_string(),
            sub_input: input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for DropSubOrchAfterScheduleHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let sub_orch = ctx.schedule_sub_orchestration(&self.sub_name, &self.sub_input);
        drop(sub_orch); // Drop after schedule
        Ok("sub_orch_dropped".to_string())
    }
}

/// Drop sub-orchestration after schedule - child should be in cancelled list
#[test]
fn sub_orch_drop_after_schedule_cancelled() {
    let history = vec![started_event(1)];
    let mut engine = create_engine(history);

    let result = execute(
        &mut engine,
        DropSubOrchAfterScheduleHandler::new("ChildOrch", "child_input"),
    );

    assert_completed(&result, "sub_orch_dropped");

    // Sub-orchestration should be in cancelled list
    let cancelled = engine.cancelled_sub_orchestration_ids();
    assert!(
        !cancelled.is_empty(),
        "Child sub-orchestration should be in cancelled list, got {cancelled:?}"
    );

    // Cancellation request should be recorded in history delta.
    // For fresh execution, SubOrchestrationScheduled will be event_id=2.
    let has_cancel_event = engine.history_delta().iter().any(|e| {
        matches!(&e.kind, EventKind::SubOrchestrationCancelRequested { reason } if reason == "dropped_future")
            && e.source_event_id == Some(2)
    });
    assert!(
        has_cancel_event,
        "Expected SubOrchestrationCancelRequested(source_event_id=2, reason=dropped_future) in history delta"
    );
}

/// Handler that polls a sub-orchestration then drops it via select2
pub struct SubOrchSelectLoserHandler {
    sub_name: String,
    sub_input: String,
    timer_duration: Duration,
}

impl SubOrchSelectLoserHandler {
    pub fn new(name: &str, input: &str, duration: Duration) -> Arc<Self> {
        Arc::new(Self {
            sub_name: name.to_string(),
            sub_input: input.to_string(),
            timer_duration: duration,
        })
    }
}

#[async_trait]
impl OrchestrationHandler for SubOrchSelectLoserHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let sub_orch = ctx.schedule_sub_orchestration(&self.sub_name, &self.sub_input);
        let timer = ctx.schedule_timer(self.timer_duration);
        // Timer wins, sub-orchestration is dropped
        match ctx.select2(sub_orch, timer).await {
            Either2::First(result) => result,
            Either2::Second(()) => Ok("timer_won_sub_orch_cancelled".to_string()),
        }
    }
}

/// Sub-orchestration select loser - child should be in cancelled_sub_orchestration_ids
#[test]
fn sub_orch_select_loser_child_cancelled() {
    // History must match handler's schedule order: sub_orch first, timer second
    let history = vec![
        started_event(1),
        sub_orch_scheduled(2, "ChildOrch", "sub::2", "child_input"),
        timer_created(3, 1000),
    ];
    let mut engine = create_engine(history);

    // Timer fires first
    engine.prep_completions(vec![timer_fired_msg(3, 1000)]);

    let result = execute(
        &mut engine,
        SubOrchSelectLoserHandler::new("ChildOrch", "child_input", Duration::from_millis(1)),
    );

    assert_completed(&result, "timer_won_sub_orch_cancelled");

    // Sub-orchestration should be in cancelled list
    let cancelled = engine.cancelled_sub_orchestration_ids();
    assert!(
        cancelled.iter().any(|id| id == "sub::2"),
        "Child sub-orchestration should be cancelled, got {cancelled:?}"
    );
}

// ============================================================================
// Replay Determinism Tests
// ============================================================================

/// Replay with select loser should produce same cancellation
#[test]
fn replay_select_loser_same_cancellation() {
    // Replay scenario: history has completed turn with timer winning
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        timer_created(3, 1000),
        timer_fired(4, 3, 1000), // Timer completed in history
    ];
    let mut engine = create_engine(history);

    let result = execute(
        &mut engine,
        Select2TimerWinsHandler::new("Task", "input", Duration::from_millis(1)),
    );

    assert_completed(&result, "timer_won");

    // During replay, same cancellation should occur
    let cancelled = engine.cancelled_activity_ids();
    assert!(cancelled.contains(&2), "Replay should still cancel the activity");
}

/// Completed sub-orchestration should NOT be in cancelled list
#[test]
fn completed_sub_orch_not_cancelled() {
    let history = vec![
        started_event(1),
        sub_orch_scheduled(2, "ChildOrch", "sub::2", "input"),
        sub_orch_completed(3, 2, "child_result"),
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, SubOrchHandler::new("ChildOrch", "input"));

    assert_completed(&result, "child_result");

    // Completed sub-orch should NOT be in cancelled list
    let cancelled = engine.cancelled_sub_orchestration_ids();
    assert!(
        cancelled.is_empty(),
        "Completed sub-orchestration should NOT be cancelled, got {cancelled:?}"
    );
}

// ============================================================================
// Dehydration Tests - Suspended orchestrations should NOT trigger cancellation
// ============================================================================

/// Handler that awaits an activity (will block/dehydrate if no completion)
#[allow(dead_code)]
pub struct AwaitActivityHandler {
    activity_name: String,
    activity_input: String,
}

#[allow(dead_code)]
impl AwaitActivityHandler {
    pub fn new(name: &str, input: &str) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for AwaitActivityHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let result = ctx.schedule_activity(&self.activity_name, &self.activity_input).await?;
        Ok(format!("got:{result}"))
    }
}

/// Handler that drops an activity then awaits a timer (completes within turn)
pub struct DropActivityThenAwaitTimerHandler;

#[async_trait]
impl OrchestrationHandler for DropActivityThenAwaitTimerHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        // Schedule and immediately drop the activity
        let activity = ctx.schedule_activity("DroppedTask", "input");
        drop(activity);

        // Now await a timer that completes - orchestration finishes this turn
        ctx.schedule_timer(Duration::from_millis(100)).await;
        Ok("done_after_drop".to_string())
    }
}

/// Explicit drop mid-execution SHOULD trigger cancellation.
///
/// This proves the cancellation mechanism works: when a future is dropped
/// while the orchestration is still running (before returning), the cancellation
/// is collected. Even if the orchestration then dehydrates waiting for something
/// else, the earlier explicit drop is still captured.
#[test]
fn explicit_drop_then_await_captures_cancellation() {
    // History: activity scheduled, timer scheduled, but timer NOT fired yet
    // Orchestration will dehydrate waiting for timer
    let history = vec![
        started_event(1),
        activity_scheduled(2, "DroppedTask", "input"),
        timer_created(3, 100),
        // No timer_fired - orchestration will dehydrate
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, Arc::new(DropActivityThenAwaitTimerHandler));

    // Orchestration dehydrates waiting for timer
    assert_continue(&result);

    // The explicitly dropped activity SHOULD still be in cancelled list
    // even though orchestration didn't complete this turn
    let cancelled = engine.cancelled_activity_ids();
    assert!(
        cancelled.contains(&2),
        "Explicitly dropped activity should be cancelled even when orchestration dehydrates, got {cancelled:?}"
    );
}

// ============================================================================
// Cancellation Request + Completion Race Tests
// ============================================================================

/// Completion after cancel request should be tolerated (race), as long as the
/// cancellation decision is consistent with replay.
#[test]
fn replay_tolerates_completion_after_cancel_request() {
    // History: select2 timer wins => activity was dropped and cancellation requested,
    // but activity completion arrives later anyway.
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        timer_created(3, 1000),
        timer_fired(4, 3, 1000),
        activity_cancel_requested(5, 2, "dropped_future"),
        activity_completed(6, 2, "late_result"),
    ];
    let mut engine = create_engine(history);

    let result = execute(
        &mut engine,
        Select2TimerWinsHandler::new("Task", "input", Duration::from_millis(1000)),
    );
    assert_completed(&result, "timer_won");
}

/// If history says a dropped-future cancellation happened, but replay no longer drops
/// the future (e.g., now it is awaited), treat as nondeterminism.
#[test]
fn replay_fails_when_cancellation_decisions_differ() {
    // History shows cancellation requested for the activity (dropped_future), but also
    // has a completion. If the orchestration now awaits the activity, it will not drop
    // the future and cancellation set will differ.
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        activity_cancel_requested(3, 2, "dropped_future"),
        activity_completed(4, 2, "done"),
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));
    assert_nondeterminism(&result);
}

// ============================================================================
// Cross-Turn Cancellation Tests
// ============================================================================

/// Activity scheduled in turn N, cancelled in turn N+1 after replay.
///
/// This tests the complete cross-turn cancellation flow:
/// - Turn 1: Activity and timer scheduled, both events persisted
/// - Turn 2: Timer fires, activity loses select2, gets cancelled during REPLAY
///
/// We verify BOTH outputs:
/// 1. Side-channel `cancelled_activity_ids()` contains the activity (for provider lock-stealing)
/// 2. `ActivityCancelRequested` event is in `history_delta` (for history breadcrumb)
#[test]
fn cross_turn_activity_cancellation_emits_both_signals() {
    // Simulate persisted history from turn 1: activity and timer scheduled
    let persisted_history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        timer_created(3, 1000),
    ];
    let persisted_len = persisted_history.len();

    // Create engine with explicit persisted_history_len to indicate this is a replay turn
    let mut engine = create_engine_with_persisted_len(persisted_history, persisted_len);

    // Turn 2: Timer fires (completion message arrives)
    engine.prep_completions(vec![timer_fired_msg(3, 1000)]);

    let result = execute(
        &mut engine,
        Select2TimerWinsHandler::new("Task", "input", Duration::from_millis(1)),
    );

    assert_completed(&result, "timer_won");

    // Verify 1: Side-channel signal for provider lock-stealing
    let cancelled = engine.cancelled_activity_ids();
    assert!(
        cancelled.contains(&2),
        "cancelled_activity_ids should contain activity 2 for provider lock-stealing, got {cancelled:?}"
    );

    // Verify 2: History event for observability/replay determinism
    let cancel_events: Vec<_> = engine
        .history_delta()
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ActivityCancelRequested { reason } if reason == "dropped_future"))
        .collect();

    assert_eq!(
        cancel_events.len(),
        1,
        "Expected exactly 1 ActivityCancelRequested in history_delta, got {cancel_events:?}"
    );
    assert_eq!(
        cancel_events[0].source_event_id,
        Some(2),
        "ActivityCancelRequested should reference the scheduled activity (event_id=2)"
    );
}

/// Sub-orchestration scheduled in turn N, cancelled in turn N+1 after replay.
///
/// Same as above but for sub-orchestrations.
#[test]
fn cross_turn_sub_orch_cancellation_emits_both_signals() {
    // Simulate persisted history from turn 1
    let persisted_history = vec![
        started_event(1),
        sub_orch_scheduled(2, "ChildOrch", "sub::2", "input"),
        timer_created(3, 1000),
    ];
    let persisted_len = persisted_history.len();

    let mut engine = create_engine_with_persisted_len(persisted_history, persisted_len);

    // Turn 2: Timer fires
    engine.prep_completions(vec![timer_fired_msg(3, 1000)]);

    let result = execute(
        &mut engine,
        SubOrchSelectLoserHandler::new("ChildOrch", "input", Duration::from_millis(1)),
    );

    assert_completed(&result, "timer_won_sub_orch_cancelled");

    // Verify 1: Side-channel signal for CancelInstance work item
    let cancelled = engine.cancelled_sub_orchestration_ids();
    assert!(
        cancelled.iter().any(|id| id == "sub::2"),
        "cancelled_sub_orchestration_ids should contain 'sub::2', got {cancelled:?}"
    );

    // Verify 2: History event for observability/replay determinism
    let cancel_events: Vec<_> = engine
        .history_delta()
        .iter()
        .filter(
            |e| matches!(&e.kind, EventKind::SubOrchestrationCancelRequested { reason } if reason == "dropped_future"),
        )
        .collect();

    assert_eq!(
        cancel_events.len(),
        1,
        "Expected exactly 1 SubOrchestrationCancelRequested in history_delta, got {cancel_events:?}"
    );
    assert_eq!(
        cancel_events[0].source_event_id,
        Some(2),
        "SubOrchestrationCancelRequested should reference the scheduled sub-orch (event_id=2)"
    );
}

/// If the cancellation-request event already exists in persisted history, do not re-emit
/// provider side-channel cancellation IDs on replay turns.
#[test]
fn replay_does_not_reemit_activity_cancellation_side_channel_when_already_in_history() {
    let persisted_history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        timer_created(3, 1000),
        timer_fired(4, 3, 1000),
        activity_cancel_requested(5, 2, "dropped_future"),
    ];
    let persisted_len = persisted_history.len();

    let mut engine = create_engine_with_persisted_len(persisted_history, persisted_len);

    let result = execute(
        &mut engine,
        Select2TimerWinsHandler::new("Task", "input", Duration::from_millis(1000)),
    );
    assert_completed(&result, "timer_won");

    let cancelled = engine.cancelled_activity_ids();
    assert!(
        cancelled.is_empty(),
        "cancelled_activity_ids should be empty on replay when cancellation-request already exists in history, got {cancelled:?}"
    );

    let cancel_events: Vec<_> = engine
        .history_delta()
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ActivityCancelRequested { reason } if reason == "dropped_future"))
        .collect();
    assert!(
        cancel_events.is_empty(),
        "No new ActivityCancelRequested events expected in history_delta, got {cancel_events:?}"
    );
}

/// Same as above but for sub-orchestrations.
#[test]
fn replay_does_not_reemit_sub_orch_cancellation_side_channel_when_already_in_history() {
    let persisted_history = vec![
        started_event(1),
        sub_orch_scheduled(2, "ChildOrch", "sub::2", "input"),
        timer_created(3, 1000),
        timer_fired(4, 3, 1000),
        sub_orch_cancel_requested(5, 2, "dropped_future"),
    ];
    let persisted_len = persisted_history.len();

    let mut engine = create_engine_with_persisted_len(persisted_history, persisted_len);

    let result = execute(
        &mut engine,
        SubOrchSelectLoserHandler::new("ChildOrch", "input", Duration::from_millis(1000)),
    );
    assert_completed(&result, "timer_won_sub_orch_cancelled");

    let cancelled = engine.cancelled_sub_orchestration_ids();
    assert!(
        cancelled.is_empty(),
        "cancelled_sub_orchestration_ids should be empty on replay when cancellation-request already exists in history, got {cancelled:?}"
    );

    let cancel_events: Vec<_> = engine
        .history_delta()
        .iter()
        .filter(
            |e| matches!(&e.kind, EventKind::SubOrchestrationCancelRequested { reason } if reason == "dropped_future"),
        )
        .collect();
    assert!(
        cancel_events.is_empty(),
        "No new SubOrchestrationCancelRequested events expected in history_delta, got {cancel_events:?}"
    );
}

/// If persisted history already has a dropped-future cancellation, replay may also produce NEW
/// cancellations for schedules created this turn; this should not be treated as nondeterminism.
#[test]
fn replay_allows_new_cancellations_in_current_turn_even_when_prior_cancellations_exist() {
    // Persisted history includes a dropped-future cancellation for schedule_id=2.
    let persisted_history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        timer_created(3, 1000),
        timer_fired(4, 3, 1000),
        activity_cancel_requested(5, 2, "dropped_future"),
    ];
    let persisted_len = persisted_history.len();

    let mut engine = create_engine_with_persisted_len(persisted_history, persisted_len);

    let result = execute(
        &mut engine,
        Select2TimerWinsThenDropNewActivityHandler::new("Task", "input", Duration::from_millis(1000), "Extra", "x"),
    );
    assert_completed(&result, "timer_won_plus_extra_drop");

    // Side-channel should include ONLY the new cancellation (for the new schedule), not the
    // already-persisted cancellation for schedule_id=2.
    let cancelled = engine.cancelled_activity_ids();
    assert!(
        !cancelled.contains(&2),
        "Should not re-emit persisted cancellation schedule_id=2 in side-channel, got {cancelled:?}"
    );
    assert!(
        cancelled.contains(&6),
        "Expected side-channel cancellation for newly scheduled/dropped activity (event_id=6), got {cancelled:?}"
    );
}

/// If persisted history already contains a dropped-future cancellation-request for a replayed
/// schedule, but the current code no longer drops that future, replay must fail with nondeterminism.
#[test]
fn replay_fails_if_persisted_dropped_future_cancel_is_no_longer_emitted_by_code() {
    let persisted_history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        timer_created(3, 1000),
        timer_fired(4, 3, 1000),
        activity_cancel_requested(5, 2, "dropped_future"),
    ];
    let persisted_len = persisted_history.len();
    let mut engine = create_engine_with_persisted_len(persisted_history, persisted_len);

    // This handler does NOT drop the activity; it awaits it (and we never provide completion).
    // The replay engine should detect that the persisted cancellation decision is no longer
    // produced by the code and treat it as nondeterminism.
    let result = execute(
        &mut engine,
        ScheduleActivityAndTimerThenAwaitActivityHandler::new("Task", "input", Duration::from_millis(1000)),
    );

    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "cancellation mismatch (activities)");
}

// ============================================================================
// Duplicate Cancellation Events Tests
// ============================================================================

/// Duplicate ActivityCancelRequested events in history are benign (no-op during replay).
#[test]
fn duplicate_activity_cancel_requested_is_benign() {
    // History has two ActivityCancelRequested for the same source_event_id.
    // This can happen if e.g., future was dropped and then parent was cancelled.
    // Replay should not fail - cancel-request events are no-ops.
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        timer_created(3, 1000),
        timer_fired(4, 3, 1000),
        activity_cancel_requested(5, 2, "dropped_future"),
        activity_cancel_requested(6, 2, "orchestration_terminal_failed"), // Duplicate
    ];
    let mut engine = create_engine(history);

    let result = execute(
        &mut engine,
        Select2TimerWinsHandler::new("Task", "input", Duration::from_millis(1000)),
    );
    assert_completed(&result, "timer_won");
}

/// Duplicate SubOrchestrationCancelRequested events in history are benign.
#[test]
fn duplicate_sub_orch_cancel_requested_is_benign() {
    // History has two SubOrchestrationCancelRequested for the same source_event_id.
    // This can happen if future was dropped and then parent was cancelled.
    // Replay should not fail - cancel-request events are no-ops.
    let history = vec![
        started_event(1),
        sub_orch_scheduled(2, "ChildOrch", "sub::2", "input"),
        timer_created(3, 1000),
        timer_fired(4, 3, 1000),
        sub_orch_cancel_requested(5, 2, "dropped_future"),
        sub_orch_cancel_requested(6, 2, "orchestration_terminal_failed"), // Duplicate
    ];
    let mut engine = create_engine(history);

    let result = execute(
        &mut engine,
        SubOrchSelectLoserHandler::new("ChildOrch", "input", Duration::from_millis(1000)),
    );
    assert_completed(&result, "timer_won_sub_orch_cancelled");
}

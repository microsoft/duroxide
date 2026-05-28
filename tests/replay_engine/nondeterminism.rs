// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Nondeterminism Detection Tests
//!
//! Tests verifying the engine detects replay mismatches.

use super::helpers::*;

/// History has activity, handler schedules timer - should fail with nondeterminism.
///
/// Original orchestration code (that created history):
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     Ok(result)
/// }
/// ```
///
/// Changed orchestration code (causes mismatch):
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     ctx.schedule_timer(Duration::from_secs(60)).await;  // Different!
///     Ok("done".to_string())
/// }
/// ```
#[test]
fn schedule_mismatch_activity_vs_timer() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // Original code scheduled activity
    ];
    let mut engine = create_engine(history);

    // Handler schedules timer instead of activity
    let result = execute(&mut engine, SingleTimerHandler::new(std::time::Duration::from_secs(60)));

    assert_nondeterminism(&result);
}

/// History has timer, handler schedules activity - should fail with nondeterminism.
///
/// Original orchestration scheduled a timer, but code was changed to schedule an activity.
#[test]
fn schedule_mismatch_timer_vs_activity() {
    let history = vec![
        started_event(1),       // OrchestrationStarted
        timer_created(2, 1000), // Original code scheduled timer
    ];
    let mut engine = create_engine(history);

    // Handler schedules activity instead of timer
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    assert_nondeterminism(&result);
}

/// History has activity with name "A", handler schedules activity "B" - should fail.
///
/// Original orchestration called schedule_activity("ActivityA", ...) but code now
/// calls schedule_activity("ActivityB", ...) - activity name changed.
#[test]
fn schedule_mismatch_wrong_activity_name() {
    let history = vec![
        started_event(1),                            // OrchestrationStarted
        activity_scheduled(2, "ActivityA", "input"), // Original name: "ActivityA"
    ];
    let mut engine = create_engine(history);

    // Handler schedules activity with different name
    let result = execute(&mut engine, SingleActivityHandler::new("ActivityB", "input"));

    assert_nondeterminism(&result);
}

/// History has activity with input "x", handler schedules with input "y" - should fail.
///
/// Activity input changed between runs - this is nondeterministic.
#[test]
fn schedule_mismatch_wrong_input() {
    let history = vec![
        started_event(1),                         // OrchestrationStarted
        activity_scheduled(2, "Task", "input-x"), // Original input: "input-x"
    ];
    let mut engine = create_engine(history);

    // Handler schedules activity with different input
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input-y"));

    assert_nondeterminism(&result);
}

/// History has external subscription "X", handler waits for "Y" - should fail.
///
/// Original code waited for "EventX", but code now waits for "EventY".
#[test]
fn schedule_mismatch_wrong_external_name() {
    let history = vec![
        started_event(1),                 // OrchestrationStarted
        external_subscribed(2, "EventX"), // Original: schedule_wait("EventX")
    ];
    let mut engine = create_engine(history);

    // Handler waits for different event
    let result = execute(&mut engine, WaitExternalHandler::new("EventY"));

    assert_nondeterminism(&result);
}

/// History has schedule, handler returns immediately.
/// This is legitimate because user code can schedule work without awaiting it.
///
/// Orchestration code that produces this history:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let _ = ctx.schedule_activity("Task", "input");  // fire-and-forget, no .await
///     Ok("done".to_string())
/// }
/// ```
#[test]
fn history_schedule_no_emitted_action() {
    // History from a previous run where activity was scheduled but not awaited
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // from schedule_activity() call
    ];
    let mut engine = create_engine(history);

    // Handler returns immediately without scheduling anything
    // This simulates a replay where the original code scheduled but didn't await
    let result = execute(&mut engine, ImmediateHandler::ok("done"));

    // Completes successfully - unconsumed schedule events are valid
    // (the original orchestration may have scheduled without awaiting)
    assert_completed(&result, "done");
}

/// Completion message for non-existent schedule - should fail with nondeterminism.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     Ok("done".to_string())  // No activities scheduled
/// }
/// ```
///
/// But a completion arrives for activity id=999 which was never scheduled.
#[test]
fn completion_without_schedule() {
    let history = vec![started_event(1)]; // OrchestrationStarted - nothing scheduled
    let mut engine = create_engine(history);

    // Send completion for activity that was never scheduled
    engine.prep_completions(vec![activity_completed_msg(999, "result")]);

    // prep_completions should set abort_error
    let result = execute(&mut engine, ImmediateHandler::ok("done"));

    assert_nondeterminism(&result);
}

/// Completion kind mismatch - activity completion for timer schedule.
///
/// Orchestration scheduled a timer, but an ActivityCompleted message arrives
/// for that same id. This indicates data corruption or bug.
#[test]
fn completion_kind_mismatch() {
    let history = vec![
        started_event(1),       // OrchestrationStarted
        timer_created(2, 1000), // Scheduled a timer, not an activity
    ];
    let mut engine = create_engine(history);

    // Send activity completion for timer schedule
    engine.prep_completions(vec![activity_completed_msg(2, "result")]);

    let result = execute(&mut engine, SingleTimerHandler::new(std::time::Duration::from_secs(60)));

    // Should fail - completion kind doesn't match schedule kind
    assert_nondeterminism(&result);
}
/// Timer completion for activity schedule - should fail.
///
/// TimerFired completion arrives but the schedule at that id is an activity,
/// not a timer. Tests prep_completions nondeterminism detection.
#[test]
fn timer_completion_for_activity_schedule() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // Scheduled an activity, not a timer
    ];
    let mut engine = create_engine(history);

    // Send timer fired for activity schedule id
    engine.prep_completions(vec![timer_fired_msg(2, 1000)]);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    // Should fail - timer completion for activity schedule
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "completion kind mismatch");
}

/// Sub-orchestration completion for activity schedule - should fail.
///
/// SubOrchCompleted arrives but the schedule at that id is an activity,
/// not a sub-orchestration. Tests prep_completions nondeterminism detection.
#[test]
fn sub_orch_completion_for_activity_schedule() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // Scheduled an activity, not a sub-orch
    ];
    let mut engine = create_engine(history);

    // Send sub-orch completion for activity schedule id
    engine.prep_completions(vec![sub_orch_completed_msg(2, "result")]);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    // Should fail - sub-orch completion for activity schedule
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "completion kind mismatch");
}

/// Sub-orchestration completion for timer schedule - should fail.
///
/// SubOrchCompleted arrives for timer id. Tests prep_completions nondeterminism.
#[test]
fn sub_orch_completion_for_timer_schedule() {
    let history = vec![
        started_event(1),       // OrchestrationStarted
        timer_created(2, 1000), // Scheduled a timer, not a sub-orch
    ];
    let mut engine = create_engine(history);

    // Send sub-orch completion for timer schedule id
    engine.prep_completions(vec![sub_orch_completed_msg(2, "result")]);

    let result = execute(&mut engine, SingleTimerHandler::new(std::time::Duration::from_secs(60)));

    // Should fail - sub-orch completion for timer schedule
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "completion kind mismatch");
}

/// Timer completion for sub-orchestration schedule - should fail.
///
/// TimerFired arrives but the schedule at that id is a sub-orchestration.
#[test]
fn timer_completion_for_sub_orch_schedule() {
    let history = vec![
        started_event(1),                                          // OrchestrationStarted
        sub_orch_scheduled(2, "ChildOrch", "child-inst", "input"), // Scheduled a sub-orch
    ];
    let mut engine = create_engine(history);

    // Send timer fired for sub-orch schedule id
    engine.prep_completions(vec![timer_fired_msg(2, 1000)]);

    let result = execute(&mut engine, SubOrchHandler::new("ChildOrch", "input"));

    // Should fail - timer completion for sub-orch schedule
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "completion kind mismatch");
}

/// OrchestrationChained in history but handler doesn't emit matching action.
///
/// History has OrchestrationChained event but the current code doesn't emit
/// a matching StartOrchestrationDetached action.
///
/// To trigger this path, the handler must SUSPEND (not return immediately) so that
/// the OrchestrationChained event is actually processed. If the handler returns
/// immediately, trailing events are ignored by design.
#[test]
fn orchestration_chained_no_matching_action() {
    // History: started an activity, then started a detached orchestration
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"), // Handler schedules this
        orchestration_chained(3, "ChildOrch", "child-inst", "input"), // Fire-and-forget child
    ];
    let mut engine = create_engine(history);

    // Handler schedules an activity (which suspends) but NOT the detached orchestration
    // The activity matches, but when we try to process OrchestrationChained, there's no action
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    // Should fail - history has OrchestrationChained but no emitted action
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "OrchestrationChained");
}

/// OrchestrationChained action mismatch - different orchestration name.
///
/// History has OrchestrationChained for "ChildA", code emits for "ChildB".
/// To trigger mismatch detection, handler must suspend after starting detached orch.
#[test]
fn orchestration_chained_name_mismatch() {
    // History: started a detached orch "ChildA", then scheduled an activity
    let history = vec![
        started_event(1),
        orchestration_chained(2, "ChildA", "child-inst", "input"), // Original: ChildA
        activity_scheduled(3, "Task", "work"),                     // Then scheduled activity
    ];
    let mut engine = create_engine(history);

    // Handler starts a DIFFERENT detached orch (ChildB) then schedules activity (which suspends)
    let result = execute(
        &mut engine,
        DetachedThenActivityHandler::new("ChildB", "child-inst", "input", "Task", "work"),
    );

    // Should fail - OrchestrationChained name mismatch
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "schedule mismatch");
}

/// Activity completion for timer schedule during history replay.
///
/// During history processing (not prep_completions), an ActivityCompleted
/// event references a schedule that is not an activity.
#[test]
fn activity_completed_for_timer_in_history() {
    let history = vec![
        started_event(1),
        timer_created(2, 1000),            // Timer schedule
        activity_completed(3, 2, "wrong"), // Activity completion for timer id - corruption!
    ];
    let mut engine = create_engine(history);

    let result = execute(&mut engine, SingleTimerHandler::new(std::time::Duration::from_secs(60)));

    // Should fail - activity completion for timer schedule
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "completion kind mismatch");
}

/// Timer completion for already-completed timer.
///
/// TimerFired event in history references a timer that was already completed
/// (schedule is not in open_schedules).
///
/// The handler must suspend AFTER the first timer completes so that the
/// duplicate completion is processed. We do this by scheduling a second
/// timer that never fires.
#[test]
fn timer_completion_for_closed_schedule() {
    let history = vec![
        started_event(1),
        timer_created(2, 1000),
        timer_fired(3, 2, 1000), // Timer completed
        timer_created(4, 2000),  // Second timer (keeps handler suspended)
        timer_fired(5, 2, 1000), // Duplicate completion for first timer!
    ];
    let mut engine = create_engine(history);

    // Handler schedules two timers sequentially
    let result = execute(
        &mut engine,
        TwoTimersHandler::new(std::time::Duration::from_secs(1), std::time::Duration::from_secs(2)),
    );

    // Should fail - completion without open schedule
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "completion without open schedule");
}

/// Sub-orchestration completion for already-completed sub-orch.
///
/// SubOrchestrationCompleted event in history references a sub-orch that
/// was already completed.
///
/// The handler must suspend AFTER the first sub-orch completes so that the
/// duplicate completion is processed. We do this by scheduling a second
/// activity that never completes.
#[test]
fn sub_orch_completion_for_closed_schedule() {
    let history = vec![
        started_event(1),
        sub_orch_scheduled(2, "Child", "child-inst", "input"),
        sub_orch_completed(3, 2, "first"),        // Sub-orch completed
        activity_scheduled(4, "Pending", "work"), // Second work (keeps handler suspended)
        sub_orch_completed(5, 2, "second"),       // Duplicate completion!
    ];
    let mut engine = create_engine(history);

    // Handler schedules sub-orch then activity sequentially
    let result = execute(
        &mut engine,
        SubOrchThenActivityHandler::new("Child", "input", "Pending", "work"),
    );

    // Should fail - completion without open schedule
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "completion without open schedule");
}

/// Activity completion for already-completed activity.
///
/// ActivityCompleted event references an activity that was already completed.
///
/// The handler must suspend AFTER the first activity completes so that the
/// duplicate completion is processed.
#[test]
fn activity_completion_for_closed_schedule() {
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        activity_completed(3, 2, "first"),        // Activity completed
        activity_scheduled(4, "Pending", "work"), // Second activity (keeps handler suspended)
        activity_completed(5, 2, "second"),       // Duplicate completion for first activity!
    ];
    let mut engine = create_engine(history);

    // Handler schedules two activities sequentially
    let result = execute(
        &mut engine,
        TwoActivitiesHandler::new(("Task", "input"), ("Pending", "work")),
    );

    // Should fail - completion without open schedule
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "completion without open schedule");
}

/// ActivityFailed for already-completed activity.
///
/// ActivityFailed event references an activity that was already completed
/// (tests the Failed variant, not Completed).
#[test]
fn activity_failed_for_closed_schedule() {
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        activity_completed(3, 2, "first"),        // Activity completed
        activity_scheduled(4, "Pending", "work"), // Second activity (keeps handler suspended)
        activity_failed(5, 2, "duplicate error"), // Duplicate failure for first activity!
    ];
    let mut engine = create_engine(history);

    let result = execute(
        &mut engine,
        TwoActivitiesHandler::new(("Task", "input"), ("Pending", "work")),
    );

    // Should fail - completion without open schedule
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "completion without open schedule");
}

/// SubOrchestrationFailed for already-completed sub-orch.
///
/// SubOrchestrationFailed event references a sub-orch that was already
/// completed (tests the Failed variant, not Completed).
#[test]
fn sub_orch_failed_for_closed_schedule() {
    let history = vec![
        started_event(1),
        sub_orch_scheduled(2, "Child", "child-inst", "input"),
        sub_orch_completed(3, 2, "first"),        // Sub-orch completed
        activity_scheduled(4, "Pending", "work"), // Activity keeps handler suspended
        sub_orch_failed(5, 2, "duplicate error"), // Duplicate failure!
    ];
    let mut engine = create_engine(history);

    let result = execute(
        &mut engine,
        SubOrchThenActivityHandler::new("Child", "input", "Pending", "work"),
    );

    // Should fail - completion without open schedule
    assert_nondeterminism(&result);
    assert_failed_with_message(&result, "completion without open schedule");
}

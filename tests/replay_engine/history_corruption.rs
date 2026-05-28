// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! History Corruption Tests
//!
//! Tests for invalid history detection and graceful handling.

use super::helpers::*;

// ============================================================================
// 8.1 Structural Corruption
// ============================================================================

/// Empty history should fail - every orchestration needs OrchestrationStarted.
#[test]
fn empty_history() {
    let mut engine = create_engine(vec![]); // No events at all
    let result = execute(&mut engine, ImmediateHandler::ok("done"));

    assert_failed_with_message(&result, "empty");
}

/// Missing OrchestrationStarted event - history starts with activity.
#[test]
fn missing_started_event() {
    let history = vec![
        activity_scheduled(1, "Task", "input"), // No OrchestrationStarted!
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, ImmediateHandler::ok("done"));

    assert_failed_with_message(&result, "OrchestrationStarted");
}

/// OrchestrationStarted not first - activity before started event.
#[test]
fn started_not_first() {
    let history = vec![
        activity_scheduled(1, "Task", "input"), // Wrong order!
        started_event(2),                       // Should be first
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, ImmediateHandler::ok("done"));

    assert_failed_with_message(&result, "OrchestrationStarted");
}

/// Event ID zero should work (0 is valid).
#[test]
fn event_id_zero() {
    let history = vec![started_event(0)]; // id=0 is allowed
    let mut engine = create_engine(history);
    let result = execute(&mut engine, ImmediateHandler::ok("done"));

    assert_completed(&result, "done");
}

/// Event ID gaps should work - IDs don't need to be contiguous.
#[test]
fn event_id_gap() {
    let history = vec![
        started_event(1),                         // id=1
        activity_scheduled(100, "Task", "input"), // id=100 (big gap)
        activity_completed(200, 100, "result"),   // id=200
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    assert_completed(&result, "result");
}

/// Non-monotonic event IDs - engine should allocate next_id from max.
#[test]
fn non_monotonic_event_ids() {
    let history = vec![
        started_event(1),                       // id=1
        activity_scheduled(5, "Task", "input"), // id=5
        activity_completed(3, 5, "result"),     // id=3 < 5, but still valid
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    // Should work - engine finds max event_id
    assert_completed(&result, "result");
}

// ============================================================================
// 8.2 Completion Linkage Corruption
// ============================================================================

/// Completion with source_id pointing to non-existent schedule.
#[test]
fn completion_source_not_found() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // Schedule at id=2
    ];
    let mut engine = create_engine(history);

    // Send completion for id=99 which doesn't exist
    engine.prep_completions(vec![activity_completed_msg(99, "result")]);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    // Should fail with nondeterminism
    assert_nondeterminism(&result);
}

/// Completion kind mismatch - activity completion for timer schedule.
#[test]
fn completion_source_wrong_type() {
    let history = vec![
        started_event(1),       // OrchestrationStarted
        timer_created(2, 1000), // Timer, not activity
    ];
    let mut engine = create_engine(history);

    // Send activity completion for timer id
    engine.prep_completions(vec![activity_completed_msg(2, "result")]);

    let result = execute(&mut engine, SingleTimerHandler::new(std::time::Duration::from_secs(60)));

    assert_nondeterminism(&result);
}

/// Multiple completions for same source in history - second should be ignored.
#[test]
fn multiple_completions_same_source_in_history() {
    let history = vec![
        started_event(1),                         // OrchestrationStarted
        activity_scheduled(2, "Task", "input"),   // Schedule
        activity_completed(3, 2, "first-result"), // Already completed
    ];
    let mut engine = create_engine(history);

    // Try to send another completion for same source
    engine.prep_completions(vec![activity_completed_msg(2, "second-result")]);

    // Should be filtered as duplicate
    assert!(engine.history_delta().is_empty());
}

/// Completion from different execution_id - stale completion from previous run.
#[test]
fn completion_for_different_execution() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // Current execution
    ];
    let mut engine = create_engine(history);

    // Send completion with wrong execution_id (from previous execution)
    let msg = duroxide::providers::WorkItem::ActivityCompleted {
        instance: TEST_INSTANCE.to_string(),
        execution_id: 999, // Wrong - stale execution
        id: 2,
        result: "result".to_string(),
    };
    engine.prep_completions(vec![msg]);

    // Should be filtered
    assert!(engine.history_delta().is_empty());
}

// ============================================================================
// 8.3 Terminal Event Corruption
// ============================================================================

/// History with OrchestrationCompleted - orchestration is already done.
#[test]
fn terminal_completed() {
    let history = vec![
        started_event(1),                   // OrchestrationStarted
        orchestration_completed(2, "done"), // Already terminal
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, ImmediateHandler::ok("new result"));

    // Already terminal - should be no-op
    assert_continue(&result);
}

/// History with OrchestrationFailed - orchestration is already failed.
#[test]
fn terminal_failed() {
    let history = vec![
        started_event(1),                 // OrchestrationStarted
        orchestration_failed(2, "error"), // Already terminal
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, ImmediateHandler::ok("new result"));

    assert_continue(&result);
}

/// History with OrchestrationContinuedAsNew - this execution is done.
#[test]
fn terminal_continued_as_new() {
    let history = vec![
        started_event(1),                               // OrchestrationStarted
        orchestration_continued_as_new(2, "new input"), // Already continued
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, ImmediateHandler::ok("new result"));

    assert_continue(&result);
}

/// Multiple terminal events - first wins, rest are no-op.
#[test]
fn multiple_terminal_events() {
    let history = vec![
        started_event(1),                    // OrchestrationStarted
        orchestration_completed(2, "first"), // First terminal
        orchestration_failed(3, "second"),   // Should be ignored
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, ImmediateHandler::ok("new result"));

    // Any terminal present means no-op
    assert_continue(&result);
}

/// Events after terminal - should be no-op (terminal already reached).
#[test]
fn events_after_terminal() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        orchestration_completed(2, "done"),     // Terminal
        activity_scheduled(3, "Task", "input"), // After terminal (corruption)
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, ImmediateHandler::ok("new result"));

    assert_continue(&result);
}

/// Cancel after completed - should be no-op (already terminal).
#[test]
fn cancel_after_completed() {
    let history = vec![
        started_event(1),                   // OrchestrationStarted
        orchestration_completed(2, "done"), // Terminal
        cancel_requested(3, "too late"),    // Cancel after terminal (irrelevant)
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, ImmediateHandler::ok("new result"));

    // Already completed, cancel is irrelevant
    assert_continue(&result);
}

// ============================================================================
// 8.4 Execution ID Validation
// ============================================================================

/// Completion with wrong execution_id is filtered.
#[test]
fn completion_wrong_execution() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // Current execution
    ];
    let mut engine = create_engine(history);

    // Completion from stale execution_id
    let msg = duroxide::providers::WorkItem::ActivityCompleted {
        instance: TEST_INSTANCE.to_string(),
        execution_id: 999, // Wrong execution
        id: 2,
        result: "result".to_string(),
    };
    engine.prep_completions(vec![msg]);

    assert!(engine.history_delta().is_empty());
}

// ============================================================================
// 8.5 Field Value Edge Cases
// ============================================================================

/// Empty activity name should work.
#[test]
fn empty_activity_name() {
    let history = vec![
        started_event(1),                   // OrchestrationStarted
        activity_scheduled(2, "", "input"), // Empty name is valid
        activity_completed(3, 2, "result"),
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new("", "input"));

    assert_completed(&result, "result");
}

/// Unicode in activity name should work.
#[test]
fn unicode_in_names() {
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Grüß Gott 🎉", "日本語入力"), // Unicode name & input
        activity_completed(3, 2, "emoji result 👍"),         // Unicode result
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new("Grüß Gott 🎉", "日本語入力"));

    assert_completed(&result, "emoji result 👍");
}

/// Very long strings should work (no arbitrary limits).
#[test]
fn very_long_strings() {
    let long_name = "x".repeat(10000); // 10KB name
    let long_input = "y".repeat(10000); // 10KB input
    let long_result = "z".repeat(10000); // 10KB result

    let history = vec![
        started_event(1),
        activity_scheduled(2, &long_name, &long_input),
        activity_completed(3, 2, &long_result),
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new(&long_name, &long_input));

    assert_completed(&result, &long_result);
}

/// Special characters (newlines, tabs, null) should work.
#[test]
fn special_characters() {
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Name\nWith\tSpecial", "Input\r\nCRLF"), // Special chars
        activity_completed(3, 2, "Result\0WithNull"),                  // Null byte
    ];
    let mut engine = create_engine(history);
    let result = execute(
        &mut engine,
        SingleActivityHandler::new("Name\nWith\tSpecial", "Input\r\nCRLF"),
    );

    assert_completed(&result, "Result\0WithNull");
}

// ============================================================================
// 8.6 Event Order Corruption
// ============================================================================

/// Timer fired before created (in history) - should fail.
#[test]
fn timer_fired_before_created() {
    let history = vec![
        started_event(1),
        timer_fired(2, 3, 1000), // References timer at id=3 (doesn't exist yet!)
        timer_created(3, 1000),  // Timer created after fired
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleTimerHandler::new(std::time::Duration::from_secs(60)));

    // Timer fired references a schedule that hasn't been processed yet
    // This should cause nondeterminism
    assert_nondeterminism(&result);
}

/// Activity completed before scheduled (in history) - should fail.
#[test]
fn activity_completed_before_scheduled() {
    let history = vec![
        started_event(1),
        activity_completed(2, 3, "result"), // References activity at id=3 (doesn't exist yet!)
        activity_scheduled(3, "Task", "input"), // Scheduled after completed
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    assert_nondeterminism(&result);
}

// ============================================================================
// 8.7 Orphaned Events
// ============================================================================

/// Orphan completion with no schedule in prep_completions.
#[test]
fn orphan_completion_no_schedule() {
    let history = vec![started_event(1)]; // No activities scheduled
    let mut engine = create_engine(history);

    // Send completion for non-existent activity
    engine.prep_completions(vec![activity_completed_msg(99, "orphan")]);

    // Should set abort_error
    let result = execute(&mut engine, ImmediateHandler::ok("done"));
    assert_nondeterminism(&result);
}

/// Orphan timer fired - no timer was created.
#[test]
fn orphan_timer_fired() {
    let history = vec![started_event(1)]; // No timers created
    let mut engine = create_engine(history);

    // Send timer fired for non-existent timer
    engine.prep_completions(vec![timer_fired_msg(99, 1000)]);

    let result = execute(&mut engine, ImmediateHandler::ok("done"));
    assert_nondeterminism(&result);
}

/// Orphan external event (no subscription) is materialized unconditionally.
/// The replay loop's causal check skips delivery, but the event remains in history.
#[test]
fn orphan_external_event() {
    let history = vec![started_event(1)]; // No subscriptions
    let mut engine = create_engine(history);

    // Send external event with no matching subscription
    engine.prep_completions(vec![external_raised_msg("UnknownEvent", "data")]);

    // External events are materialized unconditionally (causal check is in replay loop)
    assert_eq!(engine.history_delta().len(), 1);

    let result = execute(&mut engine, ImmediateHandler::ok("done"));
    assert_completed(&result, "done");
}

/// Orphan sub-orchestration completed - no sub-orch was scheduled.
#[test]
fn orphan_sub_orch_completed() {
    let history = vec![started_event(1)]; // No sub-orchestrations scheduled
    let mut engine = create_engine(history);

    // Send sub-orch completed for non-existent sub-orch
    engine.prep_completions(vec![sub_orch_completed_msg(99, "result")]);

    let result = execute(&mut engine, ImmediateHandler::ok("done"));
    assert_nondeterminism(&result);
}

// ============================================================================
// 8.8 Duplicate Schedule Events
// ============================================================================

/// Duplicate activity schedule in history - second causes mismatch.
#[test]
fn duplicate_activity_schedule_in_history() {
    let history = vec![
        started_event(1),
        activity_scheduled(2, "A", "input"), // First schedule
        activity_scheduled(3, "A", "input"), // Duplicate schedule
    ];
    let mut engine = create_engine(history);

    // Handler only schedules one activity
    let result = execute(&mut engine, SingleActivityHandler::new("A", "input"));

    // First schedule matches, but second has no matching emitted action
    assert_nondeterminism(&result);
}

/// Duplicate timer in history - second causes mismatch.
#[test]
fn duplicate_timer_schedule_in_history() {
    let history = vec![
        started_event(1),
        timer_created(2, 1000), // First timer
        timer_created(3, 1000), // Duplicate timer
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleTimerHandler::new(std::time::Duration::from_secs(60)));

    // First timer matches, second has no emitted action
    assert_nondeterminism(&result);
}

/// Duplicate external subscription in history - second causes mismatch.
#[test]
fn duplicate_external_subscription_in_history() {
    let history = vec![
        started_event(1),
        external_subscribed(2, "E"), // First subscription
        external_subscribed(3, "E"), // Duplicate subscription
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, WaitExternalHandler::new("E"));

    assert_nondeterminism(&result);
}

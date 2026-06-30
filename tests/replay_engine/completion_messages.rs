// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Completion Message Processing Tests
//!
//! Tests for WorkItem → Event conversion and filtering via prep_completions.

use super::helpers::*;

/// Activity completed message gets converted to history delta.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     Ok(result)
/// }
/// ```
///
/// This test verifies prep_completions converts WorkItem::ActivityCompleted to Event.
#[test]
fn activity_completed_message() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // schedule_activity()
    ];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![activity_completed_msg(2, "result")]);

    assert_eq!(engine.history_delta().len(), 1);
    assert!(matches!(
        &engine.history_delta()[0].kind,
        duroxide::EventKind::ActivityCompleted { result } if result == "result"
    ));
}

/// Timer fired message gets converted to history delta.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     ctx.schedule_timer(Duration::from_secs(60)).await;
///     Ok("done".to_string())
/// }
/// ```
///
/// This test verifies prep_completions converts WorkItem::DurableTimer to Event.
#[test]
fn timer_fired_message() {
    let history = vec![
        started_event(1),       // OrchestrationStarted
        timer_created(2, 1000), // schedule_timer()
    ];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![timer_fired_msg(2, 1000)]);

    assert_eq!(engine.history_delta().len(), 1);
    assert!(matches!(
        &engine.history_delta()[0].kind,
        duroxide::EventKind::TimerFired { .. }
    ));
}

/// External raised message gets converted to history delta.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let data = ctx.schedule_wait("MyEvent").await;
///     Ok(data)
/// }
/// ```
///
/// This test verifies prep_completions converts WorkItem::ExternalEvent to Event.
#[test]
fn external_raised_message() {
    let history = vec![
        started_event(1),                  // OrchestrationStarted
        external_subscribed(2, "MyEvent"), // schedule_wait()
    ];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![external_raised_msg("MyEvent", "event-data")]);

    assert_eq!(engine.history_delta().len(), 1);
    assert!(matches!(
        &engine.history_delta()[0].kind,
        duroxide::EventKind::ExternalEvent { name, data } if name == "MyEvent" && data == "event-data"
    ));
}

/// Sub-orchestration completed message gets converted to history delta.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_sub_orchestration("Child", "input").await?;
///     Ok(result)
/// }
/// ```
///
/// This test verifies prep_completions converts WorkItem::SubOrchCompleted to Event.
#[test]
fn sub_orch_completed_message() {
    let history = vec![
        started_event(1),                                  // OrchestrationStarted
        sub_orch_scheduled(2, "Child", "sub::2", "input"), // schedule_sub_orchestration()
    ];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![sub_orch_completed_msg(2, "child-result")]);

    assert_eq!(engine.history_delta().len(), 1);
    assert!(matches!(
        &engine.history_delta()[0].kind,
        duroxide::EventKind::SubOrchestrationCompleted { result } if result == "child-result"
    ));
}

/// Duplicate completion (already in baseline history) is filtered.
///
/// If the same completion arrives twice (e.g., due to at-least-once delivery),
/// the second one should be ignored since it's already in history.
#[test]
fn duplicate_completion_filtered() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // schedule_activity()
        activity_completed(3, 2, "result"),     // already completed
    ];
    let mut engine = create_engine(history);

    // Send the same completion again
    engine.prep_completions(vec![activity_completed_msg(2, "result")]);

    assert!(engine.history_delta().is_empty(), "Duplicate should be filtered");
}

/// Duplicate in the same batch - only first should be added.
///
/// If two completions for the same source arrive in a single prep_completions call,
/// only the first is recorded.
#[test]
fn duplicate_in_same_batch() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // schedule_activity()
    ];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![
        activity_completed_msg(2, "first-result"),
        activity_completed_msg(2, "second-result"),
    ]);

    assert_eq!(engine.history_delta().len(), 1, "Only first should be added");
    assert!(matches!(
        &engine.history_delta()[0].kind,
        duroxide::EventKind::ActivityCompleted { result } if result == "first-result"
    ));
}

/// Completion with wrong execution_id is filtered.
///
/// Completions must match the current execution_id. This handles the case where
/// a stale completion from a previous execution arrives.
#[test]
fn wrong_execution_filtered() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // schedule_activity()
    ];
    let mut engine = create_engine(history);

    // Create a message with wrong execution_id
    let wrong_exec_msg = duroxide::providers::WorkItem::ActivityCompleted {
        instance: TEST_INSTANCE.to_string(),
        execution_id: 999, // Wrong execution_id
        id: 2,
        result: "result".to_string(),
    };

    engine.prep_completions(vec![wrong_exec_msg]);

    assert!(engine.history_delta().is_empty(), "Wrong execution should be filtered");
}

/// External event without subscription is materialized unconditionally.
///
/// All positional external events are materialized into history_delta for audit.
/// The causal check in the replay loop (execute_orchestration) skips delivery
/// when no pending subscription slot exists.
#[test]
fn external_without_subscription() {
    let history = vec![
        started_event(1), // OrchestrationStarted - no subscription
    ];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![external_raised_msg("UnknownEvent", "data")]);

    assert_eq!(
        engine.history_delta().len(),
        1,
        "External event should be materialized unconditionally (causal check is in replay loop)"
    );
}

/// Multiple completions in one batch.
///
/// prep_completions handles multiple different completions in a single call.
#[test]
fn multiple_completions_batch() {
    let history = vec![
        started_event(1),                // OrchestrationStarted
        activity_scheduled(2, "A", "a"), // 1st activity
        activity_scheduled(3, "B", "b"), // 2nd activity
    ];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![
        activity_completed_msg(2, "result-a"),
        activity_completed_msg(3, "result-b"),
    ]);

    assert_eq!(engine.history_delta().len(), 2, "Both completions should be added");
}

/// Duplicate external events in the same batch are NOT deduplicated.
///
/// Multiple ExternalRaised with the same name+data are separate arrivals by design.
/// Each one is materialized independently into history_delta.
#[test]
fn duplicate_external_events_in_batch_are_kept() {
    let history = vec![started_event(1), external_subscribed(2, "Evt")];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![
        external_raised_msg("Evt", "same-data"),
        external_raised_msg("Evt", "same-data"),
    ]);

    assert_eq!(
        engine.history_delta().len(),
        2,
        "Both external events should be materialized (duplicates are valid separate arrivals)"
    );
}

/// Duplicate external event already in baseline history is NOT deduplicated.
///
/// An ExternalRaised with the same name+data as an event already in baseline_history
/// is a new arrival and must be materialized.
#[test]
fn duplicate_external_event_in_history_is_kept() {
    let history = vec![
        started_event(1),
        external_subscribed(2, "Evt"),
        external_event(3, "Evt", "same-data"), // already in history
    ];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![external_raised_msg("Evt", "same-data")]);

    assert_eq!(
        engine.history_delta().len(),
        1,
        "External event should be materialized even if same name+data exists in history"
    );
}

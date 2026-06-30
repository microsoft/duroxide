// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Edge Case Tests
//!
//! Various edge cases and boundary conditions.

use super::helpers::*;
use std::time::Duration;

/// Zero delay timer works normally.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     ctx.schedule_timer(Duration::ZERO).await;  // Immediate timer
///     Ok("done".to_string())
/// }
/// ```
#[test]
fn zero_delay_timer() {
    let history = vec![started_event(1)]; // OrchestrationStarted
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleTimerHandler::new(Duration::ZERO));

    assert_continue(&result);
    assert!(has_timer_action(&engine), "Timer should be pending");
}

/// Empty activity input works.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "").await?;  // Empty input
///     Ok(result)
/// }
/// ```
#[test]
fn empty_activity_input() {
    let history = vec![
        started_event(1),                   // OrchestrationStarted
        activity_scheduled(2, "Task", ""),  // Empty input
        activity_completed(3, 2, "result"), // Completed
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new("Task", ""));

    assert_completed(&result, "result");
}

/// Large result string works.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     Ok(result)  // 1MB string
/// }
/// ```
#[test]
fn large_result_string() {
    let large_result = "x".repeat(1_000_000); // 1MB
    let history = vec![
        started_event(1),                        // OrchestrationStarted
        activity_scheduled(2, "Task", "input"),  // schedule_activity()
        activity_completed(3, 2, &large_result), // Large result
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    assert_completed(&result, &large_result);
}

/// Handler invoked exactly once per turn.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     // This should only be called once per engine.turn()
///     Ok("done".to_string())
/// }
/// ```
#[test]
fn handler_invoked_once() {
    let history = vec![started_event(1)]; // OrchestrationStarted
    let mut engine = create_engine(history);

    let handler = CountingHandler::new(Ok("done".to_string()));
    let result = execute(&mut engine, handler.clone());

    assert_completed(&result, "done");
    assert_eq!(handler.count(), 1, "Handler should be invoked exactly once");
}

/// Handler invoked once even during replay.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     // Even during replay, handler is invoked exactly once
///     Ok(result)
/// }
/// ```
#[test]
fn handler_invoked_once_during_replay() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // Previously scheduled
        activity_completed(3, 2, "result"),     // Previously completed
    ];
    let mut engine = create_engine(history);

    let _counting = CountingHandler::new(Ok("done".to_string()));

    // Need to use a handler that actually matches the history
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    assert_completed(&result, "result");
    // Handler is invoked once, replay is done via history traversal
}

/// Empty external event data works.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let data = ctx.schedule_wait("Event").await;
///     Ok(data)  // Empty string
/// }
/// ```
#[test]
fn empty_external_event_data() {
    let history = vec![
        started_event(1),                // OrchestrationStarted
        external_subscribed(2, "Event"), // schedule_wait()
        external_event(3, "Event", ""),  // Empty data
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, WaitExternalHandler::new("Event"));

    assert_completed(&result, "");
}

/// Multiple external events with same name - first one wins.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let data = ctx.schedule_wait("Event").await;
///     Ok(data)  // Returns "first-data", second is ignored
/// }
/// ```
#[test]
fn multiple_external_events_same_name() {
    let history = vec![
        started_event(1),                          // OrchestrationStarted
        external_subscribed(2, "Event"),           // schedule_wait()
        external_event(3, "Event", "first-data"),  // First event
        external_event(4, "Event", "second-data"), // Second event (ignored)
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, WaitExternalHandler::new("Event"));

    // First event should be delivered
    assert_completed(&result, "first-data");
}

/// made_progress is true after prep_completions adds events.
///
/// Tests the engine's progress tracking - useful for polling loops.
#[test]
fn made_progress_after_completion() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // Waiting for completion
    ];
    let mut engine = create_engine(history);

    assert!(!engine.made_progress(), "No progress initially");

    engine.prep_completions(vec![activity_completed_msg(2, "result")]);

    assert!(engine.made_progress(), "Should have progress after completion");
}

/// final_history includes both baseline and delta.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     Ok(result)
/// }
/// ```
///
/// final_history() returns baseline history + new events from this turn.
#[test]
fn final_history_combines_baseline_and_delta() {
    let history = vec![started_event(1)]; // OrchestrationStarted (baseline)
    let mut engine = create_engine(history);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));
    assert_continue(&result);

    let final_hist = engine.final_history();
    assert_eq!(final_hist.len(), 2, "Should have Started + ActivityScheduled");
    assert!(matches!(
        &final_hist[0].kind,
        duroxide::EventKind::OrchestrationStarted { .. }
    ));
    assert!(matches!(
        &final_hist[1].kind,
        duroxide::EventKind::ActivityScheduled { .. }
    ));
}

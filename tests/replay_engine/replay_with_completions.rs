// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Replay with Completions Tests
//!
//! Tests where the baseline history contains both schedule AND completion events.

use super::helpers::*;

/// Activity scheduled and completed in history - should complete.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Greet", &input).await?;
///     Ok(result)  // returns "Hello, Alice!" from completed activity
/// }
/// ```
#[test]
fn activity_completed_in_history() {
    // History from a completed execution:
    // Turn 1: Started, scheduled activity, yielded
    // Turn 2: Activity completed, orchestration returned result
    let history = vec![
        started_event(1),                          // OrchestrationStarted
        activity_scheduled(2, "Greet", "Alice"),   // schedule_activity() emitted
        activity_completed(3, 2, "Hello, Alice!"), // activity finished
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new("Greet", "Alice"));

    assert_completed(&result, "Hello, Alice!");
    assert!(engine.pending_actions().is_empty(), "No new actions expected");
}

/// Activity scheduled and failed in history - handler propagates error.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Greet", "Alice").await?;  // Returns Err
///     Ok(result)  // Never reached
/// }
/// ```
#[test]
fn activity_failed_in_history() {
    let history = vec![
        started_event(1),                             // OrchestrationStarted
        activity_scheduled(2, "Greet", "Alice"),      // schedule_activity() emitted
        activity_failed(3, 2, "service unavailable"), // activity failed
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new("Greet", "Alice"));

    // Handler awaits activity which returns Err, so handler returns Err
    assert_failed(&result);
}

/// Timer created and fired in history - should complete.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     ctx.schedule_timer(Duration::from_secs(60)).await;
///     Ok("timer_done".to_string())
/// }
/// ```
#[test]
fn timer_fired_in_history() {
    let history = vec![
        started_event(1),        // OrchestrationStarted
        timer_created(2, 1000),  // schedule_timer() emitted
        timer_fired(3, 2, 1000), // timer fired
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleTimerHandler::new(std::time::Duration::from_secs(60)));

    assert_completed(&result, "timer_done");
}

/// External subscription and event in history - should complete.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let data = ctx.schedule_wait("Approval").await;
///     Ok(data)  // returns "approved-data"
/// }
/// ```
#[test]
fn external_received_in_history() {
    let history = vec![
        started_event(1),                               // OrchestrationStarted
        external_subscribed(2, "Approval"),             // schedule_wait() emitted
        external_event(3, "Approval", "approved-data"), // external event raised
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, WaitExternalHandler::new("Approval"));

    assert_completed(&result, "approved-data");
}

/// Sub-orchestration scheduled and completed in history - should complete.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_sub_orchestration("ChildOrch", "child-input").await?;
///     Ok(result)  // returns "child-result"
/// }
/// ```
#[test]
fn sub_orch_completed_in_history() {
    let history = vec![
        started_event(1),                                            // OrchestrationStarted
        sub_orch_scheduled(2, "ChildOrch", "sub::2", "child-input"), // schedule_sub_orchestration()
        sub_orch_completed(3, 2, "child-result"),                    // sub-orchestration completed
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SubOrchHandler::new("ChildOrch", "child-input"));

    assert_completed(&result, "child-result");
}

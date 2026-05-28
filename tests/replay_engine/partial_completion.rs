// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Partial Completion Tests
//!
//! Tests where history has schedule events but completions haven't arrived yet.

use super::helpers::*;

/// Activity scheduled but not completed - should Continue with no new actions.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Greet", "Alice").await?;  // Still waiting...
///     Ok(result)
/// }
/// ```
#[test]
fn activity_scheduled_no_completion() {
    let history = vec![
        started_event(1),                        // OrchestrationStarted
        activity_scheduled(2, "Greet", "Alice"), // schedule_activity() - waiting for completion
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleActivityHandler::new("Greet", "Alice"));

    assert_continue(&result);
    assert!(
        engine.pending_actions().is_empty(),
        "No new actions expected (already scheduled)"
    );
}

/// Timer created but not fired - should Continue with no new actions.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     ctx.schedule_timer(Duration::from_secs(60)).await;  // Still waiting...
///     Ok("timer_done".to_string())
/// }
/// ```
#[test]
fn timer_created_no_fire() {
    let history = vec![
        started_event(1),       // OrchestrationStarted
        timer_created(2, 1000), // schedule_timer() - waiting for fire
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SingleTimerHandler::new(std::time::Duration::from_secs(60)));

    assert_continue(&result);
    assert!(
        engine.pending_actions().is_empty(),
        "No new actions expected (already scheduled)"
    );
}

/// External subscribed but no event - should Continue with no new actions.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let data = ctx.schedule_wait("Approval").await;  // Still waiting...
///     Ok(data)
/// }
/// ```
#[test]
fn external_subscribed_no_event() {
    let history = vec![
        started_event(1),                   // OrchestrationStarted
        external_subscribed(2, "Approval"), // schedule_wait() - waiting for event
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, WaitExternalHandler::new("Approval"));

    assert_continue(&result);
    assert!(
        engine.pending_actions().is_empty(),
        "No new actions expected (already subscribed)"
    );
}

/// Sub-orchestration scheduled but not completed - should Continue with no new actions.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_sub_orchestration("ChildOrch", "child-input").await?;  // Still waiting...
///     Ok(result)
/// }
/// ```
#[test]
fn sub_orch_scheduled_no_completion() {
    let history = vec![
        started_event(1),                                            // OrchestrationStarted
        sub_orch_scheduled(2, "ChildOrch", "sub::2", "child-input"), // schedule_sub_orchestration() - waiting
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SubOrchHandler::new("ChildOrch", "child-input"));

    assert_continue(&result);
    assert!(
        engine.pending_actions().is_empty(),
        "No new actions expected (already scheduled)"
    );
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Cancellation Tests
//!
//! Tests for OrchestrationCancelRequested handling.

use super::helpers::*;

/// Cancel via message - should yield Cancelled.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     Ok("success".to_string())  // Would complete, but cancel takes precedence
/// }
/// ```
///
/// External cancel request arrives via prep_completions.
#[test]
fn cancel_via_message() {
    let history = vec![started_event(1)]; // OrchestrationStarted
    let mut engine = create_engine(history);

    engine.prep_completions(vec![cancel_instance_msg("user requested")]);

    let result = execute(&mut engine, ImmediateHandler::ok("success"));

    assert_cancelled_with_reason(&result, "user requested");
}

/// Cancel in history - should yield Cancelled.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     Ok("success".to_string())  // Would complete, but was cancelled
/// }
/// ```
///
/// OrchestrationCancelRequested already exists in persisted history.
#[test]
fn cancel_in_history() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        cancel_requested(2, "admin cancelled"), // Cancel was recorded
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, ImmediateHandler::ok("success"));

    assert_cancelled_with_reason(&result, "admin cancelled");
}

/// Cancel takes precedence over completion.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     Ok("would have completed".to_string())
/// }
/// ```
///
/// Even if handler returns Ok, cancel request wins.
#[test]
fn cancel_precedence_over_completion() {
    let history = vec![started_event(1)]; // OrchestrationStarted
    let mut engine = create_engine(history);

    engine.prep_completions(vec![cancel_instance_msg("cancelled")]);

    // Handler would return Ok, but cancel takes precedence
    let result = execute(&mut engine, ImmediateHandler::ok("would have completed"));

    assert_cancelled(&result);
}

/// Cancel with pending activity - should yield Cancelled.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;  // Waiting...
///     Ok(result)
/// }
/// ```
///
/// Cancel arrives while activity is pending.
#[test]
fn cancel_with_pending_activity() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // Activity waiting for completion
    ];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![cancel_instance_msg("cancelled")]);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    assert_cancelled(&result);
}

/// Duplicate cancel messages - only one cancel event recorded.
///
/// Once cancellation is recorded, subsequent cancel requests are ignored.
#[test]
fn cancel_duplicate_filtered() {
    let history = vec![
        started_event(1),                    // OrchestrationStarted
        cancel_requested(2, "first cancel"), // Already cancelled
    ];
    let mut engine = create_engine(history);

    // Send another cancel
    engine.prep_completions(vec![cancel_instance_msg("second cancel")]);

    // Should be filtered - cancel already in history
    assert!(engine.history_delta().is_empty(), "Duplicate cancel should be filtered");
}

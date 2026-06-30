// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Activity Failure Handling Tests
//!
//! Tests for different error categories in activity failures.

use super::helpers::*;

/// Application error propagates to handler.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     // Activity returns Err("business rule violated") - propagates via ?
///     Ok(result)  // Never reached
/// }
/// ```
#[test]
fn application_error_propagates() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // schedule_activity()
    ];
    let mut engine = create_engine(history);

    // Send application-level failure
    engine.prep_completions(vec![activity_failed_msg(2, "business rule violated")]);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    // Handler awaits activity which returns Err, so handler returns Err (application)
    assert_failed(&result);
    match &result {
        duroxide::runtime::replay_engine::TurnResult::Failed(details) => {
            assert!(matches!(details, duroxide::ErrorDetails::Application { .. }));
        }
        _ => panic!("Expected application failure"),
    }
}

/// Infrastructure error aborts turn immediately.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     // Activity worker lost database connection - infrastructure failure
///     Ok(result)  // Never reached - turn aborts
/// }
/// ```
#[test]
fn infrastructure_error_aborts() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // schedule_activity()
    ];
    let mut engine = create_engine(history);

    // Send infrastructure-level failure
    engine.prep_completions(vec![activity_failed_infra_msg(2, "database connection lost")]);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    // Infrastructure error aborts - doesn't reach handler
    assert_failed(&result);
    match &result {
        duroxide::runtime::replay_engine::TurnResult::Failed(details) => {
            assert!(matches!(details, duroxide::ErrorDetails::Infrastructure { .. }));
        }
        _ => panic!("Expected infrastructure failure"),
    }
}

/// Configuration error aborts turn immediately.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     // Activity "Task" not registered - configuration error
///     Ok(result)  // Never reached - turn aborts
/// }
/// ```
#[test]
fn configuration_error_aborts() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // schedule_activity()
    ];
    let mut engine = create_engine(history);

    // Send configuration-level failure
    engine.prep_completions(vec![activity_failed_config_msg(2, "activity not registered")]);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    // Configuration error aborts
    assert_failed(&result);
    match &result {
        duroxide::runtime::replay_engine::TurnResult::Failed(details) => {
            assert!(matches!(details, duroxide::ErrorDetails::Configuration { .. }));
        }
        _ => panic!("Expected configuration failure"),
    }
}

/// Sub-orchestration infrastructure error aborts parent.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_sub_orchestration("Child", "input").await?;
///     // Child had infrastructure failure - parent aborts
///     Ok(result)  // Never reached
/// }
/// ```
#[test]
fn sub_orch_infra_error_aborts_parent() {
    let history = vec![
        started_event(1),                                  // OrchestrationStarted
        sub_orch_scheduled(2, "Child", "sub::2", "input"), // schedule_sub_orchestration()
    ];
    let mut engine = create_engine(history);

    // Send infrastructure failure from child
    let msg = duroxide::providers::WorkItem::SubOrchFailed {
        parent_instance: TEST_INSTANCE.to_string(),
        parent_execution_id: TEST_EXECUTION_ID,
        parent_id: 2,
        details: duroxide::ErrorDetails::Infrastructure {
            operation: "child".to_string(),
            message: "child infra failure".to_string(),
            retryable: false,
        },
    };
    engine.prep_completions(vec![msg]);

    let result = execute(&mut engine, SubOrchHandler::new("Child", "input"));

    // Infrastructure error from child aborts parent
    assert_failed(&result);
}

/// Sub-orchestration application error propagates to handler
#[test]
fn sub_orch_app_error_propagates() {
    let history = vec![started_event(1), sub_orch_scheduled(2, "Child", "sub::2", "input")];
    let mut engine = create_engine(history);

    engine.prep_completions(vec![sub_orch_failed_msg(2, "child business error")]);

    let result = execute(&mut engine, SubOrchHandler::new("Child", "input"));

    // Application error propagates to handler
    assert_failed(&result);
}

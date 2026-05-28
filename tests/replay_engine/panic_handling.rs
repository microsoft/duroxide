// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Handler Panic Handling Tests
//!
//! Tests for orchestration panics.

use super::helpers::*;

/// Panic with string message.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     panic!("oops something went wrong");
/// }
/// ```
#[test]
fn panic_string() {
    let history = vec![started_event(1)]; // OrchestrationStarted
    let mut engine = create_engine(history);
    let result = execute(&mut engine, PanicHandler::new("oops something went wrong"));

    assert_panicked(&result);
    assert_failed_with_message(&result, "oops something went wrong");
}

/// Panic with non-string payload.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     panic!("{}", 42);  // Non-string panic
/// }
/// ```
#[test]
fn panic_other() {
    use async_trait::async_trait;
    use duroxide::{OrchestrationContext, OrchestrationHandler};
    use std::sync::Arc;

    struct PanicIntHandler;

    #[async_trait]
    impl OrchestrationHandler for PanicIntHandler {
        async fn invoke(&self, _ctx: OrchestrationContext, _input: String) -> Result<String, String> {
            panic!("{}", 42);
        }
    }

    let history = vec![started_event(1)];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, Arc::new(PanicIntHandler));

    assert_panicked(&result);
    // Non-string panics get a generic message
    assert_failed_with_message(&result, "42");
}

/// Panic during activity await.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let _ = ctx.schedule_activity("Task", "input").await;
///     panic!("panic after activity");  // Panics after activity completes
/// }
/// ```
#[test]
fn panic_during_await() {
    use async_trait::async_trait;
    use duroxide::{OrchestrationContext, OrchestrationHandler};
    use std::sync::Arc;

    struct PanicAfterScheduleHandler;

    #[async_trait]
    impl OrchestrationHandler for PanicAfterScheduleHandler {
        async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
            let _ = ctx.schedule_activity("Task", "input").await;
            panic!("panic after activity");
        }
    }

    // History has the activity scheduled and completed
    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "input"),
        activity_completed(3, 2, "result"),
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, Arc::new(PanicAfterScheduleHandler));

    assert_panicked(&result);
    assert_failed_with_message(&result, "panic after activity");
}

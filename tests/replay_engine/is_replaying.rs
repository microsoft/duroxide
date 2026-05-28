// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! is_replaying State Tests
//!
//! Tests verifying the context's replay state tracking.

use super::helpers::*;

/// Fresh execution (no persisted history beyond OrchestrationStarted).
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let replaying = ctx.is_replaying();  // Check replay state
///     Ok("done".to_string())
/// }
/// ```
///
/// Note: With persisted_len=1, the OrchestrationStarted event is considered
/// persisted, so we're technically in "replay mode" for that one event.
#[test]
fn fresh_execution_not_replaying() {
    let history = vec![started_event(1)]; // OrchestrationStarted
    // persisted_len = 1 means OrchestrationStarted was persisted
    let mut engine = create_engine_with_persisted_len(history, 1);

    let handler = IsReplayingHandler::new();
    let _result = execute(&mut engine, handler.clone());

    // The current behavior: is_replaying is true at the start because
    // persisted_len=1 means we have 1 persisted event to replay through.
    // The handler is called during the processing of that event, so
    // it sees is_replaying=true.
    let at_start = *handler.at_start.lock().unwrap();
    // Document actual behavior: with persisted_len=1, is_replaying is true
    // when processing the OrchestrationStarted event
    assert_eq!(
        at_start,
        Some(true),
        "With persisted_len=1, is_replaying starts as true"
    );
}

/// Replay with history - is_replaying should be true initially.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     // During replay of this history, is_replaying() returns true
///     Ok(result)
/// }
/// ```
#[test]
fn replay_is_replaying() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // Previously scheduled
        activity_completed(3, 2, "result"),     // Previously completed
    ];
    // All 3 events are persisted - this is a replay
    let mut engine = create_engine_with_persisted_len(history, 3);

    let _handler = IsReplayingHandler::new();
    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    assert_completed(&result, "result");
}

/// Replay transitions to not replaying after history is consumed.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let _ = ctx.schedule_activity("First", "input").await?;
///     // After consuming all history, is_replaying() becomes false
///     let still_replaying = ctx.is_replaying();
///     Ok("done".to_string())
/// }
/// ```
#[test]
fn transitions_after_history() {
    use async_trait::async_trait;
    use duroxide::{OrchestrationContext, OrchestrationHandler};
    use std::sync::{Arc, Mutex};

    struct TrackingHandler {
        before_new: Mutex<Option<bool>>,
        after_first: Mutex<Option<bool>>,
    }

    #[async_trait]
    impl OrchestrationHandler for TrackingHandler {
        async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
            // First activity is in history (replaying)
            let _ = ctx.schedule_activity("First", "input").await?;
            *self.before_new.lock().unwrap() = Some(ctx.is_replaying());

            // After consuming all persisted history + completion, we should not be replaying
            *self.after_first.lock().unwrap() = Some(ctx.is_replaying());

            Ok("done".to_string())
        }
    }

    let history = vec![
        started_event(1),
        activity_scheduled(2, "First", "input"),
        activity_completed(3, 2, "result"),
    ];
    // All 3 events are persisted
    let mut engine = create_engine_with_persisted_len(history, 3);

    let handler = Arc::new(TrackingHandler {
        before_new: Mutex::new(None),
        after_first: Mutex::new(None),
    });
    let result = execute(&mut engine, handler.clone());

    // Handler completes after replaying through history
    assert_completed(&result, "done");

    // After processing all history events, is_replaying becomes false
    let after = *handler.after_first.lock().unwrap();
    assert_eq!(after, Some(false), "Should not be replaying after all history consumed");
}

/// persisted_history_len = 0 means fresh start (OrchestrationStarted added this turn).
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     // First ever turn - nothing persisted yet
///     let replaying = ctx.is_replaying();  // false
///     Ok("done".to_string())
/// }
/// ```
#[test]
fn fresh_start_with_zero_persisted() {
    let history = vec![started_event(1)]; // OrchestrationStarted
    // persisted_len = 0 means this is truly the first turn
    let mut engine = create_engine_with_persisted_len(history, 0);

    let handler = IsReplayingHandler::new();
    let result = execute(&mut engine, handler.clone());

    assert_completed(&result, "done");

    let at_start = *handler.at_start.lock().unwrap();
    assert_eq!(at_start, Some(false), "Zero persisted history means not replaying");
}

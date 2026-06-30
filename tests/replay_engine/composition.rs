// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Select/Join Composition Tests
//!
//! Tests for aggregate future behavior (select, join).

use super::helpers::*;
use async_trait::async_trait;
use duroxide::{OrchestrationContext, OrchestrationHandler};
use std::sync::Arc;
use std::time::Duration;

struct LargeSubOrchestrationFanInHandler {
    count: usize,
}

impl LargeSubOrchestrationFanInHandler {
    fn new(count: usize) -> Arc<Self> {
        Arc::new(Self { count })
    }
}

#[async_trait]
impl OrchestrationHandler for LargeSubOrchestrationFanInHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let futures = (0..self.count)
            .map(|idx| ctx.schedule_sub_orchestration_with_id("Child", format!("child-{idx}"), idx.to_string()))
            .collect::<Vec<_>>();

        let results = ctx.join(futures).await;
        let completed = results.into_iter().collect::<Result<Vec<_>, _>>()?;
        for (idx, result) in completed.iter().enumerate() {
            if result != &idx.to_string() {
                return Err(format!("result at index {idx} was {result}"));
            }
        }

        Ok(completed.len().to_string())
    }
}

/// select2 where activity wins (completes before timer).
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let activity = ctx.schedule_activity("Task", "input");
///     let timer = ctx.schedule_timer(Duration::from_secs(60));
///     match ctx.select2(activity, timer).await {
///         Either2::First(result) => Ok(format!("activity:{}", result?)),
///         Either2::Second(()) => Ok("timeout".to_string()),
///     }
/// }
/// ```
#[test]
fn select2_activity_wins() {
    let history = vec![
        started_event(1),                            // OrchestrationStarted
        activity_scheduled(2, "Task", "input"),      // schedule_activity()
        timer_created(3, 1000),                      // schedule_timer()
        activity_completed(4, 2, "activity-result"), // Activity finished first
    ];
    let mut engine = create_engine(history);
    let handler = Select2Handler::new("Task", "input", Duration::from_secs(60));
    let result = execute(&mut engine, handler);

    assert_completed(&result, "activity:activity-result");
}

/// select2 where timer wins (fires before activity completes).
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let activity = ctx.schedule_activity("Task", "input");
///     let timer = ctx.schedule_timer(Duration::from_secs(60));
///     match ctx.select2(activity, timer).await {
///         Either2::First(result) => Ok(format!("activity:{}", result?)),
///         Either2::Second(()) => Ok("timeout".to_string()),
///     }
/// }
/// ```
#[test]
fn select2_timer_wins() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // schedule_activity()
        timer_created(3, 1000),                 // schedule_timer()
        timer_fired(4, 3, 1000),                // Timer fired first
    ];
    let mut engine = create_engine(history);
    let handler = Select2Handler::new("Task", "input", Duration::from_secs(60));
    let result = execute(&mut engine, handler);

    assert_completed(&result, "timeout");
}

/// select2 where both are pending - neither has completed yet.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let activity = ctx.schedule_activity("Task", "input");
///     let timer = ctx.schedule_timer(Duration::from_secs(60));
///     match ctx.select2(activity, timer).await {  // Still waiting...
///         Either2::First(result) => Ok(format!("activity:{}", result?)),
///         Either2::Second(()) => Ok("timeout".to_string()),
///     }
/// }
/// ```
#[test]
fn select2_both_pending() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // schedule_activity() - pending
        timer_created(3, 1000),                 // schedule_timer() - pending
    ];
    let mut engine = create_engine(history);
    let handler = Select2Handler::new("Task", "input", Duration::from_secs(60));
    let result = execute(&mut engine, handler);

    assert_continue(&result);
}

/// join where all activities complete.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let a = ctx.schedule_activity("A", "a");
///     let b = ctx.schedule_activity("B", "b");
///     let results = ctx.join(vec![a, b]).await;
///     // results = [Ok("result-a"), Ok("result-b")]
///     Ok(results.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>().join(","))
/// }
/// ```
#[test]
fn join_all_complete() {
    let history = vec![
        started_event(1),                     // OrchestrationStarted
        activity_scheduled(2, "A", "a"),      // schedule_activity("A")
        activity_scheduled(3, "B", "b"),      // schedule_activity("B")
        activity_completed(4, 2, "result-a"), // A completed
        activity_completed(5, 3, "result-b"), // B completed
    ];
    let mut engine = create_engine(history);
    let handler = JoinActivitiesHandler::new(vec![("A", "a"), ("B", "b")]);
    let result = execute(&mut engine, handler);

    assert_completed(&result, "result-a,result-b");
}

/// join where one activity is still pending.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let a = ctx.schedule_activity("A", "a");
///     let b = ctx.schedule_activity("B", "b");
///     let results = ctx.join(vec![a, b]).await;  // Still waiting for B...
///     Ok(results.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>().join(","))
/// }
/// ```
#[test]
fn join_partial_complete() {
    let history = vec![
        started_event(1),                     // OrchestrationStarted
        activity_scheduled(2, "A", "a"),      // schedule_activity("A")
        activity_scheduled(3, "B", "b"),      // schedule_activity("B")
        activity_completed(4, 2, "result-a"), // A completed, B still pending
    ];
    let mut engine = create_engine(history);
    let handler = JoinActivitiesHandler::new(vec![("A", "a"), ("B", "b")]);
    let result = execute(&mut engine, handler);

    assert_continue(&result);
}

/// join fresh schedule - schedules all activities.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let a = ctx.schedule_activity("A", "a");
///     let b = ctx.schedule_activity("B", "b");
///     let c = ctx.schedule_activity("C", "c");
///     let results = ctx.join(vec![a, b, c]).await;  // All 3 scheduled this turn
///     Ok(results.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>().join(","))
/// }
/// ```
#[test]
fn join_fresh_schedule() {
    let history = vec![started_event(1)]; // OrchestrationStarted - nothing scheduled yet
    let mut engine = create_engine(history);
    let handler = JoinActivitiesHandler::new(vec![("A", "a"), ("B", "b"), ("C", "c")]);
    let result = execute(&mut engine, handler);

    assert_continue(&result);
    assert_eq!(engine.pending_actions().len(), 3, "Three activities should be pending");
}

/// join where one activity fails - propagates error.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let a = ctx.schedule_activity("A", "a");
///     let b = ctx.schedule_activity("B", "b");
///     let results = ctx.join(vec![a, b]).await;
///     // results = [Ok("result-a"), Err("B failed")]
///     // Handler returns Err because one failed
/// }
/// ```
#[test]
fn join_one_fails() {
    let history = vec![
        started_event(1),                     // OrchestrationStarted
        activity_scheduled(2, "A", "a"),      // schedule_activity("A")
        activity_scheduled(3, "B", "b"),      // schedule_activity("B")
        activity_completed(4, 2, "result-a"), // A completed
        activity_failed(5, 3, "B failed"),    // B failed
    ];
    let mut engine = create_engine(history);
    let handler = JoinActivitiesHandler::new(vec![("A", "a"), ("B", "b")]);
    let result = execute(&mut engine, handler);

    // Join collects results, if one fails the combined result is Err
    assert_failed(&result);
}

/// Large fan-in where every sub-orchestration has completed in history.
///
/// This mirrors a production-scale failure mode: parent bulk orchestrations had
/// all child SubOrchestrationCompleted events persisted, no queue items remained,
/// but replay did not produce a terminal event for the parent.
#[test]
fn join_large_completed_sub_orchestration_fan_in_reaches_terminal() {
    const FAN_IN_COUNT: usize = 1024;

    let mut history = Vec::with_capacity(1 + FAN_IN_COUNT * 2);
    history.push(started_event(1));

    for idx in 0..FAN_IN_COUNT {
        history.push(sub_orch_scheduled(
            (idx + 2) as u64,
            "Child",
            &format!("child-{idx}"),
            &idx.to_string(),
        ));
    }

    for idx in 0..FAN_IN_COUNT {
        history.push(sub_orch_completed(
            (FAN_IN_COUNT + idx + 2) as u64,
            (idx + 2) as u64,
            &idx.to_string(),
        ));
    }

    let mut engine = create_engine(history);
    let handler = LargeSubOrchestrationFanInHandler::new(FAN_IN_COUNT);
    let result = execute(&mut engine, handler);

    assert_completed(&result, &FAN_IN_COUNT.to_string());
    assert!(
        engine.pending_actions().is_empty(),
        "fully replayed fan-in should not emit new pending work"
    );
}

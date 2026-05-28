// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Sequential Progress Tests
//!
//! Tests verifying multi-step orchestration replay.

use super::helpers::*;
use std::time::Duration;

/// Two activities: first one done, second should be scheduled.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let r1 = ctx.schedule_activity("A1", "input1").await?;  // Completed
///     let r2 = ctx.schedule_activity("A2", "input2").await?;  // Scheduled now
///     Ok(format!("{},{}", r1, r2))
/// }
/// ```
#[test]
fn two_activities_first_done() {
    let history = vec![
        started_event(1),                      // OrchestrationStarted
        activity_scheduled(2, "A1", "input1"), // 1st schedule_activity()
        activity_completed(3, 2, "result1"),   // 1st activity done
    ];
    let mut engine = create_engine(history);
    let handler = TwoActivitiesHandler::new(("A1", "input1"), ("A2", "input2"));
    let result = execute(&mut engine, handler);

    assert_continue(&result);
    assert_eq!(engine.pending_actions().len(), 1, "Second activity should be pending");
    assert!(has_activity_action(&engine, "A2"), "A2 should be in pending actions");
}

/// Two activities: both done, should complete.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let r1 = ctx.schedule_activity("A1", "input1").await?;
///     let r2 = ctx.schedule_activity("A2", "input2").await?;
///     Ok(format!("{},{}", r1, r2))
/// }
/// ```
#[test]
fn two_activities_both_done() {
    // History from a completed multi-step execution:
    // Turn 1: Started, scheduled A1, yielded
    // Turn 2: A1 completed, scheduled A2, yielded
    // Turn 3: A2 completed, orchestration returned
    let history = vec![
        started_event(1),                      // OrchestrationStarted
        activity_scheduled(2, "A1", "input1"), // 1st schedule_activity()
        activity_completed(3, 2, "result1"),   // 1st activity done
        activity_scheduled(4, "A2", "input2"), // 2nd schedule_activity()
        activity_completed(5, 4, "result2"),   // 2nd activity done
    ];
    let mut engine = create_engine(history);
    let handler = TwoActivitiesHandler::new(("A1", "input1"), ("A2", "input2"));
    let result = execute(&mut engine, handler);

    assert_completed(&result, "result1,result2");
}

/// Two identical activities (same name + input) with different results.
///
/// This validates that completions are routed by schedule identity (event_id / source_event_id),
/// not by (name, input).
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let r1 = ctx.schedule_activity("Task", "same").await?;
///     let r2 = ctx.schedule_activity("Task", "same").await?;
///     Ok(format!("{r1},{r2}"))
/// }
/// ```
#[test]
fn identical_activities_both_done_routes_correctly() {
    use async_trait::async_trait;
    use duroxide::{OrchestrationContext, OrchestrationHandler};
    use std::sync::Arc;

    struct IdenticalActivitiesHandler;

    #[async_trait]
    impl OrchestrationHandler for IdenticalActivitiesHandler {
        async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
            let r1 = ctx.schedule_activity("Task", "same").await?;
            let r2 = ctx.schedule_activity("Task", "same").await?;
            Ok(format!("{r1},{r2}"))
        }
    }

    let history = vec![
        started_event(1),
        activity_scheduled(2, "Task", "same"),
        activity_completed(3, 2, "R1"),
        activity_scheduled(4, "Task", "same"),
        activity_completed(5, 4, "R2"),
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, Arc::new(IdenticalActivitiesHandler));

    assert_completed(&result, "R1,R2");
}

/// Same activity name+input twice with a timer between, across a restart boundary.
///
/// We simulate restart boundaries by recreating a fresh ReplayEngine each turn using the
/// persisted history from the previous turn.
#[test]
fn identical_activities_routes_correctly_across_restart_boundary() {
    use async_trait::async_trait;
    use duroxide::{OrchestrationContext, OrchestrationHandler};
    use std::sync::Arc;

    struct SameActivityTwiceWithTimer;

    #[async_trait]
    impl OrchestrationHandler for SameActivityTwiceWithTimer {
        async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
            let r1 = ctx.schedule_activity("Task", "same").await?;
            ctx.schedule_timer(Duration::from_secs(60)).await;
            let r2 = ctx.schedule_activity("Task", "same").await?;
            Ok(format!("{r1},{r2}"))
        }
    }

    let handler = Arc::new(SameActivityTwiceWithTimer);

    // Turn 1: schedule first activity.
    let history_t1 = vec![started_event(1)];
    let mut engine1 = create_engine(history_t1);
    let result1 = execute(&mut engine1, handler.clone());
    assert_continue(&result1);
    assert!(has_activity_action(&engine1, "Task"), "Task should be pending");
    assert!(!has_timer_action(&engine1), "Timer should not be pending yet");

    // Persist turn 1 schedule.
    let mut persisted = vec![started_event(1)];
    persisted.extend(engine1.history_delta().to_vec());

    // Simulate worker completion for the first activity.
    let first_sched_id = match engine1.pending_actions().first() {
        Some(duroxide::Action::CallActivity {
            scheduling_event_id, ..
        }) => *scheduling_event_id,
        other => panic!("Expected CallActivity pending action, got {other:?}"),
    };
    persisted.push(activity_completed(3, first_sched_id, "R1"));

    // Turn 2 (after restart): should replay first completion and schedule timer (only).
    let mut engine2 = create_engine(persisted.clone());
    let result2 = execute(&mut engine2, handler.clone());
    assert_continue(&result2);
    assert!(has_timer_action(&engine2), "Timer should be pending");
    assert!(
        !has_activity_action(&engine2, "Task"),
        "Activity should not be rescheduled"
    );

    // Persist timer schedule.
    persisted.extend(engine2.history_delta().to_vec());

    // Find the timer schedule id and simulate timer firing.
    let timer_sched_id = engine2
        .pending_actions()
        .iter()
        .find_map(|a| match a {
            duroxide::Action::CreateTimer {
                scheduling_event_id, ..
            } => Some(*scheduling_event_id),
            _ => None,
        })
        .expect("Expected CreateTimer pending action");
    persisted.push(timer_fired(5, timer_sched_id, 0));

    // Turn 3 (after restart): should replay timer fired and schedule the second activity.
    let mut engine3 = create_engine(persisted.clone());
    let result3 = execute(&mut engine3, handler.clone());
    assert_continue(&result3);
    assert!(has_activity_action(&engine3, "Task"), "Second Task should be pending");
    assert!(!has_timer_action(&engine3), "No new timer expected");

    // Persist second activity schedule.
    persisted.extend(engine3.history_delta().to_vec());

    // Simulate worker completion for second activity.
    let second_sched_id = engine3
        .pending_actions()
        .iter()
        .find_map(|a| match a {
            duroxide::Action::CallActivity {
                scheduling_event_id, ..
            } => Some(*scheduling_event_id),
            _ => None,
        })
        .expect("Expected CallActivity pending action for second Task");
    persisted.push(activity_completed(7, second_sched_id, "R2"));

    // Turn 4 (after restart): should complete with both results.
    let mut engine4 = create_engine(persisted);
    let result4 = execute(&mut engine4, handler);
    assert_completed(&result4, "R1,R2");
}

/// Activity completed, then timer - should schedule timer.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let r = ctx.schedule_activity("Task", "input").await?;  // Completed
///     ctx.schedule_timer(Duration::from_secs(60)).await;       // Scheduled now
///     Ok(r)
/// }
/// ```
#[test]
fn activity_then_timer() {
    let history = vec![
        started_event(1),                       // OrchestrationStarted
        activity_scheduled(2, "Task", "input"), // schedule_activity()
        activity_completed(3, 2, "done"),       // activity done
    ];
    let mut engine = create_engine(history);
    let handler = ActivityThenTimerHandler::new("Task", "input", Duration::from_secs(60));
    let result = execute(&mut engine, handler);

    assert_continue(&result);
    assert!(has_timer_action(&engine), "Timer should be pending");
}

/// Timer fired, then activity - should schedule activity.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     ctx.schedule_timer(Duration::from_secs(60)).await;  // Fired
///     let r = ctx.schedule_activity("Task", "input").await?;  // Scheduled now
///     Ok(r)
/// }
/// ```
#[test]
fn timer_then_activity() {
    use async_trait::async_trait;
    use duroxide::{OrchestrationContext, OrchestrationHandler};
    use std::sync::Arc;

    struct TimerThenActivityHandler;

    #[async_trait]
    impl OrchestrationHandler for TimerThenActivityHandler {
        async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
            ctx.schedule_timer(Duration::from_secs(60)).await;
            let r = ctx.schedule_activity("Task", "input").await?;
            Ok(r)
        }
    }

    let history = vec![started_event(1), timer_created(2, 1000), timer_fired(3, 2, 1000)];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, Arc::new(TimerThenActivityHandler));

    assert_continue(&result);
    assert!(has_activity_action(&engine, "Task"), "Activity should be pending");
}

/// Many sequential activities - all complete.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let mut results = Vec::new();
///     for i in 0..10 {
///         let r = ctx.schedule_activity(&format!("Act{i}"), &format!("input{i}")).await?;
///         results.push(r);
///     }
///     Ok(results.join(","))
/// }
/// ```
#[test]
fn many_sequential_activities() {
    use async_trait::async_trait;
    use duroxide::{OrchestrationContext, OrchestrationHandler};
    use std::sync::Arc;

    struct TenActivitiesHandler;

    #[async_trait]
    impl OrchestrationHandler for TenActivitiesHandler {
        async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
            let mut results = Vec::new();
            for i in 0..10 {
                let r = ctx.schedule_activity(format!("Act{i}"), format!("input{i}")).await?;
                results.push(r);
            }
            Ok(results.join(","))
        }
    }

    // Build history with 10 activities scheduled and completed
    let mut history = vec![started_event(1)];
    let mut event_id = 2u64;
    for i in 0..10 {
        let sched_id = event_id;
        history.push(activity_scheduled(event_id, &format!("Act{i}"), &format!("input{i}")));
        event_id += 1;
        history.push(activity_completed(event_id, sched_id, &format!("result{i}")));
        event_id += 1;
    }

    let mut engine = create_engine(history);
    let result = execute(&mut engine, Arc::new(TenActivitiesHandler));

    let expected: Vec<String> = (0..10).map(|i| format!("result{i}")).collect();
    assert_completed(&result, &expected.join(","));
}

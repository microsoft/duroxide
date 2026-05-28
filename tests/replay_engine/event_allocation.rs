// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Event ID Allocation Tests
//!
//! Tests verifying correct event ID assignment for new events.

use super::helpers::*;

/// Sequential allocation starting from 2 after OrchestrationStarted.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let _ = ctx.schedule_activity("A", "a");  // Gets event_id 2
///     let _ = ctx.schedule_activity("B", "b");  // Gets event_id 3
///     let _ = ctx.schedule_activity("C", "c");  // Gets event_id 4
///     Ok("done".to_string())
/// }
/// ```
#[test]
fn sequential_allocation() {
    let history = vec![started_event(1)]; // OrchestrationStarted has id=1
    let mut engine = create_engine(history);

    let handler = MultiScheduleNoAwaitHandler::new(vec![("A", "a"), ("B", "b"), ("C", "c")]);
    let result = execute(&mut engine, handler);

    assert_completed(&result, "done");

    // The three schedules should be allocated IDs 2, 3, 4.
    // Note: the unawaited futures are dropped at end-of-turn, which also emits
    // ActivityCancelRequested events (and consumes additional event IDs).
    let scheduled_ids: Vec<u64> = engine
        .history_delta()
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::ActivityScheduled { .. }))
        .map(|e| e.event_id())
        .collect();
    assert_eq!(scheduled_ids, vec![2, 3, 4], "Scheduled event IDs should start at 2");

    // Regardless of event type, allocation should be contiguous starting at 2.
    let ids = delta_event_ids(&engine);
    assert_eq!(ids.first().copied(), Some(2), "First delta event ID should be 2");
    assert!(
        ids.windows(2).all(|w| w[1] == w[0] + 1),
        "Event IDs should be sequential starting from 2, got {ids:?}"
    );
}

/// Allocation after replay continues from max event_id.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let r1 = ctx.schedule_activity("First", "input").await?;  // id=2, completed at id=3
///     let r2 = ctx.schedule_activity("Second", "input2").await?; // Gets id=4 (max+1)
///     Ok(format!("{},{}", r1, r2))
/// }
/// ```
#[test]
fn allocation_after_replay() {
    let history = vec![
        started_event(1),                        // id=1
        activity_scheduled(2, "First", "input"), // id=2
        activity_completed(3, 2, "result"),      // id=3 (max so far)
    ];
    let mut engine = create_engine(history);

    let handler = TwoActivitiesHandler::new(("First", "input"), ("Second", "input2"));
    let result = execute(&mut engine, handler);

    assert_continue(&result);

    // Second activity should get ID 4 (max was 3)
    let ids = delta_event_ids(&engine);
    assert_eq!(ids, vec![4], "New event should get ID = max + 1");
}

/// Allocation with gaps in existing history.
///
/// Even if history has non-sequential IDs (e.g., 1, 5), new events
/// continue from the maximum (5+1=6).
#[test]
fn allocation_with_gaps() {
    let history = vec![
        started_event(1),                       // id=1
        activity_scheduled(5, "Task", "input"), // id=5 (gap: no 2,3,4)
    ];
    let mut engine = create_engine(history);

    // Inject completion for the existing activity
    engine.prep_completions(vec![activity_completed_msg(5, "result")]);

    let handler = TwoActivitiesHandler::new(("Task", "input"), ("Next", "next"));
    let result = execute(&mut engine, handler);

    assert_continue(&result);

    // Completion gets id 6, new schedule gets id 7
    let ids = delta_event_ids(&engine);
    assert!(ids.contains(&6), "Completion should get ID 6");
    assert!(ids.contains(&7), "New schedule should get ID 7");
}

/// Pending actions have correct scheduling_event_id.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     Ok(result)
/// }
/// ```
///
/// The pending Action should reference the same event_id as the schedule event.
#[test]
fn pending_actions_have_correct_event_id() {
    let history = vec![started_event(1)]; // OrchestrationStarted
    let mut engine = create_engine(history);

    let result = execute(&mut engine, SingleActivityHandler::new("Task", "input"));

    assert_continue(&result);

    // Check the pending action has the right scheduling_event_id
    assert_eq!(engine.pending_actions().len(), 1);
    match &engine.pending_actions()[0] {
        duroxide::Action::CallActivity {
            scheduling_event_id, ..
        } => {
            assert_eq!(*scheduling_event_id, 2, "Action should reference event_id 2");
        }
        _ => panic!("Expected CallActivity action"),
    }
}

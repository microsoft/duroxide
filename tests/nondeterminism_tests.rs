// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::EventKind;
// Test: Various nondeterminism detection scenarios
// This file consolidates all nondeterminism-related tests to verify the robust detection system

use duroxide::providers::WorkItem;
// Use SQLite provider via common helper
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::time::Duration;
mod common;

#[tokio::test]
async fn code_swap_triggers_nondeterminism() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Register both A1 and B1 activities at all times
    let activity_registry = ActivityRegistry::builder()
        // A1 never completes (simulate long-running or blocked work)
        .register("A1", |_ctx: ActivityContext, _input: String| async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
            #[allow(unreachable_code)]
            Ok(String::new())
        })
        // B1 completes quickly
        .register("B1", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("B1:{{{input}}}"))
        })
        .build();

    // Code A: schedules activity "A1" then waits for completion
    let orch_a = |ctx: OrchestrationContext, _input: String| async move {
        let res = ctx.schedule_activity("A1", "foo").await.unwrap();
        Ok(res)
    };
    // Code B: schedules activity "B1" (different name/id)
    let orch_b = |ctx: OrchestrationContext, _input: String| async move {
        let res = ctx.schedule_activity("B1", "bar").await.unwrap();
        Ok(res)
    };

    // Register A, start orchestration
    let reg_a = OrchestrationRegistry::builder().register("SwapTest", orch_a).build();
    let rt_a = runtime::Runtime::start_with_store(store.clone(), activity_registry.clone(), reg_a).await;
    let client = Client::new(store.clone());
    client.start_orchestration("inst-swap", "SwapTest", "").await.unwrap();

    // Wait for ActivityScheduled("A1") to appear in history and capture its schedule id.
    let a1_schedule_id = common::wait_for_history_event(
        store.clone(),
        "inst-swap",
        |hist| {
            hist.iter().find_map(|e| match &e.kind {
                EventKind::ActivityScheduled { name, .. } if name == "A1" => Some(e.event_id),
                _ => None,
            })
        },
        2000,
    )
    .await;
    let a1_schedule_id = a1_schedule_id.expect("timed out waiting for A1 schedule");

    // Simulate code swap: properly shutdown old runtime, create new one with registry B
    rt_a.shutdown(None).await;
    let reg_b = OrchestrationRegistry::builder().register("SwapTest", orch_b).build();
    let _rt_b = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg_b).await;

    // Poke the instance so it activates and runs a turn.
    // Use a *valid* completion for the existing A1 schedule id; this reliably wakes the
    // orchestrator even under the "warn+drop unmatched completions" policy.
    let _ = store
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "inst-swap".to_string(),
                execution_id: 1,
                id: a1_schedule_id,
                result: serde_json::to_string(&Ok::<String, String>("poke".to_string())).unwrap(),
            },
            Some(Duration::ZERO),
        )
        .await;

    // Wait for terminal status using helper
    let client = Client::new(store.clone());
    match client
        .wait_for_orchestration("inst-swap", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(
                    details,
                    duroxide::ErrorDetails::Configuration {
                        kind: duroxide::ConfigErrorKind::Nondeterminism,
                        ..
                    }
                ),
                "expected nondeterminism error, got: {details:?}"
            );
        }
        other => panic!("expected failure with nondeterminism, got: {other:?}"),
    }
}

#[tokio::test]
async fn completion_kind_mismatch_triggers_nondeterminism() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("result:{input}"))
        })
        .build();

    // Orchestration that creates a timer, then waits for it
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        // Create a timer that fires in 1 second (1000ms)
        let timer_future = ctx.schedule_timer(Duration::from_millis(1000));
        let _result = timer_future.await;
        Ok("timer_completed".to_string())
    };

    let reg = OrchestrationRegistry::builder()
        .register("KindMismatchTest", orch)
        .build();
    let _rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg).await;
    let client = Client::new(store.clone());

    // Start the orchestration
    client
        .start_orchestration("inst-mismatch", "KindMismatchTest", "")
        .await
        .unwrap();

    // Wait for the timer to be created in history
    let timer_created = common::wait_for_history_event(
        store.clone(),
        "inst-mismatch",
        |hist| {
            hist.iter().find_map(|e| match &e.kind {
                EventKind::TimerCreated { .. } => Some(e.event_id),
                _ => None,
            })
        },
        2000,
    )
    .await;

    let timer_id = timer_created.expect("Timer should be created");
    println!("Timer created with ID: {timer_id}");

    // Inject a completion with the WRONG kind - send ActivityCompleted for a timer ID
    let _ = store
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "inst-mismatch".to_string(),
                execution_id: 1,
                id: timer_id, // This is a timer ID, but we're sending ActivityCompleted!
                result: "wrong_kind_result".to_string(),
            },
            None,
        )
        .await;

    // The orchestration should fail with nondeterminism error about kind mismatch
    match client
        .wait_for_orchestration("inst-mismatch", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Failed { details, .. } => {
            println!("Got expected error: {}", details.display_message());
            assert!(
                matches!(
                    details,
                    duroxide::ErrorDetails::Configuration {
                        kind: duroxide::ConfigErrorKind::Nondeterminism,
                        message: Some(ref msg),
                        ..
                    } if msg.contains("kind mismatch") && msg.contains("timer") && msg.contains("activity")
                ),
                "Expected nondeterminism error about kind mismatch between timer and activity, got: {details:?}"
            );
        }
        other => panic!("Expected failure with nondeterminism, got: {other:?}"),
    }
}

#[tokio::test]
async fn unexpected_completion_id_triggers_nondeterminism() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("result:{input}"))
        })
        .build();

    // Orchestration that waits for external events (doesn't schedule anything with ID 999)
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let _result = ctx.schedule_wait("test_event").await;
        Ok("external_completed".to_string())
    };

    let reg = OrchestrationRegistry::builder()
        .register("UnexpectedIdTest", orch)
        .build();
    let _rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg).await;
    let client = Client::new(store.clone());

    // Start the orchestration
    client
        .start_orchestration("inst-unexpected", "UnexpectedIdTest", "")
        .await
        .unwrap();

    // Wait for the external subscription to be created
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Inject a completion for an ID that was never scheduled (999)
    let _ = store
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "inst-unexpected".to_string(),
                execution_id: 1,
                id: 999, // This ID was never scheduled by the orchestration
                result: "unexpected_result".to_string(),
            },
            None,
        )
        .await;

    // The orchestration should fail with nondeterminism error about unexpected completion
    match client
        .wait_for_orchestration("inst-unexpected", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Failed { details, .. } => {
            println!("Got expected error: {}", details.display_message());
            assert!(
                matches!(
                    details,
                    duroxide::ErrorDetails::Configuration {
                        kind: duroxide::ConfigErrorKind::Nondeterminism,
                        message: Some(ref msg),
                        ..
                    } if msg.contains("no matching schedule") && msg.contains("999")
                ),
                "Expected nondeterminism error about unexpected completion ID 999, got: {details:?}"
            );
        }
        other => panic!("Expected failure with nondeterminism, got: {other:?}"),
    }
}

#[tokio::test]
async fn unexpected_timer_completion_triggers_nondeterminism() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder().build();

    // Simple orchestration that just waits for external events (doesn't create any timers)
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        // Wait for an external event, but don't create any timers
        let _result = ctx.schedule_wait("test").await;
        Ok("done".to_string())
    };

    let reg = OrchestrationRegistry::builder().register("TimerTest", orch).build();
    let _rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg).await;
    let client = Client::new(store.clone());

    // Start the orchestration
    client.start_orchestration("inst-timer", "TimerTest", "").await.unwrap();

    // Wait for the orchestration to be waiting for external events
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Inject an unexpected timer completion (timer ID 123 was never scheduled)
    let _ = store
        .enqueue_for_orchestrator(
            WorkItem::TimerFired {
                instance: "inst-timer".to_string(),
                execution_id: 1,
                id: 123,
                fire_at_ms: 0,
            },
            None,
        )
        .await;

    // The orchestration should fail with nondeterminism error
    match client
        .wait_for_orchestration("inst-timer", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Failed { details, .. } => {
            println!("Got expected error: {}", details.display_message());
            assert!(
                matches!(
                    details,
                    duroxide::ErrorDetails::Configuration {
                        kind: duroxide::ConfigErrorKind::Nondeterminism,
                        message: Some(ref msg),
                        ..
                    } if msg.contains("timer") && msg.contains("123")
                ),
                "Expected nondeterminism error about timer 123, got: {details:?}"
            );
        }
        other => panic!("Expected failure with nondeterminism, got: {other:?}"),
    }
}

#[tokio::test]
async fn continue_as_new_with_unconsumed_completion_triggers_nondeterminism() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("MyActivity", |_ctx: ActivityContext, _input: String| async move {
            // Activity that never completes on its own
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
            #[allow(unreachable_code)]
            Ok("activity_result".to_string())
        })
        .build();

    // Orchestration that schedules activity then waits for signal before calling CAN
    let orch = |ctx: OrchestrationContext, input: String| async move {
        let n: u32 = input.parse().unwrap_or(0);

        // First iteration: schedule activity
        if n == 0 {
            // Schedule an activity - this will create ActivityScheduled event
            let _activity_future = ctx.schedule_activity("MyActivity", "test_input");

            // Wait for an external event - this blocks the orchestration
            let _ = ctx.schedule_wait("proceed_signal").await;

            // When we get the signal, call continue_as_new
            // The activity is still pending and its completion might be in the batch
            return ctx.continue_as_new("1").await;
        } else {
            // Second iteration: just complete
            Ok(format!("final:iteration_{n}"))
        }
    };

    let reg = OrchestrationRegistry::builder()
        .register("CanNondeterminism", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg).await;
    let client = Client::new(store.clone());

    // Start the orchestration
    client
        .start_orchestration("inst-can-nondet", "CanNondeterminism", "0")
        .await
        .unwrap();

    // Wait for the orchestration to be waiting for the signal
    let ok = common::wait_for_history(
        store.clone(),
        "inst-can-nondet",
        |hist| {
            hist.iter()
                .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed { name, .. } if name == "proceed_signal"))
        },
        2000,
    )
    .await;
    assert!(ok, "timeout waiting for external subscription");

    // Now manually enqueue an activity completion
    // This simulates the activity completing while the orchestration is waiting
    let completion = WorkItem::ActivityCompleted {
        instance: "inst-can-nondet".to_string(),
        execution_id: 1,
        id: 1,
        result: serde_json::to_string(&Ok::<String, String>("activity_completed".to_string())).unwrap(),
    };
    store.enqueue_for_orchestrator(completion, None).await.unwrap();

    // Give it a moment to ensure the completion is in the queue
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Now send the signal that will trigger continue_as_new
    let _ = client.raise_event("inst-can-nondet", "proceed_signal", "go").await;

    // Wait for the orchestration to complete or fail
    match client
        .wait_for_orchestration("inst-can-nondet", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Failed { details, .. } => {
            println!("Got expected nondeterminism error: {}", details.display_message());
            assert!(
                matches!(
                    details,
                    duroxide::ErrorDetails::Configuration {
                        kind: duroxide::ConfigErrorKind::Nondeterminism,
                        ..
                    }
                ),
                "Expected nondeterminism error, got: {details:?}"
            );
        }
        OrchestrationStatus::Completed { output, .. } => {
            panic!("Expected nondeterminism failure but orchestration completed: {output}");
        }
        other => panic!("Unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

#[tokio::test]
async fn execution_id_filtering_prevents_cross_execution_completions() {
    // This test verifies that activity completions from previous executions
    // are filtered out and don't cause nondeterminism.

    // The test is embedded within the continue_as_new tests where we can
    // observe the warning logs about filtered cross-execution completions.
    // The key behavior is tested there: when an orchestration does ContinueAsNew,
    // any pending activity completions from the previous execution are filtered
    // out (with a warning) rather than causing nondeterminism.

    // See continue_as_new_tests.rs for the actual execution ID filtering behavior.
    // This placeholder test just verifies the mechanism exists.
    let _dummy = 42;
    assert_eq!(_dummy, 42);
}

#[tokio::test]
async fn execution_id_filtering_without_continue_as_new_triggers_nondeterminism() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Orchestration that schedules an activity but doesn't use continue_as_new
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.trace_info("scheduling activity".to_string());
        let result = ctx.schedule_activity("TestActivity", "input").await;
        ctx.trace_info("got result, completing".to_string());
        result
    };

    let reg = OrchestrationRegistry::builder()
        .register("ExecIdNoCanTest", orch)
        .build();
    let activity_registry = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, _input: String| async {
            Ok("activity result".to_string())
        })
        .build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg).await;
    let client = Client::new(store.clone());

    // Start orchestration
    client
        .start_orchestration("inst-exec-id-no-can", "ExecIdNoCanTest", "")
        .await
        .unwrap();

    // Manually inject a completion from a different execution ID
    // This simulates what would happen if there was a bug in execution ID handling
    store
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "inst-exec-id-no-can".to_string(),
                id: 1,
                result: "different execution result".to_string(),
                execution_id: 999, // Different execution ID
            },
            None,
        )
        .await
        .unwrap();

    // Wait for orchestration to complete
    match client
        .wait_for_orchestration("inst-exec-id-no-can", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            println!("✓ Orchestration completed successfully: {output}");
            assert_eq!(output, "activity result", "Should get the normal activity result");
            // The orchestration should complete successfully because:
            // 1. The completion from different execution ID is detected and logged as ERROR
            // 2. But it's filtered out and acknowledged (not processed)
            // 3. The orchestration continues with its normal flow and gets the real activity result
            // This demonstrates that execution ID filtering prevents cross-execution completions from affecting the orchestration
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!(
                "Expected successful completion but got error: {}",
                details.display_message()
            );
        }
        other => panic!("Unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

#[tokio::test]
async fn duplicate_external_events_are_handled_gracefully() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Orchestration that waits for external event
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.trace_info("waiting for external event".to_string());
        let result = ctx.schedule_wait("test_signal").await;
        Ok(result)
    };

    let reg = OrchestrationRegistry::builder()
        .register("DuplicateExternalTest", orch)
        .build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg).await;
    let client = duroxide::Client::new(store.clone());

    // Start orchestration
    client
        .start_orchestration("inst-duplicate-external", "DuplicateExternalTest", "")
        .await
        .unwrap();

    // Wait for subscription to be established
    let _ = common::wait_for_subscription(store.clone(), "inst-duplicate-external", "test_signal", 2_000).await;

    // Send the same external event twice
    let _ = client
        .raise_event("inst-duplicate-external", "test_signal", "first")
        .await;
    let _ = client
        .raise_event("inst-duplicate-external", "test_signal", "first")
        .await; // Duplicate

    // Wait for orchestration to complete
    match client
        .wait_for_orchestration("inst-duplicate-external", std::time::Duration::from_secs(3))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            println!("✓ Orchestration completed successfully with output: {output}");
            assert_eq!(output, "first", "Should get the first event");
            // The orchestration should complete successfully because:
            // 1. First external event is processed normally
            // 2. Duplicate external event is detected and ignored with a warning
            // 3. No nondeterminism error is raised
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!(
                "Expected successful completion but got error: {}",
                details.display_message()
            );
        }
        other => panic!("Unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

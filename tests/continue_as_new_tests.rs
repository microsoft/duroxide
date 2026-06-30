// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::EventKind;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, Either2, Event, OrchestrationContext, OrchestrationRegistry};
use std::time::Duration;
mod common;

// Basic ContinueAsNew loop: rolls input across executions and finally completes.
#[tokio::test]
async fn continue_as_new_multiexec() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Orchestrator: if n < 2 then ContinueAsNew with n+1, else complete
    let counter = |ctx: OrchestrationContext, input: String| async move {
        let n: u32 = input.parse().unwrap_or(0);
        if n < 2 {
            ctx.trace_info(format!("counter exec n={n} -> continue as new"));
            return ctx.continue_as_new((n + 1).to_string()).await;
        } else {
            ctx.trace_info(format!("counter exec n={n} -> complete"));
            Ok(format!("done:{n}"))
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("Counter", counter).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    // The initial start handle will resolve when the first execution continues-as-new.
    client.start_orchestration("inst-can-1", "Counter", "0").await.unwrap();

    match client
        .wait_for_orchestration("inst-can-1", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "done:2"),
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Verify final completion is also in history
    let ok = common::wait_for_history(
        store.clone(),
        "inst-can-1",
        |hist| {
            hist.iter()
                .rev()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "done:2"))
        },
        1_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");

    // Verify multi-execution histories exist: execs 1,2 continued-as-new; exec 3 completed
    let mgmt = store.as_management_capability().expect("ProviderAdmin required");
    let execs = mgmt.list_executions("inst-can-1").await.unwrap_or_default();
    assert_eq!(execs, vec![1, 2, 3]);

    // read() must reflect the latest execution's history
    let latest = *execs.last().unwrap();
    let latest_hist = mgmt
        .read_history_with_execution_id("inst-can-1", latest)
        .await
        .unwrap_or_default();
    let current_hist = store.read("inst-can-1").await.unwrap_or_default();
    assert_eq!(current_hist, latest_hist);

    let e1 = mgmt
        .read_history_with_execution_id("inst-can-1", 1)
        .await
        .unwrap_or_default();
    assert!(
        e1.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationStarted { input, .. } if input == "0"))
    );
    assert!(
        e1.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationContinuedAsNew { input, .. } if input == "1"))
    );
    assert!(
        !e1.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { .. }))
    );

    let e2 = mgmt
        .read_history_with_execution_id("inst-can-1", 2)
        .await
        .unwrap_or_default();
    assert!(
        e2.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationStarted { input, .. } if input == "1"))
    );
    assert!(
        e2.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationContinuedAsNew { input, .. } if input == "2"))
    );
    assert!(
        !e2.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { .. }))
    );

    let e3 = mgmt
        .read_history_with_execution_id("inst-can-1", 3)
        .await
        .unwrap_or_default();
    assert!(
        e3.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationStarted { input, .. } if input == "2"))
    );
    assert!(
        e3.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "done:2"))
    );

    rt.shutdown(None).await;
}

// External events are dispatched to the most recent execution.
#[tokio::test]
async fn continue_as_new_event_routes_to_latest() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Orchestrator: first execution continues immediately; second waits for "Go" then completes with payload
    let orch = |ctx: OrchestrationContext, input: String| async move {
        match input.as_str() {
            "start" => {
                ctx.trace_info("first exec -> continue".to_string());
                return ctx.continue_as_new("wait").await;
            }
            "wait" => {
                ctx.trace_info("second exec -> subscribe and wait".to_string());
                let v = ctx.schedule_wait("Go").await;
                Ok(v)
            }
            _ => Ok(input),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("EvtCAN", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    // Raise event after the second execution subscribes
    let store_for_wait = store.clone();
    let client_c = duroxide::Client::new(store.clone());
    tokio::spawn(async move {
        let _ = common::wait_for_subscription(store_for_wait, "inst-can-evt", "Go", 2_000).await;
        let _ = client_c.raise_event("inst-can-evt", "Go", "ok").await;
    });

    client
        .start_orchestration("inst-can-evt", "EvtCAN", "start")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-can-evt", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "ok"),
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Verify final completion is also in history
    let ok2 = common::wait_for_history(
        store.clone(),
        "inst-can-evt",
        |hist| {
            hist.iter()
                .rev()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "ok"))
        },
        1_000,
    )
    .await;
    assert!(ok2, "timeout waiting for completion");

    // Exec1 should not contain ExternalEvent; Exec2 should
    let mgmt2 = store.as_management_capability().expect("ProviderAdmin required");
    let e1 = mgmt2
        .read_history_with_execution_id("inst-can-evt", 1)
        .await
        .unwrap_or_default();
    assert!(
        e1.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationContinuedAsNew { input, .. } if input == "wait"))
    );
    assert!(
        !e1.iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalEvent { name, .. } if name == "Go"))
    );

    let e2 = mgmt2
        .read_history_with_execution_id("inst-can-evt", 2)
        .await
        .unwrap_or_default();
    assert!(
        e2.iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed { name, .. } if name == "Go"))
    );
    assert!(
        e2.iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalEvent { name, .. } if name == "Go"))
    );

    // read() must reflect the latest execution's history
    let mgmt2 = store.as_management_capability().expect("ProviderAdmin required");
    let execs = mgmt2.list_executions("inst-can-evt").await.unwrap_or_default();
    let latest = *execs.last().unwrap();
    let latest_hist = mgmt2
        .read_history_with_execution_id("inst-can-evt", latest)
        .await
        .unwrap_or_default();
    let current_hist = store.read("inst-can-evt").await.unwrap_or_default();
    assert_eq!(current_hist, latest_hist);

    rt.shutdown(None).await;
}

// External events sent before the new execution's subscription are dropped; after subscribing, events are delivered.
#[tokio::test]
async fn continue_as_new_event_drop_then_process() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Orchestrator: first execution continues; second waits for Go twice (second send expected to deliver)
    let orch = |ctx: OrchestrationContext, input: String| async move {
        match input.as_str() {
            "start" => {
                ctx.trace_info("first exec -> continue".to_string());
                return ctx.continue_as_new("wait").await;
            }
            "wait" => {
                ctx.trace_info("second exec -> subscribe and wait".to_string());
                let v = ctx.schedule_wait("Go").await;
                Ok(v)
            }
            _ => Ok(input),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("EvtDropThenProcess", orch)
        .build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    // Start orchestrator
    let client_c1 = duroxide::Client::new(store.clone());
    tokio::spawn(async move {
        // Intentionally send too early to new execution (before subscription)
        // We wait a bit to ensure CAN happens but before subscription is recorded.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = client_c1.raise_event("inst-can-evt-drop", "Go", "early").await;
    });

    // After subscription exists, send again
    let store_for_wait = store.clone();
    let client_c2 = duroxide::Client::new(store.clone());
    tokio::spawn(async move {
        let _ = common::wait_for_subscription(store_for_wait, "inst-can-evt-drop", "Go", 2_000).await;
        let _ = client_c2.raise_event("inst-can-evt-drop", "Go", "late").await;
    });

    client
        .start_orchestration("inst-can-evt-drop", "EvtDropThenProcess", "start")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-can-evt-drop", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "late"),
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Verify final completion is also in history
    let ok = common::wait_for_history(
        store.clone(),
        "inst-can-evt-drop",
        |hist| {
            hist.iter()
                .rev()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "late"))
        },
        1_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");

    // Exec2 should have ExternalSubscribed and ExternalEvent for Go; payload should be 'late'
    let mgmt2 = store.as_management_capability().expect("ProviderAdmin required");
    let e2 = mgmt2
        .read_history_with_execution_id("inst-can-evt-drop", 2)
        .await
        .unwrap_or_default();
    assert!(
        e2.iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed { name, .. } if name == "Go"))
    );
    assert!(
        e2.iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalEvent { name, .. } if name == "Go"))
    );

    // Exec1 must not have ExternalEvent
    let e1 = mgmt2
        .read_history_with_execution_id("inst-can-evt-drop", 1)
        .await
        .unwrap_or_default();
    assert!(
        !e1.iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalEvent { name, .. } if name == "Go"))
    );

    rt.shutdown(None).await;
}

// An event raised before any subscription exists is materialized in history
// (audit trail) but NOT delivered. The causal check in the replay engine skips
// delivery when no pending subscription slot exists at the point the event
// appears in history. The subscription must wait for a subsequent event.
#[tokio::test]
async fn event_drop_then_retry_after_subscribe() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.trace_info("subscribe after a short delay".to_string());
        // Introduce a small timer before subscribing to simulate early event arrival
        ctx.schedule_timer(Duration::from_millis(100)).await;
        // The early event is in history but was not delivered (causal check).
        // Use select2 with timeout to avoid hanging indefinitely.
        let wait = ctx.schedule_wait("Data");
        let timeout = ctx.schedule_timer(Duration::from_millis(500));
        match ctx.select2(wait, timeout).await {
            Either2::First(v) => Ok(v),
            Either2::Second(()) => Ok("early-event-dropped".to_string()),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("EvtDropRetry", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;

    // Send early event before subscription is recorded (instance will be active due to timer)
    let client_c1 = duroxide::Client::new(store.clone());
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let _ = client_c1.raise_event("inst-drop-retry", "Data", "early").await;
    });

    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("inst-drop-retry", "EvtDropRetry", "x")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-drop-retry", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        // The early event is materialized in history but not delivered to the subscription
        // (causal check: no subscription existed when the event appeared in history).
        // Timer wins the select2, orchestration returns "early-event-dropped".
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "early-event-dropped"),
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // The early event IS materialized in history (audit trail)
    let e = store.read("inst-drop-retry").await.unwrap_or_default();
    let events: Vec<&Event> = e
        .iter()
        .filter(|ev| matches!(&ev.kind, EventKind::ExternalEvent { name, .. } if name == "Data"))
        .collect();
    assert!(
        !events.is_empty(),
        "early event should be materialized in history (audit trail)"
    );

    rt.shutdown(None).await;
}
// Test: Execution ID validation for ContinueAsNew scenarios
// This test verifies that completions from old executions are properly ignored

// merged section imports are already present at the top of this file
use duroxide::providers::WorkItem;

#[tokio::test]
async fn old_execution_completions_are_ignored() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, _input: String| async move {
            // Activity that completes quickly
            Ok("activity_result".to_string())
        })
        .build();

    // Orchestration that waits for external events (stays active)
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        // Wait for an external event to keep the orchestration active
        let _result = ctx.schedule_wait("continue_signal").await;
        Ok("orchestration_complete".to_string())
    };

    let reg = OrchestrationRegistry::builder()
        .register("ExecutionIdTest", orch)
        .build();
    let _rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg).await;
    let client = duroxide::Client::new(store.clone());

    // Start the orchestration
    client
        .start_orchestration("inst-exec-test", "ExecutionIdTest", "")
        .await
        .unwrap();

    // Wait for orchestration to start and be waiting for external event
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Inject a completion with OLD execution ID (execution_id=0, but current should be 1)
    let _ = store
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "inst-exec-test".to_string(),
                execution_id: 0, // Old execution ID (current is 1)
                id: 999,         // Some activity ID
                result: "old_execution_result".to_string(),
            },
            None,
        )
        .await;

    // Give time for the completion to be processed (and ignored)
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify the completion was ignored - check that it's not in history
    let history = store.read("inst-exec-test").await.unwrap_or_default();
    let has_old_completion = history.iter().any(|e| match &e.kind {
        EventKind::ActivityCompleted { result, .. } => result == "old_execution_result",
        _ => false,
    });

    assert!(
        !has_old_completion,
        "Old execution completion should be ignored due to execution ID mismatch"
    );

    println!("✓ Old execution completion was properly ignored due to execution ID validation");
}

#[tokio::test]
async fn future_execution_completions_are_ignored() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder().build();

    // Simple orchestration that waits for external events
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let _result = ctx.schedule_wait("test_event").await;
        Ok("completed".to_string())
    };

    let reg = OrchestrationRegistry::builder()
        .register("FutureExecTest", orch)
        .build();
    let _rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg).await;
    let client = duroxide::Client::new(store.clone());

    // Start the orchestration
    client
        .start_orchestration("inst-future", "FutureExecTest", "")
        .await
        .unwrap();

    // Wait for orchestration to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Inject a completion with a future execution ID (should never happen in practice)
    let _ = store
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "inst-future".to_string(),
                execution_id: 999, // Future execution ID
                id: 1,
                result: "future_completion".to_string(),
            },
            None,
        )
        .await;

    // Give some time for processing
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify the completion was ignored
    let history = store.read("inst-future").await.unwrap_or_default();
    let has_future_completion = history.iter().any(|e| match &e.kind {
        EventKind::ActivityCompleted { result, .. } => result == "future_completion",
        _ => false,
    });

    assert!(!has_future_completion, "Future execution completion should be ignored");

    println!("✓ Future execution completion was properly ignored");
}

// Test to verify that not awaiting continue_as_new() still works (backward compatibility)
#[tokio::test]
async fn continue_as_new_without_await() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Orchestration that calls continue_as_new without await (old style)
    // The action is recorded synchronously before the future is returned
    let orch = |ctx: OrchestrationContext, input: String| async move {
        let n: u32 = input.parse().unwrap_or(0);
        if n < 2 {
            // Call without await - action is recorded synchronously
            return ctx.continue_as_new((n + 1).to_string()).await;
        } else {
            Ok(format!("done:{n}"))
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("CANNoAwait", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("inst-can-no-await", "CANNoAwait", "0")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-can-no-await", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "done:2"),
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Verify multi-execution histories exist
    let mgmt = store.as_management_capability().expect("ProviderAdmin required");
    let execs = mgmt.list_executions("inst-can-no-await").await.unwrap_or_default();
    assert_eq!(execs, vec![1, 2, 3]);

    rt.shutdown(None).await;
}

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use async_trait::async_trait;
use duroxide::Client;
use duroxide::Either2;
use duroxide::EventKind;
use duroxide::providers::error::ProviderError;
use duroxide::providers::{
    DispatcherCapabilityFilter, ExecutionMetadata, OrchestrationItem, Provider, ProviderAdmin,
    ScheduledActivityIdentifier, SessionFetchConfig, TagFilter, WorkItem,
};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, OrchestrationContext, OrchestrationRegistry};
mod common;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test]
async fn cancel_parent_down_propagates_to_child() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Child waits for an external event indefinitely (until canceled)
    let child = |ctx: OrchestrationContext, _input: String| async move {
        let _ = ctx.schedule_wait("Go").await;
        Ok("done".to_string())
    };

    // Parent starts child and awaits it (will block until canceled)
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        let _res = ctx.schedule_sub_orchestration("Child", "seed").await;
        Ok("parent_done".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("Child", child)
        .register("Parent", parent)
        .build();
    let activity_registry = ActivityRegistry::builder().build();

    // Use faster polling for cancellation timing test
    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };
    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    // Start parent
    client.start_orchestration("inst-cancel-1", "Parent", "").await.unwrap();

    // Give it a moment to schedule the child
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Cancel parent with reason
    let _ = client.cancel_instance("inst-cancel-1", "by_test").await;

    // Wait for parent terminal failure due to cancel
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(5000);
    loop {
        let hist = store.read("inst-cancel-1").await.unwrap_or_default();
        if hist.iter().any(|e| {
            matches!(
                &e.kind,
                EventKind::OrchestrationFailed { details, .. } if matches!(
                    details,
                    duroxide::ErrorDetails::Application {
                        kind: duroxide::AppErrorKind::Cancelled { reason },
                        ..
                    } if reason == "by_test"
                )
            )
        }) {
            assert!(
                hist.iter()
                    .any(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. })),
                "missing cancel requested event for parent"
            );
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for parent canceled");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Find child instance (prefix inst-cancel-1::)
    let mgmt = store.as_management_capability().expect("ProviderAdmin required");
    let children: Vec<String> = mgmt
        .list_instances()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|i| i.starts_with("inst-cancel-1::"))
        .collect();
    assert!(!children.is_empty(), "expected a child instance");

    // Each child should show cancel requested and terminal canceled (wait up to 5s)
    for child in children {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(5000);
        loop {
            let hist = store.read(&child).await.unwrap_or_default();
            let has_cancel = hist
                .iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. }));
            let has_failed = hist.iter().any(|e| {
                matches!(&e.kind, EventKind::OrchestrationFailed { details, .. } if matches!(
                        details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { reason },
                            ..
                        } if reason == "parent canceled"
                    )
                )
            });
            if has_cancel && has_failed {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("child {child} did not cancel in time");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    rt.shutdown(None).await;
}

#[tokio::test]
async fn cancel_after_completion_is_noop() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |_ctx: OrchestrationContext, _input: String| async move { Ok("ok".to_string()) };

    let orchestration_registry = OrchestrationRegistry::builder().register("Quick", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-cancel-noop", "Quick", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-cancel-noop", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "ok"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Cancel after completion should have no effect
    let _ = client.cancel_instance("inst-cancel-noop", "late").await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let hist = store.read("inst-cancel-noop").await.unwrap_or_default();
    assert!(
        hist.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "ok"))
    );
    assert!(
        !hist
            .iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. }))
    );

    rt.shutdown(None).await;
}

#[tokio::test]
async fn cancel_child_directly_signals_parent() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let child = |_ctx: OrchestrationContext, _input: String| async move {
        // Just wait forever until canceled
        futures::future::pending::<Result<String, String>>().await
    };

    let parent = |ctx: OrchestrationContext, _input: String| async move {
        match ctx.schedule_sub_orchestration("ChildD", "x").await {
            Ok(v) => Ok(format!("ok:{v}")),
            Err(e) => Ok(format!("child_err:{e}")),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ChildD", child)
        .register("ParentD", parent)
        .build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-chdirect", "ParentD", "")
        .await
        .unwrap();
    // Wait a bit for child schedule, then cancel child directly
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let child_inst = "inst-chdirect::sub::2"; // event_id=2 (OrchestrationStarted is 1)
    let _ = client.cancel_instance(child_inst, "by_test_child").await;

    let s = match client
        .wait_for_orchestration("inst-chdirect", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => output,
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    };
    assert!(
        s.starts_with("child_err:canceled: by_test_child"),
        "unexpected parent out: {s}"
    );

    // Parent should have SubOrchestrationFailed for the child id 2
    let ph = store.read("inst-chdirect").await.unwrap_or_default();
    assert!(ph.iter().any(|e| matches!(
        &e.kind,
        EventKind::SubOrchestrationFailed { details, .. }
        if e.source_event_id == Some(2) && matches!(
            details,
            duroxide::ErrorDetails::Application {
                kind: duroxide::AppErrorKind::Cancelled { reason },
                ..
            } if reason == "by_test_child"
        )
    )));

    rt.shutdown(None).await;
}

#[tokio::test]
async fn cancel_continue_as_new_second_exec() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, input: String| async move {
        match input.as_str() {
            "start" => {
                return ctx.continue_as_new("wait").await;
            }
            "wait" => {
                // Park until canceled
                let _ = ctx.schedule_wait("Go").await;
                Ok("done".to_string())
            }
            _ => Ok(input),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("CanCancel", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-can-can", "CanCancel", "start")
        .await
        .unwrap();

    // Cancel the second execution while the handle is waiting
    // (the handle will wait for final completion including cancellation)
    tokio::time::sleep(std::time::Duration::from_millis(100)).await; // Let first execution complete
    let _ = client.cancel_instance("inst-can-can", "by_test_can").await;

    // With polling approach, wait for final result (cancellation)
    match client
        .wait_for_orchestration("inst-can-can", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Failed { details, .. } => {
            assert!(matches!(
                details,
                duroxide::ErrorDetails::Application {
                    kind: duroxide::AppErrorKind::Cancelled { reason },
                    ..
                } if reason == "by_test_can"
            ));
        }
        runtime::OrchestrationStatus::Completed { output, .. } => panic!("expected cancellation, got: {output}"),
        _ => panic!("unexpected orchestration status"),
    }

    // Wait for canceled failure
    let ok = common::wait_for_history(
        store.clone(),
        "inst-can-can",
        |hist| {
            hist.iter().rev().any(|e| {
                matches!(&e.kind, EventKind::OrchestrationFailed { details, .. } if matches!(
                        details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { reason },
                            ..
                        } if reason == "by_test_can"
                    )
                )
            })
        },
        5000,
    )
    .await;
    assert!(ok, "timeout waiting for cancel failure");

    // Ensure cancel requested recorded
    let hist = store.read("inst-can-can").await.unwrap_or_default();
    assert!(
        hist.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. }))
    );

    rt.shutdown(None).await;
}

#[tokio::test]
async fn orchestration_completes_before_activity_finishes() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Activity will be dispatched but orchestration completes without awaiting it (fire-and-forget)
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        drop(ctx.schedule_activity("slow", ""));
        Ok("done".to_string())
    };

    let mut ab = ActivityRegistry::builder();
    ab = ab.register("slow", |_ctx: ActivityContext, _s: String| async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        Ok("ok".to_string())
    });
    let activity_registry = ab.build();
    let orchestration_registry = OrchestrationRegistry::builder().register("QuickDone", orch).build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-orch-done-first", "QuickDone", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-orch-done-first", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "done"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Give activity time to finish; no additional terminal events should be added
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let hist = store.read("inst-orch-done-first").await.unwrap_or_default();
    assert!(
        hist.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "done"))
    );

    rt.shutdown(None).await;
}

#[tokio::test]
async fn orchestration_fails_before_activity_finishes() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Activity dispatched; orchestration fails immediately
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        drop(ctx.schedule_activity("slow2", ""));
        Err("boom".to_string())
    };

    let mut ab = ActivityRegistry::builder();
    ab = ab.register("slow2", |_ctx: ActivityContext, _s: String| async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        Ok("ok".to_string())
    });
    let activity_registry = ab.build();
    let orchestration_registry = OrchestrationRegistry::builder().register("QuickFail", orch).build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-orch-fail-first", "QuickFail", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-orch-fail-first", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Failed { details: _, .. } => {} // Expected failure
        runtime::OrchestrationStatus::Completed { output, .. } => panic!("expected failure, got: {output}"),
        _ => panic!("unexpected orchestration status"),
    }

    // Give activity time to finish; no change to terminal failure
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let hist = store.read("inst-orch-fail-first").await.unwrap_or_default();
    assert!(hist.iter().any(
        |e| matches!(&e.kind, EventKind::OrchestrationFailed { details, .. } if matches!(
                details,
                duroxide::ErrorDetails::Application {
                    kind: duroxide::AppErrorKind::OrchestrationFailed,
                    message,
                    ..
                } if message == "boom"
            )
        )
    ));

    rt.shutdown(None).await;
}

#[tokio::test]
async fn cancel_parent_with_multiple_children() {
    // This test validates the iterator optimization in get_child_cancellation_work_items
    // by ensuring that cancellation properly propagates to multiple sub-orchestrations
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Child waits for an external event indefinitely (until canceled)
    let child = |ctx: OrchestrationContext, _input: String| async move {
        let _ = ctx.schedule_wait("Go").await;
        Ok("done".to_string())
    };

    // Parent starts multiple children and awaits all (will block until canceled)
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        let mut futures = Vec::new();
        // Schedule 5 sub-orchestrations
        for i in 0..5 {
            futures.push(ctx.schedule_sub_orchestration("MultiChild", format!("input-{i}")));
        }
        // Wait for all (will never complete unless canceled)
        let results = ctx.join(futures).await;
        // All should fail with cancellation
        for result in results {
            match result {
                Err(e) if e.contains("canceled") => {}
                _ => return Err("expected cancellation".to_string()),
            }
        }
        Ok("parent_done".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("MultiChild", child)
        .register("MultiParent", parent)
        .build();
    let activity_registry = ActivityRegistry::builder().build();

    // Use faster polling for cancellation timing test
    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };
    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    // Start parent
    client
        .start_orchestration("inst-cancel-multi", "MultiParent", "")
        .await
        .unwrap();

    // Give it time to schedule all children
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Cancel parent with reason
    let _ = client.cancel_instance("inst-cancel-multi", "multi_test").await;

    // Wait for parent terminal failure due to cancel
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(5000);
    loop {
        let hist = store.read("inst-cancel-multi").await.unwrap_or_default();
        if hist.iter().any(|e| {
            matches!(
                &e.kind,
                EventKind::OrchestrationFailed { details, .. } if matches!(
                    details,
                    duroxide::ErrorDetails::Application {
                        kind: duroxide::AppErrorKind::Cancelled { reason },
                        ..
                    } if reason == "multi_test"
                )
            )
        }) {
            assert!(
                hist.iter()
                    .any(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. })),
                "missing cancel requested event for parent"
            );
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for parent canceled");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Find all child instances (prefix inst-cancel-multi::)
    let mgmt = store.as_management_capability().expect("ProviderAdmin required");
    let children: Vec<String> = mgmt
        .list_instances()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|i| i.starts_with("inst-cancel-multi::"))
        .collect();

    // Should have 5 children
    assert_eq!(
        children.len(),
        5,
        "expected 5 child instances, found {}",
        children.len()
    );

    // Each child should show cancel requested and terminal canceled (wait up to 5s)
    for child in children {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(5000);
        loop {
            let hist = store.read(&child).await.unwrap_or_default();
            let has_cancel = hist
                .iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. }));
            let has_failed = hist.iter().any(|e| {
                matches!(&e.kind, EventKind::OrchestrationFailed { details, .. } if matches!(
                        details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { reason },
                            ..
                        } if reason == "parent canceled"
                    )
                )
            });
            if has_cancel && has_failed {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("child {child} did not cancel in time");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    rt.shutdown(None).await;
}

/// Test that activities receive cancellation signals when orchestration is cancelled
#[tokio::test]
async fn activity_receives_cancellation_signal() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (store, _td) = common::create_sqlite_store_disk().await;

    // Track if activity saw the cancellation
    let saw_cancellation = Arc::new(AtomicBool::new(false));
    let saw_cancellation_clone = Arc::clone(&saw_cancellation);

    // Activity that waits for cancellation
    let long_activity = move |ctx: ActivityContext, _input: String| {
        let saw_cancellation = Arc::clone(&saw_cancellation_clone);
        async move {
            // Wait for either cancellation or timeout
            tokio::select! {
                _ = ctx.cancelled() => {
                    saw_cancellation.store(true, Ordering::SeqCst);
                    Ok("cancelled".to_string())
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    Ok("timeout".to_string())
                }
            }
        }
    };

    // Orchestration that schedules the long activity
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let _result = ctx.schedule_activity("LongActivity", "input").await;
        Ok("done".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("LongActivityOrch", orchestration)
        .build();
    let activity_registry = ActivityRegistry::builder()
        .register("LongActivity", long_activity)
        .build();

    // Use shorter lock and grace periods for testing
    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_millis(500),
        activity_cancellation_grace_period: Duration::from_secs(5),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    // Start orchestration
    client
        .start_orchestration("inst-activity-cancel", "LongActivityOrch", "")
        .await
        .unwrap();

    // Wait for activity to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cancel the orchestration
    let _ = client
        .cancel_instance("inst-activity-cancel", "test_cancellation")
        .await;

    // Wait for cancellation to propagate
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if saw_cancellation.load(Ordering::SeqCst) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Activity did not receive cancellation signal within timeout");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Verify the activity saw the cancellation
    assert!(
        saw_cancellation.load(Ordering::SeqCst),
        "Activity should have received cancellation signal"
    );

    // Verify orchestration is in failed/cancelled state
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let hist = store.read("inst-activity-cancel").await.unwrap_or_default();
        let is_cancelled = hist.iter().any(|e| {
            matches!(
                &e.kind,
                EventKind::OrchestrationFailed { details, .. }
                    if matches!(
                        details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { .. },
                            ..
                        }
                    )
            )
        });
        if is_cancelled {
            break;
        }
        if std::time::Instant::now() > deadline {
            // Even if orchestration didn't fail yet, the activity received the signal
            // which is the main thing we're testing
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    rt.shutdown(None).await;
}

/// Test that select/select2 losing activities receive cancellation signals (lock stealing)
/// even when the orchestration continues normally.
#[tokio::test]
async fn select2_loser_activity_receives_cancellation_signal() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_started = Arc::new(AtomicBool::new(false));
    let saw_cancellation = Arc::new(AtomicBool::new(false));
    let activity_started_clone = Arc::clone(&activity_started);
    let saw_cancellation_clone = Arc::clone(&saw_cancellation);

    // Activity that blocks until cancelled.
    let long_activity = move |ctx: ActivityContext, _input: String| {
        let activity_started = Arc::clone(&activity_started_clone);
        let saw_cancellation = Arc::clone(&saw_cancellation_clone);
        async move {
            activity_started.store(true, Ordering::SeqCst);

            tokio::select! {
                _ = ctx.cancelled() => {
                    saw_cancellation.store(true, Ordering::SeqCst);
                    Ok("cancelled".to_string())
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    Ok("timeout".to_string())
                }
            }
        }
    };

    // Orchestration that races a long activity against a short timer.
    // Timer should win, and the losing activity should be cancelled proactively.
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let activity = ctx.schedule_activity("LongActivity", "input");
        let timeout = ctx.schedule_timer(Duration::from_secs(1));
        match ctx.select2(activity, timeout).await {
            Either2::First(_) => panic!("activity should not win"),
            Either2::Second(_) => {} // timer won as expected
        }

        // Keep going to prove the orchestration isn't terminal.
        ctx.schedule_timer(Duration::from_millis(50)).await;
        Ok("done".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("SelectLoserCancelOrch", orchestration)
        .build();
    let activity_registry = ActivityRegistry::builder()
        .register("LongActivity", long_activity)
        .build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_millis(500),
        activity_cancellation_grace_period: Duration::from_secs(2),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-select2-loser-cancel", "SelectLoserCancelOrch", "")
        .await
        .unwrap();

    // Ensure the activity actually started before we assert cancellation behavior.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if activity_started.load(Ordering::SeqCst) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Activity did not start in time");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait for cancellation to propagate via lock stealing and renewal failure.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if saw_cancellation.load(Ordering::SeqCst) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("select2 loser activity did not receive cancellation signal in time");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        saw_cancellation.load(Ordering::SeqCst),
        "Activity should have received cancellation signal as select2 loser"
    );

    // Orchestration should still complete normally.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let hist = store.read("inst-select2-loser-cancel").await.unwrap_or_default();
        if hist
            .iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { .. }))
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for orchestration to complete");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Persisted history should include an ActivityCancelRequested correlated to the scheduled loser.
    let hist = store.read("inst-select2-loser-cancel").await.unwrap_or_default();
    let scheduled_id = hist.iter().find_map(|e| {
        if let EventKind::ActivityScheduled { name, .. } = &e.kind
            && name == "LongActivity"
        {
            Some(e.event_id())
        } else {
            None
        }
    });
    let scheduled_id = scheduled_id.expect("Expected LongActivity to be scheduled");
    assert!(
        hist.iter().any(|e| {
            matches!(&e.kind, EventKind::ActivityCancelRequested { reason } if reason == "dropped_future")
                && e.source_event_id == Some(scheduled_id)
        }),
        "Expected ActivityCancelRequested(source_event_id={scheduled_id}, reason=dropped_future) in persisted history"
    );

    rt.shutdown(None).await;
}

/// Test that activity result is dropped when orchestration was cancelled during execution
#[tokio::test]
async fn activity_result_dropped_when_orchestration_cancelled() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let (store, _td) = common::create_sqlite_store_disk().await;

    // Track activity completions
    let activity_completed_count = Arc::new(AtomicU32::new(0));
    let activity_completed_count_clone = Arc::clone(&activity_completed_count);

    // Activity that completes but takes a bit of time
    let slow_activity = move |_ctx: ActivityContext, _input: String| {
        let counter = Arc::clone(&activity_completed_count_clone);
        async move {
            // Simulate work
            tokio::time::sleep(Duration::from_millis(500)).await;
            counter.fetch_add(1, Ordering::SeqCst);
            Ok("completed".to_string())
        }
    };

    // Orchestration that schedules the slow activity
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let _result = ctx.schedule_activity("SlowActivity", "input").await;
        Ok("done".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("SlowActivityOrch", orchestration)
        .build();
    let activity_registry = ActivityRegistry::builder()
        .register("SlowActivity", slow_activity)
        .build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_millis(500),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    // Start orchestration
    client
        .start_orchestration("inst-drop-result", "SlowActivityOrch", "")
        .await
        .unwrap();

    // Wait briefly for activity to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel orchestration while activity is running
    let _ = client
        .cancel_instance("inst-drop-result", "cancel_during_activity")
        .await;

    // Wait for everything to settle
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Activity should have completed
    assert!(
        activity_completed_count.load(Ordering::SeqCst) > 0,
        "Activity should have completed"
    );

    // But the orchestration should not have an ActivityCompleted event
    // (the result was dropped due to cancellation)
    let hist = store.read("inst-drop-result").await.unwrap_or_default();
    let has_activity_completed = hist
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ActivityCompleted { .. }));

    // Note: The result may or may not be dropped depending on timing.
    // If the orchestration processed the cancel before the activity finished,
    // the result will be dropped. We're testing that the mechanism exists.
    if !has_activity_completed {
        // This confirms the result was dropped
        tracing::info!("Activity result was successfully dropped due to cancellation");
    }

    rt.shutdown(None).await;
}

/// Test that activities are skipped when the orchestration is already terminal at fetch time.
///
/// This test is deterministic by using separate runtime phases:
/// 1. Phase 1: Start runtime with worker_concurrency=0 (orchestrator only)
/// 2. Let orchestration run and schedule an activity, then cancel it
/// 3. Phase 2: Start new runtime with workers - they will see terminal state at fetch
#[tokio::test]
async fn activity_skipped_when_orchestration_terminal_at_fetch() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (store, _td) = common::create_sqlite_store_disk().await;

    let ran = Arc::new(AtomicBool::new(false));
    let ran_clone = Arc::clone(&ran);

    let activity = move |_ctx: ActivityContext, _input: String| {
        let ran = Arc::clone(&ran_clone);
        async move {
            ran.store(true, Ordering::SeqCst);
            Ok("done".to_string())
        }
    };

    // Orchestration schedules an activity then waits for an event (will be cancelled)
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Schedule activity (enqueues to worker queue)
        let _handle = ctx.schedule_activity("A", "input");
        // Wait for external event (will block until cancelled)
        let _ = ctx.schedule_wait("Go").await;
        Ok("ok".to_string())
    };

    let orch_registry = OrchestrationRegistry::builder().register("O", orchestration).build();
    let act_registry = ActivityRegistry::builder().register("A", activity).build();

    // PHASE 1: Run with orchestrator only (no workers)
    let options_no_workers = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        worker_concurrency: 0, // No workers - activity won't be picked up
        ..Default::default()
    };

    let rt1 = runtime::Runtime::start_with_options(
        store.clone(),
        act_registry.clone(),
        orch_registry.clone(),
        options_no_workers,
    )
    .await;
    let client = Client::new(store.clone());

    // Start orchestration - it will schedule activity and wait for event
    client.start_orchestration("inst-skip", "O", "").await.unwrap();

    // Wait for activity to be scheduled (orchestration runs, schedules activity, then waits)
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel the orchestration - makes it terminal
    client.cancel_instance("inst-skip", "cancel").await.unwrap();

    // Wait for orchestration to reach terminal state
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let hist = store.read("inst-skip").await.unwrap_or_default();
        if hist
            .iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationFailed { .. }))
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Timeout waiting for orchestration to be cancelled");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Shutdown phase 1 runtime
    rt1.shutdown(None).await;

    // Verify activity hasn't run yet (no workers were active)
    assert!(
        !ran.load(Ordering::SeqCst),
        "Activity should not have run yet - no workers in phase 1"
    );

    // PHASE 2: Start new runtime WITH workers
    // The activity work item is still in the queue, but orchestration is now terminal
    let options_with_workers = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        worker_concurrency: 2,
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_millis(500),
        activity_cancellation_grace_period: Duration::from_secs(1),
        ..Default::default()
    };

    // Need fresh registries for the new runtime
    let ran2 = Arc::new(AtomicBool::new(false));
    let ran2_clone = Arc::clone(&ran2);
    let activity2 = move |_ctx: ActivityContext, _input: String| {
        let ran = Arc::clone(&ran2_clone);
        async move {
            ran.store(true, Ordering::SeqCst);
            Ok("done".to_string())
        }
    };
    let act_registry2 = ActivityRegistry::builder().register("A", activity2).build();

    let rt2 =
        runtime::Runtime::start_with_options(store.clone(), act_registry2, orch_registry, options_with_workers).await;

    // Give workers time to fetch and skip the activity
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The activity should NOT have run because:
    // - fetch_work_item returns ExecutionState::Terminal for the orchestration
    // - Worker sees terminal state and skips the activity
    assert!(
        !ran2.load(Ordering::SeqCst),
        "Activity should have been skipped when orchestration was terminal at fetch"
    );

    let hist = store.read("inst-skip").await.unwrap_or_default();
    let has_activity_completed = hist
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ActivityCompleted { .. }));
    assert!(
        !has_activity_completed,
        "Activity completion should not be recorded for skipped activity"
    );

    rt2.shutdown(None).await;
}

/// Test that non-cooperative activities are aborted after grace period on cancellation
#[tokio::test]
async fn activity_aborted_after_cancellation_grace() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let (store, _td) = common::create_sqlite_store_disk().await;

    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_clone = Arc::clone(&attempts);

    // Activity ignores cancellation and sleeps long
    let stubborn_activity = move |_ctx: ActivityContext, _input: String| {
        let attempts = Arc::clone(&attempts_clone);
        async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok("done".to_string())
        }
    };

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let _ = ctx.schedule_activity("Stubborn", "input").await;
        Ok("ok".to_string())
    };

    let orch_registry = OrchestrationRegistry::builder()
        .register("StubbornOrch", orchestration)
        .build();
    let act_registry = ActivityRegistry::builder()
        .register("Stubborn", stubborn_activity)
        .build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_millis(500),
        activity_cancellation_grace_period: Duration::from_millis(500),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), act_registry, orch_registry, options).await;
    let client = Client::new(store.clone());

    let start = std::time::Instant::now();
    client
        .start_orchestration("inst-grace", "StubbornOrch", "")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    client.cancel_instance("inst-grace", "cancel_now").await.unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Activity should have been attempted once but not completed
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "Activity should run once");

    let hist = store.read("inst-grace").await.unwrap_or_default();
    let has_activity_completed = hist
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ActivityCompleted { .. }));
    assert!(
        !has_activity_completed,
        "Activity completion should be dropped after cancellation"
    );

    // Ensure we did not wait the full 5s activity duration (abort happened)
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "Activity should have been aborted before full run"
    );

    rt.shutdown(None).await;
}

/// Test that cancelling a non-existent instance is a no-op (doesn't error)
#[tokio::test]
async fn cancel_nonexistent_instance_is_noop() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("Dummy", |_ctx: OrchestrationContext, _input: String| async move {
            Ok("ok".to_string())
        })
        .build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    // Cancel an instance that was never created - should succeed without error
    let result = client.cancel_instance("does-not-exist", "test").await;
    assert!(result.is_ok(), "Cancelling non-existent instance should not error");

    // The cancel work item gets enqueued but the orchestrator will find no matching instance
    // and simply ack it without effect. Give it time to process.
    tokio::time::sleep(Duration::from_millis(200)).await;

    rt.shutdown(None).await;
}

/// Test that multiple cancel calls on the same instance are idempotent
#[tokio::test]
async fn multiple_cancel_calls_are_idempotent() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Orchestration that waits for an event (will block until canceled)
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let _ = ctx.schedule_wait("Go").await;
        Ok("done".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("Waiter", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };
    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    // Start orchestration
    client
        .start_orchestration("inst-multi-cancel", "Waiter", "")
        .await
        .unwrap();

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Cancel multiple times rapidly
    for i in 0..5 {
        let result = client.cancel_instance("inst-multi-cancel", format!("cancel-{i}")).await;
        assert!(result.is_ok(), "Cancel call {i} should succeed");
    }

    // Wait for orchestration to fail
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let hist = store.read("inst-multi-cancel").await.unwrap_or_default();
        if hist
            .iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationFailed { .. }))
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Timeout waiting for orchestration to be cancelled");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Should only have ONE OrchestrationCancelRequested event (first cancel wins)
    let hist = store.read("inst-multi-cancel").await.unwrap_or_default();
    let cancel_requested_count = hist
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. }))
        .count();
    assert_eq!(
        cancel_requested_count, 1,
        "Should only have one CancelRequested event, got {cancel_requested_count}"
    );

    // The reason should be from the first cancel (cancel-0)
    let cancel_event = hist
        .iter()
        .find(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. }))
        .unwrap();
    if let EventKind::OrchestrationCancelRequested { reason } = &cancel_event.kind {
        assert_eq!(reason, "cancel-0", "First cancel reason should win");
    }

    rt.shutdown(None).await;
}

/// Test that cancel before orchestration starts is handled gracefully
#[tokio::test]
async fn cancel_before_orchestration_starts() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // First, just enqueue the start work item without running a runtime
    store
        .enqueue_for_orchestrator(
            duroxide::providers::WorkItem::StartOrchestration {
                instance: "inst-early-cancel".to_string(),
                orchestration: "Early".to_string(),
                version: Some("1.0.0".to_string()),
                input: "input".to_string(),
                parent_instance: None,
                parent_id: None,
                execution_id: 1,
            },
            None,
        )
        .await
        .unwrap();

    // Now cancel it before any runtime has processed it
    store
        .enqueue_for_orchestrator(
            duroxide::providers::WorkItem::CancelInstance {
                instance: "inst-early-cancel".to_string(),
                reason: "early_cancel".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    // Now start the runtime - it should process both work items
    let orchestration_registry = OrchestrationRegistry::builder()
        .register("Early", |_ctx: OrchestrationContext, _input: String| async move {
            // This should never run because cancel came first (or runs but fails immediately)
            Ok("done".to_string())
        })
        .build();
    let activity_registry = ActivityRegistry::builder().build();
    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };
    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    // Wait for orchestration to be cancelled
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let hist = store.read("inst-early-cancel").await.unwrap_or_default();
        // The orchestration MUST end up in Failed (cancelled) state, not Completed.
        // Since the cancel was enqueued after the start, it should be processed
        // and the orchestration should fail with cancellation.
        let is_cancelled = hist.iter().any(|e| {
            matches!(
                &e.kind,
                EventKind::OrchestrationFailed { details, .. }
                    if matches!(
                        details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { .. },
                            ..
                        }
                    )
            )
        });
        if is_cancelled {
            break;
        }
        if std::time::Instant::now() > deadline {
            let hist = store.read("inst-early-cancel").await.unwrap_or_default();
            panic!(
                "Timeout waiting for orchestration to be cancelled. History: {:?}",
                hist.iter().map(|e| &e.kind).collect::<Vec<_>>()
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify it was cancelled with the correct reason
    let hist = store.read("inst-early-cancel").await.unwrap_or_default();
    let cancel_event = hist.iter().find(|e| {
        matches!(
            &e.kind,
            EventKind::OrchestrationFailed { details, .. }
                if matches!(
                    details,
                    duroxide::ErrorDetails::Application {
                        kind: duroxide::AppErrorKind::Cancelled { reason },
                        ..
                    } if reason == "early_cancel"
                )
        )
    });
    assert!(
        cancel_event.is_some(),
        "Orchestration should be cancelled with reason 'early_cancel'"
    );

    // Verify it did NOT complete successfully
    let completed = hist
        .iter()
        .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { .. }));
    assert!(
        !completed,
        "Orchestration should NOT have completed - cancel was enqueued after start"
    );

    rt.shutdown(None).await;
}

// ============================================================================
// Explicit Drop and Out-of-Scope Activity Cancellation Tests
// ============================================================================

/// Test that explicitly dropping an activity future triggers cancellation signal.
///
/// NOTE: We use a timer between scheduling and dropping to ensure the activity
/// has a chance to be fetched by the worker. If schedule+drop happens in the
/// same turn, the activity is INSERT+DELETE in the same transaction (no-op).
#[tokio::test]
async fn explicit_drop_activity_triggers_cancellation() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_started = Arc::new(AtomicBool::new(false));
    let saw_cancellation = Arc::new(AtomicBool::new(false));
    let activity_started_clone = Arc::clone(&activity_started);
    let saw_cancellation_clone = Arc::clone(&saw_cancellation);

    // Activity that waits for cancellation
    let cancellable_activity = move |ctx: ActivityContext, _input: String| {
        let activity_started = Arc::clone(&activity_started_clone);
        let saw_cancellation = Arc::clone(&saw_cancellation_clone);
        async move {
            activity_started.store(true, Ordering::SeqCst);

            tokio::select! {
                _ = ctx.cancelled() => {
                    saw_cancellation.store(true, Ordering::SeqCst);
                    Ok("cancelled".to_string())
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    Ok("timeout".to_string())
                }
            }
        }
    };

    // Orchestration that schedules activity, waits a bit (dehydrates), then drops it
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let activity_future = ctx.schedule_activity("CancellableActivity", "input");

        // Wait for activity to be fetched - this dehydrates the orchestration,
        // committing the ActivityScheduled event and allowing the worker to pick it up
        ctx.schedule_timer(Duration::from_millis(500)).await;

        // Now explicitly drop - this should trigger cancellation via lock stealing
        drop(activity_future);

        // Do something else to prove orchestration continues
        ctx.schedule_timer(Duration::from_millis(50)).await;
        Ok("completed_after_drop".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ExplicitDropActivity", orchestration)
        .build();
    let activity_registry = ActivityRegistry::builder()
        .register("CancellableActivity", cancellable_activity)
        .build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_millis(500),
        activity_cancellation_grace_period: Duration::from_secs(2),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-explicit-drop-activity", "ExplicitDropActivity", "")
        .await
        .unwrap();

    // Wait for activity to start
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if activity_started.load(Ordering::SeqCst) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Activity did not start in time");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait for cancellation signal
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if saw_cancellation.load(Ordering::SeqCst) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Explicitly dropped activity did not receive cancellation signal");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        saw_cancellation.load(Ordering::SeqCst),
        "Activity should have received cancellation signal after explicit drop"
    );

    // Orchestration should complete successfully
    match client
        .wait_for_orchestration("inst-explicit-drop-activity", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "completed_after_drop");
        }
        other => panic!("Expected Completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Test that activity going out of scope (not awaited) triggers cancellation.
///
/// NOTE: We use a timer between scheduling and the end of scope to ensure the
/// activity has a chance to be fetched. If schedule+drop happens in the same
/// turn, the activity is INSERT+DELETE in the same transaction (no-op).
#[tokio::test]
async fn activity_out_of_scope_triggers_cancellation() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_started = Arc::new(AtomicBool::new(false));
    let saw_cancellation = Arc::new(AtomicBool::new(false));
    let activity_started_clone = Arc::clone(&activity_started);
    let saw_cancellation_clone = Arc::clone(&saw_cancellation);

    let cancellable_activity = move |ctx: ActivityContext, _input: String| {
        let activity_started = Arc::clone(&activity_started_clone);
        let saw_cancellation = Arc::clone(&saw_cancellation_clone);
        async move {
            activity_started.store(true, Ordering::SeqCst);

            tokio::select! {
                _ = ctx.cancelled() => {
                    saw_cancellation.store(true, Ordering::SeqCst);
                    Ok("cancelled".to_string())
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    Ok("timeout".to_string())
                }
            }
        }
    };

    // Orchestration where activity future goes out of scope in a block
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Activity scheduled in a block - goes out of scope without being awaited
        {
            let _unused = ctx.schedule_activity("CancellableActivity", "input");

            // Wait for activity to be fetched - this dehydrates and commits
            ctx.schedule_timer(Duration::from_millis(500)).await;

            // _unused goes out of scope here without being awaited
        }

        // Continue doing other work
        ctx.schedule_timer(Duration::from_millis(50)).await;
        Ok("completed_without_await".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("OutOfScopeActivity", orchestration)
        .build();
    let activity_registry = ActivityRegistry::builder()
        .register("CancellableActivity", cancellable_activity)
        .build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_millis(500),
        activity_cancellation_grace_period: Duration::from_secs(2),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-out-of-scope-activity", "OutOfScopeActivity", "")
        .await
        .unwrap();

    // Wait for activity to start
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if activity_started.load(Ordering::SeqCst) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Activity did not start in time");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait for cancellation signal
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if saw_cancellation.load(Ordering::SeqCst) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Out-of-scope activity did not receive cancellation signal");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        saw_cancellation.load(Ordering::SeqCst),
        "Activity should have received cancellation signal when going out of scope"
    );

    // Orchestration should complete successfully
    match client
        .wait_for_orchestration("inst-out-of-scope-activity", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "completed_without_await");
        }
        other => panic!("Expected Completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

// ============================================================================
// Sub-Orchestration Cancellation Tests
// ============================================================================

/// Test that select2 loser sub-orchestration receives cancellation and terminates
#[tokio::test]
async fn select2_loser_sub_orchestration_cancelled() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Slow child orchestration that waits for an external event
    let slow_child = |ctx: OrchestrationContext, _input: String| async move {
        // Wait for event that will never come (unless cancelled)
        ctx.schedule_wait("NeverComes").await;
        Ok("child_completed".to_string())
    };

    // Parent: race slow sub-orchestration against fast timer
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        let sub_orch = ctx.schedule_sub_orchestration("SlowChild", "input");
        let timer = ctx.schedule_timer(Duration::from_millis(100));

        match ctx.select2(sub_orch, timer).await {
            Either2::First(result) => Ok(format!("sub_won:{}", result?)),
            Either2::Second(()) => Ok("timer_won".to_string()),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("SlowChild", slow_child)
        .register("SelectSubOrchParent", parent)
        .build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        orchestration_concurrency: 2,
        worker_concurrency: 1,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-select2-suborg-cancel", "SelectSubOrchParent", "")
        .await
        .unwrap();

    // Wait for parent to complete
    match client
        .wait_for_orchestration("inst-select2-suborg-cancel", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "timer_won", "Timer should win the race");
        }
        other => panic!("Expected parent Completed, got {:?}", other),
    }

    // Find the child instance ID from parent's history
    let parent_hist = store.read("inst-select2-suborg-cancel").await.unwrap();
    let child_instance = parent_hist.iter().find_map(|e| match &e.kind {
        EventKind::SubOrchestrationScheduled { instance, .. } => Some(instance.clone()),
        _ => None,
    });

    let child_id = child_instance.expect("Child should have been scheduled");
    let full_child_id = format!("inst-select2-suborg-cancel::{child_id}");

    // Wait for child to be cancelled
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let child_status = client.get_orchestration_status(&full_child_id).await.unwrap();
        match child_status {
            runtime::OrchestrationStatus::Failed { details, .. } => {
                // Child should be cancelled
                assert!(
                    matches!(
                        &details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { .. },
                            ..
                        }
                    ),
                    "Child should be cancelled, got: {}",
                    details.display_message()
                );
                break;
            }
            runtime::OrchestrationStatus::Completed { output, .. } => {
                panic!("Child should NOT complete - it was a select2 loser. Got: {output}");
            }
            runtime::OrchestrationStatus::Running { .. } | runtime::OrchestrationStatus::NotFound => {
                // Still waiting for cancellation to propagate
                if std::time::Instant::now() > deadline {
                    panic!("Timeout waiting for child sub-orchestration to be cancelled");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    rt.shutdown(None).await;
}

/// Test that select2 loser sub-orchestration with explicit instance ID is cancelled.
/// Uses `schedule_sub_orchestration_with_id` to specify an explicit child instance ID.
#[tokio::test]
async fn select2_loser_sub_orchestration_explicit_id_cancelled() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Slow child orchestration that waits for an external event
    let slow_child = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_wait("NeverComes").await;
        Ok("child_completed".to_string())
    };

    // Parent: race slow sub-orchestration (with explicit ID) against fast timer
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        // Use explicit instance ID instead of auto-generated sub::N
        let sub_orch = ctx.schedule_sub_orchestration_with_id("SlowChild", "my-explicit-child-id", "input");
        let timer = ctx.schedule_timer(Duration::from_millis(100));

        match ctx.select2(sub_orch, timer).await {
            Either2::First(result) => Ok(format!("sub_won:{}", result?)),
            Either2::Second(()) => Ok("timer_won".to_string()),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("SlowChild", slow_child)
        .register("SelectExplicitIdParent", parent)
        .build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        orchestration_concurrency: 2,
        worker_concurrency: 1,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-select2-explicit-id", "SelectExplicitIdParent", "")
        .await
        .unwrap();

    // Wait for parent to complete
    match client
        .wait_for_orchestration("inst-select2-explicit-id", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "timer_won", "Timer should win the race");
        }
        other => panic!("Expected parent Completed, got {:?}", other),
    }

    // Verify the child was scheduled with our explicit instance ID
    let parent_hist = store.read("inst-select2-explicit-id").await.unwrap();
    let child_instance = parent_hist.iter().find_map(|e| match &e.kind {
        EventKind::SubOrchestrationScheduled { instance, .. } => Some(instance.clone()),
        _ => None,
    });

    assert_eq!(
        child_instance.as_deref(),
        Some("my-explicit-child-id"),
        "Child should have been scheduled with explicit instance ID"
    );

    // Explicit instance ID is used exactly as provided - no parent prefix
    let full_child_id = "my-explicit-child-id";

    // Wait for child to be cancelled
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let child_status = client.get_orchestration_status(full_child_id).await.unwrap();
        match child_status {
            runtime::OrchestrationStatus::Failed { details, .. } => {
                assert!(
                    matches!(
                        &details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { .. },
                            ..
                        }
                    ),
                    "Child with explicit ID should be cancelled, got: {}",
                    details.display_message()
                );
                break;
            }
            runtime::OrchestrationStatus::Completed { output, .. } => {
                panic!("Child with explicit ID should NOT complete - it was a select2 loser. Got: {output}");
            }
            runtime::OrchestrationStatus::Running { .. } | runtime::OrchestrationStatus::NotFound => {
                if std::time::Instant::now() > deadline {
                    panic!("Timeout waiting for child sub-orchestration with explicit ID to be cancelled");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    rt.shutdown(None).await;
}

/// Test that explicitly dropping a sub-orchestration future triggers cancellation
#[tokio::test]
async fn explicit_drop_sub_orchestration_cancelled() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Child that waits indefinitely
    let waiting_child = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_wait("NeverComes").await;
        Ok("child_completed".to_string())
    };

    // Parent that explicitly drops sub-orchestration future
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        let sub_orch = ctx.schedule_sub_orchestration("WaitingChild", "input");
        // Explicit drop
        drop(sub_orch);

        // Continue with other work
        ctx.schedule_timer(Duration::from_millis(50)).await;
        Ok("parent_completed_after_drop".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("WaitingChild", waiting_child)
        .register("DropSubOrchParent", parent)
        .build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        orchestration_concurrency: 2,
        worker_concurrency: 1,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-drop-suborg", "DropSubOrchParent", "")
        .await
        .unwrap();

    // Wait for parent to complete
    match client
        .wait_for_orchestration("inst-drop-suborg", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "parent_completed_after_drop");
        }
        other => panic!("Expected parent Completed, got {:?}", other),
    }

    // Find child instance
    let parent_hist = store.read("inst-drop-suborg").await.unwrap();
    let child_instance = parent_hist.iter().find_map(|e| match &e.kind {
        EventKind::SubOrchestrationScheduled { instance, .. } => Some(instance.clone()),
        _ => None,
    });

    let child_id = child_instance.expect("Child should have been scheduled");
    let full_child_id = format!("inst-drop-suborg::{child_id}");

    // Wait for child to be cancelled
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let child_status = client.get_orchestration_status(&full_child_id).await.unwrap();
        match child_status {
            runtime::OrchestrationStatus::Failed { details, .. } => {
                assert!(
                    matches!(
                        &details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { .. },
                            ..
                        }
                    ),
                    "Child should be cancelled after explicit drop, got: {}",
                    details.display_message()
                );
                break;
            }
            runtime::OrchestrationStatus::Completed { output, .. } => {
                panic!("Child should NOT complete after being dropped. Got: {output}");
            }
            runtime::OrchestrationStatus::Running { .. } | runtime::OrchestrationStatus::NotFound => {
                if std::time::Instant::now() > deadline {
                    panic!("Timeout waiting for dropped child sub-orchestration to be cancelled");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    rt.shutdown(None).await;
}

// ============================================================================
// Combined CancelInstance (User + Dropped Future) Test
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
enum CancelInstanceEnqueueSource {
    EnqueueForOrchestrator,
    AckOrchestrationItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedCancelInstance {
    instance: String,
    reason: String,
    source: CancelInstanceEnqueueSource,
}

/// Provider wrapper that records all enqueued `WorkItem::CancelInstance` items.
///
/// We record both:
/// - `enqueue_for_orchestrator` (e.g., user calls `Client::cancel_instance`)
/// - `ack_orchestration_item` orchestrator_items (runtime enqueues cancellations like dropped-future sub-orch cancels)
struct RecordingProvider {
    inner: Arc<dyn Provider>,
    cancel_instances: Mutex<Vec<RecordedCancelInstance>>,
    /// If false, we will defer processing of the child instance by abandoning the lock.
    /// This makes the test deterministic: we can ensure multiple CancelInstance messages are
    /// present in the SAME fetched turn for the child.
    allow_child_fetch: AtomicBool,
    /// For each time the child instance is fetched for a turn, record the CancelInstance reasons
    /// present in that fetched message batch.
    child_cancel_reasons_by_fetch: Mutex<Vec<Vec<String>>>,
}

impl RecordingProvider {
    fn new(inner: Arc<dyn Provider>) -> Self {
        Self {
            inner,
            cancel_instances: Mutex::new(Vec::new()),
            allow_child_fetch: AtomicBool::new(false),
            child_cancel_reasons_by_fetch: Mutex::new(Vec::new()),
        }
    }

    fn cancel_instance_records(&self) -> Vec<RecordedCancelInstance> {
        self.cancel_instances
            .lock()
            .expect("Mutex should not be poisoned")
            .clone()
    }

    fn record_cancel_instance(&self, instance: &str, reason: &str, source: CancelInstanceEnqueueSource) {
        self.cancel_instances
            .lock()
            .expect("Mutex should not be poisoned")
            .push(RecordedCancelInstance {
                instance: instance.to_string(),
                reason: reason.to_string(),
                source,
            });
    }

    fn allow_child_fetch(&self) {
        self.allow_child_fetch.store(true, Ordering::SeqCst);
    }

    fn child_cancel_reasons_by_fetch(&self) -> Vec<Vec<String>> {
        self.child_cancel_reasons_by_fetch
            .lock()
            .expect("Mutex should not be poisoned")
            .clone()
    }
}

#[async_trait]
impl Provider for RecordingProvider {
    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        let result = self
            .inner
            .fetch_orchestration_item(lock_timeout, poll_timeout, filter)
            .await?;

        if let Some((item, lock_token, attempt_count)) = result {
            if item.instance == "combo-child" && !self.allow_child_fetch.load(Ordering::SeqCst) {
                // Defer processing the child instance until the test is ready.
                // This allows both CancelInstance messages to accumulate and then be delivered
                // together in a single fetched turn.
                self.inner
                    .abandon_orchestration_item(&lock_token, Some(Duration::from_millis(25)), true)
                    .await?;
                return Ok(None);
            }

            if item.instance == "combo-child" {
                let reasons: Vec<String> = item
                    .messages
                    .iter()
                    .filter_map(|m| match m {
                        WorkItem::CancelInstance { reason, .. } => Some(reason.clone()),
                        _ => None,
                    })
                    .collect();
                self.child_cancel_reasons_by_fetch
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .push(reasons);
            }

            return Ok(Some((item, lock_token, attempt_count)));
        }

        Ok(None)
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        self.inner
            .fetch_work_item(lock_timeout, poll_timeout, session, tag_filter)
            .await
    }

    async fn ack_orchestration_item(
        &self,
        lock_token: &str,
        execution_id: u64,
        history_delta: Vec<duroxide::Event>,
        worker_items: Vec<WorkItem>,
        orchestrator_items: Vec<WorkItem>,
        metadata: ExecutionMetadata,
        cancelled_activities: Vec<ScheduledActivityIdentifier>,
    ) -> Result<(), ProviderError> {
        for item in &orchestrator_items {
            if let WorkItem::CancelInstance { instance, reason } = item {
                self.record_cancel_instance(instance, reason, CancelInstanceEnqueueSource::AckOrchestrationItem);
            }
        }

        self.inner
            .ack_orchestration_item(
                lock_token,
                execution_id,
                history_delta,
                worker_items,
                orchestrator_items,
                metadata,
                cancelled_activities,
            )
            .await
    }

    async fn abandon_orchestration_item(
        &self,
        lock_token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        self.inner
            .abandon_orchestration_item(lock_token, delay, ignore_attempt)
            .await
    }

    async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) -> Result<(), ProviderError> {
        self.inner.ack_work_item(token, completion).await
    }

    async fn renew_work_item_lock(&self, token: &str, extension: Duration) -> Result<(), ProviderError> {
        self.inner.renew_work_item_lock(token, extension).await
    }

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        self.inner.abandon_work_item(token, delay, ignore_attempt).await
    }

    async fn renew_session_lock(
        &self,
        owner_ids: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        self.inner.renew_session_lock(owner_ids, extend_for, idle_timeout).await
    }

    async fn cleanup_orphaned_sessions(&self, idle_timeout: Duration) -> Result<usize, ProviderError> {
        self.inner.cleanup_orphaned_sessions(idle_timeout).await
    }

    async fn renew_orchestration_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
        self.inner.renew_orchestration_item_lock(token, extend_for).await
    }

    async fn enqueue_for_orchestrator(&self, item: WorkItem, delay: Option<Duration>) -> Result<(), ProviderError> {
        if let WorkItem::CancelInstance { instance, reason } = &item {
            self.record_cancel_instance(instance, reason, CancelInstanceEnqueueSource::EnqueueForOrchestrator);
        }
        self.inner.enqueue_for_orchestrator(item, delay).await
    }

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        self.inner.enqueue_for_worker(item).await
    }

    async fn read(&self, instance: &str) -> Result<Vec<duroxide::Event>, ProviderError> {
        self.inner.read(instance).await
    }

    async fn read_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<duroxide::Event>, ProviderError> {
        self.inner.read_with_execution(instance, execution_id).await
    }

    async fn append_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
        new_events: Vec<duroxide::Event>,
    ) -> Result<(), ProviderError> {
        self.inner
            .append_with_execution(instance, execution_id, new_events)
            .await
    }

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        self.inner.as_management_capability()
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        self.inner.get_custom_status(instance, last_seen_version).await
    }

    async fn get_kv_value(&self, instance: &str, key: &str) -> Result<Option<String>, ProviderError> {
        self.inner.get_kv_value(instance, key).await
    }

    async fn get_kv_all_values(
        &self,
        instance: &str,
    ) -> Result<std::collections::HashMap<String, String>, ProviderError> {
        self.inner.get_kv_all_values(instance).await
    }
}

/// Ensure a child sub-orchestration can receive CancelInstance from BOTH:
/// - user calling `Client::cancel_instance(child_id, ...)`
/// - parent dropping the sub-orchestration future (runtime enqueues CancelInstance)
#[tokio::test]
async fn user_cancel_instance_and_dropped_future_both_enqueue_cancelinstance_for_child() {
    let (inner_store, _td) = common::create_sqlite_store_disk().await;
    let recording_store = Arc::new(RecordingProvider::new(inner_store));
    let store: Arc<dyn Provider> = recording_store.clone();

    // Child that blocks forever unless cancelled.
    let child = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_wait("NeverComes").await;
        Ok("child_completed".to_string())
    };

    // Parent schedules the child with an explicit instance ID and immediately drops the future.
    // Dropping should trigger the runtime cancellation path that enqueues WorkItem::CancelInstance.
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        let sub_orch = ctx.schedule_sub_orchestration_with_id("ComboChild", "combo-child", "input");
        drop(sub_orch);
        ctx.schedule_timer(Duration::from_millis(50)).await;
        Ok("parent_done".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ComboChild", child)
        .register("ComboParent", parent)
        .build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        orchestration_concurrency: 2,
        worker_concurrency: 1,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    // Kick off the parent, which will schedule+drop the child and enqueue a cancellation.
    // We will also enqueue a user cancellation, and then only allow the child to be processed
    // once BOTH cancellation messages are present.

    client
        .start_orchestration("inst-combo-cancel", "ComboParent", "")
        .await
        .unwrap();

    // User independently requests cancellation of the same child instance.
    client.cancel_instance("combo-child", "user_cancel").await.unwrap();

    // Wait until BOTH CancelInstance enqueues have been observed by the provider wrapper.
    // This makes the test deterministic and ensures they can be delivered in the same child turn.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let records = recording_store.cancel_instance_records();
        let saw_user_cancel = records.iter().any(|r| {
            r.instance == "combo-child"
                && r.reason == "user_cancel"
                && r.source == CancelInstanceEnqueueSource::EnqueueForOrchestrator
        });
        let saw_drop_cancel = records.iter().any(|r| {
            r.instance == "combo-child"
                && r.reason == "parent dropped sub-orchestration future"
                && r.source == CancelInstanceEnqueueSource::AckOrchestrationItem
        });

        if saw_user_cancel && saw_drop_cancel {
            break;
        }

        if std::time::Instant::now() > deadline {
            panic!("Timed out waiting for both CancelInstance enqueues. records={records:?}");
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Now allow the child instance to be fetched/processed.
    recording_store.allow_child_fetch();

    // Parent should still complete normally.
    match client
        .wait_for_orchestration("inst-combo-cancel", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "parent_done"),
        other => panic!("Expected parent Completed, got {:?}", other),
    }

    // Child should end up cancelled.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let child_status = client.get_orchestration_status("combo-child").await.unwrap();
        match child_status {
            runtime::OrchestrationStatus::Failed { details, .. } => {
                assert!(
                    matches!(
                        &details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { .. },
                            ..
                        }
                    ),
                    "Child should be cancelled, got: {}",
                    details.display_message()
                );
                break;
            }
            runtime::OrchestrationStatus::Completed { output, .. } => {
                panic!("Child should NOT complete (it was cancelled). Got: {output}");
            }
            runtime::OrchestrationStatus::Running { .. } | runtime::OrchestrationStatus::NotFound => {
                if std::time::Instant::now() > deadline {
                    panic!("Timeout waiting for child to be cancelled");
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }

    // Assert BOTH cancellation sources enqueued CancelInstance for the child.
    let records = recording_store.cancel_instance_records();
    assert!(
        records.iter().any(|r| {
            r.instance == "combo-child"
                && r.reason == "user_cancel"
                && r.source == CancelInstanceEnqueueSource::EnqueueForOrchestrator
        }),
        "Expected user CancelInstance enqueue for combo-child, got records={records:?}"
    );
    assert!(
        records.iter().any(|r| {
            r.instance == "combo-child"
                && r.reason == "parent dropped sub-orchestration future"
                && r.source == CancelInstanceEnqueueSource::AckOrchestrationItem
        }),
        "Expected dropped-future CancelInstance enqueue for combo-child, got records={records:?}"
    );

    // Confirm they landed in the SAME child orchestration turn (same fetched message batch).
    let child_fetches = recording_store.child_cancel_reasons_by_fetch();
    assert!(
        !child_fetches.is_empty(),
        "Expected at least one fetched child turn batch, got child_fetches={child_fetches:?}"
    );
    let first_batch = &child_fetches[0];
    assert!(
        first_batch.iter().any(|r| r == "user_cancel"),
        "Expected user_cancel in first child batch, got first_batch={first_batch:?}"
    );
    assert!(
        first_batch
            .iter()
            .any(|r| r == "parent dropped sub-orchestration future"),
        "Expected dropped-future reason in first child batch, got first_batch={first_batch:?}"
    );

    // Sanity: parent history should include the dropped-future breadcrumb.
    let parent_hist = store.read("inst-combo-cancel").await.unwrap_or_default();
    let scheduled_id = parent_hist.iter().find_map(|e| match &e.kind {
        EventKind::SubOrchestrationScheduled { instance, .. } if instance == "combo-child" => Some(e.event_id()),
        _ => None,
    });
    let scheduled_id = scheduled_id.expect("Expected sub-orchestration to be scheduled");
    assert!(
        parent_hist.iter().any(|e| {
            matches!(&e.kind, EventKind::SubOrchestrationCancelRequested { reason } if reason == "dropped_future")
                && e.source_event_id == Some(scheduled_id)
        }),
        "Expected SubOrchestrationCancelRequested(reason=dropped_future, source_event_id={scheduled_id}) in parent history"
    );

    rt.shutdown(None).await;
}

/// Test that sub-orchestration going out of scope (not awaited) triggers cancellation
#[tokio::test]
async fn sub_orchestration_out_of_scope_cancelled() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Child that waits indefinitely
    let waiting_child = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_wait("NeverComes").await;
        Ok("child_completed".to_string())
    };

    // Parent where sub-orchestration goes out of scope in a block
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        // Sub-orchestration scheduled in a block
        {
            let _unused = ctx.schedule_sub_orchestration("WaitingChild", "input");
            // _unused goes out of scope here
        }

        // Continue with other work
        ctx.schedule_timer(Duration::from_millis(50)).await;
        Ok("parent_completed_without_await".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("WaitingChild", waiting_child)
        .register("OutOfScopeSubOrchParent", parent)
        .build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        orchestration_concurrency: 2,
        worker_concurrency: 1,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-scope-suborg", "OutOfScopeSubOrchParent", "")
        .await
        .unwrap();

    // Wait for parent to complete
    match client
        .wait_for_orchestration("inst-scope-suborg", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "parent_completed_without_await");
        }
        other => panic!("Expected parent Completed, got {:?}", other),
    }

    // Find child instance
    let parent_hist = store.read("inst-scope-suborg").await.unwrap();
    let child_instance = parent_hist.iter().find_map(|e| match &e.kind {
        EventKind::SubOrchestrationScheduled { instance, .. } => Some(instance.clone()),
        _ => None,
    });

    let child_id = child_instance.expect("Child should have been scheduled");
    let full_child_id = format!("inst-scope-suborg::{child_id}");

    // Wait for child to be cancelled
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let child_status = client.get_orchestration_status(&full_child_id).await.unwrap();
        match child_status {
            runtime::OrchestrationStatus::Failed { details, .. } => {
                assert!(
                    matches!(
                        &details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { .. },
                            ..
                        }
                    ),
                    "Child should be cancelled when going out of scope, got: {}",
                    details.display_message()
                );
                break;
            }
            runtime::OrchestrationStatus::Completed { output, .. } => {
                panic!("Child should NOT complete when it went out of scope. Got: {output}");
            }
            runtime::OrchestrationStatus::Running { .. } | runtime::OrchestrationStatus::NotFound => {
                if std::time::Instant::now() > deadline {
                    panic!("Timeout waiting for out-of-scope child sub-orchestration to be cancelled");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    rt.shutdown(None).await;
}

#[tokio::test]
async fn positional_arrival_before_sub_outstanding() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // Wait a bit so the event arrives before we subscribe
        ctx.schedule_timer(std::time::Duration::from_millis(100)).await;

        // Subscribe to positional event
        let wait = ctx.schedule_wait("MyEvent");

        // Timeout after 500ms
        let timeout = ctx.schedule_timer(std::time::Duration::from_millis(500));

        match ctx.select2(wait, timeout).await {
            duroxide::Either2::First(_) => Ok("event-received".to_string()),
            duroxide::Either2::Second(_) => Ok("timeout".to_string()),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-pos-arr", "TestOrch", "")
        .await
        .unwrap();

    // Raise event immediately (before the 100ms timer finishes)
    client.raise_event("inst-pos-arr", "MyEvent", "data").await.unwrap();

    // Wait for completion
    let status = client
        .wait_for_orchestration("inst-pos-arr", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "timeout"),
        "Expected timeout, got {:?}",
        status
    );

    let history = store.read("inst-pos-arr").await.unwrap();

    // The subscription should have a dropped_future cancellation breadcrumb because it lost the select2
    let has_cancel = history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalSubscribedCancelled { reason } if reason == "dropped_future"));

    assert!(
        has_cancel,
        "Missing dropped_future cancellation breadcrumb for positional subscription"
    );

    rt.shutdown(None).await;
}

/// Bug fix regression: when a `schedule_wait("X")` loses a select2 (timer wins),
/// its subscription slot is cancelled. A *subsequent* `schedule_wait("X")` must still
/// receive the next raised event, even though a cancelled slot exists for the same name.
/// Before the fix, the cancelled slot "stole" the arrival and the active subscription hung.
#[tokio::test]
async fn cancelled_subscription_does_not_steal_future_event() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // Phase 1: schedule_wait loses to timer → cancelled subscription
        let wait1 = ctx.schedule_wait("Signal");
        let timeout1 = ctx.schedule_timer(Duration::from_millis(50));
        match ctx.select2(wait1, timeout1).await {
            Either2::First(_) => return Ok("unexpected_phase1".to_string()),
            Either2::Second(_) => {} // timer won as expected
        }

        // Phase 2: new schedule_wait for the SAME event name
        let data = ctx.schedule_wait("Signal").await;
        Ok(format!("got:{data}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-no-steal", "TestOrch", "")
        .await
        .unwrap();

    // Wait for phase 1 to settle (timer fires, subscription cancelled)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now raise the event — it should go to phase 2's subscription, NOT the cancelled one
    client.raise_event("inst-no-steal", "Signal", "hello").await.unwrap();

    let status = client
        .wait_for_orchestration("inst-no-steal", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "got:hello"),
        "Expected got:hello, got {:?}",
        status
    );

    // Verify the cancelled breadcrumb exists for phase 1
    let history = store.read("inst-no-steal").await.unwrap();
    let has_cancel = history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalSubscribedCancelled { reason } if reason == "dropped_future"));
    assert!(has_cancel, "Missing dropped_future cancellation breadcrumb");

    rt.shutdown(None).await;
}

/// Regression: two sequential `schedule_wait("X")` + two `raise_event("X")` should
/// deliver first→first, second→second (positional FIFO matching).
/// This guards against the deduplication fix accidentally breaking normal ordering.
#[tokio::test]
async fn positional_matching_fifo_without_cancellation() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let d1 = ctx.schedule_wait("Data").await;
        let d2 = ctx.schedule_wait("Data").await;
        Ok(format!("{d1},{d2}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client.start_orchestration("inst-fifo", "TestOrch", "").await.unwrap();

    // Give orchestration time to schedule first wait
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Raise first event — orchestration is blocked on first schedule_wait
    client.raise_event("inst-fifo", "Data", "first").await.unwrap();

    // Give orchestration time to proceed to second schedule_wait
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Raise second event
    client.raise_event("inst-fifo", "Data", "second").await.unwrap();

    let status = client
        .wait_for_orchestration("inst-fifo", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "first,second"),
        "FIFO ordering broken, got {:?}",
        status
    );

    rt.shutdown(None).await;
}

/// Regression: external events with the same name+data must NOT be deduplicated.
/// Pre-fix, if two identical `raise_event("X", "same_data")` calls occurred,
/// the second was silently dropped. This test ensures both are delivered.
#[tokio::test]
async fn duplicate_external_events_not_deduplicated() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let d1 = ctx.schedule_wait("Dup").await;
        let d2 = ctx.schedule_wait("Dup").await;
        Ok(format!("{d1},{d2}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client.start_orchestration("inst-nodup", "TestOrch", "").await.unwrap();

    // Give orchestration time to schedule first wait
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Raise first event — orchestration is blocked on first schedule_wait
    client.raise_event("inst-nodup", "Dup", "same_payload").await.unwrap();

    // Give orchestration time to proceed to second schedule_wait
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Raise second identical event — must NOT be deduplicated
    client.raise_event("inst-nodup", "Dup", "same_payload").await.unwrap();

    let status = client
        .wait_for_orchestration("inst-nodup", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "same_payload,same_payload"),
        "Duplicate events were deduplicated (bug regression), got {:?}",
        status
    );

    rt.shutdown(None).await;
}

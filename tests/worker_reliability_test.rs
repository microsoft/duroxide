// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use common::fault_injection::{FailingProvider, WorkItemCall};
use duroxide::EventKind;
use duroxide::providers::WorkItem;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime;
use duroxide::runtime::registry::{ActivityRegistry, OrchestrationRegistry};
use duroxide::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

mod common;

/// Test that verifies activity completion reliability after crash between dequeue and enqueue
///
/// Scenario:
/// 1. Orchestration schedules an activity
/// 2. Activity is dequeued from worker queue and executed
/// 3. System crashes after activity execution but before completion is enqueued
/// 4. System restarts
/// 5. Activity should be redelivered and executed again (at-least-once semantics)
#[tokio::test]
async fn activity_reliability_after_crash_before_completion_enqueue() {
    // Use SQLite store for persistence across "crash"
    // Shorten worker lock lease so redelivery happens quickly after restart
    // Safety: setting environment variables in tests is process-wide; we set it before creating the store
    // and use a deterministic value to control lease duration. We restore it at the end of the test.
    let prev_lease = std::env::var("DUROXIDE_SQLITE_LOCK_TIMEOUT_MS").ok();
    unsafe {
        std::env::set_var("DUROXIDE_SQLITE_LOCK_TIMEOUT_MS", "1000");
    }
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Simple orchestration that schedules an activity and waits for completion
    let orch = |ctx: OrchestrationContext, input: String| async move {
        ctx.trace_info("Starting activity reliability test orchestration");

        // Schedule an activity
        let result = ctx.schedule_activity("TestActivity", input).await?;

        ctx.trace_info("Activity completed successfully");
        Ok(format!("Activity result: {result}"))
    };

    let activity_registry = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, input: String| async move {
            println!("Executing TestActivity with input: {input}");
            // Simulate some work
            Ok(format!("Processed: {input}"))
        })
        .build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ActivityReliabilityTest", orch)
        .build();

    // Phase 1: Start orchestration and let it schedule the activity
    let store1 = store.clone();
    let rt1 = runtime::Runtime::start_with_store(
        store1.clone(),
        activity_registry.clone(),
        orchestration_registry.clone(),
    )
    .await;

    let instance = "inst-activity-reliability";
    let client1 = duroxide::Client::new(store1.clone());
    client1
        .start_orchestration(instance, "ActivityReliabilityTest", "test-data")
        .await
        .unwrap();

    // Wait for activity to be scheduled
    assert!(
        common::wait_for_history(
            store1.clone(),
            instance,
            |h| h.iter().any(|e| matches!(&e.kind, EventKind::ActivityScheduled { .. })),
            2_000
        )
        .await,
        "Activity should be scheduled"
    );

    // Verify activity hasn't completed yet (we'll simulate crash before completion)
    let hist_before = store1.read(instance).await.unwrap_or_default();
    assert!(
        !hist_before
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ActivityCompleted { .. })),
        "Activity should not have completed yet"
    );

    // Verify activity was scheduled but not completed
    assert!(
        hist_before
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ActivityScheduled { name, .. } if name == "TestActivity"))
    );
    assert!(
        !hist_before.iter().any(
            |e| matches!(&e.kind, EventKind::ActivityCompleted { result, .. } if result == "Processed: test-data")
        )
    );

    // Simulate crash by shutting down runtime
    println!("Simulating crash - shutting down runtime before activity completes...");
    rt1.shutdown(None).await;

    // Small delay to ensure shutdown completes
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Phase 2: "Restart" system with new runtime but same store
    println!("Restarting system...");
    let store2 = store.clone();
    let rt2 = runtime::Runtime::start_with_store(store2.clone(), activity_registry, orchestration_registry).await;

    // The runtime should automatically resume the orchestration and reprocess pending activities

    // Wait for orchestration to complete
    let client2 = duroxide::Client::new(store2.clone());
    match client2
        .wait_for_orchestration(instance, std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "Activity result: Processed: test-data");
            println!("✅ Orchestration completed successfully after restart");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed after restart: {}", details.display_message());
        }
        status => {
            panic!("Unexpected orchestration status after restart: {status:?}");
        }
    }

    // Verify the activity actually completed
    let hist_after = store2.read(instance).await.unwrap_or_default();

    // Debug: print all events (can be removed in production)
    println!("History after restart:");
    for (i, event) in hist_after.iter().enumerate() {
        println!("  {i}: {event:?}");
    }

    // Should have exactly one ActivityScheduled and one ActivityCompleted for our TestActivity
    let test_activity_scheduled_count = hist_after
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ActivityScheduled { name, .. } if name == "TestActivity"))
        .count();
    let test_activity_completed_count = hist_after
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ActivityCompleted { result, .. } if result == "Processed: test-data"))
        .count();

    assert_eq!(
        test_activity_scheduled_count, 1,
        "Should have exactly one TestActivity scheduled event"
    );
    assert_eq!(
        test_activity_completed_count, 1,
        "Should have exactly one TestActivity completed event"
    );

    println!("✅ Activity reliability test passed - activity completed correctly after restart");
    rt2.shutdown(None).await;

    // Restore environment
    match prev_lease {
        Some(v) => unsafe {
            std::env::set_var("DUROXIDE_SQLITE_LOCK_TIMEOUT_MS", v);
        },
        None => unsafe {
            std::env::remove_var("DUROXIDE_SQLITE_LOCK_TIMEOUT_MS");
        },
    }
}

/// Test multiple activities with crash/recovery
///
/// This test verifies that scheduled activities persist across system crashes and completions
/// resume correctly after restart. SQLite provider provides transactional guarantees that
/// ensure history and work items are persisted atomically.
///
/// Test flow:
/// 1. Start orchestration with 3 parallel activities
/// 2. Wait for all activities to be scheduled in history
/// 3. Crash runtime before any activity completes
/// 4. Restart runtime and verify all activities complete successfully
#[tokio::test]
async fn multiple_activities_reliability_after_crash() {
    // Use SQLite store for persistence across "crash" - SQLite provides atomicity
    // Shorten worker lock lease so redelivery happens quickly after restart
    // We restore the environment variable at the end of the test
    let prev_lease = std::env::var("DUROXIDE_SQLITE_LOCK_TIMEOUT_MS").ok();
    unsafe {
        std::env::set_var("DUROXIDE_SQLITE_LOCK_TIMEOUT_MS", "1000");
    }
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Orchestration with multiple activities
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.trace_info("Starting multi-activity reliability test");

        // Schedule three activities in parallel
        let a1 = ctx.schedule_activity("TestActivity", "task1");
        let a2 = ctx.schedule_activity("TestActivity", "task2");
        let a3 = ctx.schedule_activity("TestActivity", "task3");

        // Wait for all activities
        let results = ctx.join(vec![a1, a2, a3]).await;

        let mut outputs = Vec::new();
        for result in results {
            match result {
                Ok(output) => outputs.push(output),
                Err(e) => return Err(e),
            }
        }

        Ok(format!("All activities completed: {outputs:?}"))
    };

    let activity_registry = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, input: String| async move {
            println!("Executing TestActivity with input: {input}");
            Ok(format!("Processed: {input}"))
        })
        .build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("MultiActivityTest", orch)
        .build();

    // Phase 1: Start and wait for all activities to be scheduled
    let store1 = store.clone();
    let rt1 = runtime::Runtime::start_with_store(
        store1.clone(),
        activity_registry.clone(),
        orchestration_registry.clone(),
    )
    .await;

    let instance = "inst-multi-activity-reliability";
    let client1 = duroxide::Client::new(store1.clone());
    client1
        .start_orchestration(instance, "MultiActivityTest", "")
        .await
        .unwrap();

    // Wait for all 3 activities to be scheduled
    assert!(
        common::wait_for_history(
            store1.clone(),
            instance,
            |h| h
                .iter()
                .filter(|e| matches!(&e.kind, EventKind::ActivityScheduled { .. }))
                .count()
                >= 3,
            2_000
        )
        .await,
        "All 3 activities should be scheduled"
    );

    // Crash before any activity completes
    let hist_before = store1.read(instance).await.unwrap_or_default();
    assert_eq!(
        hist_before
            .iter()
            .filter(|e| matches!(&e.kind, EventKind::ActivityCompleted { .. }))
            .count(),
        0,
        "No activities should have completed yet"
    );

    println!("Crashing with 3 pending activities...");
    rt1.shutdown(None).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Phase 2: Restart and verify all activities complete
    println!("Restarting...");
    let store2 = store.clone();
    let rt2 = runtime::Runtime::start_with_store(store2.clone(), activity_registry, orchestration_registry).await;

    // Wait for completion
    let client2 = duroxide::Client::new(store2.clone());
    match client2
        .wait_for_orchestration(instance, std::time::Duration::from_secs(20))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            println!("✅ All activities completed after recovery");
            assert!(output.contains("All activities completed"));
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        status => {
            panic!("Unexpected status: {status:?}");
        }
    }

    // Verify all 3 TestActivity activities completed
    let hist_after = store2.read(instance).await.unwrap_or_default();
    let test_activity_completed_count = hist_after
        .iter()
        .filter(
            |e| matches!(&e.kind, EventKind::ActivityCompleted { result, .. } if result.starts_with("Processed: task")),
        )
        .count();

    assert_eq!(
        test_activity_completed_count, 3,
        "All 3 TestActivity activities should have completed"
    );

    rt2.shutdown(None).await;

    // Restore environment
    match prev_lease {
        Some(v) => unsafe {
            std::env::set_var("DUROXIDE_SQLITE_LOCK_TIMEOUT_MS", v);
        },
        None => unsafe {
            std::env::remove_var("DUROXIDE_SQLITE_LOCK_TIMEOUT_MS");
        },
    }
}

struct AckTest {
    provider: Arc<FailingProvider>,
    client: Client,
    rt: Arc<runtime::Runtime>,
    _db: tempfile::NamedTempFile,
}

impl AckTest {
    async fn new(activities: ActivityRegistry) -> Self {
        // WAL keeps exact call counts independent of shared-cache deadlocks.
        let db = tempfile::NamedTempFile::new().unwrap();
        let sqlite = Arc::new(
            SqliteProvider::new(&format!("sqlite:{}", db.path().display()), None)
                .await
                .unwrap(),
        );
        let provider = Arc::new(FailingProvider::new(sqlite));
        let orchestrations = OrchestrationRegistry::builder()
            .register("AckOrch", |ctx: OrchestrationContext, _input: String| async move {
                ctx.schedule_activity("CountingActivity", "{}").await
            })
            .build();
        let options = runtime::RuntimeOptions {
            worker_concurrency: 1,
            orchestration_concurrency: 1,
            worker_lock_timeout: Duration::from_secs(2),
            worker_lock_renewal_buffer: Duration::from_millis(500),
            activity_cancellation_grace_period: Duration::ZERO,
            ..Default::default()
        };
        let rt = runtime::Runtime::start_with_options(provider.clone(), activities, orchestrations, options).await;
        let client = Client::new(provider.clone());

        Self {
            provider,
            client,
            rt,
            _db: db,
        }
    }

    async fn start(&self) {
        self.client
            .start_orchestration("ack-test", "AckOrch", "")
            .await
            .unwrap();
    }

    async fn finish(&self) -> runtime::OrchestrationStatus {
        let status = self
            .client
            .wait_for_orchestration("ack-test", Duration::from_secs(10))
            .await
            .unwrap();

        // A commit can become visible before the worker finishes retrying its acknowledgement.
        Arc::clone(&self.rt).shutdown(Some(10_000)).await;
        status
    }
}

fn counting_activity(result: Result<String, String>) -> (ActivityRegistry, Arc<AtomicU32>) {
    let execution_count = Arc::new(AtomicU32::new(0));
    let counter = execution_count.clone();
    let activities = ActivityRegistry::builder()
        .register("CountingActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter.clone();
            let result = result.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                result
            }
        })
        .build();
    (activities, execution_count)
}

#[tokio::test]
async fn worker_abandon_on_ack_failure_enables_retry() {
    let (activities, execution_count) = counting_activity(Ok("done".to_string()));
    let test = AckTest::new(activities).await;
    test.provider.fail_next_ack_work_item();
    test.start().await;

    assert!(matches!(
        test.finish().await,
        runtime::OrchestrationStatus::Completed { output, .. } if output == "done"
    ));
    assert_eq!(execution_count.load(Ordering::SeqCst), 2);

    let calls = test.provider.work_item_calls();
    let [
        WorkItemCall::Ack { token, completion },
        WorkItemCall::Abandon { token: abandoned },
        WorkItemCall::Ack {
            token: redelivered,
            completion: retried_completion,
        },
    ] = calls.as_slice()
    else {
        panic!("Permanent failure must abandon without retrying the ack: {calls:?}");
    };
    assert_eq!(token, abandoned);
    assert_ne!(token, redelivered);
    assert_eq!(completion, retried_completion);
}

#[tokio::test]
async fn worker_retries_retryable_ack_failure_without_reexecuting_activity() {
    let (activities, execution_count) = counting_activity(Ok("done".to_string()));
    let test = AckTest::new(activities).await;
    test.provider.fail_next_ack_work_item_retryable(4);
    test.start().await;

    assert!(matches!(
        test.finish().await,
        runtime::OrchestrationStatus::Completed { output, .. } if output == "done"
    ));
    assert_eq!(test.provider.remaining_retryable_ack_failures(), 0);
    assert_eq!(execution_count.load(Ordering::SeqCst), 1);

    let calls = test.provider.work_item_calls();
    assert_eq!(calls.len(), 5, "The fifth and final attempt must succeed");
    assert!(matches!(
        &calls[0],
        WorkItemCall::Ack {
            completion: Some(WorkItem::ActivityCompleted { result, .. }),
            ..
        } if result == "done"
    ));
    assert_eq!(calls, vec![calls[0].clone(); 5]);
}

#[tokio::test]
async fn worker_abandons_after_retryable_ack_attempts_are_exhausted() {
    let (activities, execution_count) = counting_activity(Ok("done".to_string()));
    let test = AckTest::new(activities).await;
    test.provider.fail_next_ack_work_item_retryable(5);
    test.start().await;

    assert!(matches!(
        test.finish().await,
        runtime::OrchestrationStatus::Completed { output, .. } if output == "done"
    ));
    assert_eq!(test.provider.remaining_retryable_ack_failures(), 0);
    assert_eq!(execution_count.load(Ordering::SeqCst), 2);

    let calls = test.provider.work_item_calls();
    assert_eq!(calls.len(), 7, "Five attempts, abandon, then a successful redelivery");
    let WorkItemCall::Ack { token, completion } = &calls[0] else {
        panic!("Expected an acknowledgement: {calls:?}");
    };
    assert!(calls[..5].iter().all(|call| call == &calls[0]), "{calls:?}");
    assert_eq!(calls[5], WorkItemCall::Abandon { token: token.clone() });
    let WorkItemCall::Ack {
        token: redelivered,
        completion: retried_completion,
    } = &calls[6]
    else {
        panic!("Expected acknowledgement after redelivery: {calls:?}");
    };
    assert_ne!(token, redelivered);
    assert_eq!(completion, retried_completion);
}

#[tokio::test]
async fn worker_retries_application_error_ack_without_reexecuting_activity() {
    let (activities, execution_count) = counting_activity(Err("activity error".to_string()));
    let test = AckTest::new(activities).await;
    test.provider.fail_next_ack_work_item_retryable(3);
    test.start().await;

    assert!(matches!(
        test.finish().await,
        runtime::OrchestrationStatus::Failed { .. }
    ));
    assert_eq!(test.provider.remaining_retryable_ack_failures(), 0);
    assert_eq!(execution_count.load(Ordering::SeqCst), 1);

    let calls = test.provider.work_item_calls();
    assert_eq!(calls.len(), 4);
    assert!(matches!(
        &calls[0],
        WorkItemCall::Ack {
            completion: Some(WorkItem::ActivityFailed {
                details: ErrorDetails::Application {
                    kind: AppErrorKind::ActivityFailed,
                    message,
                    retryable: false,
                },
                ..
            }),
            ..
        } if message == "activity error"
    ));
    assert_eq!(calls, vec![calls[0].clone(); 4]);
}

#[tokio::test]
async fn worker_does_not_retry_cancelled_activity_ack() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let activity_entered = entered.clone();
    let execution_count = Arc::new(AtomicU32::new(0));
    let counter = execution_count.clone();
    let activities = ActivityRegistry::builder()
        .register("CountingActivity", move |_ctx: ActivityContext, _input: String| {
            let entered = activity_entered.clone();
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                std::future::pending::<Result<String, String>>().await
            }
        })
        .build();
    let test = AckTest::new(activities).await;
    test.start().await;
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    test.provider.fail_next_ack_work_item_retryable(1);
    test.client
        .cancel_instance("ack-test", "test cancellation")
        .await
        .unwrap();

    assert!(matches!(
        test.finish().await,
        runtime::OrchestrationStatus::Failed {
            details: ErrorDetails::Application {
                kind: AppErrorKind::Cancelled { .. },
                ..
            },
            ..
        }
    ));
    assert_eq!(test.provider.remaining_retryable_ack_failures(), 0);
    assert_eq!(execution_count.load(Ordering::SeqCst), 1);

    let calls = test.provider.work_item_calls();
    assert_eq!(calls.len(), 2);
    let WorkItemCall::Ack {
        token,
        completion: None,
    } = &calls[0]
    else {
        panic!("Cancellation must acknowledge without a completion: {calls:?}");
    };
    assert_eq!(calls[1], WorkItemCall::Abandon { token: token.clone() });
}

#[tokio::test(flavor = "current_thread")]
async fn worker_redelivers_after_transient_renewal_and_cancellation_ack_failures() {
    let execution_count = Arc::new(AtomicU32::new(0));
    let counter = execution_count.clone();
    let activities = ActivityRegistry::builder()
        .register("CountingActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter.clone();
            async move {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    std::future::pending::<()>().await;
                }
                Ok("done".to_string())
            }
        })
        .build();
    let test = AckTest::new(activities).await;
    test.provider.fail_next_renew_work_item_lock();
    test.provider.fail_next_ack_work_item_retryable(1);
    test.start().await;

    assert!(matches!(
        test.finish().await,
        runtime::OrchestrationStatus::Completed { output, .. } if output == "done"
    ));
    assert_eq!(test.provider.remaining_retryable_ack_failures(), 0);
    assert_eq!(execution_count.load(Ordering::SeqCst), 2);

    let calls = test.provider.work_item_calls();
    let [
        WorkItemCall::Ack {
            token,
            completion: None,
        },
        WorkItemCall::Abandon { token: abandoned },
        WorkItemCall::Ack {
            token: redelivered,
            completion: Some(WorkItem::ActivityCompleted { result, .. }),
        },
    ] = calls.as_slice()
    else {
        panic!("A failed cancellation ack must allow the live activity to be redelivered: {calls:?}");
    };
    assert_eq!(token, abandoned);
    assert_ne!(token, redelivered);
    assert_eq!(result, "done");
}

#[tokio::test(flavor = "current_thread")]
async fn worker_retryable_ack_error_after_commit_is_reported_as_uncertain() {
    let (captured, _guard) = common::tracing_capture::install_tracing_capture();
    let (activities, execution_count) = counting_activity(Ok("done".to_string()));
    let test = AckTest::new(activities).await;
    test.provider.set_ack_then_fail(true);
    test.provider.fail_next_ack_work_item_retryable(1);
    test.start().await;

    assert!(matches!(
        test.finish().await,
        runtime::OrchestrationStatus::Completed { output, .. } if output == "done"
    ));
    assert_eq!(test.provider.remaining_retryable_ack_failures(), 0);
    assert_eq!(execution_count.load(Ordering::SeqCst), 1);

    let calls = test.provider.work_item_calls();
    assert_eq!(calls.len(), 2);
    let WorkItemCall::Ack {
        completion: Some(WorkItem::ActivityCompleted { result, .. }),
        ..
    } = &calls[0]
    else {
        panic!("Expected an activity completion: {calls:?}");
    };
    assert_eq!(result, "done");
    assert_eq!(calls[0], calls[1]);
    assert_eq!(test.rt.metrics_snapshot().unwrap().activity_infra_errors, 0);

    let events = captured.lock().unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.field("ack_outcome").as_deref() == Some("uncertain")),
        "The runtime must report an uncertain acknowledgement outcome"
    );
    assert!(
        !events.iter().any(|event| event.message.contains("atomic ack failed")),
        "An uncertain acknowledgement must not be reported as a confirmed failure"
    );
}

#[tokio::test]
async fn worker_uncertain_ack_before_commit_recovers_after_lock_expiry() {
    let (activities, execution_count) = counting_activity(Ok("done".to_string()));
    let test = AckTest::new(activities).await;
    test.provider.fail_next_ack_work_item_retryable(1);
    test.provider.fail_next_ack_work_item();
    test.start().await;

    assert!(matches!(
        test.finish().await,
        runtime::OrchestrationStatus::Completed { output, .. } if output == "done"
    ));
    assert_eq!(execution_count.load(Ordering::SeqCst), 2);
    assert_eq!(test.rt.metrics_snapshot().unwrap().activity_infra_errors, 0);

    let calls = test.provider.work_item_calls();
    assert_eq!(
        calls.len(),
        3,
        "Two uncertain ack attempts, then redelivery without abandon"
    );
    let WorkItemCall::Ack { token, completion } = &calls[0] else {
        panic!("Expected an acknowledgement: {calls:?}");
    };
    assert_eq!(calls[0], calls[1]);
    let WorkItemCall::Ack {
        token: redelivered,
        completion: retried_completion,
    } = &calls[2]
    else {
        panic!("Expected acknowledgement after lock expiry: {calls:?}");
    };
    assert_ne!(token, redelivered);
    assert_eq!(completion, retried_completion);
}

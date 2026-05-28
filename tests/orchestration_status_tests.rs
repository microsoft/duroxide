// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Tests for OrchestrationStatus determination across all scenarios
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, OrchestrationStatus};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use std::time::Duration;

mod common;

/// Test: Status is NotFound for non-existent instance
#[tokio::test]
async fn test_status_not_found() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let client = Client::new(store.clone());
    let status = client.get_orchestration_status("nonexistent-instance").await.unwrap();

    assert!(
        matches!(status, OrchestrationStatus::NotFound),
        "Expected NotFound, got: {status:?}"
    );
}

/// Test: Status is Running when orchestration is in progress
#[tokio::test]
async fn test_status_running() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("BlockForever", |_ctx: ActivityContext, _: String| async move {
            // Never completes
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            Ok("never".to_string())
        })
        .build();

    let orchestration =
        |ctx: OrchestrationContext, _input: String| async move { ctx.schedule_activity("BlockForever", "").await };

    let orchestrations = OrchestrationRegistry::builder()
        .register("RunningOrch", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("test-running", "RunningOrch", "")
        .await
        .unwrap();

    // Give it a moment to start but not complete
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let status = client.get_orchestration_status("test-running").await.unwrap();
    assert!(
        matches!(status, OrchestrationStatus::Running { .. }),
        "Expected Running, got: {status:?}"
    );

    rt.shutdown(None).await;
}

/// Test: Status is Completed with correct output
#[tokio::test]
async fn test_status_completed() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("ReturnValue", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("result: {input}"))
        })
        .build();

    let orchestration =
        |ctx: OrchestrationContext, input: String| async move { ctx.schedule_activity("ReturnValue", input).await };

    let orchestrations = OrchestrationRegistry::builder()
        .register("CompletedOrch", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("test-completed", "CompletedOrch", "test-input")
        .await
        .unwrap();

    // Wait for completion
    let status = client
        .wait_for_orchestration("test-completed", std::time::Duration::from_secs(2))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "result: test-input");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    // Check status again (should still be Completed)
    let status = client.get_orchestration_status("test-completed").await.unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "result: test-input");
        }
        other => panic!("Expected Completed on re-check, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Test: Status is Failed with correct error message
#[tokio::test]
async fn test_status_failed() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("FailActivity", |_ctx: ActivityContext, _: String| async move {
            Err("intentional failure".to_string())
        })
        .build();

    let orchestration =
        |ctx: OrchestrationContext, _input: String| async move { ctx.schedule_activity("FailActivity", "").await };

    let orchestrations = OrchestrationRegistry::builder()
        .register("FailedOrch", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("test-failed", "FailedOrch", "")
        .await
        .unwrap();

    // Wait for failure
    let status = client
        .wait_for_orchestration("test-failed", std::time::Duration::from_secs(2))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(matches!(
                details,
                duroxide::ErrorDetails::Application {
                    kind: duroxide::AppErrorKind::OrchestrationFailed,
                    message,
                    ..
                } if message == "intentional failure"
            ));
        }
        other => panic!("Expected Failed, got: {other:?}"),
    }

    // Check status again (should still be Failed)
    let status = client.get_orchestration_status("test-failed").await.unwrap();
    match status {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(matches!(
                details,
                duroxide::ErrorDetails::Application {
                    kind: duroxide::AppErrorKind::OrchestrationFailed,
                    message,
                    ..
                } if message == "intentional failure"
            ));
        }
        other => panic!("Expected Failed on re-check, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Test: Status shows latest execution after ContinueAsNew
#[tokio::test]
async fn test_status_after_continue_as_new() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder().build();

    let orchestration = |ctx: OrchestrationContext, input: String| async move {
        let n: i32 = input.parse().unwrap_or(0);

        if n < 2 {
            // Continue to next iteration
            return ctx.continue_as_new((n + 1).to_string()).await;
        } else {
            // Done
            Ok(format!("done: {n}"))
        }
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("ContinueAsNewOrch", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("test-continue", "ContinueAsNewOrch", "0")
        .await
        .unwrap();

    // Poll until we get a Completed status with the expected final value
    // (ContinueAsNew creates multiple executions; we want the final one)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut final_status = None;

    while std::time::Instant::now() < deadline {
        match client.get_orchestration_status("test-continue").await.unwrap() {
            OrchestrationStatus::Completed { output, .. } if output == "done: 2" => {
                final_status = Some(output);
                break;
            }
            OrchestrationStatus::Failed { details, .. } => {
                panic!("Orchestration failed unexpectedly: {}", details.display_message());
            }
            _ => {
                // Still running or intermediate execution
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }

    assert!(
        final_status.is_some(),
        "Expected final execution to complete with 'done: 2'"
    );

    // Verify provider returns latest execution's history
    let history = store.read("test-continue").await.unwrap_or_default();
    assert!(
        history
            .iter()
            .any(|e| matches!(&e.kind, duroxide::EventKind::OrchestrationStarted { input, .. } if input == "2")),
        "History should contain the final execution's start event with input=2"
    );

    rt.shutdown(None).await;
}

/// Test: Status is Failed when orchestration is cancelled
#[tokio::test]
async fn test_status_cancelled() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("LongTask", |_ctx: ActivityContext, _: String| async move {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            Ok("done".to_string())
        })
        .build();

    let orchestration =
        |ctx: OrchestrationContext, _input: String| async move { ctx.schedule_activity("LongTask", "").await };

    let orchestrations = OrchestrationRegistry::builder()
        .register("CancellableOrch", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("test-cancelled", "CancellableOrch", "")
        .await
        .unwrap();

    // Give it time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Request cancellation
    let _ = client
        .cancel_instance("test-cancelled", "test requested cancellation")
        .await;

    // Wait for cancellation to take effect
    let status = client
        .wait_for_orchestration("test-cancelled", std::time::Duration::from_secs(5))
        .await
        .unwrap();

    // Cancellation results in Failed status with Cancelled error
    match status {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(
                    &details,
                    duroxide::ErrorDetails::Application {
                        kind: duroxide::AppErrorKind::Cancelled { reason },
                        ..
                    } if reason.contains("test requested cancellation")
                ),
                "Cancelled orchestration should have Cancelled error, got: {details:?}"
            );
        }
        other => panic!("Expected Failed (cancelled), got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Test: Status transitions correctly through lifecycle
#[tokio::test]
async fn test_status_lifecycle_transitions() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("QuickTask", |_ctx: ActivityContext, _: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Ok("quick".to_string())
        })
        .build();

    let orchestration =
        |ctx: OrchestrationContext, _input: String| async move { ctx.schedule_activity("QuickTask", "").await };

    let orchestrations = OrchestrationRegistry::builder()
        .register("LifecycleOrch", orchestration)
        .build();

    // TIMING-SENSITIVE: Test checks status after 50ms, needs fast polling to see Running state
    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };
    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());

    // Initially: NotFound
    let status = client.get_orchestration_status("test-lifecycle").await.unwrap();
    assert!(matches!(status, OrchestrationStatus::NotFound));

    // Start orchestration
    client
        .start_orchestration("test-lifecycle", "LifecycleOrch", "")
        .await
        .unwrap();

    // Brief moment: should be Running
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let status = client.get_orchestration_status("test-lifecycle").await.unwrap();
    assert!(
        matches!(status, OrchestrationStatus::Running { .. }),
        "Should be Running after start, got: {status:?}"
    );

    // Wait for completion: should be Completed
    let status = client
        .wait_for_orchestration("test-lifecycle", std::time::Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "quick");
        }
        other => panic!("Expected final Completed, got: {other:?}"),
    }

    // Still Completed on subsequent checks
    let status = client.get_orchestration_status("test-lifecycle").await.unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    rt.shutdown(None).await;
}

/// Test: Multiple orchestrations have independent statuses
#[tokio::test]
async fn test_status_independence() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("SuccessTask", |_ctx: ActivityContext, _: String| async move {
            Ok("success".to_string())
        })
        .register("FailTask", |_ctx: ActivityContext, _: String| async move {
            Err("failed".to_string())
        })
        .build();

    let success_orch =
        |ctx: OrchestrationContext, _input: String| async move { ctx.schedule_activity("SuccessTask", "").await };

    let fail_orch =
        |ctx: OrchestrationContext, _input: String| async move { ctx.schedule_activity("FailTask", "").await };

    let orchestrations = OrchestrationRegistry::builder()
        .register("SuccessOrch", success_orch)
        .register("FailOrch", fail_orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = Client::new(store.clone());

    // Start both
    client
        .start_orchestration("inst-success", "SuccessOrch", "")
        .await
        .unwrap();
    client.start_orchestration("inst-fail", "FailOrch", "").await.unwrap();

    // Wait for both to finish (use wait_for_orchestration instead of sleep for reliability)
    let status1 = client
        .wait_for_orchestration("inst-success", Duration::from_secs(5))
        .await
        .unwrap();
    let status2 = client
        .wait_for_orchestration("inst-fail", Duration::from_secs(5))
        .await
        .unwrap();

    assert!(
        matches!(status1, OrchestrationStatus::Completed { .. }),
        "inst-success should be Completed"
    );
    assert!(
        matches!(status2, OrchestrationStatus::Failed { .. }),
        "inst-fail should be Failed"
    );

    rt.shutdown(None).await;
}

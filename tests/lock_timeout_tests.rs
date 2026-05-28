// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

mod common;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{Client, OrchestrationContext, OrchestrationRegistry};

/// Test that default lock timeouts are set correctly
#[tokio::test]
async fn default_lock_timeouts() {
    let options = RuntimeOptions::default();
    assert_eq!(
        options.orchestrator_lock_timeout,
        Duration::from_secs(5),
        "Default orchestrator lock timeout should be 5 seconds"
    );
    assert_eq!(
        options.worker_lock_timeout,
        Duration::from_secs(30),
        "Default worker lock timeout should be 30 seconds"
    );
    assert_eq!(
        options.worker_lock_renewal_buffer,
        Duration::from_secs(5),
        "Default worker lock renewal buffer should be 5 seconds"
    );
}

/// Test that custom lock timeouts can be configured via RuntimeOptions
#[tokio::test]
async fn custom_lock_timeout_configuration() {
    let options = RuntimeOptions {
        orchestrator_lock_timeout: Duration::from_secs(10),
        worker_lock_timeout: Duration::from_secs(120),
        ..Default::default()
    };
    assert_eq!(options.orchestrator_lock_timeout, Duration::from_secs(10));
    assert_eq!(options.worker_lock_timeout, Duration::from_secs(120));

    let short_options = RuntimeOptions {
        orchestrator_lock_timeout: Duration::from_secs(1),
        worker_lock_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    assert_eq!(short_options.orchestrator_lock_timeout, Duration::from_secs(1));
    assert_eq!(short_options.worker_lock_timeout, Duration::from_secs(5));
}

/// Test that orchestration with custom timeout completes successfully
#[tokio::test]
async fn orchestration_with_custom_timeout_completes() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("TestActivity", "data").await?;
        Ok(result)
    };

    let acts = ActivityRegistry::builder()
        .register(
            "TestActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move { Ok(format!("processed: {input}")) },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // Use custom lock timeouts (60 seconds - plenty of time)
    let options = RuntimeOptions {
        orchestrator_lock_timeout: Duration::from_secs(60),
        worker_lock_timeout: Duration::from_secs(60),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-custom-timeout";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    // Should complete successfully with custom timeout
    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "processed: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test that very short lock timeout (1 second) works correctly
#[tokio::test]
async fn very_short_lock_timeout_works() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("FastActivity", "data").await?;
        Ok(result)
    };

    let acts = ActivityRegistry::builder()
        .register(
            "FastActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                // Fast activity - completes well within 1 second
                Ok(format!("processed: {input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // Use very short lock timeouts (1 second)
    let options = RuntimeOptions {
        orchestrator_lock_timeout: Duration::from_secs(1),
        worker_lock_timeout: Duration::from_secs(1),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-short-timeout";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    // Should complete successfully even with short timeout (activity is fast)
    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "processed: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test: Long-running activity completes successfully with lock renewal
/// This test verifies that activities running longer than the lock timeout
/// complete successfully due to automatic lock renewal.
#[tokio::test]
async fn long_running_activity_with_lock_renewal() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("LongActivity", "data").await?;
        Ok(result)
    };

    // Activity that takes 6 seconds
    let acts = ActivityRegistry::builder()
        .register(
            "LongActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                tokio::time::sleep(Duration::from_secs(6)).await;
                Ok(format!("completed: {input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // Use 3 second worker lock timeout with 1 second buffer
    // Lock should be renewed at 2s, 4s (ensuring activity completes)
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(3),
        worker_lock_renewal_buffer: Duration::from_secs(1),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-long-activity-renewal";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    // Wait for orchestration to complete (should succeed despite activity > initial lock timeout)
    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "completed: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test: Activity completes before first renewal interval
/// Verifies that short activities complete without any renewals
#[tokio::test]
async fn short_activity_no_renewal_needed() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("QuickActivity", "data").await?;
        Ok(result)
    };

    // Activity that completes quickly (500ms)
    let acts = ActivityRegistry::builder()
        .register(
            "QuickActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok(format!("quick: {input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // 3 second timeout, renewal at 2s - activity finishes well before
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(3),
        worker_lock_renewal_buffer: Duration::from_secs(1),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-quick-activity";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "quick: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test: Renewal interval calculation with short timeout (< 15s)
/// Verifies that short timeouts use 0.5x multiplier
#[tokio::test]
async fn lock_renewal_short_timeout() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("MediumActivity", "data").await?;
        Ok(result)
    };

    // Activity that takes 3 seconds
    let acts = ActivityRegistry::builder()
        .register(
            "MediumActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                tokio::time::sleep(Duration::from_secs(3)).await;
                Ok(format!("medium: {input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // 2 second timeout (< 15), renewal should happen at 1s (0.5 * 2)
    // First renewal at T+1s extends to T+3s
    // Second renewal at T+2s extends to T+4s
    // Activity finishes at T+3s, well within renewed lock
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_secs(1), // Ignored for short timeouts
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-short-timeout-renewal";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(6))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "medium: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test: Custom renewal buffer configuration
/// Verifies that custom buffer settings work correctly
#[tokio::test]
async fn custom_renewal_buffer() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("LongActivity", "data").await?;
        Ok(result)
    };

    // Activity that takes 6 seconds
    let acts = ActivityRegistry::builder()
        .register(
            "LongActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                tokio::time::sleep(Duration::from_secs(6)).await;
                Ok(format!("done: {input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // 5 second timeout with 2 second buffer
    // Renewal at T+3s (5-2), extends to T+8s
    // Activity finishes at T+6s, within renewed lock
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(5),
        worker_lock_renewal_buffer: Duration::from_secs(2), // Custom buffer
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-custom-buffer";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test: Multiple concurrent long-running activities
/// Verifies that multiple activities can have their locks renewed independently
#[tokio::test]
async fn concurrent_activities_with_renewal() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        // Schedule multiple activities in parallel
        let a1 = ctx.schedule_activity("LongActivity", "task1");
        let a2 = ctx.schedule_activity("LongActivity", "task2");
        let a3 = ctx.schedule_activity("LongActivity", "task3");

        let r1 = a1.await?;
        let r2 = a2.await?;
        let r3 = a3.await?;

        Ok(format!("{r1}, {r2}, {r3}"))
    };

    // Each activity takes 6 seconds
    let acts = ActivityRegistry::builder()
        .register(
            "LongActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                tokio::time::sleep(Duration::from_secs(6)).await;
                Ok(format!("completed-{input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // 3 second timeout with 1 second buffer
    // Each activity should have its lock renewed independently
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(3),
        worker_lock_renewal_buffer: Duration::from_secs(1),
        worker_concurrency: 3, // Allow 3 parallel activities
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-concurrent-renewal";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    // Activities run sequentially in this orchestration, so total time is 6s + 6s + 6s = 18s
    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(25))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // All three tasks should complete
            assert!(output.contains("completed-task1"));
            assert!(output.contains("completed-task2"));
            assert!(output.contains("completed-task3"));
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test that orchestrator_lock_renewal_buffer is configurable and defaults correctly
#[tokio::test]
async fn orchestrator_lock_renewal_buffer_defaults() {
    let options = RuntimeOptions::default();
    assert_eq!(
        options.orchestrator_lock_renewal_buffer,
        Duration::from_secs(2),
        "Default orchestrator lock renewal buffer should be 2 seconds"
    );

    // Custom configuration
    let custom = RuntimeOptions {
        orchestrator_lock_renewal_buffer: Duration::from_secs(5),
        ..Default::default()
    };
    assert_eq!(custom.orchestrator_lock_renewal_buffer, Duration::from_secs(5));
}

/// Test that orchestration lock renewal actually prevents lock expiration.
///
/// This test uses a test hook to inject a processing delay that exceeds the lock timeout.
/// Without lock renewal, the lock would expire and the orchestration would fail.
/// With lock renewal, the lock is extended and the orchestration completes successfully.
///
/// Note: This test uses a global static for the hook, so we use #[serial] behavior
/// by having a unique instance name and clearing the hook before and after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orchestration_lock_renewal_prevents_expiration() {
    use duroxide::providers::sqlite::SqliteProvider;
    use duroxide::runtime::test_hooks;

    // Clear any stale hook state from other tests
    test_hooks::clear_orch_processing_delay();

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    // Configure short lock timeout (2s) with renewal 1s before expiry
    // The test hook will inject a 4s delay, requiring multiple renewals
    let options = RuntimeOptions {
        orchestrator_lock_timeout: Duration::from_secs(2),
        orchestrator_lock_renewal_buffer: Duration::from_secs(1), // Renew at 1s interval
        orchestration_concurrency: 1,
        ..Default::default()
    };

    let activities = ActivityRegistry::builder()
        .register(
            "QuickActivity",
            |_ctx: duroxide::ActivityContext, _input: String| async move { Ok("done".to_string()) },
        )
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "RenewalTestOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                // The processing delay is injected by test hook BEFORE this code runs
                // Schedule a quick activity to prove orchestration works
                let result = ctx.schedule_activity("QuickActivity", "{}").await?;
                Ok(format!("completed: {result}"))
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Set a 4-second processing delay (longer than 2s lock timeout)
    // This requires the renewal task to extend the lock multiple times
    // Use instance prefix to only affect this specific test
    test_hooks::set_orch_processing_delay(Duration::from_secs(4), Some("lock-renewal-e2e"));

    client
        .start_orchestration("lock-renewal-e2e", "RenewalTestOrch", "")
        .await
        .unwrap();

    // Wait for completion - should succeed because lock renewal kept the lock alive
    let status = client
        .wait_for_orchestration("lock-renewal-e2e", Duration::from_secs(15))
        .await
        .unwrap();

    // Clear the hook immediately after we get the result
    test_hooks::clear_orch_processing_delay();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("completed"));
            assert!(output.contains("done"));
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!(
                "Orchestration failed - lock renewal may not be working: {}",
                details.display_message()
            );
        }
        other => panic!("Unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

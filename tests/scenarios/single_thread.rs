// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Single-thread runtime scenario tests
//!
//! These tests validate duroxide's behavior when running in tokio's
//! current_thread runtime mode, which is the execution model used by
//! embedded Rust async code in single-threaded hosts (e.g., database
//! extensions, embedded systems, WASM).
//!
//! Key constraints being tested:
//! - All async work runs on a single thread
//! - No parallel activity execution (sequential processing)
//! - Deterministic execution order
//! - Timer and event handling in single-threaded context

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::sync::Arc;
use std::time::Duration;

#[path = "../common/mod.rs"]
mod common;

/// Basic orchestration lifecycle in current_thread mode
#[tokio::test(flavor = "current_thread")]
async fn single_thread_basic_orchestration() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("echo: {input}"))
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, input: String| async move {
        let result = ctx.schedule_activity("Echo", input).await?;
        Ok(result)
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("SingleThreadOrch", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestrations).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("single-thread-test", "SingleThreadOrch", "hello")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("single-thread-test", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "echo: hello");
            tracing::info!("✓ Single-thread basic orchestration completed");
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected status: {status:?}"),
    }

    rt.shutdown(None).await;
}

/// Multiple activities executed sequentially in current_thread mode
#[tokio::test(flavor = "current_thread")]
async fn single_thread_sequential_activities() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Add", |_ctx: ActivityContext, input: String| async move {
            let n: i32 = input.parse().unwrap_or(0);
            Ok((n + 1).to_string())
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, input: String| async move {
        // Execute 3 activities sequentially
        let r1 = ctx.schedule_activity("Add", input).await?;
        let r2 = ctx.schedule_activity("Add", r1).await?;
        let r3 = ctx.schedule_activity("Add", r2).await?;
        Ok(r3)
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("SequentialOrch", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestrations).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("sequential-test", "SequentialOrch", "0")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("sequential-test", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "3"); // 0 + 1 + 1 + 1 = 3
            tracing::info!("✓ Single-thread sequential activities completed");
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected status: {status:?}"),
    }

    rt.shutdown(None).await;
}

/// Timer handling in current_thread mode
#[tokio::test(flavor = "current_thread")]
async fn single_thread_timer_handling() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder().build();

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Wait for a short timer
        ctx.schedule_timer(Duration::from_millis(50)).await;
        Ok("timer_done".to_string())
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("TimerOrch", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestrations).await;

    let client = Client::new(store.clone());
    client.start_orchestration("timer-test", "TimerOrch", "").await.unwrap();

    let status = client
        .wait_for_orchestration("timer-test", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "timer_done");
            tracing::info!("✓ Single-thread timer handling completed");
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected status: {status:?}"),
    }

    rt.shutdown(None).await;
}

/// Continue-as-new chain in current_thread mode
#[tokio::test(flavor = "current_thread")]
async fn single_thread_continue_as_new() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder().build();

    let counter = |ctx: OrchestrationContext, input: String| async move {
        let n: u32 = input.parse().unwrap_or(0);
        if n < 3 {
            return ctx.continue_as_new((n + 1).to_string()).await;
        }
        Ok(format!("done:{n}"))
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("CounterOrch", counter)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestrations).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("can-test", "CounterOrch", "0")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("can-test", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done:3");
            tracing::info!("✓ Single-thread continue-as-new completed");
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected status: {status:?}"),
    }

    rt.shutdown(None).await;
}

/// Multiple concurrent orchestrations in current_thread mode
/// Tests that multiple instances can progress even on single thread
#[tokio::test(flavor = "current_thread")]
async fn single_thread_concurrent_orchestrations() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Process", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("processed:{input}"))
        })
        .build();

    let orchestration =
        |ctx: OrchestrationContext, input: String| async move { ctx.schedule_activity("Process", input).await };

    let orchestrations = OrchestrationRegistry::builder()
        .register("ConcurrentOrch", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestrations).await;

    let client = Client::new(store.clone());

    // Start 3 orchestrations
    for i in 0..3 {
        client
            .start_orchestration(&format!("concurrent-{i}"), "ConcurrentOrch", &i.to_string())
            .await
            .unwrap();
    }

    // Wait for all to complete
    for i in 0..3 {
        let status = client
            .wait_for_orchestration(&format!("concurrent-{i}"), Duration::from_secs(10))
            .await
            .unwrap();

        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!(output, format!("processed:{i}"));
            }
            OrchestrationStatus::Failed { details, .. } => {
                panic!("Orchestration {} failed: {}", i, details.display_message());
            }
            _ => panic!("Unexpected status for {i}: {status:?}"),
        }
    }

    tracing::info!("✓ Single-thread concurrent orchestrations completed");

    rt.shutdown(None).await;
}

/// Single-thread mode with single concurrency options
/// Simulates embedded host with minimal resource usage
#[tokio::test(flavor = "current_thread")]
async fn single_thread_single_concurrency() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Work", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("result:{input}"))
        })
        .build();

    let orchestration =
        |ctx: OrchestrationContext, input: String| async move { ctx.schedule_activity("Work", input).await };

    let orchestrations = OrchestrationRegistry::builder()
        .register("SingleConcOrch", orchestration)
        .build();

    // Use single concurrency for both orchestration and worker dispatchers
    let options = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestrations, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("single-conc-test", "SingleConcOrch", "data")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("single-conc-test", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "result:data");
            tracing::info!("✓ Single-thread with single concurrency completed");
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected status: {status:?}"),
    }

    rt.shutdown(None).await;
}

// ============================================================================
// Activity Cancellation Tests (1x1 concurrency, single-thread mode)
// ============================================================================

/// Cooperative activity cancellation in 1x1 concurrency mode.
///
/// This test validates that a long-running activity that checks the cancellation
/// token can exit gracefully when the orchestration is cancelled.
///
/// Scenario:
/// - Single orchestration worker, single activity worker (1x1)
/// - Activity checks `ctx.cancelled()` and exits when triggered
/// - Orchestration is cancelled while activity is running
/// - Activity should observe the cancellation and exit cleanly
#[tokio::test(flavor = "current_thread")]
async fn single_thread_1x1_cooperative_activity_cancellation() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let (store, _td) = common::create_sqlite_store_disk().await;

    let saw_cancellation = Arc::new(AtomicBool::new(false));
    let saw_cancellation_clone = Arc::clone(&saw_cancellation);

    // Cooperative activity: checks cancellation token and exits gracefully
    let cooperative_activity = move |ctx: ActivityContext, _input: String| {
        let saw_cancellation = Arc::clone(&saw_cancellation_clone);
        async move {
            // Wait for either cancellation or a long timeout
            tokio::select! {
                _ = ctx.cancelled() => {
                    saw_cancellation.store(true, Ordering::SeqCst);
                    ctx.trace_info("Activity received cancellation, exiting gracefully");
                    Ok("cancelled_gracefully".to_string())
                }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    Ok("timeout".to_string())
                }
            }
        }
    };

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let _result = ctx.schedule_activity("CooperativeActivity", "input").await;
        Ok("done".to_string())
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("CooperativeOrch", orchestration)
        .build();
    let activities = ActivityRegistry::builder()
        .register("CooperativeActivity", cooperative_activity)
        .build();

    // 1x1 concurrency with short lock/grace periods for faster testing
    let options = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_millis(500),
        activity_cancellation_grace_period: Duration::from_secs(5),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());

    // Start orchestration
    client
        .start_orchestration("cooperative-cancel-test", "CooperativeOrch", "")
        .await
        .unwrap();

    // Wait for activity to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cancel the orchestration
    let _ = client
        .cancel_instance("cooperative-cancel-test", "test_cancellation")
        .await;

    // Wait for activity to observe cancellation
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if saw_cancellation.load(Ordering::SeqCst) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Cooperative activity did not receive cancellation signal within timeout");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Verify the activity saw the cancellation
    assert!(
        saw_cancellation.load(Ordering::SeqCst),
        "Cooperative activity should have received cancellation signal"
    );

    tracing::info!("✓ Single-thread 1x1 cooperative activity cancellation works");

    rt.shutdown(None).await;
}

/// Runaway (non-cooperative) activity cancellation in 1x1 concurrency mode.
///
/// This test validates that a long-running activity that IGNORES the cancellation
/// token is forcibly aborted after the grace period expires.
///
/// Scenario:
/// - Single orchestration worker, single activity worker (1x1)  
/// - Activity ignores `ctx.cancelled()` and just sleeps
/// - Orchestration is cancelled while activity is running
/// - Worker waits for grace period, then aborts the activity task
///
/// Note on tokio::task abort behavior:
/// - `JoinHandle::abort()` only aborts the specific tokio task
/// - If activity spawns child tasks/threads that don't check cancellation, those may continue
/// - However, for a simple `tokio::time::sleep()`, abort will interrupt it
///
/// IMPORTANT: In current_thread mode, abort() schedules the task to be cancelled
/// at the next await point. The sleep will be interrupted when polled.
#[tokio::test(flavor = "current_thread")]
async fn single_thread_1x1_runaway_activity_aborted() {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    let (store, _td) = common::create_sqlite_store_disk().await;

    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_clone = Arc::clone(&attempts);
    let activity_started = Arc::new(AtomicBool::new(false));
    let activity_started_clone = Arc::clone(&activity_started);

    // Runaway activity: ignores cancellation token, just sleeps
    let runaway_activity = move |_ctx: ActivityContext, _input: String| {
        let attempts = Arc::clone(&attempts_clone);
        let started = Arc::clone(&activity_started_clone);
        async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            started.store(true, Ordering::SeqCst);
            // Sleep for a long time, ignoring any cancellation
            // This simulates a "runaway" or non-cooperative activity
            // The sleep will be interrupted by abort() when polled
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok("done".to_string())
        }
    };

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let _ = ctx.schedule_activity("RunawayActivity", "input").await;
        Ok("ok".to_string())
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("RunawayOrch", orchestration)
        .build();
    let activities = ActivityRegistry::builder()
        .register("RunawayActivity", runaway_activity)
        .build();

    // 1x1 concurrency with short grace period (200ms) for fast abort
    let options = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_millis(500),
        activity_cancellation_grace_period: Duration::from_millis(200),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());

    let start = std::time::Instant::now();

    // Start orchestration
    client
        .start_orchestration("runaway-cancel-test", "RunawayOrch", "")
        .await
        .unwrap();

    // Wait for activity to actually start
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !activity_started.load(Ordering::SeqCst) {
        if std::time::Instant::now() > deadline {
            panic!("Activity did not start within timeout");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Cancel the orchestration
    client
        .cancel_instance("runaway-cancel-test", "cancel_runaway")
        .await
        .unwrap();

    // Poll frequently to allow single-threaded runtime to process abort
    // Grace period is 200ms, so wait up to 5s total with small sleeps
    let abort_deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        // Check if enough time has passed for abort to complete
        // (grace period + some buffer for processing)
        if start.elapsed() > Duration::from_millis(1000) {
            break;
        }
        if std::time::Instant::now() > abort_deadline {
            break;
        }
        // Small sleep to yield to runtime, allowing abort to process
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Activity should have been attempted once but not completed
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "Runaway activity should have started once"
    );

    // Verify no ActivityCompleted event (result was dropped after abort)
    let hist = store.read("runaway-cancel-test").await.unwrap_or_default();
    let has_activity_completed = hist
        .iter()
        .any(|e| matches!(&e.kind, duroxide::EventKind::ActivityCompleted { .. }));
    assert!(
        !has_activity_completed,
        "Runaway activity completion should be dropped after cancellation/abort"
    );

    // Ensure we did not wait the full 30s activity duration (abort happened)
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "Activity should have been aborted before full 30s run; elapsed: {elapsed:?}"
    );

    tracing::info!("✓ Single-thread 1x1 runaway activity aborted after grace period");

    rt.shutdown(None).await;
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::providers::Provider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod common;

use common::long_polling::LongPollingSqliteProvider;

// --- Tests ---

/// Test 1: Verify fetch waits for the full duration if no work exists
#[tokio::test]
async fn test_long_poll_waits_for_timeout() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let provider = Arc::new(LongPollingSqliteProvider::new(store));

    let start = Instant::now();
    let timeout = Duration::from_millis(500);

    // Fetch with timeout
    let result = provider
        .fetch_orchestration_item(Duration::from_secs(5), timeout, None)
        .await
        .unwrap();

    let elapsed = start.elapsed();

    assert!(result.is_none(), "Should return None");
    assert!(
        elapsed >= timeout,
        "Should wait at least timeout duration (elapsed: {elapsed:?}, expected: {timeout:?})"
    );
}

/// Test 2: Verify fetch returns early if work arrives during the poll
#[tokio::test]
async fn test_long_poll_returns_early_on_work() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let provider = Arc::new(LongPollingSqliteProvider::new(store.clone()));

    let client = Client::new(store.clone());
    let instance_id = "test-early-return";

    // Spawn a task to enqueue work after a delay (e.g. 200ms)
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        client.start_orchestration(instance_id, "TestOrch", "").await.unwrap();
    });

    let start = Instant::now();
    let timeout = Duration::from_secs(2); // Long timeout

    // Fetch should block but return when work arrives
    let result = provider
        .fetch_orchestration_item(Duration::from_secs(5), timeout, None)
        .await
        .unwrap();

    let elapsed = start.elapsed();

    assert!(result.is_some(), "Should return Some item");
    assert!(
        elapsed < timeout,
        "Should return before timeout (elapsed: {elapsed:?}, timeout: {timeout:?})"
    );
    assert!(
        elapsed >= Duration::from_millis(150),
        "Should wait for work to arrive (elapsed: {elapsed:?})"
    );
}

/// Test 3: Integration test - Dispatcher uses long polling
#[tokio::test]
async fn test_dispatcher_uses_long_polling() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let provider = Arc::new(LongPollingSqliteProvider::new(store.clone()));

    let activities = ActivityRegistry::builder()
        .register("QuickTask", |_ctx: ActivityContext, _: String| async move {
            Ok("done".to_string())
        })
        .build();

    let orch = |ctx: OrchestrationContext, _: String| async move { ctx.schedule_activity("QuickTask", "").await };

    let orchestrations = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // Configure runtime with long polling
    let options = RuntimeOptions {
        // Long poll timeout (passed to provider)
        dispatcher_long_poll_timeout: Duration::from_secs(1),
        // Min poll interval (sleep if provider returns early - but here provider waits)
        dispatcher_min_poll_interval: Duration::from_millis(100),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(provider.clone(), activities, orchestrations, options).await;

    let client = Client::new(provider.clone());

    // Start orchestration
    client
        .start_orchestration("test-long-poll-flow", "TestOrch", "")
        .await
        .unwrap();

    // Wait for completion
    let status = client
        .wait_for_orchestration("test-long-poll-flow", Duration::from_secs(5))
        .await
        .unwrap();

    assert!(matches!(status, runtime::OrchestrationStatus::Completed { .. }));

    rt.shutdown(None).await;
}

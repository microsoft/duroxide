// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Tests for RuntimeOptions configuration
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use std::time::{Duration, Instant};

mod common;

/// Test: Runtime uses default polling frequency (10ms)
#[tokio::test]
async fn test_default_polling_frequency() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("QuickTask", |_ctx: ActivityContext, _: String| async move {
            Ok("done".to_string())
        })
        .build();

    let orch = |ctx: OrchestrationContext, _: String| async move { ctx.schedule_activity("QuickTask", "").await };

    let orchestrations = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // Start with default options (10ms polling)
    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = Client::new(store.clone());
    let start_time = Instant::now();

    client
        .start_orchestration("test-default", "TestOrch", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("test-default", std::time::Duration::from_secs(2))
        .await
        .unwrap();

    let elapsed = start_time.elapsed();

    assert!(matches!(status, runtime::OrchestrationStatus::Completed { .. }));
    // With 10ms polling, should complete reasonably fast (< 500ms for simple workflow)
    assert!(elapsed.as_millis() < 500, "Took too long: {}ms", elapsed.as_millis());

    rt.shutdown(None).await;
}

/// Test: Runtime uses custom polling frequency (50ms)
#[tokio::test]
async fn test_custom_polling_frequency() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("QuickTask", |_ctx: ActivityContext, _: String| async move {
            Ok("done".to_string())
        })
        .build();

    let orch = |ctx: OrchestrationContext, _: String| async move { ctx.schedule_activity("QuickTask", "").await };

    let orchestrations = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // Start with slower polling (50ms)
    let options = RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(50),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());

    client.start_orchestration("test-custom", "TestOrch", "").await.unwrap();

    let status = client
        .wait_for_orchestration("test-custom", std::time::Duration::from_secs(2))
        .await
        .unwrap();

    assert!(matches!(status, runtime::OrchestrationStatus::Completed { .. }));

    rt.shutdown(None).await;
}

/// Test: Fast polling (1ms) for high-throughput scenarios
#[tokio::test]
async fn test_fast_polling() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("Task", |_ctx: ActivityContext, _: String| async move {
            Ok("done".to_string())
        })
        .build();

    let orch = |ctx: OrchestrationContext, _: String| async move { ctx.schedule_activity("Task", "").await };

    let orchestrations = OrchestrationRegistry::builder().register("FastOrch", orch).build();

    // Very responsive: 1ms polling
    let options = RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(1),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());
    let start_time = Instant::now();

    client.start_orchestration("test-fast", "FastOrch", "").await.unwrap();

    let status = client
        .wait_for_orchestration("test-fast", std::time::Duration::from_secs(2))
        .await
        .unwrap();

    let elapsed = start_time.elapsed();

    assert!(matches!(status, runtime::OrchestrationStatus::Completed { .. }));
    // Fast polling should complete very quickly
    assert!(elapsed.as_millis() < 300, "Took too long: {}ms", elapsed.as_millis());

    rt.shutdown(None).await;
}

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

use common::long_polling::{LongPollingSqliteProvider, ProviderGate, ProviderOperation};

// --- Tests ---

#[tokio::test]
async fn lock_expiration_validation_waits_for_the_first_claim() {
    use duroxide::provider_validation::{ProviderFactory, instance_locking};
    use duroxide::providers::sqlite::SqliteProvider;

    struct GatedFactory(Arc<dyn Provider>);

    #[async_trait::async_trait]
    impl ProviderFactory for GatedFactory {
        async fn create_provider(&self) -> Arc<dyn Provider> {
            self.0.clone()
        }

        fn lock_timeout(&self) -> Duration {
            Duration::from_millis(500)
        }
    }

    let gate = Arc::new(ProviderGate::default());
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    // Isolate claim timing from SQLite's concurrent-writer behavior.
    let mut held_connections = Vec::new();
    for _ in 1..store.get_pool().options().get_max_connections() {
        held_connections.push(store.get_pool().acquire().await.unwrap());
    }
    let provider = Arc::new(
        LongPollingSqliteProvider::new(store).with_gate(ProviderOperation::BeforeOrchestrationFetch, gate.clone()),
    );
    let validation = tokio::spawn(async move {
        instance_locking::test_multi_threaded_lock_expiration_recovery(&GatedFactory(provider)).await;
    });
    tokio::time::timeout(Duration::from_secs(2), gate.wait_until_entered())
        .await
        .expect("the first fetch should reach the gate");
    // Exceed both the lease and the recovery margin before permitting a claim.
    tokio::time::sleep(Duration::from_millis(700)).await;
    gate.open();
    tokio::time::timeout(Duration::from_secs(3), validation)
        .await
        .expect("claim-relative recovery should finish")
        .expect("connection acquisition time must not consume the lease-expiry margin");
}

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

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::providers::{
    DispatcherCapabilityFilter, ExecutionMetadata, OrchestrationItem, Provider, ProviderError,
    ScheduledActivityIdentifier, SessionFetchConfig, TagFilter, WorkItem,
};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, Event, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod common;

/// A wrapper around any Provider that simulates long polling behavior.
///
/// It implements `fetch_*` by polling the inner provider in a loop until
/// work is found or the timeout expires.
struct LongPollingSqliteProvider {
    inner: Arc<dyn Provider>,
}

impl LongPollingSqliteProvider {
    fn new(inner: Arc<dyn Provider>) -> Self {
        Self { inner }
    }

    /// Helper to poll inner provider until timeout
    async fn poll_until<T, F, Fut>(&self, poll_timeout: Duration, f: F) -> Result<Option<T>, ProviderError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Option<T>, ProviderError>>,
    {
        let start = Instant::now();
        loop {
            // Try to fetch
            if let Some(item) = f().await? {
                return Ok(Some(item));
            }

            // Check timeout
            if start.elapsed() >= poll_timeout {
                return Ok(None);
            }

            // Wait a bit before retrying (simulating internal wait)
            // Use a short interval to be responsive to work arrival in tests
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[async_trait::async_trait]
impl Provider for LongPollingSqliteProvider {
    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        _filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        self.poll_until(poll_timeout, || {
            self.inner.fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        })
        .await
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        // Clone session config for use in closure iterations
        let session_owned = session.cloned();
        let tag_filter = tag_filter.clone();
        self.poll_until(poll_timeout, || {
            self.inner
                .fetch_work_item(lock_timeout, Duration::ZERO, session_owned.as_ref(), &tag_filter)
        })
        .await
    }

    // Pass-through methods
    async fn ack_orchestration_item(
        &self,
        lock_token: &str,
        execution_id: u64,
        history_delta: Vec<Event>,
        worker_items: Vec<WorkItem>,
        orchestrator_items: Vec<WorkItem>,
        metadata: ExecutionMetadata,
        cancelled_activities: Vec<ScheduledActivityIdentifier>,
    ) -> Result<(), ProviderError> {
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

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        self.inner.read(instance).await
    }

    async fn append_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
        new_events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        self.inner
            .append_with_execution(instance, execution_id, new_events)
            .await
    }

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        self.inner.enqueue_for_worker(item).await
    }

    async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) -> Result<(), ProviderError> {
        self.inner.ack_work_item(token, completion).await
    }

    async fn renew_work_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
        self.inner.renew_work_item_lock(token, extend_for).await
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
        self.inner.enqueue_for_orchestrator(item, delay).await
    }

    // Optional methods
    async fn read_with_execution(&self, instance: &str, execution_id: u64) -> Result<Vec<Event>, ProviderError> {
        self.inner.read_with_execution(instance, execution_id).await
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        self.inner.get_custom_status(instance, last_seen_version).await
    }
}

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

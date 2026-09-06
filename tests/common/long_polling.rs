// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! A `Provider` wrapper that simulates true long polling.
//!
//! The bundled SQLite provider ignores `poll_timeout` and returns immediately, so
//! dispatcher loops spin every `dispatcher_min_poll_interval`. Providers that honour
//! the long-poll timeout (e.g. Postgres `LISTEN/NOTIFY`) park inside `fetch_*` for
//! tens of seconds, which is the condition under which shutdown bugs surface.
//!
//! Also carries a leak sentinel: every in-flight fetch holds a clone of `sentinel`,
//! so a test can assert `Arc::strong_count(&sentinel) == 1` to prove no poller task
//! survived shutdown.

use duroxide::Event;
use duroxide::providers::{
    DispatcherCapabilityFilter, ExecutionMetadata, OrchestrationItem, Provider, ProviderError,
    ScheduledActivityIdentifier, SessionFetchConfig, TagFilter, WorkItem,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A wrapper around any Provider that simulates long polling behavior.
///
/// It implements `fetch_*` by polling the inner provider in a loop until
/// work is found or the timeout expires.
pub struct LongPollingSqliteProvider {
    inner: Arc<dyn Provider>,
    /// Cloned into every in-flight fetch; strong count > 1 after shutdown means a
    /// poller task leaked.
    sentinel: Arc<()>,
}

impl LongPollingSqliteProvider {
    pub fn new(inner: Arc<dyn Provider>) -> Self {
        Self {
            inner,
            sentinel: Arc::new(()),
        }
    }

    /// Handle used to detect leaked poller tasks after shutdown.
    pub fn sentinel(&self) -> Arc<()> {
        Arc::clone(&self.sentinel)
    }

    /// Helper to poll inner provider until timeout
    async fn poll_until<T, F, Fut>(&self, poll_timeout: Duration, f: F) -> Result<Option<T>, ProviderError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Option<T>, ProviderError>>,
    {
        // Dropped when the calling task's future is dropped (i.e. on abort).
        let _in_flight = Arc::clone(&self.sentinel);

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

    async fn get_kv_value(&self, instance: &str, key: &str) -> Result<Option<String>, ProviderError> {
        self.inner.get_kv_value(instance, key).await
    }

    async fn get_kv_all_values(
        &self,
        instance: &str,
    ) -> Result<std::collections::HashMap<String, String>, ProviderError> {
        self.inner.get_kv_all_values(instance).await
    }

    async fn get_instance_stats(&self, instance: &str) -> Result<Option<duroxide::SystemStats>, ProviderError> {
        self.inner.get_instance_stats(instance).await
    }
}

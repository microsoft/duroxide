//! Instrumented provider wrapper that adds metrics to any provider implementation.

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use super::{
    DispatcherCapabilityFilter, ExecutionMetadata, OrchestrationItem, Provider, ProviderAdmin, ProviderError,
    ScheduledActivityIdentifier, SessionFetchConfig, TagFilter, WorkItem,
};
use crate::Event;
use crate::runtime::observability::MetricsProvider;

/// Wrapper that adds metrics instrumentation to any Provider implementation.
///
/// This follows the decorator pattern to automatically record:
/// - Operation duration for all provider methods
/// - Error counts by operation type
/// - Success/failure status
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use duroxide::providers::sqlite::SqliteProvider;
/// use duroxide::providers::instrumented::InstrumentedProvider;
/// use duroxide::providers::Provider;
///
/// # async fn example() {
/// let provider = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
/// let metrics = None; // Or Some(Arc<MetricsProvider>)
///
/// // Wrap provider with instrumentation
/// let instrumented: Arc<dyn Provider> = Arc::new(
///     InstrumentedProvider::new(provider, metrics)
/// );
///
/// // All operations now automatically emit metrics!
/// # }
/// ```
pub struct InstrumentedProvider {
    inner: Arc<dyn Provider>,
    metrics: Option<Arc<MetricsProvider>>,
}

impl InstrumentedProvider {
    pub fn new(inner: Arc<dyn Provider>, metrics: Option<Arc<MetricsProvider>>) -> Self {
        Self { inner, metrics }
    }

    #[inline]
    fn record_operation(&self, operation: &str, duration: Duration, status: &str) {
        if let Some(ref m) = self.metrics {
            m.record_provider_operation(operation, duration.as_secs_f64(), status);
        }
    }

    #[inline]
    fn record_error(&self, operation: &str, error: &ProviderError) {
        if let Some(ref m) = self.metrics {
            let error_type = if error.message.contains("deadlock") {
                "deadlock"
            } else if error.message.contains("timeout") {
                "timeout"
            } else if error.message.contains("connection") {
                "connection"
            } else {
                "other"
            };
            m.record_provider_error(operation, error_type);
        }
    }
}

#[async_trait]
impl Provider for InstrumentedProvider {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn version(&self) -> &str {
        self.inner.version()
    }

    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        let start = std::time::Instant::now();
        let result = self
            .inner
            .fetch_orchestration_item(lock_timeout, poll_timeout, filter)
            .await;
        let duration = start.elapsed();

        self.record_operation(
            "fetch_orchestration_item",
            duration,
            if result.is_ok() { "success" } else { "error" },
        );
        if let Err(ref e) = result {
            self.record_error("fetch_orchestration_item", e);
        }

        result
    }

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
        let start = std::time::Instant::now();
        let result = self
            .inner
            .ack_orchestration_item(
                lock_token,
                execution_id,
                history_delta,
                worker_items,
                orchestrator_items,
                metadata,
                cancelled_activities,
            )
            .await;
        let duration = start.elapsed();

        self.record_operation(
            "ack_orchestration_item",
            duration,
            if result.is_ok() { "success" } else { "error" },
        );
        if let Err(ref e) = result {
            self.record_error("ack_orchestration_item", e);
        }

        result
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
        let start = std::time::Instant::now();
        let result = self.inner.read(instance).await;
        let duration = start.elapsed();

        self.record_operation("read", duration, if result.is_ok() { "success" } else { "error" });
        if let Err(ref e) = result {
            self.record_error("read", e);
        }

        result
    }

    async fn read_with_execution(&self, instance: &str, execution_id: u64) -> Result<Vec<Event>, ProviderError> {
        self.inner.read_with_execution(instance, execution_id).await
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

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        let start = std::time::Instant::now();
        let result = self
            .inner
            .fetch_work_item(lock_timeout, poll_timeout, session, tag_filter)
            .await;
        let duration = start.elapsed();

        self.record_operation(
            "fetch_work_item",
            duration,
            if result.is_ok() { "success" } else { "error" },
        );
        if let Err(ref e) = result {
            self.record_error("fetch_work_item", e);
        }

        result
    }

    async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) -> Result<(), ProviderError> {
        let start = std::time::Instant::now();
        let result = self.inner.ack_work_item(token, completion).await;
        let duration = start.elapsed();

        self.record_operation(
            "ack_work_item",
            duration,
            if result.is_ok() { "success" } else { "error" },
        );
        if let Err(ref e) = result {
            self.record_error("ack_work_item", e);
        }

        result
    }

    async fn renew_work_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
        self.inner.renew_work_item_lock(token, extend_for).await
    }

    async fn renew_session_lock(
        &self,
        owner_ids: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        let start = std::time::Instant::now();
        let result = self.inner.renew_session_lock(owner_ids, extend_for, idle_timeout).await;
        self.record_operation(
            "renew_session_lock",
            start.elapsed(),
            if result.is_ok() { "success" } else { "error" },
        );
        if let Err(ref e) = result {
            self.record_error("renew_session_lock", e);
        }
        result
    }

    async fn cleanup_orphaned_sessions(&self, idle_timeout: Duration) -> Result<usize, ProviderError> {
        let start = std::time::Instant::now();
        let result = self.inner.cleanup_orphaned_sessions(idle_timeout).await;
        self.record_operation(
            "cleanup_orphaned_sessions",
            start.elapsed(),
            if result.is_ok() { "success" } else { "error" },
        );
        if let Err(ref e) = result {
            self.record_error("cleanup_orphaned_sessions", e);
        }
        result
    }

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        self.inner.abandon_work_item(token, delay, ignore_attempt).await
    }

    async fn renew_orchestration_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
        self.inner.renew_orchestration_item_lock(token, extend_for).await
    }

    async fn enqueue_for_orchestrator(&self, item: WorkItem, delay: Option<Duration>) -> Result<(), ProviderError> {
        let start = std::time::Instant::now();
        let result = self.inner.enqueue_for_orchestrator(item, delay).await;
        let duration = start.elapsed();

        self.record_operation(
            "enqueue_orchestrator",
            duration,
            if result.is_ok() { "success" } else { "error" },
        );
        if let Err(ref e) = result {
            self.record_error("enqueue_orchestrator", e);
        }

        result
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
}

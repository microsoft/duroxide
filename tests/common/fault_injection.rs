//! Fault Injection Providers for Testing
//!
//! Provides wrapper providers that can inject faults for testing error handling,
//! poison message detection, and observability.

// These types are used by poison_message_tests but not by all test files that import common.
#![allow(dead_code)]

use async_trait::async_trait;
use duroxide::providers::error::ProviderError;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::{
    DispatcherCapabilityFilter, ExecutionMetadata, OrchestrationItem, Provider, ProviderAdmin,
    ScheduledActivityIdentifier, SessionFetchConfig, TagFilter, WorkItem,
};
use duroxide::{Event, EventKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

/// A provider wrapper that can inject poison conditions by artificially
/// inflating attempt counts on fetch operations.
///
/// This allows testing poison message detection without actually abandoning
/// messages max_attempts times.
pub struct PoisonInjectingProvider {
    inner: Arc<SqliteProvider>,
    /// Orchestration fetch will return this attempt_count (0 = no injection)
    inject_orchestration_attempt_count: AtomicU32,
    /// Activity fetch will return this attempt_count (0 = no injection)
    inject_activity_attempt_count: AtomicU32,
    /// If true, orchestration injection persists across fetches until cleared
    orchestration_injection_persistent: AtomicBool,
    /// If true, activity injection persists across fetches until cleared
    activity_injection_persistent: AtomicBool,
    /// Skip this many orchestration fetches before injecting (0 = immediate)
    orchestration_skip_count: AtomicU32,
    /// Skip this many activity fetches before injecting (0 = immediate)
    activity_skip_count: AtomicU32,
}

impl PoisonInjectingProvider {
    pub fn new(inner: Arc<SqliteProvider>) -> Self {
        Self {
            inner,
            inject_orchestration_attempt_count: AtomicU32::new(0),
            inject_activity_attempt_count: AtomicU32::new(0),
            orchestration_injection_persistent: AtomicBool::new(false),
            activity_injection_persistent: AtomicBool::new(false),
            orchestration_skip_count: AtomicU32::new(0),
            activity_skip_count: AtomicU32::new(0),
        }
    }

    /// Next orchestration fetch will return this attempt_count instead of real one.
    /// Automatically resets after one fetch.
    pub fn inject_orchestration_poison(&self, attempt_count: u32) {
        self.orchestration_injection_persistent.store(false, Ordering::SeqCst);
        self.orchestration_skip_count.store(0, Ordering::SeqCst);
        self.inject_orchestration_attempt_count
            .store(attempt_count, Ordering::SeqCst);
    }

    /// All orchestration fetches will return this attempt_count until cleared.
    #[allow(dead_code)]
    pub fn inject_orchestration_poison_persistent(&self, attempt_count: u32) {
        self.orchestration_injection_persistent.store(true, Ordering::SeqCst);
        self.orchestration_skip_count.store(0, Ordering::SeqCst);
        self.inject_orchestration_attempt_count
            .store(attempt_count, Ordering::SeqCst);
    }

    /// Skip N orchestration fetches, then inject poison on the NEXT fetch only.
    /// Useful for poisoning sub-orchestrations after parent has been processed.
    pub fn inject_orchestration_poison_after_skip(&self, skip: u32, attempt_count: u32) {
        self.orchestration_injection_persistent.store(false, Ordering::SeqCst);
        self.orchestration_skip_count.store(skip, Ordering::SeqCst);
        self.inject_orchestration_attempt_count
            .store(attempt_count, Ordering::SeqCst);
    }

    /// Next activity fetch will return this attempt_count instead of real one.
    /// Automatically resets after one fetch.
    #[allow(dead_code)]
    pub fn inject_activity_poison(&self, attempt_count: u32) {
        self.activity_injection_persistent.store(false, Ordering::SeqCst);
        self.activity_skip_count.store(0, Ordering::SeqCst);
        self.inject_activity_attempt_count
            .store(attempt_count, Ordering::SeqCst);
    }

    /// All activity fetches will return this attempt_count until cleared.
    pub fn inject_activity_poison_persistent(&self, attempt_count: u32) {
        self.activity_injection_persistent.store(true, Ordering::SeqCst);
        self.activity_skip_count.store(0, Ordering::SeqCst);
        self.inject_activity_attempt_count
            .store(attempt_count, Ordering::SeqCst);
    }

    /// Clear all injections.
    #[allow(dead_code)]
    pub fn clear_injections(&self) {
        self.inject_orchestration_attempt_count.store(0, Ordering::SeqCst);
        self.inject_activity_attempt_count.store(0, Ordering::SeqCst);
        self.orchestration_injection_persistent.store(false, Ordering::SeqCst);
        self.activity_injection_persistent.store(false, Ordering::SeqCst);
        self.orchestration_skip_count.store(0, Ordering::SeqCst);
        self.activity_skip_count.store(0, Ordering::SeqCst);
    }
}

#[async_trait]
impl Provider for PoisonInjectingProvider {
    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        let result = self
            .inner
            .fetch_orchestration_item(lock_timeout, poll_timeout, filter)
            .await?;

        if let Some((item, lock_token, real_attempt_count)) = result {
            // Check if we need to skip this fetch
            let skip = self.orchestration_skip_count.load(Ordering::SeqCst);
            if skip > 0 {
                self.orchestration_skip_count.fetch_sub(1, Ordering::SeqCst);
                return Ok(Some((item, lock_token, real_attempt_count)));
            }

            let persistent = self.orchestration_injection_persistent.load(Ordering::SeqCst);
            let injected = if persistent {
                self.inject_orchestration_attempt_count.load(Ordering::SeqCst)
            } else {
                self.inject_orchestration_attempt_count.swap(0, Ordering::SeqCst)
            };
            let attempt_count = if injected > 0 { injected } else { real_attempt_count };
            Ok(Some((item, lock_token, attempt_count)))
        } else {
            Ok(None)
        }
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        let result = self
            .inner
            .fetch_work_item(lock_timeout, poll_timeout, session, tag_filter)
            .await?;

        if let Some((item, lock_token, real_attempt_count)) = result {
            // Check if we need to skip this fetch
            let skip = self.activity_skip_count.load(Ordering::SeqCst);
            if skip > 0 {
                self.activity_skip_count.fetch_sub(1, Ordering::SeqCst);
                return Ok(Some((item, lock_token, real_attempt_count)));
            }

            let persistent = self.activity_injection_persistent.load(Ordering::SeqCst);
            let injected = if persistent {
                self.inject_activity_attempt_count.load(Ordering::SeqCst)
            } else {
                self.inject_activity_attempt_count.swap(0, Ordering::SeqCst)
            };
            let attempt_count = if injected > 0 { injected } else { real_attempt_count };
            Ok(Some((item, lock_token, attempt_count)))
        } else {
            Ok(None)
        }
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

    async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) -> Result<(), ProviderError> {
        self.inner.ack_work_item(token, completion).await
    }

    async fn renew_work_item_lock(&self, token: &str, extension: Duration) -> Result<(), ProviderError> {
        self.inner.renew_work_item_lock(token, extension).await
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

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        self.inner.enqueue_for_worker(item).await
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        self.inner.read(instance).await
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

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        // Delegate to inner's management capability
        self.inner.as_management_capability()
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
}

/// A provider wrapper that bypasses the capability filter on fetch_orchestration_item.
///
/// This allows testing the runtime's defense-in-depth check by making the
/// provider return items that should have been filtered out. The runtime-side
/// check (in orchestration.rs) should catch and abandon these items.
pub struct FilterBypassProvider {
    inner: Arc<dyn Provider + Send + Sync>,
}

impl FilterBypassProvider {
    pub fn new(inner: Arc<dyn Provider + Send + Sync>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Provider for FilterBypassProvider {
    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        _filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        // Bypass: always fetch with filter=None, ignoring whatever the runtime passed
        self.inner
            .fetch_orchestration_item(lock_timeout, poll_timeout, None)
            .await
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        self.inner
            .fetch_work_item(lock_timeout, poll_timeout, session, tag_filter)
            .await
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

    async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) -> Result<(), ProviderError> {
        self.inner.ack_work_item(token, completion).await
    }

    async fn renew_work_item_lock(&self, token: &str, extension: Duration) -> Result<(), ProviderError> {
        self.inner.renew_work_item_lock(token, extension).await
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

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        self.inner.enqueue_for_worker(item).await
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        self.inner.read(instance).await
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

    async fn get_kv_value(&self, instance: &str, key: &str) -> Result<Option<String>, ProviderError> {
        self.inner.get_kv_value(instance, key).await
    }

    async fn get_kv_all_values(
        &self,
        instance: &str,
    ) -> Result<std::collections::HashMap<String, String>, ProviderError> {
        self.inner.get_kv_all_values(instance).await
    }
}

/// A provider wrapper that can inject transient failures for testing
/// error handling and retry logic.
pub struct FailingProvider {
    inner: Arc<SqliteProvider>,
    fail_next_ack_work_item: AtomicBool,
    fail_next_ack_orchestration_item: AtomicBool,
    fail_next_fetch_orchestration_item: AtomicBool,
    fail_next_fetch_work_item: AtomicBool,
    /// If true, allow failure event commits to succeed even when failing acks
    allow_failure_commits: AtomicBool,
    /// If true, do the actual ack before returning error (for testing post-ack failures)
    ack_then_fail: AtomicBool,
}

impl FailingProvider {
    pub fn new(inner: Arc<SqliteProvider>) -> Self {
        Self {
            inner,
            fail_next_ack_work_item: AtomicBool::new(false),
            fail_next_ack_orchestration_item: AtomicBool::new(false),
            fail_next_fetch_orchestration_item: AtomicBool::new(false),
            fail_next_fetch_work_item: AtomicBool::new(false),
            allow_failure_commits: AtomicBool::new(false),
            ack_then_fail: AtomicBool::new(false),
        }
    }

    pub fn fail_next_ack_work_item(&self) {
        self.fail_next_ack_work_item.store(true, Ordering::SeqCst);
    }

    pub fn fail_next_ack_orchestration_item(&self) {
        self.fail_next_ack_orchestration_item.store(true, Ordering::SeqCst);
    }

    pub fn fail_next_fetch_orchestration_item(&self) {
        self.fail_next_fetch_orchestration_item.store(true, Ordering::SeqCst);
    }

    pub fn fail_next_fetch_work_item(&self) {
        self.fail_next_fetch_work_item.store(true, Ordering::SeqCst);
    }

    /// When set, failure event commits (OrchestrationFailed) will succeed
    /// even when ack_orchestration_item is set to fail.
    pub fn set_allow_failure_commits(&self, allow: bool) {
        self.allow_failure_commits.store(allow, Ordering::SeqCst);
    }

    /// When set, ack operations will complete successfully before returning error.
    /// Useful for testing scenarios where ack succeeds but caller thinks it failed.
    pub fn set_ack_then_fail(&self, enabled: bool) {
        self.ack_then_fail.store(enabled, Ordering::SeqCst);
    }
}

#[async_trait]
impl Provider for FailingProvider {
    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        if self.fail_next_fetch_orchestration_item.swap(false, Ordering::SeqCst) {
            Err(ProviderError::retryable(
                "fetch_orchestration_item",
                "simulated transient infrastructure failure",
            ))
        } else {
            self.inner
                .fetch_orchestration_item(lock_timeout, poll_timeout, filter)
                .await
        }
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        if self.fail_next_fetch_work_item.swap(false, Ordering::SeqCst) {
            Err(ProviderError::retryable(
                "fetch_work_item",
                "simulated transient infrastructure failure",
            ))
        } else {
            self.inner
                .fetch_work_item(lock_timeout, poll_timeout, session, tag_filter)
                .await
        }
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
        if self.fail_next_ack_orchestration_item.swap(false, Ordering::SeqCst) {
            // Check if this is a failure event commit and we should allow it
            if self.allow_failure_commits.load(Ordering::SeqCst) {
                let is_failure_commit = history_delta
                    .iter()
                    .any(|e| matches!(&e.kind, EventKind::OrchestrationFailed { .. }));
                if is_failure_commit {
                    return self
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
                }
            }
            Err(ProviderError::permanent(
                "ack_orchestration_item",
                "simulated infrastructure failure",
            ))
        } else {
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

    async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) -> Result<(), ProviderError> {
        if self.fail_next_ack_work_item.swap(false, Ordering::SeqCst) {
            // If ack_then_fail is set, do the actual ack first
            if self.ack_then_fail.load(Ordering::SeqCst) {
                self.inner.ack_work_item(token, completion).await?;
            }
            Err(ProviderError::permanent(
                "ack_work_item",
                "simulated infrastructure failure",
            ))
        } else {
            self.inner.ack_work_item(token, completion).await
        }
    }

    async fn renew_work_item_lock(&self, token: &str, extension: Duration) -> Result<(), ProviderError> {
        self.inner.renew_work_item_lock(token, extension).await
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

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        self.inner.enqueue_for_worker(item).await
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        self.inner.read(instance).await
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

    async fn get_kv_value(&self, instance: &str, key: &str) -> Result<Option<String>, ProviderError> {
        self.inner.get_kv_value(instance, key).await
    }

    async fn get_kv_all_values(
        &self,
        instance: &str,
    ) -> Result<std::collections::HashMap<String, String>, ProviderError> {
        self.inner.get_kv_all_values(instance).await
    }
}

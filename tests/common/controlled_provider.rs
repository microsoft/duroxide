// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use duroxide::Event;
use duroxide::providers::{
    DeleteInstanceResult, DispatcherCapabilityFilter, ExecutionInfo, ExecutionMetadata, InstanceFilter, InstanceInfo,
    OrchestrationItem, Provider, ProviderAdmin, ProviderError, PruneOptions, PruneResult, QueueDepths,
    ScheduledActivityIdentifier, SessionFetchConfig, SystemMetrics, TagFilter, WorkItem,
};
use duroxide::runtime::test_hooks::{LifecycleHooks, LifecyclePoint, ProviderOperation};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub enum PollBehavior {
    Honor,
    Ignore,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct FetchRequest {
    pub queue: usize,
    pub lock_timeout: Duration,
    pub poll_timeout: Duration,
}

#[derive(Debug, serde::Serialize)]
pub struct PollingSample {
    pub behavior: PollBehavior,
    pub window: Duration,
    pub inner_probe_interval: Duration,
    pub outer_fetches: [usize; 2],
    pub inner_probes: [usize; 2],
    pub requests: Vec<FetchRequest>,
}

struct Measurement {
    start: Instant,
    window: Duration,
    requests: Vec<FetchRequest>,
    inner_probes: [usize; 2],
}

struct ActiveFetch<'a>(&'a AtomicUsize);

impl Drop for ActiveFetch<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A real-provider wrapper with controlled admission, return/commit gates and separate
/// outer dispatcher requests versus inner provider probes. It never cancels inner I/O.
pub struct ControlledProvider {
    inner: Arc<dyn Provider>,
    behavior: PollBehavior,
    hooks: LifecycleHooks,
    ready: [CancellationToken; 2],
    start: CancellationToken,
    stop: CancellationToken,
    active: AtomicUsize,
    operations: AtomicUsize,
    errors: Mutex<HashMap<ProviderOperation, String>>,
    metadata_panic: Mutex<Option<String>>,
    measurement: Mutex<Option<Measurement>>,
}

impl ControlledProvider {
    pub const PROBE_INTERVAL: Duration = Duration::from_millis(50);

    pub fn new(inner: Arc<dyn Provider>, behavior: PollBehavior, hooks: LifecycleHooks) -> Self {
        Self {
            inner,
            behavior,
            hooks,
            ready: std::array::from_fn(|_| CancellationToken::new()),
            start: CancellationToken::new(),
            stop: CancellationToken::new(),
            active: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            errors: Mutex::new(HashMap::new()),
            metadata_panic: Mutex::new(None),
            measurement: Mutex::new(None),
        }
    }

    pub async fn wait_for_dispatchers(&self) {
        for ready in &self.ready {
            ready.cancelled().await;
        }
    }

    pub fn begin_window(&self, window: Duration) -> Instant {
        let mut measurement = self.measurement.lock().unwrap();
        assert!(measurement.is_none(), "a measurement cannot reset an active window");
        let start = Instant::now();
        *measurement = Some(Measurement {
            start,
            window,
            requests: Vec::new(),
            inner_probes: [0; 2],
        });
        drop(measurement);
        self.start.cancel();
        start
    }

    pub fn sample(&self) -> PollingSample {
        let measurement = self.measurement.lock().unwrap();
        let measurement = measurement.as_ref().expect("measurement window started");
        assert!(
            measurement.start.elapsed() >= measurement.window,
            "window has not ended"
        );
        PollingSample {
            behavior: self.behavior,
            window: measurement.window,
            inner_probe_interval: Self::PROBE_INTERVAL,
            outer_fetches: std::array::from_fn(|queue| {
                measurement
                    .requests
                    .iter()
                    .filter(|request| request.queue == queue)
                    .count()
            }),
            inner_probes: measurement.inner_probes,
            requests: measurement.requests.clone(),
        }
    }

    pub fn stop_polling(&self) {
        self.stop.cancel();
        self.start.cancel();
    }

    pub fn active_fetches(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub fn active_operations(&self) -> usize {
        self.operations.load(Ordering::SeqCst)
    }

    pub fn fail_before(&self, operation: ProviderOperation, message: &str) {
        assert!(
            self.errors
                .lock()
                .unwrap()
                .insert(operation, message.to_string())
                .is_none()
        );
    }

    pub fn panic_on_name(&self, message: &str) {
        assert!(
            self.metadata_panic
                .lock()
                .unwrap()
                .replace(message.to_string())
                .is_none()
        );
    }

    fn admin(&self) -> &dyn ProviderAdmin {
        self.inner
            .as_management_capability()
            .expect("fixture requires an admin provider")
    }

    async fn operation<T: Send>(
        &self,
        operation: ProviderOperation,
        committed: bool,
        work: impl Future<Output = Result<T, ProviderError>> + Send,
    ) -> Result<T, ProviderError> {
        self.operations.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveFetch(&self.operations);
        self.hooks.checkpoint(LifecyclePoint::ProviderEnter(operation)).await;
        let error = self.errors.lock().unwrap().remove(&operation);
        let result = match error {
            Some(message) => Err(ProviderError::permanent("controlled-provider", message)),
            None => work.await,
        };
        self.returned(operation, result, committed).await
    }

    fn record_request(&self, request: FetchRequest) {
        let mut measurement = self.measurement.lock().unwrap();
        if let Some(measurement) = measurement.as_mut()
            && measurement.start.elapsed() < measurement.window
        {
            measurement.requests.push(request);
        }
    }

    fn record_probe(&self, queue: usize) {
        let mut measurement = self.measurement.lock().unwrap();
        if let Some(measurement) = measurement.as_mut()
            && measurement.start.elapsed() < measurement.window
        {
            measurement.inner_probes[queue] += 1;
        }
    }

    async fn poll<T, F, Fut>(
        &self,
        queue: usize,
        lock_timeout: Duration,
        poll_timeout: Duration,
        fetch: F,
    ) -> Result<Option<T>, ProviderError>
    where
        T: Send,
        F: Fn() -> Fut + Send,
        Fut: Future<Output = Result<Option<T>, ProviderError>> + Send,
    {
        self.ready[queue].cancel();
        self.start.cancelled().await;
        self.record_request(FetchRequest {
            queue,
            lock_timeout,
            poll_timeout,
        });
        let deadline = Instant::now()
            .checked_add(poll_timeout)
            .expect("fixture poll duration representable");
        loop {
            self.record_probe(queue);
            let result = fetch().await?;
            if result.is_some()
                || matches!(self.behavior, PollBehavior::Ignore)
                || Instant::now() >= deadline
                || self.stop.is_cancelled()
            {
                return Ok(result);
            }
            tokio::select! {
                () = self.stop.cancelled() => {},
                () = tokio::time::sleep_until((Instant::now() + Self::PROBE_INTERVAL).min(deadline).into()) => {},
            }
        }
    }

    async fn returned<T>(
        &self,
        operation: ProviderOperation,
        result: Result<T, ProviderError>,
        committed: bool,
    ) -> Result<T, ProviderError> {
        if committed && result.is_ok() {
            self.hooks.checkpoint(LifecyclePoint::ProviderCommit(operation)).await;
        }
        self.hooks.checkpoint(LifecyclePoint::ProviderReturn(operation)).await;
        result
    }
}

#[async_trait::async_trait]
impl Provider for ControlledProvider {
    fn name(&self) -> &str {
        let message = self.metadata_panic.lock().unwrap().take();
        if let Some(message) = message {
            panic!("{message}");
        }
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
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveFetch(&self.active);
        self.operation(
            ProviderOperation::FetchOrchestration,
            true,
            self.poll(0, lock_timeout, poll_timeout, || {
                self.inner
                    .fetch_orchestration_item(lock_timeout, Duration::ZERO, filter)
            }),
        )
        .await
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveFetch(&self.active);
        self.operation(
            ProviderOperation::FetchActivity,
            true,
            self.poll(1, lock_timeout, poll_timeout, || {
                self.inner
                    .fetch_work_item(lock_timeout, Duration::ZERO, session, tag_filter)
            }),
        )
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
        self.operation(
            ProviderOperation::AcknowledgeOrchestration,
            true,
            self.inner.ack_orchestration_item(
                lock_token,
                execution_id,
                history_delta,
                worker_items,
                orchestrator_items,
                metadata,
                cancelled_activities,
            ),
        )
        .await
    }

    async fn abandon_orchestration_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        self.inner
            .abandon_orchestration_item(token, delay, ignore_attempt)
            .await
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        self.operation(ProviderOperation::Read, false, self.inner.read(instance))
            .await
    }

    async fn read_with_execution(&self, instance: &str, execution_id: u64) -> Result<Vec<Event>, ProviderError> {
        self.operation(
            ProviderOperation::Read,
            false,
            self.inner.read_with_execution(instance, execution_id),
        )
        .await
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
        self.operation(
            ProviderOperation::EnqueueActivity,
            true,
            self.inner.enqueue_for_worker(item),
        )
        .await
    }

    async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) -> Result<(), ProviderError> {
        self.operation(
            ProviderOperation::AcknowledgeActivity,
            true,
            self.inner.ack_work_item(token, completion),
        )
        .await
    }

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        self.inner.abandon_work_item(token, delay, ignore_attempt).await
    }

    async fn renew_work_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
        self.operation(
            ProviderOperation::RenewActivity,
            false,
            self.inner.renew_work_item_lock(token, extend_for),
        )
        .await
    }

    async fn renew_orchestration_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
        self.operation(
            ProviderOperation::RenewOrchestration,
            false,
            self.inner.renew_orchestration_item_lock(token, extend_for),
        )
        .await
    }

    async fn renew_session_lock(
        &self,
        owner_ids: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        self.operation(
            ProviderOperation::SessionMaintenance,
            false,
            self.inner.renew_session_lock(owner_ids, extend_for, idle_timeout),
        )
        .await
    }

    async fn cleanup_orphaned_sessions(&self, idle_timeout: Duration) -> Result<usize, ProviderError> {
        self.operation(
            ProviderOperation::SessionMaintenance,
            false,
            self.inner.cleanup_orphaned_sessions(idle_timeout),
        )
        .await
    }

    async fn enqueue_for_orchestrator(&self, item: WorkItem, delay: Option<Duration>) -> Result<(), ProviderError> {
        self.operation(
            ProviderOperation::EnqueueOrchestration,
            true,
            self.inner.enqueue_for_orchestrator(item, delay),
        )
        .await
    }

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        self.inner
            .as_management_capability()
            .map(|_| self as &dyn ProviderAdmin)
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

#[async_trait::async_trait]
impl ProviderAdmin for ControlledProvider {
    async fn list_instances(&self) -> Result<Vec<String>, ProviderError> {
        self.admin().list_instances().await
    }
    async fn list_instances_by_status(&self, status: &str) -> Result<Vec<String>, ProviderError> {
        self.admin().list_instances_by_status(status).await
    }
    async fn list_executions(&self, instance: &str) -> Result<Vec<u64>, ProviderError> {
        self.admin().list_executions(instance).await
    }
    async fn read_history_with_execution_id(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        self.admin()
            .read_history_with_execution_id(instance, execution_id)
            .await
    }
    async fn read_history(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        self.admin().read_history(instance).await
    }
    async fn latest_execution_id(&self, instance: &str) -> Result<u64, ProviderError> {
        self.admin().latest_execution_id(instance).await
    }
    async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, ProviderError> {
        self.admin().get_instance_info(instance).await
    }
    async fn get_execution_info(&self, instance: &str, execution_id: u64) -> Result<ExecutionInfo, ProviderError> {
        self.admin().get_execution_info(instance, execution_id).await
    }
    async fn get_system_metrics(&self) -> Result<SystemMetrics, ProviderError> {
        self.operation(
            ProviderOperation::SystemMetrics,
            false,
            self.admin().get_system_metrics(),
        )
        .await
    }
    async fn get_queue_depths(&self) -> Result<QueueDepths, ProviderError> {
        self.operation(ProviderOperation::QueueDepths, false, self.admin().get_queue_depths())
            .await
    }
    async fn list_children(&self, instance_id: &str) -> Result<Vec<String>, ProviderError> {
        self.admin().list_children(instance_id).await
    }
    async fn get_parent_id(&self, instance_id: &str) -> Result<Option<String>, ProviderError> {
        self.admin().get_parent_id(instance_id).await
    }
    async fn delete_instances_atomic(
        &self,
        ids: &[String],
        force: bool,
    ) -> Result<DeleteInstanceResult, ProviderError> {
        self.admin().delete_instances_atomic(ids, force).await
    }
    async fn delete_instance_bulk(&self, filter: InstanceFilter) -> Result<DeleteInstanceResult, ProviderError> {
        self.admin().delete_instance_bulk(filter).await
    }
    async fn prune_executions(&self, instance_id: &str, options: PruneOptions) -> Result<PruneResult, ProviderError> {
        self.admin().prune_executions(instance_id, options).await
    }
    async fn prune_executions_bulk(
        &self,
        filter: InstanceFilter,
        options: PruneOptions,
    ) -> Result<PruneResult, ProviderError> {
        self.admin().prune_executions_bulk(filter, options).await
    }
}

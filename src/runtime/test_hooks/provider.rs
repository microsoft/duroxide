// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Real-provider forwarding with scoped I/O gates, compiled only for lifecycle instrumentation.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use super::{LifecycleHooks, LifecyclePoint, ProviderOperation};
use crate::providers::{
    DispatcherCapabilityFilter, ExecutionMetadata, OrchestrationItem, Provider, ProviderAdmin, ProviderError,
    ScheduledActivityIdentifier, SessionFetchConfig, TagFilter, WorkItem,
};
use crate::{Event, SystemStats};

pub struct TestProvider {
    inner: Arc<dyn Provider>,
    hooks: LifecycleHooks,
}

impl TestProvider {
    pub fn new(inner: Arc<dyn Provider>, hooks: LifecycleHooks) -> Self {
        Self { inner, hooks }
    }

    async fn operation<T: Send>(
        &self,
        operation: ProviderOperation,
        committed: bool,
        work: impl Future<Output = Result<T, ProviderError>> + Send,
    ) -> Result<T, ProviderError> {
        self.hooks.checkpoint(LifecyclePoint::ProviderEnter(operation)).await;
        let result = work.await;
        if committed && result.is_ok() {
            self.hooks.checkpoint(LifecyclePoint::ProviderCommit(operation)).await;
        }
        self.hooks.checkpoint(LifecyclePoint::ProviderReturn(operation)).await;
        result
    }
}

#[async_trait::async_trait]
impl Provider for TestProvider {
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
        self.operation(
            ProviderOperation::FetchOrchestration,
            false,
            self.inner.fetch_orchestration_item(lock_timeout, poll_timeout, filter),
        )
        .await
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tags: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        self.operation(
            ProviderOperation::FetchActivity,
            false,
            self.inner.fetch_work_item(lock_timeout, poll_timeout, session, tags),
        )
        .await
    }

    async fn ack_orchestration_item(
        &self,
        token: &str,
        execution_id: u64,
        history: Vec<Event>,
        workers: Vec<WorkItem>,
        orchestrators: Vec<WorkItem>,
        metadata: ExecutionMetadata,
        cancellations: Vec<ScheduledActivityIdentifier>,
    ) -> Result<(), ProviderError> {
        self.operation(
            ProviderOperation::AcknowledgeOrchestration,
            true,
            self.inner.ack_orchestration_item(
                token,
                execution_id,
                history,
                workers,
                orchestrators,
                metadata,
                cancellations,
            ),
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

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        self.inner.abandon_work_item(token, delay, ignore_attempt).await
    }

    async fn renew_orchestration_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
        self.operation(
            ProviderOperation::RenewOrchestration,
            false,
            self.inner.renew_orchestration_item_lock(token, extend_for),
        )
        .await
    }

    async fn renew_work_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
        self.operation(
            ProviderOperation::RenewActivity,
            false,
            self.inner.renew_work_item_lock(token, extend_for),
        )
        .await
    }

    async fn renew_session_lock(
        &self,
        owners: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        self.operation(
            ProviderOperation::SessionMaintenance,
            false,
            self.inner.renew_session_lock(owners, extend_for, idle_timeout),
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

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        self.operation(
            ProviderOperation::EnqueueActivity,
            true,
            self.inner.enqueue_for_worker(item),
        )
        .await
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        self.operation(ProviderOperation::Read, false, self.inner.read(instance))
            .await
    }

    async fn read_with_execution(&self, instance: &str, execution: u64) -> Result<Vec<Event>, ProviderError> {
        self.operation(
            ProviderOperation::Read,
            false,
            self.inner.read_with_execution(instance, execution),
        )
        .await
    }

    async fn append_with_execution(
        &self,
        instance: &str,
        execution: u64,
        events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        self.inner.append_with_execution(instance, execution, events).await
    }

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        self.inner.as_management_capability()
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        self.inner.get_custom_status(instance, version).await
    }

    async fn get_kv_value(&self, instance: &str, key: &str) -> Result<Option<String>, ProviderError> {
        self.inner.get_kv_value(instance, key).await
    }

    async fn get_kv_all_values(&self, instance: &str) -> Result<HashMap<String, String>, ProviderError> {
        self.inner.get_kv_all_values(instance).await
    }

    async fn get_instance_stats(&self, instance: &str) -> Result<Option<SystemStats>, ProviderError> {
        self.inner.get_instance_stats(instance).await
    }
}

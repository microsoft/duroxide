// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(dead_code)]

#[path = "controlled_provider.rs"]
pub mod controlled_provider;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use controlled_provider::{ControlledProvider, PollBehavior};
use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::test_hooks::{LifecycleGate, LifecycleHooks, LifecyclePoint};
use duroxide::runtime::{ObservabilityConfig, Runtime, RuntimeOptions};
use duroxide::{Client, OrchestrationRegistry};

pub const LIMIT: Duration = Duration::from_secs(5);
pub const SECRET: &str = "postgres://sentinel-user:sentinel-password@sentinel-host/db?token=sentinel-token";

pub fn options() -> RuntimeOptions {
    RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        dispatcher_long_poll_timeout: Duration::from_millis(50),
        dispatcher_min_poll_interval: Duration::from_millis(5),
        orchestrator_lock_timeout: Duration::from_secs(2),
        worker_lock_timeout: Duration::from_secs(2),
        observability: ObservabilityConfig {
            log_level: "off".to_string(),
            ..Default::default()
        },
        ..Default::default()
    }
}

pub struct Fixture {
    pub inner: Arc<dyn Provider>,
    pub provider: Arc<ControlledProvider>,
    pub hooks: LifecycleHooks,
}

impl Fixture {
    pub async fn new() -> Self {
        let inner: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let hooks = LifecycleHooks::default();
        let provider = Arc::new(ControlledProvider::new(
            Arc::clone(&inner),
            PollBehavior::Ignore,
            hooks.clone(),
        ));
        provider.begin_window(Duration::from_secs(60));
        Self { inner, provider, hooks }
    }

    pub fn prepare(
        &self,
        activities: ActivityRegistry,
        orchestrations: OrchestrationRegistry,
        options: RuntimeOptions,
    ) -> Arc<Runtime> {
        let runtime = Runtime::prepare(
            Arc::clone(&self.provider) as Arc<dyn Provider>,
            activities,
            orchestrations,
            options,
        )
        .unwrap();
        runtime.set_lifecycle_hooks(self.hooks.clone()).unwrap();
        runtime
    }

    pub fn empty(&self) -> Arc<Runtime> {
        self.prepare(
            ActivityRegistry::builder().build(),
            OrchestrationRegistry::builder().build(),
            options(),
        )
    }

    pub fn client(&self) -> Client {
        Client::new(Arc::clone(&self.inner))
    }

    pub fn hold(&self, point: LifecyclePoint) -> LifecycleGate {
        self.hooks.hold(point).unwrap()
    }

    pub fn assert_retired(&self) {
        for counts in self.hooks.counts() {
            assert_eq!(counts.active, 0, "{counts:?}");
            assert_eq!(counts.started, counts.completed, "{counts:?}");
        }
        let cleanup = self.hooks.cleanup_counts();
        assert_eq!(cleanup.active, 0, "{cleanup:?}");
        assert_eq!(cleanup.started, cleanup.completed, "{cleanup:?}");
        assert_eq!(self.provider.active_operations(), 0);
        assert_eq!(self.provider.active_fetches(), 0);
    }
}

pub async fn bounded<T>(work: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, work)
        .await
        .expect("controlled fixture made progress")
}

pub async fn entered(gate: &LifecycleGate) {
    bounded(gate.entered()).await;
}

pub async fn until(mut predicate: impl FnMut() -> bool) {
    bounded(async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

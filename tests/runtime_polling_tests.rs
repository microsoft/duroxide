// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

#[path = "common/controlled_provider.rs"]
mod controlled_provider;

use controlled_provider::{ControlledProvider, PollBehavior};
use duroxide::OrchestrationRegistry;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::{Provider, WorkItem};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::test_hooks::{LifecycleHooks, LifecyclePoint, ProviderOperation};
use duroxide::runtime::{Runtime, RuntimeOptions, ShutdownOutcome};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn records_controlled_ten_second_polling_windows() {
    let window = Duration::from_secs(10);
    let poll_timeout = Duration::from_secs(2);
    let min_interval = Duration::from_millis(500);
    for behavior in [PollBehavior::Honor, PollBehavior::Ignore] {
        let inner: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let provider = Arc::new(ControlledProvider::new(inner, behavior, LifecycleHooks::default()));
        let options = RuntimeOptions {
            orchestration_concurrency: 1,
            worker_concurrency: 1,
            dispatcher_long_poll_timeout: poll_timeout,
            dispatcher_min_poll_interval: min_interval,
            ..RuntimeOptions::default()
        };
        let expected_locks = [options.orchestrator_lock_timeout, options.worker_lock_timeout];
        let runtime = Runtime::start_with_options(
            Arc::clone(&provider) as Arc<dyn Provider>,
            ActivityRegistry::builder().build(),
            OrchestrationRegistry::builder().build(),
            options,
        )
        .await;
        let lifetime = Arc::downgrade(&runtime);
        tokio::time::timeout(Duration::from_secs(5), provider.wait_for_dispatchers())
            .await
            .unwrap();
        let start = provider.begin_window(window);
        tokio::time::sleep_until((start + window).into()).await;
        let sample = provider.sample();

        provider.stop_polling();
        assert_eq!(
            runtime.shutdown_with_grace(Duration::from_secs(1)).await,
            Ok(ShutdownOutcome::Drained)
        );
        // Retain the baseline's independent lifetime and provider-frame checks.
        tokio::time::timeout(Duration::from_secs(6), async {
            while lifetime.strong_count() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("runtime-owned fixture tasks retired");
        assert_eq!(provider.active_fetches(), 0);

        let expected_outer = match behavior {
            PollBehavior::Honor => 5_usize,
            PollBehavior::Ignore => 20_usize,
        };
        for queue in 0..2 {
            assert!(sample.outer_fetches[queue].abs_diff(expected_outer) <= 1, "{sample:?}");
            assert!(sample.inner_probes[queue] >= sample.outer_fetches[queue]);
            if matches!(behavior, PollBehavior::Ignore) {
                assert_eq!(sample.inner_probes[queue], sample.outer_fetches[queue]);
            } else {
                assert!(sample.inner_probes[queue] > sample.outer_fetches[queue]);
            }
        }
        for request in &sample.requests {
            assert_eq!(request.poll_timeout, poll_timeout);
            assert_eq!(request.lock_timeout, expected_locks[request.queue]);
        }
        println!("DUROXIDE_POLLING_SAMPLE {}", serde_json::to_string(&sample).unwrap());
    }
}

#[tokio::test]
async fn provider_commit_and_return_gates_do_not_cancel_committed_io() {
    let inner: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let hooks = LifecycleHooks::default();
    let committed = hooks
        .hold(LifecyclePoint::ProviderCommit(ProviderOperation::EnqueueOrchestration))
        .unwrap();
    let returned = hooks
        .hold(LifecyclePoint::ProviderReturn(ProviderOperation::EnqueueOrchestration))
        .unwrap();
    let provider = Arc::new(ControlledProvider::new(Arc::clone(&inner), PollBehavior::Ignore, hooks));
    let enqueue = Arc::clone(&provider);
    let request = tokio::spawn(async move {
        enqueue
            .enqueue_for_orchestrator(
                WorkItem::StartOrchestration {
                    instance: "commit-boundary".to_string(),
                    orchestration: "test".to_string(),
                    input: String::new(),
                    version: None,
                    parent_instance: None,
                    parent_id: None,
                    parent_execution_id: None,
                    execution_id: 1,
                },
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), committed.entered())
        .await
        .unwrap();
    assert!(!request.is_finished());
    let (_, token, _) = inner
        .fetch_orchestration_item(Duration::from_secs(10), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("commit is visible before wrapper returns");
    inner.abandon_orchestration_item(&token, None, false).await.unwrap();
    committed.release();
    tokio::time::timeout(Duration::from_secs(5), returned.entered())
        .await
        .unwrap();
    assert!(!request.is_finished());
    returned.release();
    request.await.unwrap().unwrap();
}

#[tokio::test]
async fn provider_return_gate_counts_io_until_actual_return() {
    let inner: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let hooks = LifecycleHooks::default();
    let returned = hooks
        .hold(LifecyclePoint::ProviderReturn(ProviderOperation::FetchOrchestration))
        .unwrap();
    let provider = Arc::new(ControlledProvider::new(inner, PollBehavior::Ignore, hooks));
    provider.begin_window(Duration::from_secs(1));
    let fetch = Arc::clone(&provider);
    let request = tokio::spawn(async move {
        fetch
            .fetch_orchestration_item(Duration::from_secs(10), Duration::ZERO, None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), returned.entered())
        .await
        .unwrap();
    assert_eq!(provider.active_fetches(), 1);
    provider.stop_polling();
    assert!(!request.is_finished());
    returned.release();
    request.await.unwrap().unwrap();
    assert_eq!(provider.active_fetches(), 0);
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use duroxide::providers::{Provider, sqlite::SqliteProvider};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{ObservabilityConfig, Runtime, RuntimeOptions, RuntimeShutdownError, ShutdownOutcome};
use duroxide::{
    ActivityContext, Client, Event, EventKind, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus,
};
use tokio::sync::Notify;

const LIMIT: Duration = Duration::from_secs(5);

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, future)
        .await
        .expect("production fixture made progress")
}

fn options() -> RuntimeOptions {
    RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        dispatcher_min_poll_interval: Duration::from_millis(5),
        dispatcher_long_poll_timeout: Duration::from_millis(50),
        observability: ObservabilityConfig {
            log_level: "off".to_string(),
            ..Default::default()
        },
        ..Default::default()
    }
}

async fn provider() -> Arc<dyn Provider> {
    Arc::new(SqliteProvider::new_in_memory().await.unwrap())
}

fn activity_flow() -> OrchestrationRegistry {
    OrchestrationRegistry::builder()
        .register("flow", |ctx: OrchestrationContext, _: String| async move {
            ctx.schedule_activity("work", "").await
        })
        .build()
}

async fn history_where(
    provider: &Arc<dyn Provider>,
    instance: &str,
    predicate: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    bounded(async {
        loop {
            let history = provider.read(instance).await.unwrap();
            if predicate(&history) {
                return history;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
}

#[tokio::test]
async fn activity_panic_history_preserves_the_actual_tokio_task_identity() {
    let provider = provider().await;
    let client = Client::new(Arc::clone(&provider));
    let task_id = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&task_id);
    let activities = ActivityRegistry::builder()
        .register("work", move |_: ActivityContext, _: String| {
            let captured = Arc::clone(&captured);
            async move {
                *captured.lock().unwrap() = Some(tokio::task::id());
                panic!("production activity panic");
                #[allow(unreachable_code)]
                Ok(String::new())
            }
        })
        .build();
    let runtime = Runtime::start_with_options(Arc::clone(&provider), activities, activity_flow(), options()).await;
    client.start_orchestration("panic", "flow", "").await.unwrap();
    assert!(matches!(
        bounded(client.wait_for_orchestration("panic", LIMIT)).await.unwrap(),
        OrchestrationStatus::Failed { .. }
    ));
    let history = provider.read("panic").await.unwrap();
    let messages: Vec<_> = history
        .iter()
        .filter_map(|event| match &event.kind {
            EventKind::ActivityFailed {
                details: duroxide::ErrorDetails::Application { message, .. },
            } => Some(message.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        messages,
        vec![format!(
            "task {} panicked with message {:?}",
            task_id.lock().unwrap().unwrap(),
            "production activity panic"
        )]
    );
    assert_eq!(
        bounded(runtime.shutdown_with_grace(Duration::from_secs(1))).await,
        Ok(ShutdownOutcome::Drained)
    );
}

#[tokio::test]
async fn legacy_none_drains_admitted_activity_within_default_grace() {
    let provider = provider().await;
    let client = Client::new(Arc::clone(&provider));
    let active = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler_active = Arc::clone(&active);
    let handler_release = Arc::clone(&release);
    let activities = ActivityRegistry::builder()
        .register("work", move |_: ActivityContext, _: String| {
            let active = Arc::clone(&handler_active);
            let release = Arc::clone(&handler_release);
            async move {
                active.notify_one();
                release.notified().await;
                Ok("completed during grace".to_string())
            }
        })
        .build();
    let runtime = Runtime::start_with_options(Arc::clone(&provider), activities, activity_flow(), options()).await;
    client.start_orchestration("legacy", "flow", "").await.unwrap();
    bounded(active.notified()).await;
    let shutdown = tokio::spawn(Arc::clone(&runtime).shutdown(None));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !shutdown.is_finished(),
        "None must not force an admitted activity immediately"
    );
    release.notify_one();
    bounded(shutdown).await.unwrap();
    assert_eq!(
        runtime.shutdown_with_grace(Duration::ZERO).await,
        Ok(ShutdownOutcome::Drained)
    );
    let peer = Runtime::start_with_options(
        provider,
        ActivityRegistry::builder().build(),
        activity_flow(),
        options(),
    )
    .await;
    assert!(matches!(
        bounded(client.wait_for_orchestration("legacy", LIMIT)).await.unwrap(),
        OrchestrationStatus::Completed { output, .. } if output == "completed during grace"
    ));
    peer.shutdown(Some(0)).await;
}

#[tokio::test]
async fn finalization_actions_and_can_are_committed_before_retained_cleanup() {
    for continue_as_new in [false, true] {
        let provider = provider().await;
        let client = Client::new(Arc::clone(&provider));
        let owner = Arc::new(Mutex::new(None::<Weak<Runtime>>));
        let finalizations = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let cleanup_entered = Arc::new(Notify::new());
        let handler_owner = Arc::clone(&owner);
        let handler_finalizations = Arc::clone(&finalizations);
        let handler_release = Arc::clone(&release);
        let handler_entered = Arc::clone(&cleanup_entered);
        let registry = OrchestrationRegistry::builder()
            .register("flow", move |ctx: OrchestrationContext, _: String| {
                let owner = Arc::clone(&handler_owner);
                let finalizations = Arc::clone(&handler_finalizations);
                let release = Arc::clone(&handler_release);
                let entered = Arc::clone(&handler_entered);
                let final_ctx = ctx.clone();
                let cleanup_ctx = ctx.clone();
                async move {
                    ctx.register_invocation_cleanup_with_finalization(
                        async move {
                            entered.notify_one();
                            release.notified().await;
                            cleanup_ctx.set_kv_value("late", "must-not-persist");
                            Ok(())
                        },
                        move || {
                            finalizations.fetch_add(1, Ordering::SeqCst);
                            final_ctx.set_kv_value("admitted", "value");
                            if continue_as_new {
                                drop(final_ctx.continue_as_new("next"));
                            } else {
                                final_ctx.schedule_orchestration("child", "detached", "");
                            }
                            owner
                                .lock()
                                .unwrap()
                                .as_ref()
                                .unwrap()
                                .upgrade()
                                .unwrap()
                                .request_shutdown();
                        },
                    )
                    .unwrap();
                    ctx.schedule_wait("pause").await;
                    Ok(String::new())
                }
            })
            .build();
        let mut config = options();
        config.worker_concurrency = 0;
        let runtime = Runtime::prepare(
            Arc::clone(&provider),
            ActivityRegistry::builder().build(),
            registry,
            config,
        )
        .unwrap();
        *owner.lock().unwrap() = Some(Arc::downgrade(&runtime));
        Arc::clone(&runtime).start_execution().await.unwrap();
        client.start_orchestration("admitted", "flow", "").await.unwrap();
        bounded(cleanup_entered.notified()).await;
        let before = history_where(&provider, "admitted", |history| {
            history.iter().any(|event| {
                matches!(&event.kind,
                EventKind::KeyValueSet { key, .. } if key == "admitted")
            })
        })
        .await;
        assert_eq!(finalizations.load(Ordering::SeqCst), 1);
        assert_eq!(
            before.iter().any(|event| matches!(&event.kind,
                EventKind::OrchestrationContinuedAsNew { input } if input == "next")),
            continue_as_new
        );
        assert_eq!(
            provider.get_kv_value("admitted", "admitted").await.unwrap(),
            Some("value".to_string())
        );
        let mut shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::ZERO));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
                .await
                .is_err()
        );
        release.notify_one();
        assert_eq!(bounded(shutdown).await.unwrap(), Ok(ShutdownOutcome::Forced));
        assert_eq!(provider.read_with_execution("admitted", 1).await.unwrap(), before);
        assert_eq!(provider.get_kv_value("admitted", "late").await.unwrap(), None);
    }
}

#[tokio::test]
async fn finalization_failure_rejects_the_new_batch_without_releasing_foreign_work() {
    let provider = provider().await;
    let client = Client::new(Arc::clone(&provider));
    let release = Arc::new(Notify::new());
    let cleanup_entered = Arc::new(Notify::new());
    let handler_release = Arc::clone(&release);
    let handler_entered = Arc::clone(&cleanup_entered);
    let registry = OrchestrationRegistry::builder()
        .register("flow", move |ctx: OrchestrationContext, _: String| {
            let release = Arc::clone(&handler_release);
            let entered = Arc::clone(&handler_entered);
            let final_ctx = ctx.clone();
            async move {
                ctx.register_invocation_cleanup_with_finalization(
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    },
                    move || {
                        final_ctx.set_kv_value("failed", "must-not-persist");
                        final_ctx.schedule_orchestration("child", "failed-detached", "");
                        panic!("production finalization sentinel");
                    },
                )
                .unwrap();
                ctx.set_kv_value("before-finalization", "must-not-persist");
                ctx.schedule_activity("new-work", "").await
            }
        })
        .build();
    let runtime = Runtime::start_with_options(
        Arc::clone(&provider),
        ActivityRegistry::builder().build(),
        registry,
        options(),
    )
    .await;
    client.start_orchestration("failed", "flow", "").await.unwrap();
    bounded(cleanup_entered.notified()).await;
    let mut shutdown = tokio::spawn(Arc::clone(&runtime).shutdown_with_grace(Duration::ZERO));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err()
    );
    release.notify_one();
    assert_eq!(
        bounded(shutdown).await.unwrap(),
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    assert!(provider.read("failed").await.unwrap().is_empty());
    assert_eq!(provider.get_kv_value("failed", "failed").await.unwrap(), None);
    assert_eq!(
        provider.get_kv_value("failed", "before-finalization").await.unwrap(),
        None
    );
    assert!(matches!(
        client.get_orchestration_status("failed-detached").await.unwrap(),
        OrchestrationStatus::NotFound
    ));
    assert_eq!(
        provider
            .as_management_capability()
            .unwrap()
            .get_queue_depths()
            .await
            .unwrap()
            .worker_queue,
        0
    );
    assert_eq!(
        runtime.start_execution().await,
        Err(duroxide::runtime::RuntimeStartError::ShutdownRequested)
    );
}

#[tokio::test]
async fn cleanup_error_preserves_an_already_acknowledged_completion() {
    let provider = provider().await;
    let client = Client::new(Arc::clone(&provider));
    let release = Arc::new(Notify::new());
    let handler_release = Arc::clone(&release);
    let registry = OrchestrationRegistry::builder()
        .register("flow", move |ctx: OrchestrationContext, _: String| {
            let release = Arc::clone(&handler_release);
            async move {
                ctx.register_invocation_cleanup(async move {
                    release.notified().await;
                    Err("production cleanup sentinel".to_string())
                })
                .unwrap();
                ctx.set_kv_value("committed", "value");
                Ok("committed completion".to_string())
            }
        })
        .build();
    let runtime = Runtime::start_with_options(
        Arc::clone(&provider),
        ActivityRegistry::builder().build(),
        registry,
        options(),
    )
    .await;
    client.start_orchestration("committed", "flow", "").await.unwrap();
    assert!(matches!(
        bounded(client.wait_for_orchestration("committed", LIMIT))
            .await
            .unwrap(),
        OrchestrationStatus::Completed { .. }
    ));
    let before = provider.read("committed").await.unwrap();
    release.notify_one();
    assert_eq!(
        bounded(runtime.shutdown_with_grace(Duration::from_secs(1))).await,
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    assert_eq!(provider.read("committed").await.unwrap(), before);
    assert_eq!(
        provider.get_kv_value("committed", "committed").await.unwrap(),
        Some("value".to_string())
    );
}

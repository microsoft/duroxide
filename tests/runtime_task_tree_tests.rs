// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::clone_on_ref_ptr)]

#[path = "common/runtime_lifecycle.rs"]
mod support;
#[path = "common/tracing_capture.rs"]
mod tracing_capture;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::test_hooks::{LifecyclePoint, LifecycleTask, ProviderOperation};
use duroxide::runtime::{Runtime, RuntimeShutdownError, RuntimeStartError, ShutdownOutcome};
use duroxide::{ActivityContext, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use futures_util::FutureExt;
use support::{Fixture, SECRET, bounded, entered, options, until};
use tokio_util::sync::CancellationToken;

fn flow() -> OrchestrationRegistry {
    OrchestrationRegistry::builder()
        .register("flow", |ctx: OrchestrationContext, _: String| async move {
            ctx.schedule_activity("work", "payload").await
        })
        .build()
}

fn pending_activity() -> ActivityRegistry {
    ActivityRegistry::builder()
        .register("work", |_: ActivityContext, _: String| async {
            std::future::pending::<Result<String, String>>().await
        })
        .build()
}

#[tokio::test]
async fn startup_failure_after_each_dispatcher_batch_retains_and_cleans_started_tasks() {
    for task in [
        LifecycleTask::GaugePoller,
        LifecycleTask::OrchestrationSlot,
        LifecycleTask::WorkerSlot,
    ] {
        let f = Fixture::new().await;
        let failed = LifecyclePoint::StartupAfterSpawn(task);
        let startup = f.hold(failed);
        f.hooks.fail_once_with_message(failed, SECRET).unwrap();
        let descendant = f.hold(LifecyclePoint::ParentWork(task));
        let runtime = f.empty();
        let starter = tokio::spawn(runtime.clone().start_execution());
        entered(&startup).await;
        entered(&descendant).await;
        startup.release();
        assert_eq!(bounded(starter).await.unwrap(), Err(RuntimeStartError::StartupFailed));
        assert_eq!(
            bounded(runtime.shutdown_with_grace(Duration::ZERO)).await,
            Err(RuntimeShutdownError::Failed { quiescent: true })
        );
        f.assert_retired();
        descendant.release();
    }
}

#[tokio::test]
async fn every_tracked_task_role_reports_faults_and_retires_all_remaining_work() {
    let (events, _capture) = tracing_capture::install_tracing_capture();
    for (task, expected_role) in [
        (LifecycleTask::GaugePoller, "gauge-poller"),
        (LifecycleTask::OrchestrationSlot, "orchestration-slot"),
        (LifecycleTask::OrchestrationRenewal, "orchestration-renewal"),
        (LifecycleTask::WorkerSlot, "worker-slot"),
        (LifecycleTask::SessionManager, "session-manager"),
        (LifecycleTask::ActivityManager, "activity-manager"),
        (LifecycleTask::ActivityInvocation, "activity-invocation"),
    ] {
        let f = Fixture::new().await;
        let fault = LifecyclePoint::ParentWork(task);
        f.hooks.fail_once_with_message(fault, SECRET).unwrap();
        let ack = if task == LifecycleTask::OrchestrationRenewal {
            Some(f.hold(LifecyclePoint::ProviderEnter(
                ProviderOperation::AcknowledgeOrchestration,
            )))
        } else {
            None
        };
        f.client().start_orchestration("task-fault", "flow", "").await.unwrap();
        let runtime = f.prepare(pending_activity(), flow(), options());
        let lifecycle_id = events
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|event| event.field("category").as_deref() == Some("runtime_prepared"))
            .unwrap()
            .field("lifecycle_id")
            .unwrap();
        let started = runtime.clone().start_execution().await;
        assert!(matches!(started, Ok(()) | Err(RuntimeStartError::StartupFailed)));
        until(|| f.hooks.hits(fault) == 1).await;
        assert_eq!(
            bounded(runtime.shutdown_with_grace(Duration::ZERO)).await,
            Err(RuntimeShutdownError::Failed { quiescent: true }),
            "{task:?}"
        );
        f.assert_retired();
        if let Some(ack) = ack {
            ack.release();
        }
        let captured = events.lock().unwrap();
        assert!(
            captured.iter().any(|event| event.level == tracing::Level::ERROR
                && event.target == "duroxide::runtime::lifecycle"
                && event.message.contains("failed")
                && event.field("category").as_deref() == Some("owned_execution_failed")
                && event.field("lifecycle_id").as_deref() == Some(lifecycle_id.as_str())
                && event.field("task").as_deref() == Some(expected_role)
                && event.field("kind").as_deref() == Some("PollPanicked")),
            "missing original {expected_role}/PollPanicked report for lifecycle {lifecycle_id}: {captured:?}"
        );
    }
    let captured = events.lock().unwrap();
    assert!(!format!("{captured:?}").contains("sentinel"));
}

#[tokio::test]
async fn legacy_none_allows_completion_during_its_one_second_grace_but_zero_forces() {
    for grace in [None, Some(0)] {
        let f = Fixture::new().await;
        let active = CancellationToken::new();
        let release = CancellationToken::new();
        let handler_active = active.clone();
        let handler_release = release.clone();
        let activities = ActivityRegistry::builder()
            .register("work", move |_: ActivityContext, _: String| {
                let active = handler_active.clone();
                let release = handler_release.clone();
                async move {
                    active.cancel();
                    release.cancelled().await;
                    Ok("completed within default grace".to_string())
                }
            })
            .build();
        let runtime = f.prepare(activities, flow(), options());
        runtime.clone().start_execution().await.unwrap();
        f.client()
            .start_orchestration("legacy-grace", "flow", "")
            .await
            .unwrap();
        bounded(active.cancelled()).await;
        let shutdown = tokio::spawn(runtime.clone().shutdown(grace));
        until(|| f.hooks.hits(LifecyclePoint::ShutdownCoordinator) == 1).await;
        if grace.is_none() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(!shutdown.is_finished(), "None must not force admitted work immediately");
            assert_eq!(f.hooks.hits(LifecyclePoint::ForceRequested), 0);
            release.cancel();
        } else {
            until(|| f.hooks.hits(LifecyclePoint::ForceRequested) == 1).await;
        }
        bounded(shutdown).await.unwrap();
        assert_eq!(
            runtime.shutdown_with_grace(Duration::ZERO).await,
            Ok(if grace.is_none() {
                ShutdownOutcome::Drained
            } else {
                ShutdownOutcome::Forced
            })
        );
        assert_eq!(
            f.hooks
                .hits(LifecyclePoint::ProviderCommit(ProviderOperation::AcknowledgeActivity)),
            usize::from(grace.is_none())
        );
        f.assert_retired();
        if grace.is_none() {
            let peer =
                Runtime::start_with_options(f.inner.clone(), ActivityRegistry::builder().build(), flow(), options())
                    .await;
            assert!(
                matches!(bounded(f.client().wait_for_orchestration("legacy-grace", Duration::from_secs(3))).await.unwrap(),
                OrchestrationStatus::Completed { output, .. } if output == "completed within default grace")
            );
            peer.shutdown(Some(0)).await;
        }
    }
}

#[tokio::test]
async fn legacy_none_forces_work_held_through_default_grace_expiry() {
    let f = Fixture::new().await;
    let active = f.hold(LifecyclePoint::ParentWork(LifecycleTask::ActivityInvocation));
    let runtime = f.prepare(pending_activity(), flow(), options());
    runtime.clone().start_execution().await.unwrap();
    f.client()
        .start_orchestration("legacy-expiry", "flow", "")
        .await
        .unwrap();
    entered(&active).await;
    let start = std::time::Instant::now();
    let shutdown = tokio::spawn(runtime.clone().shutdown(None));
    until(|| f.hooks.hits(LifecyclePoint::ForceRequested) == 1).await;
    assert!(start.elapsed() >= Duration::from_secs(1), "None's grace was shortened");
    bounded(shutdown).await.unwrap();
    assert_eq!(
        runtime.shutdown_with_grace(Duration::ZERO).await,
        Ok(ShutdownOutcome::Forced)
    );
    assert_eq!(
        f.hooks
            .hits(LifecyclePoint::ProviderCommit(ProviderOperation::AcknowledgeActivity)),
        0
    );
    f.assert_retired();
    active.release();
}

#[tokio::test]
async fn admitted_activity_finishes_and_acks_with_renewal_during_grace_without_new_admission() {
    let f = Fixture::new().await;
    let active = CancellationToken::new();
    let release = CancellationToken::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let renewal = f.hold(LifecyclePoint::ProviderReturn(ProviderOperation::RenewActivity));
    let handler_active = active.clone();
    let handler_release = release.clone();
    let handler_calls = calls.clone();
    let activities = ActivityRegistry::builder()
        .register("work", move |ctx: ActivityContext, _: String| {
            let active = handler_active.clone();
            let release = handler_release.clone();
            let calls = handler_calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                active.cancel();
                release.cancelled().await;
                assert!(!ctx.is_cancelled(), "runtime stop is not instance cancellation");
                assert!(!matches!(
                    ctx.get_client().get_orchestration_status("admitted").await.unwrap(),
                    OrchestrationStatus::NotFound
                ));
                Ok("finished".to_string())
            }
        })
        .build();
    let runtime = f.prepare(activities, flow(), options());
    runtime.clone().start_execution().await.unwrap();
    f.client().start_orchestration("admitted", "flow", "").await.unwrap();
    bounded(active.cancelled()).await;
    runtime.request_shutdown();
    let shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::from_secs(4)));
    f.client()
        .start_orchestration("not-admitted", "flow", "")
        .await
        .unwrap();
    entered(&renewal).await;
    assert_eq!(f.hooks.counts()[LifecycleTask::ActivityInvocation as usize].active, 1);
    assert_eq!(f.hooks.counts()[LifecycleTask::ActivityManager as usize].active, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    renewal.release();
    release.cancel();
    assert_eq!(bounded(shutdown).await.unwrap(), Ok(ShutdownOutcome::Drained));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.hooks.hits(LifecyclePoint::ForceRequested), 0);
    assert_eq!(
        f.hooks
            .hits(LifecyclePoint::ProviderCommit(ProviderOperation::AcknowledgeActivity)),
        1
    );
    f.assert_retired();

    let peer =
        Runtime::start_with_options(f.inner.clone(), ActivityRegistry::builder().build(), flow(), options()).await;
    assert!(
        matches!(bounded(f.client().wait_for_orchestration("admitted", Duration::from_secs(3))).await.unwrap(),
        OrchestrationStatus::Completed { output, .. } if output == "finished")
    );
    peer.shutdown(Some(0)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_retains_blocking_invocation_and_recovers_unacknowledged_work() {
    let f = Fixture::new().await;
    let active = CancellationToken::new();
    let handler_active = active.clone();
    let (release, receiver) = std::sync::mpsc::channel();
    let receiver = Arc::new(Mutex::new(receiver));
    let activities = ActivityRegistry::builder()
        .register("work", move |_: ActivityContext, _: String| {
            let active = handler_active.clone();
            let receiver = receiver.clone();
            async move {
                tokio::task::block_in_place(|| {
                    active.cancel();
                    receiver.lock().unwrap().recv_timeout(Duration::from_secs(10)).unwrap();
                });
                Ok("old execution".to_string())
            }
        })
        .build();
    let runtime = f.prepare(activities, flow(), options());
    runtime.clone().start_execution().await.unwrap();
    f.client().start_orchestration("blocking", "flow", "").await.unwrap();
    bounded(active.cancelled()).await;
    let mut shutdown = tokio::spawn(runtime.clone().shutdown_with_grace(Duration::from_millis(50)));
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(f.hooks.counts()[LifecycleTask::ActivityInvocation as usize].active, 1);
    let mut legacy = Box::pin(runtime.shutdown(Some(0)));
    assert!(
        legacy.as_mut().now_or_never().is_none(),
        "legacy return must still await cleanup"
    );
    release.send(()).unwrap();
    assert_eq!(bounded(shutdown).await.unwrap(), Ok(ShutdownOutcome::Forced));
    bounded(legacy).await;
    f.assert_retired();
    assert_eq!(
        f.hooks
            .hits(LifecyclePoint::ProviderCommit(ProviderOperation::AcknowledgeActivity)),
        0
    );

    let activities = ActivityRegistry::builder()
        .register("work", |_: ActivityContext, _: String| async {
            Ok("recovered".to_string())
        })
        .build();
    let peer = Runtime::start_with_options(f.inner.clone(), activities, flow(), options()).await;
    assert!(
        matches!(bounded(f.client().wait_for_orchestration("blocking", Duration::from_secs(3))).await.unwrap(),
        OrchestrationStatus::Completed { output, .. } if output == "recovered")
    );
    peer.shutdown(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_failure_cannot_publish_cleanup_while_a_native_callback_is_blocked() {
    let f = Fixture::new().await;
    let fault = LifecyclePoint::ParentWork(LifecycleTask::ActivityManager);
    let manager = f.hold(fault);
    f.hooks.fail_once(fault).unwrap();
    let active = CancellationToken::new();
    let entered_activity = active.clone();
    let (release, held) = std::sync::mpsc::channel();
    let held = Arc::new(Mutex::new(held));
    let activities = ActivityRegistry::builder()
        .register("work", move |_: ActivityContext, _: String| {
            let entered_activity = entered_activity.clone();
            let held = held.clone();
            async move {
                tokio::task::block_in_place(|| {
                    entered_activity.cancel();
                    held.lock().unwrap().recv_timeout(Duration::from_secs(10)).unwrap();
                });
                Ok("late".to_string())
            }
        })
        .build();
    let runtime = f.prepare(activities, flow(), options());
    runtime.clone().start_execution().await.unwrap();
    f.client()
        .start_orchestration("fault-with-callback", "flow", "")
        .await
        .unwrap();
    entered(&manager).await;
    bounded(active.cancelled()).await;
    manager.release();
    until(|| f.hooks.hits(LifecyclePoint::ForceRequested) == 1).await;
    let mut shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::ZERO));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(f.hooks.counts()[LifecycleTask::ActivityInvocation as usize].active, 1);
    release.send(()).unwrap();
    assert_eq!(
        bounded(shutdown).await.unwrap(),
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    f.assert_retired();
}

#[tokio::test]
async fn shutdown_hook_fault_remains_an_error_without_losing_startup_cleanup() {
    let f = Fixture::new().await;
    let held = f.hold(LifecyclePoint::StartupBeforeIo);
    f.hooks.fail_once(LifecyclePoint::ShutdownCoordinator).unwrap();
    let runtime = f.empty();
    let starter = tokio::spawn(runtime.clone().start_execution());
    entered(&held).await;
    assert_eq!(
        bounded(runtime.shutdown_with_grace(Duration::ZERO)).await,
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    assert!(matches!(
        bounded(starter).await.unwrap(),
        Err(RuntimeStartError::StartupFailed | RuntimeStartError::ShutdownRequested)
    ));
    f.assert_retired();
    held.release();
}

#[tokio::test]
async fn application_panics_remain_durable_application_errors_with_their_message() {
    let f = Fixture::new().await;
    let task_id = Arc::new(Mutex::new(None));
    let captured_task_id = task_id.clone();
    let activities = ActivityRegistry::builder()
        .register("work", move |_: ActivityContext, _: String| {
            let task_id = captured_task_id.clone();
            async move {
                *task_id.lock().unwrap() = Some(tokio::task::id());
                panic!("application-panic-message");
                #[allow(unreachable_code)]
                Ok(String::new())
            }
        })
        .build();
    let runtime = f.prepare(activities, flow(), options());
    runtime.clone().start_execution().await.unwrap();
    f.client().start_orchestration("app-panic", "flow", "").await.unwrap();
    assert!(
        matches!(bounded(f.client().wait_for_orchestration("app-panic", Duration::from_secs(3))).await.unwrap(),
        OrchestrationStatus::Failed { details, .. } if details.display_message().contains("application-panic-message"))
    );
    let expected = format!(
        "task {} panicked with message {:?}",
        task_id
            .lock()
            .unwrap()
            .expect("activity recorded its actual Tokio task"),
        "application-panic-message"
    );
    let history = f.inner.read("app-panic").await.unwrap();
    let activity_errors: Vec<_> = history
        .iter()
        .filter_map(|event| match &event.kind {
            duroxide::EventKind::ActivityFailed {
                details:
                    duroxide::ErrorDetails::Application {
                        kind: duroxide::AppErrorKind::ActivityFailed,
                        message,
                        ..
                    },
            } => Some(message.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(activity_errors, vec![expected.as_str()]);
    assert_eq!(
        runtime.shutdown_with_grace(Duration::from_secs(1)).await,
        Ok(ShutdownOutcome::Drained)
    );
    f.assert_retired();
}

#[tokio::test]
async fn a_panicking_diagnostic_subscriber_cannot_discard_tracked_cleanup() {
    use std::sync::atomic::AtomicBool;
    use tracing_subscriber::prelude::*;
    struct PanicOnce(Arc<AtomicBool>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PanicOnce {
        fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            if *event.metadata().level() == tracing::Level::ERROR && !self.0.swap(true, Ordering::SeqCst) {
                panic!("diagnostic subscriber failure");
            }
        }
    }
    let panicked = Arc::new(AtomicBool::new(false));
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(PanicOnce(panicked.clone())));
    let f = Fixture::new().await;
    let fault = LifecyclePoint::ParentWork(LifecycleTask::WorkerSlot);
    let failed = f.hold(fault);
    f.hooks.fail_once(fault).unwrap();
    let runtime = f.empty();
    runtime.clone().start_execution().await.unwrap();
    entered(&failed).await;
    failed.release();
    until(|| panicked.load(Ordering::SeqCst)).await;
    assert_eq!(
        bounded(runtime.shutdown_with_grace(Duration::ZERO)).await,
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    f.assert_retired();
}

#[tokio::test]
async fn a_panicking_force_diagnostic_cannot_skip_abort_or_cleanup() {
    use std::sync::atomic::AtomicBool;
    use tracing_subscriber::prelude::*;
    struct PanicOnce(Arc<AtomicBool>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PanicOnce {
        fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            if *event.metadata().level() == tracing::Level::WARN && !self.0.swap(true, Ordering::SeqCst) {
                panic!("force diagnostic failed");
            }
        }
    }
    let panicked = Arc::new(AtomicBool::new(false));
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(PanicOnce(panicked.clone())));
    let f = Fixture::new().await;
    let held = f.hold(LifecyclePoint::ParentWork(LifecycleTask::WorkerSlot));
    let runtime = f.empty();
    runtime.clone().start_execution().await.unwrap();
    entered(&held).await;
    assert_eq!(
        bounded(runtime.shutdown_with_grace(Duration::ZERO)).await,
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    assert!(panicked.load(Ordering::SeqCst));
    f.assert_retired();
    held.release();
}

#[tokio::test]
async fn force_cancels_pending_renewal_session_and_gauge_provider_calls() {
    for operation in [
        ProviderOperation::RenewOrchestration,
        ProviderOperation::RenewActivity,
        ProviderOperation::SessionMaintenance,
        ProviderOperation::SystemMetrics,
    ] {
        let f = Fixture::new().await;
        let mut config = options();
        config.observability.gauge_poll_interval = Duration::from_millis(10);
        config.session_lock_timeout = Duration::from_secs(2);
        let ack = if operation == ProviderOperation::RenewOrchestration {
            Some(f.hold(LifecyclePoint::ProviderEnter(
                ProviderOperation::AcknowledgeOrchestration,
            )))
        } else {
            None
        };
        let runtime = f.prepare(pending_activity(), flow(), config);
        runtime.clone().start_execution().await.unwrap();
        let held = f.hold(LifecyclePoint::ProviderReturn(operation));
        if operation != ProviderOperation::SystemMetrics {
            f.client()
                .start_orchestration("held-maintenance", "flow", "")
                .await
                .unwrap();
        }
        entered(&held).await;
        assert!(f.provider.active_operations() > 0);
        assert_eq!(
            bounded(runtime.shutdown_with_grace(Duration::ZERO)).await,
            Ok(ShutdownOutcome::Forced),
            "{operation:?}"
        );
        f.assert_retired();
        held.release();
        if let Some(ack) = ack {
            ack.release();
        }
    }
}

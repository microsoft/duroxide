// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "common/runtime_lifecycle.rs"]
mod support;
#[path = "common/tracing_capture.rs"]
mod tracing_capture;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

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
async fn startup_failure_after_each_root_spawn_reports_error_before_retained_rollback_finishes() {
    for task in [
        LifecycleTask::GaugePoller,
        LifecycleTask::OrchestrationDispatcher,
        LifecycleTask::WorkDispatcher,
    ] {
        let f = Fixture::new().await;
        let failed = LifecyclePoint::StartupAfterSpawn(task);
        let startup = f.hold(failed);
        f.hooks.fail_once_with_message(failed, SECRET).unwrap();
        let descendant = f.hold(match task {
            LifecycleTask::GaugePoller => LifecyclePoint::ParentWork(LifecycleTask::GaugePoller),
            LifecycleTask::OrchestrationDispatcher => {
                LifecyclePoint::ProviderReturn(ProviderOperation::FetchOrchestration)
            }
            LifecycleTask::WorkDispatcher => LifecyclePoint::ProviderReturn(ProviderOperation::FetchActivity),
            _ => unreachable!(),
        });
        let runtime = f.empty();
        let starter = tokio::spawn(Arc::clone(&runtime).start_execution());
        entered(&startup).await;
        entered(&descendant).await;
        startup.release();
        assert_eq!(bounded(starter).await.unwrap(), Err(RuntimeStartError::StartupFailed));
        let mut completion = Box::pin(runtime.wait_for_shutdown_completion());
        assert!(completion.as_mut().now_or_never().is_none());
        assert!(f.hooks.counts().iter().any(|count| count.active != 0));
        descendant.release();
        assert_eq!(
            bounded(completion).await,
            Err(RuntimeShutdownError::Failed { quiescent: true })
        );
        assert_eq!(f.hooks.hits(LifecyclePoint::ShutdownCoordinator), 1);
        f.assert_retired();
    }
}

async fn parent_fault_keeps_tree_owned(task: LifecycleTask) {
    let (events, _capture) = tracing_capture::install_tracing_capture();
    let f = Fixture::new().await;
    let fault = LifecyclePoint::ParentWork(task);
    let failing = f.hold(fault);
    f.hooks.fail_once_with_message(fault, SECRET).unwrap();
    let held = f.hold(match task {
        LifecycleTask::OrchestrationSlot => LifecyclePoint::ParentWork(LifecycleTask::OrchestrationRenewal),
        LifecycleTask::OrchestrationRenewal => LifecyclePoint::ParentWork(LifecycleTask::OrchestrationSlot),
        LifecycleTask::WorkerSlot | LifecycleTask::ActivityInvocation => {
            LifecyclePoint::ParentWork(LifecycleTask::ActivityManager)
        }
        LifecycleTask::ActivityManager => LifecyclePoint::ProviderReturn(ProviderOperation::Read),
        LifecycleTask::WorkDispatcher => LifecyclePoint::ProviderReturn(ProviderOperation::FetchActivity),
        _ => LifecyclePoint::ProviderReturn(ProviderOperation::FetchOrchestration),
    });
    let runtime = f.prepare(pending_activity(), flow(), options());
    if matches!(
        task,
        LifecycleTask::OrchestrationSlot
            | LifecycleTask::OrchestrationRenewal
            | LifecycleTask::WorkerSlot
            | LifecycleTask::ActivityManager
            | LifecycleTask::ActivityInvocation
    ) {
        f.client()
            .start_orchestration("parent-fault", "flow", "")
            .await
            .unwrap();
    }
    Arc::clone(&runtime).start_execution().await.unwrap();
    entered(&failing).await;
    entered(&held).await;
    let now = Instant::now();
    runtime
        .request_shutdown_until(now + Duration::from_secs(3), now + Duration::from_secs(4))
        .unwrap();
    failing.release();
    until(|| {
        events.lock().unwrap().iter().any(|event| {
            event.level == tracing::Level::ERROR
                && event.target == "duroxide::runtime::lifecycle"
                && event.message.contains("failed")
                && event.field("category").as_deref() == Some("owned_execution_failed")
        })
    })
    .await;
    if matches!(
        task,
        LifecycleTask::OrchestrationDispatcher
            | LifecycleTask::OrchestrationSlot
            | LifecycleTask::WorkDispatcher
            | LifecycleTask::WorkerSlot
    ) {
        assert_eq!(
            f.hooks.counts()[task as usize].active,
            1,
            "parent retired before its held child: {task:?}"
        );
    }
    let mut completion = Box::pin(runtime.wait_for_shutdown_completion());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), completion.as_mut())
            .await
            .is_err()
    );
    held.release();
    assert_eq!(
        bounded(completion).await,
        Err(RuntimeShutdownError::Failed { quiescent: true }),
        "{task:?}"
    );
    assert_eq!(f.hooks.hits(LifecyclePoint::ShutdownCoordinator), 1);
    f.assert_retired();
    assert!(!format!("{:?}", events.lock().unwrap()).contains("sentinel"));
}

#[tokio::test]
async fn every_task_role_fault_retains_its_children_and_remaining_tree() {
    for task in LifecycleTask::ALL {
        parent_fault_keeps_tree_owned(task).await;
    }
}

#[tokio::test]
async fn ordered_parent_panic_then_child_cleanup_keeps_ownership_100_times() {
    for _ in 0..100 {
        parent_fault_keeps_tree_owned(LifecycleTask::WorkDispatcher).await;
    }
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
    let handler_calls = Arc::clone(&calls);
    let activities = ActivityRegistry::builder()
        .register("work", move |ctx: ActivityContext, _: String| {
            let active = handler_active.clone();
            let release = handler_release.clone();
            let calls = Arc::clone(&handler_calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                active.cancel();
                release.cancelled().await;
                assert!(
                    !ctx.is_cancelled(),
                    "graceful runtime stop is not instance cancellation"
                );
                assert!(!matches!(
                    ctx.get_client().get_orchestration_status("admitted").await.unwrap(),
                    OrchestrationStatus::NotFound
                ));
                Ok("finished".to_string())
            }
        })
        .build();
    let runtime = f.prepare(activities, flow(), options());
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client().start_orchestration("admitted", "flow", "").await.unwrap();
    bounded(active.cancelled()).await;
    let now = Instant::now();
    runtime
        .request_shutdown_until(now + Duration::from_secs(4), now + Duration::from_secs(5))
        .unwrap();
    f.client()
        .start_orchestration("not-admitted", "flow", "")
        .await
        .unwrap();
    entered(&renewal).await;
    assert_eq!(f.hooks.counts()[LifecycleTask::ActivityInvocation as usize].active, 1);
    assert_eq!(f.hooks.counts()[LifecycleTask::ActivityManager as usize].active, 1);
    assert_eq!(f.hooks.counts()[LifecycleTask::SessionManager as usize].active, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    renewal.release();
    release.cancel();
    assert_eq!(
        bounded(runtime.wait_for_shutdown_completion()).await,
        Ok(ShutdownOutcome::Drained)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.hooks.hits(LifecyclePoint::ForceRequested), 0);
    assert_eq!(
        f.hooks
            .hits(LifecyclePoint::ProviderCommit(ProviderOperation::AcknowledgeActivity)),
        1
    );
    f.assert_retired();

    let peer = Runtime::start_with_options(
        Arc::clone(&f.inner),
        ActivityRegistry::builder().build(),
        flow(),
        options(),
    )
    .await;
    assert!(
        matches!(bounded(f.client().wait_for_orchestration("admitted", Duration::from_secs(3))).await.unwrap(),
        OrchestrationStatus::Completed { output, .. } if output == "finished")
    );
    peer.shutdown(Some(0)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_abort_retains_blocking_leaf_and_recovers_unfinished_activity_after_release() {
    let f = Fixture::new().await;
    let active = CancellationToken::new();
    let handler_active = active.clone();
    let (release, receiver) = std::sync::mpsc::channel();
    let receiver = Arc::new(Mutex::new(receiver));
    let activities = ActivityRegistry::builder()
        .register("work", move |_: ActivityContext, _: String| {
            let active = handler_active.clone();
            let receiver = Arc::clone(&receiver);
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
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client().start_orchestration("blocking", "flow", "").await.unwrap();
    bounded(active.cancelled()).await;
    assert_eq!(
        Arc::clone(&runtime)
            .shutdown_with_timeouts(Duration::from_millis(100), Duration::from_millis(300))
            .await,
        Err(RuntimeShutdownError::TimedOut)
    );
    assert_eq!(f.hooks.counts()[LifecycleTask::ActivityInvocation as usize].active, 1);
    assert_eq!(
        f.hooks
            .hits(LifecyclePoint::ProviderCommit(ProviderOperation::AcknowledgeActivity)),
        0
    );
    let mut completion = Box::pin(runtime.wait_for_shutdown_completion());
    assert!(completion.as_mut().now_or_never().is_none());
    release.send(()).unwrap();
    assert_eq!(bounded(completion).await, Ok(ShutdownOutcome::Forced));
    f.assert_retired();
    assert_eq!(
        f.hooks
            .hits(LifecyclePoint::ProviderCommit(ProviderOperation::AcknowledgeActivity)),
        0,
        "runtime force must not acknowledge unfinished work with None"
    );

    let activities = ActivityRegistry::builder()
        .register("work", |_: ActivityContext, _: String| async {
            Ok("recovered".to_string())
        })
        .build();
    let peer = Runtime::start_with_options(Arc::clone(&f.inner), activities, flow(), options()).await;
    assert!(
        matches!(bounded(f.client().wait_for_orchestration("blocking", Duration::from_secs(3))).await.unwrap(),
        OrchestrationStatus::Completed { output, .. } if output == "recovered")
    );
    peer.shutdown(None).await;
}

#[tokio::test]
async fn coordinator_fault_cannot_drop_startup_join_or_disable_original_force_deadline() {
    let f = Fixture::new().await;
    let held = f.hold(LifecyclePoint::StartupBeforeIo);
    f.hooks.fail_once(LifecyclePoint::ShutdownCoordinator).unwrap();
    let runtime = f.empty();
    let starter = tokio::spawn(Arc::clone(&runtime).start_execution());
    entered(&held).await;
    assert_eq!(
        Arc::clone(&runtime)
            .shutdown_with_timeouts(Duration::from_millis(50), Duration::from_millis(100))
            .await,
        Err(RuntimeShutdownError::TimedOut)
    );
    assert!(runtime.wait_for_shutdown_completion().now_or_never().is_none());
    held.release();
    assert_eq!(bounded(starter).await.unwrap(), Err(RuntimeStartError::StartupFailed));
    assert_eq!(
        bounded(runtime.wait_for_shutdown_completion()).await,
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    f.assert_retired();
}

#[tokio::test]
async fn application_panics_remain_durable_application_errors_with_their_message() {
    let f = Fixture::new().await;
    let activities = ActivityRegistry::builder()
        .register("work", |_: ActivityContext, _: String| async {
            panic!("application-panic-message");
            #[allow(unreachable_code)]
            Ok(String::new())
        })
        .build();
    let runtime = f.prepare(activities, flow(), options());
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client().start_orchestration("app-panic", "flow", "").await.unwrap();
    assert!(
        matches!(bounded(f.client().wait_for_orchestration("app-panic", Duration::from_secs(3))).await.unwrap(),
        OrchestrationStatus::Failed { details, .. } if details.display_message().contains("application-panic-message"))
    );
    assert_eq!(
        runtime.shutdown_with_grace(Duration::from_secs(1)).await,
        Ok(ShutdownOutcome::Drained)
    );
    f.assert_retired();
}

#[tokio::test]
async fn a_panicking_diagnostic_subscriber_cannot_discard_the_cleanup_epilogue() {
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
        tracing::subscriber::set_default(tracing_subscriber::registry().with(PanicOnce(Arc::clone(&panicked))));
    let f = Fixture::new().await;
    let fault = LifecyclePoint::ParentWork(LifecycleTask::WorkDispatcher);
    let failed = f.hold(fault);
    f.hooks.fail_once(fault).unwrap();
    let child = f.hold(LifecyclePoint::ProviderReturn(ProviderOperation::FetchActivity));
    let runtime = f.empty();
    Arc::clone(&runtime).start_execution().await.unwrap();
    entered(&failed).await;
    entered(&child).await;
    failed.release();
    until(|| panicked.load(Ordering::SeqCst)).await;
    assert_eq!(f.hooks.counts()[LifecycleTask::WorkDispatcher as usize].active, 1);
    assert!(runtime.wait_for_shutdown_completion().now_or_never().is_none());
    child.release();
    assert_eq!(
        bounded(runtime.wait_for_shutdown_completion()).await,
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    f.assert_retired();
}

#[tokio::test]
async fn force_never_cancels_in_flight_renewal_session_or_gauge_provider_calls() {
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
        let work = if operation == ProviderOperation::RenewOrchestration {
            Some(f.hold(LifecyclePoint::ParentWork(LifecycleTask::OrchestrationSlot)))
        } else {
            None
        };
        let runtime = f.prepare(pending_activity(), flow(), config);
        Arc::clone(&runtime).start_execution().await.unwrap();
        // Arm after startup so SystemMetrics targets the running gauge poller.
        let held = f.hold(LifecyclePoint::ProviderReturn(operation));
        if operation != ProviderOperation::SystemMetrics {
            f.client()
                .start_orchestration("held-maintenance", "flow", "")
                .await
                .unwrap();
        }
        if let Some(work) = &work {
            entered(work).await;
        }
        entered(&held).await;
        let now = Instant::now();
        runtime
            .request_shutdown_until(now, now + Duration::from_millis(300))
            .unwrap();
        until(|| f.hooks.hits(LifecyclePoint::ForceRequested) == 1).await;
        if let Some(work) = &work {
            work.release();
        }
        assert_eq!(
            Arc::clone(&runtime)
                .shutdown_with_timeouts(Duration::ZERO, Duration::from_millis(300))
                .await,
            Err(RuntimeShutdownError::TimedOut),
            "{operation:?}"
        );
        assert!(f.provider.active_operations() > 0, "{operation:?}");
        held.release();
        assert_eq!(
            bounded(runtime.wait_for_shutdown_completion()).await,
            Ok(ShutdownOutcome::Forced),
            "{operation:?}"
        );
        f.assert_retired();
    }
}

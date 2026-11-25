// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;
#[path = "common/runtime_lifecycle.rs"]
mod support;

use common::tracing_capture::{CapturedEvent, install_tracing_capture};
use duroxide::runtime::registry::{ActivityRegistry, ActivityRegistryBuilder, OrchestrationRegistryBuilder};
use duroxide::runtime::test_hooks::{LifecyclePoint, LifecycleTask, ProviderOperation};
use duroxide::runtime::{
    ActivityAdmission, Runtime, RuntimeShutdownError, RuntimeStopObserver, RuntimeStopStatus, ShutdownOutcome,
};
use duroxide::{ActivityContext, EventKind, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;
use support::{Fixture, LIMIT, bounded, options, until};

struct DropFailure(Arc<AtomicUsize>);

impl Drop for DropFailure {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::panic::resume_unwind(Box::new("private-cleanup-sentinel"));
    }
}

fn activities() -> ActivityRegistryBuilder {
    ActivityRegistry::builder().register("healthy", |_: ActivityContext, _: String| async {
        Ok("healthy".to_string())
    })
}

fn orchestrations() -> OrchestrationRegistryBuilder {
    OrchestrationRegistry::builder()
        .register("flow", |ctx: OrchestrationContext, input: String| async move {
            ctx.schedule_activity("work", input).await
        })
        .register("healthy", |ctx: OrchestrationContext, _: String| async move {
            ctx.schedule_activity("healthy", "").await
        })
}

fn assert_original_slots(fixture: &Fixture, runtime: &Runtime) {
    assert_eq!(runtime.stop_status(), RuntimeStopStatus::NotRequested);
    assert_eq!(runtime.stop_observer().state(), RuntimeStopStatus::NotRequested);
    for role in [LifecycleTask::WorkerSlot, LifecycleTask::OrchestrationSlot] {
        let counts = fixture.hooks.counts()[role as usize];
        assert_eq!(counts.started, 1, "{role:?} was replaced");
        assert_eq!(counts.active, 1, "{role:?} is no longer available");
        assert_eq!(counts.completed, 0, "{role:?} retired");
    }
}

async fn followup(fixture: &Fixture, runtime: &Runtime, suffix: &str) {
    let client = fixture.client();
    let instance = format!("healthy-{suffix}");
    client.start_orchestration(&instance, "healthy", "").await.unwrap();
    assert!(matches!(
        bounded(client.wait_for_orchestration(&instance, LIMIT)).await.unwrap(),
        OrchestrationStatus::Completed { output, .. } if output == "healthy"
    ));
    assert_original_slots(fixture, runtime);
}

async fn finish(fixture: &Fixture, runtime: Arc<Runtime>) {
    assert_eq!(
        bounded(runtime.shutdown_with_grace(Duration::from_secs(1))).await,
        Ok(ShutdownOutcome::Drained)
    );
    fixture.assert_retired();
}

fn incidents(events: &Arc<Mutex<Vec<CapturedEvent>>>, stage: &str) -> Vec<CapturedEvent> {
    events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| {
            event.field("category").as_deref() == Some("contained_user_cleanup")
                && event.field("stage").as_deref() == Some(stage)
        })
        .cloned()
        .collect()
}

fn assert_incident(event: &CapturedEvent, instance: &str, operation: &str, activity: bool) {
    assert_eq!(event.target, "duroxide::runtime::cleanup");
    assert_eq!(event.level, tracing::Level::ERROR);
    assert_eq!(event.field("instance").as_deref(), Some(instance));
    assert_eq!(event.field("execution_id").as_deref(), Some("1"));
    assert_eq!(event.field("operation").as_deref(), Some(operation));
    assert_eq!(event.field("disposition").as_deref(), Some("operation-local"));
    assert_ne!(event.field("task_id").expect("incident task identity"), "None");
    assert_eq!(
        event.field("activity_id").expect("incident activity identity") != "None",
        activity
    );
    assert!(!format!("{event:?}").contains("private-cleanup-sentinel"));
}

#[tokio::test(flavor = "current_thread")]
async fn user_cleanup_admission_preserves_returned_success_and_failure() {
    let (events, _capture) = install_tracing_capture();
    let f = Fixture::new().await;
    let drops = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&drops);
    let registry = activities()
        .register_with_admission(
            "work",
            move |_, _| Ok(ActivityAdmission::admit(DropFailure(Arc::clone(&counter)))),
            |_, input| async move {
                if input == "fail" {
                    Err("actual failure".to_string())
                } else {
                    Ok(input)
                }
            },
        )
        .build();
    let runtime = f.prepare(registry, orchestrations().build(), options());
    Arc::clone(&runtime).start_execution().await.unwrap();
    for input in ["success", "fail"] {
        for occurrence in 0..2 {
            let instance = format!("{input}-{occurrence}");
            let client = f.client();
            client.start_orchestration(&instance, "flow", input).await.unwrap();
            let status = bounded(client.wait_for_orchestration(&instance, LIMIT)).await.unwrap();
            if input == "fail" {
                assert!(matches!(status, OrchestrationStatus::Failed { details, .. }
                    if details.display_message().contains("actual failure")));
            } else {
                assert!(matches!(status, OrchestrationStatus::Completed { output, .. } if output == input));
            }
            let history = f.inner.read(&instance).await.unwrap();
            assert_eq!(
                history
                    .iter()
                    .filter(|event| matches!(
                        event.kind,
                        EventKind::ActivityCompleted { .. } | EventKind::ActivityFailed { .. }
                    ))
                    .count(),
                1
            );
            followup(&f, &runtime, &instance).await;
        }
    }
    let recorded = incidents(&events, "activity-admission");
    assert_eq!(recorded.len(), 4);
    for (event, instance) in recorded.iter().zip(["success-0", "success-1", "fail-0", "fail-1"]) {
        assert_incident(event, instance, "work", true);
        assert_eq!(event.field("payload_fate").as_deref(), Some("disposed"));
        assert_eq!(event.field("payload_cleanup_skipped").as_deref(), Some("false"));
    }
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    finish(&f, runtime).await;
}

#[tokio::test(flavor = "current_thread")]
async fn contained_activity_reporting_failure_remains_runtime_fatal() {
    use tracing_subscriber::prelude::*;

    struct Reporter {
        entered: Arc<AtomicBool>,
        observed_local: Arc<AtomicBool>,
        observer: RuntimeStopObserver,
    }
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Reporter {
        fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            if event.metadata().target() == "duroxide::runtime::cleanup" {
                self.entered.store(true, Ordering::SeqCst);
                self.observed_local.store(
                    self.observer.state() == RuntimeStopStatus::NotRequested,
                    Ordering::SeqCst,
                );
                panic!("contained-incident reporting failure");
            }
        }
    }

    let f = Fixture::new().await;
    let drops = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&drops);
    let registry = activities()
        .register_with_admission(
            "work",
            move |_, _| Ok(ActivityAdmission::admit(DropFailure(Arc::clone(&counter)))),
            |_, _| async { Ok("returned, not necessarily acknowledged".to_string()) },
        )
        .build();
    let runtime = f.prepare(registry, orchestrations().build(), options());
    let entered = Arc::new(AtomicBool::new(false));
    let observed_local = Arc::new(AtomicBool::new(false));
    let _subscriber = tracing::subscriber::set_default(tracing_subscriber::registry().with(Reporter {
        entered: Arc::clone(&entered),
        observed_local: Arc::clone(&observed_local),
        observer: runtime.stop_observer(),
    }));
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client().start_orchestration("reporting", "flow", "").await.unwrap();
    until(|| runtime.stop_status() == RuntimeStopStatus::Faulted).await;
    assert!(entered.load(Ordering::SeqCst));
    assert!(observed_local.load(Ordering::SeqCst));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        bounded(Arc::clone(&runtime).shutdown_with_grace(Duration::ZERO)).await,
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    assert_eq!(runtime.stop_status(), RuntimeStopStatus::Faulted);
    f.assert_retired();
}

#[tokio::test(flavor = "current_thread")]
async fn user_cleanup_cancellation_preserves_original_worker_and_single_ack() {
    let (events, _capture) = install_tracing_capture();
    let f = Fixture::new().await;
    let drops = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&drops);
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let registry = activities()
        .register("work", move |_: ActivityContext, _: String| {
            let counter = Arc::clone(&counter);
            let started = started.clone();
            async move {
                let _drop = DropFailure(counter);
                started.send(()).unwrap();
                std::future::pending::<()>().await;
                Ok("must not complete".to_string())
            }
        })
        .build();
    let mut config = options();
    config.activity_cancellation_grace_period = Duration::from_millis(5);
    let runtime = f.prepare(registry, orchestrations().build(), config);
    Arc::clone(&runtime).start_execution().await.unwrap();
    for occurrence in 0..2 {
        let instance = format!("cancel-{occurrence}");
        let entered_ack = LifecyclePoint::ProviderEnter(ProviderOperation::AcknowledgeActivity);
        let returned_ack = LifecyclePoint::ProviderReturn(ProviderOperation::AcknowledgeActivity);
        let committed_ack = LifecyclePoint::ProviderCommit(ProviderOperation::AcknowledgeActivity);
        let previous_enters = f.hooks.hits(entered_ack);
        let previous_returns = f.hooks.hits(returned_ack);
        let previous_commits = f.hooks.hits(committed_ack);
        let client = f.client();
        client.start_orchestration(&instance, "flow", "").await.unwrap();
        bounded(starts.recv()).await.unwrap();
        client.cancel_instance(&instance, "cancel user activity").await.unwrap();
        until(|| drops.load(Ordering::SeqCst) == occurrence + 1).await;
        until(|| f.hooks.hits(returned_ack) == previous_returns + 1).await;
        assert_eq!(f.hooks.hits(entered_ack), previous_enters + 1);
        // Cancellation steals the SQLite lease; the single ack attempt fails.
        assert_eq!(f.hooks.hits(committed_ack), previous_commits);
        let history = f.inner.read(&instance).await.unwrap();
        assert!(history.iter().any(|event| matches!(&event.kind,
            EventKind::OrchestrationFailed { details: duroxide::ErrorDetails::Application {
                kind: duroxide::AppErrorKind::Cancelled { reason }, ..
            }} if reason == "cancel user activity"
        )));
        assert!(!history.iter().any(|event| matches!(
            event.kind,
            EventKind::ActivityCompleted { .. } | EventKind::ActivityFailed { .. }
        )));
        followup(&f, &runtime, &instance).await;
    }
    let recorded = incidents(&events, "activity-invocation");
    assert_eq!(recorded.len(), 2);
    for (event, instance) in recorded.iter().zip(["cancel-0", "cancel-1"]) {
        assert_incident(event, instance, "work", true);
    }
    finish(&f, runtime).await;
}

struct ChainedPayload {
    remaining: usize,
    drops: Arc<AtomicUsize>,
}

impl Drop for ChainedPayload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.remaining > 0 {
            std::panic::resume_unwind(Box::new(Self {
                remaining: self.remaining - 1,
                drops: Arc::clone(&self.drops),
            }));
        }
    }
}

async fn payload_work(input: String, drops: Arc<AtomicUsize>) -> Result<String, String> {
    std::panic::resume_unwind(Box::new(ChainedPayload {
        remaining: input.parse().unwrap(),
        drops,
    }))
}

async fn check_payloads(activity: bool) {
    let (events, _capture) = install_tracing_capture();
    let f = Fixture::new().await;
    let drops = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&drops);
    let activity_registry = if activity {
        activities()
            .register("work", move |_: ActivityContext, input: String| {
                payload_work(input, Arc::clone(&counter))
            })
            .build()
    } else {
        activities().build()
    };
    let counter = Arc::clone(&drops);
    let orchestration_registry = orchestrations()
        .register("payload", move |_: OrchestrationContext, input: String| {
            payload_work(input, Arc::clone(&counter))
        })
        .build();
    let runtime = f.prepare(activity_registry, orchestration_registry, options());
    Arc::clone(&runtime).start_execution().await.unwrap();
    let stage = if activity {
        "activity-panic-payload"
    } else {
        "orchestration-panic-payload"
    };
    for remaining in 0_usize..=2 {
        for occurrence in 0..2 {
            let before = drops.load(Ordering::SeqCst);
            let instance = format!("payload-{remaining}-{occurrence}");
            let client = f.client();
            client
                .start_orchestration(
                    &instance,
                    if activity { "flow" } else { "payload" },
                    &remaining.to_string(),
                )
                .await
                .unwrap();
            let status = bounded(client.wait_for_orchestration(&instance, LIMIT)).await.unwrap();
            assert!(matches!(status, OrchestrationStatus::Failed { .. }), "{status:?}");
            assert_eq!(drops.load(Ordering::SeqCst) - before, (remaining + 1).min(2));
            let recorded = incidents(&events, stage);
            let current: Vec<_> = recorded
                .iter()
                .filter(|event| event.field("instance").as_deref() == Some(instance.as_str()))
                .collect();
            assert_eq!(current.len(), usize::from(remaining != 0));
            if let Some(event) = current.first() {
                assert_incident(event, &instance, if activity { "work" } else { "payload" }, activity);
                assert_eq!(
                    event.field("payload_fate").as_deref(),
                    Some(if remaining == 1 {
                        "secondary-disposed"
                    } else {
                        "cleanup-skipped"
                    })
                );
                assert_eq!(event.field("payload_disposal_failures"), Some(remaining.to_string()));
                assert_eq!(
                    event.field("payload_cleanup_skipped"),
                    Some((remaining == 2).to_string())
                );
            }
            followup(&f, &runtime, &instance).await;
        }
    }
    assert_eq!(incidents(&events, stage).len(), 4);
    finish(&f, runtime).await;
}

#[tokio::test(flavor = "current_thread")]
async fn user_cleanup_activity_payload_is_bounded_and_original_worker_continues() {
    check_payloads(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn user_cleanup_orchestration_payload_is_bounded_and_original_slot_continues() {
    check_payloads(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn user_cleanup_driver_preserves_finalized_turns_and_original_slot() {
    struct Driver {
        context: OrchestrationContext,
        _failure: DropFailure,
    }
    impl Drop for Driver {
        fn drop(&mut self) {
            self.context.set_kv_value("after-snapshot", "must-not-persist");
        }
    }

    let (events, _capture) = install_tracing_capture();
    let f = Fixture::new().await;
    let drops = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&drops);
    let registry = orchestrations()
        .register("pending", move |ctx: OrchestrationContext, _: String| {
            let counter = Arc::clone(&counter);
            async move {
                let final_ctx = ctx.clone();
                ctx.register_invocation_cleanup_with_finalization(async { Ok(()) }, move || {
                    final_ctx.set_kv_value("before-snapshot", "included")
                })
                .unwrap();
                let _driver = Driver {
                    context: ctx.clone(),
                    _failure: DropFailure(counter),
                };
                Ok(ctx.schedule_wait("hold").await)
            }
        })
        .build();
    let runtime = f.prepare(activities().build(), registry, options());
    Arc::clone(&runtime).start_execution().await.unwrap();
    for occurrence in 0..2 {
        let instance = format!("driver-{occurrence}");
        f.client().start_orchestration(&instance, "pending", "").await.unwrap();
        bounded(async {
            while f
                .inner
                .get_kv_value(&instance, "before-snapshot")
                .await
                .unwrap()
                .is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(f.inner.get_kv_value(&instance, "after-snapshot").await.unwrap(), None);
        let history = f.inner.read(&instance).await.unwrap();
        assert!(!history.iter().any(|event| matches!(
            event.kind,
            EventKind::OrchestrationFailed { .. } | EventKind::ExternalSubscribedCancelled { .. }
        )));
        followup(&f, &runtime, &instance).await;
        assert_eq!(f.inner.read(&instance).await.unwrap(), history);
    }
    let recorded = incidents(&events, "orchestration-driver");
    assert_eq!(recorded.len(), 2);
    for (event, instance) in recorded.iter().zip(["driver-0", "driver-1"]) {
        assert_incident(event, instance, "pending", false);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    finish(&f, runtime).await;
}

struct InnerReady {
    _failure: DropFailure,
}

impl Future for InnerReady {
    type Output = Result<String, String>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok("unproven outer result".to_string()))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn user_cleanup_inner_ready_does_not_invent_an_outer_result() {
    for activity in [true, false] {
        let f = Fixture::new().await;
        let drops = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&drops);
        let activity_registry = activities()
            .register("work", move |_: ActivityContext, _: String| InnerReady {
                _failure: DropFailure(Arc::clone(&counter)),
            })
            .build();
        let counter = Arc::clone(&drops);
        let orchestration_registry = orchestrations()
            .register("inner", move |_: OrchestrationContext, _: String| InnerReady {
                _failure: DropFailure(Arc::clone(&counter)),
            })
            .build();
        let runtime = f.prepare(activity_registry, orchestration_registry, options());
        Arc::clone(&runtime).start_execution().await.unwrap();
        let client = f.client();
        client
            .start_orchestration("inner", if activity { "flow" } else { "inner" }, "")
            .await
            .unwrap();
        assert!(matches!(
            bounded(client.wait_for_orchestration("inner", LIMIT)).await.unwrap(),
            OrchestrationStatus::Failed { .. }
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        followup(&f, &runtime, "inner").await;
        finish(&f, runtime).await;
    }
}

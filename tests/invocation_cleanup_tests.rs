// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[allow(dead_code)]
#[path = "replay_engine/helpers.rs"]
mod replay_helpers;
#[path = "common/runtime_lifecycle.rs"]
mod support;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::replay_engine::TurnResult;
use duroxide::runtime::test_hooks::{LifecyclePoint, LifecycleTask, ProviderOperation};
use duroxide::runtime::{InvocationCleanupError, RuntimeShutdownError, ShutdownOutcome};
use duroxide::{Event, EventKind, OrchestrationContext, OrchestrationRegistry};
use futures_util::FutureExt;
use support::{Fixture, SECRET, bounded, entered, options, until};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum Turn {
    Continue,
    ContinueAsNew,
    ApplicationError,
    Complete,
}

#[derive(Clone, Default)]
struct MockForeign {
    driver_dropped: CancellationToken,
    cleanup_entered: CancellationToken,
    release: CancellationToken,
    contexts: Arc<Mutex<Vec<OrchestrationContext>>>,
    callbacks: Arc<AtomicUsize>,
}

struct DriverDrop {
    dropped: CancellationToken,
    panic: bool,
}

impl Drop for DriverDrop {
    fn drop(&mut self) {
        self.dropped.cancel();
        assert!(!self.panic, "injected foreign driver destruction failure");
    }
}

impl MockForeign {
    fn registry(&self, turn: Turn, attach: bool, panic_on_drop: bool, fail_cleanup: bool) -> OrchestrationRegistry {
        let foreign = self.clone();
        OrchestrationRegistry::builder()
            .register("flow", move |ctx: OrchestrationContext, _: String| {
                let foreign = foreign.clone();
                async move {
                    if attach {
                        let retained = foreign.clone();
                        let cleanup_ctx = ctx.clone();
                        ctx.register_invocation_cleanup(async move {
                            assert!(
                                retained.driver_dropped.is_cancelled(),
                                "retirement started before driver destruction"
                            );
                            retained.cleanup_entered.cancel();
                            retained.release.cancelled().await;
                            // Simulate a badly behaved foreign continuation after finalization.
                            // It must not amend the already captured durable turn.
                            cleanup_ctx.set_custom_status("retirement-only");
                            cleanup_ctx.set_kv_value("retirement-only", "must not persist");
                            if fail_cleanup { Err(SECRET.to_string()) } else { Ok(()) }
                        })
                        .map_err(|error| error.to_string())?;
                        assert_eq!(
                            ctx.register_invocation_cleanup(async { Ok(()) }),
                            Err(InvocationCleanupError::AlreadyAttached)
                        );
                    }
                    foreign.contexts.lock().unwrap().push(ctx.clone());
                    foreign.callbacks.fetch_add(1, Ordering::SeqCst);
                    let _driver = DriverDrop {
                        dropped: foreign.driver_dropped.clone(),
                        panic: panic_on_drop,
                    };
                    let activity = ctx.schedule_activity("work", "payload");
                    let event = ctx.schedule_wait("pause");
                    if matches!(turn, Turn::Continue) {
                        let _ = ctx.join2(activity, event).await;
                        return Ok("continued".to_string());
                    }
                    let result = match turn {
                        Turn::ContinueAsNew => ctx.continue_as_new("next").await,
                        Turn::ApplicationError => Err("application failure".to_string()),
                        Turn::Complete => Ok("complete".to_string()),
                        Turn::Continue => unreachable!(),
                    };
                    drop(activity);
                    drop(event);
                    result
                }
            })
            .build()
    }
}

#[test]
fn synchronous_replay_rejects_cleanup_before_foreign_activation() {
    let foreign = MockForeign::default();
    let registry = foreign.registry(Turn::Complete, true, false, false);
    let handler = registry.resolve_handler("flow").unwrap().1;
    let mut engine = replay_helpers::create_engine(vec![replay_helpers::started_event(1)]);
    let result = replay_helpers::execute(&mut engine, handler);
    assert!(matches!(result, TurnResult::Failed(details)
        if details.display_message().contains("runtime-owned asynchronous execution")));
    assert_eq!(foreign.callbacks.load(Ordering::SeqCst), 0);
    assert!(!foreign.cleanup_entered.is_cancelled());
}

async fn captured_turn(turn: Turn, attach: bool, force: bool) -> Vec<Event> {
    let f = Fixture::new().await;
    let foreign = MockForeign::default();
    let before_ack = f.hold(LifecyclePoint::ProviderEnter(
        ProviderOperation::AcknowledgeOrchestration,
    ));
    let mut config = options();
    config.worker_concurrency = 0;
    let runtime = f.prepare(
        ActivityRegistry::builder().build(),
        foreign.registry(turn, attach, false, false),
        config,
    );
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client().start_orchestration("turn", "flow", "").await.unwrap();
    entered(&before_ack).await;
    assert!(foreign.driver_dropped.is_cancelled());
    let grace = if force {
        Duration::from_millis(100)
    } else {
        Duration::from_secs(3)
    };
    let total = if force {
        Duration::from_millis(300)
    } else {
        Duration::from_secs(4)
    };
    let now = Instant::now();
    runtime.request_shutdown_until(now + grace, now + total).unwrap();
    before_ack.release();
    until(|| {
        f.hooks.hits(LifecyclePoint::ProviderCommit(
            ProviderOperation::AcknowledgeOrchestration,
        )) == 1
    })
    .await;
    if attach {
        bounded(foreign.cleanup_entered.cancelled()).await;
        assert_eq!(f.hooks.cleanup_counts().active, 1);
        assert_eq!(f.hooks.counts()[LifecycleTask::OrchestrationSlot as usize].active, 1);
        let ctx = foreign.contexts.lock().unwrap()[0].clone();
        assert_eq!(ctx.instance_id(), "turn");
        assert_eq!(ctx.execution_id(), 1);
        assert_eq!(
            ctx.register_invocation_cleanup(async { Ok(()) }),
            Err(InvocationCleanupError::Closed)
        );
        assert!(runtime.wait_for_shutdown_completion().now_or_never().is_none());
        if force {
            assert_eq!(
                Arc::clone(&runtime).shutdown_with_timeouts(grace, total).await,
                Err(RuntimeShutdownError::TimedOut)
            );
            assert!(now.elapsed() < Duration::from_secs(1));
            assert_eq!(f.hooks.cleanup_counts().active, 1);
        }
    }
    let before = f.inner.read_with_execution("turn", 1).await.unwrap();
    foreign.release.cancel();
    assert_eq!(
        bounded(runtime.wait_for_shutdown_completion()).await,
        Ok(if force {
            ShutdownOutcome::Forced
        } else {
            ShutdownOutcome::Drained
        })
    );
    f.assert_retired();
    assert_eq!(f.hooks.cleanup_counts().started, usize::from(attach));
    let history = f.inner.read_with_execution("turn", 1).await.unwrap();
    assert_eq!(
        serde_json::to_value(&before).unwrap(),
        serde_json::to_value(&history).unwrap(),
        "late invocation retirement amended the committed turn"
    );
    assert!(!serde_json::to_string(&history).unwrap().contains("retirement-only"));
    assert_eq!(f.inner.get_kv_value("turn", "retirement-only").await.unwrap(), None);
    assert_eq!(foreign.callbacks.load(Ordering::SeqCst), 1);
    history
}

#[tokio::test]
async fn replay_histories_preserve_continue_can_error_and_forced_retirement_semantics() {
    for turn in [
        Turn::Continue,
        Turn::ContinueAsNew,
        Turn::ApplicationError,
        Turn::Complete,
    ] {
        let ordinary = captured_turn(turn, false, false).await;
        let foreign = captured_turn(turn, true, false).await;
        assert_eq!(
            without_creation_times(ordinary),
            without_creation_times(foreign.clone())
        );
        if matches!(turn, Turn::Continue) {
            assert!(!foreign.iter().any(|event| matches!(
                event.kind,
                EventKind::ActivityCancelRequested { .. } | EventKind::ExternalSubscribedCancelled { .. }
            )));
        }
    }
    let ordinary = captured_turn(Turn::Continue, false, false).await;
    let forced = captured_turn(Turn::Continue, true, true).await;
    assert_eq!(without_creation_times(ordinary), without_creation_times(forced));
}

fn without_creation_times(mut history: Vec<Event>) -> Vec<Event> {
    // Independent runs have different wall-clock creation times, not different decisions.
    for event in &mut history {
        event.timestamp_ms = 0;
    }
    history
}

#[tokio::test]
async fn admitted_orchestration_and_foreign_retirement_finish_within_grace_and_keep_renewal() {
    let f = Fixture::new().await;
    let foreign = MockForeign::default();
    let ack = f.hold(LifecyclePoint::ProviderEnter(
        ProviderOperation::AcknowledgeOrchestration,
    ));
    let renewed = f.hold(LifecyclePoint::ProviderReturn(ProviderOperation::RenewOrchestration));
    let mut config = options();
    config.worker_concurrency = 0;
    let runtime = f.prepare(
        ActivityRegistry::builder().build(),
        foreign.registry(Turn::Complete, true, false, false),
        config,
    );
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client()
        .start_orchestration("admitted-turn", "flow", "")
        .await
        .unwrap();
    entered(&ack).await;
    let now = Instant::now();
    runtime
        .request_shutdown_until(now + Duration::from_secs(4), now + Duration::from_secs(5))
        .unwrap();
    f.client()
        .start_orchestration("not-admitted", "flow", "")
        .await
        .unwrap();
    entered(&renewed).await;
    assert_eq!(f.hooks.counts()[LifecycleTask::OrchestrationRenewal as usize].active, 1);
    assert_eq!(foreign.callbacks.load(Ordering::SeqCst), 1);
    renewed.release();
    ack.release();
    bounded(foreign.cleanup_entered.cancelled()).await;
    assert_eq!(f.hooks.cleanup_counts().active, 1);
    assert!(runtime.wait_for_shutdown_completion().now_or_never().is_none());
    foreign.release.cancel();
    assert_eq!(
        bounded(runtime.wait_for_shutdown_completion()).await,
        Ok(ShutdownOutcome::Drained)
    );
    assert_eq!(f.hooks.hits(LifecyclePoint::ForceRequested), 0);
    assert_eq!(
        f.hooks.hits(LifecyclePoint::ProviderCommit(
            ProviderOperation::AcknowledgeOrchestration
        )),
        1
    );
    assert_eq!(foreign.callbacks.load(Ordering::SeqCst), 1);
    assert!(
        matches!(f.client().get_orchestration_status("admitted-turn").await.unwrap(),
        duroxide::OrchestrationStatus::Completed { output, .. } if output == "complete")
    );
    f.assert_retired();
}

#[tokio::test]
async fn driver_destruction_failure_does_not_drop_its_retained_foreign_cleanup() {
    let f = Fixture::new().await;
    let ack = f.hold(LifecyclePoint::ProviderEnter(
        ProviderOperation::AcknowledgeOrchestration,
    ));
    let foreign = MockForeign::default();
    let mut config = options();
    config.worker_concurrency = 0;
    let runtime = f.prepare(
        ActivityRegistry::builder().build(),
        foreign.registry(Turn::Continue, true, true, false),
        config,
    );
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client().start_orchestration("drop-fault", "flow", "").await.unwrap();
    entered(&ack).await;
    let now = Instant::now();
    runtime
        .request_shutdown_until(now, now + Duration::from_millis(100))
        .unwrap();
    ack.release();
    bounded(foreign.cleanup_entered.cancelled()).await;
    assert_eq!(
        Arc::clone(&runtime)
            .shutdown_with_timeouts(Duration::ZERO, Duration::from_millis(100))
            .await,
        Err(RuntimeShutdownError::TimedOut)
    );
    assert_eq!(f.hooks.cleanup_counts().active, 1);
    foreign.release.cancel();
    assert_eq!(
        bounded(runtime.wait_for_shutdown_completion()).await,
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    f.assert_retired();
    let history = f.inner.read("drop-fault").await.unwrap();
    assert!(
        !history
            .iter()
            .any(|event| matches!(event.kind, EventKind::ActivityCancelRequested { .. }))
    );
}

#[tokio::test]
async fn foreign_cleanup_error_is_an_operational_failure_not_success_or_raw_error_text() {
    let f = Fixture::new().await;
    let foreign = MockForeign::default();
    let mut config = options();
    config.worker_concurrency = 0;
    let runtime = f.prepare(
        ActivityRegistry::builder().build(),
        foreign.registry(Turn::Continue, true, false, true),
        config,
    );
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client()
        .start_orchestration("cleanup-error", "flow", "")
        .await
        .unwrap();
    bounded(foreign.cleanup_entered.cancelled()).await;
    let now = Instant::now();
    runtime
        .request_shutdown_until(now + Duration::from_secs(1), now + Duration::from_secs(2))
        .unwrap();
    foreign.release.cancel();
    let error = bounded(runtime.wait_for_shutdown_completion()).await.unwrap_err();
    assert_eq!(error, RuntimeShutdownError::Failed { quiescent: true });
    assert!(!format!("{error:?}: {error}").contains("sentinel"));
    f.assert_retired();
}

#[tokio::test]
async fn fault_in_foreign_retirement_hook_does_not_discard_registered_completion() {
    let f = Fixture::new().await;
    let ack = f.hold(LifecyclePoint::ProviderEnter(
        ProviderOperation::AcknowledgeOrchestration,
    ));
    let foreign = MockForeign::default();
    f.hooks.fail_once(LifecyclePoint::ForeignCleanup).unwrap();
    let mut config = options();
    config.worker_concurrency = 0;
    let runtime = f.prepare(
        ActivityRegistry::builder().build(),
        foreign.registry(Turn::Continue, true, false, false),
        config,
    );
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client().start_orchestration("hook-error", "flow", "").await.unwrap();
    entered(&ack).await;
    let now = Instant::now();
    runtime
        .request_shutdown_until(now, now + Duration::from_millis(100))
        .unwrap();
    ack.release();
    bounded(foreign.cleanup_entered.cancelled()).await;
    assert_eq!(
        Arc::clone(&runtime)
            .shutdown_with_timeouts(Duration::ZERO, Duration::from_millis(100))
            .await,
        Err(RuntimeShutdownError::TimedOut)
    );
    assert_eq!(f.hooks.cleanup_counts().active, 1);
    foreign.release.cancel();
    assert_eq!(
        bounded(runtime.wait_for_shutdown_completion()).await,
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    f.assert_retired();
}

#[tokio::test]
async fn panicking_foreign_completion_cannot_certify_quiescence_or_release_the_runtime() {
    let f = Fixture::new().await;
    let ack = f.hold(LifecyclePoint::ProviderEnter(
        ProviderOperation::AcknowledgeOrchestration,
    ));
    let completion_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    struct BrokenCompletion(Arc<std::sync::atomic::AtomicBool>);
    impl std::future::Future for BrokenCompletion {
        type Output = Result<(), String>;
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
            panic!("broken foreign completion observer");
        }
    }
    impl Drop for BrokenCompletion {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let external_started = CancellationToken::new();
    let external_release = CancellationToken::new();
    let external_finished = CancellationToken::new();
    let started = external_started.clone();
    let release = external_release.clone();
    let finished = external_finished.clone();
    let external = tokio::spawn(async move {
        started.cancel();
        release.cancelled().await;
        finished.cancel();
    });
    bounded(external_started.cancelled()).await;
    let callback_dropped = Arc::clone(&completion_dropped);
    let registry = OrchestrationRegistry::builder()
        .register("broken-completion", move |ctx: OrchestrationContext, _: String| {
            let dropped = Arc::clone(&callback_dropped);
            async move {
                ctx.register_invocation_cleanup(BrokenCompletion(dropped)).unwrap();
                ctx.schedule_wait("never").await;
                Ok(String::new())
            }
        })
        .build();
    let runtime = f.prepare(ActivityRegistry::builder().build(), registry, options());
    let lifetime = Arc::downgrade(&runtime);
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client()
        .start_orchestration("broken-completion", "broken-completion", "")
        .await
        .unwrap();
    entered(&ack).await;
    let now = Instant::now();
    runtime
        .request_shutdown_until(now, now + Duration::from_millis(100))
        .unwrap();
    ack.release();
    let error = Arc::clone(&runtime)
        .shutdown_with_timeouts(Duration::ZERO, Duration::from_millis(100))
        .await
        .unwrap_err();
    assert_eq!(error, RuntimeShutdownError::TimedOut);
    assert!(error.to_string().contains("terminate"));
    assert!(runtime.wait_for_shutdown_completion().now_or_never().is_none());
    assert!(!external_finished.is_cancelled());
    assert!(
        !completion_dropped.load(Ordering::SeqCst),
        "panicking observer's captures were discarded"
    );
    assert_eq!(f.hooks.cleanup_counts().active, 1);
    drop(runtime);
    tokio::task::yield_now().await;
    assert!(
        lifetime.upgrade().is_some(),
        "unproven foreign quiescence released the runtime"
    );
    external_release.cancel();
    bounded(external).await.unwrap();
    assert!(
        lifetime.upgrade().is_some(),
        "a broken completion cannot retroactively certify quiescence"
    );
    assert!(!completion_dropped.load(Ordering::SeqCst));
}

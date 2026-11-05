// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

#[allow(dead_code)]
#[path = "replay_engine/helpers.rs"]
mod replay_helpers;
#[path = "common/retirement_metrics.rs"]
mod retirement_metrics;
#[path = "common/runtime_lifecycle.rs"]
mod support;
use common::tracing_capture;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::replay_engine::TurnResult;
use duroxide::runtime::test_hooks::{LifecyclePoint, LifecycleTask, ProviderOperation};
use duroxide::runtime::{
    InvocationCleanupError, RuntimeShutdownError, RuntimeStopObserver, RuntimeStopStatus, ShutdownOutcome,
};
use duroxide::{Event, EventKind, OrchestrationContext, OrchestrationRegistry};
use retirement_metrics::retirement_gauge;
use support::{Fixture, SECRET, bounded, entered, options, until};
use tokio_util::sync::CancellationToken;

fn prepared_lifecycle_id(events: &Arc<Mutex<Vec<tracing_capture::CapturedEvent>>>) -> String {
    events
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|event| event.field("category").as_deref() == Some("runtime_prepared"))
        .unwrap()
        .field("lifecycle_id")
        .unwrap()
}

fn retirement_count(events: &Arc<Mutex<Vec<tracing_capture::CapturedEvent>>>, lifecycle_id: &str) -> Option<usize> {
    events
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|event| {
            event.target == "duroxide::runtime::lifecycle"
                && event.field("category").as_deref() == Some("foreign_retirement_count")
                && event.field("lifecycle_id").as_deref() == Some(lifecycle_id)
        })
        .map(|event| event.field("pending_foreign_retirements").unwrap().parse().unwrap())
}

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
    let now = Instant::now();
    runtime.request_shutdown();
    let mut shutdown = tokio::spawn(runtime.shutdown_until(now + grace));
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
        let ctx = foreign.contexts.lock().unwrap()[0].clone();
        assert_eq!(ctx.instance_id(), "turn");
        assert_eq!(ctx.execution_id(), 1);
        assert_eq!(
            ctx.register_invocation_cleanup(async { Ok(()) }),
            Err(InvocationCleanupError::Closed)
        );
        assert!(!shutdown.is_finished());
        if force {
            assert!(
                tokio::time::timeout(Duration::from_millis(300), &mut shutdown)
                    .await
                    .is_err()
            );
            assert!(now.elapsed() < Duration::from_secs(1));
            assert_eq!(f.hooks.cleanup_counts().active, 1);
        }
    }
    let before = f.inner.read_with_execution("turn", 1).await.unwrap();
    foreign.release.cancel();
    assert_eq!(
        bounded(shutdown).await.unwrap(),
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

async fn fault_before_parent_routing(stamped: bool, fail_driver: bool) -> bool {
    use duroxide::providers::{ExecutionMetadata, WorkItem};
    use duroxide::runtime::RuntimeStartError;
    use duroxide::runtime::test_hooks::LifecycleGate;

    struct ReleaseGates(Vec<LifecycleGate>, CancellationToken);
    impl Drop for ReleaseGates {
        fn drop(&mut self) {
            for gate in &self.0 {
                gate.release();
            }
            self.1.cancel();
        }
    }
    struct FaultingDriver {
        context: OrchestrationContext,
        drops: Arc<AtomicUsize>,
        fail: bool,
    }
    impl Drop for FaultingDriver {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            self.context.set_kv_value("after-snapshot", "excluded");
            assert!(!self.fail, "post-decision driver destruction");
        }
    }

    let (events, _capture) = tracing_capture::install_tracing_capture();
    let f = Fixture::new().await;
    for (instance, execution, is_child) in [("parent", 7, false), ("child", 1, true)] {
        let parent_instance = is_child.then(|| "parent".to_string());
        let parent_id = is_child.then_some(42);
        let parent_execution_id = (is_child && stamped).then_some(7);
        common::seed_history_turn(
            f.inner.as_ref(),
            WorkItem::StartOrchestration {
                instance: instance.to_string(),
                orchestration: "flow".to_string(),
                input: String::new(),
                version: Some("1.0.0".to_string()),
                parent_instance: parent_instance.clone(),
                parent_id,
                parent_execution_id,
                execution_id: execution,
            },
            execution,
            vec![Event::with_event_id(
                1,
                instance,
                execution,
                None,
                EventKind::OrchestrationStarted {
                    name: "flow".to_string(),
                    version: "1.0.0".to_string(),
                    input: String::new(),
                    parent_instance,
                    parent_id,
                    parent_execution_id,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            if is_child {
                vec![WorkItem::CancelInstance {
                    instance: instance.to_string(),
                    reason: "controlled cancellation".to_string(),
                }]
            } else {
                vec![]
            },
            ExecutionMetadata {
                orchestration_name: Some("flow".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
        )
        .await;
    }
    let read = f.hold(LifecyclePoint::ProviderEnter(ProviderOperation::Read));
    let force = f.hold(LifecyclePoint::BeforeForce);
    let committed = f.hold(LifecyclePoint::ProviderCommit(
        ProviderOperation::AcknowledgeOrchestration,
    ));
    let release = CancellationToken::new();
    let cleanup_entered = CancellationToken::new();
    let completed = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let finalized = Arc::new(AtomicUsize::new(0));
    let _release_on_exit = ReleaseGates(vec![read.clone(), force.clone(), committed.clone()], release.clone());
    let registry = {
        let release = release.clone();
        let entered = cleanup_entered.clone();
        let completed = Arc::clone(&completed);
        let drops = Arc::clone(&drops);
        let finalized = Arc::clone(&finalized);
        OrchestrationRegistry::builder()
            .register("flow", move |ctx: OrchestrationContext, _: String| {
                let release = release.clone();
                let entered = entered.clone();
                let completed = Arc::clone(&completed);
                let drops = Arc::clone(&drops);
                let finalized = Arc::clone(&finalized);
                async move {
                    let final_ctx = ctx.clone();
                    ctx.register_invocation_cleanup_with_finalization(
                        async move {
                            entered.cancel();
                            release.cancelled().await;
                            completed.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        },
                        move || {
                            finalized.fetch_add(1, Ordering::SeqCst);
                            final_ctx.set_kv_value("before-snapshot", "included");
                        },
                    )
                    .unwrap();
                    let _driver = FaultingDriver {
                        context: ctx.clone(),
                        drops,
                        fail: fail_driver,
                    };
                    Ok(ctx.schedule_wait("never").await)
                }
            })
            .build()
    };
    let runtime = f.prepare(ActivityRegistry::builder().build(), registry, options());
    bounded(Arc::clone(&runtime).start_execution()).await.unwrap();
    let fault_count = || {
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                event.field("category").as_deref() == Some("owned_execution_failed")
                    && event.field("task").as_deref() == Some("orchestration-driver")
                    && event.field("kind").as_deref() == Some("DestructionPanicked")
            })
            .count()
    };
    if stamped {
        bounded(cleanup_entered.cancelled()).await;
    } else {
        entered(&read).await;
        let fetches = f
            .hooks
            .hits(LifecyclePoint::ProviderEnter(ProviderOperation::FetchActivity));
        // Establish scheduler progress even on the no-fault and uncorrected paths.
        until(|| {
            fault_count() > 0
                || f.hooks
                    .hits(LifecyclePoint::ProviderEnter(ProviderOperation::FetchActivity))
                    >= fetches + 2
        })
        .await;
        assert_eq!(
            f.hooks.cleanup_counts().started,
            0,
            "the held turn still owns retirement"
        );
    }
    let early_fault = fault_count() > 0;
    assert_eq!(
        runtime.stop_status(),
        if fail_driver {
            RuntimeStopStatus::Faulted
        } else {
            RuntimeStopStatus::NotRequested
        }
    );
    let early_start = Arc::clone(&runtime).start_execution().await;
    assert_eq!(finalized.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.hooks.hits(LifecyclePoint::ProviderEnter(ProviderOperation::Read)),
        usize::from(!stamped)
    );

    read.release();
    bounded(cleanup_entered.cancelled()).await;
    entered(&committed).await;
    let history = f.inner.read("child").await.unwrap();
    assert_eq!(
        f.inner.get_kv_value("child", "before-snapshot").await.unwrap(),
        Some("included".to_string())
    );
    assert_eq!(f.inner.get_kv_value("child", "after-snapshot").await.unwrap(), None);
    let (route, token, _) = bounded(
        f.inner
            .fetch_orchestration_item(Duration::from_secs(1), Duration::ZERO, None),
    )
    .await
    .unwrap()
    .expect("committed child outcome has a parent notification");
    assert!(route.messages.iter().any(|item| matches!(
        item,
        WorkItem::SubOrchFailed {
            parent_execution_id: 7,
            parent_id: 42,
            ..
        }
    )));
    f.inner.abandon_orchestration_item(&token, None, true).await.unwrap();
    committed.release();
    force.release();
    let mut shutdown = tokio::spawn(Arc::clone(&runtime).shutdown_with_grace(Duration::ZERO));
    until(|| f.hooks.hits(LifecyclePoint::ForceRequested) > 0).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert_eq!(f.hooks.cleanup_counts().active, 1);
    release.cancel();
    let outcome = bounded(shutdown).await.unwrap();
    f.assert_retired();
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert_eq!(f.hooks.cleanup_counts().started, 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(finalized.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.inner.read("child").await.unwrap(),
        history,
        "retirement rewrote committed history"
    );
    assert_eq!(
        fault_count(),
        usize::from(fail_driver),
        "turn fault was reported again during retirement"
    );
    assert!(history.iter().any(|event| matches!(&event.kind,
        EventKind::OrchestrationFailed { details: duroxide::ErrorDetails::Application {
            kind: duroxide::AppErrorKind::Cancelled { reason }, ..
        }} if reason == "controlled cancellation"
    )));
    assert_eq!(
        outcome,
        if fail_driver {
            Err(RuntimeShutdownError::Failed { quiescent: true })
        } else {
            Ok(ShutdownOutcome::Forced)
        }
    );
    assert_eq!(
        early_start,
        Err(if early_fault {
            RuntimeStartError::ShutdownRequested
        } else {
            RuntimeStartError::AlreadyStarted
        })
    );
    early_fault
}

#[tokio::test]
async fn driver_fault_precedes_a_held_legacy_parent_read() {
    assert!(fault_before_parent_routing(false, true).await);
}

#[tokio::test]
async fn stamped_parent_routes_without_waiting_for_a_legacy_read() {
    assert!(fault_before_parent_routing(true, true).await);
}

#[tokio::test]
async fn legacy_parent_routing_without_a_fault_keeps_the_runtime_running() {
    assert!(!fault_before_parent_routing(false, false).await);
}

#[tokio::test]
async fn panicking_fault_diagnostics_preserve_the_decision_and_retained_cleanup() {
    use tracing_subscriber::prelude::*;

    struct PanicOnce {
        panicked: Arc<AtomicBool>,
        observed_fault: Arc<AtomicBool>,
        observer: RuntimeStopObserver,
    }
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PanicOnce {
        fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            if *event.metadata().level() == tracing::Level::ERROR && !self.panicked.swap(true, Ordering::SeqCst) {
                self.observed_fault
                    .store(self.observer.state() == RuntimeStopStatus::Faulted, Ordering::SeqCst);
                panic!("diagnostic failure after turn destruction");
            }
        }
    }

    let expected = without_creation_times(captured_turn(Turn::Continue, true, false).await);
    let f = Fixture::new().await;
    let foreign = MockForeign::default();
    let force = f.hold(LifecyclePoint::BeforeForce);
    let committed = f.hold(LifecyclePoint::ProviderCommit(
        ProviderOperation::AcknowledgeOrchestration,
    ));
    let mut config = options();
    config.worker_concurrency = 0;
    let runtime = f.prepare(
        ActivityRegistry::builder().build(),
        foreign.registry(Turn::Continue, true, true, false),
        config,
    );
    let panicked = Arc::new(AtomicBool::new(false));
    let observed_fault = Arc::new(AtomicBool::new(false));
    let _subscriber = tracing::subscriber::set_default(tracing_subscriber::registry().with(PanicOnce {
        panicked: Arc::clone(&panicked),
        observed_fault: Arc::clone(&observed_fault),
        observer: runtime.stop_observer(),
    }));
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client().start_orchestration("turn", "flow", "").await.unwrap();
    entered(&committed).await;
    entered(&force).await;
    bounded(foreign.cleanup_entered.cancelled()).await;
    assert!(panicked.load(Ordering::SeqCst));
    assert!(
        observed_fault.load(Ordering::SeqCst),
        "fault was not published before diagnostics"
    );
    assert_eq!(runtime.stop_status(), RuntimeStopStatus::Faulted);
    let history = f.inner.read("turn").await.unwrap();
    assert_eq!(without_creation_times(history.clone()), expected);
    committed.release();
    force.release();
    let mut shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::ZERO));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(f.hooks.cleanup_counts().active, 1);
    foreign.release.cancel();
    assert_eq!(
        bounded(shutdown).await.unwrap(),
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    assert_eq!(f.hooks.cleanup_counts().started, 1);
    assert_eq!(f.inner.read("turn").await.unwrap(), history);
    f.assert_retired();
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
    runtime.request_shutdown();
    let shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::from_secs(4)));
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
    assert!(!shutdown.is_finished());
    foreign.release.cancel();
    assert_eq!(bounded(shutdown).await.unwrap(), Ok(ShutdownOutcome::Drained));
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
    bounded(foreign.cleanup_entered.cancelled()).await;
    let mut shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::ZERO));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(f.hooks.cleanup_counts().active, 1);
    foreign.release.cancel();
    assert_eq!(
        bounded(shutdown).await.unwrap(),
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
    runtime.request_shutdown();
    let shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::from_secs(1)));
    foreign.release.cancel();
    let error = bounded(shutdown).await.unwrap().unwrap_err();
    assert_eq!(error, RuntimeShutdownError::Failed { quiescent: true });
    assert!(!format!("{error:?}: {error}").contains("sentinel"));
    f.assert_retired();
}

#[tokio::test]
async fn fault_in_foreign_retirement_hook_does_not_discard_registered_completion() {
    let f = Fixture::new().await;
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
    bounded(foreign.cleanup_entered.cancelled()).await;
    let mut shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::ZERO));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(f.hooks.cleanup_counts().active, 1);
    foreign.release.cancel();
    assert_eq!(
        bounded(shutdown).await.unwrap(),
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    f.assert_retired();
}

#[tokio::test]
async fn panicking_foreign_completion_cannot_certify_quiescence_or_release_the_runtime() {
    let (events, _capture) = tracing_capture::install_tracing_capture();
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let f = Fixture::new().await;
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
    let lifecycle_id = prepared_lifecycle_id(&events);
    let lifetime = Arc::downgrade(&runtime);
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client()
        .start_orchestration("broken-completion", "broken-completion", "")
        .await
        .unwrap();
    until(|| f.hooks.cleanup_counts().active == 1).await;
    let mut shutdown = tokio::spawn(Arc::clone(&runtime).shutdown_with_grace(Duration::ZERO));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err()
    );
    assert!(!shutdown.is_finished());
    assert!(!external_finished.is_cancelled());
    assert!(
        !completion_dropped.load(Ordering::SeqCst),
        "panicking observer's captures were discarded"
    );
    assert_eq!(f.hooks.cleanup_counts().active, 1);
    assert_eq!(retirement_count(&events, &lifecycle_id), Some(1));
    assert_eq!(retirement_gauge(&snapshotter), 1.0);
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
    assert_eq!(retirement_count(&events, &lifecycle_id), Some(1));
    assert_eq!(retirement_gauge(&snapshotter), 1.0);
}

#[tokio::test]
async fn foreign_retirement_count_tracks_independent_tails_until_success_or_error() {
    let (events, _capture) = tracing_capture::install_tracing_capture();
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let f = Fixture::new().await;
    let releases: [CancellationToken; 2] = std::array::from_fn(|_| CancellationToken::new());
    let handler_releases = releases.clone();
    let registry = OrchestrationRegistry::builder()
        .register("flow", move |ctx: OrchestrationContext, input: String| {
            let index: usize = input.parse().unwrap();
            let release = handler_releases[index].clone();
            async move {
                ctx.register_invocation_cleanup(async move {
                    release.cancelled().await;
                    if index == 1 { Err(SECRET.to_string()) } else { Ok(()) }
                })
                .unwrap();
                Ok("durably complete".to_string())
            }
        })
        .build();
    let runtime = f.prepare(ActivityRegistry::builder().build(), registry, options());
    let lifecycle_id = prepared_lifecycle_id(&events);
    Arc::clone(&runtime).start_execution().await.unwrap();
    for index in 0..2 {
        let instance = format!("retained-{index}");
        f.client()
            .start_orchestration(&instance, "flow", &index.to_string())
            .await
            .unwrap();
        assert!(matches!(
            bounded(f.client().wait_for_orchestration(&instance, Duration::from_secs(3)))
                .await
                .unwrap(),
            duroxide::OrchestrationStatus::Completed { .. }
        ));
    }
    until(|| f.hooks.cleanup_counts().active == 2).await;
    assert_eq!(retirement_count(&events, &lifecycle_id), Some(2));
    assert_eq!(retirement_gauge(&snapshotter), 2.0);
    assert_eq!(
        f.inner
            .as_management_capability()
            .unwrap()
            .get_system_metrics()
            .await
            .unwrap()
            .running_instances,
        0,
        "durable workload counts do not represent retained foreign work"
    );
    releases[0].cancel();
    until(|| f.hooks.cleanup_counts().completed == 1).await;
    assert_eq!(retirement_count(&events, &lifecycle_id), Some(1));
    assert_eq!(retirement_gauge(&snapshotter), 1.0);
    releases[1].cancel();
    assert_eq!(
        bounded(runtime.shutdown_with_grace(Duration::from_secs(1))).await,
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    assert_eq!(retirement_count(&events, &lifecycle_id), Some(0));
    assert_eq!(retirement_gauge(&snapshotter), 0.0);
    f.assert_retired();
    assert!(!format!("{:?}", events.lock().unwrap()).contains("sentinel"));
}

#[tokio::test]
async fn finalization_admits_scheduling_and_continue_as_new_to_the_first_snapshot() {
    for continue_as_new in [false, true] {
        let f = Fixture::new().await;
        let finalizations = Arc::new(AtomicUsize::new(0));
        let release = CancellationToken::new();
        let cleanup_entered = CancellationToken::new();
        let handler_release = release.clone();
        let handler_entered = cleanup_entered.clone();
        let handler_finalizations = Arc::clone(&finalizations);
        let registry = OrchestrationRegistry::builder()
            .register("flow", move |ctx: OrchestrationContext, _: String| {
                let final_ctx = ctx.clone();
                let release = handler_release.clone();
                let entered = handler_entered.clone();
                let finalizations = Arc::clone(&handler_finalizations);
                let admitted = Arc::new(Mutex::new(None));
                let held_activity = Arc::clone(&admitted);
                async move {
                    ctx.register_invocation_cleanup_with_finalization(
                        async move {
                            entered.cancel();
                            release.cancelled().await;
                            drop(held_activity.lock().unwrap().take());
                            Ok(())
                        },
                        move || {
                            finalizations.fetch_add(1, Ordering::SeqCst);
                            final_ctx.set_kv_value("finalization", "admitted");
                            final_ctx.set_custom_status("admitted-status");
                            if continue_as_new {
                                drop(final_ctx.continue_as_new("admitted-next"));
                            } else {
                                final_ctx.schedule_orchestration("child", "admitted-detached", "child-input");
                                *admitted.lock().unwrap() =
                                    Some(final_ctx.schedule_activity("admitted-work", "activity-input"));
                            }
                        },
                    )
                    .unwrap();
                    ctx.schedule_wait("pause").await;
                    Ok(String::new())
                }
            })
            .build();
        let before_ack = f.hold(LifecyclePoint::ProviderEnter(
            ProviderOperation::AcknowledgeOrchestration,
        ));
        let mut config = options();
        config.worker_concurrency = 0;
        let runtime = f.prepare(ActivityRegistry::builder().build(), registry, config);
        Arc::clone(&runtime).start_execution().await.unwrap();
        f.client()
            .start_orchestration("admitted-finalization", "flow", "")
            .await
            .unwrap();
        entered(&before_ack).await;
        runtime.request_shutdown();
        before_ack.release();
        until(|| {
            f.hooks.hits(LifecyclePoint::ProviderCommit(
                ProviderOperation::AcknowledgeOrchestration,
            )) == 1
        })
        .await;
        bounded(cleanup_entered.cancelled()).await;
        assert_eq!(finalizations.load(Ordering::SeqCst), 1);
        assert_eq!(
            f.inner
                .get_kv_value("admitted-finalization", "finalization")
                .await
                .unwrap(),
            Some("admitted".to_string())
        );
        let history = f.inner.read_with_execution("admitted-finalization", 1).await.unwrap();
        let history_text = serde_json::to_string(&history).unwrap();
        assert_eq!(history_text.contains("admitted-detached"), !continue_as_new);
        assert!(history_text.contains("admitted-status"));
        assert_eq!(
            history.iter().any(|event| matches!(&event.kind,
                EventKind::OrchestrationContinuedAsNew { input } if input == "admitted-next")),
            continue_as_new
        );
        let depths = f
            .inner
            .as_management_capability()
            .unwrap()
            .get_queue_depths()
            .await
            .unwrap();
        assert_eq!(depths.worker_queue, usize::from(!continue_as_new));
        assert_eq!(depths.orchestrator_queue, 1);
        if !continue_as_new {
            assert!(history.iter().any(|event| matches!(&event.kind,
                EventKind::ActivityScheduled { name, input, .. }
                    if name == "admitted-work" && input == "activity-input")));
        }
        release.cancel();
        assert_eq!(
            bounded(runtime.shutdown_with_grace(Duration::from_secs(1))).await,
            Ok(ShutdownOutcome::Drained)
        );
        assert_eq!(
            f.inner.read_with_execution("admitted-finalization", 1).await.unwrap(),
            history
        );
        f.assert_retired();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FinalizationReplay {
    FinalizerOnly,
    ActivityOnlyFinalizer,
    NormalReplay,
    ReplayAdmittedBatch,
}

async fn finalization_over_two_turns(protocol: FinalizationReplay) {
    use duroxide::{ConfigErrorKind, ErrorDetails, OrchestrationStatus};

    let f = Fixture::new().await;
    let release = CancellationToken::new();
    let _release_on_exit = release.clone().drop_guard();
    let finalizations = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(AtomicUsize::new(0));
    let activity_calls = Arc::new(AtomicUsize::new(0));
    let child_calls = Arc::new(AtomicUsize::new(0));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let replay_flags = Arc::new(Mutex::new(Vec::new()));
    let include_child = protocol != FinalizationReplay::ActivityOnlyFinalizer;
    let activities = {
        let calls = Arc::clone(&activity_calls);
        ActivityRegistry::builder()
            .register("admitted-work", move |_, input: String| {
                let calls = Arc::clone(&calls);
                async move {
                    assert_eq!(input, "activity-input");
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok("activity-output".to_string())
                }
            })
            .build()
    };
    let registry = {
        let release = release.clone();
        let finalizations = Arc::clone(&finalizations);
        let invocations = Arc::clone(&invocations);
        let contexts = Arc::clone(&contexts);
        let replay_flags = Arc::clone(&replay_flags);
        let child_calls = Arc::clone(&child_calls);
        OrchestrationRegistry::builder()
            .register("flow", move |ctx: OrchestrationContext, _: String| {
                let final_ctx = ctx.clone();
                let release = release.clone();
                let finalizations = Arc::clone(&finalizations);
                let invocations = Arc::clone(&invocations);
                let contexts = Arc::clone(&contexts);
                let replay_flags = Arc::clone(&replay_flags);
                let admitted = Arc::new(Mutex::new(None));
                let held_activity = Arc::clone(&admitted);
                let replay_activity = Arc::clone(&admitted);
                let pending_batch = Arc::new(AtomicBool::new(false));
                let final_pending_batch = Arc::clone(&pending_batch);
                let dropped = CancellationToken::new();
                let cleanup_dropped = dropped.clone();
                async move {
                    ctx.register_invocation_cleanup_with_finalization(
                        async move {
                            assert!(cleanup_dropped.is_cancelled());
                            release.cancelled().await;
                            drop(held_activity.lock().unwrap().take());
                            Ok(())
                        },
                        move || {
                            finalizations.fetch_add(1, Ordering::SeqCst);
                            if matches!(
                                protocol,
                                FinalizationReplay::FinalizerOnly | FinalizationReplay::ActivityOnlyFinalizer
                            ) || final_pending_batch.load(Ordering::SeqCst)
                            {
                                final_ctx.set_kv_value("finalization", "admitted");
                                final_ctx.set_custom_status("admitted-status");
                                if include_child {
                                    final_ctx.schedule_orchestration("child", "admitted-detached", "child-input");
                                }
                                *admitted.lock().unwrap() =
                                    Some(final_ctx.schedule_activity("admitted-work", "activity-input"));
                            }
                        },
                    )
                    .unwrap();
                    let _driver = DriverDrop { dropped, panic: false };
                    invocations.fetch_add(1, Ordering::SeqCst);
                    contexts.lock().unwrap().push(ctx.clone());
                    if protocol == FinalizationReplay::NormalReplay {
                        replay_flags.lock().unwrap().push(ctx.is_replaying());
                        ctx.set_kv_value("finalization", "admitted");
                        ctx.set_custom_status("admitted-status");
                        ctx.schedule_orchestration("child", "admitted-detached", "child-input");
                        ctx.schedule_activity("admitted-work", "activity-input").await
                    } else {
                        let replaying = ctx.is_replaying();
                        let pause = ctx.schedule_wait("pause");
                        if protocol == FinalizationReplay::ReplayAdmittedBatch {
                            replay_flags.lock().unwrap().push(replaying);
                            if replaying {
                                ctx.set_kv_value("finalization", "admitted");
                                ctx.set_custom_status("admitted-status");
                                ctx.schedule_orchestration("child", "admitted-detached", "child-input");
                                *replay_activity.lock().unwrap() =
                                    Some(ctx.schedule_activity("admitted-work", "activity-input"));
                            } else {
                                pending_batch.store(true, Ordering::SeqCst);
                            }
                        }
                        pause.await;
                        Ok(String::new())
                    }
                }
            })
            .register("child", move |_, input: String| {
                let calls = Arc::clone(&child_calls);
                async move {
                    assert_eq!(input, "child-input");
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok("child-output".to_string())
                }
            })
            .build()
    };
    let worker = f.hold(LifecyclePoint::ProviderEnter(ProviderOperation::FetchActivity));
    let runtime = f.prepare(activities, registry, options());
    Arc::clone(&runtime).start_execution().await.unwrap();
    entered(&worker).await;
    f.client().start_orchestration("two-turns", "flow", "").await.unwrap();
    until(|| {
        f.hooks.cleanup_counts().active == 1
            && f.hooks.hits(LifecyclePoint::ProviderCommit(
                ProviderOperation::AcknowledgeOrchestration,
            )) >= 1
    })
    .await;
    let first = f.inner.read("two-turns").await.unwrap();
    assert!(matches!(
        f.client().get_orchestration_status("two-turns").await.unwrap(),
        OrchestrationStatus::Running { .. }
    ));
    assert_eq!(finalizations.load(Ordering::SeqCst), 1);
    assert_eq!(activity_calls.load(Ordering::SeqCst), 0);
    let scheduled: Vec<_> = first
        .iter()
        .filter(|event| {
            matches!(&event.kind,
                EventKind::ActivityScheduled { name, input, .. } if name == "admitted-work" && input == "activity-input"
            )
        })
        .collect();
    assert_eq!(scheduled.len(), 1);
    let activity_id = scheduled[0].event_id;
    if include_child {
        assert!(matches!(
            bounded(f.client().wait_for_orchestration("admitted-detached", Duration::from_secs(3))).await.unwrap(),
            OrchestrationStatus::Completed { output, .. } if output == "child-output"
        ));
    }
    worker.release();
    let second = bounded(async {
        loop {
            let history = f.inner.read("two-turns").await.unwrap();
            if history
                .iter()
                .any(|event| matches!(event.kind, EventKind::ActivityCompleted { .. }))
            {
                break history;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let status = f.client().get_orchestration_status("two-turns").await.unwrap();
    until(|| f.hooks.cleanup_counts().active == 2).await;
    assert_eq!(runtime.stop_status(), RuntimeStopStatus::NotRequested);
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    assert_eq!(finalizations.load(Ordering::SeqCst), 2);
    assert_eq!(activity_calls.load(Ordering::SeqCst), 1);
    assert_eq!(child_calls.load(Ordering::SeqCst), usize::from(include_child));
    assert_eq!(&second[..first.len()], &first);
    assert_eq!(
        second
            .iter()
            .filter(|event| matches!(event.kind, EventKind::ActivityScheduled { .. }))
            .count(),
        1
    );
    assert_eq!(
        second
            .iter()
            .filter(|event| matches!(event.kind, EventKind::OrchestrationChained { .. }))
            .count(),
        usize::from(include_child)
    );
    for ctx in contexts.lock().unwrap().iter() {
        assert_eq!(
            ctx.register_invocation_cleanup(async { Ok(()) }),
            Err(InvocationCleanupError::Closed)
        );
    }
    let delta = &second[first.len()..];
    assert!(matches!(&delta[0].kind, EventKind::ActivityCompleted { result } if result == "activity-output"));
    assert_eq!(delta[0].source_event_id, Some(activity_id));
    match protocol {
        FinalizationReplay::NormalReplay => {
            assert_eq!(delta.len(), 2);
            assert!(matches!(status, OrchestrationStatus::Completed { output, .. } if output == "activity-output"));
            assert!(
                matches!(&delta[1].kind, EventKind::OrchestrationCompleted { output } if output == "activity-output")
            );
            assert_eq!(*replay_flags.lock().unwrap(), [false, true]);
        }
        FinalizationReplay::ReplayAdmittedBatch => {
            assert_eq!(delta.len(), 1);
            assert!(matches!(status, OrchestrationStatus::Running { .. }));
            assert_eq!(*replay_flags.lock().unwrap(), [false, true]);
        }
        FinalizationReplay::FinalizerOnly | FinalizationReplay::ActivityOnlyFinalizer => {
            let expected = ErrorDetails::Configuration {
                kind: ConfigErrorKind::Nondeterminism,
                resource: String::new(),
                message: Some(
                    if protocol == FinalizationReplay::FinalizerOnly {
                        "history OrchestrationChained but no emitted action"
                    } else {
                        "history schedule but no emitted action"
                    }
                    .to_string(),
                ),
            };
            assert_eq!(delta.len(), 3);
            assert!(matches!(delta[1].kind, EventKind::ExternalSubscribedCancelled { .. }));
            assert_eq!(delta[1].source_event_id, Some(2));
            assert!(matches!(status, OrchestrationStatus::Failed { details, .. } if details == expected));
            assert_eq!(delta[2].kind, EventKind::OrchestrationFailed { details: expected });
        }
    }
    let mut shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::from_secs(1)));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(f.hooks.cleanup_counts().completed, 0);
    release.cancel();
    assert_eq!(bounded(shutdown).await.unwrap(), Ok(ShutdownOutcome::Drained));
    assert_eq!(f.hooks.cleanup_counts().completed, 2);
    assert_eq!(f.hooks.hits(LifecyclePoint::ForceRequested), 0);
    assert_eq!(f.inner.read("two-turns").await.unwrap(), second);
    f.assert_retired();
}

#[tokio::test]
async fn finalizer_only_detached_start_fails_normal_replay_locally() {
    finalization_over_two_turns(FinalizationReplay::FinalizerOnly).await;
}

#[tokio::test]
async fn finalizer_only_activity_fails_normal_replay_locally() {
    finalization_over_two_turns(FinalizationReplay::ActivityOnlyFinalizer).await;
}

#[tokio::test]
async fn normal_replay_completes_two_turns_with_independently_retained_cleanup() {
    finalization_over_two_turns(FinalizationReplay::NormalReplay).await;
}

#[tokio::test]
async fn normal_replay_reproduces_the_batch_admitted_by_first_turn_finalization() {
    finalization_over_two_turns(FinalizationReplay::ReplayAdmittedBatch).await;
}

#[tokio::test]
async fn pre_snapshot_finalization_faults_reject_new_effects_and_preserve_committed_history() {
    struct FinalizationCapture {
        advanced: Arc<AtomicBool>,
        fail: bool,
    }
    impl Drop for FinalizationCapture {
        fn drop(&mut self) {
            if self.fail && self.advanced.load(Ordering::SeqCst) {
                panic!("{SECRET}");
            }
        }
    }
    for (fail_capture, early_application_panic) in [(false, false), (true, false), (false, true)] {
        let (events, _capture) = tracing_capture::install_tracing_capture();
        let f = Fixture::new().await;
        let release = CancellationToken::new();
        let cleanup_entered = CancellationToken::new();
        let finalizations = Arc::new(AtomicUsize::new(0));
        let handler_release = release.clone();
        let handler_entered = cleanup_entered.clone();
        let handler_finalizations = Arc::clone(&finalizations);
        let registry = OrchestrationRegistry::builder()
            .register("flow", move |ctx: OrchestrationContext, _: String| {
                let advanced = Arc::new(AtomicBool::new(false));
                let cleanup_advanced = Arc::clone(&advanced);
                let final_advanced = Arc::clone(&advanced);
                let final_ctx = ctx.clone();
                let release = handler_release.clone();
                let entered = handler_entered.clone();
                let finalizations = Arc::clone(&handler_finalizations);
                let capture = FinalizationCapture {
                    advanced: Arc::clone(&advanced),
                    fail: fail_capture,
                };
                async move {
                    ctx.register_invocation_cleanup_with_finalization(
                        async move {
                            if cleanup_advanced.load(Ordering::SeqCst) {
                                entered.cancel();
                                release.cancelled().await;
                            }
                            Ok(())
                        },
                        move || {
                            std::hint::black_box(&capture);
                            finalizations.fetch_add(1, Ordering::SeqCst);
                            if final_advanced.load(Ordering::SeqCst) {
                                final_ctx.set_kv_value("from-finalizer", "must-not-persist");
                                final_ctx.set_custom_status("must-not-persist");
                                final_ctx.schedule_orchestration("child", "rejected-finalizer-start", "");
                                drop(final_ctx.continue_as_new("must-not-persist"));
                                assert!(fail_capture, "{SECRET}");
                            }
                        },
                    )
                    .unwrap();
                    ctx.set_kv_value("previous", "committed");
                    ctx.schedule_wait("advance").await;
                    advanced.store(true, Ordering::SeqCst);
                    ctx.set_kv_value("before-finalizer", "must-not-persist");
                    ctx.schedule_orchestration("child", "rejected-before-start", "");
                    assert!(
                        !early_application_panic,
                        "ordinary application failure before finalization"
                    );
                    ctx.schedule_activity("new-work", "must-not-persist").await
                }
            })
            .build();
        let mut config = options();
        config.worker_concurrency = 0;
        let runtime = f.prepare(ActivityRegistry::builder().build(), registry, config);
        let lifecycle_id = prepared_lifecycle_id(&events);
        Arc::clone(&runtime).start_execution().await.unwrap();
        f.client()
            .start_orchestration("failed-finalization", "flow", "")
            .await
            .unwrap();
        until(|| {
            f.hooks.hits(LifecyclePoint::ProviderCommit(
                ProviderOperation::AcknowledgeOrchestration,
            )) == 1
                && f.hooks.cleanup_counts().completed == 1
        })
        .await;
        let before = f.inner.read("failed-finalization").await.unwrap();
        assert!(!before.is_empty());
        assert_eq!(
            f.inner.get_kv_value("failed-finalization", "previous").await.unwrap(),
            Some("committed".to_string())
        );
        f.client()
            .raise_event("failed-finalization", "advance", "go")
            .await
            .unwrap();
        bounded(cleanup_entered.cancelled()).await;
        until(|| f.hooks.hits(LifecyclePoint::ForceRequested) == 1).await;
        assert_eq!(finalizations.load(Ordering::SeqCst), 2);
        for point in [
            LifecyclePoint::ProviderEnter(ProviderOperation::AcknowledgeOrchestration),
            LifecyclePoint::ProviderCommit(ProviderOperation::AcknowledgeOrchestration),
        ] {
            assert_eq!(
                f.hooks.hits(point),
                1,
                "the failed attempt must never reach acknowledgement"
            );
        }
        assert_eq!(f.inner.read("failed-finalization").await.unwrap(), before);
        for key in ["before-finalizer", "from-finalizer"] {
            assert_eq!(f.inner.get_kv_value("failed-finalization", key).await.unwrap(), None);
        }
        for instance in ["rejected-before-start", "rejected-finalizer-start"] {
            assert!(matches!(
                f.client().get_orchestration_status(instance).await.unwrap(),
                duroxide::OrchestrationStatus::NotFound
            ));
        }
        assert_eq!(
            f.inner
                .as_management_capability()
                .unwrap()
                .get_queue_depths()
                .await
                .unwrap()
                .worker_queue,
            0
        );
        assert_eq!(retirement_count(&events, &lifecycle_id), Some(1));
        let mut shutdown = tokio::spawn(Arc::clone(&runtime).shutdown_with_grace(Duration::ZERO));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
                .await
                .is_err()
        );
        release.cancel();
        assert_eq!(
            bounded(shutdown).await.unwrap(),
            Err(RuntimeShutdownError::Failed { quiescent: true })
        );
        assert_eq!(
            runtime.start_execution().await,
            Err(duroxide::runtime::RuntimeStartError::ShutdownRequested)
        );
        assert_eq!(f.inner.read("failed-finalization").await.unwrap(), before);
        assert_eq!(retirement_count(&events, &lifecycle_id), Some(0));
        f.assert_retired();
        let captured = events.lock().unwrap();
        assert!(captured.iter().any(|event| {
            event.field("lifecycle_id").as_deref() == Some(lifecycle_id.as_str())
                && event.field("task").as_deref() == Some("invocation-finalization")
                && event.field("kind").as_deref()
                    == Some(if fail_capture {
                        "DestructionPanicked"
                    } else {
                        "PollPanicked"
                    })
        }));
        assert!(!format!("{captured:?}").contains("sentinel"));
    }
}

#[tokio::test]
async fn post_decision_cleanup_failure_cannot_undo_an_acknowledged_turn() {
    let f = Fixture::new().await;
    let foreign = MockForeign::default();
    let committed = f.hold(LifecyclePoint::ProviderCommit(
        ProviderOperation::AcknowledgeOrchestration,
    ));
    let runtime = f.prepare(
        ActivityRegistry::builder().build(),
        foreign.registry(Turn::Complete, true, false, true),
        options(),
    );
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client()
        .start_orchestration("committed-before-error", "flow", "")
        .await
        .unwrap();
    entered(&committed).await;
    bounded(foreign.cleanup_entered.cancelled()).await;
    let before = f.inner.read("committed-before-error").await.unwrap();
    assert!(
        before
            .iter()
            .any(|event| matches!(event.kind, EventKind::OrchestrationCompleted { .. }))
    );
    foreign.release.cancel();
    let shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::ZERO));
    assert_eq!(
        bounded(shutdown).await.unwrap(),
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    committed.release();
    assert_eq!(f.inner.read("committed-before-error").await.unwrap(), before);
    assert_eq!(
        f.hooks.hits(LifecyclePoint::ProviderCommit(
            ProviderOperation::AcknowledgeOrchestration
        )),
        1
    );
    f.assert_retired();
}

#[tokio::test]
async fn outer_engine_decision_panic_finalizes_and_destroys_before_propagation_with_foreign_work_retained() {
    type Order = Arc<Mutex<Vec<(&'static str, bool)>>>;
    struct DecisionDriver(Order);
    impl Drop for DecisionDriver {
        fn drop(&mut self) {
            self.0.lock().unwrap().push(("driver-drop", std::thread::panicking()));
        }
    }
    let (events, _capture) = tracing_capture::install_tracing_capture();
    let f = Fixture::new().await;
    f.hooks
        .fail_once_with_message(LifecyclePoint::BeforeTurnDecision, SECRET)
        .unwrap();
    let order: Order = Arc::new(Mutex::new(Vec::new()));
    let cleanup_entered = CancellationToken::new();
    let release = CancellationToken::new();
    let handler_order = Arc::clone(&order);
    let handler_entered = cleanup_entered.clone();
    let handler_release = release.clone();
    let registry = OrchestrationRegistry::builder()
        .register("flow", move |ctx: OrchestrationContext, _: String| {
            let order = Arc::clone(&handler_order);
            let final_order = Arc::clone(&order);
            let cleanup_order = Arc::clone(&order);
            let entered = handler_entered.clone();
            let release = handler_release.clone();
            let final_ctx = ctx.clone();
            async move {
                ctx.register_invocation_cleanup_with_finalization(
                    async move {
                        assert_eq!(
                            *cleanup_order.lock().unwrap(),
                            [("finalization", false), ("driver-drop", false),]
                        );
                        entered.cancel();
                        release.cancelled().await;
                        cleanup_order.lock().unwrap().push(("completion", false));
                        Ok(())
                    },
                    move || {
                        final_order
                            .lock()
                            .unwrap()
                            .push(("finalization", std::thread::panicking()));
                        final_ctx.set_kv_value("finalization-only", "must-not-persist");
                    },
                )
                .unwrap();
                let _driver = DecisionDriver(order);
                ctx.set_kv_value("before-decision", "must-not-persist");
                ctx.schedule_orchestration("child", "outer-panic-detached", "");
                ctx.schedule_activity("new-work", "must-not-persist").await
            }
        })
        .build();
    let runtime = f.prepare(ActivityRegistry::builder().build(), registry, options());
    let lifecycle_id = prepared_lifecycle_id(&events);
    Arc::clone(&runtime).start_execution().await.unwrap();
    f.client()
        .start_orchestration("outer-decision-panic", "flow", "")
        .await
        .unwrap();
    bounded(cleanup_entered.cancelled()).await;
    until(|| f.hooks.hits(LifecyclePoint::ForceRequested) == 1).await;
    assert_eq!(f.hooks.hits(LifecyclePoint::BeforeTurnDecision), 1);
    assert_eq!(
        *order.lock().unwrap(),
        [("finalization", false), ("driver-drop", false),]
    );
    assert!(
        events.lock().unwrap().iter().any(|event| {
            event.field("lifecycle_id").as_deref() == Some(lifecycle_id.as_str())
                && event.level == tracing::Level::ERROR
                && event.message.contains("failed")
                && event.field("category").as_deref() == Some("owned_execution_failed")
                && event.field("task").as_deref() == Some("orchestration-slot")
                && event.field("kind").as_deref() == Some("PollPanicked")
        }),
        "outer engine panic did not propagate to the tracked task boundary"
    );
    for point in [
        LifecyclePoint::ProviderEnter(ProviderOperation::AcknowledgeOrchestration),
        LifecyclePoint::ProviderCommit(ProviderOperation::AcknowledgeOrchestration),
    ] {
        assert_eq!(f.hooks.hits(point), 0);
    }
    assert!(f.inner.read("outer-decision-panic").await.unwrap().is_empty());
    for key in ["before-decision", "finalization-only"] {
        assert_eq!(f.inner.get_kv_value("outer-decision-panic", key).await.unwrap(), None);
    }
    assert!(matches!(
        f.client()
            .get_orchestration_status("outer-panic-detached")
            .await
            .unwrap(),
        duroxide::OrchestrationStatus::NotFound
    ));
    assert_eq!(f.hooks.cleanup_counts().active, 1);
    assert_eq!(retirement_count(&events, &lifecycle_id), Some(1));
    let mut shutdown = tokio::spawn(runtime.shutdown_with_grace(Duration::ZERO));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err()
    );
    release.cancel();
    assert_eq!(
        bounded(shutdown).await.unwrap(),
        Err(RuntimeShutdownError::Failed { quiescent: true })
    );
    assert_eq!(
        *order.lock().unwrap(),
        [("finalization", false), ("driver-drop", false), ("completion", false),]
    );
    assert_eq!(retirement_count(&events, &lifecycle_id), Some(0));
    f.assert_retired();
    assert!(!format!("{:?}", events.lock().unwrap()).contains("sentinel"));
}

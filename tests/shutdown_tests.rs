// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Shutdown must own and stop pollers and their descendants, including when
//! provider calls are blocked or a shutdown caller is cancelled.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::providers::{Provider, TagFilter};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{Runtime, RuntimeOptions};
use duroxide::{ActivityContext, Client, EventKind, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

mod common;

use common::long_polling::{LongPollingSqliteProvider, ProviderGate, ProviderOperation};

fn options() -> RuntimeOptions {
    RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        orchestrator_lock_timeout: Duration::from_secs(3),
        worker_lock_timeout: Duration::from_secs(3),
        dispatcher_long_poll_timeout: Duration::from_secs(30),
        ..Default::default()
    }
}

fn orchestrations() -> OrchestrationRegistry {
    OrchestrationRegistry::builder()
        .register("ActivityOrch", |ctx: OrchestrationContext, _: String| async move {
            ctx.schedule_activity("Activity", "").await
        })
        .register("ImmediateOrch", |_: OrchestrationContext, _: String| async move {
            Ok("done".to_string())
        })
        .build()
}

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("timed out waiting for shutdown test synchronization")
}

async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

async fn wait_for_fetches(sentinel: &Arc<()>, pollers: usize) {
    within(async {
        // The provider and this test each own one reference in addition to fetches.
        while Arc::strong_count(sentinel) != pollers + 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

#[derive(Default)]
struct ActivityProbe {
    entered: CancellationToken,
    release: CancellationToken,
    active: AtomicBool,
    attempts: AtomicUsize,
    completed: AtomicUsize,
}

struct ActiveActivity(Arc<ActivityProbe>);

impl Drop for ActiveActivity {
    fn drop(&mut self) {
        self.0.active.store(false, Ordering::SeqCst);
    }
}

impl ActivityProbe {
    fn registry(self: &Arc<Self>) -> ActivityRegistry {
        ActivityRegistry::builder()
            .register("Activity", {
                let probe = Arc::clone(self);
                move |_: ActivityContext, _: String| {
                    let probe = Arc::clone(&probe);
                    async move {
                        let attempt = probe.attempts.fetch_add(1, Ordering::SeqCst);
                        assert!(
                            !probe.active.swap(true, Ordering::SeqCst),
                            "activity overlapped its retry"
                        );
                        let _active = ActiveActivity(Arc::clone(&probe));
                        probe.entered.cancel();
                        if attempt == 0 {
                            probe.release.cancelled().await;
                        }
                        probe.completed.fetch_add(1, Ordering::SeqCst);
                        Ok("done".to_string())
                    }
                }
            })
            .build()
    }
}

async fn start_case(
    store: Arc<dyn Provider>,
    probe: &Arc<ActivityProbe>,
    options: RuntimeOptions,
    orchestration: &str,
) -> Arc<Runtime> {
    let rt = Runtime::start_with_options(store.clone(), probe.registry(), orchestrations(), options).await;
    Client::new(store)
        .start_orchestration("shutdown-instance", orchestration, "")
        .await
        .unwrap();
    rt
}

async fn recover(store: Arc<dyn Provider>, probe: &Arc<ActivityProbe>, options: RuntimeOptions) {
    let rt = Runtime::start_with_options(store.clone(), probe.registry(), orchestrations(), options).await;
    let status = Client::new(store)
        .wait_for_orchestration("shutdown-instance", Duration::from_secs(10))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { output, .. } if output == "done"));
    within(rt.shutdown(None)).await;
}

async fn shutdown_long_pollers(orch: usize, worker: usize, grace: Option<u64>) {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let provider = Arc::new(LongPollingSqliteProvider::new(store));
    let sentinel = provider.sentinel();
    let rt = Runtime::start_with_options(
        provider,
        ActivityRegistry::builder().build(),
        orchestrations(),
        RuntimeOptions {
            orchestration_concurrency: orch,
            worker_concurrency: worker,
            ..options()
        },
    )
    .await;
    wait_for_fetches(&sentinel, orch + worker).await;

    within(rt.shutdown(grace)).await;

    assert_eq!(
        Arc::strong_count(&sentinel),
        1,
        "fetches must be dropped before shutdown returns"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_stops_pollers_parked_in_long_poll() {
    shutdown_long_pollers(2, 2, Some(2_000)).await;
}

#[tokio::test]
async fn shutdown_zero_aborts_immediately() {
    shutdown_long_pollers(2, 2, Some(0)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_works_in_single_threaded_runtime() {
    shutdown_long_pollers(1, 1, None).await;
}

#[tokio::test]
async fn shutdown_returns_as_soon_as_tasks_drain() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let rt = Runtime::start_with_options(store, ActivityRegistry::builder().build(), orchestrations(), options()).await;

    let start = Instant::now();
    let shutdown: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(rt.shutdown(Some(10_000)));
    within(shutdown).await;
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "shutdown used its grace period as a sleep"
    );
}

#[tokio::test]
async fn shutdown_is_idempotent() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let rt = Runtime::start_with_options(store, ActivityRegistry::builder().build(), orchestrations(), options()).await;

    within(rt.clone().shutdown(None)).await;
    within(rt.shutdown(None)).await;
}

#[tokio::test]
async fn shutdown_can_resume_after_its_caller_is_cancelled() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let probe = Arc::new(ActivityProbe::default());
    let rt = start_case(store.clone(), &probe, options(), "ActivityOrch").await;
    within(probe.entered.cancelled()).await;

    let mut first = Box::pin(rt.clone().shutdown(Some(10_000)));
    assert!(poll_once(first.as_mut()).await.is_pending());
    drop(first);

    within(rt.shutdown(Some(0))).await;
    assert!(!probe.active.load(Ordering::SeqCst));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "runtime descendants retained the provider"
    );
}

#[tokio::test]
async fn concurrent_shutdown_calls_both_wait_for_active_work() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let probe = Arc::new(ActivityProbe::default());
    let rt = start_case(store.clone(), &probe, options(), "ActivityOrch").await;
    within(probe.entered.cancelled()).await;

    let mut first = Box::pin(rt.clone().shutdown(Some(10_000)));
    let mut second = Box::pin(rt.shutdown(Some(10_000)));
    assert!(poll_once(first.as_mut()).await.is_pending());
    assert!(poll_once(second.as_mut()).await.is_pending());

    probe.release.cancel();
    within(async { tokio::join!(first, second) }).await;
    assert_eq!(probe.completed.load(Ordering::SeqCst), 1);
    assert!(!probe.active.load(Ordering::SeqCst));
    assert_eq!(Arc::strong_count(&store), 1);
}

#[tokio::test]
async fn concurrent_immediate_shutdown_can_end_an_existing_grace_period() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let probe = Arc::new(ActivityProbe::default());
    let rt = start_case(store.clone(), &probe, options(), "ActivityOrch").await;
    within(probe.entered.cancelled()).await;

    let mut graceful = Box::pin(rt.clone().shutdown(Some(10_000)));
    assert!(poll_once(graceful.as_mut()).await.is_pending());
    within(rt.shutdown(Some(0))).await;
    within(graceful).await;

    assert!(!probe.active.load(Ordering::SeqCst));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    assert_eq!(Arc::strong_count(&store), 1);
}

async fn abort_activity_and_recover(grace: u64) {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let probe = Arc::new(ActivityProbe::default());
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_millis(500),
        ..options()
    };
    let rt = start_case(store.clone(), &probe, options.clone(), "ActivityOrch").await;
    within(probe.entered.cancelled()).await;

    within(rt.shutdown(Some(grace))).await;
    assert!(!probe.active.load(Ordering::SeqCst));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    assert_eq!(Arc::strong_count(&store), 1);

    recover(store, &probe, options).await;
    assert_eq!(probe.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(probe.completed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_zero_stops_activity_and_renewal_before_returning() {
    abort_activity_and_recover(0).await;
}

#[tokio::test]
async fn shutdown_deadline_stops_activity_and_allows_redelivery() {
    abort_activity_and_recover(25).await;
}

#[tokio::test]
async fn shutdown_renews_work_lock_through_graceful_acknowledgement() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let ack = Arc::new(ProviderGate::default());
    let provider = Arc::new(
        LongPollingSqliteProvider::new(store.clone()).with_gate(ProviderOperation::BeforeWorkAck, ack.clone()),
    );
    let probe = Arc::new(ActivityProbe::default());
    probe.release.cancel();
    let opts = options();
    let lock_timeout = opts.worker_lock_timeout;
    let rt = start_case(provider.clone(), &probe, opts, "ActivityOrch").await;
    within(ack.wait_until_entered()).await;

    let mut shutdown = Box::pin(rt.shutdown(Some(10_000)));
    assert!(poll_once(shutdown.as_mut()).await.is_pending());
    tokio::time::sleep(lock_timeout + Duration::from_millis(200)).await;
    assert!(
        store
            .fetch_work_item(Duration::from_secs(3), Duration::ZERO, None, &TagFilter::DefaultOnly)
            .await
            .unwrap()
            .is_none(),
        "another worker could steal work while its acknowledgement was pending"
    );

    ack.open();
    within(shutdown).await;
    assert_eq!(Arc::strong_count(&provider), 1);
    recover(store, &probe, options()).await;
    assert_eq!(probe.attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn graceful_shutdown_waits_for_activity_manager_cleanup() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let renewal = Arc::new(ProviderGate::default());
    let provider = Arc::new(
        LongPollingSqliteProvider::new(store).with_gate(ProviderOperation::BeforeWorkRenewal, renewal.clone()),
    );
    let probe = Arc::new(ActivityProbe::default());
    let rt = start_case(provider.clone(), &probe, options(), "ActivityOrch").await;
    within(renewal.wait_until_entered()).await;

    let mut shutdown = Box::pin(rt.shutdown(Some(10_000)));
    assert!(poll_once(shutdown.as_mut()).await.is_pending());
    probe.release.cancel();
    within(shutdown).await;

    assert_eq!(probe.completed.load(Ordering::SeqCst), 1);
    assert_eq!(
        Arc::strong_count(&provider),
        1,
        "activity manager retained the provider"
    );
}

#[tokio::test]
async fn graceful_shutdown_waits_for_orchestration_renewal_cleanup() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let ack = Arc::new(ProviderGate::default());
    let renewal = Arc::new(ProviderGate::default());
    let provider = Arc::new(
        LongPollingSqliteProvider::new(store)
            .with_gate(ProviderOperation::BeforeOrchestrationAck, ack.clone())
            .with_gate(ProviderOperation::BeforeOrchestrationRenewal, renewal.clone()),
    );
    let probe = Arc::new(ActivityProbe::default());
    let rt = start_case(provider.clone(), &probe, options(), "ImmediateOrch").await;
    within(renewal.wait_until_entered()).await;

    let mut shutdown = Box::pin(rt.shutdown(Some(10_000)));
    assert!(poll_once(shutdown.as_mut()).await.is_pending());
    ack.open();
    within(shutdown).await;

    assert_eq!(
        Arc::strong_count(&provider),
        1,
        "orchestration renewal retained the provider"
    );
}

async fn cancel_provider_call_and_recover(operation: ProviderOperation, orchestration: &str) {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let gate = Arc::new(ProviderGate::default());
    let provider = Arc::new(LongPollingSqliteProvider::new(store.clone()).with_gate(operation, gate.clone()));
    let probe = Arc::new(ActivityProbe::default());
    probe.release.cancel();
    let opts = RuntimeOptions {
        worker_lock_timeout: Duration::from_millis(500),
        orchestrator_lock_timeout: Duration::from_millis(500),
        ..options()
    };
    let rt = start_case(provider.clone(), &probe, opts.clone(), orchestration).await;
    within(gate.wait_until_entered()).await;

    within(rt.shutdown(Some(25))).await;
    assert_eq!(
        Arc::strong_count(&provider),
        1,
        "blocked provider call survived shutdown"
    );
    recover(store.clone(), &probe, opts).await;

    let history = store.read("shutdown-instance").await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(event.kind, EventKind::OrchestrationCompleted { .. }))
            .count(),
        1,
        "recovery must produce exactly one durable orchestration completion"
    );
    if orchestration == "ActivityOrch" {
        let expected_attempts = if operation == ProviderOperation::BeforeWorkAck {
            2
        } else {
            1
        };
        assert_eq!(probe.attempts.load(Ordering::SeqCst), expected_attempts);
    }
}

#[tokio::test]
async fn shutdown_during_orchestration_fetch_recovers_an_acquired_lock() {
    cancel_provider_call_and_recover(ProviderOperation::AfterOrchestrationFetch, "ImmediateOrch").await;
}

#[tokio::test]
async fn shutdown_during_work_fetch_recovers_an_acquired_lock() {
    cancel_provider_call_and_recover(ProviderOperation::AfterWorkFetch, "ActivityOrch").await;
}

#[tokio::test]
async fn shutdown_before_orchestration_ack_recovers_the_turn() {
    cancel_provider_call_and_recover(ProviderOperation::BeforeOrchestrationAck, "ImmediateOrch").await;
}

#[tokio::test]
async fn shutdown_after_orchestration_ack_preserves_the_commit() {
    cancel_provider_call_and_recover(ProviderOperation::AfterOrchestrationAck, "ImmediateOrch").await;
}

#[tokio::test]
async fn shutdown_before_work_ack_redelivers_the_activity() {
    cancel_provider_call_and_recover(ProviderOperation::BeforeWorkAck, "ActivityOrch").await;
}

#[tokio::test]
async fn shutdown_after_work_ack_does_not_repeat_the_activity() {
    cancel_provider_call_and_recover(ProviderOperation::AfterWorkAck, "ActivityOrch").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_waits_for_non_yielding_activity_to_exit() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let probe = Arc::new(ActivityProbe::default());
    let release = Arc::new(AtomicBool::new(false));
    struct ReleaseOnDrop(Arc<AtomicBool>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let release_guard = ReleaseOnDrop(release.clone());
    let activities = ActivityRegistry::builder()
        .register("Activity", {
            let probe = probe.clone();
            move |_: ActivityContext, _: String| {
                let probe = probe.clone();
                let release = release.clone();
                async move {
                    probe.active.store(true, Ordering::SeqCst);
                    let _active = ActiveActivity(probe.clone());
                    probe.entered.cancel();
                    let start = Instant::now();
                    // Bound the deliberate executor blockage even if the test fails.
                    while !release.load(Ordering::SeqCst) && start.elapsed() < Duration::from_secs(5) {
                        std::thread::yield_now();
                    }
                    Ok("done".to_string())
                }
            }
        })
        .build();
    let rt = Runtime::start_with_options(store.clone(), activities, orchestrations(), options()).await;
    Client::new(store.clone())
        .start_orchestration("shutdown-instance", "ActivityOrch", "")
        .await
        .unwrap();
    within(probe.entered.cancelled()).await;

    let mut shutdown = Box::pin(rt.shutdown(Some(0)));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut shutdown)
            .await
            .is_err(),
        "shutdown returned before its non-yielding activity stopped"
    );
    drop(release_guard);
    within(shutdown).await;
    assert!(!probe.active.load(Ordering::SeqCst));
    assert_eq!(Arc::strong_count(&store), 1);
}

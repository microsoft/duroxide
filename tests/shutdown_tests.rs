// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Regression tests for `Runtime::shutdown`.
//!
//! Before the fix, `shutdown` only aborted the three supervisor tasks it tracked.
//! Each supervisor had itself spawned N children into a local `Vec<JoinHandle>`;
//! aborting the supervisor dropped that vec, and dropping a `JoinHandle` *detaches*
//! the task rather than cancelling it. Every dispatcher poller therefore survived
//! shutdown indefinitely, holding provider connections open.
//!
//! These tests only fail against a runtime that actually stops its child tasks, so
//! they rely on a provider that honours `poll_timeout` — the bundled SQLite provider
//! returns immediately, which is why the original bug never showed up in CI.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::providers::Provider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions, ShutdownOutcome};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod common;

use common::long_polling::LongPollingSqliteProvider;

fn empty_activities() -> ActivityRegistry {
    ActivityRegistry::builder().build()
}

fn empty_orchestrations() -> OrchestrationRegistry {
    OrchestrationRegistry::builder().build()
}

/// Options with a long poll timeout, mimicking a provider that parks inside `fetch_*`.
fn long_poll_options(orch: usize, worker: usize) -> RuntimeOptions {
    RuntimeOptions {
        orchestration_concurrency: orch,
        worker_concurrency: worker,
        dispatcher_long_poll_timeout: Duration::from_secs(30),
        ..Default::default()
    }
}

/// The core regression: every poller must stop, even when parked in a long poll.
///
/// Against the pre-fix runtime the pollers stay alive because aborting their
/// supervisor merely detaches them, so the sentinel count stays above 1.
#[tokio::test]
async fn shutdown_stops_pollers_parked_in_long_poll() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let provider = Arc::new(LongPollingSqliteProvider::new(store));
    let sentinel = provider.sentinel();
    let provider: Arc<dyn Provider> = provider;

    let rt = runtime::Runtime::start_with_options(
        provider,
        empty_activities(),
        empty_orchestrations(),
        long_poll_options(2, 2),
    )
    .await;

    // Let every poller enter its long poll.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        Arc::strong_count(&sentinel) > 1,
        "expected pollers to be parked inside fetch_* before shutdown"
    );

    let outcome = rt.shutdown(Some(2_000)).await;

    // Aborts land on the next scheduler pass; give them one.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        Arc::strong_count(&sentinel),
        1,
        "poller tasks leaked past shutdown ({outcome:?})"
    );
}

/// A graceful shutdown of an idle runtime must not burn the whole timeout.
///
/// The old implementation slept for `timeout_ms` unconditionally, so ~200 call sites
/// each paid a full second regardless of how quickly the runtime went quiet.
#[tokio::test]
async fn shutdown_returns_as_soon_as_tasks_drain() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;

    let rt = runtime::Runtime::start_with_options(
        store,
        empty_activities(),
        empty_orchestrations(),
        RuntimeOptions {
            orchestration_concurrency: 2,
            worker_concurrency: 2,
            ..Default::default()
        },
    )
    .await;

    let start = Instant::now();
    let outcome = rt.shutdown(Some(10_000)).await;
    let elapsed = start.elapsed();

    assert_eq!(outcome, ShutdownOutcome::Drained, "idle runtime should drain cleanly");
    assert!(
        elapsed < Duration::from_secs(5),
        "shutdown treated the timeout as a sleep instead of a deadline (elapsed: {elapsed:?})"
    );
}

/// `Some(0)` must still signal cancellation before aborting.
///
/// The old zero-deadline branch returned before ever setting the shutdown flag, so
/// "immediate abort" never even asked the children to stop.
#[tokio::test]
async fn shutdown_zero_aborts_immediately() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let provider = Arc::new(LongPollingSqliteProvider::new(store));
    let sentinel = provider.sentinel();
    let provider: Arc<dyn Provider> = provider;

    let rt = runtime::Runtime::start_with_options(
        provider,
        empty_activities(),
        empty_orchestrations(),
        long_poll_options(2, 2),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(300)).await;

    let start = Instant::now();
    let outcome = rt.shutdown(Some(0)).await;
    let elapsed = start.elapsed();

    assert!(
        matches!(outcome, ShutdownOutcome::Aborted { tasks } if tasks > 0),
        "expected parked pollers to be reported as aborted, got {outcome:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "zero-deadline shutdown should not wait (elapsed: {elapsed:?})"
    );

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(Arc::strong_count(&sentinel), 1, "poller tasks leaked past abort");
}

/// A task that never yields cannot be cancelled, so shutdown must fall back to abort
/// and report it rather than hanging forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_reports_abort_when_task_will_not_yield() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("Wedged", |_ctx: ActivityContext, _: String| async move {
            // Busy-loop without an await point: cancellation cannot preempt this,
            // and dropping the future is impossible until it yields.
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline {
                std::hint::spin_loop();
            }
            Ok("done".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("WedgedOrch", |ctx: OrchestrationContext, _: String| async move {
            ctx.schedule_activity("Wedged", "").await
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities,
        orchestrations,
        RuntimeOptions {
            orchestration_concurrency: 1,
            worker_concurrency: 1,
            ..Default::default()
        },
    )
    .await;

    Client::new(store)
        .start_orchestration("wedged-1", "WedgedOrch", "")
        .await
        .unwrap();

    // Wait for the activity to be picked up and wedge the worker.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let start = Instant::now();
    let outcome = rt.shutdown(Some(500)).await;
    let elapsed = start.elapsed();

    assert!(
        matches!(outcome, ShutdownOutcome::Aborted { tasks } if tasks > 0),
        "wedged worker should force an abort, got {outcome:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "shutdown must honour its deadline even when a task cannot be cancelled (elapsed: {elapsed:?})"
    );
}

/// pgrx-style embedding: single-threaded runtime, 1x1 concurrency.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_works_in_single_threaded_runtime() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;
    let provider = Arc::new(LongPollingSqliteProvider::new(store));
    let sentinel = provider.sentinel();
    let provider: Arc<dyn Provider> = provider;

    let rt = runtime::Runtime::start_with_options(
        provider,
        empty_activities(),
        empty_orchestrations(),
        long_poll_options(1, 1),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(300)).await;

    let outcome = rt.shutdown(Some(2_000)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        Arc::strong_count(&sentinel),
        1,
        "poller tasks leaked past shutdown in single-threaded mode ({outcome:?})"
    );
}

/// Shutdown must be safe to call twice.
#[tokio::test]
async fn shutdown_is_idempotent() {
    let (store, _tmp) = common::create_sqlite_store_disk().await;

    let rt = runtime::Runtime::start_with_options(
        store,
        empty_activities(),
        empty_orchestrations(),
        RuntimeOptions {
            orchestration_concurrency: 1,
            worker_concurrency: 1,
            ..Default::default()
        },
    )
    .await;

    assert_eq!(rt.clone().shutdown(Some(2_000)).await, ShutdownOutcome::Drained);
    // Second call has nothing left to track and must return immediately.
    assert_eq!(rt.shutdown(Some(2_000)).await, ShutdownOutcome::Drained);
}

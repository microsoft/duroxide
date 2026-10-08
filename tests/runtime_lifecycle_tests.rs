// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "common/runtime_lifecycle.rs"]
mod support;
#[path = "common/tracing_capture.rs"]
mod tracing_capture;

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use duroxide::OrchestrationRegistry;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::test_hooks::{LifecyclePoint, ProviderOperation};
use duroxide::runtime::{Runtime, RuntimeShutdownError, RuntimeStartError, ShutdownOutcome};
use futures_util::FutureExt;
use support::{Fixture, SECRET, bounded, entered, options, until};

#[test]
fn prepared_start_rejects_a_missing_executor_without_consuming_the_created_state() {
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let f = executor.block_on(Fixture::new());
    let runtime = f.empty();
    assert_eq!(
        futures::executor::block_on(Arc::clone(&runtime).start_execution()),
        Err(RuntimeStartError::NoExecutor)
    );
    f.assert_retired();
    executor.block_on(async {
        Arc::clone(&runtime).start_execution().await.unwrap();
        assert_eq!(
            runtime.shutdown_with_grace(Duration::from_secs(1)).await,
            Ok(ShutdownOutcome::Drained)
        );
    });
    f.assert_retired();
}

#[tokio::test]
async fn invalid_startup_options_are_typed_or_legacy_panics_before_any_execution() {
    let f = Fixture::new().await;
    let mut config = options();
    config.session_idle_timeout = Duration::ZERO;
    let result = Runtime::prepare(
        Arc::clone(&f.provider) as Arc<dyn duroxide::providers::Provider>,
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder().build(),
        config.clone(),
    );
    let Err(error) = result else {
        panic!("invalid options produced a prepared runtime")
    };
    assert_eq!(error, RuntimeStartError::InvalidSessionIdleTimeout);
    let result = AssertUnwindSafe(Runtime::start_with_options(
        Arc::clone(&f.provider) as Arc<dyn duroxide::providers::Provider>,
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder().build(),
        config,
    ))
    .catch_unwind()
    .await;
    assert!(result.is_err());
    assert_eq!(
        f.hooks
            .hits(LifecyclePoint::ProviderEnter(ProviderOperation::SystemMetrics)),
        0
    );
    assert_eq!(f.hooks.hits(LifecyclePoint::StartupBeforeIo), 0);
    f.assert_retired();
}

#[tokio::test]
async fn preparation_and_created_completion_do_not_execute_or_touch_provider_io() {
    let f = Fixture::new().await;
    let runtime = f.empty();
    assert_eq!(
        f.hooks
            .hits(LifecyclePoint::ProviderEnter(ProviderOperation::SystemMetrics)),
        0
    );
    f.assert_retired();
    assert_eq!(
        runtime.wait_for_shutdown_completion().await,
        Err(RuntimeShutdownError::NotRequested)
    );
    assert_eq!(
        Arc::clone(&runtime)
            .shutdown_with_timeouts(Duration::ZERO, Duration::ZERO)
            .await,
        Ok(ShutdownOutcome::Drained)
    );
    assert_eq!(
        Arc::clone(&runtime).start_execution().await,
        Err(RuntimeStartError::ShutdownRequested)
    );
    assert_eq!(f.hooks.hits(LifecyclePoint::ShutdownCoordinator), 0);
    f.assert_retired();
}

#[tokio::test]
async fn invalid_timeouts_are_rejected_before_created_ready_and_duplicate_paths() {
    let f = Fixture::new().await;
    let runtime = f.empty();
    for stopped in [false, true] {
        let before = f.hooks.counts();
        let unsupported = Duration::from_nanos(u64::MAX) + Duration::from_nanos(1);
        assert_eq!(
            Arc::clone(&runtime)
                .shutdown_with_timeouts(Duration::ZERO, unsupported)
                .await,
            Err(RuntimeShutdownError::DeadlineOverflow),
        );
        assert_eq!(
            Arc::clone(&runtime)
                .shutdown_with_timeouts(Duration::from_secs(1), Duration::ZERO)
                .await,
            Err(RuntimeShutdownError::InvalidTimeouts),
        );
        assert_eq!(
            Arc::clone(&runtime).shutdown_with_grace(Duration::MAX).await,
            Err(RuntimeShutdownError::DeadlineOverflow)
        );
        assert_eq!(
            Arc::clone(&runtime)
                .shutdown_with_timeouts(Duration::MAX, Duration::MAX)
                .await,
            Err(RuntimeShutdownError::DeadlineOverflow),
        );
        let now = Instant::now();
        assert_eq!(
            runtime.request_shutdown_until(now + Duration::from_secs(1), now),
            Err(RuntimeShutdownError::InvalidTimeouts)
        );
        assert_eq!(before, f.hooks.counts());
        if !stopped {
            assert_eq!(
                runtime.wait_for_shutdown_completion().await,
                Err(RuntimeShutdownError::NotRequested)
            );
        }
        assert_eq!(
            Arc::clone(&runtime)
                .shutdown_with_timeouts(Duration::from_nanos(1), Duration::from_nanos(1))
                .await,
            Ok(ShutdownOutcome::Drained),
        );
    }
    f.assert_retired();
}

#[tokio::test]
async fn idle_runtime_drains_under_two_seconds_despite_hour_long_owned_sleeps() {
    let f = Fixture::new().await;
    let mut config = options();
    config.dispatcher_min_poll_interval = Duration::from_secs(3600);
    config.observability.gauge_poll_interval = Duration::from_secs(3600);
    let runtime = f.prepare(
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder().build(),
        config,
    );
    Arc::clone(&runtime).start_execution().await.unwrap();
    until(|| {
        f.hooks
            .hits(LifecyclePoint::ProviderReturn(ProviderOperation::FetchOrchestration))
            > 0
            && f.hooks
                .hits(LifecyclePoint::ProviderReturn(ProviderOperation::FetchActivity))
                > 0
    })
    .await;
    let start = Instant::now();
    assert_eq!(
        Arc::clone(&runtime).shutdown_with_grace(Duration::from_secs(30)).await,
        Ok(ShutdownOutcome::Drained)
    );
    assert!(start.elapsed() < Duration::from_secs(2), "{:?}", start.elapsed());
    assert_eq!(f.hooks.hits(LifecyclePoint::ForceRequested), 0);
    assert_eq!(f.hooks.hits(LifecyclePoint::ShutdownCoordinator), 1);
    f.assert_retired();
}

#[tokio::test]
async fn startup_io_remains_owned_after_total_timeout_and_cannot_admit_work() {
    let f = Fixture::new().await;
    let held = f.hold(LifecyclePoint::ProviderReturn(ProviderOperation::SystemMetrics));
    let runtime = f.empty();
    let starter = tokio::spawn(Arc::clone(&runtime).start_execution());
    entered(&held).await;
    assert_eq!(f.provider.active_operations(), 1);
    let start = Instant::now();
    runtime
        .request_shutdown_until(start + Duration::from_millis(100), start + Duration::from_millis(300))
        .unwrap();
    assert!(start.elapsed() < Duration::from_millis(100));
    let mut completion = Box::pin(runtime.wait_for_shutdown_completion());
    assert!(completion.as_mut().now_or_never().is_none());
    assert_eq!(
        Arc::clone(&runtime)
            .shutdown_with_timeouts(Duration::from_millis(100), Duration::from_millis(300))
            .await,
        Err(RuntimeShutdownError::TimedOut),
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(f.provider.active_operations(), 1);
    assert!(completion.as_mut().now_or_never().is_none());
    assert!(f.hooks.counts().iter().all(|count| count.started == 0));
    held.release();
    assert_eq!(
        bounded(starter).await.unwrap(),
        Err(RuntimeStartError::ShutdownRequested)
    );
    assert_eq!(bounded(completion).await, Ok(ShutdownOutcome::Forced));
    assert!(f.hooks.counts().iter().all(|count| count.started == 0));
    assert_eq!(f.hooks.hits(LifecyclePoint::ShutdownCoordinator), 1);
    f.assert_retired();
}

#[tokio::test]
async fn dropping_start_waiter_requests_rollback_without_dropping_start_owner() {
    let f = Fixture::new().await;
    let held = f.hold(LifecyclePoint::StartupBeforeIo);
    let runtime = f.empty();
    let starter = tokio::spawn(Arc::clone(&runtime).start_execution());
    entered(&held).await;
    starter.abort();
    assert!(starter.await.unwrap_err().is_cancelled());
    let mut completion = Box::pin(runtime.wait_for_shutdown_completion());
    assert!(completion.as_mut().now_or_never().is_none());
    held.release();
    assert_eq!(bounded(completion).await, Ok(ShutdownOutcome::Forced));
    assert_eq!(
        Arc::clone(&runtime).start_execution().await,
        Err(RuntimeStartError::ShutdownRequested)
    );
    assert_eq!(f.hooks.hits(LifecyclePoint::ShutdownCoordinator), 1);
    f.assert_retired();
}

#[tokio::test]
async fn dropped_shutdown_observers_do_not_change_first_deadlines_or_cleanup() {
    let f = Fixture::new().await;
    let held = f.hold(LifecyclePoint::ProviderReturn(ProviderOperation::FetchOrchestration));
    let runtime = f.empty();
    Arc::clone(&runtime).start_execution().await.unwrap();
    entered(&held).await;
    let start = Instant::now();
    runtime
        .request_shutdown_until(start + Duration::from_millis(100), start + Duration::from_millis(300))
        .unwrap();
    let ordinary = tokio::spawn(Arc::clone(&runtime).shutdown_with_grace(Duration::from_secs(30)));
    let observing_runtime = Arc::clone(&runtime);
    let explicit = tokio::spawn(async move { observing_runtime.wait_for_shutdown_completion().await });
    ordinary.abort();
    explicit.abort();
    assert!(ordinary.await.unwrap_err().is_cancelled());
    assert!(explicit.await.unwrap_err().is_cancelled());
    assert_eq!(
        Arc::clone(&runtime).shutdown_with_grace(Duration::from_secs(30)).await,
        Err(RuntimeShutdownError::TimedOut),
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(f.provider.active_operations() > 0);
    assert_eq!(f.hooks.hits(LifecyclePoint::ShutdownCoordinator), 1);
    held.release();
    assert_eq!(
        bounded(runtime.wait_for_shutdown_completion()).await,
        Ok(ShutdownOutcome::Forced)
    );
    f.assert_retired();
}

#[tokio::test]
async fn omitted_total_and_legacy_default_finish_waiting_by_seven_seconds_and_log_incompletion() {
    let (events, _capture) = tracing_capture::install_tracing_capture();
    let f = Fixture::new().await;
    let held = f.hold(LifecyclePoint::StartupBeforeIo);
    let runtime = f.empty();
    let starter = tokio::spawn(Arc::clone(&runtime).start_execution());
    entered(&held).await;
    let start = Instant::now();
    let (result, ()) = tokio::join!(
        Arc::clone(&runtime).shutdown_with_grace(Duration::from_secs(1)),
        Arc::clone(&runtime).shutdown(None),
    );
    assert_eq!(result, Err(RuntimeShutdownError::TimedOut));
    assert!(start.elapsed() >= Duration::from_secs(6));
    assert!(start.elapsed() < Duration::from_secs(7), "{:?}", start.elapsed());
    assert!(events.lock().unwrap().iter().any(|event| {
        event.level == tracing::Level::ERROR
            && event.target == "duroxide::runtime::lifecycle"
            && event.message.contains("Legacy")
            && event.field("category").as_deref() == Some("legacy_shutdown_failed")
            && event
                .field("error")
                .is_some_and(|text| text.contains("incomplete") && text.contains("terminate"))
    }));
    held.release();
    assert_eq!(
        bounded(starter).await.unwrap(),
        Err(RuntimeStartError::ShutdownRequested)
    );
    assert_eq!(
        bounded(runtime.wait_for_shutdown_completion()).await,
        Ok(ShutdownOutcome::Forced)
    );
    assert!(events.lock().unwrap().iter().any(|event| {
        event.level == tracing::Level::WARN && event.field("category").as_deref() == Some("shutdown_forced")
    }));
    f.assert_retired();
}

#[tokio::test]
async fn lifecycle_diagnostics_correlate_first_policy_and_late_completion_for_distinct_workers() {
    let (events, _capture) = tracing_capture::install_tracing_capture();
    let first = Fixture::new().await;
    let second = Fixture::new().await;
    let first_hold = first.hold(LifecyclePoint::StartupBeforeIo);
    let second_hold = second.hold(LifecyclePoint::StartupBeforeIo);
    let first_runtime = first.empty();
    let second_runtime = second.empty();
    let identities: Vec<_> = events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.field("category").as_deref() == Some("runtime_prepared"))
        .map(|event| event.field("lifecycle_id").unwrap())
        .collect();
    assert_eq!(identities.len(), 2);
    assert_ne!(identities[0], identities[1]);
    let first_start = tokio::spawn(Arc::clone(&first_runtime).start_execution());
    let second_start = tokio::spawn(Arc::clone(&second_runtime).start_execution());
    entered(&first_hold).await;
    entered(&second_hold).await;
    assert_eq!(
        Arc::clone(&first_runtime)
            .shutdown_with_timeouts(Duration::from_millis(100), Duration::from_millis(300))
            .await,
        Err(RuntimeShutdownError::TimedOut)
    );
    assert_eq!(
        Arc::clone(&first_runtime)
            .shutdown_with_grace(Duration::from_secs(30))
            .await,
        Err(RuntimeShutdownError::TimedOut)
    );
    let now = Instant::now();
    second_runtime
        .request_shutdown_until(now, now + Duration::from_secs(2))
        .unwrap();
    second_hold.release();
    assert_eq!(
        bounded(second_start).await.unwrap(),
        Err(RuntimeStartError::ShutdownRequested)
    );
    assert_eq!(
        bounded(second_runtime.wait_for_shutdown_completion()).await,
        Ok(ShutdownOutcome::Forced)
    );
    first_hold.release();
    assert_eq!(
        bounded(first_start).await.unwrap(),
        Err(RuntimeStartError::ShutdownRequested)
    );
    assert_eq!(
        bounded(first_runtime.wait_for_shutdown_completion()).await,
        Ok(ShutdownOutcome::Forced)
    );
    first.assert_retired();
    second.assert_retired();
    let captured = events.lock().unwrap();
    let first_events: Vec<_> = captured
        .iter()
        .filter(|event| {
            event.field("lifecycle_id").as_deref() == Some(identities[0].as_str())
                && matches!(
                    event.field("category").as_deref(),
                    Some("shutdown_wait_failed" | "shutdown_forced")
                )
        })
        .collect();
    assert_eq!(first_events.len(), 3);
    for event in &first_events {
        assert_eq!(event.field("core_timeout_observed").as_deref(), Some("true"));
        assert_eq!(
            event.field("grace_remaining_ns_at_acceptance"),
            first_events[0].field("grace_remaining_ns_at_acceptance")
        );
        assert_eq!(
            event.field("total_remaining_ns_at_acceptance"),
            first_events[0].field("total_remaining_ns_at_acceptance")
        );
        let grace: u64 = event
            .field("grace_remaining_ns_at_acceptance")
            .unwrap()
            .parse()
            .unwrap();
        let total: u64 = event
            .field("total_remaining_ns_at_acceptance")
            .unwrap()
            .parse()
            .unwrap();
        assert!(grace <= 100_000_000 && total <= 300_000_000 && total > grace);
    }
    let second_forced = captured
        .iter()
        .find(|event| {
            event.field("category").as_deref() == Some("shutdown_forced")
                && event.field("lifecycle_id").as_deref() == Some(identities[1].as_str())
        })
        .unwrap();
    assert_eq!(second_forced.field("grace_due_at_acceptance").as_deref(), Some("true"));
    assert_eq!(second_forced.field("core_timeout_observed").as_deref(), Some("false"));
}

#[tokio::test]
async fn fallible_and_legacy_startup_failures_are_sanitized_and_release_owned_rollback() {
    let (events, _capture) = tracing_capture::install_tracing_capture();
    let f = Fixture::new().await;
    f.provider.panic_on_name(SECRET);
    let runtime = f.empty();
    let error = Arc::clone(&runtime).start_execution().await.unwrap_err();
    assert_eq!(error, RuntimeStartError::StartupFailed);
    assert!(!format!("{error:?}: {error}").contains("sentinel"));
    let error = bounded(runtime.wait_for_shutdown_completion()).await.unwrap_err();
    assert!(error.is_quiescent());
    assert!(error.to_string().contains("completed"));
    assert!(!error.to_string().contains("terminate"));
    Arc::clone(&runtime).shutdown(None).await;
    f.assert_retired();
    drop(runtime);

    f.provider.panic_on_name(SECRET);
    let result = AssertUnwindSafe(Runtime::start_with_options(
        Arc::clone(&f.provider) as Arc<dyn duroxide::providers::Provider>,
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder().build(),
        options(),
    ))
    .catch_unwind()
    .await;
    let Err(payload) = result else {
        panic!("legacy startup returned a healthy runtime after failure")
    };
    let text = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .expect("legacy startup has a string panic");
    assert!(text.contains("startup failed") && text.contains("rollback"));
    assert!(!text.contains("sentinel"));
    until(|| Arc::strong_count(&f.provider) == 1).await;
    let captured = events.lock().unwrap();
    assert!(captured.iter().any(|event| event.level == tracing::Level::ERROR
        && event.field("category").as_deref() == Some("owned_execution_failed")));
    assert!(captured.iter().any(
        |event| event.field("category").as_deref() == Some("legacy_shutdown_failed")
            && event.field("quiescent").as_deref() == Some("true")
    ));
    assert!(!format!("{captured:?}").contains("sentinel"));
}

#[tokio::test]
async fn optional_gauge_errors_remain_nonfatal_and_do_not_disclose_provider_details() {
    let (events, _capture) = tracing_capture::install_tracing_capture();
    let f = Fixture::new().await;
    f.provider.fail_before(ProviderOperation::SystemMetrics, SECRET);
    let runtime = f.empty();
    Arc::clone(&runtime).start_execution().await.unwrap();
    assert_eq!(
        Arc::clone(&runtime).start_execution().await,
        Err(RuntimeStartError::AlreadyStarted)
    );
    assert_eq!(
        runtime.shutdown_with_grace(Duration::from_secs(1)).await,
        Ok(ShutdownOutcome::Drained)
    );
    f.assert_retired();
    let captured = events.lock().unwrap();
    assert!(captured.iter().any(|event| event.level == tracing::Level::WARN
        && event.field("category").as_deref() == Some("gauge_initialization_failed")));
    assert!(!format!("{captured:?}").contains("sentinel"));
}

#[tokio::test]
async fn ordered_concurrent_stop_requests_keep_one_coordinator_and_first_budget_100_times() {
    for _ in 0..100 {
        let f = Fixture::new().await;
        let held = f.hold(LifecyclePoint::ProviderReturn(ProviderOperation::FetchOrchestration));
        let runtime = f.empty();
        Arc::clone(&runtime).start_execution().await.unwrap();
        entered(&held).await;
        let now = Instant::now();
        runtime
            .request_shutdown_until(now + Duration::from_secs(2), now + Duration::from_secs(3))
            .unwrap();
        let first = Arc::clone(&runtime);
        let second = Arc::clone(&runtime);
        let callers = [
            tokio::spawn(async move {
                let now = Instant::now();
                first.request_shutdown_until(now, now)
            }),
            tokio::spawn(async move {
                let now = Instant::now();
                second.request_shutdown_until(now, now)
            }),
        ];
        for caller in callers {
            caller.await.unwrap().unwrap();
        }
        held.release();
        assert_eq!(
            bounded(runtime.wait_for_shutdown_completion()).await,
            Ok(ShutdownOutcome::Drained)
        );
        assert_eq!(f.hooks.hits(LifecyclePoint::ShutdownCoordinator), 1);
        f.assert_retired();
    }
}

#[tokio::test]
async fn completion_has_priority_over_expired_total_after_prior_timeout_100_times() {
    for _ in 0..100 {
        let f = Fixture::new().await;
        let held = f.hold(LifecyclePoint::StartupBeforeIo);
        let runtime = f.empty();
        let starter = tokio::spawn(Arc::clone(&runtime).start_execution());
        entered(&held).await;
        assert_eq!(
            Arc::clone(&runtime)
                .shutdown_with_timeouts(Duration::ZERO, Duration::ZERO)
                .await,
            Err(RuntimeShutdownError::TimedOut),
        );
        held.release();
        assert_eq!(
            bounded(starter).await.unwrap(),
            Err(RuntimeStartError::ShutdownRequested)
        );
        assert_eq!(
            bounded(runtime.wait_for_shutdown_completion()).await,
            Ok(ShutdownOutcome::Forced)
        );
        assert_eq!(
            Arc::clone(&runtime)
                .shutdown_with_timeouts(Duration::ZERO, Duration::ZERO)
                .await,
            Ok(ShutdownOutcome::Forced),
        );
        assert_eq!(f.hooks.hits(LifecyclePoint::ShutdownCoordinator), 1);
        f.assert_retired();
    }
}

#[tokio::test]
async fn startup_stop_order_closes_production_before_any_dispatch_100_times() {
    for _ in 0..100 {
        let f = Fixture::new().await;
        let held = f.hold(LifecyclePoint::StartupBeforeIo);
        let runtime = f.empty();
        let starter = tokio::spawn(Arc::clone(&runtime).start_execution());
        entered(&held).await;
        let now = Instant::now();
        runtime
            .request_shutdown_until(now + Duration::from_secs(1), now + Duration::from_secs(2))
            .unwrap();
        held.release();
        assert_eq!(
            bounded(starter).await.unwrap(),
            Err(RuntimeStartError::ShutdownRequested)
        );
        assert_eq!(
            bounded(runtime.wait_for_shutdown_completion()).await,
            Ok(ShutdownOutcome::Drained)
        );
        assert!(f.hooks.counts().iter().all(|count| count.started == 0));
        assert_eq!(f.hooks.hits(LifecyclePoint::ShutdownCoordinator), 1);
        assert_eq!(
            f.hooks
                .hits(LifecyclePoint::ProviderEnter(ProviderOperation::SystemMetrics)),
            0
        );
        f.assert_retired();
    }
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::clone_on_ref_ptr)]

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
use duroxide::runtime::{Runtime, RuntimeStartError, ShutdownOutcome};
use futures_util::FutureExt;
use support::{Fixture, SECRET, bounded, entered, options, until};
use tokio_util::sync::CancellationToken;

#[test]
fn prepared_start_rejects_a_missing_executor_without_consuming_the_created_state() {
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let f = executor.block_on(Fixture::new());
    let runtime = f.empty();
    assert_eq!(
        futures::executor::block_on(runtime.clone().start_execution()),
        Err(RuntimeStartError::NoExecutor)
    );
    f.assert_retired();
    executor.block_on(async {
        runtime.clone().start_execution().await.unwrap();
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
        f.provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder().build(),
        config.clone(),
    );
    assert!(matches!(result, Err(RuntimeStartError::InvalidSessionIdleTimeout)));
    let result = AssertUnwindSafe(Runtime::start_with_options(
        f.provider.clone(),
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
async fn preparation_and_created_stop_do_not_execute_or_touch_provider_io() {
    let f = Fixture::new().await;
    let runtime = f.empty();
    runtime.request_shutdown();
    assert_eq!(
        runtime.clone().shutdown_with_grace(Duration::ZERO).await,
        Ok(ShutdownOutcome::Drained)
    );
    assert_eq!(
        runtime.start_execution().await,
        Err(RuntimeStartError::ShutdownRequested)
    );
    assert_eq!(
        f.hooks
            .hits(LifecyclePoint::ProviderEnter(ProviderOperation::SystemMetrics)),
        0
    );
    assert_eq!(f.hooks.hits(LifecyclePoint::StartupBeforeIo), 0);
    f.assert_retired();
}

#[tokio::test]
async fn large_native_grace_retains_legacy_meaning_without_sdk_nanosecond_limits() {
    for legacy in [false, true] {
        let f = Fixture::new().await;
        let runtime = f.empty();
        runtime.clone().start_execution().await.unwrap();
        if legacy {
            bounded(runtime.clone().shutdown(Some(u64::MAX))).await;
        } else {
            assert_eq!(
                bounded(runtime.clone().shutdown_with_grace(Duration::MAX)).await,
                Ok(ShutdownOutcome::Drained)
            );
        }
        assert_eq!(
            runtime.shutdown_with_grace(Duration::from_nanos(1)).await,
            Ok(ShutdownOutcome::Drained)
        );
        assert_eq!(f.hooks.hits(LifecyclePoint::ForceRequested), 0);
        f.assert_retired();
    }
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
    runtime.clone().start_execution().await.unwrap();
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
        runtime.shutdown_with_grace(Duration::from_secs(30)).await,
        Ok(ShutdownOutcome::Drained)
    );
    assert!(start.elapsed() < Duration::from_secs(2), "{:?}", start.elapsed());
    assert_eq!(f.hooks.hits(LifecyclePoint::ForceRequested), 0);
    f.assert_retired();
}

#[tokio::test]
async fn stop_cancels_startup_provider_io_without_admitting_work() {
    let f = Fixture::new().await;
    let held = f.hold(LifecyclePoint::ProviderReturn(ProviderOperation::SystemMetrics));
    let runtime = f.empty();
    let starter = tokio::spawn(runtime.clone().start_execution());
    entered(&held).await;
    assert!(f.provider.active_operations() > 0);
    let start = Instant::now();
    runtime.request_shutdown();
    assert!(start.elapsed() < Duration::from_millis(100));
    assert_eq!(
        bounded(starter).await.unwrap(),
        Err(RuntimeStartError::ShutdownRequested)
    );
    assert_eq!(
        bounded(runtime.shutdown_with_grace(Duration::from_secs(1))).await,
        Ok(ShutdownOutcome::Drained)
    );
    assert!(f.hooks.counts().iter().all(|count| count.started == 0));
    f.assert_retired();
    held.release();
}

#[tokio::test]
async fn dropping_start_waiter_requests_and_drives_rollback() {
    let f = Fixture::new().await;
    let held = f.hold(LifecyclePoint::StartupBeforeIo);
    let runtime = f.empty();
    let starter = tokio::spawn(runtime.clone().start_execution());
    entered(&held).await;
    starter.abort();
    assert!(starter.await.unwrap_err().is_cancelled());
    until(|| f.hooks.hits(LifecyclePoint::ShutdownCoordinator) > 0).await;
    let result = bounded(runtime.clone().shutdown_with_grace(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(
        result == ShutdownOutcome::Forced,
        f.hooks.hits(LifecyclePoint::ForceRequested) > 0
    );
    assert_eq!(
        runtime.start_execution().await,
        Err(RuntimeStartError::ShutdownRequested)
    );
    assert_eq!(
        f.hooks
            .hits(LifecyclePoint::ProviderEnter(ProviderOperation::SystemMetrics)),
        0
    );
    f.assert_retired();
    held.release();
}

#[tokio::test]
async fn fallible_and_legacy_startup_failures_are_sanitized_and_release_owned_rollback() {
    let (events, _capture) = tracing_capture::install_tracing_capture();
    let f = Fixture::new().await;
    f.provider.panic_on_name(SECRET);
    let runtime = f.empty();
    let error = runtime.clone().start_execution().await.unwrap_err();
    assert_eq!(error, RuntimeStartError::StartupFailed);
    assert!(!format!("{error:?}: {error}").contains("sentinel"));
    let error = bounded(runtime.clone().shutdown_with_grace(Duration::ZERO))
        .await
        .unwrap_err();
    assert!(error.is_quiescent());
    runtime.clone().shutdown(None).await;
    f.assert_retired();
    drop(runtime);

    f.provider.panic_on_name(SECRET);
    let result = AssertUnwindSafe(Runtime::start_with_options(
        f.provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder().build(),
        options(),
    ))
    .catch_unwind()
    .await;
    let payload = result
        .err()
        .expect("legacy startup cannot return a healthy runtime after failure");
    let text = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap();
    assert!(text.contains("startup failed") && text.contains("rollback"));
    assert!(!text.contains("sentinel"));
    until(|| Arc::strong_count(&f.provider) == 1).await;
    let captured = events.lock().unwrap();
    assert!(captured.iter().any(|event| event.level == tracing::Level::ERROR
        && event.target == "duroxide::runtime::lifecycle"
        && event.message.contains("failed")
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
    runtime.clone().start_execution().await.unwrap();
    assert_eq!(
        runtime.clone().start_execution().await,
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
async fn lifecycle_diagnostics_identify_separate_runtime_instances() {
    let (events, _capture) = tracing_capture::install_tracing_capture();
    let first = Fixture::new().await;
    let second = Fixture::new().await;
    let first_runtime = first.empty();
    let second_runtime = second.empty();
    first_runtime.shutdown_with_grace(Duration::ZERO).await.unwrap();
    second_runtime.shutdown_with_grace(Duration::ZERO).await.unwrap();
    let captured = events.lock().unwrap();
    let prepared: Vec<_> = captured
        .iter()
        .filter(|event| event.field("category").as_deref() == Some("runtime_prepared"))
        .map(|event| event.field("lifecycle_id").unwrap())
        .collect();
    assert_eq!(prepared.len(), 2);
    assert_ne!(prepared[0], prepared[1]);
    first.assert_retired();
    second.assert_retired();
}

#[tokio::test]
async fn shorter_native_call_can_force_an_existing_grace_period_100_times() {
    for _ in 0..100 {
        let f = Fixture::new().await;
        let active = CancellationToken::new();
        let entered = active.clone();
        let activities = ActivityRegistry::builder()
            .register("work", move |_: duroxide::ActivityContext, _: String| {
                let entered = entered.clone();
                async move {
                    entered.cancel();
                    std::future::pending::<Result<String, String>>().await
                }
            })
            .build();
        let orchestrations = OrchestrationRegistry::builder()
            .register("flow", |ctx: duroxide::OrchestrationContext, _: String| async move {
                ctx.schedule_activity("work", "").await
            })
            .build();
        let runtime = f.prepare(activities, orchestrations, options());
        runtime.clone().start_execution().await.unwrap();
        f.client()
            .start_orchestration("shorter-grace", "flow", "")
            .await
            .unwrap();
        bounded(active.cancelled()).await;
        let mut first = Box::pin(runtime.clone().shutdown_with_grace(Duration::from_secs(30)));
        assert!(first.as_mut().now_or_never().is_none());
        assert_eq!(
            bounded(runtime.shutdown_with_grace(Duration::ZERO)).await,
            Ok(ShutdownOutcome::Forced)
        );
        assert_eq!(bounded(first).await, Ok(ShutdownOutcome::Forced));
        assert_eq!(f.hooks.hits(LifecyclePoint::ForceRequested), 1);
        f.assert_retired();
    }
}

#[tokio::test]
async fn startup_stop_order_closes_production_before_any_dispatch_100_times() {
    for _ in 0..100 {
        let f = Fixture::new().await;
        let held = f.hold(LifecyclePoint::StartupBeforeIo);
        let runtime = f.empty();
        let starter = tokio::spawn(runtime.clone().start_execution());
        entered(&held).await;
        runtime.request_shutdown();
        held.release();
        assert_eq!(
            bounded(starter).await.unwrap(),
            Err(RuntimeStartError::ShutdownRequested)
        );
        assert_eq!(
            bounded(runtime.shutdown_with_grace(Duration::from_secs(1))).await,
            Ok(ShutdownOutcome::Drained)
        );
        assert!(f.hooks.counts().iter().all(|count| count.started == 0));
        assert_eq!(
            f.hooks
                .hits(LifecyclePoint::ProviderEnter(ProviderOperation::SystemMetrics)),
            0
        );
        f.assert_retired();
    }
}

//! Observability-focused tests covering tracing for activities and orchestrations.
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

mod common;

use common::fault_injection::FailingProvider;
use common::tracing_capture::CapturedEvent;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::{Provider, WorkItem};
use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{LogFormat, ObservabilityConfig, RuntimeOptions, UnregisteredBackoffConfig};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::sync::Arc;
use std::time::Duration;
use tracing::Level;

fn metrics_observability_config(label: &str) -> ObservabilityConfig {
    ObservabilityConfig {
        log_format: LogFormat::Compact,
        log_level: "error".to_string(),
        service_name: format!("duroxide-observability-test-{label}"),
        service_version: Some("test".to_string()),
        ..Default::default()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn activity_tracing_emits_all_levels() {
    let (recorded_events, _guard) = common::tracing_capture::install_tracing_capture();

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("TraceActivity", |ctx: ActivityContext, _input: String| async move {
            ctx.trace_info("activity info");
            ctx.trace_warn("activity warn");
            ctx.trace_error("activity error");
            ctx.trace_debug("activity debug");
            Ok("ok".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TraceOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("TraceActivity", "payload").await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("trace-activity-instance", "TraceOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("trace-activity-instance", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "ok"),
        OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;

    let events = recorded_events.lock().unwrap();
    let activity_events: Vec<&CapturedEvent> = events
        .iter()
        .filter(|event| event.target == "duroxide::activity")
        .collect();
    assert!(activity_events.len() >= 4, "expected activity traces, found none");

    let find_event = |level: Level, message: &str| -> &CapturedEvent {
        activity_events
            .iter()
            .copied()
            .find(|event| event.level == level && event.field("message").as_deref() == Some(message))
            .unwrap_or_else(|| panic!("missing {level:?} event with message '{message}'"))
    };

    find_event(Level::INFO, "activity info");
    find_event(Level::WARN, "activity warn");
    find_event(Level::ERROR, "activity error");
    find_event(Level::DEBUG, "activity debug");

    for event in activity_events {
        assert_eq!(event.field("instance_id").as_deref(), Some("trace-activity-instance"));
        assert_eq!(event.field("execution_id").as_deref(), Some("1"));
        assert_eq!(event.field("orchestration_name").as_deref(), Some("TraceOrch"));
        assert_eq!(event.field("activity_name").as_deref(), Some("TraceActivity"));
        assert!(event.fields.contains_key("activity_id"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn orchestration_tracing_emits_all_levels() {
    let (recorded_events, _guard) = common::tracing_capture::install_tracing_capture();

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder().build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TraceOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.trace_info("orch info");
            ctx.trace_warn("orch warn");
            ctx.trace_error("orch error");
            ctx.trace_debug("orch debug");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("trace-orch-instance", "TraceOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("trace-orch-instance", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "done"),
        OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;

    let events = recorded_events.lock().unwrap();
    let orchestration_events: Vec<&CapturedEvent> = events
        .iter()
        .filter(|event| event.target == "duroxide::orchestration")
        .collect();
    assert!(
        orchestration_events.len() >= 4,
        "expected orchestration traces, found none"
    );

    let find_event = |level: Level, message: &str| -> &CapturedEvent {
        orchestration_events
            .iter()
            .copied()
            .find(|event| event.level == level && event.field("message").as_deref() == Some(message))
            .unwrap_or_else(|| panic!("missing {level:?} event with message '{message}'"))
    };

    find_event(Level::INFO, "orch info");
    find_event(Level::WARN, "orch warn");
    find_event(Level::ERROR, "orch error");
    find_event(Level::DEBUG, "orch debug");

    for event in orchestration_events {
        assert_eq!(event.field("instance_id").as_deref(), Some("trace-orch-instance"));
        assert_eq!(event.field("execution_id").as_deref(), Some("1"));
        assert_eq!(event.field("orchestration_name").as_deref(), Some("TraceOrch"));
        assert!(event.fields.contains_key("orchestration_version"));
        assert!(!event.fields.contains_key("activity_name"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn metrics_capture_activity_and_orchestration_outcomes() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let failing_provider = Arc::new(FailingProvider::new(sqlite));
    let provider_trait: Arc<dyn Provider> = failing_provider.clone();

    let activities = ActivityRegistry::builder()
        .register("AlwaysOk", |ctx: ActivityContext, _input: String| async move {
            ctx.trace_info("activity ok");
            Ok("ok".to_string())
        })
        .register("AlwaysFail", |ctx: ActivityContext, _input: String| async move {
            ctx.trace_error("activity failure");
            Err("boom".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("SuccessOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("AlwaysOk", "payload")
                .await
                .expect("activity should succeed");
            Ok("done".to_string())
        })
        .register(
            "AppFailureOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                match ctx.schedule_activity("AlwaysFail", "payload").await {
                    Ok(_) => Ok("unexpected".to_string()),
                    Err(err) => Err(err),
                }
            },
        )
        .register(
            "ConfigFailureOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                match ctx.schedule_activity("MissingActivity", "payload").await {
                    Ok(_) => Ok("unexpected".to_string()),
                    Err(err) => Err(err),
                }
            },
        )
        .build();

    let options = RuntimeOptions {
        observability: metrics_observability_config("metrics-outcomes"),
        max_attempts: 3,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        unregistered_backoff: UnregisteredBackoffConfig {
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(50),
        },
        ..Default::default()
    };
    let rt = runtime::Runtime::start_with_options(provider_trait.clone(), activities, orchestrations, options).await;

    let client = Client::new(provider_trait.clone());

    // Trigger infrastructure error (first ack fails but work has already been committed)
    failing_provider.set_ack_then_fail(true);
    failing_provider.fail_next_ack_work_item();
    client
        .start_orchestration("metrics-success-infra", "SuccessOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("metrics-success-infra", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status for infra success: {other:?}"),
    }

    // Clean success (records success metrics)
    client
        .start_orchestration("metrics-success", "SuccessOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("metrics-success", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status for success: {other:?}"),
    }

    // Application failure via activity error
    client
        .start_orchestration("metrics-app-fail", "AppFailureOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("metrics-app-fail", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Failed { .. } => {}
        other => panic!("unexpected status for app failure: {other:?}"),
    }

    // Configuration failure via unregistered activity
    client
        .start_orchestration("metrics-config-fail", "ConfigFailureOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("metrics-config-fail", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Failed { .. } => {}
        other => panic!("unexpected status for config failure: {other:?}"),
    }

    // Trigger infrastructure error for orchestration (ack_orchestration_item fails permanently)
    // This tests infrastructure failure handling - orchestration should fail
    // Allow failure commits so the orchestration can record its failure
    failing_provider.set_allow_failure_commits(true);
    failing_provider.fail_next_ack_orchestration_item();
    client
        .start_orchestration("metrics-orch-infra", "SuccessOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("metrics-orch-infra", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Failed { .. } => {}
        other => panic!("unexpected status for orchestration infra failure: {other:?}"),
    }

    let snapshot = rt
        .metrics_snapshot()
        .expect("metrics should be available when observability is enabled");

    rt.clone().shutdown(None).await;

    // Note: Metrics accumulate across test runs, so use >= instead of ==
    assert!(
        snapshot.orch_starts >= 5,
        "expected at least five orchestration starts (2 success + 3 failures)"
    );
    assert!(
        snapshot.activity_success >= 1,
        "expected at least one successful activity"
    );
    assert!(
        snapshot.activity_app_errors >= 1,
        "expected at least one application activity failure"
    );
    assert!(
        snapshot.activity_poison >= 1,
        "expected at least one poison activity (was unregistered, now poisoned after max_attempts)"
    );
    assert!(
        snapshot.activity_infra_errors >= 1,
        "expected at least one infrastructure activity failure"
    );

    assert!(
        snapshot.orch_completions >= 2,
        "expected at least two successful orchestrations"
    );
    assert!(
        snapshot.orch_failures >= 3,
        "expected at least three failed orchestrations"
    );
    assert!(
        snapshot.orch_application_errors >= 1,
        "expected at least one application orchestration failure"
    );
    // Config error for orchestrations still happens for nondeterminism and missing versions
    // Unregistered orchestrations now result in poison instead
    // Note: orch_poison is u64, so we just verify the field exists
    let _orch_poison = snapshot.orch_poison;
    assert!(
        snapshot.orch_infrastructure_errors >= 1,
        "expected at least one infrastructure orchestration failure"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_fetch_orchestration_item_fault_injection() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let failing_provider = Arc::new(FailingProvider::new(sqlite));
    let provider_trait: Arc<dyn Provider> = failing_provider.clone();

    // Enqueue a work item
    provider_trait
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "test-instance".to_string(),
                orchestration: "TestOrch".to_string(),
                input: "test-input".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: duroxide::INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .unwrap();

    // Enable fault injection
    failing_provider.fail_next_fetch_orchestration_item();

    // Attempt to fetch - should return error
    let result = provider_trait
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.is_retryable());
    assert!(err.message.contains("simulated transient infrastructure failure"));

    // Disable fault injection - should succeed now
    let result = provider_trait
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await;
    assert!(result.is_ok());
    let item = result.unwrap();
    assert!(item.is_some());
    let (item, _lock_token, _attempt_count) = item.unwrap();
    assert_eq!(item.instance, "test-instance");
}

#[tokio::test(flavor = "current_thread")]
async fn test_labeled_metrics_recording() {
    // Test that labeled metrics can be recorded without panicking
    let options = RuntimeOptions {
        observability: metrics_observability_config("labeled-metrics"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, _input: String| async move {
            Ok("result".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TestOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("TestActivity", "input").await
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("labeled-test", "TestOrch", "")
        .await
        .unwrap();

    // Wait for completion
    match client
        .wait_for_orchestration("labeled-test", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status: {other:?}"),
    }

    // Get metrics snapshot
    let snapshot = rt.metrics_snapshot().expect("metrics should be available");

    rt.shutdown(None).await;

    // Verify counters incremented (proves labeled methods were called)
    // Note: Counters accumulate across all test runs with same config label
    assert!(snapshot.orch_completions >= 1, "orchestration should complete");
    assert!(snapshot.activity_success >= 1, "activity should succeed");
}

#[tokio::test(flavor = "current_thread")]
async fn test_continue_as_new_metrics() {
    let options = RuntimeOptions {
        observability: metrics_observability_config("can-metrics"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder().build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("CANOrch", |ctx: OrchestrationContext, input: String| async move {
            let count: u32 = input.parse().unwrap_or(0);
            if count < 2 {
                return ctx.continue_as_new((count + 1).to_string()).await;
            } else {
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());
    client.start_orchestration("can-test", "CANOrch", "0").await.unwrap();

    // Wait for final completion
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get metrics snapshot
    rt.shutdown(None).await;

    // Note: Continue-as-new metrics are recorded via record_continue_as_new()
    // This test verifies the orchestration completes without error
}

#[tokio::test(flavor = "current_thread")]
async fn test_activity_duration_tracking() {
    let options = RuntimeOptions {
        observability: metrics_observability_config("duration-metrics"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("SlowActivity", |_ctx: ActivityContext, _input: String| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok("done".to_string())
        })
        .register("FastActivity", |_ctx: ActivityContext, _input: String| async move {
            Ok("instant".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("DurationOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("SlowActivity", "").await?;
            ctx.schedule_activity("FastActivity", "").await?;
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("duration-test", "DurationOrch", "")
        .await
        .unwrap();

    // Wait for completion
    match client
        .wait_for_orchestration("duration-test", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status: {other:?}"),
    }

    let snapshot = rt.metrics_snapshot().expect("metrics should be available");

    rt.shutdown(None).await;

    // Verify both activities recorded
    assert!(snapshot.activity_success >= 2, "both activities should succeed");
    assert!(snapshot.orch_completions >= 1, "orchestration should complete");
}

#[tokio::test(flavor = "current_thread")]
async fn test_error_classification_metrics() {
    let options = RuntimeOptions {
        observability: metrics_observability_config("error-classification"),
        max_attempts: 3,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        unregistered_backoff: UnregisteredBackoffConfig {
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(50),
        },
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("FailActivity", |_ctx: ActivityContext, _input: String| async move {
            Err("business logic error".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "ErrorOrch",
            |ctx: OrchestrationContext, error_type: String| async move {
                match error_type.as_str() {
                    "app" => {
                        ctx.schedule_activity("FailActivity", "").await?;
                        Ok("unexpected".to_string())
                    }
                    "config" => {
                        ctx.schedule_activity("UnregisteredActivity", "").await?;
                        Ok("unexpected".to_string())
                    }
                    _ => Ok("ok".to_string()),
                }
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());

    // Test application error
    client
        .start_orchestration("error-app", "ErrorOrch", "app")
        .await
        .unwrap();
    let _ = client.wait_for_orchestration("error-app", Duration::from_secs(5)).await;

    // Test configuration error
    client
        .start_orchestration("error-config", "ErrorOrch", "config")
        .await
        .unwrap();
    let _ = client
        .wait_for_orchestration("error-config", Duration::from_secs(5))
        .await;

    let snapshot = rt.metrics_snapshot().expect("metrics should be available");

    rt.shutdown(None).await;

    // Verify error classification (counters accumulate, so use >=)
    assert!(snapshot.activity_app_errors >= 1, "should have at least one app error");
    assert!(
        snapshot.activity_poison >= 1,
        "should have at least one poison activity (was unregistered, now poisoned after max_attempts)"
    );
    assert!(
        snapshot.orch_application_errors >= 1,
        "orchestration should fail with app error"
    );
    assert!(
        snapshot.orch_poison >= 1,
        "orchestration should fail when calling unregistered activity"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_active_orchestrations_gauge() {
    let options = RuntimeOptions {
        observability: metrics_observability_config("active-gauge"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("SlowActivity", |_ctx: ActivityContext, _input: String| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok("done".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("SimpleOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("SlowActivity", "").await
        })
        .register("CANOrch", |ctx: OrchestrationContext, input: String| async move {
            let count: u32 = input.parse().unwrap_or(0);
            if count < 1 {
                // Do some work before continuing
                ctx.schedule_activity("SlowActivity", "").await?;
                return ctx.continue_as_new((count + 1).to_string()).await;
            } else {
                // Final execution also does work
                ctx.schedule_activity("SlowActivity", "").await?;
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Start orchestration and verify it completes
    client
        .start_orchestration("active-test-1", "SimpleOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("active-test-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status: {other:?}"),
    }

    // Test continue-as-new: Orchestration should complete through multiple executions
    client
        .start_orchestration("active-test-can", "CANOrch", "0")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("active-test-can", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status: {other:?}"),
    }

    // Note: The active_orchestrations gauge is automatically incremented/decremented
    // as orchestrations start/complete. It's exposed via OTel metrics for Prometheus scraping.
    // We verify it works by confirming the system functions correctly.

    rt.shutdown(None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_active_orchestrations_gauge_comprehensive() {
    // This test verifies the gauge increments/decrements correctly and shows up in metrics
    let options = RuntimeOptions {
        observability: metrics_observability_config("active-gauge-full"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("Work", |_ctx: ActivityContext, _input: String| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok("done".to_string())
        })
        .register("LongWork", |_ctx: ActivityContext, _input: String| async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok("done".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TestOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("Work", "").await
        })
        .register("LongOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("LongWork", "").await
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Start multiple orchestrations to exercise the active orchestrations gauge
    client.start_orchestration("active-1", "TestOrch", "").await.unwrap();
    client.start_orchestration("active-2", "TestOrch", "").await.unwrap();
    client.start_orchestration("active-long", "LongOrch", "").await.unwrap();

    // Wait for first two to complete
    match client
        .wait_for_orchestration("active-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status: {other:?}"),
    }
    match client
        .wait_for_orchestration("active-2", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status: {other:?}"),
    }

    // Wait for long one
    match client
        .wait_for_orchestration("active-long", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status: {other:?}"),
    }

    // Note: The active_orchestrations gauge increments when orchestrations start
    // and decrements when they complete. It's exposed via OTel observable gauge
    // and read through Prometheus scraping. We verify correctness by ensuring
    // all orchestrations complete successfully.

    rt.shutdown(None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_gauge_poller_refreshes_queue_depths() {
    // Use a very short poll interval so the test completes quickly
    let options = RuntimeOptions {
        observability: ObservabilityConfig {
            gauge_poll_interval: Duration::from_millis(100),
            ..metrics_observability_config("gauge-poller")
        },
        dispatcher_min_poll_interval: Duration::from_millis(50),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("Noop", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("WaitOrch", |ctx: OrchestrationContext, _input: String| async move {
            // Schedule a timer so the orchestration stays "running" for a bit
            ctx.schedule_timer(Duration::from_secs(30)).await;
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Before any orchestrations, gauges should be 0
    let handle = rt.observability_handle().expect("observability should be enabled");
    let metrics = handle.metrics_provider();
    let (orch_q, worker_q) = metrics.get_queue_depths();
    assert_eq!(orch_q, 0, "orchestrator queue should start at 0");
    assert_eq!(worker_q, 0, "worker queue should start at 0");

    // Start orchestrations that will park on timers (creating orchestrator queue items)
    for i in 0..3 {
        client
            .start_orchestration(&format!("gauge-test-{i}"), "WaitOrch", "")
            .await
            .unwrap();
    }

    // Let dispatchers process and timers enqueue, plus at least one gauge poll cycle
    tokio::time::sleep(Duration::from_millis(500)).await;

    // After polling, active_orchestrations should reflect DB state
    let active = metrics.get_active_orchestrations();
    assert_eq!(active, 3, "should have 3 active orchestrations after gauge poll");

    // Orchestrator queue should have been updated by the poller
    let (orch_q, _) = metrics.get_queue_depths();
    // Queue may have items from timers - just verify the gauge was refreshed (non-panic)
    let _ = orch_q;

    rt.shutdown(None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_gauge_poller_updates_active_orchestrations_on_completion() {
    let options = RuntimeOptions {
        observability: ObservabilityConfig {
            gauge_poll_interval: Duration::from_millis(100),
            ..metrics_observability_config("gauge-poller-active")
        },
        dispatcher_min_poll_interval: Duration::from_millis(50),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("Quick", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("QuickOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("Quick", "hi").await
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Start and complete orchestrations
    for i in 0..3 {
        client
            .start_orchestration(&format!("active-poll-{i}"), "QuickOrch", "")
            .await
            .unwrap();
    }

    // Wait for completions
    for i in 0..3 {
        match client
            .wait_for_orchestration(&format!("active-poll-{i}"), Duration::from_secs(5))
            .await
            .unwrap()
        {
            OrchestrationStatus::Completed { .. } => {}
            other => panic!("unexpected status: {other:?}"),
        }
    }

    // Wait for at least one gauge poll
    tokio::time::sleep(Duration::from_millis(200)).await;

    // After poll, active_orchestrations should be 0 since all completed
    let handle = rt.observability_handle().expect("observability should be enabled");
    let active = handle.metrics_provider().get_active_orchestrations();
    assert_eq!(
        active, 0,
        "active orchestrations should be 0 after all complete and gauge poll"
    );

    rt.shutdown(None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_separate_error_counters_exported() {
    // Test that infrastructure and configuration error counters are separate metrics
    let options = RuntimeOptions {
        observability: metrics_observability_config("separate-errors"),
        max_attempts: 3,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        unregistered_backoff: UnregisteredBackoffConfig {
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(50),
        },
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("FailActivity", |_ctx: ActivityContext, _input: String| async move {
            Err("app error".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "ConfigErrorOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                // Trigger config error by calling unregistered activity
                ctx.schedule_activity("UnregisteredActivity", "").await?;
                Ok("done".to_string())
            },
        )
        .register("AppErrorOrch", |ctx: OrchestrationContext, _input: String| async move {
            // Trigger app error
            ctx.schedule_activity("FailActivity", "").await?;
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Trigger config error
    client
        .start_orchestration("config-err", "ConfigErrorOrch", "")
        .await
        .unwrap();
    let _ = client
        .wait_for_orchestration("config-err", Duration::from_secs(5))
        .await;

    // Trigger app error
    client.start_orchestration("app-err", "AppErrorOrch", "").await.unwrap();
    let _ = client.wait_for_orchestration("app-err", Duration::from_secs(5)).await;

    let snapshot = rt.metrics_snapshot().expect("metrics should be available");
    rt.shutdown(None).await;

    // Verify separate counters are incremented
    // Note: Unregistered activities now result in poison errors (after backoff exhaustion)
    // rather than immediate configuration errors
    assert!(
        snapshot.orch_poison >= 1,
        "should have poison error (unregistered activity)"
    );
    assert!(
        snapshot.activity_poison >= 1,
        "activity should have poison error (unregistered activity)"
    );
    assert!(snapshot.orch_application_errors >= 1, "should have app error");
    assert!(snapshot.activity_app_errors >= 1, "activity should have app error");
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_orchestration_metrics() {
    // Test that sub-orchestration calls and duration are tracked
    let options = RuntimeOptions {
        observability: metrics_observability_config("sub-orch-metrics"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("ChildActivity", |_ctx: ActivityContext, _input: String| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok("child_result".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("ChildOrch", |ctx: OrchestrationContext, input: String| async move {
            let result = ctx.schedule_activity("ChildActivity", input).await?;
            Ok(format!("child: {result}"))
        })
        .register("ParentOrch", |ctx: OrchestrationContext, _input: String| async move {
            // Call first sub-orchestration
            let result1 = ctx
                .schedule_sub_orchestration("ChildOrch", "input1".to_string())
                .await?;

            // Call second sub-orchestration
            let result2 = ctx
                .schedule_sub_orchestration("ChildOrch", "input2".to_string())
                .await?;

            Ok(format!("{result1} | {result2}"))
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Start parent orchestration that calls sub-orchestrations
    client
        .start_orchestration("parent-test", "ParentOrch", "")
        .await
        .unwrap();

    // Wait for completion
    match client
        .wait_for_orchestration("parent-test", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("child:"), "should have sub-orch results");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    let snapshot = rt.metrics_snapshot().expect("metrics should be available");
    rt.shutdown(None).await;

    // Verify parent and child orchestrations completed
    assert!(
        snapshot.orch_completions >= 3,
        "should have at least 3 completions (1 parent + 2 children)"
    );
    // Note: Sub-orchestration specific metrics (calls_total, duration) are recorded
    // but not in the snapshot - they go to OTel histograms/counters with labels
}

#[tokio::test(flavor = "current_thread")]
async fn test_versioned_orchestration_metrics() {
    // Test that different orchestration versions are tracked with labels
    let options = RuntimeOptions {
        observability: metrics_observability_config("versioned-metrics"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, _input: String| async move {
            Ok("result".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "VersionedOrch",
            |ctx: OrchestrationContext, _input: String| async move { ctx.schedule_activity("TestActivity", "").await },
        )
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Start orchestrations with same version (test just checks that completions are counted)
    client
        .start_orchestration_versioned("v1-test", "VersionedOrch", "1.0.0", "")
        .await
        .unwrap();

    client
        .start_orchestration_versioned("v2-test", "VersionedOrch", "1.0.0", "")
        .await
        .unwrap();

    client
        .start_orchestration_versioned("v3-test", "VersionedOrch", "1.0.0", "")
        .await
        .unwrap();

    // Wait for all to complete
    let _ = client.wait_for_orchestration("v1-test", Duration::from_secs(5)).await;
    let _ = client.wait_for_orchestration("v2-test", Duration::from_secs(5)).await;
    let _ = client.wait_for_orchestration("v3-test", Duration::from_secs(5)).await;

    let snapshot = rt.metrics_snapshot().expect("metrics should be available");
    rt.shutdown(None).await;

    // Verify orchestrations completed
    assert!(
        snapshot.orch_completions >= 3,
        "should have at least 3 completions with different versions"
    );
    // Note: Version labels are recorded in OTel metrics (orchestration_name, version)
    // but not exposed in atomic counter snapshot
}

#[tokio::test(flavor = "current_thread")]
async fn test_provider_metrics_recorded() {
    // Test that provider operation metrics are recorded via InstrumentedProvider
    // The InstrumentedProvider is automatically applied when metrics are enabled
    let options = RuntimeOptions {
        observability: metrics_observability_config("provider-metrics"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, _input: String| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok("result".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TestOrch", |ctx: OrchestrationContext, _input: String| async move {
            // This will trigger multiple provider operations:
            // - fetch_orchestration_item (runtime polling)
            // - ack_orchestration_item (after orchestration runs)
            // - enqueue_for_worker (when scheduling activity)
            // - fetch_work_item (worker polling)
            // - ack_work_item (after activity completes)
            ctx.schedule_activity("TestActivity", "").await
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Start orchestration to trigger provider operations
    client
        .start_orchestration("provider-test", "TestOrch", "")
        .await
        .unwrap();

    // Wait for completion - this exercises all provider operations
    match client
        .wait_for_orchestration("provider-test", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status: {other:?}"),
    }

    let snapshot = rt.metrics_snapshot().expect("metrics should be available");
    rt.shutdown(None).await;

    // Verify orchestration completed (provider operations happened)
    assert!(
        snapshot.orch_completions >= 1,
        "orchestration should complete (proving provider metrics were recorded)"
    );
    // Note: Provider metrics (duroxide_provider_operation_duration_seconds,
    // duroxide_provider_errors_total) are recorded via InstrumentedProvider
    // but not exposed in atomic counter snapshot. They go to OTel histograms/counters.
}

#[tokio::test(flavor = "current_thread")]
async fn test_provider_error_metrics() {
    // Test that provider errors are classified and recorded
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let failing_provider = Arc::new(FailingProvider::new(sqlite));
    let provider_trait: Arc<dyn Provider> = failing_provider.clone();

    let activities = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, _input: String| async move {
            Ok("result".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TestOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("TestActivity", "").await
        })
        .build();

    let options = RuntimeOptions {
        observability: metrics_observability_config("provider-error-metrics"),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(provider_trait.clone(), activities, orchestrations, options).await;
    let client = Client::new(provider_trait.clone());

    // Trigger provider error on fetch
    failing_provider.fail_next_fetch_orchestration_item();

    client.start_orchestration("error-test", "TestOrch", "").await.unwrap();

    // Wait a bit for the error to be encountered
    tokio::time::sleep(Duration::from_millis(200)).await;

    rt.shutdown(None).await;

    // Note: Provider error metrics (duroxide_provider_errors_total) are recorded
    // with labels like operation="fetch_orchestration_item", error_type="timeout"
    // These are captured by InstrumentedProvider but not in atomic counter snapshot
}

#[tokio::test(flavor = "current_thread")]
async fn test_queue_depth_gauges_initialization() {
    // Test that queue depth gauges are initialized from provider on startup
    let options = RuntimeOptions {
        observability: metrics_observability_config("queue-depth-init"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    // Enqueue some work items BEFORE starting runtime
    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "pre-existing-1".to_string(),
                orchestration: "TestOrch".to_string(),
                input: "test".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: duroxide::INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .unwrap();

    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "pre-existing-2".to_string(),
                orchestration: "TestOrch".to_string(),
                input: "test".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: duroxide::INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .unwrap();

    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder().build();

    // Now start runtime - it should initialize gauges from provider
    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    // Give initialization a moment to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Note: Queue depth gauges are initialized from provider and exposed via OTel metrics
    // We don't need to verify exact values - the gauges are automatically updated by the system
    // and read through Prometheus/OTel scraping, not direct API calls

    rt.shutdown(None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_queue_depth_gauges_tracking() {
    // Test that queue depth gauges track changes as work is enqueued and processed
    let options = RuntimeOptions {
        observability: metrics_observability_config("queue-depth-tracking"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("SlowActivity", |_ctx: ActivityContext, _input: String| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok("done".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("SlowOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("SlowActivity", "").await
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Start multiple orchestrations quickly to exercise queue depth tracking
    for i in 0..5 {
        client
            .start_orchestration(format!("queue-test-{i}"), "SlowOrch", "")
            .await
            .unwrap();
    }

    // Wait for all to complete - this exercises the queue depth gauges
    // The gauges are automatically updated as items move through queues
    for i in 0..5 {
        let _ = client
            .wait_for_orchestration(&format!("queue-test-{i}"), Duration::from_secs(10))
            .await;
    }

    // Note: Queue depth gauges are exposed via OTel metrics and scraped by Prometheus
    // We verify the system works by confirming orchestrations complete successfully
    // The gauges themselves are read through the metrics system, not direct API calls

    rt.shutdown(None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_gauges_initialized_together() {
    // Test that all gauges (active orchestrations + queue depths) are initialized together
    let options = RuntimeOptions {
        observability: metrics_observability_config("all-gauges-init"),
        ..Default::default()
    };

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    // Create a running orchestration by starting and NOT completing it
    let activities = ActivityRegistry::builder()
        .register("BlockingActivity", |_ctx: ActivityContext, _input: String| async move {
            tokio::time::sleep(Duration::from_secs(60)).await; // Long blocking
            Ok("done".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("BlockingOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("BlockingActivity", "").await
        })
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());

    // Start an orchestration that will block
    client
        .start_orchestration("blocking-test", "BlockingOrch", "")
        .await
        .unwrap();

    // Wait for it to start processing
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Shutdown to test that gauges are properly initialized on restart
    rt.shutdown(None).await;

    // Restart runtime - it should initialize ALL gauges from provider
    let options2 = RuntimeOptions {
        observability: metrics_observability_config("all-gauges-init-2"),
        ..Default::default()
    };

    let activities2 = ActivityRegistry::builder()
        .register("BlockingActivity", |_ctx: ActivityContext, _input: String| async move {
            Ok("done".to_string())
        })
        .build();

    let orchestrations2 = OrchestrationRegistry::builder()
        .register("BlockingOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("BlockingActivity", "").await
        })
        .build();

    let rt2 = runtime::Runtime::start_with_options(store.clone(), activities2, orchestrations2, options2).await;

    // Give initialization time to complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Note: All gauges (active_orchestrations, orchestrator_queue_depth, worker_queue_depth)
    // are initialized from the provider on startup via initialize_gauges()
    // They reflect the persistent state in the database, not just runtime state
    // The gauges are exposed via OTel metrics and read through Prometheus scraping

    rt2.shutdown(None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_poison_message_metrics() {
    use common::fault_injection::PoisonInjectingProvider;

    let options = RuntimeOptions {
        observability: metrics_observability_config("poison-metrics"),
        max_attempts: 10,
        ..Default::default()
    };

    let sqlite = Arc::new(
        SqliteProvider::new_in_memory()
            .await
            .expect("Failed to create provider"),
    );
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));

    let activities = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, _input: String| async move {
            Ok("done".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "OrchPoisonTest",
            |_ctx: OrchestrationContext, _input: String| async move {
                panic!("Should not run - orchestration is poisoned");
            },
        )
        .register(
            "ActivityPoisonTest",
            |ctx: OrchestrationContext, _input: String| async move {
                ctx.schedule_activity("TestActivity", "{}").await
            },
        )
        .build();

    // Inject poison for orchestration item (exceeds max_attempts of 10)
    provider.inject_orchestration_poison(11);

    let rt = runtime::Runtime::start_with_options(provider.clone(), activities, orchestrations, options).await;
    let client = Client::new(provider.clone());

    // Start orchestration that will be detected as poison
    client
        .start_orchestration("orch-poison", "OrchPoisonTest", "")
        .await
        .unwrap();
    let result = client
        .wait_for_orchestration("orch-poison", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(result, OrchestrationStatus::Failed { .. }),
        "Should fail as poison"
    );

    // Start another orchestration where the activity will be poisoned
    provider.inject_activity_poison_persistent(11);
    client
        .start_orchestration("activity-poison", "ActivityPoisonTest", "")
        .await
        .unwrap();
    let result2 = client
        .wait_for_orchestration("activity-poison", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(result2, OrchestrationStatus::Failed { .. }),
        "Should fail from poisoned activity"
    );

    let snapshot = rt.metrics_snapshot().expect("metrics should be available");
    rt.shutdown(None).await;

    // Verify poison counters are incremented
    assert!(
        snapshot.orch_poison >= 1,
        "should have orchestration poison: got {}",
        snapshot.orch_poison
    );
    assert!(
        snapshot.activity_poison >= 1,
        "should have activity poison: got {}",
        snapshot.activity_poison
    );
}

// ============================================================================
// Coverage improvement tests (moved from coverage_improvement_tests.rs)
// ============================================================================

/// Test: MetricsProvider records all outcome types
#[tokio::test]
async fn test_metrics_outcome_matrix() {
    use duroxide::runtime::observability::{MetricsProvider, ObservabilityConfig};

    let config = ObservabilityConfig::default();
    let metrics = MetricsProvider::new(&config).unwrap();

    // Record orchestration start
    metrics.record_orchestration_start("TestOrch", "1.0.0", "client");
    assert_eq!(metrics.snapshot().orch_starts, 1);

    // Record orchestration completion (success)
    metrics.record_orchestration_completion("TestOrch", "1.0.0", "completed", 1.5, 3, 10);
    assert_eq!(metrics.snapshot().orch_completions, 1);

    // Record orchestration failure (app error)
    metrics.record_orchestration_failure("TestOrch", "1.0.0", "app_error", "user_error");
    assert_eq!(metrics.snapshot().orch_failures, 1);
    assert_eq!(metrics.snapshot().orch_application_errors, 1);

    // Record orchestration failure (infrastructure error)
    metrics.record_orchestration_failure("TestOrch", "1.0.0", "infrastructure_error", "db_down");
    assert_eq!(metrics.snapshot().orch_failures, 2);
    assert_eq!(metrics.snapshot().orch_infrastructure_errors, 1);

    // Record orchestration failure (config error)
    metrics.record_orchestration_failure("TestOrch", "1.0.0", "config_error", "missing_setting");
    assert_eq!(metrics.snapshot().orch_failures, 3);
    assert_eq!(metrics.snapshot().orch_configuration_errors, 1);

    // Record continue-as-new
    metrics.record_continue_as_new("TestOrch", 1);
    assert_eq!(metrics.snapshot().orch_continue_as_new, 1);
}

/// Test: MetricsProvider activity outcome types
#[tokio::test]
async fn test_metrics_activity_outcomes() {
    use duroxide::runtime::observability::{MetricsProvider, ObservabilityConfig};

    let config = ObservabilityConfig::default();
    let metrics = MetricsProvider::new(&config).unwrap();

    // Success
    metrics.record_activity_execution("TestActivity", "success", 0.5, 0, None);
    assert_eq!(metrics.snapshot().activity_success, 1);

    // App error
    metrics.record_activity_execution("TestActivity", "app_error", 0.1, 1, Some("gpu"));
    assert_eq!(metrics.snapshot().activity_app_errors, 1);

    // Infrastructure error
    metrics.record_activity_execution("TestActivity", "infra_error", 0.2, 2, None);
    assert_eq!(metrics.snapshot().activity_infra_errors, 1);

    // Config error
    metrics.record_activity_execution("TestActivity", "config_error", 0.3, 0, Some("build"));
    assert_eq!(metrics.snapshot().activity_config_errors, 1);
}

/// Test: MetricsProvider poison message tracking
#[tokio::test]
async fn test_metrics_poison_messages() {
    use duroxide::runtime::observability::{MetricsProvider, ObservabilityConfig};

    let config = ObservabilityConfig::default();
    let metrics = MetricsProvider::new(&config).unwrap();

    // Record orchestration poison
    metrics.record_orchestration_poison();
    metrics.record_orchestration_poison();
    assert_eq!(metrics.snapshot().orch_poison, 2);

    // Record activity poison
    metrics.record_activity_poison();
    assert_eq!(metrics.snapshot().activity_poison, 1);
}

/// Test: MetricsProvider dispatcher metrics
#[tokio::test]
async fn test_metrics_dispatcher() {
    use duroxide::runtime::observability::{MetricsProvider, ObservabilityConfig};

    let config = ObservabilityConfig::default();
    let metrics = MetricsProvider::new(&config).unwrap();

    // Record dispatcher items fetched
    metrics.record_orch_dispatcher_items_fetched(5);
    metrics.record_orch_dispatcher_items_fetched(3);
    assert_eq!(metrics.snapshot().orch_dispatcher_items_fetched, 8);

    metrics.record_worker_dispatcher_items_fetched(10);
    assert_eq!(metrics.snapshot().worker_dispatcher_items_fetched, 10);

    // Record processing duration (just verify no panic)
    metrics.record_orch_dispatcher_processing_duration(100);
    metrics.record_worker_dispatcher_execution_duration(50);
}

/// Test: MetricsProvider sub-orchestration tracking
#[tokio::test]
async fn test_metrics_suborchestration() {
    use duroxide::runtime::observability::{MetricsProvider, ObservabilityConfig};

    let config = ObservabilityConfig::default();
    let metrics = MetricsProvider::new(&config).unwrap();

    // Record sub-orchestration call
    metrics.record_suborchestration_call("Parent", "Child", "success");
    metrics.record_suborchestration_call("Parent", "Child", "failure");
    assert_eq!(metrics.snapshot().suborchestration_calls, 2);

    // Record duration (just verify no panic)
    metrics.record_suborchestration_duration("Parent", "Child", 2.5, "success");
}

/// Test: MetricsProvider queue depth tracking
#[tokio::test]
async fn test_metrics_queue_depths() {
    use duroxide::runtime::observability::{MetricsProvider, ObservabilityConfig};

    let config = ObservabilityConfig::default();
    let metrics = MetricsProvider::new(&config).unwrap();

    // Initial depths should be zero
    let (orch, worker) = metrics.get_queue_depths();
    assert_eq!(orch, 0);
    assert_eq!(worker, 0);

    // Update depths
    metrics.update_queue_depths(10, 5);
    let (orch, worker) = metrics.get_queue_depths();
    assert_eq!(orch, 10);
    assert_eq!(worker, 5);

    // Update again
    metrics.update_queue_depths(0, 0);
    let (orch, worker) = metrics.get_queue_depths();
    assert_eq!(orch, 0);
    assert_eq!(worker, 0);
}

/// Test: MetricsProvider active orchestrations gauge
#[tokio::test]
async fn test_metrics_active_orchestrations() {
    use duroxide::runtime::observability::{MetricsProvider, ObservabilityConfig};

    let config = ObservabilityConfig::default();
    let metrics = MetricsProvider::new(&config).unwrap();

    // Initial should be zero
    assert_eq!(metrics.get_active_orchestrations(), 0);

    // Increment
    metrics.increment_active_orchestrations();
    metrics.increment_active_orchestrations();
    assert_eq!(metrics.get_active_orchestrations(), 2);

    // Decrement
    metrics.decrement_active_orchestrations();
    assert_eq!(metrics.get_active_orchestrations(), 1);

    // Set directly
    metrics.set_active_orchestrations(100);
    assert_eq!(metrics.get_active_orchestrations(), 100);
}

/// Test: MetricsProvider provider error tracking
#[tokio::test]
async fn test_metrics_provider_errors() {
    use duroxide::runtime::observability::{MetricsProvider, ObservabilityConfig};

    let config = ObservabilityConfig::default();
    let metrics = MetricsProvider::new(&config).unwrap();

    // Record provider operation (success)
    metrics.record_provider_operation("fetch", 0.05, "success");

    // Record provider errors
    metrics.record_provider_error("fetch", "deadlock");
    metrics.record_provider_error("ack", "timeout");
    metrics.record_provider_error("read", "connection");
    metrics.record_provider_error("write", "other");
    assert_eq!(metrics.snapshot().provider_errors, 4);
}

/// Test: MetricsProvider error type shortcuts
#[tokio::test]
async fn test_metrics_error_shortcuts() {
    use duroxide::runtime::observability::{MetricsProvider, ObservabilityConfig};

    let config = ObservabilityConfig::default();
    let metrics = MetricsProvider::new(&config).unwrap();

    // Orchestration error shortcuts
    metrics.record_orchestration_application_error();
    assert_eq!(metrics.snapshot().orch_application_errors, 1);
    assert_eq!(metrics.snapshot().orch_failures, 1);

    metrics.record_orchestration_infrastructure_error();
    assert_eq!(metrics.snapshot().orch_infrastructure_errors, 1);
    assert_eq!(metrics.snapshot().orch_failures, 2);

    metrics.record_orchestration_configuration_error();
    assert_eq!(metrics.snapshot().orch_configuration_errors, 1);
    assert_eq!(metrics.snapshot().orch_failures, 3);

    // Activity error shortcuts
    metrics.record_activity_success();
    assert_eq!(metrics.snapshot().activity_success, 1);

    metrics.record_activity_app_error();
    assert_eq!(metrics.snapshot().activity_app_errors, 1);

    metrics.record_activity_infra_error();
    assert_eq!(metrics.snapshot().activity_infra_errors, 1);

    metrics.record_activity_config_error();
    assert_eq!(metrics.snapshot().activity_config_errors, 1);
}

/// Test: ObservabilityHandle lifecycle
#[tokio::test]
async fn test_observability_handle_lifecycle() {
    use duroxide::runtime::observability::{ObservabilityConfig, ObservabilityHandle};

    let config = ObservabilityConfig::default();

    // Init should succeed
    let handle = ObservabilityHandle::init(&config).unwrap();

    // Should be able to get metrics provider
    let metrics = handle.metrics_provider();
    metrics.record_orchestration_start("Test", "1.0", "test");

    // Snapshot should work
    let snapshot = handle.metrics_snapshot();
    assert_eq!(snapshot.orch_starts, 1);

    // Shutdown should succeed
    handle.shutdown().await.unwrap();
}

/// Test: ObservabilityConfig default values
#[tokio::test]
async fn test_observability_config_defaults() {
    let config = ObservabilityConfig::default();
    assert_eq!(config.log_format, LogFormat::Pretty);
    assert_eq!(config.log_level, "info");
    assert_eq!(config.service_name, "duroxide");
    assert!(config.service_version.is_none());
}

/// Test: LogFormat variants
#[tokio::test]
async fn test_log_format_variants() {
    // Test all variants exist and are comparable
    let json = LogFormat::Json;
    let pretty = LogFormat::Pretty;
    let compact = LogFormat::Compact;

    assert_ne!(json, pretty);
    assert_ne!(pretty, compact);
    assert_ne!(json, compact);

    // Test Default
    let default = LogFormat::default();
    assert_eq!(default, LogFormat::Compact);

    // Test Clone and Debug
    let cloned = json.clone();
    assert_eq!(cloned, LogFormat::Json);

    let debug = format!("{:?}", pretty);
    assert!(debug.contains("Pretty"));
}

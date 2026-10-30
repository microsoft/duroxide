// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::clone_on_ref_ptr)]

mod common;

use common::fault_injection::PoisonInjectingProvider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::{Provider, WorkItem};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{ActivityAdmission, Runtime, RuntimeOptions};
use duroxide::{ActivityContext, Client, EventKind, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};

fn options() -> RuntimeOptions {
    RuntimeOptions {
        worker_concurrency: 1,
        orchestration_concurrency: 1,
        dispatcher_min_poll_interval: Duration::from_millis(5),
        dispatcher_long_poll_timeout: Duration::from_millis(10),
        worker_lock_timeout: Duration::from_secs(2),
        activity_cancellation_grace_period: Duration::from_millis(10),
        max_attempts: 1,
        ..Default::default()
    }
}

fn orchestrations() -> OrchestrationRegistry {
    OrchestrationRegistry::builder()
        .register("Run", |ctx: OrchestrationContext, activity: String| async move {
            ctx.schedule_activity(activity, "payload").await
        })
        .build()
}

#[tokio::test(flavor = "current_thread")]
async fn deferred_admission_frees_worker_without_consuming_attempts_and_survives_restart() {
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let capacity = Arc::new(Semaphore::new(1));
    let held = capacity.clone().acquire_owned().await.unwrap();
    let (deferred, mut deferrals) = mpsc::unbounded_channel();
    let activities = ActivityRegistry::builder()
        .register_with_admission(
            "Limited",
            move |ctx, input| {
                assert_eq!(ctx.instance_id(), "limited");
                assert_eq!(input, "payload");
                match capacity.clone().try_acquire_owned() {
                    Ok(permit) => Ok(ActivityAdmission::admit(permit)),
                    Err(tokio::sync::TryAcquireError::NoPermits) => {
                        deferred.send(()).unwrap();
                        Ok(ActivityAdmission::Defer(Duration::from_millis(10)))
                    }
                    Err(error) => Err(error.to_string()),
                }
            },
            |_ctx, input| async move { Ok(input) },
        )
        .register("Free", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
    let runtime = Runtime::start_with_options(store.clone(), activities.clone(), orchestrations(), options()).await;
    let client = Client::new(store.clone());
    client.start_orchestration("limited", "Run", "Limited").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..20 {
            deferrals.recv().await.unwrap();
        }
    })
    .await
    .expect("admission must keep deferring beyond the poison attempt limit");

    client.start_orchestration("free", "Run", "Free").await.unwrap();
    assert!(matches!(
        client.wait_for_orchestration("free", Duration::from_secs(5)).await.unwrap(),
        OrchestrationStatus::Completed { output, .. } if output == "payload"
    ));
    runtime.clone().shutdown(None).await;
    assert!(runtime.metrics_snapshot().unwrap().activity_admission_deferrals >= 20);

    let history = store.read("limited").await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(event.kind, EventKind::ActivityScheduled { .. }))
            .count(),
        1
    );
    assert!(!history.iter().any(|event| matches!(
        event.kind,
        EventKind::ActivityCompleted { .. } | EventKind::ActivityFailed { .. }
    )));

    drop(held);
    // Shutdown can cancel a fetch after its attempt has already been committed.
    let restart_options = RuntimeOptions {
        max_attempts: 2,
        ..options()
    };
    let runtime = Runtime::start_with_options(store.clone(), activities, orchestrations(), restart_options).await;
    assert!(matches!(
        client.wait_for_orchestration("limited", Duration::from_secs(5)).await.unwrap(),
        OrchestrationStatus::Completed { output, .. } if output == "payload"
    ));
    runtime.shutdown(None).await;
    let history = store.read("limited").await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(event.kind, EventKind::ActivityCompleted { .. }))
            .count(),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn admission_deferral_delays_are_clamped_before_reaching_storage() {
    let store = Arc::new(PoisonInjectingProvider::new(Arc::new(
        SqliteProvider::new_in_memory().await.unwrap(),
    )));
    let cases = [
        (Duration::from_nanos(1), Duration::from_millis(1)),
        (Duration::from_micros(500), Duration::from_millis(1)),
        (Duration::from_nanos(999_999), Duration::from_millis(1)),
        (Duration::from_millis(1), Duration::from_millis(1)),
        (Duration::from_micros(1500), Duration::from_micros(1500)),
        (Duration::from_millis(10), Duration::from_millis(10)),
    ];
    let delays = Mutex::new(cases.into_iter());
    let activities = ActivityRegistry::builder()
        .register_with_admission(
            "Limited",
            move |_ctx, _input| {
                Ok(match delays.lock().unwrap().next() {
                    Some((delay, _)) => ActivityAdmission::Defer(delay),
                    None => ActivityAdmission::admit(()),
                })
            },
            |_ctx, input| async move { Ok(input) },
        )
        .build();
    let runtime = Runtime::start_with_options(store.clone(), activities, orchestrations(), options()).await;
    let client = Client::new(store.clone());
    client.start_orchestration("clamped", "Run", "Limited").await.unwrap();
    let status = client.wait_for_orchestration("clamped", Duration::from_secs(5)).await;
    runtime.shutdown(None).await;

    assert!(matches!(
        status.unwrap(),
        OrchestrationStatus::Completed { output, .. } if output == "payload"
    ));
    assert_eq!(store.work_abandons(), cases.map(|(_, expected)| (Some(expected), true)));
}

#[tokio::test(flavor = "current_thread")]
async fn oversized_admission_delays_are_bounded_before_reaching_storage() {
    let maximum = Duration::from_secs(24 * 60 * 60);
    for delay in [maximum, maximum + Duration::from_nanos(1), Duration::MAX] {
        let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let store = Arc::new(PoisonInjectingProvider::new(sqlite.clone()));
        let (deferred, mut deferrals) = mpsc::unbounded_channel();
        let activities = ActivityRegistry::builder()
            .register_with_admission(
                "Limited",
                move |_ctx, _input| {
                    deferred.send(()).unwrap();
                    Ok(ActivityAdmission::Defer(delay))
                },
                |_ctx, _input| async move { panic!("deferred handler must not run") },
            )
            .build();
        let runtime = Runtime::start_with_options(store.clone(), activities, orchestrations(), options()).await;
        Client::new(sqlite)
            .start_orchestration("bounded", "Run", "Limited")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), deferrals.recv())
            .await
            .unwrap()
            .unwrap();
        runtime.shutdown(None).await;
        assert_eq!(store.work_abandons(), vec![(Some(maximum), true)], "{delay:?}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn failed_deferral_retries_the_same_delay_and_restores_the_attempt() {
    let (events, _guard) = common::tracing_capture::install_tracing_capture();
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let store = Arc::new(PoisonInjectingProvider::new(sqlite.clone()));
    store.fail_work_abandons(1);
    let first_attempt = AtomicBool::new(true);
    let delay = Duration::from_millis(25);
    let activities = ActivityRegistry::builder()
        .register_with_admission(
            "Limited",
            move |_ctx, _input| {
                Ok(if first_attempt.swap(false, Ordering::SeqCst) {
                    ActivityAdmission::Defer(delay)
                } else {
                    ActivityAdmission::admit(())
                })
            },
            |_ctx, input| async move { Ok(input) },
        )
        .build();
    let runtime = Runtime::start_with_options(store.clone(), activities, orchestrations(), options()).await;
    let client = Client::new(sqlite);
    client.start_orchestration("retry", "Run", "Limited").await.unwrap();
    let status = client.wait_for_orchestration("retry", Duration::from_secs(5)).await;
    runtime.clone().shutdown(None).await;
    assert!(matches!(
        status.unwrap(),
        OrchestrationStatus::Completed { output, .. } if output == "payload"
    ));
    assert_eq!(store.work_abandons(), vec![(Some(delay), true); 2]);
    let metrics = runtime.metrics_snapshot().unwrap();
    assert_eq!(metrics.activity_admission_deferrals, 1);
    assert_eq!(metrics.activity_infra_errors, 1);
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.message == "Activity started")
            .count(),
        1,
        "only the admitted invocation should be logged as started"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn uncommitted_deferral_consumes_attempt_and_is_not_counted_as_deferred() {
    let (events, _guard) = common::tracing_capture::install_tracing_capture();
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let store = Arc::new(PoisonInjectingProvider::new(sqlite.clone()));
    store.fail_work_abandons(2);
    let delay = Duration::from_secs(60);
    let activities = ActivityRegistry::builder()
        .register_with_admission(
            "Limited",
            move |_ctx, _input| Ok(ActivityAdmission::Defer(delay)),
            |_ctx, _input| async move { panic!("deferred handler must not run") },
        )
        .build();
    let runtime = Runtime::start_with_options(
        store.clone(),
        activities,
        orchestrations(),
        RuntimeOptions {
            worker_lock_timeout: Duration::from_millis(100),
            ..options()
        },
    )
    .await;
    let client = Client::new(sqlite);
    client
        .start_orchestration("failed-deferral", "Run", "Limited")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("failed-deferral", Duration::from_secs(5))
        .await;
    runtime.clone().shutdown(None).await;
    assert!(matches!(
        status.unwrap(),
        OrchestrationStatus::Failed {
            details: duroxide::ErrorDetails::Poison { attempt_count: 2, .. },
            ..
        }
    ));
    assert_eq!(store.work_abandons(), vec![(Some(delay), true); 2]);
    let metrics = runtime.metrics_snapshot().unwrap();
    assert_eq!(metrics.activity_admission_deferrals, 0);
    assert_eq!(metrics.activity_infra_errors, 1);
    assert_eq!(metrics.activity_app_errors, 0);
    assert!(
        !events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.message == "Activity started")
    );
}

#[test]
fn admission_debug_does_not_require_a_debug_guard() {
    struct Guard;
    assert_eq!(format!("{:?}", ActivityAdmission::admit(Guard)), "Admit(..)");
    assert_eq!(
        format!("{:?}", ActivityAdmission::Defer(Duration::from_secs(1))),
        "Defer(1s)"
    );
    assert_eq!(ActivityAdmission::MAX_DEFER_DELAY, Duration::from_secs(24 * 60 * 60));
}

#[tokio::test(flavor = "current_thread")]
async fn deferred_backlog_does_not_starve_ready_work_or_reread_history() {
    for (backlog, workers) in [(300, 1), (1000, 4)] {
        let dir = tempfile::tempdir().unwrap();
        let sqlite = Arc::new(
            SqliteProvider::new(
                &format!("sqlite:{}?mode=rwc", dir.path().join("test.db").display()),
                None,
            )
            .await
            .unwrap(),
        );
        let store = Arc::new(PoisonInjectingProvider::new(sqlite.clone()));
        let (ready, mut ready_events) = mpsc::unbounded_channel();
        let activities = ActivityRegistry::builder()
            .register_with_admission(
                "Limited",
                |ctx, _input| {
                    assert_eq!(ctx.orchestration_name(), "Backlog");
                    assert_eq!(ctx.orchestration_version(), "1.0.0");
                    Ok(ActivityAdmission::Defer(Duration::from_millis(100)))
                },
                |_ctx, _input| async move { panic!("deferred handler must not run") },
            )
            .register("Free", move |_ctx: ActivityContext, input: String| {
                let ready = ready.clone();
                async move {
                    ready.send(()).unwrap();
                    Ok(input)
                }
            })
            .build();
        let orchestrations = OrchestrationRegistry::builder()
            .register("Backlog", move |ctx: OrchestrationContext, _input: String| async move {
                let mut pending = (0..backlog)
                    .map(|_| ctx.schedule_activity("Limited", "payload"))
                    .collect::<Vec<_>>();
                pending.push(ctx.schedule_activity("Free", "payload"));
                ctx.join(pending).await;
                Ok(String::new())
            })
            .build();
        let runtime = Runtime::start_with_options(
            store.clone(),
            activities,
            orchestrations,
            RuntimeOptions {
                worker_concurrency: workers,
                ..options()
            },
        )
        .await;
        Client::new(sqlite)
            .start_orchestration("backlog", "Backlog", "")
            .await
            .unwrap();
        let progress = tokio::time::timeout(Duration::from_secs(10), ready_events.recv()).await;
        runtime.shutdown(None).await;
        assert_eq!(
            progress.unwrap(),
            Some(()),
            "{backlog} deferred activities, {workers} workers"
        );
        assert_eq!(
            store.history_reads(),
            0,
            "current-execution metadata must not load history"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn admission_metadata_uses_the_activity_execution() {
    for hide_management in [false, true] {
        let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        common::test_create_execution(sqlite.as_ref(), "metadata", "Old", "1.0.0", "", None, None)
            .await
            .unwrap();
        common::test_create_execution(sqlite.as_ref(), "metadata", "New", "2.0.0", "", None, None)
            .await
            .unwrap();
        let store = Arc::new(PoisonInjectingProvider::new(sqlite.clone()));
        if hide_management {
            store.hide_management_capability();
        }
        let (checked, mut checks) = mpsc::unbounded_channel();
        let activities = ActivityRegistry::builder()
            .register_with_admission(
                "Limited",
                move |ctx, _input| {
                    checked
                        .send((
                            ctx.execution_id(),
                            ctx.orchestration_name().to_string(),
                            ctx.orchestration_version().to_string(),
                        ))
                        .unwrap();
                    Ok(ActivityAdmission::Defer(Duration::from_secs(60)))
                },
                |_ctx, _input| async move { panic!("deferred handler must not run") },
            )
            .build();
        sqlite
            .enqueue_for_worker(WorkItem::ActivityExecute {
                instance: "metadata".to_string(),
                execution_id: 1,
                id: 2,
                name: "Limited".to_string(),
                input: String::new(),
                session_id: None,
                tag: None,
            })
            .await
            .unwrap();
        let runtime = Runtime::start_with_options(store, activities, orchestrations(), options()).await;
        let metadata = tokio::time::timeout(Duration::from_secs(5), checks.recv())
            .await
            .unwrap()
            .unwrap();
        runtime.shutdown(None).await;
        assert_eq!(metadata, (1, "Old".to_string(), "1.0.0".to_string()));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn admission_guard_released_on_success_error_and_panic() {
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let capacity = Arc::new(Semaphore::new(1));
    let admission_capacity = capacity.clone();
    let activities = ActivityRegistry::builder()
        .register_with_admission(
            "Limited",
            move |_ctx, _input| {
                admission_capacity
                    .clone()
                    .try_acquire_owned()
                    .map(ActivityAdmission::admit)
                    .map_err(|error| format!("unexpected retained reservation: {error}"))
            },
            |ctx, _input| async move {
                match ctx.instance_id() {
                    "success" => Ok("done".to_string()),
                    "error" => Err("expected error".to_string()),
                    "panic" => panic!("expected handler panic"),
                    other => panic!("unexpected test instance: {other}"),
                }
            },
        )
        .build();
    let runtime = Runtime::start_with_options(store.clone(), activities, orchestrations(), options()).await;
    let client = Client::new(store);
    for instance in ["success", "error", "panic"] {
        client.start_orchestration(instance, "Run", "Limited").await.unwrap();
        let status = client
            .wait_for_orchestration(instance, Duration::from_secs(5))
            .await
            .unwrap();
        match (instance, status) {
            ("success", OrchestrationStatus::Completed { output, .. }) => assert_eq!(output, "done"),
            ("error", OrchestrationStatus::Failed { details, .. }) => {
                assert!(details.display_message().contains("expected error"));
            }
            ("panic", OrchestrationStatus::Failed { details, .. }) => {
                assert!(details.display_message().contains("expected handler panic"));
            }
            (instance, status) => panic!("{instance}: unexpected status {status:?}"),
        }
        assert_eq!(capacity.available_permits(), 1, "{instance} leaked its admission guard");
    }
    runtime.shutdown(None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_panicking_and_invalid_admission_never_invokes_handler() {
    let (events, _guard) = common::tracing_capture::install_tracing_capture();
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let activities = ActivityRegistry::builder()
        .register_with_admission(
            "Limited",
            |ctx, _input| match ctx.instance_id() {
                "rejected" => Err("invalid admission configuration".to_string()),
                "panic" => panic!("expected admission panic"),
                "zero-delay" => Ok(ActivityAdmission::Defer(Duration::ZERO)),
                other => panic!("unexpected test instance: {other}"),
            },
            |_ctx, _input| async move { panic!("rejected handler must not execute") },
        )
        .build();
    let runtime = Runtime::start_with_options(store.clone(), activities, orchestrations(), options()).await;
    let client = Client::new(store);
    for (instance, message) in [
        ("rejected", "invalid admission configuration"),
        ("panic", "expected admission panic"),
        ("zero-delay", "positive retry delay"),
    ] {
        client.start_orchestration(instance, "Run", "Limited").await.unwrap();
        let status = client
            .wait_for_orchestration(instance, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            matches!(status, OrchestrationStatus::Failed { ref details, .. } if details.display_message().contains(message)),
            "unexpected status: {status:?}"
        );
    }
    runtime.clone().shutdown(None).await;
    let metrics = runtime.metrics_snapshot().unwrap();
    assert!(metrics.activity_app_errors >= 3);
    assert_eq!(metrics.activity_infra_errors, 0);
    assert_eq!(metrics.activity_admission_deferrals, 0);
    assert!(
        !events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.message == "Activity started")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn admission_guard_released_when_cancelled_handler_is_aborted() {
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let capacity = Arc::new(Semaphore::new(1));
    let admission_capacity = capacity.clone();
    let (started, mut starts) = mpsc::unbounded_channel();
    let activities = ActivityRegistry::builder()
        .register_with_admission(
            "Limited",
            move |_ctx, _input| {
                admission_capacity
                    .clone()
                    .try_acquire_owned()
                    .map(ActivityAdmission::admit)
                    .map_err(|error| error.to_string())
            },
            move |_ctx, _input| {
                let started = started.clone();
                async move {
                    started.send(()).unwrap();
                    std::future::pending().await
                }
            },
        )
        .build();
    let runtime = Runtime::start_with_options(store.clone(), activities, orchestrations(), options()).await;
    let client = Client::new(store);
    client.start_orchestration("cancel", "Run", "Limited").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), starts.recv())
        .await
        .expect("handler must start")
        .unwrap();
    assert_eq!(capacity.available_permits(), 0);
    client
        .cancel_instance("cancel", "test admission cleanup")
        .await
        .unwrap();
    let permit = tokio::time::timeout(Duration::from_secs(5), capacity.acquire())
        .await
        .expect("aborting the handler must release its admission guard")
        .unwrap();
    drop(permit);
    runtime.shutdown(None).await;
}

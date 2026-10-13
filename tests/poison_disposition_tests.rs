// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Poison metrics distinguish confirmed acknowledgements from acknowledgement
//! errors, including errors returned after a committed write.
//!
//! References: <https://github.com/microsoft/duroxide/issues/47>,
//! <https://github.com/microsoft/duroxide/issues/46>.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

mod common;

use common::fault_injection::PoisonInjectingProvider;
use common::tracing_capture::{CapturedEvent, install_tracing_capture};
use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::observability::MetricsProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{ObservabilityConfig, Runtime, RuntimeOptions};
use duroxide::{Client, OrchestrationContext, OrchestrationRegistry};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use std::sync::Arc;
use std::time::Duration;

const INSTANCE: &str = "poison-disposition";
const MAX_ATTEMPTS: u32 = 10;
const INJECTED_ATTEMPT_COUNT: u32 = MAX_ATTEMPTS + 1;

async fn setup(corrupted_history: bool) -> (Arc<SqliteProvider>, Arc<PoisonInjectingProvider>) {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    if corrupted_history {
        common::seed_instance_with_pinned_version(
            &*sqlite,
            INSTANCE,
            "NeverRuns",
            semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
        )
        .await;
        let updated =
            sqlx::query("UPDATE history SET event_data = '{\"kind\":\"UnknownFutureEvent\"}' WHERE instance_id = ?")
                .bind(INSTANCE)
                .execute(sqlite.get_pool())
                .await
                .unwrap();
        assert_eq!(updated.rows_affected(), 1);
    } else {
        Client::new(sqlite.clone())
            .start_orchestration(INSTANCE, "NeverRuns", "{}")
            .await
            .unwrap();
    }

    let provider = Arc::new(PoisonInjectingProvider::new(sqlite.clone()));
    provider.inject_orchestration_poison_persistent(INJECTED_ATTEMPT_COUNT);
    (sqlite, provider)
}

async fn start(provider: Arc<PoisonInjectingProvider>) -> Arc<Runtime> {
    let orchestrations = OrchestrationRegistry::builder()
        .register("NeverRuns", |_ctx: OrchestrationContext, _input: String| async move {
            panic!("orchestration code must not run for a message detected as poison");
        })
        .build();
    Runtime::start_with_options(
        provider,
        ActivityRegistry::builder().build(),
        orchestrations,
        RuntimeOptions {
            max_attempts: MAX_ATTEMPTS,
            orchestration_concurrency: 1,
            worker_concurrency: 1,
            orchestrator_lock_timeout: Duration::from_millis(50),
            dispatcher_min_poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    )
    .await
}

async fn wait_for_dispositions(rt: &Runtime, confirmed: u64, failed: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = rt.metrics_snapshot().expect("metrics should be available");
            if snapshot.orch_poison >= confirmed && snapshot.orch_poison_failed >= failed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected poison acknowledgement outcomes before the timeout");
}

async fn assert_stored_disposition(sqlite: &SqliteProvider, committed: bool) {
    let failed_events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM history WHERE instance_id = ? AND event_type = 'OrchestrationFailed'")
            .bind(INSTANCE)
            .fetch_one(sqlite.get_pool())
            .await
            .unwrap();
    assert_eq!(failed_events, i64::from(committed));

    let queued: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM orchestrator_queue WHERE instance_id = ?")
        .bind(INSTANCE)
        .fetch_one(sqlite.get_pool())
        .await
        .unwrap();
    assert_eq!(queued, i64::from(!committed));

    if committed {
        let info = sqlite
            .as_management_capability()
            .unwrap()
            .get_instance_info(INSTANCE)
            .await
            .unwrap();
        assert_eq!(info.status, "Failed");
    }
}

fn assert_failure_logs(events: &[CapturedEvent], expected: u64) {
    let failures: Vec<_> = events
        .iter()
        .filter(|event| event.message.starts_with("Failed to acknowledge poison disposition"))
        .collect();
    assert_eq!(failures.len() as u64, expected);
    for event in failures {
        assert_eq!(event.level, tracing::Level::ERROR);
        assert_eq!(
            event.message,
            "Failed to acknowledge poison disposition; termination was not confirmed and the message may be redelivered"
        );
        assert_eq!(event.field("instance").as_deref(), Some(INSTANCE));
        assert_eq!(event.field("attempt_count"), Some(INJECTED_ATTEMPT_COUNT.to_string()));
        assert_eq!(event.field("max_attempts"), Some(MAX_ATTEMPTS.to_string()));
        assert_eq!(
            event.field("error").as_deref(),
            Some("ack_orchestration_item: Invalid lock token")
        );
    }
}

#[derive(Clone, Copy)]
enum AckOutcome {
    Confirmed,
    Rejected,
    ResponseLost,
}

async fn check_disposition(corrupted_history: bool, outcome: AckOutcome) {
    let (events, _capture) = install_tracing_capture();
    let (sqlite, provider) = setup(corrupted_history).await;
    let (confirmed, minimum_failures) = match outcome {
        AckOutcome::Confirmed => (1, 0),
        AckOutcome::Rejected => {
            provider.reject_orchestration_acks(true);
            (0, 2)
        }
        AckOutcome::ResponseLost => {
            provider.fail_next_orchestration_ack_after_commit();
            (0, 1)
        }
    };
    let rt = start(provider.clone()).await;
    wait_for_dispositions(&rt, confirmed, minimum_failures).await;
    rt.clone().shutdown(None).await;

    let snapshot = rt.metrics_snapshot().unwrap();
    let fetches = provider.orchestration_fetches(INSTANCE);
    let expected_failures = match outcome {
        AckOutcome::Confirmed => 0,
        AckOutcome::Rejected => fetches.len() as u64,
        AckOutcome::ResponseLost => 1,
    };
    assert_eq!(snapshot.orch_poison, confirmed);
    assert_eq!(snapshot.orch_poison_failed, expected_failures);
    assert_eq!(
        snapshot.orch_poison + snapshot.orch_poison_failed,
        fetches.len() as u64,
        "each delivery must record exactly one final acknowledgement outcome"
    );
    assert_stored_disposition(&sqlite, !matches!(outcome, AckOutcome::Rejected)).await;
    assert_failure_logs(&events.lock().unwrap(), expected_failures);
}

#[tokio::test(flavor = "current_thread")]
async fn failed_poison_disposition_is_not_counted_as_poisoned() {
    check_disposition(false, AckOutcome::Rejected).await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_corrupted_history_poison_disposition_is_not_counted_as_poisoned() {
    check_disposition(true, AckOutcome::Rejected).await;
}

#[tokio::test(flavor = "current_thread")]
async fn successful_poison_disposition_is_counted_once() {
    check_disposition(false, AckOutcome::Confirmed).await;
}

#[tokio::test(flavor = "current_thread")]
async fn successful_corrupted_history_poison_disposition_is_counted_once() {
    check_disposition(true, AckOutcome::Confirmed).await;
}

#[tokio::test(flavor = "current_thread")]
async fn lost_poison_ack_response_does_not_prove_the_disposition_was_uncommitted() {
    check_disposition(false, AckOutcome::ResponseLost).await;
}

#[tokio::test(flavor = "current_thread")]
async fn lost_corrupted_history_poison_ack_response_does_not_prove_the_disposition_was_uncommitted() {
    check_disposition(true, AckOutcome::ResponseLost).await;
}

async fn check_recovery(corrupted_history: bool) {
    let (events, _capture) = install_tracing_capture();
    let (sqlite, provider) = setup(corrupted_history).await;
    provider.reject_orchestration_acks(true);
    let rt = start(provider.clone()).await;

    wait_for_dispositions(&rt, 0, 2).await;
    assert_eq!(rt.metrics_snapshot().unwrap().orch_poison, 0);
    assert_stored_disposition(&sqlite, false).await;

    provider.reject_orchestration_acks(false);
    wait_for_dispositions(&rt, 1, 2).await;
    rt.clone().shutdown(None).await;

    let snapshot = rt.metrics_snapshot().unwrap();
    let fetches = provider.orchestration_fetches(INSTANCE);
    assert_eq!(snapshot.orch_poison, 1);
    assert_eq!(snapshot.orch_poison_failed, fetches.len() as u64 - 1);
    for deliveries in fetches.windows(2) {
        assert_ne!(deliveries[0].0, deliveries[1].0, "redelivery must use a fresh token");
        assert_eq!(deliveries[1].1, deliveries[0].1 + 1, "fetch attempts must persist");
    }
    assert_stored_disposition(&sqlite, true).await;
    assert_failure_logs(&events.lock().unwrap(), snapshot.orch_poison_failed);
}

#[tokio::test(flavor = "current_thread")]
async fn poison_disposition_recovers_on_redelivery_with_a_fresh_lease() {
    check_recovery(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn corrupted_history_poison_disposition_recovers_on_redelivery_with_a_fresh_lease() {
    check_recovery(true).await;
}

#[test]
fn poison_acknowledgement_counters_are_exported() {
    let recorder = DebuggingRecorder::new();
    let metrics = MetricsProvider::new(&ObservabilityConfig::default()).unwrap();
    metrics::with_local_recorder(&recorder, || {
        metrics.record_orchestration_poison();
        metrics.record_orchestration_poison_failed();
        metrics.record_orchestration_poison_failed();
    });

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.orch_poison, 1);
    assert_eq!(snapshot.orch_poison_failed, 2);
    let exported = recorder.snapshotter().snapshot().into_vec();
    for (name, expected) in [
        ("duroxide_orchestration_poison_total", 1),
        ("duroxide_orchestration_poison_failed_total", 2),
    ] {
        let (key, _, _, value) = exported
            .iter()
            .find(|(key, _, _, _)| key.key().name() == name)
            .expect("poison counter should be exported");
        assert_eq!(*value, DebugValue::Counter(expected));
        assert_eq!(key.key().labels().count(), 0);
    }
}

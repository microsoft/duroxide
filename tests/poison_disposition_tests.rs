// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Regression test for the poison disposition that could not commit.
//!
//! Reference: <https://github.com/microsoft/duroxide/issues/47>
//! (core half of the livelock analysed in <https://github.com/microsoft/duroxide/issues/46>)
//!
//! # Summary
//!
//! When a message exceeds `max_attempts` the runtime marks it as poison. The
//! poison marking, the backoff, and the attempt-count increment all commit
//! through a single ack keyed on the message's *current lock token*. If that
//! lease has expired, the ack fails and all three are lost together, so the
//! message is never terminated -- it becomes visible again and is redelivered
//! for as long as the provider keeps handing out dead leases.
//!
//! Two things were wrong with how the runtime reported that:
//!
//! 1. The ack result was discarded (`let _ = ...`), so a failed disposition was
//!    invisible. The operator saw an "exceeded max attempts, marking as poison"
//!    warning followed by an unrelated-looking "Invalid lock token" warning,
//!    with nothing connecting them or saying the message was still circulating.
//!
//! 2. `record_orchestration_poison()` was called unconditionally afterwards, so
//!    `duroxide_orchestration_poison_total` counted poison *attempts* rather
//!    than messages actually taken out of circulation. In the incident behind
//!    \#46 that meant tens of thousands of increments while zero messages were
//!    ever poisoned.
//!
//! The runtime cannot force a terminal write without a valid lease -- both
//! `ack_orchestration_item` and `abandon_orchestration_item` require one by
//! contract. What it can do, and now does, is refuse to report success it did
//! not achieve, and say plainly what happened.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

mod common;

use common::fault_injection::PoisonInjectingProvider;
use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{LogFormat, ObservabilityConfig, RuntimeOptions};
use duroxide::{Client, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::time::Duration;

const MAX_ATTEMPTS: u32 = 10;
/// Above `MAX_ATTEMPTS`, so the very first delivery takes the poison path.
const INJECTED_ATTEMPT_COUNT: u32 = 11;

fn options(label: &str) -> RuntimeOptions {
    RuntimeOptions {
        observability: ObservabilityConfig {
            log_format: LogFormat::Compact,
            log_level: "error".to_string(),
            service_name: format!("duroxide-poison-disposition-{label}"),
            service_version: Some("test".to_string()),
            ..Default::default()
        },
        max_attempts: MAX_ATTEMPTS,
        dispatcher_min_poll_interval: Duration::from_millis(5),
        ..Default::default()
    }
}

fn registries() -> (ActivityRegistry, OrchestrationRegistry) {
    let orchestrations = OrchestrationRegistry::builder()
        .register("NeverRuns", |_ctx: OrchestrationContext, _input: String| async move {
            panic!("orchestration code must not run for a message detected as poison");
        })
        .build();
    (ActivityRegistry::builder().build(), orchestrations)
}

/// A poison disposition that cannot commit must not be counted as a poisoning.
///
/// The provider here rejects every orchestration ack with a non-retryable
/// `Invalid lock token`, which is exactly what a provider does when the lease it
/// handed out had already expired. The message therefore can never be
/// terminated, and the runtime must say so rather than reporting a poisoning
/// that did not happen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_poison_disposition_is_not_counted_as_poisoned() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));

    // Every delivery looks poison-worthy, and every ack fails as if the lease
    // were dead on arrival. This is the steady state of the #46 livelock.
    provider.inject_orchestration_poison_persistent(INJECTED_ATTEMPT_COUNT);
    provider.expire_orchestration_lease(true);

    let (activities, orchestrations) = registries();
    let rt = runtime::Runtime::start_with_options(
        provider.clone() as Arc<dyn Provider>,
        activities,
        orchestrations,
        options("failed"),
    )
    .await;

    let client = Client::new(provider.clone() as Arc<dyn Provider>);
    client
        .start_orchestration("poison-disposition-fails", "NeverRuns", "{}")
        .await
        .unwrap();

    // Let the dispatcher take several deliveries, each of which enters the
    // poison path and fails to commit.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let snapshot = rt.metrics_snapshot().expect("metrics should be available");
    rt.shutdown(None).await;

    assert_eq!(
        snapshot.orch_poison, 0,
        "no message was taken out of circulation -- every poison ack failed -- so the poison \
         counter must stay at 0, but it read {}. Counting failed dispositions makes a livelocked \
         message look like a handled one.",
        snapshot.orch_poison
    );

    assert!(
        snapshot.orch_poison_failed > 0,
        "a failed poison disposition must be recorded so it is alertable; \
         orch_poison_failed was {}",
        snapshot.orch_poison_failed
    );
}

/// The counterpart: when the ack does commit, the poisoning is real and is
/// counted, and nothing is reported as failed. This guards against "fixing" the
/// case above by simply never recording the metric.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn successful_poison_disposition_is_counted_once() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));

    // Poison-worthy on the first delivery, but the lease is healthy.
    provider.inject_orchestration_poison(INJECTED_ATTEMPT_COUNT);

    let (activities, orchestrations) = registries();
    let rt = runtime::Runtime::start_with_options(
        provider.clone() as Arc<dyn Provider>,
        activities,
        orchestrations,
        options("succeeds"),
    )
    .await;

    let client = Client::new(provider.clone() as Arc<dyn Provider>);
    let instance = "poison-disposition-commits";
    client.start_orchestration(instance, "NeverRuns", "{}").await.unwrap();

    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(5))
        .await
        .expect("the orchestration should reach a terminal state");

    let snapshot = rt.metrics_snapshot().expect("metrics should be available");
    rt.shutdown(None).await;

    assert!(
        matches!(status, duroxide::OrchestrationStatus::Failed { .. }),
        "a poisoned orchestration should end Failed, got: {status:?}"
    );
    assert!(
        snapshot.orch_poison >= 1,
        "a committed poison disposition must be counted; orch_poison was {}",
        snapshot.orch_poison
    );
    assert_eq!(
        snapshot.orch_poison_failed, 0,
        "nothing failed to commit, so orch_poison_failed must stay at 0, but it read {}",
        snapshot.orch_poison_failed
    );
}

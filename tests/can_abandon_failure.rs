// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
#![allow(clippy::expect_used, clippy::clone_on_ref_ptr)]

mod common;

use common::fault_injection::PoisonInjectingProvider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::{ExecutionMetadata, Provider, WorkItem};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{Runtime, RuntimeOptions, UnregisteredBackoffConfig};
use duroxide::{Client, Event, EventKind, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn successor_first_turn_must_obey_poison_budget() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.expect("test store"));
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));
    let max_attempts = 1;
    provider.inject_orchestration_poison_after_skip(1, max_attempts + 1);
    let registry = OrchestrationRegistry::builder()
        .register("Transition", |ctx: OrchestrationContext, _: String| async move {
            if ctx.execution_id() == 1 {
                ctx.continue_as_new("next").await
            } else {
                Ok("successor incorrectly ran".into())
            }
        })
        .build();
    let client = Client::new(provider.clone());
    client
        .start_orchestration("CAN-first-turn-poison", "Transition", "")
        .await
        .expect("start");
    let runtime = Runtime::start_with_options(
        provider.clone(),
        ActivityRegistry::builder().build(),
        registry,
        RuntimeOptions {
            max_attempts,
            orchestration_concurrency: 1,
            dispatcher_min_poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let result = client
        .wait_for_orchestration("CAN-first-turn-poison", Duration::from_secs(10))
        .await;
    assert!(
        matches!(
            result.expect("poison must terminate"),
            OrchestrationStatus::Failed {
                details: duroxide::ErrorDetails::Poison { .. },
                ..
            }
        ),
        "CAN successor first-turn attempts must not be exempt from poison"
    );
    assert_late_inputs_are_drained(&client, provider.as_ref(), &runtime, "CAN-first-turn-poison").await;
}

async fn assert_late_inputs_are_drained(
    client: &Client,
    provider: &PoisonInjectingProvider,
    runtime: &Arc<Runtime>,
    instance: &str,
) {
    let before = provider.read(instance).await.expect("poisoned history");
    assert!(
        before
            .iter()
            .any(|event| matches!(event.kind, EventKind::OrchestrationContinuedAsNew { .. }))
    );
    assert!(
        before
            .iter()
            .any(|event| matches!(event.kind, EventKind::OrchestrationFailed { .. }))
    );
    client.enqueue_event(instance, "q", "late").await.expect("late queue");
    client
        .raise_event(instance, "signal", "late")
        .await
        .expect("late signal");
    client
        .cancel_instance(instance, "late cancel")
        .await
        .expect("late cancel");
    tokio::time::sleep(Duration::from_secs(2)).await;
    runtime.clone().shutdown(None).await;
    // Let an already-held batch return before inspecting the unlocked queue.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let batch = provider
        .fetch_orchestration_item(Duration::from_secs(1), Duration::ZERO, None)
        .await
        .expect("inspect terminal queue");
    assert!(
        batch.is_none(),
        "poisoned CAN successor must drain late queue, signal and cancel inputs"
    );
    assert_eq!(provider.read(instance).await.expect("unchanged history"), before);
}

#[tokio::test]
async fn unregistered_can_successor_poison_must_drain_late_inputs() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.expect("store"));
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));
    let instance = "CAN-unregistered-poison";
    seed_waiting_can(provider.as_ref(), instance, "MissingHandler", late_input(instance)).await;
    enqueue_successor(provider.as_ref(), instance, "MissingHandler", None).await;
    let runtime = Runtime::start_with_options(
        provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder().build(),
        RuntimeOptions {
            max_attempts: 1,
            orchestration_concurrency: 1,
            unregistered_backoff: UnregisteredBackoffConfig {
                base_delay: Duration::from_millis(20),
                max_delay: Duration::from_millis(20),
            },
            dispatcher_min_poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let client = Client::new(provider.clone());
    let result = client
        .wait_for_orchestration(instance, Duration::from_secs(10))
        .await
        .expect("poison");
    assert!(matches!(
        result,
        OrchestrationStatus::Failed {
            details: duroxide::ErrorDetails::Poison { .. },
            ..
        }
    ));
    assert_late_inputs_are_drained(&client, provider.as_ref(), &runtime, instance).await;
}

#[tokio::test]
async fn delayed_can_start_defers_inputs_with_counted_backoff() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.expect("store"));
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));
    let instance = "CAN-start-delayed";
    seed_waiting_can(provider.as_ref(), instance, "Transition", late_input(instance)).await;
    // The successor start exists but stays hidden, as behind an old worker's backoff.
    enqueue_successor(
        provider.as_ref(),
        instance,
        "Transition",
        Some(Duration::from_millis(900)),
    )
    .await;
    let runtime = Runtime::start_with_options(
        provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder()
            .register("Transition", |ctx: OrchestrationContext, _: String| async move {
                assert_eq!(ctx.execution_id(), 2);
                Ok(ctx.dequeue_event("q").await)
            })
            .build(),
        RuntimeOptions {
            orchestration_concurrency: 1,
            unregistered_backoff: UnregisteredBackoffConfig {
                base_delay: Duration::from_millis(100),
                max_delay: Duration::from_millis(400),
            },
            dispatcher_min_poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let result = Client::new(provider.clone())
        .wait_for_orchestration(instance, Duration::from_secs(10))
        .await;
    runtime.shutdown(None).await;
    assert!(
        matches!(result.expect("delayed start completion"),
        OrchestrationStatus::Completed { ref output, .. } if output == "late"),
        "the waiting input must reach the successor once its start is visible"
    );
    let delays = provider.counted_abandon_delays();
    assert!(
        delays.len() >= 2,
        "the input must wait through several counted deferrals: {delays:?}"
    );
    assert_eq!(
        &delays[..2],
        &[Duration::from_millis(100), Duration::from_millis(200)],
        "deferrals use the unregistered-handler backoff for the growing attempt count"
    );
    assert_eq!(
        provider.successful_ignored_abandons(),
        0,
        "deferral must not reset attempts"
    );
    let old = provider.read_with_execution(instance, 1).await.expect("old history");
    assert_eq!(old.len(), 2, "deferral must not mutate execution 1");
}

/// A batch of only stale completions for the previous execution also waits for the
/// CAN start (counted, like any other batch), and the successor then drops it.
#[tokio::test]
async fn stale_only_batch_waits_for_the_can_start() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.expect("store"));
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));
    let instance = "CAN-stale-only";
    let stale = WorkItem::ActivityCompleted {
        instance: instance.into(),
        execution_id: 1,
        id: 99,
        result: "stale".into(),
    };
    seed_waiting_can(provider.as_ref(), instance, "Transition", stale).await;
    enqueue_successor(
        provider.as_ref(),
        instance,
        "Transition",
        Some(Duration::from_millis(400)),
    )
    .await;
    let runtime = Runtime::start_with_options(
        provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder()
            .register("Transition", |ctx: OrchestrationContext, _: String| async move {
                assert_eq!(ctx.execution_id(), 2);
                Ok("successor".into())
            })
            .build(),
        RuntimeOptions {
            orchestration_concurrency: 1,
            unregistered_backoff: UnregisteredBackoffConfig {
                base_delay: Duration::from_millis(100),
                max_delay: Duration::from_millis(100),
            },
            dispatcher_min_poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let result = Client::new(provider.clone())
        .wait_for_orchestration(instance, Duration::from_secs(10))
        .await;
    runtime.shutdown(None).await;
    assert!(matches!(result.expect("successor completion"),
        OrchestrationStatus::Completed { ref output, .. } if output == "successor"));
    assert!(
        !provider.counted_abandon_delays().is_empty(),
        "the stale-only batch must be deferred with a counted attempt, not acknowledged"
    );
    let old = provider.read_with_execution(instance, 1).await.expect("old history");
    assert_eq!(old.len(), 2, "deferral must not mutate execution 1");
    let successor = provider
        .read_with_execution(instance, 2)
        .await
        .expect("successor history");
    assert!(
        !successor
            .iter()
            .any(|event| matches!(event.kind, EventKind::ActivityCompleted { .. })),
        "the successor must drop the stale completion: {successor:?}"
    );
}

/// A duplicate sub-orchestration start for a running instance fails the incoming
/// parent, unless it is the instance's own parent re-sending the same call with a
/// known parent execution id.
#[tokio::test]
async fn live_collision_ignores_only_the_same_known_parent_call() {
    for (recorded_generation, incoming, notified) in [
        (Some(1), (7, Some(1)), false), // the same call: ignored, as on the base
        (Some(1), (8, Some(1)), true),  // another call of the same parent
        (Some(1), (7, Some(2)), true),  // the same call id from a later parent execution
        (None, (7, None), true),        // unknown generations cannot prove the same call
    ] {
        let sqlite = Arc::new(SqliteProvider::new_in_memory().await.expect("store"));
        let provider = Arc::new(PoisonInjectingProvider::new(sqlite));
        let instance = "live-collision";
        let start = |parent_id: u64, parent_execution_id: Option<u64>| WorkItem::StartOrchestration {
            instance: instance.into(),
            orchestration: "Child".into(),
            input: "".into(),
            version: Some("1.0.0".into()),
            execution_id: 1,
            parent_instance: Some("parent".into()),
            parent_id: Some(parent_id),
            parent_execution_id,
        };
        common::seed_history_turn(
            provider.as_ref(),
            start(7, recorded_generation),
            1,
            vec![Event::with_event_id(
                1,
                instance,
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "Child".into(),
                    version: "1.0.0".into(),
                    input: "".into(),
                    parent_instance: Some("parent".into()),
                    parent_id: Some(7),
                    parent_execution_id: recorded_generation,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![start(incoming.0, incoming.1), late_input(instance)],
            ExecutionMetadata {
                orchestration_name: Some("Child".into()),
                orchestration_version: Some("1.0.0".into()),
                ..Default::default()
            },
        )
        .await;
        let runtime = Runtime::start_with_options(
            provider.clone(),
            ActivityRegistry::builder().build(),
            OrchestrationRegistry::builder()
                .register("Child", |ctx: OrchestrationContext, _: String| async move {
                    Ok(ctx.dequeue_event("q").await)
                })
                .build(),
            RuntimeOptions {
                orchestration_concurrency: 1,
                dispatcher_min_poll_interval: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .await;
        // The child keeps running its original start and reads the queued input.
        let result = Client::new(provider.clone())
            .wait_for_orchestration(instance, Duration::from_secs(10))
            .await;
        runtime.shutdown(None).await;
        assert!(
            matches!(result, Ok(OrchestrationStatus::Completed { ref output, .. }) if output == "late"),
            "{incoming:?}: the duplicate must not disturb the running child: {result:?}"
        );
        let notices = provider.collision_notices();
        assert_eq!(
            notices.iter().any(|(parent_id, _)| *parent_id == incoming.0),
            notified,
            "recorded generation {recorded_generation:?}, incoming {incoming:?}: notices {notices:?}"
        );
    }
}

#[tokio::test]
async fn missing_can_start_poisons_waiting_inputs_after_max_attempts() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.expect("store"));
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));
    let instance = "CAN-start-missing";
    // Corruption: execution 1 ended in continue-as-new, but its successor start is gone.
    seed_waiting_can(provider.as_ref(), instance, "Transition", late_input(instance)).await;
    let max_attempts = 3;
    let runtime = Runtime::start_with_options(
        provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder()
            .register("Transition", |_: OrchestrationContext, _: String| async move {
                Ok("successor incorrectly ran".into())
            })
            .build(),
        RuntimeOptions {
            max_attempts,
            orchestration_concurrency: 1,
            unregistered_backoff: UnregisteredBackoffConfig {
                base_delay: Duration::from_millis(20),
                max_delay: Duration::from_millis(20),
            },
            dispatcher_min_poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let client = Client::new(provider.clone());
    let result = client
        .wait_for_orchestration(instance, Duration::from_secs(10))
        .await
        .expect("a missing start must end the wait");
    assert!(
        matches!(
            result,
            OrchestrationStatus::Failed {
                details: duroxide::ErrorDetails::Poison { .. },
                ..
            }
        ),
        "a start that never appears must poison the waiting inputs: {result:?}"
    );
    assert_eq!(
        provider.counted_abandon_delays().len(),
        max_attempts as usize,
        "each waiting fetch up to max_attempts defers with a counted attempt"
    );
    let history = provider
        .read_with_execution(instance, 1)
        .await
        .expect("poisoned history");
    assert!(matches!(
        history.last().map(|e| &e.kind),
        Some(EventKind::OrchestrationFailed { .. })
    ));
    assert!(
        provider
            .read_with_execution(instance, 2)
            .await
            .unwrap_or_default()
            .is_empty()
    );
    // Failed after CAN is terminal: later inputs are drained, not deferred forever.
    assert_late_inputs_are_drained(&client, provider.as_ref(), &runtime, instance).await;
}

#[tokio::test]
async fn failure_commit_must_preserve_collision_notification() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.expect("store"));
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));
    let instance = "collision-failure-commit";
    let start_item = |parent| WorkItem::StartOrchestration {
        instance: instance.into(),
        orchestration: "Transition".into(),
        input: "".into(),
        version: Some("1.0.0".into()),
        execution_id: 1,
        parent_instance: parent,
        parent_id: Some(42),
        parent_execution_id: Some(1),
    };
    common::seed_history_turn(
        provider.as_ref(),
        start_item(None),
        1,
        vec![Event::with_event_id(
            1,
            instance,
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "Transition".into(),
                version: "1.0.0".into(),
                input: "".into(),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        )],
        vec![start_item(Some("incoming-parent".into()))],
        ExecutionMetadata {
            orchestration_name: Some("Transition".into()),
            orchestration_version: Some("1.0.0".into()),
            ..Default::default()
        },
    )
    .await;
    provider.fail_next_orchestration_ack();
    let runtime = Runtime::start_with_options(
        provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder()
            .register("Transition", |_: OrchestrationContext, _: String| async {
                Ok("done".into())
            })
            .build(),
        RuntimeOptions {
            orchestration_concurrency: 1,
            ..Default::default()
        },
    )
    .await;
    let result = Client::new(provider.clone())
        .wait_for_orchestration(instance, Duration::from_secs(10))
        .await;
    runtime.shutdown(None).await;
    assert!(matches!(
        result.expect("failure commit"),
        OrchestrationStatus::Failed { .. }
    ));
    assert_eq!(
        provider.failure_commit_collision_notifications(),
        1,
        "the fallback transaction must atomically notify the colliding parent"
    );
}

#[tokio::test]
async fn failed_can_abandon_leaves_waiting_inputs_for_lease_redelivery() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.expect("test store"));
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));
    let instance = "CAN-abandon-failure";
    seed_waiting_can(provider.as_ref(), instance, "Transition", late_input(instance)).await;
    // Seeding fetched the instance once; count only the runtime's deliveries.
    let seeded = provider.orchestration_fetches(instance).len();
    provider.fail_orchestration_abandon_persistently();
    let registry = OrchestrationRegistry::builder()
        .register("Transition", |ctx: OrchestrationContext, _: String| async move {
            assert_eq!(ctx.execution_id(), 2);
            Ok(ctx.dequeue_event("q").await)
        })
        .build();
    let runtime = Runtime::start_with_options(
        provider.clone(),
        ActivityRegistry::builder().build(),
        registry,
        RuntimeOptions {
            orchestration_concurrency: 2,
            orchestrator_lock_timeout: Duration::from_millis(300),
            orchestrator_lock_renewal_buffer: Duration::from_millis(100),
            unregistered_backoff: UnregisteredBackoffConfig {
                base_delay: Duration::from_millis(50),
                max_delay: Duration::from_millis(200),
            },
            dispatcher_min_poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    // An abandon that fails is left to the lease: it expires and the same input is
    // fetched again under a new lock, attempt kept.
    let deliveries = tokio::time::timeout(Duration::from_secs(10), async {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            let fetches = provider.orchestration_fetches(instance);
            if fetches.len() >= seeded + 2 {
                break fetches[seeded..].to_vec();
            }
            tick.tick().await;
        }
    })
    .await
    .expect("the waiting input must be redelivered after its lease expires");
    assert_ne!(deliveries[0].0, deliveries[1].0, "redelivery must use a new lock");
    assert!(
        deliveries[1].1 > deliveries[0].1,
        "the failed delivery keeps its attempt: {deliveries:?}"
    );
    assert!(
        provider.failed_abandon_calls() >= 2,
        "both deliveries must have failed to abandon"
    );
    let history = provider.read(instance).await.expect("history during failure");
    assert!(
        !history
            .iter()
            .any(|event| matches!(event.kind, EventKind::OrchestrationFailed { .. })),
        "failed abandons must not poison a waiting input"
    );
    provider.clear_injections();
    enqueue_successor(provider.as_ref(), instance, "Transition", None).await;
    let result = Client::new(provider.clone())
        .wait_for_orchestration(instance, Duration::from_secs(10))
        .await;
    runtime.shutdown(None).await;
    assert!(
        matches!(result.expect("waiting input must remain recoverable"),
        OrchestrationStatus::Completed { ref output, .. } if output == "late"),
        "waiting input must reach the successor after abandon recovery"
    );
    let old = provider.read_with_execution(instance, 1).await.expect("old history");
    assert_eq!(old.len(), 2, "deferral must not mutate execution 1");
}

fn late_input(instance: &str) -> WorkItem {
    WorkItem::QueueMessage {
        instance: instance.into(),
        name: "q".into(),
        data: "late".into(),
    }
}

async fn seed_waiting_can(provider: &PoisonInjectingProvider, instance: &str, name: &str, waiting: WorkItem) {
    common::seed_history_turn(
        provider,
        WorkItem::StartOrchestration {
            instance: instance.into(),
            orchestration: name.into(),
            input: "old".into(),
            version: Some("1.0.0".into()),
            execution_id: 1,
            parent_instance: None,
            parent_id: None,
            parent_execution_id: None,
        },
        1,
        vec![
            Event::with_event_id(
                1,
                instance,
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: name.into(),
                    version: "1.0.0".into(),
                    input: "old".into(),
                    parent_instance: None,
                    parent_id: None,
                    parent_execution_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            ),
            Event::with_event_id(
                2,
                instance,
                1,
                None,
                EventKind::OrchestrationContinuedAsNew { input: "next".into() },
            ),
        ],
        vec![waiting],
        ExecutionMetadata {
            status: Some("ContinuedAsNew".into()),
            ..Default::default()
        },
    )
    .await;
}

async fn enqueue_successor(provider: &PoisonInjectingProvider, instance: &str, name: &str, delay: Option<Duration>) {
    provider
        .enqueue_for_orchestrator(
            WorkItem::ContinueAsNew {
                instance: instance.into(),
                orchestration: name.into(),
                input: "next".into(),
                version: Some("1.0.0".into()),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                carry_forward_events: vec![],
                initial_custom_status: None,
            },
            delay,
        )
        .await
        .expect("delayed existing successor");
}

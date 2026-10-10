// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Runtime-level queue cancellation and event-version preservation checks.

use super::{Event, EventKind, ExecutionMetadata, ProviderFactory, start_item};
use crate::providers::WorkItem;
use crate::runtime::registry::ActivityRegistry;
use crate::runtime::{Runtime, RuntimeOptions};
use crate::{Client, Either3, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::time::Duration;

/// Duplicate client starts cannot switch a running execution to the latest
/// handler, another handler name, or a replacement input.
pub async fn test_duplicate_start_preserves_pinned_handler<F: ProviderFactory>(factory: &F) {
    for duplicate_name in ["Original", "WrongHandler"] {
        let provider = factory.create_provider().await;
        let instance = format!("provider-pinned-handler-{duplicate_name}");
        provider
            .enqueue_for_orchestrator(start_item(&instance), None)
            .await
            .expect("seed start");
        let (_, lock, _) = provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .expect("seed fetch")
            .expect("visible seed");
        provider
            .ack_orchestration_item(
                &lock,
                1,
                vec![
                    Event::with_event_id(
                        1,
                        &instance,
                        1,
                        None,
                        EventKind::OrchestrationStarted {
                            name: "Original".into(),
                            version: "1.0.0".into(),
                            input: "original-input".into(),
                            parent_instance: None,
                            parent_id: None,
                            parent_execution_id: None,
                            carry_forward_events: None,
                            initial_custom_status: None,
                        },
                    ),
                    Event::with_event_id(
                        2,
                        &instance,
                        1,
                        None,
                        EventKind::ActivityScheduled {
                            name: "tick".into(),
                            input: "v1".into(),
                            session_id: None,
                            tag: None,
                        },
                    ),
                ],
                vec![],
                vec![
                    WorkItem::StartOrchestration {
                        instance: instance.clone(),
                        orchestration: duplicate_name.into(),
                        input: "wrong-input".into(),
                        version: None,
                        execution_id: 1,
                        parent_instance: None,
                        parent_id: None,
                        parent_execution_id: None,
                    },
                    WorkItem::ActivityCompleted {
                        instance: instance.clone(),
                        execution_id: 1,
                        id: 2,
                        result: "ok".into(),
                    },
                ],
                ExecutionMetadata {
                    status: Some("Running".into()),
                    orchestration_name: Some("Original".into()),
                    orchestration_version: Some("1.0.0".into()),
                    ..Default::default()
                },
                vec![],
            )
            .await
            .expect("seed running pinned history and duplicate batch");
        let registry = OrchestrationRegistry::builder()
            .register_versioned(
                "Original",
                "1.0.0",
                |ctx: OrchestrationContext, input: String| async move {
                    ctx.schedule_activity("tick", "v1").await?;
                    Ok(format!("v1:{input}"))
                },
            )
            .register_versioned("Original", "2.0.0", |ctx: OrchestrationContext, _: String| async move {
                ctx.schedule_activity("tick", "v2").await?;
                Ok("v2".into())
            })
            .register("WrongHandler", |ctx: OrchestrationContext, _: String| async move {
                ctx.schedule_activity("tick", "wrong").await?;
                Ok("wrong".into())
            })
            .build();
        let runtime = Runtime::start_with_options(
            provider.clone(),
            ActivityRegistry::builder().build(),
            registry,
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .await;
        let result = Client::new(provider)
            .wait_for_orchestration(&instance, Duration::from_secs(10))
            .await;
        runtime.shutdown(None).await;
        assert!(matches!(result.expect("duplicate client start must be inert"),
            OrchestrationStatus::Completed { ref output, .. } if output == "v1:original-input"));
    }
}

/// During rolling upgrades an unregistered successor can hide its CAN start
/// through backoff. New inputs must survive until a compatible runtime resumes it.
pub async fn test_continue_as_new_unregistered_backoff<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let instance = "provider-can-unregistered-backoff";
    provider
        .enqueue_for_orchestrator(start_item(instance), None)
        .await
        .expect("seed start");
    let (_, lock, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .expect("seed fetch")
        .expect("visible seed");
    provider
        .ack_orchestration_item(
            &lock,
            1,
            vec![
                Event::with_event_id(
                    1,
                    instance,
                    1,
                    None,
                    EventKind::OrchestrationStarted {
                        name: "RollingSuccessor".into(),
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
            vec![],
            vec![WorkItem::ContinueAsNew {
                instance: instance.into(),
                orchestration: "RollingSuccessor".into(),
                input: "next".into(),
                version: Some("1.0.0".into()),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                carry_forward_events: vec![("q".into(), "carried".into())],
                initial_custom_status: None,
            }],
            ExecutionMetadata {
                status: Some("ContinuedAsNew".into()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .expect("seed terminal and immediately visible successor atomically");
    let old_runtime = Runtime::start_with_options(
        provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder().build(),
        RuntimeOptions {
            orchestration_concurrency: 1,
            dispatcher_min_poll_interval: Duration::from_millis(10),
            unregistered_backoff: crate::runtime::UnregisteredBackoffConfig {
                base_delay: Duration::from_secs(2),
                max_delay: Duration::from_secs(2),
            },
            ..Default::default()
        },
    )
    .await;
    let hidden = tokio::time::timeout(Duration::from_secs(10), async {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            tick.tick().await;
            match provider
                .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
                .await
            {
                Ok(None) => break,
                Ok(Some((item, lock, _))) => {
                    assert!(
                        item.messages
                            .iter()
                            .any(|item| matches!(item, WorkItem::ContinueAsNew { .. }))
                    );
                    provider
                        .abandon_orchestration_item(&lock, None, true)
                        .await
                        .expect("release competing inspection fetch");
                }
                Err(error) if error.retryable => {
                    tracing::warn!(%error, "Retrying backoff-test visibility inspection");
                }
                Err(error) => panic!("backoff-test fetch failed: {error}"),
            }
        }
    })
    .await;
    old_runtime.shutdown(None).await;
    hidden.expect("unregistered runtime must fetch and hide the start");
    assert!(
        provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .expect("inspect after runtime stopped")
            .is_none(),
        "start must be delayed by unregistered-handler backoff, not merely locked"
    );
    assert_eq!(
        provider
            .read_with_execution(instance, 1)
            .await
            .expect("predecessor history")
            .len(),
        2
    );
    let client = Client::new(provider.clone());
    client
        .enqueue_event(instance, "q", "late")
        .await
        .expect("input during successor backoff");
    let registry = OrchestrationRegistry::builder()
        .register("RollingSuccessor", |ctx: OrchestrationContext, _: String| async move {
            assert_eq!(ctx.execution_id(), 2);
            let first = ctx.dequeue_event("q").await;
            let second = ctx.dequeue_event("q").await;
            Ok(format!("{first},{second}"))
        })
        .build();
    let new_runtime = Runtime::start_with_options(
        provider.clone(),
        ActivityRegistry::builder().build(),
        registry,
        RuntimeOptions {
            dispatcher_min_poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let result = client.wait_for_orchestration(instance, Duration::from_secs(10)).await;
    new_runtime.shutdown(None).await;
    assert!(
        matches!(result.expect("successor and window input must survive rolling-node backoff"),
        OrchestrationStatus::Completed { ref output, .. } if output == "carried,late")
    );
}

/// A duplicate start must not replace the durable CAN successor, even when it
/// precedes that successor in queue order. A colliding parent must be notified.
pub async fn test_continue_as_new_duplicate_start<F: ProviderFactory>(factory: &F) {
    for (stamp, collision) in [("0.1.30", false), ("0.1.30", true), ("0.1.31", false), ("0.1.31", true)] {
        let provider = factory.create_provider().await;
        let parent = "provider-can-duplicate-parent";
        let instance = format!("{parent}::can-child");
        let persist = |instance: String, history: Vec<Event>, messages: Vec<WorkItem>, status: &str| {
            let provider = provider.clone();
            let status = status.to_string();
            async move {
                provider
                    .enqueue_for_orchestrator(start_item(&instance), None)
                    .await
                    .expect("seed start");
                let (_, lock, _) = provider
                    .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
                    .await
                    .expect("seed fetch")
                    .expect("visible seed");
                provider
                    .ack_orchestration_item(
                        &lock,
                        1,
                        history,
                        vec![],
                        messages,
                        ExecutionMetadata {
                            status: Some(status),
                            ..Default::default()
                        },
                        vec![],
                    )
                    .await
                    .expect("seed acknowledgement");
            }
        };
        let started = |instance: &str, name: &str| {
            let mut event = Event::with_event_id(
                1,
                instance,
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: name.into(),
                    version: "1.0.0".into(),
                    input: "".into(),
                    parent_instance: None,
                    parent_id: None,
                    parent_execution_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            );
            event.duroxide_version = stamp.into();
            event
        };
        if collision {
            persist(
                parent.into(),
                vec![
                    started(parent, "CollisionParent"),
                    Event::with_event_id(
                        2,
                        parent,
                        1,
                        None,
                        EventKind::SubOrchestrationScheduled {
                            name: "Transition".into(),
                            instance: "can-child".into(),
                            input: "duplicate".into(),
                        },
                    ),
                ],
                vec![],
                "Running",
            )
            .await;
        }
        let messages = vec![
            WorkItem::StartOrchestration {
                instance: instance.clone(),
                orchestration: "WrongHandler".into(),
                input: "duplicate".into(),
                version: None,
                execution_id: 1,
                parent_instance: collision.then(|| parent.into()),
                parent_id: collision.then_some(2),
                parent_execution_id: collision.then_some(1),
            },
            WorkItem::ContinueAsNew {
                instance: instance.clone(),
                orchestration: "Transition".into(),
                input: "next".into(),
                version: Some("1.0.0".into()),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                carry_forward_events: vec![("q".into(), "carried".into())],
                initial_custom_status: None,
            },
            WorkItem::QueueMessage {
                instance: instance.clone(),
                name: "q".into(),
                data: "late".into(),
            },
        ];
        persist(
            instance.clone(),
            vec![
                started(&instance, "Transition"),
                Event::with_event_id(
                    2,
                    &instance,
                    1,
                    None,
                    EventKind::OrchestrationContinuedAsNew { input: "next".into() },
                ),
            ],
            messages,
            "ContinuedAsNew",
        )
        .await;
        let registry = OrchestrationRegistry::builder()
            .register("Transition", |ctx: OrchestrationContext, _: String| async move {
                assert_eq!(ctx.execution_id(), 2);
                let first = ctx.dequeue_event("q").await;
                let second = ctx.dequeue_event("q").await;
                Ok(format!("{first},{second}"))
            })
            .register("CollisionParent", |ctx: OrchestrationContext, _: String| async move {
                match ctx
                    .schedule_sub_orchestration_with_id("Transition", "can-child", "duplicate")
                    .await
                {
                    Ok(value) => Err(format!("collision unexpectedly resolved: {value}")),
                    Err(error) => Ok(error),
                }
            })
            .build();
        let runtime = Runtime::start_with_options(
            provider.clone(),
            ActivityRegistry::builder().build(),
            registry,
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .await;
        let client = Client::new(provider.clone());
        let result = client.wait_for_orchestration(&instance, Duration::from_secs(10)).await;
        let parent_result = if collision {
            Some(client.wait_for_orchestration(parent, Duration::from_secs(10)).await)
        } else {
            None
        };
        runtime.shutdown(None).await;
        assert!(
            matches!(result.expect("CAN successor must not livelock behind duplicate start"),
            OrchestrationStatus::Completed { ref output, .. } if output == "carried,late")
        );
        if let Some(parent_result) = parent_result {
            assert!(
                matches!(parent_result.expect("colliding parent must receive a notification"),
                OrchestrationStatus::Completed { ref output, .. }
                if output.contains("already exists") && !output.contains("is terminal")),
                "collision notification must describe an existing logical instance, not a dead successor"
            );
        }
        let old = provider
            .read_with_execution(&instance, 1)
            .await
            .expect("predecessor history");
        assert_eq!(old.len(), 2, "duplicate start must not mutate old recorded history");
        let next = provider
            .read_with_execution(&instance, 2)
            .await
            .expect("successor history");
        assert!(
            matches!(&next[0].kind, EventKind::OrchestrationStarted { name, input, .. }
            if name == "Transition" && input == "next")
        );
    }
}

/// Persistent inputs must survive a CAN-terminal fetch without the successor's start item.
/// The delayed start deterministically exposes the provider's visibility-cutoff window.
pub async fn test_continue_as_new_transition_delivery<F: ProviderFactory>(factory: &F, stamp: &str) {
    for shape in [
        "queue",
        "race",
        "mixed-stale",
        "signal",
        "cancel",
        "future-completion",
        "stale-only",
    ] {
        let race = shape == "race";
        let provider = factory.create_provider().await;
        let instance = format!("provider-can-window-{stamp}-{shape}");
        let mut start = Event::with_event_id(
            1,
            &instance,
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "Transition".into(),
                version: "1.0.0".into(),
                input: "old".into(),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        );
        start.duroxide_version = stamp.into();
        provider
            .enqueue_for_orchestrator(start_item(&instance), None)
            .await
            .expect("must enqueue the seed execution");
        let (_, lock, _) = provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .expect("must fetch the seed execution")
            .expect("seed execution must be visible");
        let mut inputs = vec![WorkItem::QueueMessage {
            instance: instance.clone(),
            name: "q".into(),
            data: "late".into(),
        }];
        if shape == "mixed-stale" {
            inputs.extend([
                WorkItem::ActivityCompleted {
                    instance: instance.clone(),
                    execution_id: 1,
                    id: 99,
                    result: "stale".into(),
                },
                WorkItem::TimerFired {
                    instance: instance.clone(),
                    execution_id: 1,
                    id: 100,
                    fire_at_ms: 1,
                },
                WorkItem::SubOrchCompleted {
                    parent_instance: instance.clone(),
                    parent_execution_id: 1,
                    parent_id: 101,
                    result: "stale".into(),
                },
            ]);
        }
        if shape == "future-completion" || shape == "stale-only" {
            inputs = vec![WorkItem::ActivityCompleted {
                instance: instance.clone(),
                execution_id: if shape == "future-completion" { 2 } else { 1 },
                id: 99,
                result: "unmatched".into(),
            }];
        }
        if shape == "signal" {
            inputs.push(WorkItem::ExternalRaised {
                instance: instance.clone(),
                name: "signal".into(),
                data: "answer".into(),
            });
        }
        if shape == "cancel" {
            inputs.push(WorkItem::CancelInstance {
                instance: instance.clone(),
                reason: "requested-during-CAN".into(),
            });
        }
        provider
            .ack_orchestration_item(
                &lock,
                1,
                vec![
                    start,
                    Event::with_event_id(
                        2,
                        &instance,
                        1,
                        None,
                        EventKind::OrchestrationContinuedAsNew { input: "next".into() },
                    ),
                ],
                vec![],
                inputs,
                ExecutionMetadata {
                    status: Some("ContinuedAsNew".into()),
                    output: Some("next".into()),
                    orchestration_name: Some("Transition".into()),
                    orchestration_version: Some("1.0.0".into()),
                    pinned_duroxide_version: Some(stamp.parse().expect("test stamp must parse")),
                    ..Default::default()
                },
                vec![],
            )
            .await
            .expect("must persist the CAN-terminal seed and arrival atomically");
        let successor_start = WorkItem::ContinueAsNew {
            instance: instance.clone(),
            orchestration: "Transition".into(),
            input: "next".into(),
            version: Some("1.0.0".into()),
            parent_instance: None,
            parent_id: None,
            parent_execution_id: None,
            carry_forward_events: vec![("q".into(), "carried".into())],
            initial_custom_status: None,
        };
        provider
            .enqueue_for_orchestrator(successor_start, Some(Duration::from_secs(2)))
            .await
            .expect("must enqueue the delayed successor start");
        let registry = OrchestrationRegistry::builder()
            .register("Transition", move |ctx: OrchestrationContext, _: String| async move {
                if ctx.execution_id() == 1 {
                    return ctx.continue_as_new("next").await;
                }
                if shape == "stale-only" {
                    return Ok(ctx.dequeue_event("q").await);
                }
                if shape == "signal" {
                    let answer = ctx.schedule_wait("signal").await;
                    let first = ctx.dequeue_event("q").await;
                    let second = ctx.dequeue_event("q").await;
                    return Ok(format!("{first},{second}|{answer}"));
                }
                let mut values = Vec::new();
                while values.len() < 2 {
                    if race {
                        if let crate::Either2::First(value) = ctx
                            .select2(ctx.dequeue_event("q"), ctx.schedule_timer(Duration::from_millis(10)))
                            .await
                        {
                            values.push(value);
                        }
                    } else {
                        values.push(ctx.dequeue_event("q").await);
                    }
                }
                Ok(values.join(","))
            })
            .build();
        let runtime = Runtime::start_with_options(
            provider.clone(),
            ActivityRegistry::builder().build(),
            registry,
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(10),
                // Waiting inputs back off and count attempts like an unregistered handler;
                // the 2 s hidden start must arrive well inside the default poison budget.
                unregistered_backoff: crate::runtime::UnregisteredBackoffConfig {
                    base_delay: Duration::from_millis(250),
                    max_delay: Duration::from_secs(1),
                },
                ..Default::default()
            },
        )
        .await;
        if shape == "signal" {
            tokio::time::timeout(Duration::from_secs(10), async {
                let mut tick = tokio::time::interval(Duration::from_millis(20));
                loop {
                    tick.tick().await;
                    let history = provider
                        .read_with_execution(&instance, 2)
                        .await
                        .expect("observe successor signal binding");
                    if history
                        .iter()
                        .any(|event| matches!(&event.kind, EventKind::ExternalSubscribed { name } if name == "signal"))
                        && history.iter().any(|event| {
                            matches!(&event.kind, EventKind::ExternalEvent { name, data }
                                if name == "signal" && data == "answer")
                        })
                    {
                        break;
                    }
                }
            })
            .await
            .expect("successor must audit the preserved window signal and bind its wait");
            Client::new(provider.clone())
                .raise_event(&instance, "signal", "fresh")
                .await
                .expect("fresh answer after subscription");
        }
        let result = Client::new(provider.clone())
            .wait_for_orchestration(&instance, Duration::from_secs(10))
            .await;
        runtime.shutdown(None).await;
        let successor = provider
            .read_with_execution(&instance, 2)
            .await
            .expect("must read the successor history");
        let old = provider
            .read_with_execution(&instance, 1)
            .await
            .expect("predecessor history");
        assert_eq!(old.len(), 2, "{stamp}/{shape}: admission cannot mutate execution 1");
        assert!(matches!(old[0].kind, EventKind::OrchestrationStarted { .. }));
        assert!(matches!(old[1].kind, EventKind::OrchestrationContinuedAsNew { .. }));
        assert!(
            !successor.is_empty(),
            "{stamp}/{shape}: successor never started; this does not prove arrival loss"
        );
        assert_eq!(
            successor[0]
                .duroxide_version
                .parse::<semver::Version>()
                .expect("successor stamp must parse"),
            crate::providers::current_build_version(),
            "the predecessor stamp does not select the successor's policy"
        );
        let result = result.unwrap_or_else(|error| {
            panic!("{stamp}/{shape}: terminal CAN must preserve the queued input: {error:?}; history={successor:?}")
        });
        if shape == "future-completion" {
            assert!(
                matches!(
                    &result,
                    OrchestrationStatus::Failed {
                        details: crate::ErrorDetails::Configuration {
                            kind: crate::ConfigErrorKind::Nondeterminism,
                            ..
                        },
                        ..
                    }
                ),
                "{stamp}/{shape}: a future-scoped malformed completion must be validated, not silently ACKed: {result:?}"
            );
            continue;
        }
        if shape == "stale-only" {
            assert!(
                matches!(&result, OrchestrationStatus::Completed { output, .. } if output == "carried"),
                "{stamp}/{shape}: stale-only batch must not obstruct the successor: {result:?}"
            );
            continue;
        }
        if shape == "cancel" {
            assert!(
                matches!(&result, OrchestrationStatus::Failed { details: crate::ErrorDetails::Application {
                    kind: crate::AppErrorKind::Cancelled { reason }, ..
                }, .. } if reason == "requested-during-CAN"),
                "{stamp}/{shape}: logical instance cancellation must reach the successor: {result:?}"
            );
            continue;
        }
        let expected_output = if shape == "signal" {
            let binding = successor
                .iter()
                .find(|event| matches!(&event.kind, EventKind::ExternalSubscribed { name } if name == "signal"))
                .expect("successor must bind its signal wait")
                .event_id;
            let answer = successor
                .iter()
                .find(|event| {
                    matches!(&event.kind, EventKind::ExternalEvent { name, data } if name == "signal" && data == "answer")
                })
                .expect("preserved window signal must reach the successor audit history")
                .event_id;
            // Abandon visibility may split the CAN start and window inputs into
            // separate fetches. Bound admission is defined at event application,
            // not by the time the test enqueued the signal.
            if answer < binding {
                "carried,late|fresh"
            } else {
                "carried,late|answer"
            }
        } else {
            "carried,late"
        };
        assert!(
            matches!(result, OrchestrationStatus::Completed { ref output, .. } if output == expected_output),
            "{stamp}/{shape}: carried input must precede the preserved window arrival: {result:?}"
        );
        if shape == "signal" {
            let OrchestrationStatus::Completed { output, .. } = &result else {
                unreachable!("completion was asserted above");
            };
            let consumed = output
                .strip_prefix("carried,late|")
                .expect("signal output must preserve queued values");
            if expected_output == "carried,late|fresh" {
                assert_eq!(consumed, "fresh", "the audited answer before binding must be skipped");
                assert_ne!(
                    consumed, "answer",
                    "an early answer must not resolve the successor's wait"
                );
            } else {
                assert_eq!(
                    consumed, "answer",
                    "the answer applied after binding must resolve that wait"
                );
                assert_ne!(
                    consumed, "fresh",
                    "fresh must not be consumed after the bound answer resolved"
                );
            }
        }
        let delivered: Vec<_> = successor
            .iter()
            .filter_map(|event| match &event.kind {
                EventKind::QueueEventDelivered { name, data } if name == "q" => Some(data.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            delivered,
            ["carried", "late"],
            "{stamp}/{shape}: no loss or duplication"
        );
        if shape == "signal" {
            assert_eq!(
                successor
                    .iter()
                    .filter(|event| matches!(&event.kind, EventKind::ExternalEvent { name, data }
                    if name == "signal" && data == "answer"))
                    .count(),
                1,
                "positional input must reach successor admission exactly once"
            );
        }
        // Only mixed-stale injects stale completions (ids 99-101 of execution 1). In the race
        // shape the successor's own timers can legitimately reach those ids while it waits.
        if shape == "mixed-stale" {
            assert!(
                !successor.iter().any(|event| matches!(
                    event.kind,
                    EventKind::ActivityCompleted { .. }
                        | EventKind::TimerFired { .. }
                        | EventKind::SubOrchestrationCompleted { .. }
                ) && event.source_event_id.is_some_and(|id| id >= 99)),
                "{stamp}/{shape}: stale execution completions must not be applied to the successor"
            );
        }
    }
}

/// Queue Q1/Q2/Q6 FIFO and replay checks on every provider.
pub async fn test_queue_race_cancellation_replay<F: ProviderFactory>(factory: &F) {
    for shape in ["q1", "q2", "q6"] {
        let provider = factory.create_provider().await;
        let instance = format!("provider-queue-race-{shape}");
        let event = |id, source, kind| Event::with_event_id(id, &instance, 1, source, kind);
        let mut start = event(
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "QueueRace".into(),
                version: "1.0.0".into(),
                input: shape.into(),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        );
        start.duroxide_version = "0.1.31".into();
        let mut history = vec![start];
        match shape {
            "q1" => {
                history.extend([
                    event(2, None, EventKind::QueueSubscribed { name: "q".into() }),
                    event(3, None, EventKind::QueueSubscribed { name: "q".into() }),
                    event(4, None, EventKind::TimerCreated { fire_at_ms: 1000 }),
                    event(5, Some(4), EventKind::TimerFired { fire_at_ms: 1000 }),
                    event(
                        6,
                        None,
                        EventKind::QueueEventDelivered {
                            name: "q".into(),
                            data: "first".into(),
                        },
                    ),
                    event(
                        7,
                        None,
                        EventKind::QueueEventDelivered {
                            name: "q".into(),
                            data: "second".into(),
                        },
                    ),
                    event(8, None, EventKind::QueueSubscribed { name: "q".into() }),
                    event(9, None, EventKind::QueueSubscribed { name: "q".into() }),
                    event(
                        10,
                        Some(2),
                        EventKind::QueueSubscriptionCancelled {
                            reason: "dropped_future".into(),
                        },
                    ),
                    event(
                        11,
                        Some(3),
                        EventKind::QueueSubscriptionCancelled {
                            reason: "dropped_future".into(),
                        },
                    ),
                ]);
            }
            "q2" => {
                history.extend([
                    event(2, None, EventKind::QueueSubscribed { name: "q".into() }),
                    event(3, None, EventKind::QueueSubscribed { name: "q".into() }),
                    event(4, None, EventKind::TimerCreated { fire_at_ms: 1000 }),
                    event(
                        5,
                        None,
                        EventKind::QueueEventDelivered {
                            name: "q".into(),
                            data: "first".into(),
                        },
                    ),
                    event(
                        6,
                        None,
                        EventKind::QueueEventDelivered {
                            name: "q".into(),
                            data: "second".into(),
                        },
                    ),
                    event(7, None, EventKind::QueueSubscribed { name: "q".into() }),
                    event(
                        8,
                        Some(3),
                        EventKind::QueueSubscriptionCancelled {
                            reason: "dropped_future".into(),
                        },
                    ),
                ]);
            }
            "q6" => {
                history.extend([
                    event(2, None, EventKind::QueueSubscribed { name: "q".into() }),
                    event(3, None, EventKind::TimerCreated { fire_at_ms: 1000 }),
                    event(4, None, EventKind::TimerCreated { fire_at_ms: 1100 }),
                    event(5, Some(4), EventKind::TimerFired { fire_at_ms: 1100 }),
                    event(
                        6,
                        None,
                        EventKind::QueueEventDelivered {
                            name: "q".into(),
                            data: "first".into(),
                        },
                    ),
                    event(7, None, EventKind::QueueSubscribed { name: "q".into() }),
                    event(
                        8,
                        Some(2),
                        EventKind::QueueSubscriptionCancelled {
                            reason: "dropped_future".into(),
                        },
                    ),
                ]);
            }
            _ => unreachable!(),
        }
        let checkpoint_id = history.last().unwrap().event_id + 1;
        let expected = if shape == "q6" { "first" } else { "first,second" };
        history.push(event(
            checkpoint_id,
            None,
            EventKind::ActivityScheduled {
                name: "checkpoint".into(),
                input: expected.into(),
                session_id: None,
                tag: None,
            },
        ));
        provider
            .enqueue_for_orchestrator(start_item(&instance), None)
            .await
            .unwrap();
        let (_, lock, _) = provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .unwrap();
        provider
            .ack_orchestration_item(
                &lock,
                1,
                history.clone(),
                vec![],
                vec![WorkItem::ActivityCompleted {
                    instance: instance.clone(),
                    execution_id: 1,
                    id: checkpoint_id,
                    result: "ok".into(),
                }],
                ExecutionMetadata {
                    orchestration_name: Some("QueueRace".into()),
                    orchestration_version: Some("1.0.0".into()),
                    ..Default::default()
                },
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(
            provider.read_with_execution(&instance, 1).await.unwrap(),
            history,
            "provider must preserve full event metadata and pinned semantic version"
        );
        let registry = OrchestrationRegistry::builder()
            .register("QueueRace", |ctx: OrchestrationContext, input: String| async move {
                let mut values = Vec::new();
                if input == "q6" {
                    let inner = ctx.select2(ctx.dequeue_event("q"), ctx.schedule_timer(Duration::from_millis(10)));
                    let _ = ctx.select2(inner, ctx.schedule_timer(Duration::from_millis(100))).await;
                    values.push(ctx.dequeue_event("q").await);
                } else {
                    match ctx
                        .select3(
                            ctx.dequeue_event("q"),
                            ctx.dequeue_event("q"),
                            ctx.schedule_timer(Duration::from_millis(10)),
                        )
                        .await
                    {
                        Either3::First(value) | Either3::Second(value) => values.push(value),
                        Either3::Third(()) => {}
                    }
                    while values.len() < 2 {
                        values.push(ctx.dequeue_event("q").await);
                    }
                }
                let output = values.join(",");
                ctx.schedule_activity("checkpoint", &output).await?;
                Ok(output)
            })
            .build();
        let runtime = Runtime::start_with_options(
            provider.clone(),
            ActivityRegistry::builder().build(),
            registry,
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .await;
        let result = Client::new(provider)
            .wait_for_orchestration(&instance, Duration::from_secs(30))
            .await;
        runtime.shutdown(None).await;
        let result = result.unwrap_or_else(|error| panic!("{shape}: queue replay did not complete: {error:?}"));
        assert!(
            matches!(result,OrchestrationStatus::Completed{ref output,..}if output==expected),
            "{shape}: {result:?}"
        );
    }
}

/// Continue-as-new must carry unread arrivals, including held and non-prefix cases.
pub async fn test_continue_as_new_queue_race_replay<F: ProviderFactory>(factory: &F) {
    for shape in ["can", "can-drain", "can-held", "can-held-nonprefix"] {
        let provider = factory.create_provider().await;
        let instance = format!("provider-combined-race-{shape}");
        let event = |id, source, kind| Event::with_event_id(id, &instance, 1, source, kind);
        let mut start = event(
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "CombinedRace".into(),
                version: "1.0.0".into(),
                input: shape.into(),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        );
        start.duroxide_version = "0.1.31".into();
        let mut history = vec![start];
        let incoming = {
            history.extend([
                event(2, None, EventKind::QueueSubscribed { name: "q".into() }),
                event(3, None, EventKind::TimerCreated { fire_at_ms: 1000 }),
            ]);
            let mut messages = vec![
                WorkItem::TimerFired {
                    instance: instance.clone(),
                    execution_id: 1,
                    id: 3,
                    fire_at_ms: 1000,
                },
                WorkItem::QueueMessage {
                    instance: instance.clone(),
                    name: "q".into(),
                    data: "m1".into(),
                },
            ];
            if shape == "can-drain" || shape == "can-held-nonprefix" {
                messages.push(WorkItem::QueueMessage {
                    instance: instance.clone(),
                    name: "q".into(),
                    data: "m2".into(),
                });
            }
            messages
        };
        provider
            .enqueue_for_orchestrator(start_item(&instance), None)
            .await
            .unwrap();
        let (_, lock, _) = provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .unwrap();
        provider
            .ack_orchestration_item(
                &lock,
                1,
                history,
                vec![],
                incoming,
                ExecutionMetadata {
                    orchestration_name: Some("CombinedRace".into()),
                    orchestration_version: Some("1.0.0".into()),
                    ..Default::default()
                },
                vec![],
            )
            .await
            .unwrap();
        let registry = OrchestrationRegistry::builder()
            .register("CombinedRace", |ctx: OrchestrationContext, input: String| async move {
                if ctx.execution_id() == 1 {
                    if input.starts_with("can-held") {
                        let mut held = ctx.dequeue_event("q");
                        let _ = ctx
                            .select2(&mut held, ctx.schedule_timer(Duration::from_millis(10)))
                            .await;
                        if input == "can-held-nonprefix" {
                            ctx.dequeue_event("q").await;
                        }
                        return ctx.continue_as_new(input).await;
                    }
                    let _ = ctx
                        .select2(ctx.dequeue_event("q"), ctx.schedule_timer(Duration::from_millis(10)))
                        .await;
                    if input == "can-drain" {
                        ctx.dequeue_event("q").await;
                    }
                    ctx.continue_as_new(input).await
                } else {
                    Ok(ctx.dequeue_event("q").await)
                }
            })
            .build();
        let runtime = Runtime::start_with_options(
            provider.clone(),
            ActivityRegistry::builder().build(),
            registry,
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .await;
        let result = Client::new(provider)
            .wait_for_orchestration(&instance, Duration::from_secs(30))
            .await;
        runtime.shutdown(None).await;
        let result = result.unwrap_or_else(|error| panic!("{shape}: unread CAN arrivals did not complete: {error:?}"));
        let expected = match shape {
            "can-drain" => "m2",
            _ => "m1",
        };
        assert!(
            matches!(result,OrchestrationStatus::Completed{ref output,..}if output==expected),
            "{shape}: {result:?}"
        );
    }
}

/// An old semantic version must round-trip without being restamped by a newer reader.
pub async fn test_queue_replay_version_stamp_roundtrip<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let instance = "queue-version-roundtrip";
    let mut event = Event::with_event_id(
        1,
        instance,
        1,
        None,
        EventKind::OrchestrationStarted {
            name: "TestOrch".into(),
            version: "1.0.0".into(),
            input: "{}".into(),
            parent_instance: None,
            parent_id: None,
            parent_execution_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    );
    event.duroxide_version = "0.1.30".into();
    provider
        .enqueue_for_orchestrator(start_item(instance), None)
        .await
        .unwrap();
    let (_, lock, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &lock,
            1,
            vec![event.clone()],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("TestOrch".into()),
                orchestration_version: Some("1.0.0".into()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(provider.read_with_execution(instance, 1).await.unwrap(), vec![event]);
    provider
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: instance.into(),
                name: "fetch-stamp".into(),
                data: "wake".into(),
            },
            None,
        )
        .await
        .expect("enqueue fetch-path stamp probe");
    let (fetched, lock, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .expect("fetch old-stamped execution")
        .expect("old-stamped execution was not fetchable");
    assert_eq!(
        fetched.history[0].duroxide_version, "0.1.30",
        "fetch path restamped history"
    );
    provider
        .abandon_orchestration_item(&lock, None, true)
        .await
        .expect("release stamp probe lock");
}

/// Sequential positional round trips with a long timer loser, then identical replay.
/// The binding barrier deliberately serializes signals; this is not competitive
/// signal-race coverage. Engine tests cover same-batch signal/timer races.
pub async fn test_positional_wait_race_replay<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let registry = OrchestrationRegistry::builder()
        .register("LiveSignalRace", |ctx: OrchestrationContext, _: String| async move {
            let first = ctx
                .select2(
                    ctx.schedule_wait("s"),
                    ctx.schedule_timer(Duration::from_secs(30))
                        .map(|()| "timeout".to_string()),
                )
                .await
                .into_tuple()
                .1;
            Ok(format!("{first:?}:{}", ctx.schedule_wait("s").await))
        })
        .build();
    let handler = registry
        .resolve_handler("LiveSignalRace")
        .expect("registered live signal handler")
        .1;
    let runtime = Runtime::start_with_options(
        provider.clone(),
        ActivityRegistry::builder().build(),
        registry,
        RuntimeOptions::default(),
    )
    .await;
    let client = Client::new(provider.clone());
    client
        .start_orchestration("provider-live-signal", "LiveSignalRace", "")
        .await
        .expect("start live signal race");
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            tick.tick().await;
            let history = provider
                .read("provider-live-signal")
                .await
                .expect("read live signal history");
            if history
                .iter()
                .any(|e| matches!(e.kind, EventKind::ExternalSubscribed { .. }))
            {
                break;
            }
        }
    })
    .await
    .expect("signal subscription never appeared");
    client
        .raise_event("provider-live-signal", "s", "one")
        .await
        .expect("raise first signal");
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            tick.tick().await;
            let history = provider
                .read("provider-live-signal")
                .await
                .expect("read replacement signal binding");
            if history
                .iter()
                .filter(|event| matches!(&event.kind, EventKind::ExternalSubscribed { name } if name == "s"))
                .count()
                >= 2
            {
                break;
            }
        }
    })
    .await
    .expect("replacement signal subscription never appeared");
    client
        .raise_event("provider-live-signal", "s", "two")
        .await
        .expect("raise second signal");
    let result = client
        .wait_for_orchestration("provider-live-signal", Duration::from_secs(10))
        .await;
    runtime.shutdown(None).await;
    let output = match result.unwrap_or_else(|error| {
        panic!("live signal race did not finish after both subscriptions were bound: {error:?}")
    }) {
        OrchestrationStatus::Completed { output, .. } => output,
        other => panic!("live signal race failed: {other:?}"),
    };
    let history = provider
        .read("provider-live-signal")
        .await
        .expect("read recorded live signal turns");
    // Terminal histories short-circuit to Continue. Replay the recorded body
    // without its terminal marker, as the runtime would before completion.
    let history: Vec<_> = history
        .into_iter()
        .filter(|event| !matches!(event.kind, EventKind::OrchestrationCompleted { .. }))
        .collect();
    for _ in 0..2 {
        let mut engine =
            crate::runtime::replay_engine::ReplayEngine::new("provider-live-signal".into(), 1, history.clone());
        let result = engine.execute_orchestration(
            handler.clone(),
            "".into(),
            "LiveSignalRace".into(),
            "1.0.0".into(),
            "validation",
        );
        assert!(
            matches!(result, crate::runtime::replay_engine::TurnResult::Completed(ref value) if *value == output),
            "provider live/replay differs: {result:?}, live={output}, history={history:?}"
        );
        assert!(engine.history_delta().is_empty(), "replay appended history");
    }
}

/// A fetched 0.1.30 execution must take a legacy decision that differs from the new policy.
pub async fn test_legacy_queue_race_decision_preserved<F: ProviderFactory>(factory: &F) {
    use crate::runtime::replay_engine::{ReplayEngine, TurnResult};
    for stamp in ["0.1.30", "0.1.31"] {
        let provider = factory.create_provider().await;
        let instance = format!("provider-legacy-decision-{stamp}");
        let registry = OrchestrationRegistry::builder()
            .register("LegacyCompetition", |ctx: OrchestrationContext, _: String| async move {
                let first = ctx.dequeue_event("q");
                let second = ctx.dequeue_event("q");
                let t0 = ctx.schedule_timer(Duration::from_secs(30));
                let t1 = ctx.schedule_timer(Duration::from_secs(60));
                let _ = ctx.select2(first, t0).await;
                let value = match ctx.select2(second, t1).await {
                    crate::Either2::First(value) => value,
                    crate::Either2::Second(()) => "second-timeout".into(),
                };
                ctx.schedule_activity("checkpoint", &value).await?;
                Ok(value)
            })
            .build();
        let handler = registry
            .resolve_handler("LegacyCompetition")
            .expect("registered competition handler")
            .1;
        let mut start = Event::with_event_id(
            1,
            &instance,
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "LegacyCompetition".into(),
                version: "1.0.0".into(),
                input: "".into(),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        );
        start.duroxide_version = stamp.into();
        let mut engine = ReplayEngine::new(instance.clone(), 1, vec![start.clone()]);
        assert!(matches!(
            engine.execute_orchestration(
                handler.clone(),
                "".into(),
                "LegacyCompetition".into(),
                "1.0.0".into(),
                "validation"
            ),
            TurnResult::Continue
        ));
        let mut history = vec![start];
        history.extend_from_slice(engine.history_delta());
        // Timer completions must already be visible in the provider's queue.
        // Matching checks the schedule identity, not its wall-clock deadline.
        for event in &mut history {
            if let EventKind::TimerCreated { fire_at_ms } = &mut event.kind {
                *fire_at_ms = 1;
            }
        }
        let timers: Vec<_> = history
            .iter()
            .filter_map(|e| {
                if let EventKind::TimerCreated { fire_at_ms } = e.kind {
                    Some(WorkItem::TimerFired {
                        instance: instance.clone(),
                        execution_id: 1,
                        id: e.event_id(),
                        fire_at_ms,
                    })
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(timers.len(), 2);
        provider
            .enqueue_for_orchestrator(start_item(&instance), None)
            .await
            .expect("seed competition");
        let (_, lock, _) = provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .expect("fetch competition seed")
            .expect("missing competition seed");
        provider
            .ack_orchestration_item(
                &lock,
                1,
                history,
                vec![],
                vec![
                    timers[0].clone(),
                    WorkItem::QueueMessage {
                        instance: instance.clone(),
                        name: "q".into(),
                        data: "first".into(),
                    },
                    timers[1].clone(),
                ],
                ExecutionMetadata {
                    orchestration_name: Some("LegacyCompetition".into()),
                    orchestration_version: Some("1.0.0".into()),
                    ..Default::default()
                },
                vec![],
            )
            .await
            .expect("persist competition prefix");
        let (fetched, lock, _) = provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .expect("fetch competition prefix")
            .expect("missing competition turn");
        assert_eq!(
            fetched.history[0].duroxide_version, stamp,
            "fetch path changed the execution policy"
        );
        let mut turn = ReplayEngine::new(instance.clone(), 1, fetched.history.clone());
        turn.prep_completions(fetched.messages);
        let live = turn.execute_orchestration(
            handler,
            "".into(),
            "LegacyCompetition".into(),
            "1.0.0".into(),
            "validation",
        );
        assert!(
            matches!(live, TurnResult::Continue),
            "competition evaluation failed: {live:?}"
        );
        let checkpoint = turn
            .history_delta()
            .iter()
            .find_map(|event| match &event.kind {
                EventKind::ActivityScheduled { name, .. } if name == "checkpoint" => Some(event.event_id()),
                _ => None,
            })
            .expect("competition did not schedule checkpoint");
        provider
            .ack_orchestration_item(
                &lock,
                1,
                turn.history_delta().to_vec(),
                vec![],
                vec![WorkItem::ActivityCompleted {
                    instance: instance.clone(),
                    execution_id: 1,
                    id: checkpoint,
                    result: "ok".into(),
                }],
                ExecutionMetadata::default(),
                vec![],
            )
            .await
            .expect("persist fetched competition decisions");
        let runtime = Runtime::start_with_options(
            provider.clone(),
            ActivityRegistry::builder()
                .register("checkpoint", |_: crate::ActivityContext, input: String| async move {
                    Ok(input)
                })
                .build(),
            registry,
            RuntimeOptions::default(),
        )
        .await;
        let result = Client::new(provider.clone())
            .wait_for_orchestration(&instance, Duration::from_secs(10))
            .await;
        let history = provider
            .read(&instance)
            .await
            .expect("read competition diagnostic history");
        runtime.shutdown(None).await;
        let result = result
            .unwrap_or_else(|error| panic!("{stamp}: competition did not finish: {error:?}, history={history:?}"));
        let expected = if stamp == "0.1.30" { "second-timeout" } else { "first" };
        assert!(
            matches!(result,OrchestrationStatus::Completed{ref output,..}if output==expected),
            "{stamp}: legacy-discriminating competition returned {result:?}"
        );
    }
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::EventKind;
use duroxide::providers::WorkItem;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, Event, OrchestrationContext, OrchestrationRegistry};
use std::time::Duration;
mod common;

#[tokio::test]
async fn external_duplicate_workitems_dedup() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let v = ctx.schedule_wait("Evt").await;
        Ok(v)
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("WaitEvt", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    let inst = "inst-ext-dup";
    client.start_orchestration(inst, "WaitEvt", "").await.unwrap();
    assert!(common::wait_for_subscription(store.clone(), inst, "Evt", 2_000).await);

    // enqueue duplicate externals
    let wi = WorkItem::ExternalRaised {
        instance: inst.to_string(),
        name: "Evt".to_string(),
        data: "ok".to_string(),
    };
    let _ = store.enqueue_for_orchestrator(wi.clone(), None).await;
    let _ = store.enqueue_for_orchestrator(wi.clone(), None).await;

    // wait for completion
    let ok = common::wait_for_history(
        store.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "ok"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");

    // Both external events are materialized in history (unconditional materialization).
    // Only the first is delivered to the subscription (causal check); the second
    // has no pending subscription slot and is a no-op at replay time.
    let hist = store.read(inst).await.unwrap_or_default();
    let external_events: Vec<&Event> = hist
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ExternalEvent { name, .. } if name == "Evt"))
        .collect();
    assert_eq!(
        external_events.len(),
        2,
        "expected 2 ExternalEvents (both materialized for audit), got {}",
        external_events.len()
    );

    rt.shutdown(None).await;
}

#[tokio::test]
async fn timer_duplicate_workitems_dedup() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(Duration::from_millis(100)).await;
        Ok("t".to_string())
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("OneTimer", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    let inst = "inst-timer-dup";
    client.start_orchestration(inst, "OneTimer", "").await.unwrap();

    // wait for TimerCreated and get id
    assert!(
        common::wait_for_history(
            store.clone(),
            inst,
            |h| h.iter().any(|e| matches!(&e.kind, EventKind::TimerCreated { .. })),
            2_000
        )
        .await
    );
    let (id, fire_at_ms) = {
        let hist = store.read(inst).await.unwrap_or_default();
        let mut t_id = 0u64;
        let mut t_fire = 0u64;
        for e in hist.iter() {
            if let EventKind::TimerCreated { fire_at_ms } = &e.kind {
                t_id = e.event_id;
                t_fire = *fire_at_ms;
                break;
            }
        }
        (t_id, t_fire)
    };

    // enqueue duplicate TimerFired for the same id
    let wi = WorkItem::TimerFired {
        instance: inst.to_string(),
        execution_id: 1,
        id,
        fire_at_ms,
    };
    let _ = store.enqueue_for_orchestrator(wi.clone(), None).await;
    let _ = store.enqueue_for_orchestrator(wi.clone(), None).await;

    // wait for completion
    let ok = common::wait_for_history(
        store.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "t"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");

    // exactly one TimerFired in history
    let hist = store.read(inst).await.unwrap_or_default();
    let fired: Vec<&Event> = hist
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::TimerFired { .. }))
        .collect();
    assert_eq!(fired.len(), 1, "expected 1 TimerFired, got {}", fired.len());

    rt.shutdown(None).await;
}

#[tokio::test]
async fn activity_duplicate_completion_workitems_dedup() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Activity sleeps to give us time to inject duplicates
    let activity_registry = ActivityRegistry::builder()
        .register("SlowEcho", |_ctx: ActivityContext, input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Ok(input)
        })
        .build();
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let out = ctx.schedule_activity("SlowEcho", "x".to_string()).await.unwrap();
        Ok(out)
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("OneSlowAct", orch).build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    let inst = "inst-act-dup";
    client.start_orchestration(inst, "OneSlowAct", "").await.unwrap();

    // wait for ActivityScheduled to get id
    assert!(
        common::wait_for_history(
            store.clone(),
            inst,
            |h| h
                .iter()
                .any(|e| matches!(&e.kind, EventKind::ActivityScheduled { name, .. } if name == "SlowEcho")),
            2_000
        )
        .await
    );
    let id = {
        let hist = store.read(inst).await.unwrap_or_default();
        let mut t_id = 0u64;
        for e in hist.iter() {
            if let EventKind::ActivityScheduled { name, .. } = &e.kind
                && name == "SlowEcho"
            {
                t_id = e.event_id;
                break;
            }
        }
        t_id
    };

    // enqueue duplicate ActivityCompleted with result matching the worker to avoid mismatches
    let wi = WorkItem::ActivityCompleted {
        instance: inst.to_string(),
        execution_id: 1,
        id,
        result: "x".to_string(),
    };
    let _ = store.enqueue_for_orchestrator(wi.clone(), None).await;
    let _ = store.enqueue_for_orchestrator(wi.clone(), None).await;

    // wait for completion and assert single ActivityCompleted recorded
    let ok = common::wait_for_history(
        store.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "x"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");

    let hist = store.read(inst).await.unwrap_or_default();
    let acts: Vec<&Event> = hist
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ActivityCompleted { .. }) && e.source_event_id == Some(id))
        .collect();
    assert_eq!(
        acts.len(),
        1,
        "expected 1 ActivityCompleted for id={}, got {}",
        id,
        acts.len()
    );

    rt.shutdown(None).await;
}
// merged file: imports above already declared; avoid reimporting

// Simulate crash windows by interleaving dequeue and persistence.
// We approximate by injecting duplicates around the same window; idempotence + peek-lock should ensure correctness.

#[tokio::test]
async fn crash_after_dequeue_before_append_completion() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        // Wait for external then complete with payload
        let v = ctx.schedule_wait("Evt").await;
        Ok(v)
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("WaitEvt", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    // Start orchestration and wait for subscription
    let inst = "inst-crash-before-append";
    client.start_orchestration(inst, "WaitEvt", "").await.unwrap();
    assert!(common::wait_for_subscription(store.clone(), inst, "Evt", 2_000).await);

    // Enqueue the external work item
    let wi = WorkItem::ExternalRaised {
        instance: inst.to_string(),
        name: "Evt".to_string(),
        data: "ok".to_string(),
    };
    let _ = store.enqueue_for_orchestrator(wi.clone(), None).await;
    // Simulate crash-before-append by enqueuing duplicate before runtime gets to append
    let _ = store.enqueue_for_orchestrator(wi.clone(), None).await;

    // Wait for completion, ensure a single ExternalEvent recorded
    let ok = common::wait_for_history(
        store.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "ok"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");
    // Both external events are materialized in history (unconditional materialization).
    // Only the first is delivered (causal check); the duplicate is a no-op at replay time.
    let hist = store.read(inst).await.unwrap_or_default();
    let evs: Vec<&Event> = hist
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ExternalEvent { .. }))
        .collect();
    assert_eq!(evs.len(), 2);

    rt.shutdown(None).await;
}

#[tokio::test]
async fn crash_after_append_before_ack_timer() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(Duration::from_millis(50)).await;
        Ok("t".to_string())
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("OneTimer", orch).build();
    let activity_registry = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    let inst = "inst-crash-after-append";
    client.start_orchestration(inst, "OneTimer", "").await.unwrap();

    assert!(
        common::wait_for_history(
            store.clone(),
            inst,
            |h| h.iter().any(|e| matches!(&e.kind, EventKind::TimerCreated { .. })),
            2_000
        )
        .await
    );
    // Get timer id
    let (id, fire_at_ms) = {
        let hist = store.read(inst).await.unwrap_or_default();
        let mut t_id = 0u64;
        let mut t_fire = 0u64;
        for e in hist.iter() {
            if let EventKind::TimerCreated { fire_at_ms } = &e.kind {
                t_id = e.event_id;
                t_fire = *fire_at_ms;
                break;
            }
        }
        (t_id, t_fire)
    };

    // Inject duplicate TimerFired simulating a crash after append-before-ack
    let wi = WorkItem::TimerFired {
        instance: inst.to_string(),
        execution_id: 1,
        id,
        fire_at_ms,
    };
    let _ = store.enqueue_for_orchestrator(wi.clone(), None).await;
    let _ = store.enqueue_for_orchestrator(wi.clone(), None).await;

    let ok = common::wait_for_history(
        store.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "t"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");
    let hist = store.read(inst).await.unwrap_or_default();
    let fired: Vec<&Event> = hist
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::TimerFired { .. }))
        .collect();
    assert_eq!(fired.len(), 1);

    rt.shutdown(None).await;
}

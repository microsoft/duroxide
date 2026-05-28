// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Replay engine extensibility verification via topic-based external events.
//!
//! This test proves that the replay engine can be extended with new EventKind variants
//! (ExternalSubscribed2/ExternalEvent2) and that the full pipeline works:
//!   schedule_wait2() → Action::WaitExternal2 → EventKind::ExternalSubscribed2
//!   → provider storage → replay → deliver_external_event2
//!
//! It also verifies that continue-as-new creates a clean event boundary:
//! v2's execution history contains zero v1 events.
//!
//! Both v1 and v2 handlers live in the same binary via register_versioned —
//! this is NOT testing binary-level isolation (that's the serde boundary test).

use duroxide::providers::{ExecutionMetadata, WorkItem};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{Client, EventKind, OrchestrationContext, OrchestrationRegistry};
use std::time::Duration;

#[path = "../common/mod.rs"]
mod common;

/// E2E: v1 orchestration upgrades to v2 via continue_as_new_versioned.
///
/// V1 (version "1.0.0"):
///   - Receives 3 external events via schedule_wait (name-only matching)
///   - After processing all 3, calls continue_as_new_versioned("2.0.0", accumulated_state)
///
/// V2 (version "2.0.0"):
///   - Receives 2 external events via schedule_wait2 (name+topic matching)
///   - Returns final output
///
/// Assertions:
///   1. Orchestration completes successfully with correct output
///   2. V1 execution history contains ExternalSubscribed / ExternalEvent events
///   3. V2 execution history contains ExternalSubscribed2 / ExternalEvent2 events
///   4. Both execution histories are independently valid (no cross-contamination)
#[tokio::test]
async fn e2e_eternal_orch_v1_to_v2_event_upgrade() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // V1 handler: name-only external events, then upgrade to v2
    let v1_handler = |ctx: OrchestrationContext, _input: String| async move {
        let mut collected = Vec::new();
        for _ in 0..3 {
            let data = ctx.schedule_wait("approval").await;
            collected.push(data);
        }
        let state = collected.join(",");
        ctx.continue_as_new_versioned("2.0.0", state).await
    };

    // V2 handler: topic-based external events, returns final output
    let v2_handler = |ctx: OrchestrationContext, input: String| async move {
        let mut collected: Vec<String> = input.split(',').map(|s| s.to_string()).collect();
        for _ in 0..2 {
            let data = ctx.schedule_wait2("approval", "orders.us-east").await;
            collected.push(data);
        }
        Ok(format!("final:{}", collected.join("+")))
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("Upgrader", v1_handler)
        .register_versioned("Upgrader", "2.0.0", v2_handler)
        .build();

    let options = RuntimeOptions {
        max_attempts: 10,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        options,
    )
    .await;

    let client = Client::new(store.clone());
    client
        .start_orchestration_versioned("upgrader-1", "Upgrader", "1.0.0", "init")
        .await
        .unwrap();

    // Wait for orchestration to be started by runtime
    let started = common::wait_for_history(
        store.clone(),
        "upgrader-1",
        |hist| {
            hist.iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationStarted { .. }))
        },
        10000,
    )
    .await;
    assert!(started, "orchestration did not start");

    // --- V1 phase: send 3 external events ---
    for i in 0..3 {
        // Wait for the (i+1)th ExternalSubscribed to appear
        let expected_count = i + 1;
        let found = common::wait_for_history(
            store.clone(),
            "upgrader-1",
            move |hist| {
                let count = hist
                    .iter()
                    .filter(|e| matches!(&e.kind, EventKind::ExternalSubscribed { name } if name == "approval"))
                    .count();
                count >= expected_count
            },
            10000,
        )
        .await;
        assert!(found, "v1 subscription {i} did not appear in history");

        client
            .raise_event("upgrader-1", "approval", format!("v1-data-{i}"))
            .await
            .unwrap();

        if i < 2 {
            // For events 0 and 1: wait for ExternalEvent to be processed
            let expected_event_count = i + 1;
            let processed = common::wait_for_history(
                store.clone(),
                "upgrader-1",
                move |hist| {
                    let count = hist
                        .iter()
                        .filter(|e| matches!(&e.kind, EventKind::ExternalEvent { .. }))
                        .count();
                    count >= expected_event_count
                },
                10000,
            )
            .await;
            assert!(processed, "v1 event {i} was not processed");
        }
        // For the last event (i=2): don't wait here — the orchestration will
        // immediately continue_as_new after processing it, which changes the
        // latest execution and makes store.read() return the new execution's history.
    }

    // --- Wait for v2 execution to start ---
    // After CAN, store.read() returns execution 2's history which has OrchestrationStarted v2.0.0
    let v2_started = common::wait_for_history(
        store.clone(),
        "upgrader-1",
        |hist| {
            hist.iter().any(|e| {
                matches!(
                    &e.kind,
                    EventKind::OrchestrationStarted { version, .. } if version == "2.0.0"
                )
            })
        },
        10000,
    )
    .await;
    assert!(v2_started, "v2 execution did not start after continue-as-new");

    // --- V2 phase: send 2 external events with topics ---
    // After CAN, the new execution starts. Wait for V2's subscriptions and send events.
    for i in 0..2 {
        let expected_count = i + 1;
        let found_v2 = common::wait_for_history(
            store.clone(),
            "upgrader-1",
            move |hist| {
                let count = hist
                    .iter()
                    .filter(|e| {
                        matches!(
                            &e.kind,
                            EventKind::ExternalSubscribed2 { name, topic }
                            if name == "approval" && topic == "orders.us-east"
                        )
                    })
                    .count();
                count >= expected_count
            },
            10000,
        )
        .await;
        assert!(found_v2, "v2 subscription {i} did not appear in history");

        client
            .raise_event2("upgrader-1", "approval", "orders.us-east", format!("v2-data-{i}"))
            .await
            .unwrap();

        // Wait for each v2 event to be processed
        let expected_event_count = i + 1;
        let processed = common::wait_for_history(
            store.clone(),
            "upgrader-1",
            move |hist| {
                let count = hist
                    .iter()
                    .filter(|e| matches!(&e.kind, EventKind::ExternalEvent2 { .. }))
                    .count();
                count >= expected_event_count
            },
            10000,
        )
        .await;
        assert!(processed, "v2 event {i} was not processed");
    }

    // --- Wait for final completion ---
    let status = client
        .wait_for_orchestration("upgrader-1", Duration::from_secs(5))
        .await
        .unwrap();

    match &status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(
                output.as_str(),
                "final:v1-data-0+v1-data-1+v1-data-2+v2-data-0+v2-data-1",
                "final output should contain all v1 and v2 data"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message());
        }
        other => panic!("unexpected status: {other:?}"),
    }

    // --- Verify execution histories ---
    // Get all executions for the instance
    let mgmt = store.as_management_capability().expect("management capability");
    let executions = mgmt.list_executions("upgrader-1").await.unwrap();
    assert!(
        executions.len() >= 2,
        "expected at least 2 executions (v1 + v2), got {}",
        executions.len()
    );

    // V1 execution (execution_id = 1): should have ExternalSubscribed + ExternalEvent, NOT ExternalSubscribed2/ExternalEvent2
    let v1_history = mgmt.read_history_with_execution_id("upgrader-1", 1).await.unwrap();
    let v1_has_subscribed = v1_history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed { .. }));
    let v1_has_event = v1_history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalEvent { .. }));
    let v1_has_subscribed2 = v1_history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed2 { .. }));
    let v1_has_event2 = v1_history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalEvent2 { .. }));

    assert!(v1_has_subscribed, "v1 should have ExternalSubscribed events");
    assert!(v1_has_event, "v1 should have ExternalEvent events");
    assert!(!v1_has_subscribed2, "v1 should NOT have ExternalSubscribed2 events");
    assert!(!v1_has_event2, "v1 should NOT have ExternalEvent2 events");

    // V2 execution (execution_id = 2): should have ExternalSubscribed2 + ExternalEvent2, NOT ExternalSubscribed/ExternalEvent
    let v2_history = mgmt.read_history_with_execution_id("upgrader-1", 2).await.unwrap();
    let v2_has_subscribed2 = v2_history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed2 { .. }));
    let v2_has_event2 = v2_history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalEvent2 { .. }));
    let v2_has_subscribed = v2_history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed { .. }));
    let v2_has_event = v2_history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalEvent { .. }));

    assert!(v2_has_subscribed2, "v2 should have ExternalSubscribed2 events");
    assert!(v2_has_event2, "v2 should have ExternalEvent2 events");
    assert!(
        !v2_has_subscribed,
        "v2 should NOT have ExternalSubscribed events (clean boundary)"
    );
    assert!(
        !v2_has_event,
        "v2 should NOT have ExternalEvent events (clean boundary)"
    );

    rt.shutdown(None).await;
}

/// E2E: Simulates a duroxide upgrade where v1 history was created by an older
/// duroxide version (0.1.10) and is already in the DB before the current runtime starts.
///
/// Phase 1 — "old duroxide 0.1.10" (seeded via provider APIs, no runtime):
///   1. Directly write history events stamped with duroxide_version "0.1.10"
///   2. Seed: OrchestrationStarted(v1.0.0), 3x ExternalSubscribed, 2x ExternalEvent
///   3. Leave orchestration suspended (waiting for 3rd external event)
///
/// Phase 2 — "new duroxide 0.1.16" (current runtime):
///   4. Start runtime with v1 + v2 handlers
///   5. Send 3rd v1 event → v1 replays old history, completes, calls continue_as_new("2.0.0")
///   6. Send v2 events → v2 completes
///
/// This proves:
///   - Current runtime can replay history written by an older duroxide version
///   - Capability filtering accepts the older-versioned execution (0.1.10 ∈ [0.0.0, 0.1.16])
///   - The duroxide_version field on the OrchestrationStarted event is preserved, not overwritten
///   - CAN boundary separates old-version events from new-version events
#[tokio::test]
async fn e2e_upgrade_with_preexisting_v1_history() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let instance = "upgrade-hist";
    let old_duroxide_version = "0.1.10";

    // ========== Phase 1: Seed history as if written by duroxide 0.1.10 ==========
    //
    // Simulate what an older runtime would have produced after:
    //   Turn 1: OrchestrationStarted + ExternalSubscribed (orchestration suspends on schedule_wait)
    //   Turn 2: ExternalEvent(data=old-0) + ExternalSubscribed (orchestration loops, suspends again)
    //   Turn 3: ExternalEvent(data=old-1) + ExternalSubscribed (orchestration loops, suspends again)
    //
    // History layout (7 events):
    //   event_id 1: OrchestrationStarted { name: "Upgrader", version: "1.0.0" }
    //   event_id 2: ExternalSubscribed { name: "approval" }
    //   event_id 3: ExternalEvent { name: "approval", data: "old-0" }
    //   event_id 4: ExternalSubscribed { name: "approval" }
    //   event_id 5: ExternalEvent { name: "approval", data: "old-1" }
    //   event_id 6: ExternalSubscribed { name: "approval" }
    // All stamped with duroxide_version "0.1.10".

    let execution_id = duroxide::INITIAL_EXECUTION_ID;
    let ev = |id, kind| common::make_versioned_event(id, instance, execution_id, None, kind, old_duroxide_version);

    // --- Turn 1: OrchestrationStarted + ExternalSubscribed ---
    common::seed_history_turn(
        &*store,
        WorkItem::StartOrchestration {
            instance: instance.to_string(),
            orchestration: "Upgrader".to_string(),
            version: Some("1.0.0".to_string()),
            input: "seed".to_string(),
            parent_instance: None,
            parent_id: None,
            execution_id,
        },
        execution_id,
        vec![
            ev(
                1,
                EventKind::OrchestrationStarted {
                    name: "Upgrader".to_string(),
                    version: "1.0.0".to_string(),
                    input: "seed".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            ),
            ev(
                2,
                EventKind::ExternalSubscribed {
                    name: "approval".to_string(),
                },
            ),
        ],
        vec![],
        ExecutionMetadata {
            orchestration_name: Some("Upgrader".to_string()),
            orchestration_version: Some("1.0.0".to_string()),
            pinned_duroxide_version: Some(semver::Version::new(0, 1, 10)),
            ..Default::default()
        },
    )
    .await;

    // --- Turn 2: ExternalEvent(old-0) + ExternalSubscribed ---
    common::seed_history_turn(
        &*store,
        WorkItem::ExternalRaised {
            instance: instance.to_string(),
            name: "approval".to_string(),
            data: "old-0".to_string(),
        },
        execution_id,
        vec![
            ev(
                3,
                EventKind::ExternalEvent {
                    name: "approval".to_string(),
                    data: "old-0".to_string(),
                },
            ),
            ev(
                4,
                EventKind::ExternalSubscribed {
                    name: "approval".to_string(),
                },
            ),
        ],
        vec![],
        ExecutionMetadata {
            orchestration_name: Some("Upgrader".to_string()),
            orchestration_version: Some("1.0.0".to_string()),
            ..Default::default()
        },
    )
    .await;

    // --- Turn 3: ExternalEvent(old-1) + ExternalSubscribed ---
    common::seed_history_turn(
        &*store,
        WorkItem::ExternalRaised {
            instance: instance.to_string(),
            name: "approval".to_string(),
            data: "old-1".to_string(),
        },
        execution_id,
        vec![
            ev(
                5,
                EventKind::ExternalEvent {
                    name: "approval".to_string(),
                    data: "old-1".to_string(),
                },
            ),
            ev(
                6,
                EventKind::ExternalSubscribed {
                    name: "approval".to_string(),
                },
            ),
        ],
        vec![],
        ExecutionMetadata {
            orchestration_name: Some("Upgrader".to_string()),
            orchestration_version: Some("1.0.0".to_string()),
            ..Default::default()
        },
    )
    .await;

    // Verify seeded history
    let seeded_history = store.read(instance).await.unwrap();
    assert_eq!(seeded_history.len(), 6, "should have 6 seeded events");
    assert_eq!(
        seeded_history[0].duroxide_version, old_duroxide_version,
        "OrchestrationStarted should be stamped with old version"
    );
    let sub_count = seeded_history
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ExternalSubscribed { .. }))
        .count();
    let evt_count = seeded_history
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::ExternalEvent { .. }))
        .count();
    assert_eq!(sub_count, 3, "should have 3 ExternalSubscribed");
    assert_eq!(evt_count, 2, "should have 2 ExternalEvent");

    // ========== Phase 2: "New duroxide" (0.1.16) — v1 + v2 ==========

    let v1_handler = |ctx: OrchestrationContext, _input: String| async move {
        let mut collected = Vec::new();
        for _ in 0..3 {
            let data = ctx.schedule_wait("approval").await;
            collected.push(data);
        }
        let state = collected.join(",");
        ctx.continue_as_new_versioned("2.0.0", state).await
    };

    let v2_handler = |ctx: OrchestrationContext, input: String| async move {
        let mut collected: Vec<String> = input.split(',').map(|s| s.to_string()).collect();
        for _ in 0..2 {
            let data = ctx.schedule_wait2("approval", "orders.us-east").await;
            collected.push(data);
        }
        Ok(format!("upgraded:{}", collected.join("+")))
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("Upgrader", v1_handler)
        .register_versioned("Upgrader", "2.0.0", v2_handler)
        .build();

    let options = RuntimeOptions {
        max_attempts: 10,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        options,
    )
    .await;

    let client = Client::new(store.clone());

    // Send 3rd v1 event — the new runtime must replay old history and resume
    client.raise_event(instance, "approval", "old-2").await.unwrap();

    // Wait for v2 execution to start (CAN happened)
    let v2_started = common::wait_for_history(
        store.clone(),
        instance,
        |hist| {
            hist.iter().any(|e| {
                matches!(
                    &e.kind,
                    EventKind::OrchestrationStarted { version, .. } if version == "2.0.0"
                )
            })
        },
        10000,
    )
    .await;
    assert!(v2_started, "phase2: v2 did not start after CAN");

    // Send v2 events
    for i in 0..2 {
        let expected_count = i + 1;
        let found = common::wait_for_history(
            store.clone(),
            instance,
            move |hist| {
                hist.iter()
                    .filter(|e| {
                        matches!(
                            &e.kind,
                            EventKind::ExternalSubscribed2 { name, topic }
                            if name == "approval" && topic == "orders.us-east"
                        )
                    })
                    .count()
                    >= expected_count
            },
            10000,
        )
        .await;
        assert!(found, "phase2: v2 subscription {i} did not appear");

        client
            .raise_event2(instance, "approval", "orders.us-east", format!("new-{i}"))
            .await
            .unwrap();

        let expected_evt = i + 1;
        let processed = common::wait_for_history(
            store.clone(),
            instance,
            move |hist| {
                hist.iter()
                    .filter(|e| matches!(&e.kind, EventKind::ExternalEvent2 { .. }))
                    .count()
                    >= expected_evt
            },
            10000,
        )
        .await;
        assert!(processed, "phase2: v2 event {i} not processed");
    }

    // Wait for final completion
    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(5))
        .await
        .unwrap();

    match &status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(
                output.as_str(),
                "upgraded:old-0+old-1+old-2+new-0+new-1",
                "output should contain data from both phases"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message());
        }
        other => panic!("unexpected status: {other:?}"),
    }

    // Verify v1 execution history retains old duroxide_version stamp
    let mgmt = store.as_management_capability().expect("management capability");
    let v1_history = mgmt.read_history_with_execution_id(instance, 1).await.unwrap();
    let started_event = v1_history
        .iter()
        .find(|e| matches!(&e.kind, EventKind::OrchestrationStarted { .. }))
        .unwrap();
    assert_eq!(
        started_event.duroxide_version, old_duroxide_version,
        "v1 OrchestrationStarted must retain the old duroxide version stamp"
    );
    assert!(
        v1_history
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed { .. })),
        "v1 execution must have ExternalSubscribed"
    );
    assert!(
        !v1_history
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed2 { .. })),
        "v1 execution must NOT have ExternalSubscribed2"
    );

    // Verify v2 execution has current duroxide_version and only v2 event kinds
    let v2_history = mgmt.read_history_with_execution_id(instance, 2).await.unwrap();
    let v2_started = v2_history
        .iter()
        .find(|e| matches!(&e.kind, EventKind::OrchestrationStarted { .. }))
        .unwrap();
    assert_eq!(
        v2_started.duroxide_version,
        env!("CARGO_PKG_VERSION"),
        "v2 OrchestrationStarted must be stamped with current duroxide version"
    );
    assert!(
        v2_history
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed2 { .. })),
        "v2 execution must have ExternalSubscribed2"
    );
    assert!(
        !v2_history
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed { .. })),
        "v2 execution must NOT have ExternalSubscribed"
    );

    rt.shutdown(None).await;
}

/// Serde acceptance test: with the replay-version-test feature, v2 events
/// can be deserialized successfully.
///
/// The rejection counterpart (v2 events FAIL deserialization without the feature)
/// lives in tests/serde_boundary_tests.rs and runs in the no-features pass.
#[test]
fn v2_events_deserialize_with_feature_flag() {
    let subscribed2_json = r#"{"type": "ExternalSubscribed2", "name": "x", "topic": "y"}"#;
    let event2_json = r#"{"type": "ExternalEvent2", "name": "x", "topic": "y", "data": "payload"}"#;

    let s2 = serde_json::from_str::<EventKind>(subscribed2_json)
        .expect("ExternalSubscribed2 should deserialize with feature flag");
    assert!(
        matches!(s2, EventKind::ExternalSubscribed2 { ref name, ref topic } if name == "x" && topic == "y"),
        "deserialized ExternalSubscribed2 should have correct fields"
    );

    let e2 =
        serde_json::from_str::<EventKind>(event2_json).expect("ExternalEvent2 should deserialize with feature flag");
    assert!(
        matches!(e2, EventKind::ExternalEvent2 { ref name, ref topic, ref data } if name == "x" && topic == "y" && data == "payload"),
        "deserialized ExternalEvent2 should have correct fields"
    );
}

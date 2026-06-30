// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::EventKind;
use duroxide::providers::Provider;
use duroxide::providers::ProviderAdmin;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, Event, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::time::Duration;

mod common;
use common::test_create_execution;

// Helper to create runtime with registries for tests
#[allow(dead_code)]
async fn create_test_runtime(activity_registry: ActivityRegistry) -> Arc<runtime::Runtime> {
    // Create a minimal orchestration registry for basic tests
    let orchestration_registry = OrchestrationRegistry::builder().build();
    runtime::Runtime::start(activity_registry, orchestration_registry).await
}

// 3) Deterministic replay on a tiny flow (activity only)
#[tokio::test]
async fn deterministic_replay_activity_only() {
    let orchestrator = |ctx: OrchestrationContext| async move {
        let a = ctx.schedule_activity("A", "2").await.unwrap();
        format!("a={a}")
    };

    let activity_registry = ActivityRegistry::builder()
        .register("A", |_ctx: ActivityContext, input: String| async move {
            Ok(input.parse::<i32>().unwrap_or(0).saturating_add(1).to_string())
        })
        .build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("TestOrchestration", move |ctx, _input| async move {
            Ok(orchestrator(ctx).await)
        })
        .build();

    let store = SqliteProvider::new_in_memory().await.unwrap();
    let store = Arc::new(store) as Arc<dyn Provider>;
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("inst-unit-1", "TestOrchestration", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-unit-1", std::time::Duration::from_secs(5))
        .await
        .unwrap();

    let output = match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => output,
        _ => panic!("Expected completed status"),
    };
    assert_eq!(output, "a=3");

    // Note: run_turn replay verification removed in Phase 2 (simplified mode only)
    // The runtime test above already verifies the orchestration works correctly.
    // Legacy run_turn is no longer available.
    rt.shutdown(None).await;
}

// Provider admin APIs moved to provider-local tests; runtime tests should use runtime-only APIs.

#[tokio::test]
async fn runtime_duplicate_orchestration_deduped_single_execution() {
    // Start runtime and attempt to start the same instance twice concurrently
    let activity_registry = ActivityRegistry::builder().build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("TestOrch", |ctx, _| async move {
            // Slow a bit to allow duplicate enqueue to happen
            ctx.schedule_timer(Duration::from_millis(20)).await;
            Ok("ok".to_string())
        })
        .build();

    let store = SqliteProvider::new_in_memory().await.unwrap();
    let store = Arc::new(store) as Arc<dyn Provider>;
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let inst = "dup-orch";

    let client = duroxide::Client::new(store.clone());
    // Fire two start requests for the same instance
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    // Both should resolve to the same single execution/result
    match client
        .wait_for_orchestration(inst, std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "ok"),
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Check history
    let hist1 = client.read_execution_history(inst, 1).await.unwrap();

    // Ensure there is only one terminal event in history
    let term_count = hist1
        .iter()
        .filter(|e| {
            matches!(
                &e.kind,
                EventKind::OrchestrationCompleted { .. } | EventKind::OrchestrationFailed { .. }
            )
        })
        .count();
    assert_eq!(term_count, 1, "should have exactly one terminal event");

    // Both handles observed the same execution: only one start/terminal
    let started_count = hist1
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::OrchestrationStarted { .. }))
        .count();
    assert_eq!(started_count, 1);

    rt.shutdown(None).await;
}

#[tokio::test]
async fn orchestration_descriptor_root_and_child() {
    // Root orchestrations
    let activity_registry = ActivityRegistry::builder().build();
    let parent = |ctx: OrchestrationContext, _| async move {
        let _ = ctx.schedule_sub_orchestration("ChildDsc", "x").await;
        Ok("done".into())
    };
    let child = |_ctx: OrchestrationContext, _input: String| async move { Ok("child".into()) };
    let reg = OrchestrationRegistry::builder()
        .register("ParentDsc", parent)
        .register("ChildDsc", child)
        .build();
    let store = SqliteProvider::new_in_memory().await.unwrap();
    let store = Arc::new(store) as Arc<dyn Provider>;
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("inst-desc", "ParentDsc", "seed")
        .await
        .unwrap();
    // wait for completion
    let _ = client
        .wait_for_orchestration("inst-desc", std::time::Duration::from_secs(2))
        .await;
    // Root descriptor
    let d = rt.get_orchestration_descriptor("inst-desc").await.unwrap();
    assert_eq!(d.name, "ParentDsc");
    assert!(!d.version.is_empty());
    assert!(d.parent_instance.is_none());
    assert!(d.parent_id.is_none());
    // Child descriptor (event_id=2 since OrchestrationStarted is event_id=1)
    let dchild = rt.get_orchestration_descriptor("inst-desc::sub::2").await.unwrap();
    assert_eq!(dchild.name, "ChildDsc");
    assert!(!dchild.version.is_empty());
    assert_eq!(dchild.parent_instance.as_deref(), Some("inst-desc"));
    assert_eq!(dchild.parent_id, Some(2));
    rt.shutdown(None).await;
}

#[tokio::test]
async fn orchestration_status_apis() {
    use duroxide::OrchestrationStatus;

    // Registry with two orchestrations: one completes after a short timer, one fails immediately
    let activity_registry = ActivityRegistry::builder().build();
    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ShortTimer", |ctx, _| async move {
            ctx.schedule_timer(Duration::from_millis(100)).await;
            Ok("ok".to_string())
        })
        .register("AlwaysFails", |_ctx, _| async move { Err("boom".to_string()) })
        .build();

    let store = SqliteProvider::new_in_memory().await.unwrap();
    let store = Arc::new(store) as Arc<dyn Provider>;
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    // NotFound for unknown instance
    let s = client.get_orchestration_status("no-such").await.unwrap();
    assert!(matches!(s, OrchestrationStatus::NotFound));

    // Start a running orchestration; should be Running after dispatcher processes it
    let inst_running = "inst-status-running";
    client
        .start_orchestration(inst_running, "ShortTimer", "")
        .await
        .unwrap();
    // Wait for the orchestrator dispatcher to process the queued work item
    let mut s1 = OrchestrationStatus::NotFound;
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        s1 = client.get_orchestration_status(inst_running).await.unwrap();
        if matches!(s1, OrchestrationStatus::Running { .. }) {
            break;
        }
    }
    assert!(
        matches!(s1, OrchestrationStatus::Running { .. }),
        "expected Running, got {s1:?}"
    );

    // After completion, should be Completed with output
    match client
        .wait_for_orchestration(inst_running, std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "ok"),
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }
    let s2 = client.get_orchestration_status(inst_running).await.unwrap();
    assert!(matches!(s2, OrchestrationStatus::Completed { .. }));
    if let OrchestrationStatus::Completed { output, .. } = s2 {
        assert_eq!(output, "ok");
    }

    // Failed orchestration
    let inst_fail = "inst-status-fail";
    client.start_orchestration(inst_fail, "AlwaysFails", "").await.unwrap();

    match client
        .wait_for_orchestration(inst_fail, std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Failed { details: _, .. } => {} // Expected failure
        duroxide::OrchestrationStatus::Completed { output, .. } => panic!("expected failure, got: {output}"),
        _ => panic!("unexpected orchestration status"),
    }
    let s3 = client.get_orchestration_status(inst_fail).await.unwrap();
    assert!(matches!(s3, OrchestrationStatus::Failed { .. }));
    if let OrchestrationStatus::Failed { details, .. } = s3 {
        assert_eq!(details.display_message(), "boom");
    }

    rt.shutdown(None).await;
}

// Providers: filesystem multi-execution persistence and latest read() contract
#[tokio::test]
async fn providers_fs_multi_execution_persistence_and_latest_read() {
    let _tmp = tempfile::tempdir().unwrap();
    let fs = SqliteProvider::new_in_memory().await.unwrap();

    // Create execution #1 using test helper
    test_create_execution(&fs, "pfs", "O", "0.0.0", "0", None, None)
        .await
        .unwrap();
    fs.append_with_execution(
        "pfs",
        1,
        vec![Event::with_event_id(
            2,
            "pfs".to_string(),
            1,
            None,
            EventKind::OrchestrationContinuedAsNew { input: "1".into() },
        )],
    )
    .await
    .unwrap();
    let e1_before = fs.read_with_execution("pfs", 1).await.unwrap_or_default();

    // Create execution #2 using test helper
    let _eid2 = test_create_execution(&fs, "pfs", "O", "0.0.0", "1", None, None)
        .await
        .unwrap();
    fs.append_with_execution(
        "pfs",
        2,
        vec![Event::with_event_id(
            2,
            "pfs".to_string(),
            2,
            None,
            EventKind::OrchestrationCompleted { output: "ok".into() },
        )],
    )
    .await
    .unwrap();

    // Execution list must contain both
    let execs = ProviderAdmin::list_executions(&fs, "pfs").await.unwrap_or_default();
    assert_eq!(execs, vec![1, 2]);

    // Older execution history remains unchanged
    let e1_after = fs.read_with_execution("pfs", 1).await.unwrap_or_default();
    assert_eq!(e1_before, e1_after);

    // Latest read() equals latest execution history
    let latest_hist = fs.read_with_execution("pfs", 2).await.unwrap_or_default();
    let current_hist = fs.read("pfs").await.unwrap_or_default();
    assert_eq!(current_hist, latest_hist);
}

// Providers: in-memory multi-execution persistence and latest read() contract
#[tokio::test]
async fn providers_inmem_multi_execution_persistence_and_latest_read() {
    let mem = SqliteProvider::new_in_memory().await.unwrap();

    test_create_execution(&mem, "pmem", "O", "0.0.0", "0", None, None)
        .await
        .unwrap();
    mem.append_with_execution(
        "pmem",
        1,
        vec![Event::with_event_id(
            2,
            "pmem".to_string(),
            1,
            None,
            EventKind::OrchestrationContinuedAsNew { input: "1".into() },
        )],
    )
    .await
    .unwrap();
    let e1_before = mem.read_with_execution("pmem", 1).await.unwrap_or_default();

    let _eid2 = test_create_execution(&mem, "pmem", "O", "0.0.0", "1", None, None)
        .await
        .unwrap();
    mem.append_with_execution(
        "pmem",
        2,
        vec![Event::with_event_id(
            2,
            "pmem".to_string(),
            2,
            None,
            EventKind::OrchestrationCompleted { output: "ok".into() },
        )],
    )
    .await
    .unwrap();

    let execs = ProviderAdmin::list_executions(&mem, "pmem").await.unwrap_or_default();
    assert_eq!(execs, vec![1, 2]);

    let e1_after = mem.read_with_execution("pmem", 1).await.unwrap_or_default();
    assert_eq!(e1_before, e1_after);

    let latest_hist = mem.read_with_execution("pmem", 2).await.unwrap_or_default();
    let current_hist = mem.read("pmem").await.unwrap_or_default();
    assert_eq!(current_hist, latest_hist);
}

// OrchestrationContext metadata accessors - converted to runtime test
#[tokio::test]
async fn orchestration_context_metadata_accessors() {
    let activity_registry = ActivityRegistry::builder().build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register_versioned("MetadataOrch", "2.1.0", |ctx, _| async move {
            // Verify all accessors return the expected values
            assert_eq!(ctx.instance_id(), "test-instance-123");
            assert_eq!(ctx.execution_id(), 1);
            assert_eq!(ctx.orchestration_name(), "MetadataOrch");
            assert_eq!(ctx.orchestration_version(), "2.1.0");
            Ok("done".to_string())
        })
        .build();

    let store = SqliteProvider::new_in_memory().await.unwrap();
    let store = Arc::new(store) as Arc<dyn Provider>;
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;

    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("test-instance-123", "MetadataOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-instance-123", Duration::from_secs(5))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "done"),
        other => panic!("Expected Completed, got {other:?}"),
    }

    rt.shutdown(None).await;
}

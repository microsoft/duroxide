// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::provider_validation::{Event, EventKind, ExecutionMetadata, start_item};
use crate::provider_validations::ProviderFactory;
use crate::providers::WorkItem;
use std::time::Duration;

/// Test management: list_instances returns all instance IDs
pub async fn test_list_instances<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing management: list_instances returns all instance IDs");
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    // Create a few instances
    for i in 0..3 {
        crate::provider_validation::create_instance(&*provider, &format!("mgmt-inst-{i}"))
            .await
            .unwrap();
    }

    // List all instances
    let instances = mgmt.list_instances().await.unwrap();
    assert!(instances.len() >= 3, "Should list all created instances");
    for i in 0..3 {
        assert!(
            instances.contains(&format!("mgmt-inst-{i}")),
            "Should include instance mgmt-inst-{i}"
        );
    }
    tracing::info!("✓ Test passed: list_instances verified");
}

/// Test management: list_instances_by_status filters correctly
pub async fn test_list_instances_by_status<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing management: list_instances_by_status filters correctly");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create instance and complete it
    provider
        .enqueue_for_orchestrator(start_item("mgmt-completed"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Ack with Completed status
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "mgmt-completed".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                status: Some("Completed".to_string()),
                output: Some("done".to_string()),
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // Query by status
    let completed = mgmt.list_instances_by_status("Completed").await.unwrap();
    assert!(
        completed.contains(&"mgmt-completed".to_string()),
        "Should list completed instance"
    );
    tracing::info!("✓ Test passed: list_instances_by_status verified");
}

/// Test management: list_executions returns all execution IDs
pub async fn test_list_executions<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing management: list_executions returns all execution IDs");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create instance with first execution
    provider
        .enqueue_for_orchestrator(start_item("mgmt-multi-exec"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "mgmt-multi-exec".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // List executions
    let executions = mgmt.list_executions("mgmt-multi-exec").await.unwrap();
    assert!(executions.contains(&1), "Should list execution 1");
    tracing::info!("✓ Test passed: list_executions verified");
}

/// Test management: get_instance_info returns metadata
pub async fn test_get_instance_info<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing management: get_instance_info returns metadata");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create and complete instance
    provider
        .enqueue_for_orchestrator(start_item("mgmt-info"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "mgmt-info".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "InfoOrch".to_string(),
                    version: "2.0.0".to_string(),
                    input: "test".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("InfoOrch".to_string()),
                orchestration_version: Some("2.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // Get instance info
    let info = mgmt.get_instance_info("mgmt-info").await.unwrap();
    assert_eq!(info.instance_id, "mgmt-info");
    assert_eq!(info.orchestration_name, "InfoOrch");
    assert_eq!(info.orchestration_version, "2.0.0");
    assert_eq!(info.current_execution_id, 1);
    tracing::info!("✓ Test passed: get_instance_info verified");
}

/// Test management: get_execution_info returns execution metadata
pub async fn test_get_execution_info<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing management: get_execution_info returns execution metadata");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create instance
    provider
        .enqueue_for_orchestrator(start_item("mgmt-exec-info"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "mgmt-bulk-del".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                status: Some("Completed".to_string()),
                output: Some("result".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // Get execution info
    let info = mgmt.get_execution_info("mgmt-exec-info", 1).await.unwrap();
    assert_eq!(info.execution_id, 1);
    assert_eq!(info.status, "Completed");
    assert_eq!(info.output, Some("result".to_string()));
    tracing::info!("✓ Test passed: get_execution_info verified");
}

/// Test management: get_system_metrics returns accurate counts
pub async fn test_get_system_metrics<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing management: get_system_metrics returns accurate counts");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Get baseline metrics
    let metrics = mgmt.get_system_metrics().await.unwrap();
    let baseline_instances = metrics.total_instances;

    // Create new instance
    provider
        .enqueue_for_orchestrator(start_item("mgmt-metrics"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "mgmt-cancel".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // Metrics should reflect new instance
    let updated_metrics = mgmt.get_system_metrics().await.unwrap();
    assert!(
        updated_metrics.total_instances > baseline_instances,
        "total_instances should increase"
    );
    assert!(updated_metrics.total_events > 0, "total_events should be > 0");
    tracing::info!("✓ Test passed: get_system_metrics verified");
}

/// Test management: get_queue_depths returns current queue sizes
pub async fn test_get_queue_depths<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing management: get_queue_depths returns current queue sizes");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Get baseline
    let depths = mgmt.get_queue_depths().await.unwrap();
    let baseline_orch = depths.orchestrator_queue;

    // Enqueue work
    provider
        .enqueue_for_orchestrator(start_item("mgmt-queue"), None)
        .await
        .unwrap();

    // Queue depth should increase
    let updated_depths = mgmt.get_queue_depths().await.unwrap();
    assert!(
        updated_depths.orchestrator_queue > baseline_orch,
        "orchestrator_queue should increase"
    );
    tracing::info!("✓ Test passed: get_queue_depths verified");
}

// ─────────────────────────────────────────────────────────────────────────────
// get_instance_stats provider validation tests
// ─────────────────────────────────────────────────────────────────────────────

/// get_instance_stats returns None for a non-existent instance.
pub async fn test_get_instance_stats_nonexistent<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing get_instance_stats: returns None for non-existent instance");
    let provider = factory.create_provider().await;

    let stats = provider.get_instance_stats("no-such-instance").await.unwrap();
    assert!(stats.is_none(), "non-existent instance should return None");

    tracing::info!("✓ Test passed: get_instance_stats returns None for non-existent instance");
}

/// get_instance_stats returns correct history counts and sizes.
pub async fn test_get_instance_stats_history<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing get_instance_stats: history counts and sizes");
    let provider = factory.create_provider().await;

    // create_instance acks with OrchestrationStarted (1 event)
    crate::provider_validation::create_instance(&*provider, "stats-hist")
        .await
        .unwrap();

    let stats = provider
        .get_instance_stats("stats-hist")
        .await
        .unwrap()
        .expect("instance should exist");

    assert!(stats.history_event_count >= 1, "should have at least 1 event");
    assert!(stats.history_size_bytes > 0, "history should have non-zero size");
    assert_eq!(stats.kv_user_key_count, 0, "no KV keys set");
    assert_eq!(stats.kv_total_value_bytes, 0, "no KV value bytes");
    assert_eq!(stats.queue_pending_count, 0, "no carry-forward events");

    tracing::info!("✓ Test passed: get_instance_stats history counts correct");
}

/// get_instance_stats returns correct KV metrics.
pub async fn test_get_instance_stats_kv<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing get_instance_stats: KV metrics");
    let provider = factory.create_provider().await;

    crate::provider_validation::create_instance(&*provider, "stats-kv")
        .await
        .unwrap();

    // Ack with KV events and Completed status so kv_delta merges into kv_store
    provider
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: "stats-kv".to_string(),
                name: "poke".to_string(),
                data: "{}".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    let (_, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![
                Event::with_event_id(
                    100,
                    "stats-kv",
                    1,
                    None,
                    EventKind::KeyValueSet {
                        key: "k1".to_string(),
                        value: "aaa".to_string(), // 3 bytes
                        last_updated_at_ms: 0,
                    },
                ),
                Event::with_event_id(
                    101,
                    "stats-kv",
                    1,
                    None,
                    EventKind::KeyValueSet {
                        key: "k2".to_string(),
                        value: "bbbbb".to_string(), // 5 bytes
                        last_updated_at_ms: 0,
                    },
                ),
                Event::with_event_id(
                    102,
                    "stats-kv",
                    1,
                    None,
                    EventKind::OrchestrationCompleted {
                        output: "done".to_string(),
                    },
                ),
            ],
            vec![],
            vec![],
            ExecutionMetadata {
                status: Some("Completed".to_string()),
                output: Some("done".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    let stats = provider
        .get_instance_stats("stats-kv")
        .await
        .unwrap()
        .expect("instance should exist");

    assert_eq!(stats.kv_user_key_count, 2, "should have 2 KV keys");
    assert_eq!(stats.kv_total_value_bytes, 8, "3 + 5 = 8 bytes");

    tracing::info!("✓ Test passed: get_instance_stats KV metrics correct");
}

/// get_instance_stats returns correct queue_pending_count when carry-forward events exist.
pub async fn test_get_instance_stats_carry_forward<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing get_instance_stats: carry-forward events → queue_pending_count");
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_orchestrator(start_item("cf-stats"), None)
        .await
        .unwrap();

    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    // Ack with OrchestrationStarted that carries 3 pending external events
    provider
        .ack_orchestration_item(
            &lock_token,
            crate::INITIAL_EXECUTION_ID,
            vec![Event::with_event_id(
                crate::INITIAL_EVENT_ID,
                "cf-stats",
                crate::INITIAL_EXECUTION_ID,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: Some(vec![
                        ("raised-1".to_string(), r#"{"data":"a"}"#.to_string()),
                        ("raised-2".to_string(), r#"{"data":"b"}"#.to_string()),
                        ("raised-3".to_string(), r#"{"data":"c"}"#.to_string()),
                    ]),
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    let stats = provider
        .get_instance_stats("cf-stats")
        .await
        .unwrap()
        .expect("instance should exist");

    assert_eq!(stats.queue_pending_count, 3, "should have 3 carry-forward events");
    assert!(stats.history_event_count >= 1, "should have at least 1 event");
    assert!(stats.history_size_bytes > 0, "history should have non-zero size");
    assert_eq!(stats.kv_user_key_count, 0, "no KV keys set");

    tracing::info!("✓ Test passed: get_instance_stats carry-forward count correct");
}

/// get_instance_stats returns correct KV stats when values exist only in kv_delta (non-terminal ack).
pub async fn test_get_instance_stats_kv_delta_only<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing get_instance_stats: KV delta-only (non-terminal ack)");
    let provider = factory.create_provider().await;

    // Create a Running instance
    crate::provider_validation::create_instance(&*provider, "kv-delta-stats")
        .await
        .unwrap();

    // Send an external event so we can fetch and ack again
    provider
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: "kv-delta-stats".to_string(),
                name: "poke".to_string(),
                data: "{}".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    let (_, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    // Ack with KV writes but NO terminal status — keeps the instance Running.
    // KV values live in kv_delta (not merged into kv_store yet).
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![
                Event::with_event_id(
                    100,
                    "kv-delta-stats",
                    1,
                    None,
                    EventKind::KeyValueSet {
                        key: "dk1".to_string(),
                        value: "xxx".to_string(), // 3 bytes
                        last_updated_at_ms: 0,
                    },
                ),
                Event::with_event_id(
                    101,
                    "kv-delta-stats",
                    1,
                    None,
                    EventKind::KeyValueSet {
                        key: "dk2".to_string(),
                        value: "yyyyyy".to_string(), // 6 bytes
                        last_updated_at_ms: 0,
                    },
                ),
                Event::with_event_id(
                    102,
                    "kv-delta-stats",
                    1,
                    None,
                    EventKind::ActivityScheduled {
                        name: "Noop".to_string(),
                        input: "{}".to_string(),
                        session_id: None,
                        tag: None,
                    },
                ),
            ],
            vec![],
            vec![],
            ExecutionMetadata::default(), // no status change → stays Running
            vec![],
        )
        .await
        .unwrap();

    let stats = provider
        .get_instance_stats("kv-delta-stats")
        .await
        .unwrap()
        .expect("instance should exist");

    assert_eq!(stats.kv_user_key_count, 2, "should see 2 delta-only KV keys");
    assert_eq!(stats.kv_total_value_bytes, 9, "3 + 6 = 9 bytes from delta");
    assert_eq!(stats.queue_pending_count, 0, "no carry-forward events");

    tracing::info!("✓ Test passed: get_instance_stats KV delta-only correct");
}

/// get_instance_stats returns correct KV stats after delta→store merge on completion,
/// including key overwrites during the merge.
pub async fn test_get_instance_stats_kv_merged<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing get_instance_stats: KV delta→store merge with overwrites");
    let provider = factory.create_provider().await;

    // Create a Running instance
    crate::provider_validation::create_instance(&*provider, "kv-merged-stats")
        .await
        .unwrap();

    // ── Turn 1: write KV keys, stay Running ──
    provider
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: "kv-merged-stats".to_string(),
                name: "poke1".to_string(),
                data: "{}".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    let (_, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item for turn 1");

    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![
                Event::with_event_id(
                    100,
                    "kv-merged-stats",
                    1,
                    None,
                    EventKind::KeyValueSet {
                        key: "mk1".to_string(),
                        value: "aaa".to_string(), // 3 bytes
                        last_updated_at_ms: 0,
                    },
                ),
                Event::with_event_id(
                    101,
                    "kv-merged-stats",
                    1,
                    None,
                    EventKind::KeyValueSet {
                        key: "mk2".to_string(),
                        value: "bb".to_string(), // 2 bytes
                        last_updated_at_ms: 0,
                    },
                ),
                Event::with_event_id(
                    102,
                    "kv-merged-stats",
                    1,
                    None,
                    EventKind::ActivityScheduled {
                        name: "Noop".to_string(),
                        input: "{}".to_string(),
                        session_id: None,
                        tag: None,
                    },
                ),
            ],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Verify delta-only state
    let stats = provider
        .get_instance_stats("kv-merged-stats")
        .await
        .unwrap()
        .expect("instance should exist (delta phase)");
    assert_eq!(stats.kv_user_key_count, 2, "delta phase: 2 keys");
    assert_eq!(stats.kv_total_value_bytes, 5, "delta phase: 3 + 2 = 5 bytes");

    // ── Turn 2: overwrite mk1, then complete (triggers merge) ──
    provider
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: "kv-merged-stats".to_string(),
                name: "poke2".to_string(),
                data: "{}".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    let (_, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item for turn 2");

    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![
                Event::with_event_id(
                    200,
                    "kv-merged-stats",
                    1,
                    None,
                    EventKind::KeyValueSet {
                        key: "mk1".to_string(),
                        value: "zzzzz".to_string(), // 5 bytes, overwrites "aaa"
                        last_updated_at_ms: 1,
                    },
                ),
                Event::with_event_id(
                    201,
                    "kv-merged-stats",
                    1,
                    None,
                    EventKind::OrchestrationCompleted {
                        output: "done".to_string(),
                    },
                ),
            ],
            vec![],
            vec![],
            ExecutionMetadata {
                status: Some("Completed".to_string()),
                output: Some("done".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // After merge: mk1="zzzzz" (5), mk2="bb" (2) → 2 keys, 7 bytes
    let stats = provider
        .get_instance_stats("kv-merged-stats")
        .await
        .unwrap()
        .expect("instance should exist (merged phase)");
    assert_eq!(
        stats.kv_user_key_count, 2,
        "merged: still 2 keys (overwrite, not duplicate)"
    );
    assert_eq!(stats.kv_total_value_bytes, 7, "merged: 5 + 2 = 7 bytes");
    assert_eq!(stats.queue_pending_count, 0, "no carry-forward events");

    tracing::info!("✓ Test passed: get_instance_stats KV delta→store merge correct");
}

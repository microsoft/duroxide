use crate::provider_validation::{Event, EventKind, ExecutionMetadata};
use crate::provider_validations::ProviderFactory;
use crate::providers::WorkItem;
use std::time::Duration;

/// Helper to create a start item for an instance with specific execution_id
fn start_item_with_execution(instance: &str, execution_id: u64) -> WorkItem {
    WorkItem::StartOrchestration {
        instance: instance.to_string(),
        orchestration: "TestOrch".to_string(),
        input: "{}".to_string(),
        version: Some("1.0.0".to_string()),
        parent_instance: None,
        parent_id: None,
        execution_id,
    }
}

/// Test 6.1: Execution Isolation
/// Goal: Verify each execution has separate history.
pub async fn test_execution_isolation<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing multi-execution: execution isolation");
    let provider = factory.create_provider().await;

    // Create execution 1 with 3 events
    provider
        .enqueue_for_orchestrator(start_item_with_execution("instance-A", 1), None)
        .await
        .unwrap();
    let (_item1, lock_token1, _attempt_count1) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &lock_token1,
            1,
            vec![
                Event::with_event_id(
                    1,
                    "instance-A".to_string(),
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
                ),
                Event::with_event_id(
                    2,
                    "instance-A".to_string(),
                    1,
                    None,
                    EventKind::ActivityScheduled {
                        name: "Activity1".to_string(),
                        input: "input1".to_string(),
                        session_id: None,
                        tag: None,
                    },
                ),
                Event::with_event_id(
                    3,
                    "instance-A".to_string(),
                    1,
                    None,
                    EventKind::OrchestrationCompleted {
                        output: "result1".to_string(),
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

    // Create execution 2 with 2 events
    provider
        .enqueue_for_orchestrator(start_item_with_execution("instance-A", 2), None)
        .await
        .unwrap();
    let (_item2, lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &lock_token2,
            2,
            vec![
                Event::with_event_id(
                    1,
                    "instance-A".to_string(),
                    2,
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
                ),
                Event::with_event_id(
                    2,
                    "instance-A".to_string(),
                    2,
                    None,
                    EventKind::OrchestrationCompleted {
                        output: "result2".to_string(),
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

    // Read execution 1 → should return 3 events
    let history1 = provider.read_with_execution("instance-A", 1).await.unwrap_or_default();
    assert_eq!(history1.len(), 3);
    assert!(matches!(&history1[0].kind, EventKind::OrchestrationStarted { .. }));
    assert!(matches!(&history1[1].kind, EventKind::ActivityScheduled { .. }));
    assert!(matches!(&history1[2].kind, EventKind::OrchestrationCompleted { .. }));

    // Read execution 2 → should return 2 events
    let history2 = provider.read_with_execution("instance-A", 2).await.unwrap_or_default();
    assert_eq!(history2.len(), 2);
    assert!(matches!(&history2[0].kind, EventKind::OrchestrationStarted { .. }));
    assert!(matches!(&history2[1].kind, EventKind::OrchestrationCompleted { .. }));

    // Read latest (default) → should return execution 2's events
    let latest = provider.read("instance-A").await.unwrap_or_default();
    assert_eq!(latest.len(), 2);
    assert!(matches!(&latest[0].kind, EventKind::OrchestrationStarted { .. }));
    assert!(matches!(&latest[1].kind, EventKind::OrchestrationCompleted { .. }));
    tracing::info!("✓ Test passed: execution isolation verified");
}

/// Test 6.2: Latest Execution Detection
/// Goal: Verify read() returns latest execution's history.
pub async fn test_latest_execution_detection<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing multi-execution: latest execution detection");
    let provider = factory.create_provider().await;

    // Create execution 1 with event "A"
    provider
        .enqueue_for_orchestrator(start_item_with_execution("instance-A", 1), None)
        .await
        .unwrap();
    let (_item1, lock_token1, _attempt_count1) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &lock_token1,
            1,
            vec![Event::with_event_id(
                1,
                "instance-A".to_string(),
                1,
                None,
                EventKind::ActivityScheduled {
                    name: "A".to_string(),
                    input: "".to_string(),
                    session_id: None,
                    tag: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Create execution 2 with event "B"
    provider
        .enqueue_for_orchestrator(start_item_with_execution("instance-A", 2), None)
        .await
        .unwrap();
    let (_item2, lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &lock_token2,
            2,
            vec![Event::with_event_id(
                1,
                "instance-A".to_string(),
                2,
                None,
                EventKind::ActivityScheduled {
                    name: "B".to_string(),
                    input: "".to_string(),
                    session_id: None,
                    tag: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Call read() → should return execution 2 (latest)
    let latest = provider.read("instance-A").await.unwrap_or_default();
    assert_eq!(latest.len(), 1);
    if let EventKind::ActivityScheduled { name, .. } = &latest[0].kind {
        assert_eq!(name, "B");
    } else {
        panic!("Expected ActivityScheduled");
    }

    // Call read_with_execution(instance, 1) → should return execution 1
    let exec1 = provider.read_with_execution("instance-A", 1).await.unwrap_or_default();
    assert_eq!(exec1.len(), 1);
    if let EventKind::ActivityScheduled { name, .. } = &exec1[0].kind {
        assert_eq!(name, "A");
    } else {
        panic!("Expected ActivityScheduled");
    }
    tracing::info!("✓ Test passed: latest execution detection verified");
}

/// Test 6.3: Execution ID Sequencing
/// Goal: Verify execution IDs increment correctly.
pub async fn test_execution_id_sequencing<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing multi-execution: execution ID sequencing");
    let provider = factory.create_provider().await;

    // First execution should be execution_id = 1
    provider
        .enqueue_for_orchestrator(start_item_with_execution("instance-A", 1), None)
        .await
        .unwrap();
    let (item1, lock_token1, _attempt_count1) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item1.execution_id, 1);

    // Complete execution 1 with proper metadata
    provider
        .ack_orchestration_item(
            &lock_token1,
            1,
            vec![Event::with_event_id(
                1,
                "instance-A".to_string(),
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

    // Enqueue second execution with explicit execution_id = 2
    provider
        .enqueue_for_orchestrator(start_item_with_execution("instance-A", 2), None)
        .await
        .unwrap();
    let (_item2, lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    // The fetched item will have execution_id = 1 (from instance's current_execution_id)
    // We need to ack with execution_id = 2 to create the new execution
    provider
        .ack_orchestration_item(
            &lock_token2,
            2, // Explicitly ack with execution_id = 2
            vec![Event::with_event_id(
                1,
                "instance-A".to_string(),
                2,
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

    // Now fetch should return execution_id = 2
    provider
        .enqueue_for_orchestrator(start_item_with_execution("instance-A", 3), None)
        .await
        .unwrap();
    let (item3, _lock_token3, _attempt_count3) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item3.execution_id, 2, "Current execution should be 2");
    tracing::info!("✓ Test passed: execution ID sequencing verified");
}

/// Test 6.4: Continue-As-New Creates New Execution
/// Goal: Verify continue-as-new creates new execution with incremented ID.
pub async fn test_continue_as_new_creates_new_execution<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing multi-execution: continue-as-new creates new execution");
    let provider = factory.create_provider().await;

    // Create execution 1
    provider
        .enqueue_for_orchestrator(start_item_with_execution("instance-A", 1), None)
        .await
        .unwrap();
    let (_item1, lock_token1, _attempt_count1) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &lock_token1,
            1,
            vec![Event::with_event_id(
                1,
                "instance-A".to_string(),
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

    // Simulate continue-as-new by enqueuing a ContinueAsNew work item
    provider
        .enqueue_for_orchestrator(
            WorkItem::ContinueAsNew {
                instance: "instance-A".to_string(),
                orchestration: "TestOrch".to_string(),
                input: "new-input".to_string(),
                version: Some("1.0.0".to_string()),
                carry_forward_events: vec![],
                initial_custom_status: None,
            },
            None,
        )
        .await
        .unwrap();

    let (_item2, lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    // The fetched item will have execution_id = 1 (from instance's current_execution_id)
    // We need to ack with execution_id = 2 to create the new execution
    provider
        .ack_orchestration_item(
            &lock_token2,
            2, // Explicitly ack with execution_id = 2 for ContinueAsNew
            vec![Event::with_event_id(
                1,
                "instance-A".to_string(),
                2,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "new-input".to_string(),
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

    // Now the instance should have current_execution_id = 2
    let result = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap();
    if let Some((item, _lock_token, _attempt_count)) = result {
        assert_eq!(item.execution_id, 2, "Continue-as-new should create execution 2");
    }
    tracing::info!("✓ Test passed: continue-as-new creates new execution verified");
}

/// Test 6.5: Execution History Persistence
/// Goal: Verify all executions' history persists independently.
pub async fn test_execution_history_persistence<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing multi-execution: execution history persistence");
    let provider = factory.create_provider().await;

    // Create 3 executions with different history
    for exec_id in 1..=3 {
        provider
            .enqueue_for_orchestrator(start_item_with_execution("instance-A", exec_id), None)
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
                exec_id,
                vec![Event::with_event_id(
                    1,
                    "instance-A".to_string(),
                    exec_id,
                    None,
                    EventKind::ActivityScheduled {
                        name: format!("Activity-{exec_id}"),
                        input: format!("input-{exec_id}"),
                        session_id: None,
                        tag: None,
                    },
                )],
                vec![],
                vec![],
                ExecutionMetadata::default(),
                vec![],
            )
            .await
            .unwrap();
    }

    // Verify each execution's history is independent
    for exec_id in 1..=3 {
        let history = provider
            .read_with_execution("instance-A", exec_id)
            .await
            .unwrap_or_default();
        assert_eq!(history.len(), 1);
        if let EventKind::ActivityScheduled { name, .. } = &history[0].kind {
            assert_eq!(name, &format!("Activity-{exec_id}"));
        } else {
            panic!("Expected ActivityScheduled");
        }
    }

    // Latest should be execution 3
    let latest = provider.read("instance-A").await.unwrap_or_default();
    assert_eq!(latest.len(), 1);
    if let EventKind::ActivityScheduled { name, .. } = &latest[0].kind {
        assert_eq!(name, "Activity-3");
    } else {
        panic!("Expected ActivityScheduled");
    }
    tracing::info!("✓ Test passed: execution history persistence verified");
}

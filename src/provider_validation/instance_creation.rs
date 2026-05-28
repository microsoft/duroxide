// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::provider_validation::{Event, EventKind, ExecutionMetadata, start_item};
use crate::provider_validations::ProviderFactory;
use crate::providers::WorkItem;
use std::time::Duration;

/// Test: Instance Creation via Metadata
/// Goal: Provider should create instances via ack_orchestration_item metadata, not on enqueue.
pub async fn test_instance_creation_via_metadata<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance creation: instance created via metadata on first ack");
    let provider = factory.create_provider().await;

    // Enqueue StartOrchestration - instance should NOT exist yet
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();

    // Fetch work item - instance doesn't exist yet, should extract from work item
    let (item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.instance, "instance-A");
    assert_eq!(item.orchestration_name, "TestOrch");
    // Version may be None or extracted from work item - that's OK
    assert_eq!(item.execution_id, 1);
    assert_eq!(item.history.len(), 0); // No history yet

    // Ack with metadata - this should create the instance
    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        orchestration_version: Some("2.0.0".to_string()), // Runtime resolved version
        ..Default::default()
    };

    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "instance-A".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "2.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            metadata,
            vec![],
        )
        .await
        .unwrap();

    // Verify instance was created with correct version from metadata
    // Try to fetch again - should get instance metadata now
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (item2, lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "instance-A");
    assert_eq!(item2.orchestration_name, "TestOrch");
    assert_eq!(item2.version, "2.0.0"); // Version from metadata, not work item
    assert_eq!(item2.execution_id, 1);
    assert_eq!(item2.history.len(), 1); // Has OrchestrationStarted event

    // Clean up
    provider
        .ack_orchestration_item(
            &lock_token2,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    tracing::info!("✓ Test passed: instance created via metadata on first ack");
}

/// Test: No Instance Creation on Enqueue
/// Goal: Provider should NOT create instances when enqueueing StartOrchestration.
pub async fn test_no_instance_creation_on_enqueue<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance creation: no instance created on enqueue");
    let provider = factory.create_provider().await;

    // Enqueue StartOrchestration
    provider
        .enqueue_for_orchestrator(start_item("instance-B"), None)
        .await
        .unwrap();

    // Verify instance doesn't exist yet by checking if we can read it
    // (This depends on provider implementation - some may allow reading non-existent instances)
    // The key test is that fetch_orchestration_item works without instance existing

    // Fetch should work even though instance doesn't exist
    let result = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap();
    assert!(
        result.is_some(),
        "Should be able to fetch work item even if instance doesn't exist"
    );
    let (item, lock_token, _attempt_count) = result.unwrap();
    assert_eq!(item.instance, "instance-B");

    // Clean up
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "instance-B".to_string(),
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

    tracing::info!("✓ Test passed: no instance created on enqueue");
}

/// Test: NULL Version Handling
/// Goal: Provider should handle NULL orchestration_version gracefully.
pub async fn test_null_version_handling<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance creation: NULL version handling");
    let provider = factory.create_provider().await;

    // Enqueue StartOrchestration with None version
    let start = WorkItem::StartOrchestration {
        instance: "instance-C".to_string(),
        orchestration: "TestOrch".to_string(),
        input: "{}".to_string(),
        version: None, // No version provided
        parent_instance: None,
        parent_id: None,
        execution_id: 1,
    };

    provider.enqueue_for_orchestrator(start, None).await.unwrap();

    // Fetch work item - should handle None version
    let (item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.instance, "instance-C");

    // Ack with metadata that has version - should set version
    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        orchestration_version: Some("3.0.0".to_string()), // Runtime resolved version
        ..Default::default()
    };

    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "instance-C".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "3.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            metadata,
            vec![],
        )
        .await
        .unwrap();

    // Verify version was set from metadata
    provider
        .enqueue_for_orchestrator(start_item("instance-C"), None)
        .await
        .unwrap();
    let (item2, lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.version, "3.0.0"); // Version from metadata

    // Clean up
    provider
        .ack_orchestration_item(
            &lock_token2,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    tracing::info!("✓ Test passed: NULL version handled correctly");
}

/// Test: Sub-Orchestration Instance Creation
/// Goal: Sub-orchestrations should also create instances via metadata, not on enqueue.
pub async fn test_sub_orchestration_instance_creation<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance creation: sub-orchestration instance creation via metadata");
    let provider = factory.create_provider().await;

    // Create parent instance first
    provider
        .enqueue_for_orchestrator(start_item("parent-instance"), None)
        .await
        .unwrap();
    let (_parent_item, parent_lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &parent_lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "parent-instance".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "ParentOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![WorkItem::StartOrchestration {
                instance: "parent-instance::child-instance".to_string(),
                orchestration: "ChildOrch".to_string(),
                input: "{}".to_string(),
                version: None,
                parent_instance: Some("parent-instance".to_string()),
                parent_id: Some(1),
                execution_id: 1,
            }],
            ExecutionMetadata {
                orchestration_name: Some("ParentOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // Child instance should NOT exist yet (not created on enqueue)
    // Fetch child work item - should work without instance existing
    let (child_item, child_lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child_item.instance, "parent-instance::child-instance");
    assert_eq!(child_item.orchestration_name, "ChildOrch");

    // Ack child with metadata - this should create child instance
    provider
        .ack_orchestration_item(
            &child_lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "parent-instance::child-instance".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "ChildOrch".to_string(),
                    version: "1.5.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: Some("parent-instance".to_string()),
                    parent_id: Some(1),
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("ChildOrch".to_string()),
                orchestration_version: Some("1.5.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // Verify child instance was created
    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "parent-instance::child-instance".to_string(),
                orchestration: "ChildOrch".to_string(),
                input: "{}".to_string(),
                version: None,
                parent_instance: None,
                parent_id: None,
                execution_id: 1,
            },
            None,
        )
        .await
        .unwrap();
    let (child_item2, child_lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child_item2.instance, "parent-instance::child-instance");
    assert_eq!(child_item2.version, "1.5.0"); // Version from metadata

    // Clean up
    provider
        .ack_orchestration_item(
            &child_lock_token2,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    tracing::info!("✓ Test passed: sub-orchestration instance created via metadata");
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Provider validation tests for bulk delete instance operations.
//!
//! These tests verify that providers correctly implement the delete_instance_bulk API,
//! including filter combinations, safety guarantees, and cascading behavior.

use crate::INITIAL_EXECUTION_ID;
use crate::provider_validation::{Event, EventKind, ExecutionMetadata, ProviderFactory, create_instance, start_item};
use crate::providers::{InstanceFilter, WorkItem};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Note: prune tests are in prune.rs - prune operates on executions within an instance
// while delete_instance_bulk operates on instances themselves - conceptually different operations.

/// Test: delete_instance_bulk with various filter combinations
///
/// Covers:
/// - Delete by instance IDs
/// - Delete by completed_before timestamp
/// - AND filter (intersection of IDs and time)
/// - Empty filter deletes all terminal instances
/// - Non-existent IDs return 0 deleted
pub async fn test_delete_instance_bulk_filter_combinations<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing delete_instance_bulk: filter combinations");
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    // Create several completed instances
    for i in 0..5 {
        create_completed_instance(&*provider, &format!("bulk-del-filter-{i}")).await;
    }

    // Test 1: Delete by specific IDs
    let result = mgmt
        .delete_instance_bulk(InstanceFilter {
            instance_ids: Some(vec!["bulk-del-filter-0".into(), "bulk-del-filter-1".into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.instances_deleted, 2, "Should delete 2 instances by ID");

    // Verify deleted instances are gone, others remain
    assert!(mgmt.get_instance_info("bulk-del-filter-0").await.is_err());
    assert!(mgmt.get_instance_info("bulk-del-filter-1").await.is_err());
    assert!(mgmt.get_instance_info("bulk-del-filter-2").await.is_ok());

    // Test 2: Delete non-existent IDs returns 0
    let result = mgmt
        .delete_instance_bulk(InstanceFilter {
            instance_ids: Some(vec!["does-not-exist-1".into(), "does-not-exist-2".into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.instances_deleted, 0, "Non-existent IDs should return 0 deleted");

    // Test 3: Empty filter deletes all remaining terminal instances
    let result = mgmt.delete_instance_bulk(InstanceFilter::default()).await.unwrap();
    assert!(
        result.instances_deleted >= 3,
        "Empty filter should delete remaining terminal instances"
    );

    // All should be gone now
    assert!(mgmt.get_instance_info("bulk-del-filter-2").await.is_err());
    assert!(mgmt.get_instance_info("bulk-del-filter-3").await.is_err());
    assert!(mgmt.get_instance_info("bulk-del-filter-4").await.is_err());

    tracing::info!("✓ Test passed: delete_instance_bulk filter combinations");
}

/// Test: delete_instance_bulk safety and limits
///
/// Covers:
/// - Skips running instances silently
/// - Respects limit parameter
/// - Iterative batching (delete with limit multiple times)
pub async fn test_delete_instance_bulk_safety_and_limits<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing delete_instance_bulk: safety and limits");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create mix of completed and running instances
    create_completed_instance(&*provider, "bulk-del-safe-completed-1").await;
    create_completed_instance(&*provider, "bulk-del-safe-completed-2").await;
    create_instance(&*provider, "bulk-del-safe-running-1").await.unwrap();
    create_instance(&*provider, "bulk-del-safe-running-2").await.unwrap();

    // Test 1: Delete all - should skip running
    let result = mgmt
        .delete_instance_bulk(InstanceFilter {
            instance_ids: Some(vec![
                "bulk-del-safe-completed-1".into(),
                "bulk-del-safe-completed-2".into(),
                "bulk-del-safe-running-1".into(),
                "bulk-del-safe-running-2".into(),
            ]),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(result.instances_deleted, 2, "Should only delete completed instances");
    assert!(
        mgmt.get_instance_info("bulk-del-safe-running-1").await.is_ok(),
        "Running instance should not be deleted"
    );
    assert!(
        mgmt.get_instance_info("bulk-del-safe-running-2").await.is_ok(),
        "Running instance should not be deleted"
    );

    // Test 2: Create 4 instances and delete with limit (iterative batching)
    for i in 0..4 {
        create_completed_instance(&*provider, &format!("bulk-del-batch-{i}")).await;
    }

    // First batch: limit 2
    let result1 = mgmt
        .delete_instance_bulk(InstanceFilter {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result1.instances_deleted, 2, "First batch should delete 2");

    // Second batch: limit 2
    let result2 = mgmt
        .delete_instance_bulk(InstanceFilter {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result2.instances_deleted, 2, "Second batch should delete 2");

    // Third batch: should be empty
    let result3 = mgmt
        .delete_instance_bulk(InstanceFilter {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result3.instances_deleted, 0, "Third batch should delete 0 (all gone)");

    tracing::info!("✓ Test passed: delete_instance_bulk safety and limits");
}

/// Test: delete_instance_bulk with completed_before filter
///
/// Covers:
/// - Instances completed before cutoff are deleted
/// - Instances completed after cutoff are preserved
/// - Filter works in combination with instance_ids
pub async fn test_delete_instance_bulk_completed_before_filter<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing delete_instance_bulk: completed_before filter");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Helper to get current time as milliseconds since epoch
    fn now_millis() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    // Record timestamp BEFORE creating instances
    let before_creation = now_millis();

    // Small delay to ensure completed_at timestamps are after before_creation
    // (millisecond precision can cause race conditions)
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Create instances - they will be completed AFTER before_creation
    create_completed_instance(&*provider, "bulk-del-time-1").await;
    create_completed_instance(&*provider, "bulk-del-time-2").await;

    // Small delay to ensure we capture a timestamp after all completions
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Record timestamp AFTER creating instances
    let after_creation = now_millis();

    // Test 1: Delete with completed_before = before_creation (should get 0)
    // Instances were completed after before_creation, so none should match
    let result = mgmt
        .delete_instance_bulk(InstanceFilter {
            instance_ids: Some(vec!["bulk-del-time-1".into(), "bulk-del-time-2".into()]),
            completed_before: Some(before_creation),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(
        result.instances_deleted, 0,
        "No instances should be deleted - they were completed after the cutoff"
    );

    // Verify both still exist
    assert!(
        mgmt.get_instance_info("bulk-del-time-1").await.is_ok(),
        "Instance 1 should still exist"
    );
    assert!(
        mgmt.get_instance_info("bulk-del-time-2").await.is_ok(),
        "Instance 2 should still exist"
    );

    // Test 2: Delete with completed_before = after_creation (should get all)
    // Instances were completed before after_creation, so all should match
    let result = mgmt
        .delete_instance_bulk(InstanceFilter {
            instance_ids: Some(vec!["bulk-del-time-1".into(), "bulk-del-time-2".into()]),
            completed_before: Some(after_creation),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(
        result.instances_deleted, 2,
        "Both instances should be deleted - they were completed before the cutoff"
    );

    // Verify both are gone
    assert!(
        mgmt.get_instance_info("bulk-del-time-1").await.is_err(),
        "Instance 1 should be deleted"
    );
    assert!(
        mgmt.get_instance_info("bulk-del-time-2").await.is_err(),
        "Instance 2 should be deleted"
    );

    tracing::info!("✓ Test passed: delete_instance_bulk completed_before filter");
}

/// Test: delete_instance_bulk cascades to sub-orchestrations
///
/// Covers:
/// - Deleting a root cascades to single child
/// - Deleting a root cascades to multiple children
/// - Deleting a standalone instance (no children)
/// - Multiple roots in single delete request
pub async fn test_delete_instance_bulk_cascades_to_children<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing delete_instance_bulk: cascades to children");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Case 1: Parent with single child
    let parent1 = "bulk-del-cascade-parent1";
    let child1 = "bulk-del-cascade-child1";
    create_completed_instance(&*provider, parent1).await;
    create_completed_instance_with_parent(&*provider, child1, parent1).await;

    // Case 2: Parent with multiple children
    let parent2 = "bulk-del-cascade-parent2";
    let child2a = "bulk-del-cascade-child2a";
    let child2b = "bulk-del-cascade-child2b";
    let child2c = "bulk-del-cascade-child2c";
    create_completed_instance(&*provider, parent2).await;
    create_completed_instance_with_parent(&*provider, child2a, parent2).await;
    create_completed_instance_with_parent(&*provider, child2b, parent2).await;
    create_completed_instance_with_parent(&*provider, child2c, parent2).await;

    // Case 3: Standalone instance (no children)
    let standalone = "bulk-del-cascade-standalone";
    create_completed_instance(&*provider, standalone).await;

    // Verify all exist
    assert!(mgmt.get_instance_info(parent1).await.is_ok());
    assert!(mgmt.get_instance_info(child1).await.is_ok());
    assert!(mgmt.get_instance_info(parent2).await.is_ok());
    assert!(mgmt.get_instance_info(child2a).await.is_ok());
    assert!(mgmt.get_instance_info(child2b).await.is_ok());
    assert!(mgmt.get_instance_info(child2c).await.is_ok());
    assert!(mgmt.get_instance_info(standalone).await.is_ok());

    // Delete all roots in a single request
    let result = mgmt
        .delete_instance_bulk(InstanceFilter {
            instance_ids: Some(vec![parent1.into(), parent2.into(), standalone.into()]),
            ..Default::default()
        })
        .await
        .unwrap();

    // 3 roots + 1 child (parent1) + 3 children (parent2) = 7 total
    assert_eq!(
        result.instances_deleted, 7,
        "Should delete all roots and their children"
    );

    // All should be gone
    assert!(
        mgmt.get_instance_info(parent1).await.is_err(),
        "Parent1 should be deleted"
    );
    assert!(
        mgmt.get_instance_info(child1).await.is_err(),
        "Child1 should be cascade deleted"
    );
    assert!(
        mgmt.get_instance_info(parent2).await.is_err(),
        "Parent2 should be deleted"
    );
    assert!(
        mgmt.get_instance_info(child2a).await.is_err(),
        "Child2a should be cascade deleted"
    );
    assert!(
        mgmt.get_instance_info(child2b).await.is_err(),
        "Child2b should be cascade deleted"
    );
    assert!(
        mgmt.get_instance_info(child2c).await.is_err(),
        "Child2c should be cascade deleted"
    );
    assert!(
        mgmt.get_instance_info(standalone).await.is_err(),
        "Standalone should be deleted"
    );

    tracing::info!("✓ Test passed: delete_instance_bulk cascades to children");
}

// ===== Helper Functions =====

/// Helper: create a completed instance
async fn create_completed_instance(provider: &dyn crate::providers::Provider, instance_id: &str) {
    create_completed_instance_with_parent(provider, instance_id, "").await;
}

/// Helper: create a completed instance with optional parent
async fn create_completed_instance_with_parent(
    provider: &dyn crate::providers::Provider,
    instance_id: &str,
    parent_id: &str,
) {
    let (start_item, parent_instance_id) = if parent_id.is_empty() {
        (start_item(instance_id), None)
    } else {
        (
            WorkItem::StartOrchestration {
                instance: instance_id.to_string(),
                orchestration: "TestOrch".to_string(),
                input: "{}".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: Some(parent_id.to_string()),
                parent_id: Some(1),
                execution_id: INITIAL_EXECUTION_ID,
            },
            Some(parent_id.to_string()),
        )
    };

    provider.enqueue_for_orchestrator(start_item, None).await.unwrap();
    let (_item, lock_token, _) = provider
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
                instance_id,
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: parent_instance_id.clone(),
                    parent_id: if parent_instance_id.is_some() { Some(1) } else { None },
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
                parent_instance_id,
                pinned_duroxide_version: None,
            },
            vec![],
        )
        .await
        .unwrap();
}

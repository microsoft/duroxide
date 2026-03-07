//! Provider validation tests for deletion operations.
//!
//! These tests verify that providers correctly implement the deletion API,
//! including cascading deletes, force delete semantics, and the critical
//! lock deletion that prevents zombie instance recreation.

use crate::INITIAL_EXECUTION_ID;
use crate::provider_validation::{Event, EventKind, ExecutionMetadata, ProviderFactory, create_instance, start_item};
use crate::providers::{TagFilter, WorkItem};
use std::time::Duration;

// ===== Consolidated Delete Tests =====

/// Test: delete_instance works for terminal instances (completed and failed)
///
/// Covers:
/// - Delete completed instance
/// - Delete failed instance
/// - Verify all tables cleaned (history, executions, queues)
/// - Result counts are accurate
pub async fn test_delete_terminal_instances<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing deletion: delete terminal instances (completed and failed)");
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    // Create and complete an instance
    let completed_id = "delete-term-completed";
    create_completed_instance(&*provider, completed_id).await;

    // Create and fail an instance
    let failed_id = "delete-term-failed";
    create_failed_instance(&*provider, failed_id).await;

    // Create a cancelled instance
    // Note: Cancelled instances manifest as "Failed" status with AppErrorKind::Cancelled.
    // From a deletion perspective, they behave identically to failed instances.
    let cancelled_id = "delete-term-cancelled";
    create_cancelled_instance(&*provider, cancelled_id).await;

    // Verify all exist
    assert!(mgmt.get_instance_info(completed_id).await.is_ok());
    assert!(mgmt.get_instance_info(failed_id).await.is_ok());
    assert!(mgmt.get_instance_info(cancelled_id).await.is_ok());

    // Delete completed instance and verify counts
    let result = mgmt.delete_instance(completed_id, false).await.unwrap();
    assert!(result.instances_deleted >= 1, "Completed instance should be deleted");
    assert!(result.executions_deleted >= 1, "Should delete at least 1 execution");
    assert!(result.events_deleted >= 1, "Should delete at least 1 event");

    // Verify completed instance is gone
    assert!(
        mgmt.get_instance_info(completed_id).await.is_err(),
        "Completed instance should not exist after deletion"
    );

    // Delete failed instance
    let result = mgmt.delete_instance(failed_id, false).await.unwrap();
    assert!(result.instances_deleted >= 1, "Failed instance should be deleted");

    // Verify failed instance is gone
    assert!(
        mgmt.get_instance_info(failed_id).await.is_err(),
        "Failed instance should not exist after deletion"
    );

    // Delete cancelled instance
    let result = mgmt.delete_instance(cancelled_id, false).await.unwrap();
    assert!(result.instances_deleted >= 1, "Cancelled instance should be deleted");

    // Verify cancelled instance is gone
    assert!(
        mgmt.get_instance_info(cancelled_id).await.is_err(),
        "Cancelled instance should not exist after deletion"
    );

    tracing::info!("✓ Test passed: delete terminal instances (completed, failed, cancelled)");
}

/// Test: delete running instance - rejected without force, succeeds with force
///
/// Covers:
/// - Running instance rejected without force
/// - Force=true succeeds on running instance
pub async fn test_delete_running_rejected_force_succeeds<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing deletion: running rejected, force succeeds");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create a running instance (no terminal status)
    let instance_id = "delete-running-force";
    create_instance(&*provider, instance_id).await.unwrap();

    // Try to delete without force - should fail
    let result = mgmt.delete_instance(instance_id, false).await;
    assert!(result.is_err(), "Should reject deletion of running instance");
    let err = result.unwrap_err();
    assert!(!err.is_retryable(), "Error should be permanent");
    assert!(
        err.to_string().to_lowercase().contains("running"),
        "Error message should mention 'running'"
    );

    // Verify instance still exists
    assert!(
        mgmt.get_instance_info(instance_id).await.is_ok(),
        "Instance should still exist after rejected deletion"
    );

    // Force delete - should succeed
    let result = mgmt.delete_instance(instance_id, true).await.unwrap();
    assert!(result.instances_deleted >= 1, "Force delete should succeed");

    // Verify instance is gone
    assert!(
        mgmt.get_instance_info(instance_id).await.is_err(),
        "Instance should not exist after force deletion"
    );

    tracing::info!("✓ Test passed: running rejected, force succeeds");
}

/// Test: delete non-existent instance returns error
pub async fn test_delete_nonexistent_instance<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing deletion: non-existent instance returns error");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    let result = mgmt.delete_instance("does-not-exist-12345", false).await;
    assert!(result.is_err(), "Should return error for non-existent instance");
    let err = result.unwrap_err();
    assert!(!err.is_retryable(), "Error should be permanent");
    assert!(
        err.to_string().to_lowercase().contains("not found"),
        "Error should mention 'not found'"
    );

    tracing::info!("✓ Test passed: non-existent instance returns error");
}

/// Test: delete cleans up queues, locks, and allows ID reuse
///
/// Covers:
/// - Queue entries deleted
/// - Locks cleared
/// - ID can be reused after deletion
pub async fn test_delete_cleans_queues_and_locks<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing deletion: cleans queues, locks, allows ID reuse");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    let instance_id = "delete-queues-locks";

    // Create instance with pending queue messages
    provider
        .enqueue_for_orchestrator(start_item(instance_id), None)
        .await
        .unwrap();

    // Also enqueue an activity (to test worker queue cleanup)
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: instance_id.to_string(),
            execution_id: 1,
            id: 1,
            name: "TestActivity".to_string(),
            input: "{}".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .unwrap();

    // Fetch and ack to create the instance
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
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // Delete and verify queue messages were cleaned
    let result = mgmt.delete_instance(instance_id, false).await.unwrap();
    assert!(result.queue_messages_deleted >= 1, "Should have deleted queue messages");

    // Verify ID can be reused (locks were cleared)
    // Use a distinct orchestration name and input to verify this is truly a new instance
    let new_orch_name = "ReusedOrch";
    let new_input = r#"{"reused": true}"#;
    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: instance_id.to_string(),
                orchestration: new_orch_name.to_string(),
                version: Some("2.0.0".to_string()),
                input: new_input.to_string(),
                parent_instance: None,
                parent_id: None,
                execution_id: 1,
            },
            None,
        )
        .await
        .unwrap();
    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Verify the fetched item has the new orchestration details
    assert_eq!(
        item.orchestration_name, new_orch_name,
        "Fetched item should have new orchestration name"
    );

    // This ack should succeed (no stale lock blocking it)
    let result = provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![Event::with_event_id(
                1,
                instance_id,
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: new_orch_name.to_string(),
                    version: "2.0.0".to_string(),
                    input: new_input.to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some(new_orch_name.to_string()),
                orchestration_version: Some("2.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await;

    assert!(
        result.is_ok(),
        "Should be able to recreate instance after deletion (locks cleared)"
    );

    // Verify the new instance has the new orchestration details (not stale data)
    let info = mgmt.get_instance_info(instance_id).await.unwrap();
    assert_eq!(
        info.orchestration_name, new_orch_name,
        "Recreated instance should have new orchestration name, not stale data"
    );

    // Cleanup
    mgmt.delete_instance(instance_id, true).await.unwrap();

    tracing::info!("✓ Test passed: cleans queues, locks, allows ID reuse");
}

/// Test: cascade delete hierarchy
///
/// Covers:
/// - Cannot delete sub-orchestration directly (with or without force)
/// - Delete root cascades to children
/// - Deep hierarchy (3+ levels) cascades correctly
/// - Multiple children are all deleted
/// - Result aggregates counts from all descendants
pub async fn test_cascade_delete_hierarchy<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing deletion: cascade delete hierarchy");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create a hierarchy: root -> child1, child2
    //                     child1 -> grandchild (3 levels deep)
    let root_id = "cascade-root";
    let child1_id = "cascade-child1";
    let child2_id = "cascade-child2";
    let grandchild_id = "cascade-grandchild";

    // Create root
    create_completed_instance_with_parent(&*provider, root_id, None).await;

    // Create child1 (parent = root)
    create_completed_instance_with_parent(&*provider, child1_id, Some(root_id)).await;

    // Create child2 (parent = root)
    create_completed_instance_with_parent(&*provider, child2_id, Some(root_id)).await;

    // Create grandchild (parent = child1) - 3 levels deep
    create_completed_instance_with_parent(&*provider, grandchild_id, Some(child1_id)).await;

    // Verify all exist
    assert!(mgmt.get_instance_info(root_id).await.is_ok());
    assert!(mgmt.get_instance_info(child1_id).await.is_ok());
    assert!(mgmt.get_instance_info(child2_id).await.is_ok());
    assert!(mgmt.get_instance_info(grandchild_id).await.is_ok());

    // Try to delete child1 directly - should fail
    let result = mgmt.delete_instance(child1_id, false).await;
    assert!(result.is_err(), "Should not allow direct deletion of sub-orchestration");
    let err = result.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("sub-orchestration")
            || err.to_string().to_lowercase().contains("root")
            || err.to_string().to_lowercase().contains("parent"),
        "Error should mention sub-orchestration, root, or parent"
    );

    // Try with force=true - should still fail
    let result = mgmt.delete_instance(child1_id, true).await;
    assert!(result.is_err(), "Should not allow force deletion of sub-orchestration");

    // Try to delete grandchild directly - should fail
    let result = mgmt.delete_instance(grandchild_id, false).await;
    assert!(
        result.is_err(),
        "Should not allow direct deletion of deeply nested sub-orchestration"
    );

    // Delete root - should cascade to all descendants
    let result = mgmt.delete_instance(root_id, false).await.unwrap();
    assert!(result.instances_deleted >= 1, "Root should be deleted");
    // Result should aggregate counts from all 4 instances
    assert!(
        result.executions_deleted >= 4,
        "Should delete executions from all 4 instances"
    );

    // All should be gone
    assert!(mgmt.get_instance_info(root_id).await.is_err(), "Root should be deleted");
    assert!(
        mgmt.get_instance_info(child1_id).await.is_err(),
        "Child1 should be cascade deleted"
    );
    assert!(
        mgmt.get_instance_info(child2_id).await.is_err(),
        "Child2 should be cascade deleted"
    );
    assert!(
        mgmt.get_instance_info(grandchild_id).await.is_err(),
        "Grandchild should be cascade deleted"
    );

    tracing::info!("✓ Test passed: cascade delete hierarchy");
}

/// CRITICAL TEST: Force delete prevents ack recreation (zombie prevention)
///
/// This test verifies the critical safety property: when an instance is force-deleted
/// while an orchestration turn is in progress, the subsequent ack must fail and
/// the instance must NOT be recreated.
pub async fn test_force_delete_prevents_ack_recreation<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ CRITICAL TEST: force delete prevents ack recreation");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    let instance_id = "zombie-prevention";

    // Step 1: Create instance and fetch (acquire lock)
    provider
        .enqueue_for_orchestrator(start_item(instance_id), None)
        .await
        .unwrap();
    let (_item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Step 2: Ack to create the instance
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

    // Step 3: Enqueue another message and fetch (acquire new lock)
    provider
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: instance_id.to_string(),
                name: "test-event".to_string(),
                data: "{}".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    let (_item2, lock_token2, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Step 4: Force delete the instance (while we hold the lock)
    let delete_result = mgmt.delete_instance(instance_id, true).await.unwrap();
    assert!(delete_result.instances_deleted >= 1, "Force delete should succeed");

    // Step 5: Try to ack with the old lock token - this MUST fail
    let ack_result = provider
        .ack_orchestration_item(
            &lock_token2,
            1,
            vec![Event::with_event_id(
                2,
                instance_id,
                1,
                None,
                EventKind::ExternalEvent {
                    name: "test-event".to_string(),
                    data: "{}".to_string(),
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await;

    // The ack should fail because the lock was deleted
    assert!(
        ack_result.is_err(),
        "CRITICAL: Ack must fail after force delete to prevent zombie recreation"
    );

    // Step 6: Verify instance was NOT recreated
    let info_result = mgmt.get_instance_info(instance_id).await;
    assert!(
        info_result.is_err(),
        "CRITICAL: Instance must NOT be recreated after force delete"
    );

    tracing::info!("✓ CRITICAL TEST PASSED: force delete prevents ack recreation");
}

// ===== Helper Functions =====

/// Helper: create a completed instance
pub(crate) async fn create_completed_instance(provider: &dyn crate::providers::Provider, instance_id: &str) {
    create_completed_instance_with_parent(provider, instance_id, None).await;
}

/// Helper: create a completed instance with optional parent
async fn create_completed_instance_with_parent(
    provider: &dyn crate::providers::Provider,
    instance_id: &str,
    parent_id: Option<&str>,
) {
    let start_item = if let Some(parent) = parent_id {
        WorkItem::StartOrchestration {
            instance: instance_id.to_string(),
            orchestration: "TestOrch".to_string(),
            input: "{}".to_string(),
            version: Some("1.0.0".to_string()),
            parent_instance: Some(parent.to_string()),
            parent_id: Some(1),
            execution_id: INITIAL_EXECUTION_ID,
        }
    } else {
        start_item(instance_id)
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
                    parent_instance: parent_id.map(|s| s.to_string()),
                    parent_id: parent_id.map(|_| 1),
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
                parent_instance_id: parent_id.map(|s| s.to_string()),
                pinned_duroxide_version: None,
            },
            vec![],
        )
        .await
        .unwrap();
}

/// Helper: create a failed instance
async fn create_failed_instance(provider: &dyn crate::providers::Provider, instance_id: &str) {
    provider
        .enqueue_for_orchestrator(start_item(instance_id), None)
        .await
        .unwrap();
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
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                status: Some("Failed".to_string()),
                output: Some("error: something went wrong".to_string()),
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();
}

async fn create_cancelled_instance(provider: &dyn crate::providers::Provider, instance_id: &str) {
    provider
        .enqueue_for_orchestrator(start_item(instance_id), None)
        .await
        .unwrap();
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
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                // Cancelled instances have "Failed" status with a cancelled reason
                status: Some("Failed".to_string()),
                output: Some("cancelled: user requested cancellation".to_string()),
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();
}

// TODO: Consider moving primitive API tests (list_children, get_parent_id, get_instance_tree,
// delete_instances_atomic) to a new file: `hierarchy.rs` or `tree_operations.rs`.
// These primitives are about instance tree navigation, distinct from deletion-specific tests.
// ===== Primitive API Tests =====

/// Test: list_children returns direct children only
///
/// Covers:
/// - Returns empty for leaf nodes
/// - Returns direct children only (not grandchildren)
/// - Order doesn't matter (just needs all children)
pub async fn test_list_children<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing primitive: list_children");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create a hierarchy: root -> child1, child2 -> grandchild
    let root_id = "list-children-root";
    let child1_id = format!("{root_id}::sub::2");
    let child2_id = format!("{root_id}::sub::3");
    let grandchild_id = format!("{child2_id}::sub::4");

    // Create root
    create_completed_instance(&*provider, root_id).await;

    // Create children (with parent_instance_id set)
    create_child_instance(&*provider, &child1_id, root_id).await;
    create_child_instance(&*provider, &child2_id, root_id).await;

    // Create grandchild
    create_child_instance(&*provider, &grandchild_id, &child2_id).await;

    // Test list_children on root - should return child1 and child2 only
    let children = mgmt.list_children(root_id).await.unwrap();
    assert_eq!(children.len(), 2, "Root should have 2 direct children");
    assert!(children.contains(&child1_id), "Should contain child1");
    assert!(children.contains(&child2_id), "Should contain child2");
    assert!(!children.contains(&grandchild_id), "Should NOT contain grandchild");

    // Test list_children on child2 - should return grandchild only
    let children = mgmt.list_children(&child2_id).await.unwrap();
    assert_eq!(children.len(), 1, "child2 should have 1 child");
    assert!(children.contains(&grandchild_id), "Should contain grandchild");

    // Test list_children on leaf - should return empty
    let children = mgmt.list_children(&grandchild_id).await.unwrap();
    assert!(children.is_empty(), "Leaf should have no children");

    // Test list_children on non-existent instance - should return empty (not error)
    let children = mgmt.list_children("non-existent").await.unwrap();
    assert!(children.is_empty(), "Non-existent instance should return empty");

    // Cleanup
    mgmt.delete_instance(root_id, true).await.unwrap();

    tracing::info!("✓ Test passed: list_children");
}

/// Test: get_parent_id returns correct parent or None for root
///
/// Covers:
/// - Returns None for root orchestrations
/// - Returns Some(parent_id) for sub-orchestrations
/// - Returns error for non-existent instance
pub async fn test_delete_get_parent_id<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing primitive: get_parent_id");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create a root and a child
    let root_id = "parent-id-root";
    let child_id = format!("{root_id}::sub::2");

    create_completed_instance(&*provider, root_id).await;
    create_child_instance(&*provider, &child_id, root_id).await;

    // Test root has no parent
    let parent = mgmt.get_parent_id(root_id).await.unwrap();
    assert!(parent.is_none(), "Root should have no parent");

    // Test child has parent
    let parent = mgmt.get_parent_id(&child_id).await.unwrap();
    assert_eq!(parent, Some(root_id.to_string()), "Child should have root as parent");

    // Test non-existent instance returns error
    let result = mgmt.get_parent_id("non-existent").await;
    assert!(result.is_err(), "Non-existent instance should return error");

    // Cleanup
    mgmt.delete_instance(root_id, true).await.unwrap();

    tracing::info!("✓ Test passed: get_parent_id");
}

/// Test: get_instance_tree returns full hierarchy
///
/// Covers:
/// - Returns single instance for leaf nodes
/// - Returns full tree for root with descendants
/// - Contains all instances in the hierarchy (order is implementation-defined)
pub async fn test_delete_get_instance_tree<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing composite: get_instance_tree");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create a hierarchy: root -> child1, child2 -> grandchild
    let root_id = "tree-root";
    let child1_id = format!("{root_id}::sub::2");
    let child2_id = format!("{root_id}::sub::3");
    let grandchild_id = format!("{child2_id}::sub::4");

    create_completed_instance(&*provider, root_id).await;
    create_child_instance(&*provider, &child1_id, root_id).await;
    create_child_instance(&*provider, &child2_id, root_id).await;
    create_child_instance(&*provider, &grandchild_id, &child2_id).await;

    // Get tree from root
    let tree = mgmt.get_instance_tree(root_id).await.unwrap();

    // Verify tree properties
    assert_eq!(tree.root_id, root_id, "Root ID should match");
    assert_eq!(tree.size(), 4, "Tree should have 4 instances");
    assert!(!tree.is_root_only(), "Tree should have children");

    // Verify all instances are in the tree (order is implementation-defined)
    assert!(tree.all_ids.contains(&root_id.to_string()));
    assert!(tree.all_ids.contains(&child1_id));
    assert!(tree.all_ids.contains(&child2_id));
    assert!(tree.all_ids.contains(&grandchild_id));

    // Get tree from leaf - should be single instance
    let leaf_tree = mgmt.get_instance_tree(&grandchild_id).await.unwrap();
    assert_eq!(leaf_tree.size(), 1, "Leaf tree should have 1 instance");
    assert!(leaf_tree.is_root_only(), "Single instance tree should be root-only");

    // Cleanup
    mgmt.delete_instance(root_id, true).await.unwrap();

    tracing::info!("✓ Test passed: get_instance_tree");
}

/// Test: delete_instances_atomic deletes multiple instances atomically
///
/// Covers:
/// - Deletes all specified instances
/// - Respects force flag for running instances
/// - Returns aggregate counts
pub async fn test_delete_instances_atomic<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing primitive: delete_instances_atomic");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create three independent completed instances
    let ids = vec![
        "atomic-del-1".to_string(),
        "atomic-del-2".to_string(),
        "atomic-del-3".to_string(),
    ];

    for id in &ids {
        create_completed_instance(&*provider, id).await;
    }

    // Verify all exist
    for id in &ids {
        assert!(mgmt.get_instance_info(id).await.is_ok());
    }

    // Delete all atomically
    let result = mgmt.delete_instances_atomic(&ids, false).await.unwrap();

    assert!(result.instances_deleted >= 1, "Should report deletion");
    assert!(result.executions_deleted >= 3, "Should delete at least 3 executions");

    // Note on atomicity testing: The rollback behavior (when a delete fails, nothing is deleted)
    // is tested by `test_delete_instances_atomic_orphan_detection`, which verifies that when
    // the orphan check fails, the entire transaction rolls back and no instances are deleted.
    // Testing other failure scenarios would require fault injection in the provider implementation.

    // Verify all are gone
    for id in &ids {
        assert!(
            mgmt.get_instance_info(id).await.is_err(),
            "Instance {id} should be gone"
        );
    }

    tracing::info!("✓ Test passed: delete_instances_atomic");
}

/// Test: delete_instances_atomic respects force flag
///
/// Covers:
/// - Fails without force when any instance is running
/// - Succeeds with force even when instances are running
pub async fn test_delete_instances_atomic_force<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing primitive: delete_instances_atomic with force");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create one completed and one running instance
    let completed_id = "atomic-force-completed".to_string();
    let running_id = "atomic-force-running".to_string();

    create_completed_instance(&*provider, &completed_id).await;
    create_instance(&*provider, &running_id).await.unwrap(); // Running (no terminal status)

    let ids = vec![completed_id.clone(), running_id.clone()];

    // Without force - should fail
    let result = mgmt.delete_instances_atomic(&ids, false).await;
    assert!(
        result.is_err(),
        "Should fail without force when running instance present"
    );

    // Verify both still exist
    assert!(mgmt.get_instance_info(&completed_id).await.is_ok());
    assert!(mgmt.get_instance_info(&running_id).await.is_ok());

    // With force - should succeed
    let result = mgmt.delete_instances_atomic(&ids, true).await.unwrap();
    assert!(result.instances_deleted >= 1, "Should delete with force");

    // Verify both are gone
    assert!(mgmt.get_instance_info(&completed_id).await.is_err());
    assert!(mgmt.get_instance_info(&running_id).await.is_err());

    tracing::info!("✓ Test passed: delete_instances_atomic with force");
}

/// CRITICAL TEST: delete_instances_atomic detects orphan race condition
///
/// This test verifies that `delete_instances_atomic` prevents orphans that could
/// be created if a child is spawned between `get_instance_tree()` and the delete call.
///
/// Covers:
/// - Fails when deleting parent but not child (would create orphan)
/// - Error message indicates the issue
/// - Transaction rolls back (nothing deleted)
/// - Succeeds when all instances in hierarchy are included
pub async fn test_delete_instances_atomic_orphan_detection<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ CRITICAL TEST: delete_instances_atomic orphan detection");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create a hierarchy: root -> child
    let root_id = "orphan-detect-root";
    let child_id = format!("{root_id}::sub::2");

    create_completed_instance(&*provider, root_id).await;
    create_child_instance(&*provider, &child_id, root_id).await;

    // Verify both exist
    assert!(mgmt.get_instance_info(root_id).await.is_ok());
    assert!(mgmt.get_instance_info(&child_id).await.is_ok());

    // Try to delete only the root (simulates race: child spawned after get_instance_tree)
    // This MUST fail to prevent orphaning the child
    let result = mgmt.delete_instances_atomic(&[root_id.to_string()], false).await;

    assert!(
        result.is_err(),
        "CRITICAL: delete_instances_atomic must fail when it would create orphans"
    );

    let err = result.unwrap_err();
    let err_msg = err.to_string().to_lowercase();
    assert!(
        err_msg.contains("child") || err_msg.contains("orphan") || err_msg.contains("tree"),
        "Error should mention child/orphan/tree issue: {err}"
    );

    // Verify nothing was deleted (transaction rolled back)
    assert!(
        mgmt.get_instance_info(root_id).await.is_ok(),
        "Root should still exist after failed delete"
    );
    assert!(
        mgmt.get_instance_info(&child_id).await.is_ok(),
        "Child should still exist after failed delete"
    );

    // Now delete with both instances - should succeed
    let result = mgmt
        .delete_instances_atomic(&[root_id.to_string(), child_id.clone()], false)
        .await;
    assert!(result.is_ok(), "Delete with complete tree should succeed: {result:?}");

    // Verify both are gone
    assert!(mgmt.get_instance_info(root_id).await.is_err(), "Root should be deleted");
    assert!(
        mgmt.get_instance_info(&child_id).await.is_err(),
        "Child should be deleted"
    );

    tracing::info!("✓ CRITICAL TEST PASSED: delete_instances_atomic orphan detection");
}

/// CRITICAL TEST: Stale activity from deleted instance doesn't corrupt recreated instance
///
/// Scenario:
/// 1. Create instance A (execution_id=1), schedule activity (id=2)
/// 2. Activity starts execution (worker fetches + holds lock)
/// 3. Force delete instance A
/// 4. Recreate instance A with same ID/name/version/input (execution_id=1 again)
/// 5. Old activity tries to complete → ack should FAIL
/// 6. New instance should NOT see stale completion
///
/// This tests a critical safety invariant: force delete must clear worker_queue
/// entries so stale completions can't corrupt a recreated instance.
pub async fn test_stale_activity_after_delete_recreate<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ CRITICAL TEST: stale activity after delete+recreate");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();
    let instance_id = "stale-activity-recreate";

    // Step 1: Create instance and schedule an activity
    provider
        .enqueue_for_orchestrator(start_item(instance_id), None)
        .await
        .unwrap();

    let (_item, orch_lock, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Ack with OrchestrationStarted + ActivityScheduled events
    provider
        .ack_orchestration_item(
            &orch_lock,
            INITIAL_EXECUTION_ID,
            vec![
                Event::with_event_id(
                    1,
                    instance_id,
                    INITIAL_EXECUTION_ID,
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
                    instance_id,
                    INITIAL_EXECUTION_ID,
                    Some(2),
                    EventKind::ActivityScheduled {
                        name: "OldActivity".to_string(),
                        input: "old-input".to_string(),
                        session_id: None,
                        tag: None,
                    },
                ),
            ],
            vec![WorkItem::ActivityExecute {
                instance: instance_id.to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: 2,
                name: "OldActivity".to_string(),
                input: "old-input".to_string(),
                session_id: None,
                tag: None,
            }],
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

    // Step 2: Worker fetches the activity (holds the lock)
    let (_work_item, old_worker_lock, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // Step 3: Force delete instance A
    let delete_result = mgmt.delete_instance(instance_id, true).await.unwrap();
    assert!(delete_result.instances_deleted >= 1, "Force delete should succeed");

    // Verify instance is gone
    assert!(
        mgmt.get_instance_info(instance_id).await.is_err(),
        "Instance should be deleted"
    );

    // Step 4: Recreate instance A with same ID, name, version, input
    provider
        .enqueue_for_orchestrator(start_item(instance_id), None)
        .await
        .unwrap();

    let (_item2, orch_lock2, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Ack the start of new instance
    provider
        .ack_orchestration_item(
            &orch_lock2,
            INITIAL_EXECUTION_ID,
            vec![Event::with_event_id(
                1,
                instance_id,
                INITIAL_EXECUTION_ID,
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

    // Step 5: Verify new instance exists
    let info = mgmt.get_instance_info(instance_id).await.unwrap();
    assert_eq!(info.orchestration_name, "TestOrch");

    // Step 6: OLD activity tries to complete - should FAIL
    // The worker_queue entry was deleted when we force-deleted the instance,
    // so ack_work_item should fail because the lock_token no longer exists.
    let old_ack_result = provider
        .ack_work_item(
            &old_worker_lock,
            Some(WorkItem::ActivityCompleted {
                instance: instance_id.to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: 2,
                result: "stale-result-from-old-instance".to_string(),
            }),
        )
        .await;

    // CRITICAL: The stale ack must fail
    assert!(
        old_ack_result.is_err(),
        "CRITICAL: Stale activity ack must fail to prevent corruption of recreated instance"
    );

    // Step 7: Verify new instance wasn't corrupted
    // Enqueue an event to trigger a fetch and verify no stale completion is present
    provider
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: instance_id.to_string(),
                name: "probe-event".to_string(),
                data: "{}".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // The new instance should have NO completion messages from the old activity
    let has_stale_completion = item.messages.iter().any(|m| {
        matches!(
            m,
            WorkItem::ActivityCompleted { result, .. } if result.contains("stale-result")
        )
    });
    assert!(
        !has_stale_completion,
        "CRITICAL: New instance must NOT see stale completion from deleted instance. Messages: {:?}",
        item.messages
    );

    // Cleanup: ack without changes and delete
    let _ = provider
        .ack_orchestration_item(
            &lock_token,
            INITIAL_EXECUTION_ID,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata {
                status: Some("Completed".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await;
    let _ = mgmt.delete_instance(instance_id, true).await;

    tracing::info!("✓ CRITICAL TEST PASSED: stale activity after delete+recreate");
}

// ===== Helper for creating child instances =====

async fn create_child_instance(provider: &dyn crate::providers::Provider, instance_id: &str, parent_id: &str) {
    use crate::provider_validation::create_instance_with_parent;

    // Create the instance with parent relationship and complete it
    create_instance_with_parent(provider, instance_id, Some(parent_id.to_string()))
        .await
        .unwrap();

    // Complete the instance
    let metadata = ExecutionMetadata {
        status: Some("Completed".to_string()),
        output: Some("done".to_string()),
        orchestration_name: Some("ChildOrch".to_string()),
        orchestration_version: Some("1.0.0".to_string()),
        parent_instance_id: Some(parent_id.to_string()),
        pinned_duroxide_version: None,
    };

    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    provider
        .ack_orchestration_item(
            &lock_token,
            INITIAL_EXECUTION_ID,
            vec![Event::with_event_id(
                1,
                instance_id,
                INITIAL_EXECUTION_ID,
                None,
                EventKind::OrchestrationStarted {
                    name: "ChildOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: Some(parent_id.to_string()),
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
}

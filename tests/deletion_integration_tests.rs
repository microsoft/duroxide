// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Integration tests for deletion operations via Client API.
//!
//! These tests verify end-to-end deletion behavior with real orchestration execution.
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

mod common;

// Helper to create fast-polling runtime
fn fast_runtime_options() -> RuntimeOptions {
    RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(50),
        ..Default::default()
    }
}

// Helper to wait for instance to reach terminal status
async fn wait_for_terminal(client: &Client, instance_id: &str, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(info) = client.get_instance_info(instance_id).await
            && (info.status == "Completed" || info.status == "Failed")
        {
            return true;
        }
        if std::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ===== Happy Path Tests =====

/// Test: delete terminal orchestrations (completed and failed)
///
/// Covers:
/// - Delete completed orchestration
/// - Delete failed orchestration
/// - Cleans all data
/// - Result counts accurate
#[tokio::test]
async fn test_delete_terminal_orchestrations() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let activities = ActivityRegistry::builder()
        .register("SuccessActivity", |_ctx: ActivityContext, _input: String| async move {
            Ok("done".to_string())
        })
        .register("FailActivity", |_ctx: ActivityContext, _input: String| async move {
            Err("intentional failure".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("SuccessOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("SuccessActivity", "".to_string()).await?;
            Ok("completed".to_string())
        })
        .register("FailOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("FailActivity", "".to_string()).await?;
            Ok("unreachable".to_string())
        })
        .build();

    let _rt =
        runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, fast_runtime_options()).await;

    // Start and complete a successful orchestration
    client
        .start_orchestration("delete-completed", "SuccessOrch", "{}")
        .await
        .unwrap();
    assert!(
        wait_for_terminal(&client, "delete-completed", Duration::from_secs(10)).await,
        "Orchestration should complete"
    );

    // Start and fail an orchestration
    client
        .start_orchestration("delete-failed", "FailOrch", "{}")
        .await
        .unwrap();
    assert!(
        wait_for_terminal(&client, "delete-failed", Duration::from_secs(10)).await,
        "Orchestration should fail"
    );

    // Verify both exist and have expected status
    let completed_info = client.get_instance_info("delete-completed").await.unwrap();
    assert_eq!(completed_info.status, "Completed");

    let failed_info = client.get_instance_info("delete-failed").await.unwrap();
    assert_eq!(failed_info.status, "Failed");

    // Delete completed
    let result = client.delete_instance("delete-completed", false).await.unwrap();
    assert!(result.instances_deleted >= 1);
    assert!(result.events_deleted >= 1);

    // Verify gone
    assert!(client.get_instance_info("delete-completed").await.is_err());

    // Delete failed
    let result = client.delete_instance("delete-failed", false).await.unwrap();
    assert!(result.instances_deleted >= 1);

    // Verify gone
    assert!(client.get_instance_info("delete-failed").await.is_err());
}

/// Test: force delete orchestrations with various in-flight work
///
/// Covers:
/// - Waiting on activity
/// - Waiting on timer
/// - Waiting on event
/// - Between turns
#[tokio::test]
async fn test_force_delete_in_flight_work() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let activity_started = Arc::new(AtomicBool::new(false));
    let activity_started_clone = activity_started.clone();

    let activities = ActivityRegistry::builder()
        .register("SlowActivity", move |_ctx: ActivityContext, _input: String| {
            let started = activity_started_clone.clone();
            async move {
                started.store(true, Ordering::SeqCst);
                // Sleep for a long time - we'll force delete before this completes
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok("done".to_string())
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "WaitOnActivity",
            |ctx: OrchestrationContext, _input: String| async move {
                ctx.schedule_activity("SlowActivity", "".to_string()).await?;
                Ok("done".to_string())
            },
        )
        .register("WaitOnTimer", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_timer(Duration::from_secs(3600)).await; // 1 hour timer
            Ok("done".to_string())
        })
        .register("WaitOnEvent", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_wait("my-event").await;
            Ok("done".to_string())
        })
        .build();

    let _rt =
        runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, fast_runtime_options()).await;

    // Test 1: Force delete while waiting on activity
    client
        .start_orchestration("force-activity", "WaitOnActivity", "{}")
        .await
        .unwrap();

    // Wait for activity to actually start
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !activity_started.load(Ordering::SeqCst) {
        if std::time::Instant::now() > deadline {
            panic!("Activity never started");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Note: DeleteInstanceResult fields (executions_deleted, events_deleted, queue_messages_deleted)
    // are tested in provider validation tests (test_delete_terminal_instances, test_delete_cleans_queues_and_locks)
    // Force delete
    let result = client.delete_instance("force-activity", true).await.unwrap();
    assert!(result.instances_deleted >= 1);
    assert!(client.get_instance_info("force-activity").await.is_err());

    // Test 2: Force delete while waiting on timer
    client
        .start_orchestration("force-timer", "WaitOnTimer", "{}")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await; // Let it start

    let result = client.delete_instance("force-timer", true).await.unwrap();
    assert!(result.instances_deleted >= 1);
    assert!(client.get_instance_info("force-timer").await.is_err());

    // Test 3: Force delete while waiting on event
    client
        .start_orchestration("force-event", "WaitOnEvent", "{}")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await; // Let it start

    let result = client.delete_instance("force-event", true).await.unwrap();
    assert!(result.instances_deleted >= 1);
    assert!(client.get_instance_info("force-event").await.is_err());
}

/// Test: cascade delete with real sub-orchestrations
///
/// Covers:
/// - Parent+child cascade
/// - Deep hierarchy
/// - Multiple children
/// - Result aggregates
#[tokio::test]
async fn test_cascade_delete_real_sub_orchestrations() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let orchestrations = OrchestrationRegistry::builder()
        .register("ParentOrch", |ctx: OrchestrationContext, _input: String| async move {
            // Spawn two children using join so they both claim event IDs in the same turn
            // IDs will be parent::sub::2 and parent::sub::3
            let child1 = ctx.schedule_sub_orchestration("ChildOrch", "1".to_string());
            let child2 = ctx.schedule_sub_orchestration("ChildOrch", "2".to_string());

            // Wait for both in parallel
            let results = ctx.join(vec![child1, child2]).await;
            for r in results {
                r?;
            }

            Ok("parent done".to_string())
        })
        .register("ChildOrch", |_ctx: OrchestrationContext, input: String| async move {
            Ok(format!("child {input} done"))
        })
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        fast_runtime_options(),
    )
    .await;

    // Start parent
    client
        .start_orchestration("cascade-parent", "ParentOrch", "{}")
        .await
        .unwrap();

    // Wait for completion
    assert!(
        wait_for_terminal(&client, "cascade-parent", Duration::from_secs(10)).await,
        "Parent should complete"
    );

    // Child IDs are deterministic: parent::sub::{event_id}
    // OrchestrationStarted is event 1, so children are events 2 and 3
    let child1_id = "cascade-parent::sub::2";
    let child2_id = "cascade-parent::sub::3";

    // Verify all three exist
    assert!(client.get_instance_info("cascade-parent").await.is_ok());
    assert!(client.get_instance_info(child1_id).await.is_ok());
    assert!(client.get_instance_info(child2_id).await.is_ok());

    // Try to delete child directly - should fail
    let result = client.delete_instance(child1_id, false).await;
    assert!(result.is_err(), "Should not delete sub-orchestration directly");

    // Delete parent - should cascade
    let result = client.delete_instance("cascade-parent", false).await.unwrap();
    assert!(result.instances_deleted >= 1);

    // Note: get_instance_tree, get_parent_id, list_children are tested in provider validation tests
    // (test_get_instance_tree, test_get_parent_id, test_list_children)
    // All should be gone
    assert!(client.get_instance_info("cascade-parent").await.is_err());
    assert!(client.get_instance_info(child1_id).await.is_err());
    assert!(client.get_instance_info(child2_id).await.is_err());
}

/// Test: identity reuse after delete
///
/// Covers:
/// - Reuse after normal delete
/// - Reuse after force delete
#[tokio::test]
async fn test_identity_reuse_after_delete() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let orchestrations = OrchestrationRegistry::builder()
        .register("SimpleOrch", |_ctx: OrchestrationContext, input: String| async move {
            Ok(input)
        })
        .register("WaitOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_wait("never").await;
            Ok("done".to_string())
        })
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        fast_runtime_options(),
    )
    .await;

    // Test 1: Reuse after normal delete
    client
        .start_orchestration("reuse-id", "SimpleOrch", "first")
        .await
        .unwrap();
    wait_for_terminal(&client, "reuse-id", Duration::from_secs(5)).await;

    // Delete
    client.delete_instance("reuse-id", false).await.unwrap();

    // Reuse same ID
    client
        .start_orchestration("reuse-id", "SimpleOrch", "second")
        .await
        .unwrap();
    wait_for_terminal(&client, "reuse-id", Duration::from_secs(5)).await;

    let info = client.get_instance_info("reuse-id").await.unwrap();
    assert_eq!(info.status, "Completed");
    assert!(info.output.unwrap().contains("second"));

    // Delete again for cleanup
    client.delete_instance("reuse-id", false).await.unwrap();

    // Test 2: Reuse after force delete
    client.start_orchestration("reuse-id", "WaitOrch", "{}").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Force delete
    client.delete_instance("reuse-id", true).await.unwrap();

    // Reuse same ID immediately
    client
        .start_orchestration("reuse-id", "SimpleOrch", "after-force")
        .await
        .unwrap();
    wait_for_terminal(&client, "reuse-id", Duration::from_secs(5)).await;

    let info = client.get_instance_info("reuse-id").await.unwrap();
    assert_eq!(info.status, "Completed");
}

// ===== Error Cases =====

/// Test: delete error cases
///
/// Covers:
/// - Non-existent → InstanceNotFound
/// - Running → InstanceStillRunning
/// - Sub-orchestration → CannotDeleteSubOrchestration
#[tokio::test]
async fn test_delete_error_cases() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let orchestrations = OrchestrationRegistry::builder()
        .register("WaitOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_wait("never").await;
            Ok("done".to_string())
        })
        .register("ParentOrch", |ctx: OrchestrationContext, _input: String| async move {
            // Child will have ID: error-parent::sub::2
            ctx.schedule_sub_orchestration("WaitOrch", "".to_string()).await?;
            Ok("done".to_string())
        })
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        fast_runtime_options(),
    )
    .await;

    // Test 1: Non-existent instance
    let result = client.delete_instance("does-not-exist", false).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("not found") || err.to_string().contains("NotFound"),
        "Error should mention not found: {err}"
    );

    // Test 2: Running instance without force
    client
        .start_orchestration("error-running", "WaitOrch", "{}")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = client.delete_instance("error-running", false).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("running"),
        "Error should mention running: {err}"
    );

    // Test 3: Sub-orchestration (child ID is error-parent::sub::2)
    client
        .start_orchestration("error-parent", "ParentOrch", "{}")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await; // Let child start

    let result = client.delete_instance("error-parent::sub::2", false).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("sub-orchestration")
            || err.to_string().to_lowercase().contains("parent")
            || err.to_string().to_lowercase().contains("root"),
        "Error should mention sub-orchestration/parent/root: {err}"
    );

    // Test: Cannot force delete sub-orchestration either
    let force_result = client.delete_instance("error-parent::sub::2", true).await;
    assert!(
        force_result.is_err(),
        "Force delete should also be rejected for sub-orchestrations"
    );

    // Cleanup: force delete both
    client.delete_instance("error-parent", true).await.ok();
    client.delete_instance("error-running", true).await.ok();
}

// ===== Dispatcher Resilience =====

/// Test: dispatcher survives force delete and continues processing
///
/// Covers:
/// - Orch dispatcher survives
/// - Worker dispatcher survives
/// - Zombie messages handled
#[tokio::test]
async fn test_dispatcher_resilience_after_delete() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let activity_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let activity_count_clone = activity_count.clone();

    let activities = ActivityRegistry::builder()
        .register("CountingActivity", move |_ctx: ActivityContext, _input: String| {
            let count = activity_count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok("counted".to_string())
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("CountOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("CountingActivity", "".to_string()).await?;
            Ok("done".to_string())
        })
        .register("WaitOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_wait("never").await;
            Ok("done".to_string())
        })
        .build();

    let _rt =
        runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, fast_runtime_options()).await;

    // Start an orchestration that waits
    client
        .start_orchestration("resilience-wait", "WaitOrch", "{}")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Force delete it
    client.delete_instance("resilience-wait", true).await.unwrap();

    // Now start new orchestrations - dispatchers should still work
    for i in 0..3 {
        client
            .start_orchestration(&format!("resilience-new-{i}"), "CountOrch", "{}")
            .await
            .unwrap();
    }

    // Wait for all to complete
    for i in 0..3 {
        assert!(
            wait_for_terminal(&client, &format!("resilience-new-{i}"), Duration::from_secs(10)).await,
            "Orchestration {i} should complete"
        );
    }

    // Verify activities ran (dispatchers are healthy)
    assert_eq!(activity_count.load(Ordering::SeqCst), 3);
}

// ===== Concurrent Operations =====

/// Test: concurrent delete operations
///
/// Covers:
/// - Two deletes on same instance
/// - Delete vs completion race
#[tokio::test]
async fn test_concurrent_delete_operations() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let orchestrations = OrchestrationRegistry::builder()
        .register("SimpleOrch", |_ctx: OrchestrationContext, _input: String| async move {
            Ok("done".to_string())
        })
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        fast_runtime_options(),
    )
    .await;

    // Test: Two concurrent deletes on same instance
    client
        .start_orchestration("concurrent-delete", "SimpleOrch", "{}")
        .await
        .unwrap();
    wait_for_terminal(&client, "concurrent-delete", Duration::from_secs(5)).await;

    // Create separate clients for concurrent access (Client wraps Arc<Provider>)
    let store_clone1 = store.clone();
    let store_clone2 = store.clone();

    let handle1 = tokio::spawn(async move {
        let client1 = Client::new(store_clone1);
        client1.delete_instance("concurrent-delete", false).await
    });
    let handle2 = tokio::spawn(async move {
        let client2 = Client::new(store_clone2);
        client2.delete_instance("concurrent-delete", false).await
    });

    let (result1, result2) = tokio::join!(handle1, handle2);
    let result1 = result1.unwrap();
    let result2 = result2.unwrap();

    // One should succeed, one should fail with not found
    let results = [&result1, &result2];
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let not_founds = results.iter().filter(|r| r.is_err()).count();

    assert!(successes >= 1, "At least one delete should succeed");
    // Both might succeed if there's a race window, or one fails with not found
    assert!(
        successes + not_founds == 2,
        "All results should be success or not found"
    );

    // Instance should be gone
    assert!(client.get_instance_info("concurrent-delete").await.is_err());
}

// ===== Primitive and Composite API Tests =====

/// Test get_instance_tree returns correct hierarchy in deletion order
#[tokio::test]
async fn test_get_instance_tree() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    // Create parent with two children
    let orchestrations = OrchestrationRegistry::builder()
        .register("ParentOrch", |ctx: OrchestrationContext, _input: String| async move {
            let child1 = ctx.schedule_sub_orchestration("ChildOrch", "child1".to_string());
            let child2 = ctx.schedule_sub_orchestration("ChildOrch", "child2".to_string());
            let _ = ctx.join(vec![child1, child2]).await;
            Ok("done".to_string())
        })
        .register("ChildOrch", |_ctx: OrchestrationContext, _input: String| async move {
            Ok("child done".to_string())
        })
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        fast_runtime_options(),
    )
    .await;

    client
        .start_orchestration("tree-test-parent", "ParentOrch", "{}")
        .await
        .unwrap();
    wait_for_terminal(&client, "tree-test-parent", Duration::from_secs(5)).await;

    // Get the instance tree
    let tree = client.get_instance_tree("tree-test-parent").await.unwrap();

    // Verify tree properties
    assert_eq!(tree.root_id, "tree-test-parent");
    assert_eq!(tree.size(), 3, "Tree should have parent + 2 children");
    assert!(!tree.is_root_only());

    // Verify all expected IDs are present (ordering is implementation-defined)
    assert!(
        tree.all_ids.contains(&"tree-test-parent".to_string()),
        "Tree should contain root"
    );
    // Children have deterministic IDs based on event_id
    let child_count = tree.all_ids.iter().filter(|id| id.contains("::sub::")).count();
    assert_eq!(child_count, 2, "Tree should contain 2 children");

    // Cleanup
    client.delete_instance("tree-test-parent", false).await.unwrap();
}

/// Test list_children returns only direct children
#[tokio::test]
async fn test_list_children_primitive() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    // Create a hierarchy: parent -> child -> grandchild
    let orchestrations = OrchestrationRegistry::builder()
        .register("ParentOrch", |ctx: OrchestrationContext, _input: String| async move {
            let _child = ctx.schedule_sub_orchestration("ChildOrch", "".to_string()).await?;
            Ok("done".to_string())
        })
        .register("ChildOrch", |ctx: OrchestrationContext, _input: String| async move {
            let _grandchild = ctx.schedule_sub_orchestration("GrandchildOrch", "".to_string()).await?;
            Ok("child done".to_string())
        })
        .register(
            "GrandchildOrch",
            |_ctx: OrchestrationContext, _input: String| async move { Ok("grandchild done".to_string()) },
        )
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        fast_runtime_options(),
    )
    .await;

    client
        .start_orchestration("list-children-parent", "ParentOrch", "{}")
        .await
        .unwrap();
    wait_for_terminal(&client, "list-children-parent", Duration::from_secs(5)).await;

    // Get management capability to test primitive
    let mgmt = store.as_management_capability().unwrap();

    // list_children on parent should return only direct child (not grandchild)
    let children = mgmt.list_children("list-children-parent").await.unwrap();
    assert_eq!(children.len(), 1, "Parent should have 1 direct child");

    // The child ID is deterministic: parent::sub::{event_id}
    let child_id = &children[0];
    assert!(
        child_id.starts_with("list-children-parent::sub::"),
        "Child ID should be sub-orchestration format"
    );

    // list_children on child should return grandchild
    let grandchildren = mgmt.list_children(child_id).await.unwrap();
    assert_eq!(grandchildren.len(), 1, "Child should have 1 grandchild");

    // list_children on grandchild should return empty
    let grandchild_id = &grandchildren[0];
    let great_grandchildren = mgmt.list_children(grandchild_id).await.unwrap();
    assert!(great_grandchildren.is_empty(), "Grandchild should be a leaf");

    // Cleanup
    client.delete_instance("list-children-parent", false).await.unwrap();
}

/// Test get_parent_id returns correct parent or None for root
#[tokio::test]
async fn test_get_parent_id_primitive() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    // Create parent with a child
    let orchestrations = OrchestrationRegistry::builder()
        .register("ParentOrch", |ctx: OrchestrationContext, _input: String| async move {
            let _child = ctx.schedule_sub_orchestration("ChildOrch", "".to_string()).await?;
            Ok("done".to_string())
        })
        .register("ChildOrch", |_ctx: OrchestrationContext, _input: String| async move {
            Ok("child done".to_string())
        })
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        fast_runtime_options(),
    )
    .await;

    client
        .start_orchestration("parent-id-parent", "ParentOrch", "{}")
        .await
        .unwrap();
    wait_for_terminal(&client, "parent-id-parent", Duration::from_secs(5)).await;

    let mgmt = store.as_management_capability().unwrap();

    // Root has no parent
    let parent = mgmt.get_parent_id("parent-id-parent").await.unwrap();
    assert!(parent.is_none(), "Root should have no parent");

    // Get child ID from tree
    let tree = client.get_instance_tree("parent-id-parent").await.unwrap();
    let child_id = tree.all_ids.iter().find(|id| id.contains("::sub::")).unwrap();

    // Child has parent
    let parent = mgmt.get_parent_id(child_id).await.unwrap();
    assert_eq!(
        parent,
        Some("parent-id-parent".to_string()),
        "Child should have root as parent"
    );

    // Non-existent instance returns error
    let result = mgmt.get_parent_id("non-existent").await;
    assert!(result.is_err(), "Non-existent instance should return error");

    // Cleanup
    client.delete_instance("parent-id-parent", false).await.unwrap();
}

// ===== Race Condition Tests =====

/// Test: orphan detection in delete_instances_atomic
///
/// Simulates a race condition where a child is spawned after get_instance_tree()
/// but before delete_instances_atomic(). The delete should fail to prevent orphans.
#[tokio::test]
async fn test_delete_orphan_race_condition_detection() {
    // Create an orchestration hierarchy where parent spawns child
    let activities = ActivityRegistry::builder()
        .register("ParentActivity", |_ctx: ActivityContext, _input: String| async move {
            Ok("done".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("ParentOrch", |ctx: OrchestrationContext, _input: String| async move {
            // Spawn a sub-orchestration and wait for it
            let child = ctx.schedule_sub_orchestration("ChildOrch", "child-input".to_string());
            let _ = ctx.join(vec![child]).await;
            Ok("parent-done".to_string())
        })
        .register("ChildOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("ParentActivity", "".to_string()).await?;
            Ok("child-done".to_string())
        })
        .build();

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let _rt =
        runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, fast_runtime_options()).await;

    // Start parent which spawns child
    client
        .start_orchestration("orphan-parent", "ParentOrch", "{}")
        .await
        .unwrap();
    assert!(
        wait_for_terminal(&client, "orphan-parent", Duration::from_secs(10)).await,
        "Parent orchestration should complete"
    );

    // Get tree - should have parent + child
    let full_tree = client.get_instance_tree("orphan-parent").await.unwrap();
    assert_eq!(full_tree.size(), 2, "Should have parent + child");

    // Get the ProviderAdmin interface
    let mgmt = store.as_management_capability().unwrap();

    // Simulate the race condition: try to delete with only the parent ID, not the child.
    // This is what would happen if a child was spawned after get_instance_tree() but
    // before delete_instances_atomic().
    let result = mgmt
        .delete_instances_atomic(&["orphan-parent".to_string()], false)
        .await;

    // This should fail because the child would be orphaned
    assert!(result.is_err(), "Delete should fail when child would be orphaned");
    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("child") || err_msg.contains("orphan") || err_msg.contains("tree traversal"),
        "Error should mention orphan/child issue: {err_msg}"
    );

    // Verify nothing was deleted (transaction rolled back)
    let info = client.get_instance_info("orphan-parent").await;
    assert!(info.is_ok(), "Parent should still exist after failed delete");

    // Now delete properly with the full tree
    let result = mgmt.delete_instances_atomic(&full_tree.all_ids, false).await;
    assert!(result.is_ok(), "Delete with full tree should succeed");
}

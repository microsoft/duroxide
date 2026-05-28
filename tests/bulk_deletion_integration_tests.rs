// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Integration tests for bulk delete instances operations via Client API.
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::providers::InstanceFilter;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{Client, OrchestrationContext, OrchestrationRegistry};
use std::time::Duration;

mod common;

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

/// Test: bulk deletion retention policies
///
/// Covers:
/// - Delete specific IDs
/// - Empty filter deletes all terminal
/// - Iterative with limit (batching)
#[tokio::test]
async fn test_delete_instance_bulk_retention_policies() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let orchestrations = OrchestrationRegistry::builder()
        .register("SimpleOrch", |_ctx: OrchestrationContext, input: String| async move {
            Ok(input)
        })
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        RuntimeOptions::default(),
    )
    .await;

    // Create 6 completed instances
    for i in 0..6 {
        client
            .start_orchestration(&format!("bulk-del-test-{i}"), "SimpleOrch", &format!("{i}"))
            .await
            .unwrap();
    }

    // Wait for all to complete
    for i in 0..6 {
        assert!(
            wait_for_terminal(&client, &format!("bulk-del-test-{i}"), Duration::from_secs(5)).await,
            "Instance {i} should complete"
        );
    }

    // Test 1: Delete specific IDs
    let result = client
        .delete_instance_bulk(InstanceFilter {
            instance_ids: Some(vec!["bulk-del-test-0".into(), "bulk-del-test-1".into()]),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(result.instances_deleted, 2);
    // Deleted instances should be gone
    assert!(client.get_instance_info("bulk-del-test-0").await.is_err());
    assert!(client.get_instance_info("bulk-del-test-1").await.is_err());
    // All remaining instances (2-5) should still exist
    for i in 2..6 {
        assert!(
            client.get_instance_info(&format!("bulk-del-test-{i}")).await.is_ok(),
            "Instance {i} should still exist"
        );
    }

    // Test 2: Iterative with limit (4 remaining, delete 2 at a time)
    let result1 = client
        .delete_instance_bulk(InstanceFilter {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result1.instances_deleted, 2);

    let result2 = client
        .delete_instance_bulk(InstanceFilter {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result2.instances_deleted, 2);

    let result3 = client
        .delete_instance_bulk(InstanceFilter {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result3.instances_deleted, 0, "All should be deleted now");

    // Verify all are gone
    for i in 0..6 {
        assert!(
            client.get_instance_info(&format!("bulk-del-test-{i}")).await.is_err(),
            "Instance {i} should be deleted"
        );
    }
}

/// Test: bulk deletion safety
///
/// Covers:
/// - Skips running orchestrations
/// - Cascades to sub-orchestrations
#[tokio::test]
async fn test_delete_instance_bulk_safety() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let orchestrations = OrchestrationRegistry::builder()
        .register("SimpleOrch", |_ctx: OrchestrationContext, _input: String| async move {
            Ok("done".to_string())
        })
        .register("WaitOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_wait("never").await;
            Ok("done".to_string())
        })
        .register("ParentOrch", |ctx: OrchestrationContext, _input: String| async move {
            // Child ID will be bulk-del-parent::sub::2
            ctx.schedule_sub_orchestration("SimpleOrch", "".to_string()).await?;
            Ok("parent done".to_string())
        })
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        orchestrations,
        RuntimeOptions::default(),
    )
    .await;

    // Test 1: Bulk delete skips running instances
    client
        .start_orchestration("bulk-del-completed", "SimpleOrch", "{}")
        .await
        .unwrap();
    client
        .start_orchestration("bulk-del-running", "WaitOrch", "{}")
        .await
        .unwrap();

    wait_for_terminal(&client, "bulk-del-completed", Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(200)).await; // Let running start

    let result = client
        .delete_instance_bulk(InstanceFilter {
            instance_ids: Some(vec!["bulk-del-completed".into(), "bulk-del-running".into()]),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(result.instances_deleted, 1, "Only completed should be deleted");
    assert!(client.get_instance_info("bulk-del-completed").await.is_err());
    assert!(
        client.get_instance_info("bulk-del-running").await.is_ok(),
        "Running should not be deleted"
    );

    // Cleanup running
    client.delete_instance("bulk-del-running", true).await.unwrap();

    // Test 2: Bulk delete cascades to sub-orchestrations
    client
        .start_orchestration("bulk-del-parent", "ParentOrch", "{}")
        .await
        .unwrap();
    wait_for_terminal(&client, "bulk-del-parent", Duration::from_secs(5)).await;

    // Child ID is deterministic: bulk-del-parent::sub::2
    let child_id = "bulk-del-parent::sub::2";

    // Both parent and child should exist
    assert!(client.get_instance_info("bulk-del-parent").await.is_ok());
    assert!(client.get_instance_info(child_id).await.is_ok());

    // Delete parent via bulk API
    let result = client
        .delete_instance_bulk(InstanceFilter {
            instance_ids: Some(vec!["bulk-del-parent".into()]),
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(result.instances_deleted >= 1);

    // Both should be gone (cascade)
    assert!(client.get_instance_info("bulk-del-parent").await.is_err());
    assert!(client.get_instance_info(child_id).await.is_err());
}

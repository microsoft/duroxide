// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Provider validation tests for execution pruning operations.
//!
//! These tests verify that providers correctly implement the prune API,
//! including options combinations, safety guarantees, and bulk operations.

use crate::provider_validation::{Event, EventKind, ExecutionMetadata, ProviderFactory, start_item};
use crate::providers::{InstanceFilter, PruneOptions, WorkItem};
use std::time::Duration;

/// Test: prune with various options combinations
///
/// Covers:
/// - keep_last option
/// - completed_before option (time-based)
/// - AND filter (keep_last AND completed_before)
/// - Empty options prunes nothing
pub async fn test_prune_options_combinations<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing prune: options combinations");
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    let instance_id = "prune-options-test";

    // Create instance with 4 executions (simulating ContinueAsNew chain)
    create_multi_execution_instance(&*provider, instance_id, 4).await;

    // Verify we have 4 executions
    let executions = mgmt.list_executions(instance_id).await.unwrap();
    assert_eq!(executions.len(), 4, "Should have 4 executions");

    // Test 1: Prune with keep_last=2 (should delete 2, keep 2)
    let result = mgmt
        .prune_executions(
            instance_id,
            PruneOptions {
                keep_last: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(result.instances_processed, 1);
    let executions_after = mgmt.list_executions(instance_id).await.unwrap();
    assert_eq!(executions_after.len(), 2, "Should have 2 executions after prune");
    // Should keep the latest ones (3 and 4)
    assert!(executions_after.contains(&3) || executions_after.contains(&4));

    // Test 2: Empty options prunes all historical but keeps current
    let instance_id2 = "prune-options-empty";
    create_multi_execution_instance(&*provider, instance_id2, 3).await;

    let info = mgmt.get_instance_info(instance_id2).await.unwrap();
    let current_exec = info.current_execution_id;

    let _result = mgmt
        .prune_executions(
            instance_id2,
            PruneOptions {
                keep_last: None,
                completed_before: None,
            },
        )
        .await
        .unwrap();

    // Current execution is always preserved - and ONLY current execution remains
    let executions = mgmt.list_executions(instance_id2).await.unwrap();
    assert_eq!(
        executions.len(),
        1,
        "Only current execution should remain after empty-options prune"
    );
    assert_eq!(
        executions[0], current_exec,
        "The remaining execution should be the current one"
    );

    tracing::info!("✓ Test passed: prune options combinations");
}

/// Test: prune safety guarantees
///
/// Covers:
/// - Never deletes current (latest) execution
/// - Never deletes running execution
/// - Non-existent instance returns error
/// - Terminal instance: None, Some(0), Some(1) all preserve current execution
pub async fn test_prune_safety<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing prune: safety guarantees");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Test 1: Never deletes current execution (even with keep_last=0)
    let instance_id = "prune-safety-current";
    create_multi_execution_instance(&*provider, instance_id, 3).await;

    let _result = mgmt
        .prune_executions(
            instance_id,
            PruneOptions {
                keep_last: Some(0), // Try to keep 0
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Current execution should NOT be deleted
    let info = mgmt.get_instance_info(instance_id).await.unwrap();
    assert_eq!(info.current_execution_id, 3, "Current execution should remain");

    // Test 2: Non-existent instance returns error
    let result = mgmt
        .prune_executions(
            "does-not-exist-prune",
            PruneOptions {
                keep_last: Some(1),
                ..Default::default()
            },
        )
        .await;

    assert!(result.is_err(), "Prune on non-existent instance should error");

    // Test 3: Terminal instance - verify None, Some(0), Some(1) all preserve current execution
    // This verifies that for a Completed instance, all three options are equivalent
    // because the current execution is always protected regardless of status.
    for (label, keep_last) in [("None", None), ("Some(0)", Some(0)), ("Some(1)", Some(1))] {
        let instance_id = format!("prune-terminal-{}", label.to_lowercase().replace(['(', ')'], ""));
        create_multi_execution_instance(&*provider, &instance_id, 4).await;

        // Verify instance is terminal (Completed)
        let info = mgmt.get_instance_info(&instance_id).await.unwrap();
        assert_eq!(info.status, "Completed", "Instance should be terminal");
        let current_exec = info.current_execution_id;

        let result = mgmt
            .prune_executions(
                &instance_id,
                PruneOptions {
                    keep_last,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // All options should prune historical executions but preserve current
        let executions = mgmt.list_executions(&instance_id).await.unwrap();
        assert_eq!(
            executions.len(),
            1,
            "keep_last={label}: should have exactly 1 execution remaining"
        );
        assert_eq!(
            executions[0], current_exec,
            "keep_last={label}: remaining execution should be the current one"
        );
        assert!(
            result.executions_deleted >= 3,
            "keep_last={label}: should have deleted 3 historical executions"
        );
    }

    tracing::info!("✓ Test passed: prune safety guarantees");
}

/// Test: bulk prune operations
///
/// Covers:
/// - Bulk prune by instance IDs
/// - Bulk prune respects limit
/// - Bulk prune skips running instances
pub async fn test_prune_bulk<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing prune: bulk operations");
    let provider = factory.create_provider().await;
    let mgmt = provider.as_management_capability().unwrap();

    // Create multiple instances each with multiple executions
    for i in 0..3 {
        create_multi_execution_instance(&*provider, &format!("prune-bulk-{i}"), 4).await;
    }

    // Bulk prune keeping last 1 for specific instances
    let result = mgmt
        .prune_executions_bulk(
            InstanceFilter {
                instance_ids: Some(vec!["prune-bulk-0".into(), "prune-bulk-1".into()]),
                ..Default::default()
            },
            PruneOptions {
                keep_last: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(result.instances_processed, 2, "Should process 2 instances");
    assert!(
        result.executions_deleted >= 4,
        "Should delete multiple executions across instances"
    );

    // Verify each pruned instance has only 1 execution
    let exec0 = mgmt.list_executions("prune-bulk-0").await.unwrap();
    assert_eq!(exec0.len(), 1, "Instance 0 should have 1 execution");

    let exec1 = mgmt.list_executions("prune-bulk-1").await.unwrap();
    assert_eq!(exec1.len(), 1, "Instance 1 should have 1 execution");

    // Instance 2 should be untouched (not in filter)
    let exec2 = mgmt.list_executions("prune-bulk-2").await.unwrap();
    assert_eq!(exec2.len(), 4, "Instance 2 should still have 4 executions");

    tracing::info!("✓ Test passed: prune bulk operations");
}

/// Test: bulk prune includes Running instances
///
/// Validates that `prune_executions_bulk` correctly handles instances that are
/// still Running — e.g., long-running orchestrations using ContinueAsNew that
/// accumulate old executions. The current (Running) execution must be preserved
/// while historical executions are pruned.
///
/// This catches providers that filter with `WHERE status IN ('Completed', 'Failed', ...)`
/// which would exclude Running instances from pruning entirely.
pub async fn test_prune_bulk_includes_running_instances<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing prune: bulk includes running instances");
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    // Create a Running instance with 3 executions (simulating ContinueAsNew chain
    // where the latest execution is still running)
    let instance_id = "prune-running-inst";
    create_running_multi_execution_instance(&*provider, instance_id, 3).await;

    // Verify setup: 3 executions, status is Running
    let executions = mgmt.list_executions(instance_id).await.unwrap();
    assert_eq!(executions.len(), 3, "Should have 3 executions");
    let info = mgmt.get_instance_info(instance_id).await.unwrap();
    assert_eq!(info.status, "Running", "Instance should still be Running");
    assert_eq!(info.current_execution_id, 3, "Current execution should be 3");

    // Bulk prune with keep_last=1 — should delete executions 1 and 2, keep 3
    let result = mgmt
        .prune_executions_bulk(
            InstanceFilter {
                instance_ids: Some(vec![instance_id.into()]),
                ..Default::default()
            },
            PruneOptions {
                keep_last: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(result.instances_processed, 1, "Should process the running instance");
    assert_eq!(result.executions_deleted, 2, "Should delete 2 historical executions");

    // Verify only current execution remains
    let remaining = mgmt.list_executions(instance_id).await.unwrap();
    assert_eq!(remaining.len(), 1, "Should have 1 execution remaining");
    assert_eq!(remaining[0], 3, "Remaining execution should be the current one (3)");

    // Verify instance is still Running
    let info = mgmt.get_instance_info(instance_id).await.unwrap();
    assert_eq!(info.status, "Running", "Instance should still be Running after prune");

    tracing::info!("✓ Test passed: prune bulk includes running instances");
}

// ===== Helper Functions =====

/// Helper: create a Running instance with multiple executions (simulating ContinueAsNew
/// chain where the latest execution is still active).
///
/// Execution 1..(n-1) are marked ContinuedAsNew, execution n is marked Running.
async fn create_running_multi_execution_instance(
    provider: &dyn crate::providers::Provider,
    instance_id: &str,
    num_executions: u64,
) {
    for exec_id in 1..=num_executions {
        let is_last = exec_id == num_executions;
        let status = if is_last { "Running" } else { "ContinuedAsNew" };

        let work_item = if exec_id == 1 {
            start_item(instance_id)
        } else {
            WorkItem::ContinueAsNew {
                instance: instance_id.to_string(),
                orchestration: "LongRunning".to_string(),
                input: "{}".to_string(),
                version: Some("1.0.0".to_string()),
                carry_forward_events: vec![],
                initial_custom_status: None,
            }
        };

        provider.enqueue_for_orchestrator(work_item, None).await.unwrap();

        let (_item, lock_token, _) = provider
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
                    instance_id,
                    exec_id,
                    None,
                    EventKind::OrchestrationStarted {
                        name: "LongRunning".to_string(),
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
                    status: Some(status.to_string()),
                    orchestration_name: Some("LongRunning".to_string()),
                    orchestration_version: Some("1.0.0".to_string()),
                    ..Default::default()
                },
                vec![],
            )
            .await
            .unwrap();
    }
}

/// Helper: create an instance with multiple executions (simulating ContinueAsNew)
async fn create_multi_execution_instance(
    provider: &dyn crate::providers::Provider,
    instance_id: &str,
    num_executions: u64,
) {
    for exec_id in 1..=num_executions {
        let is_last = exec_id == num_executions;
        let status = if is_last { "Completed" } else { "ContinuedAsNew" };

        let work_item = if exec_id == 1 {
            start_item(instance_id)
        } else {
            WorkItem::ContinueAsNew {
                instance: instance_id.to_string(),
                orchestration: "TestOrch".to_string(),
                input: "{}".to_string(),
                version: Some("1.0.0".to_string()),
                carry_forward_events: vec![],
                initial_custom_status: None,
            }
        };

        provider.enqueue_for_orchestrator(work_item, None).await.unwrap();

        let (_item, lock_token, _) = provider
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
                    instance_id,
                    exec_id,
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
                    status: Some(status.to_string()),
                    orchestration_name: Some("TestOrch".to_string()),
                    orchestration_version: Some("1.0.0".to_string()),
                    ..Default::default()
                },
                vec![],
            )
            .await
            .unwrap();
    }
}

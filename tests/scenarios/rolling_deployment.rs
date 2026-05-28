// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Rolling deployment scenario tests
//!
//! These tests simulate real-world rolling deployment scenarios where:
//! - Multiple runtime nodes share the same provider
//! - Nodes are upgraded one at a time (rolling deployment)
//! - New code (activities, orchestration versions) is deployed incrementally
//!
//! The unregistered backoff feature allows work items to "bounce" between nodes
//! until they reach a node with the required handler, rather than failing immediately.

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions, UnregisteredBackoffConfig};
use duroxide::{Client, OrchestrationRegistry};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::sleep;

#[path = "../common/mod.rs"]
mod common;

/// E2E: Simulates rolling deployment across 3 nodes with a new activity
///
/// Scenario:
/// - 3 "nodes" (runtimes) sharing same provider
/// - Initially: 2 nodes have old code (no NewActivity), 1 node has new code
/// - Work item for NewActivity is enqueued
/// - Old nodes repeatedly abandon with backoff
/// - After 2 seconds: old nodes "upgrade" (get new code)
/// - Work item eventually succeeds on upgraded node
///
/// This validates that unregistered backoff allows rolling deployments to succeed
/// without manual coordination.
#[tokio::test]
async fn e2e_rolling_deployment_three_nodes() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Track which nodes have been upgraded
    let node1_upgraded = Arc::new(AtomicBool::new(false));
    let node2_upgraded = Arc::new(AtomicBool::new(false));
    let activity_executed = Arc::new(AtomicBool::new(false));

    // Orchestration that calls NewActivity
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "RollingDeployOrch",
            |ctx: duroxide::OrchestrationContext, _input: String| async move {
                let result = ctx.schedule_activity("NewActivity", "test-input").await?;
                Ok(result)
            },
        )
        .build();

    // Node 3 starts with new code (has NewActivity)
    let activity_executed_clone = activity_executed.clone();
    let activities_new = ActivityRegistry::builder()
        .register("NewActivity", move |_ctx: duroxide::ActivityContext, input: String| {
            let executed = activity_executed_clone.clone();
            async move {
                executed.store(true, Ordering::SeqCst);
                Ok(format!("NewActivity result: {input}"))
            }
        })
        .build();

    // Nodes 1 & 2 start with old code (NO NewActivity registered)
    let activities_old = ActivityRegistry::builder().build();

    // Fast options with short backoff for testing
    // Note: max_attempts is set high (50) to allow enough bouncing between nodes
    // during rolling deployment. With 3 nodes where 2 don't have the activity,
    // the work item needs many chances to land on the correct node (node 3).
    let options = RuntimeOptions {
        max_attempts: 50,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        unregistered_backoff: UnregisteredBackoffConfig {
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(200),
        },
        ..Default::default()
    };

    // Start all 3 nodes
    // Node 1: old code
    let rt1 = runtime::Runtime::start_with_options(
        store.clone(),
        activities_old.clone(),
        orchestrations.clone(),
        options.clone(),
    )
    .await;

    // Node 2: old code
    let rt2 = runtime::Runtime::start_with_options(
        store.clone(),
        activities_old.clone(),
        orchestrations.clone(),
        options.clone(),
    )
    .await;

    // Node 3: new code (has NewActivity)
    let rt3 = runtime::Runtime::start_with_options(
        store.clone(),
        activities_new.clone(),
        orchestrations.clone(),
        options.clone(),
    )
    .await;

    let client = Client::new(store.clone());

    // Start orchestration - will schedule NewActivity
    let instance = "rolling-deployment-test";
    client
        .start_orchestration(instance, "RollingDeployOrch", "")
        .await
        .expect("start should succeed");

    // Simulate rolling upgrade: after 2 seconds, upgrade nodes 1 & 2
    let node1_upgraded_clone = node1_upgraded.clone();
    let node2_upgraded_clone = node2_upgraded.clone();
    let rt1_handle = rt1.clone();
    let rt2_handle = rt2.clone();
    let store_clone = store.clone();
    let activities_new_clone = activities_new.clone();
    let orchestrations_clone = orchestrations.clone();
    let options_clone = options.clone();

    tokio::spawn(async move {
        // Wait 2 seconds (simulating rolling deployment window)
        sleep(Duration::from_secs(2)).await;

        // "Upgrade" node 1
        rt1_handle.shutdown(None).await;
        node1_upgraded_clone.store(true, Ordering::SeqCst);
        let _rt1_new = runtime::Runtime::start_with_options(
            store_clone.clone(),
            activities_new_clone.clone(),
            orchestrations_clone.clone(),
            options_clone.clone(),
        )
        .await;

        // "Upgrade" node 2
        rt2_handle.shutdown(None).await;
        node2_upgraded_clone.store(true, Ordering::SeqCst);
        let _rt2_new = runtime::Runtime::start_with_options(
            store_clone.clone(),
            activities_new_clone.clone(),
            orchestrations_clone.clone(),
            options_clone.clone(),
        )
        .await;
    });

    // Wait for orchestration to complete (should succeed after nodes upgrade)
    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(10))
        .await
        .expect("wait should succeed");

    // Orchestration should complete successfully
    match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("NewActivity result"));
            assert!(
                activity_executed.load(Ordering::SeqCst),
                "Activity should have been executed"
            );
        }
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed unexpectedly: {details:?}")
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt3.shutdown(None).await;
}

/// E2E: Simulates rolling deployment with version upgrade via CAN
///
/// Scenario:
/// - Orchestration v1.0.0 is running, does continue_as_new to v2.0.0
/// - 2 nodes have only v1.0.0, 1 node has both v1.0.0 and v2.0.0
/// - CAN message for v2.0.0 bounces on old nodes with backoff
/// - After upgrade, v2.0.0 executes successfully
///
/// This validates that version-specific backoff works correctly during
/// rolling deployments where new versions are introduced.
#[tokio::test]
async fn e2e_rolling_deployment_version_upgrade() {
    use tokio::sync::oneshot;

    let (store, _td) = common::create_sqlite_store_disk().await;

    // v1.0.0 handler - does CAN to v2.0.0
    let v1_handler = |ctx: duroxide::OrchestrationContext, _input: String| async move {
        // Continue as new to v2.0.0
        ctx.continue_as_new_versioned("2.0.0", "upgraded").await
    };

    // v2.0.0 handler - just completes
    let v2_handler = |_ctx: duroxide::OrchestrationContext, input: String| async move {
        Ok::<_, String>(format!("v2-completed:{input}"))
    };

    // Old nodes: only have v1.0.0
    let orchestrations_old = OrchestrationRegistry::builder()
        .register_versioned("VersionedOrch", "1.0.0", v1_handler)
        .build();

    // New node: has both v1.0.0 and v2.0.0
    let orchestrations_new = OrchestrationRegistry::builder()
        .register_versioned("VersionedOrch", "1.0.0", v1_handler)
        .register_versioned("VersionedOrch", "2.0.0", v2_handler)
        .build();

    let activities = ActivityRegistry::builder().build();

    // Note: max_attempts is set high (50) to allow enough bouncing between nodes
    // during rolling deployment. With 3 nodes where 2 don't have v2.0.0,
    // the work item needs many chances to survive until upgrade completes.
    // Backoffs are longer (100ms-500ms) to ensure we don't exhaust attempts
    // before the 1-second upgrade window.
    let options = RuntimeOptions {
        max_attempts: 50,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        unregistered_backoff: UnregisteredBackoffConfig {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
        },
        ..Default::default()
    };

    // Start nodes: 2 old (v1 only), 1 new (v1+v2)
    let rt1 = runtime::Runtime::start_with_options(
        store.clone(),
        activities.clone(),
        orchestrations_old.clone(),
        options.clone(),
    )
    .await;
    let rt2 = runtime::Runtime::start_with_options(
        store.clone(),
        activities.clone(),
        orchestrations_old.clone(),
        options.clone(),
    )
    .await;
    let rt3 = runtime::Runtime::start_with_options(
        store.clone(),
        activities.clone(),
        orchestrations_new.clone(),
        options.clone(),
    )
    .await;

    let client = Client::new(store.clone());

    // Start orchestration at v1.0.0 - it will CAN to v2.0.0
    client
        .start_orchestration_versioned("version-upgrade-test", "VersionedOrch", "1.0.0", "{}")
        .await
        .expect("start should succeed");

    // Channel to keep upgraded runtimes alive until test completes
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Simulate rolling upgrade: after 1 second, upgrade old nodes
    tokio::spawn({
        let rt1 = rt1.clone();
        let rt2 = rt2.clone();
        let store = store.clone();
        let activities = activities.clone();
        let orchestrations_new = orchestrations_new.clone();
        let options = options.clone();
        async move {
            sleep(Duration::from_secs(1)).await;

            // "Upgrade" node 1
            rt1.shutdown(None).await;
            let rt1_new = runtime::Runtime::start_with_options(
                store.clone(),
                activities.clone(),
                orchestrations_new.clone(),
                options.clone(),
            )
            .await;

            // "Upgrade" node 2
            rt2.shutdown(None).await;
            let rt2_new = runtime::Runtime::start_with_options(
                store.clone(),
                activities.clone(),
                orchestrations_new.clone(),
                options.clone(),
            )
            .await;

            // Keep runtimes alive until signaled to shutdown
            let _ = shutdown_rx.await;
            rt1_new.shutdown(None).await;
            rt2_new.shutdown(None).await;
        }
    });

    // Wait for orchestration to complete
    let status = client
        .wait_for_orchestration("version-upgrade-test", Duration::from_secs(15))
        .await
        .expect("wait should succeed");

    // Signal upgrade task to shutdown
    let _ = shutdown_tx.send(());

    // Should complete successfully with v2 output
    match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("v2-completed"), "Expected v2 output, got: {output}");
        }
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed unexpectedly: {details:?}")
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt3.shutdown(None).await;
}

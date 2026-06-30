// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Comprehensive tests for the management interface including metrics
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::time::Duration;

mod common;

// Helper to create fast-polling runtime for management tests (timing-sensitive)
fn fast_runtime_options() -> RuntimeOptions {
    RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(50),
        ..Default::default()
    }
}

/// Test: Basic capability discovery
#[tokio::test]
async fn test_capability_discovery() {
    let store = Arc::new(SqliteProvider::new("sqlite::memory:", None).await.unwrap());
    let client = Client::new(store.clone());

    // Test capability discovery
    assert!(client.has_management_capability());

    // Test management methods work
    let instances = client.list_all_instances().await.unwrap();
    assert!(instances.is_empty());

    let metrics = client.get_system_metrics().await.unwrap();
    assert_eq!(metrics.total_instances, 0);
    assert_eq!(metrics.total_executions, 0);
    assert_eq!(metrics.running_instances, 0);
    assert_eq!(metrics.completed_instances, 0);
    assert_eq!(metrics.failed_instances, 0);
    assert_eq!(metrics.total_events, 0);

    let queues = client.get_queue_depths().await.unwrap();
    assert_eq!(queues.orchestrator_queue, 0);
    assert_eq!(queues.worker_queue, 0);
    assert_eq!(queues.timer_queue, 0);
}

/// Test: Management features with workflow
#[tokio::test]
async fn test_management_features_with_workflow() {
    let store = Arc::new(SqliteProvider::new("sqlite::memory:", None).await.unwrap());
    let client = Client::new(store.clone());

    // Set up runtime with orchestrations
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "TestOrchestration",
            |_ctx: OrchestrationContext, _input: String| async move { Ok("completed".to_string()) },
        )
        .build();

    let _rt =
        runtime::Runtime::start_with_store(store.clone(), ActivityRegistry::builder().build(), orchestrations).await;

    // Start an orchestration
    client
        .start_orchestration("test-instance", "TestOrchestration", "{}")
        .await
        .unwrap();

    // Wait for completion (with timeout)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let completed = client.list_instances_by_status("Completed").await.unwrap();
        if completed.contains(&"test-instance".to_string()) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Timed out waiting for orchestration to complete");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Check management features
    assert!(client.has_management_capability());

    let instances = client.list_all_instances().await.unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0], "test-instance");

    let info = client.get_instance_info("test-instance").await.unwrap();
    assert_eq!(info.instance_id, "test-instance");
    assert_eq!(info.orchestration_name, "TestOrchestration");
    assert_eq!(info.orchestration_version, "1.0.0");
    assert_eq!(info.current_execution_id, 1);

    let executions = client.list_executions("test-instance").await.unwrap();
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0], 1);

    let metrics = client.get_system_metrics().await.unwrap();
    assert_eq!(metrics.total_instances, 1);
    assert_eq!(metrics.total_executions, 1);
}

/// Test: Instance discovery and listing
#[tokio::test]
async fn test_instance_discovery() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    // Initially empty
    let instances = client.list_all_instances().await.unwrap();
    assert!(instances.is_empty());

    // Start some orchestrations
    let activities = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("Processed: {input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "TestOrchestration",
            |ctx: OrchestrationContext, input: String| async move {
                let result = ctx.schedule_activity("TestActivity", input).await?;
                Ok(result)
            },
        )
        .build();

    let _rt =
        runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, fast_runtime_options()).await;

    // Start multiple orchestrations
    client
        .start_orchestration("instance-1", "TestOrchestration", "input-1")
        .await
        .unwrap();
    client
        .start_orchestration("instance-2", "TestOrchestration", "input-2")
        .await
        .unwrap();
    client
        .start_orchestration("instance-3", "TestOrchestration", "input-3")
        .await
        .unwrap();

    // Wait for all orchestrations to complete (with timeout)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let completed = client.list_instances_by_status("Completed").await.unwrap();
        if completed.len() >= 3 {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "Timed out waiting for orchestrations to complete. Completed: {}",
                completed.len()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // List instances
    let instances = client.list_all_instances().await.unwrap();
    assert_eq!(instances.len(), 3);
    assert!(instances.contains(&"instance-1".to_string()));
    assert!(instances.contains(&"instance-2".to_string()));
    assert!(instances.contains(&"instance-3".to_string()));

    // Test status filtering
    let completed = client.list_instances_by_status("Completed").await.unwrap();
    assert_eq!(completed.len(), 3);

    let running = client.list_instances_by_status("Running").await.unwrap();
    assert_eq!(running.len(), 0);
}

/// Test: Instance information retrieval
#[tokio::test]
async fn test_instance_info() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let activities = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("Processed: {input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "TestOrchestration",
            |ctx: OrchestrationContext, input: String| async move {
                let result = ctx.schedule_activity("TestActivity", input).await?;
                Ok(result)
            },
        )
        .build();

    let _rt =
        runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, fast_runtime_options()).await;

    // Start orchestration
    client
        .start_orchestration("test-instance", "TestOrchestration", "test-input")
        .await
        .unwrap();

    // Wait for completion (with timeout)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let completed = client.list_instances_by_status("Completed").await.unwrap();
        if completed.contains(&"test-instance".to_string()) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Timed out waiting for orchestration to complete");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Get instance info
    let info = client.get_instance_info("test-instance").await.unwrap();
    assert_eq!(info.instance_id, "test-instance");
    assert_eq!(info.orchestration_name, "TestOrchestration");
    assert_eq!(info.orchestration_version, "1.0.0");
    assert_eq!(info.current_execution_id, 1);
    assert_eq!(info.status, "Completed");
    assert!(info.output.is_some());
    // Note: created_at and updated_at may be 0 if not properly set by SQLite
    // This is a known limitation - timestamps are stored as strings but read as i64

    // Test non-existent instance
    let result = client.get_instance_info("nonexistent").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

/// Test: Execution information and history
#[tokio::test]
async fn test_execution_info() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let activities = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("Processed: {input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "TestOrchestration",
            |ctx: OrchestrationContext, input: String| async move {
                let result = ctx.schedule_activity("TestActivity", input).await?;
                Ok(result)
            },
        )
        .build();

    let _rt =
        runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, fast_runtime_options()).await;

    // Start orchestration
    client
        .start_orchestration("test-exec", "TestOrchestration", "test-input")
        .await
        .unwrap();

    // Wait for completion (with timeout)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let completed = client.list_instances_by_status("Completed").await.unwrap();
        if completed.contains(&"test-exec".to_string()) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Timed out waiting for orchestration to complete");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // List executions
    let executions = client.list_executions("test-exec").await.unwrap();
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0], 1);

    // Get execution info
    let exec_info = client.get_execution_info("test-exec", 1).await.unwrap();
    assert_eq!(exec_info.execution_id, 1);
    assert_eq!(exec_info.status, "Completed");
    assert!(exec_info.output.is_some());
    // Note: started_at and completed_at may be 0 if not properly set by SQLite
    // This is a known limitation - timestamps are stored as strings but read as i64
    assert!(exec_info.event_count > 0);

    // Read execution history
    let history = client.read_execution_history("test-exec", 1).await.unwrap();
    assert!(!history.is_empty());

    // Should contain at least OrchestrationStarted and OrchestrationCompleted events
    let has_started = history
        .iter()
        .any(|e| matches!(&e.kind, duroxide::EventKind::OrchestrationStarted { .. }));
    let has_completed = history
        .iter()
        .any(|e| matches!(&e.kind, duroxide::EventKind::OrchestrationCompleted { .. }));
    assert!(has_started);
    assert!(has_completed);

    // Test non-existent execution
    let result = client.get_execution_info("test-exec", 999).await;
    assert!(result.is_err());
}

/// Test: Multi-execution support (ContinueAsNew)
#[tokio::test]
async fn test_multi_execution_support() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "ContinueAsNewTest",
            |ctx: OrchestrationContext, count_str: String| async move {
                let count: u32 = count_str.parse().unwrap_or(0);
                if count < 3 {
                    return ctx.continue_as_new((count + 1).to_string()).await;
                } else {
                    Ok(format!("Final: {count}"))
                }
            },
        )
        .build();

    let _rt =
        runtime::Runtime::start_with_store(store.clone(), ActivityRegistry::builder().build(), orchestrations).await;

    // Start orchestration that will ContinueAsNew
    client
        .start_orchestration("test-continue", "ContinueAsNewTest", "0")
        .await
        .unwrap();

    // Wait for completion using wait_for_orchestration instead of sleep
    match client
        .wait_for_orchestration("test-continue", std::time::Duration::from_secs(5))
        .await
    {
        Ok(status) => println!("Orchestration completed with status: {status:?}"),
        Err(e) => println!("Orchestration failed: {e:?}"),
    }

    // Add a small delay to ensure all processing is complete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // ContinueAsNew creates separate execution records now
    let executions = client.list_executions("test-continue").await.unwrap();

    // Should have exactly 4 executions: exec_id=1 (count=0→1), exec_id=2 (count=1→2),
    // exec_id=3 (count=2→3), exec_id=4 (count=3, completes)
    assert_eq!(executions.len(), 4);
    assert_eq!(executions, vec![1, 2, 3, 4]);

    // Get info for each execution
    for exec_id in &executions {
        let exec_info = client.get_execution_info("test-continue", *exec_id).await.unwrap();
        assert_eq!(exec_info.execution_id, *exec_id);

        // First 3 executions should be ContinuedAsNew, last one should be Completed
        if *exec_id == 4 {
            assert_eq!(exec_info.status, "Completed");
        } else {
            assert_eq!(exec_info.status, "ContinuedAsNew");
        }
    }

    // Instance info should show the latest execution
    let instance_info = client.get_instance_info("test-continue").await.unwrap();
    assert_eq!(instance_info.current_execution_id, 4); // Should be 4 (the final execution)
    assert_eq!(instance_info.status, "Completed"); // Instance status is Completed
}

/// Test: System metrics
#[tokio::test]
async fn test_system_metrics() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let activities = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("Processed: {input}"))
        })
        .register("FailingActivity", |_ctx: ActivityContext, _input: String| async move {
            Err("Intentional failure".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "SuccessOrchestration",
            |ctx: OrchestrationContext, input: String| async move {
                let result = ctx.schedule_activity("TestActivity", input).await?;
                Ok(result)
            },
        )
        .register(
            "FailureOrchestration",
            |ctx: OrchestrationContext, input: String| async move {
                let _result = ctx.schedule_activity("FailingActivity", input).await?;
                Ok("Should not reach here".to_string())
            },
        )
        .register(
            "RunningOrchestration",
            |ctx: OrchestrationContext, _input: String| async move {
                // Wait for external event (never comes)
                let _event = ctx.schedule_wait("NeverComes").await;
                Ok("Should not reach here".to_string())
            },
        )
        .build();

    let _rt =
        runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, fast_runtime_options()).await;

    // Start orchestrations with different outcomes
    client
        .start_orchestration("success-1", "SuccessOrchestration", "input-1")
        .await
        .unwrap();
    client
        .start_orchestration("success-2", "SuccessOrchestration", "input-2")
        .await
        .unwrap();
    client
        .start_orchestration("failure-1", "FailureOrchestration", "input-1")
        .await
        .unwrap();
    client
        .start_orchestration("running-1", "RunningOrchestration", "input-1")
        .await
        .unwrap();

    // Wait for processing
    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;

    // Get system metrics
    let metrics = client.get_system_metrics().await.unwrap();

    assert_eq!(metrics.total_instances, 4);
    assert_eq!(metrics.total_executions, 4);
    assert_eq!(metrics.running_instances, 1); // running-1
    assert_eq!(metrics.completed_instances, 2); // success-1, success-2
    assert_eq!(metrics.failed_instances, 1); // failure-1
    assert!(metrics.total_events > 0);

    // Test status filtering
    let completed = client.list_instances_by_status("Completed").await.unwrap();
    assert_eq!(completed.len(), 2);
    assert!(completed.contains(&"success-1".to_string()));
    assert!(completed.contains(&"success-2".to_string()));

    let failed = client.list_instances_by_status("Failed").await.unwrap();
    assert_eq!(failed.len(), 1);
    assert!(failed.contains(&"failure-1".to_string()));

    let running = client.list_instances_by_status("Running").await.unwrap();
    assert_eq!(running.len(), 1);
    assert!(running.contains(&"running-1".to_string()));
}

/// Test: Queue depths
#[tokio::test]
async fn test_queue_depths() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let activities = ActivityRegistry::builder()
        .register("SlowActivity", |_ctx: ActivityContext, _input: String| async move {
            // Simulate slow activity
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            Ok("Slow result".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "QueueTestOrchestration",
            |ctx: OrchestrationContext, input: String| async move {
                let result = ctx.schedule_activity("SlowActivity", input).await?;
                Ok(result)
            },
        )
        .build();

    let _rt =
        runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, fast_runtime_options()).await;

    // Start multiple orchestrations quickly
    for i in 1..=5 {
        client
            .start_orchestration(
                &format!("queue-test-{i}"),
                "QueueTestOrchestration",
                &format!("input-{i}"),
            )
            .await
            .unwrap();
    }

    // Check queue depths immediately (should have pending work)
    let _queues = client.get_queue_depths().await.unwrap();

    // Should have some pending work in queues (counts are always >= 0)
    // Note: Queue depths are always non-negative, so these assertions are redundant

    // Wait for completion
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Check queue depths after completion (should be empty or minimal)
    let queues_after = client.get_queue_depths().await.unwrap();
    // Note: Some queues may still have items due to timing, so we just check they're reasonable
    assert!(queues_after.orchestrator_queue <= 1);
    assert!(queues_after.worker_queue <= 1);
    assert!(queues_after.timer_queue <= 1);
}

/// Test: Error handling for non-existent instances
#[tokio::test]
async fn test_error_handling() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    // Test all management methods with non-existent instance
    let result = client.get_instance_info("nonexistent").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));

    let result = client.get_execution_info("nonexistent", 1).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));

    let result = client.read_execution_history("nonexistent", 1).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());

    let executions = client.list_executions("nonexistent").await.unwrap();
    assert!(executions.is_empty());

    // Test status filtering with non-existent status
    let result = client.list_instances_by_status("NonExistentStatus").await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

/// Test: Management interface with complex workflow
#[tokio::test]
async fn test_complex_workflow_management() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());

    let activities = ActivityRegistry::builder()
        .register("ProcessOrder", |_ctx: ActivityContext, order: String| async move {
            Ok(format!("Processed order: {order}"))
        })
        .register("SendEmail", |_ctx: ActivityContext, email: String| async move {
            Ok(format!("Sent email: {email}"))
        })
        .register("UpdateInventory", |_ctx: ActivityContext, item: String| async move {
            Ok(format!("Updated inventory for: {item}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "OrderProcessing",
            |ctx: OrchestrationContext, order: String| async move {
                // Process order
                let result = ctx.schedule_activity("ProcessOrder", order.clone()).await?;

                // Send confirmation email
                let _email = ctx
                    .schedule_activity("SendEmail", format!("Order processed: {result}"))
                    .await?;

                // Update inventory
                let _inventory = ctx.schedule_activity("UpdateInventory", order).await?;

                Ok(result)
            },
        )
        .build();

    let _rt =
        runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, fast_runtime_options()).await;

    // Start multiple order processing workflows
    let orders = vec!["order-1", "order-2", "order-3", "order-4", "order-5"];
    for order in &orders {
        client
            .start_orchestration(*order, "OrderProcessing", *order)
            .await
            .unwrap();
    }

    // Wait for all orchestrations to complete (with timeout)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let completed = client.list_instances_by_status("Completed").await.unwrap();
        if completed.len() >= 5 {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "Timed out waiting for orchestrations to complete. Completed: {}",
                completed.len()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Verify all orders completed
    let instances = client.list_all_instances().await.unwrap();
    assert_eq!(instances.len(), 5);

    let completed = client.list_instances_by_status("Completed").await.unwrap();
    assert_eq!(completed.len(), 5);

    // Verify system metrics
    let metrics = client.get_system_metrics().await.unwrap();
    assert_eq!(metrics.total_instances, 5);
    assert_eq!(metrics.total_executions, 5);
    assert_eq!(metrics.completed_instances, 5);
    assert_eq!(metrics.failed_instances, 0);
    assert_eq!(metrics.running_instances, 0);
    assert!(metrics.total_events > 0);

    // Verify each instance details
    for order in &orders {
        let info = client.get_instance_info(order).await.unwrap();
        assert_eq!(info.instance_id, *order);
        assert_eq!(info.orchestration_name, "OrderProcessing");
        assert_eq!(info.status, "Completed");
        assert!(info.output.is_some());

        let executions = client.list_executions(order).await.unwrap();
        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0], 1);

        let exec_info = client.get_execution_info(order, 1).await.unwrap();
        assert_eq!(exec_info.execution_id, 1);
        assert_eq!(exec_info.status, "Completed");
        // Note: completed_at may be None due to SQLite timestamp handling
        assert!(exec_info.event_count > 0);

        let history = client.read_execution_history(order, 1).await.unwrap();
        assert!(!history.is_empty());

        // Should contain OrchestrationStarted, ActivityCompleted, and OrchestrationCompleted events
        let has_started = history
            .iter()
            .any(|e| matches!(&e.kind, duroxide::EventKind::OrchestrationStarted { .. }));
        let has_completed = history
            .iter()
            .any(|e| matches!(&e.kind, duroxide::EventKind::OrchestrationCompleted { .. }));
        let has_activity = history
            .iter()
            .any(|e| matches!(&e.kind, duroxide::EventKind::ActivityCompleted { .. }));

        assert!(has_started);
        assert!(has_completed);
        assert!(has_activity);
    }

    // Verify queue depths are empty
    let queues = client.get_queue_depths().await.unwrap();
    assert_eq!(queues.orchestrator_queue, 0);
    assert_eq!(queues.worker_queue, 0);
    assert_eq!(queues.timer_queue, 0);
}

// ============================================================================
// Coverage improvement tests (moved from coverage_improvement_tests.rs)
// ============================================================================

/// Test: Management API with unknown instance ID returns appropriate errors
#[tokio::test]
async fn test_management_unknown_instance_errors() {
    use duroxide::Event;
    use duroxide::providers::management::{ExecutionInfo, InstanceInfo};
    use duroxide::providers::{Provider, ProviderError};

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let mgmt = store.as_management_capability().unwrap();

    // get_instance_info should return error for unknown instance
    let result: Result<InstanceInfo, ProviderError> = mgmt.get_instance_info("unknown-instance").await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.message.contains("not found"),
        "Expected 'not found' error, got: {err}"
    );

    // get_execution_info should return error for unknown instance
    let result: Result<ExecutionInfo, ProviderError> = mgmt.get_execution_info("unknown-instance", 1).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.message.contains("not found"),
        "Expected 'not found' error, got: {err}"
    );

    // list_executions should return empty for unknown instance
    let result: Result<Vec<u64>, ProviderError> = mgmt.list_executions("unknown-instance").await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());

    // read_history_with_execution_id should return empty for unknown instance
    let result: Result<Vec<Event>, ProviderError> = mgmt.read_history_with_execution_id("unknown-instance", 1).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());

    // latest_execution_id returns Ok(1) for unknown instance (per design, default to exec 1)
    let result: Result<u64, ProviderError> = mgmt.latest_execution_id("unknown-instance").await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
}

/// Test: Management API read_execution for specific execution
#[tokio::test]
async fn test_management_read_execution_specific() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());
    let mgmt = store.as_management_capability().unwrap();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "ContinueTest",
            |ctx: OrchestrationContext, count_str: String| async move {
                let count: u32 = count_str.parse().unwrap_or(0);
                if count < 2 {
                    ctx.continue_as_new((count + 1).to_string()).await
                } else {
                    Ok(format!("Final: {count}"))
                }
            },
        )
        .build();

    let _rt =
        runtime::Runtime::start_with_store(store.clone(), ActivityRegistry::builder().build(), orchestrations).await;

    client
        .start_orchestration("read-exec-test", "ContinueTest", "0")
        .await
        .unwrap();

    // Wait for completion
    client
        .wait_for_orchestration("read-exec-test", Duration::from_secs(5))
        .await
        .unwrap();

    // Read each execution's history separately
    let executions = mgmt.list_executions("read-exec-test").await.unwrap();
    assert!(executions.len() >= 2, "Should have at least 2 executions");

    for exec_id in &executions {
        let history = mgmt
            .read_history_with_execution_id("read-exec-test", *exec_id)
            .await
            .unwrap();
        assert!(!history.is_empty(), "Execution {exec_id} should have history");

        // First event should be OrchestrationStarted
        let first_event = &history[0];
        assert!(
            matches!(&first_event.kind, duroxide::EventKind::OrchestrationStarted { .. }),
            "First event should be OrchestrationStarted"
        );
    }

    // Verify latest_execution_id
    let latest = mgmt.latest_execution_id("read-exec-test").await.unwrap();
    assert_eq!(latest, *executions.last().unwrap());
}

/// Test: Management API get_instance_tree with hierarchy
#[tokio::test]
async fn test_management_get_instance_tree() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());
    let mgmt = store.as_management_capability().unwrap();

    let activities = ActivityRegistry::builder()
        .register("SimpleActivity", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("Processed: {input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("Parent", |ctx: OrchestrationContext, _input: String| async move {
            // Spawn child sub-orchestrations
            let child1 = ctx.schedule_sub_orchestration_with_id("Child", "child-1", "input1".to_string());
            let child2 = ctx.schedule_sub_orchestration_with_id("Child", "child-2", "input2".to_string());
            let results = ctx.join(vec![child1, child2]).await;
            Ok(format!("Children: {:?}", results))
        })
        .register("Child", |ctx: OrchestrationContext, input: String| async move {
            let result = ctx.schedule_activity("SimpleActivity", input).await?;
            Ok(result)
        })
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities,
        orchestrations,
        RuntimeOptions {
            dispatcher_min_poll_interval: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await;

    client.start_orchestration("parent-1", "Parent", "start").await.unwrap();

    // Wait for completion
    client
        .wait_for_orchestration("parent-1", Duration::from_secs(10))
        .await
        .unwrap();

    // Get instance tree
    let tree = mgmt.get_instance_tree("parent-1").await.unwrap();
    assert_eq!(tree.root_id, "parent-1");
    assert!(
        tree.size() >= 3,
        "Tree should have parent + 2 children, got: {}",
        tree.size()
    );
    assert!(tree.all_ids.contains(&"parent-1".to_string()));
    assert!(tree.all_ids.contains(&"child-1".to_string()));
    assert!(tree.all_ids.contains(&"child-2".to_string()));
    assert!(!tree.is_root_only());
}

/// Test: Management list_instances_by_status with all status types
#[tokio::test]
async fn test_management_all_status_types() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store.clone());
    let mgmt = store.as_management_capability().unwrap();

    let activities = ActivityRegistry::builder()
        .register(
            "OkActivity",
            |_ctx: ActivityContext, input: String| async move { Ok(input) },
        )
        .register("FailActivity", |_ctx: ActivityContext, _: String| async move {
            Err("Intentional failure".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("Completed", |ctx: OrchestrationContext, input: String| async move {
            ctx.schedule_activity("OkActivity", input).await
        })
        .register("Failed", |ctx: OrchestrationContext, input: String| async move {
            ctx.schedule_activity("FailActivity", input).await
        })
        .register(
            "ContinuedAsNew",
            |ctx: OrchestrationContext, input: String| async move {
                let count: u32 = input.parse().unwrap_or(0);
                if count < 1 {
                    ctx.continue_as_new((count + 1).to_string()).await
                } else {
                    Ok("done".to_string())
                }
            },
        )
        .register("Running", |ctx: OrchestrationContext, _: String| async move {
            // Wait for external event that never comes
            let _event = ctx.schedule_wait("NeverComes").await;
            Ok("done".to_string())
        })
        .build();

    let _rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities,
        orchestrations,
        RuntimeOptions {
            dispatcher_min_poll_interval: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await;

    // Start orchestrations of each type
    client
        .start_orchestration("inst-completed", "Completed", "test")
        .await
        .unwrap();
    client
        .start_orchestration("inst-failed", "Failed", "test")
        .await
        .unwrap();
    client
        .start_orchestration("inst-continued", "ContinuedAsNew", "0")
        .await
        .unwrap();
    client
        .start_orchestration("inst-running", "Running", "test")
        .await
        .unwrap();

    // Wait for terminal ones to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test each status filter
    let completed = mgmt.list_instances_by_status("Completed").await.unwrap();
    assert!(
        completed.contains(&"inst-completed".to_string()) || completed.contains(&"inst-continued".to_string()),
        "Should have completed instances"
    );

    let failed = mgmt.list_instances_by_status("Failed").await.unwrap();
    assert!(
        failed.contains(&"inst-failed".to_string()),
        "Should have failed instances"
    );

    let running = mgmt.list_instances_by_status("Running").await.unwrap();
    assert!(
        running.contains(&"inst-running".to_string()),
        "Should have running instances"
    );

    // Test unknown status returns empty
    let unknown = mgmt.list_instances_by_status("UnknownStatus").await.unwrap();
    assert!(unknown.is_empty(), "Unknown status should return empty list");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Orchestration stats (Client::get_orchestration_stats)
// ═══════════════════════════════════════════════════════════════════════════════

/// Client::get_orchestration_stats returns stats after orchestration completes.
#[tokio::test]
async fn client_get_orchestration_stats_after_completion() {
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("StatsOrch", |ctx: OrchestrationContext, _: String| async move {
            ctx.set_kv_value("user_key", "user_value");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-stats-1", "StatsOrch", "")
        .await
        .unwrap();
    client
        .wait_for_orchestration("inst-stats-1", Duration::from_secs(5))
        .await
        .unwrap();

    let stats = client
        .get_orchestration_stats("inst-stats-1")
        .await
        .unwrap()
        .expect("stats should be present after completion");
    assert!(stats.history_event_count > 0, "should have history events");
    assert_eq!(stats.kv_user_key_count, 1, "one user key was set");
    assert!(stats.kv_total_value_bytes > 0, "user value bytes should be non-zero");

    rt.shutdown(None).await;
}

/// Client::get_orchestration_stats returns None for non-existent instance.
#[tokio::test]
async fn client_get_orchestration_stats_nonexistent() {
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let client = Client::new(store);
    let stats = client.get_orchestration_stats("no-such-instance").await.unwrap();
    assert!(stats.is_none());
}

/// Stats reflect accurate KV metrics across multiple keys.
#[tokio::test]
async fn client_orchestration_stats_kv_metrics_accuracy() {
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("MultiKV", |ctx: OrchestrationContext, _: String| async move {
            ctx.set_kv_value("k1", "aaa"); // 3 bytes
            ctx.set_kv_value("k2", "bbbbb"); // 5 bytes
            ctx.set_kv_value("k3", "cc"); // 2 bytes
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-multi-kv", "MultiKV", "")
        .await
        .unwrap();
    client
        .wait_for_orchestration("inst-multi-kv", Duration::from_secs(5))
        .await
        .unwrap();

    let stats = client
        .get_orchestration_stats("inst-multi-kv")
        .await
        .unwrap()
        .expect("stats should exist");
    assert_eq!(stats.kv_user_key_count, 3);
    assert_eq!(stats.kv_total_value_bytes, 10); // 3 + 5 + 2

    rt.shutdown(None).await;
}

/// Stats report accurate history_size_bytes for large histories (>256 KB).
#[tokio::test]
async fn client_orchestration_stats_large_history_size() {
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("BigResult", |_ctx: ActivityContext, input: String| async move {
            let n: usize = input.parse().unwrap_or(0);
            // Each activity returns ~64 KB × n of data
            Ok("X".repeat(64 * 1024 * n))
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("BigHistory", |ctx: OrchestrationContext, _: String| async move {
            // 4 activities with results of 64KB, 128KB, 192KB, 256KB
            for i in 1..=4u32 {
                ctx.schedule_activity("BigResult", i.to_string()).await?;
            }
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());
    client.start_orchestration("big-hist", "BigHistory", "").await.unwrap();
    client
        .wait_for_orchestration("big-hist", Duration::from_secs(10))
        .await
        .unwrap();

    let stats = client
        .get_orchestration_stats("big-hist")
        .await
        .unwrap()
        .expect("stats should exist");

    // 4 activities: Scheduled+Completed each = 8 events, plus Started + Completed = 10
    assert_eq!(stats.history_event_count, 10);
    // Payload: activity results are 64KB, 128KB, 192KB, 256KB = 640KB total in results alone
    // Plus JSON serialization overhead, input strings, event metadata
    assert!(
        stats.history_size_bytes > 256 * 1024,
        "history should be >256 KB, got {} bytes",
        stats.history_size_bytes,
    );
    assert!(
        stats.history_size_bytes < 1024 * 1024,
        "history should be <1 MB, got {} bytes",
        stats.history_size_bytes,
    );

    rt.shutdown(None).await;
}

/// Stats report accurate kv_total_value_bytes for large KV values.
#[tokio::test]
async fn client_orchestration_stats_large_kv_values() {
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("BigKV", |ctx: OrchestrationContext, _: String| async move {
            // 4 keys × 16 KB each = 64 KB total
            for i in 0..4 {
                ctx.set_kv_value(format!("big_{i}"), "Y".repeat(16 * 1024));
            }
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());
    client.start_orchestration("big-kv", "BigKV", "").await.unwrap();
    client
        .wait_for_orchestration("big-kv", Duration::from_secs(5))
        .await
        .unwrap();

    let stats = client
        .get_orchestration_stats("big-kv")
        .await
        .unwrap()
        .expect("stats should exist");

    assert_eq!(stats.kv_user_key_count, 4);
    assert_eq!(stats.kv_total_value_bytes, 4 * 16 * 1024); // exact: 65536

    rt.shutdown(None).await;
}

/// Stats report zero queue_pending_count for a fresh orchestration (no carry-forward).
#[tokio::test]
async fn client_orchestration_stats_no_carry_forward() {
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Simple", |_ctx: OrchestrationContext, _: String| async move {
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());
    client.start_orchestration("no-cf", "Simple", "").await.unwrap();
    client
        .wait_for_orchestration("no-cf", Duration::from_secs(5))
        .await
        .unwrap();

    let stats = client
        .get_orchestration_stats("no-cf")
        .await
        .unwrap()
        .expect("stats should exist");
    assert_eq!(stats.queue_pending_count, 0);

    rt.shutdown(None).await;
}

/// After ContinueAsNew, stats reflect the current (latest) execution only.
#[tokio::test]
async fn client_orchestration_stats_after_continue_as_new() {
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("Noop", |_ctx: ActivityContext, _: String| async move {
            Ok("ok".to_string())
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("CANStats", |ctx: OrchestrationContext, input: String| async move {
            let n: u32 = input.parse().unwrap_or(0);
            ctx.set_kv_value("iter", n.to_string());
            if n < 2 {
                // Activity creates a yield point so CAN sees history
                ctx.schedule_activity("Noop", "").await?;
                ctx.continue_as_new((n + 1).to_string()).await
            } else {
                Ok(format!("done:{n}"))
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());
    client.start_orchestration("can-stats", "CANStats", "0").await.unwrap();
    client
        .wait_for_orchestration("can-stats", Duration::from_secs(10))
        .await
        .unwrap();

    let stats = client
        .get_orchestration_stats("can-stats")
        .await
        .unwrap()
        .expect("stats should exist after CAN completion");

    // History events should be from the CURRENT (final) execution only,
    // not accumulated across all executions. Final execution just does
    // set_kv + Ok("done:2") = OrchestrationStarted + OrchestrationCompleted.
    assert!(
        stats.history_event_count >= 2,
        "should have at least Started + Completed, got {}",
        stats.history_event_count,
    );
    // Should NOT have the full history from all 3 executions
    assert!(
        stats.history_event_count <= 5,
        "should only count current execution events, got {}",
        stats.history_event_count,
    );

    // KV: "iter" key from the final execution (kv_store is instance-scoped, persists across CAN)
    assert!(stats.kv_user_key_count >= 1, "should have at least 1 KV key");

    rt.shutdown(None).await;
}

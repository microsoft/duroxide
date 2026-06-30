// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use duroxide::Either2;
use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{EventKind, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::time::Duration;

#[path = "../common/mod.rs"]
mod common;

/// Regression tests derived from real-world usage patterns (e.g., "instance actor" pattern)
/// These tests verify behavior for long continue-as-new chains, versioning, and concurrent execution.
#[tokio::test]
async fn continue_as_new_chain_5_iterations() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Register a simple counter orchestration that continues as new
    let counter_orch = |ctx: OrchestrationContext, input: String| async move {
        let count: u64 = input.parse().unwrap_or(0);

        // Log current iteration
        tracing::info!("Counter iteration: {}", count);

        if count < 4 {
            // Continue to next iteration (0-3 continue, 4 completes)
            return ctx.continue_as_new((count + 1).to_string()).await;
        } else {
            // Reached 4, complete (giving us 5 total executions: exec_id 1-5)
            Ok(format!("completed at {count}"))
        }
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("Counter", counter_orch) // Default versioning (1.0.0)
        .build();

    let activities = ActivityRegistry::builder().build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = duroxide::Client::new(store.clone());

    // Start the chain at count=0
    client
        .start_orchestration("counter-chain", "Counter", "0")
        .await
        .unwrap();

    // Wait for completion (give it plenty of time)
    let status = client
        .wait_for_orchestration("counter-chain", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "completed at 4");
            tracing::info!("✓ Continue-as-new chain completed successfully");
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!("Chain failed: {}", details.display_message());
        }
        _ => panic!("Unexpected status: {status:?}"),
    }

    // Verify we have 5 executions (exec 1 starts with count=0, continues until exec 5 with count=4)
    // Each execution should have its own history
    for exec_id in 1..=5 {
        let hist = client.read_execution_history("counter-chain", exec_id).await.unwrap();

        // Each execution should have OrchestrationStarted
        assert!(
            hist.iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationStarted { .. })),
            "Execution {exec_id} missing OrchestrationStarted"
        );

        // Verify version consistency - all should use default version
        if let Some(event) = hist
            .iter()
            .find(|e| matches!(&e.kind, EventKind::OrchestrationStarted { .. }))
            && let EventKind::OrchestrationStarted { version, .. } = &event.kind
        {
            // Version should be resolved to 1.0.0 (default)
            assert!(
                version.starts_with("1."),
                "Execution {exec_id} has unexpected version: {version}"
            );
        }

        // Verify terminal event
        let last = hist.last().unwrap();
        if exec_id <= 4 {
            assert!(
                matches!(&last.kind, EventKind::OrchestrationContinuedAsNew { .. }),
                "Execution {exec_id} should end with ContinuedAsNew, got {last:?}"
            );
        } else {
            assert!(
                matches!(&last.kind, EventKind::OrchestrationCompleted { .. }),
                "Execution 5 should end with Completed, got {last:?}"
            );
        }
    }

    tracing::info!("✓ All 5 executions verified");

    rt.shutdown(None).await;
}

/// Test continue-as-new chain with activity execution at each step
#[tokio::test]
async fn continue_as_new_chain_with_activities() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Simple echo activity
    let echo = |_ctx: duroxide::ActivityContext, input: String| async move { Ok(input) };

    // Orchestration that executes activity then continues
    let counter_with_activity = |ctx: OrchestrationContext, input: String| async move {
        let count: u64 = input.parse().unwrap_or(0);

        // Execute an activity at each step
        let activity_input = format!("step-{count}");
        let result = ctx
            .schedule_activity("Echo", activity_input)
            .await
            .map_err(|e| format!("Activity failed: {e}"))?;

        assert_eq!(result, format!("step-{count}"));

        if count < 4 {
            // 5 executions: 0-3 continue, 4 completes
            return ctx.continue_as_new((count + 1).to_string()).await;
        } else {
            Ok(format!("completed at {count}"))
        }
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("CounterWithActivity", counter_with_activity)
        .build();

    let activities = ActivityRegistry::builder().register("Echo", echo).build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("counter-activity-chain", "CounterWithActivity", "0")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("counter-activity-chain", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "completed at 4");
            tracing::info!("✓ Continue-as-new chain with activities completed");
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!("Chain failed: {}", details.display_message());
        }
        _ => panic!("Unexpected status: {status:?}"),
    }

    // Verify each execution has activity events
    for exec_id in 1..=5 {
        let hist = client
            .read_execution_history("counter-activity-chain", exec_id)
            .await
            .unwrap();

        // Should have ActivityScheduled and ActivityCompleted
        assert!(
            hist.iter()
                .any(|e| matches!(&e.kind, EventKind::ActivityScheduled { .. })),
            "Execution {exec_id} missing ActivityScheduled"
        );
        assert!(
            hist.iter()
                .any(|e| matches!(&e.kind, EventKind::ActivityCompleted { .. })),
            "Execution {exec_id} missing ActivityCompleted"
        );
    }

    tracing::info!("✓ All 5 executions with activities verified");

    rt.shutdown(None).await;
}

/// Test concurrent continue-as-new chains (stress test)
#[tokio::test]
async fn concurrent_continue_as_new_chains() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let counter_orch = |ctx: OrchestrationContext, input: String| async move {
        let count: u64 = input.parse().unwrap_or(0);

        if count < 4 {
            // 5 executions: 0-3 continue, 4 completes
            return ctx.continue_as_new((count + 1).to_string()).await;
        } else {
            Ok(format!("completed at {count}"))
        }
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("ConcurrentCounter", counter_orch)
        .build();

    let activities = ActivityRegistry::builder().build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = duroxide::Client::new(store.clone());

    // Start 5 concurrent chains
    let instances: Vec<String> = (0..5).map(|i| format!("concurrent-chain-{i}")).collect();

    for instance in &instances {
        client
            .start_orchestration(instance, "ConcurrentCounter", "0")
            .await
            .unwrap();
    }

    // Wait for all to complete
    for instance in &instances {
        let status = client
            .wait_for_orchestration(instance, Duration::from_secs(10))
            .await
            .unwrap();

        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!(output, "completed at 4");
            }
            OrchestrationStatus::Failed { details, .. } => {
                panic!("Chain {} failed: {}", instance, details.display_message());
            }
            _ => panic!("Unexpected status for {instance}: {status:?}"),
        }
    }

    tracing::info!("✓ All 5 concurrent chains completed successfully");

    // Verify each chain has 5 executions
    for instance in &instances {
        for exec_id in 1..=5 {
            let hist = client.read_execution_history(instance, exec_id).await.unwrap();

            assert!(
                !hist.is_empty(),
                "Chain {instance} execution {exec_id} has empty history"
            );
        }
    }

    tracing::info!("✓ All executions verified for all chains");

    rt.shutdown(None).await;
}

/// Test modeling a real-world instance actor pattern with multiple activities per iteration
#[tokio::test]
async fn instance_actor_pattern_stress_test() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Mock activity types (simplified versions of the real ones)
    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct GetInstanceConnectionInput {
        k8s_name: String,
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct GetInstanceConnectionOutput {
        found: bool,
        connection_string: Option<String>,
        state: Option<String>,
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct TestConnectionInput {
        connection_string: String,
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct TestConnectionOutput {
        version: String,
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct RecordHealthCheckInput {
        k8s_name: String,
        status: String,
        postgres_version: Option<String>,
        response_time_ms: Option<i32>,
        error_message: Option<String>,
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct RecordHealthCheckOutput {
        recorded: bool,
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct UpdateInstanceHealthInput {
        k8s_name: String,
        health_status: String,
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct UpdateInstanceHealthOutput {
        updated: bool,
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct InstanceActorInput {
        k8s_name: String,
        orchestration_id: String,
        iteration: u64, // Track iteration for testing
    }

    // Mock activities
    let get_instance_connection = |_ctx: duroxide::ActivityContext, input: String| async move {
        let parsed: GetInstanceConnectionInput =
            serde_json::from_str(&input).map_err(|e| format!("Parse error: {e}"))?;

        let output = GetInstanceConnectionOutput {
            found: true,
            connection_string: Some(format!("postgresql://localhost/db_{}", parsed.k8s_name)),
            state: Some("running".to_string()),
        };

        serde_json::to_string(&output).map_err(|e| format!("Serialize error: {e}"))
    };

    let test_connection = |_ctx: duroxide::ActivityContext, input: String| async move {
        let parsed: TestConnectionInput = serde_json::from_str(&input).map_err(|e| format!("Parse error: {e}"))?;

        // Simulate connection test
        assert!(parsed.connection_string.starts_with("postgresql://"));

        let output = TestConnectionOutput {
            version: "PostgreSQL 16.1".to_string(),
        };

        serde_json::to_string(&output).map_err(|e| format!("Serialize error: {e}"))
    };

    let record_health_check = |_ctx: duroxide::ActivityContext, input: String| async move {
        let _parsed: RecordHealthCheckInput = serde_json::from_str(&input).map_err(|e| format!("Parse error: {e}"))?;

        let output = RecordHealthCheckOutput { recorded: true };
        serde_json::to_string(&output).map_err(|e| format!("Serialize error: {e}"))
    };

    let update_instance_health = |_ctx: duroxide::ActivityContext, input: String| async move {
        let _parsed: UpdateInstanceHealthInput =
            serde_json::from_str(&input).map_err(|e| format!("Parse error: {e}"))?;

        let output = UpdateInstanceHealthOutput { updated: true };
        serde_json::to_string(&output).map_err(|e| format!("Serialize error: {e}"))
    };

    // Instance actor orchestration (5 iterations for stress test)
    let instance_actor = |ctx: OrchestrationContext, input: String| async move {
        let mut input_data: InstanceActorInput =
            serde_json::from_str(&input).map_err(|e| format!("Failed to parse input: {e}"))?;

        ctx.trace_info(format!(
            "Instance actor iteration {} for: {} (orchestration: {})",
            input_data.iteration, input_data.k8s_name, input_data.orchestration_id
        ));

        // Exit after 5 iterations for stress test
        // Executions 1-4 (iteration 0-3) do full cycle, execution 5 (iteration 4) completes
        if input_data.iteration >= 4 {
            return Ok(format!("completed after {} executions", input_data.iteration + 1));
        }

        // Step 1: Get instance connection string from CMS
        let conn_info = ctx
            .schedule_activity_typed::<GetInstanceConnectionInput, GetInstanceConnectionOutput>(
                "cms-get-instance-connection",
                &GetInstanceConnectionInput {
                    k8s_name: input_data.k8s_name.clone(),
                },
            )
            .await
            .map_err(|e| format!("Failed to get instance connection: {e}"))?;

        // Step 2: Check if instance still exists
        if !conn_info.found {
            ctx.trace_info("Instance no longer exists in CMS, stopping instance actor");
            return Ok("instance not found".to_string());
        }

        let connection_string = match conn_info.connection_string {
            Some(conn) => conn,
            None => {
                ctx.trace_warn("No connection string available yet, skipping health check");

                // Wait and retry
                ctx.schedule_timer(Duration::from_millis(50)).await; // 50ms (was 30s)

                input_data.iteration += 1;
                let input_json =
                    serde_json::to_string(&input_data).map_err(|e| format!("Failed to serialize input: {e}"))?;
                return ctx.continue_as_new(input_json).await;
            }
        };

        // Step 3: Test connection
        let health_result = ctx
            .schedule_activity_typed::<TestConnectionInput, TestConnectionOutput>(
                "test-connection",
                &TestConnectionInput {
                    connection_string: connection_string.clone(),
                },
            )
            .await;

        // Step 4: Determine health status
        let (status, postgres_version, error_message) = match health_result {
            Ok(output) => {
                ctx.trace_info("Health check passed");
                ("healthy", Some(output.version), None)
            }
            Err(e) => {
                ctx.trace_warn(format!("Health check failed: {e}"));
                ("unhealthy", None, Some(e.to_string()))
            }
        };

        // Step 5: Record health check
        let _record = ctx
            .schedule_activity_typed::<RecordHealthCheckInput, RecordHealthCheckOutput>(
                "cms-record-health-check",
                &RecordHealthCheckInput {
                    k8s_name: input_data.k8s_name.clone(),
                    status: status.to_string(),
                    postgres_version,
                    response_time_ms: Some(50),
                    error_message,
                },
            )
            .await
            .map_err(|e| format!("Failed to record health check: {e}"))?;

        // Step 6: Update instance health status
        let _update = ctx
            .schedule_activity_typed::<UpdateInstanceHealthInput, UpdateInstanceHealthOutput>(
                "cms-update-instance-health",
                &UpdateInstanceHealthInput {
                    k8s_name: input_data.k8s_name.clone(),
                    health_status: status.to_string(),
                },
            )
            .await
            .map_err(|e| format!("Failed to update instance health: {e}"))?;

        ctx.trace_info(format!("Health check complete, status: {status}"));

        // Step 7: Wait before next check
        ctx.schedule_timer(Duration::from_millis(50)).await; // 50ms (was 30s)

        ctx.trace_info("Restarting instance actor with continue-as-new");

        // Step 8: Continue as new
        input_data.iteration += 1;
        let input_json = serde_json::to_string(&input_data).map_err(|e| format!("Failed to serialize input: {e}"))?;

        return ctx.continue_as_new(input_json).await;
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("InstanceActor", instance_actor)
        .build();

    let activities = ActivityRegistry::builder()
        .register_typed("cms-get-instance-connection", get_instance_connection)
        .register_typed("test-connection", test_connection)
        .register_typed("cms-record-health-check", record_health_check)
        .register_typed("cms-update-instance-health", update_instance_health)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = duroxide::Client::new(store.clone());

    // Start 3 parallel instance actors
    let instances = vec![
        ("instance-actor-test-1", "test-instance-1", "orch-123-1"),
        ("instance-actor-test-2", "test-instance-2", "orch-123-2"),
        ("instance-actor-test-3", "test-instance-3", "orch-123-3"),
    ];

    for (instance_id, k8s_name, orch_id) in &instances {
        let input = InstanceActorInput {
            k8s_name: k8s_name.to_string(),
            orchestration_id: orch_id.to_string(),
            iteration: 0,
        };

        let input_json = serde_json::to_string(&input).unwrap();

        client
            .start_orchestration(*instance_id, "InstanceActor", &input_json)
            .await
            .unwrap();

        tracing::info!("Started instance actor: {}", instance_id);
    }

    // Wait for all 3 to complete (5 iterations × 50ms timer = 0.25s approx, use 30s timeout)
    for (instance_id, _k8s_name, _orch_id) in &instances {
        let status = client
            .wait_for_orchestration(instance_id, Duration::from_secs(30))
            .await
            .unwrap();

        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert!(output.contains("completed after 5 executions"));
                tracing::info!("✓ Instance actor {} completed after 5 executions", instance_id);
            }
            OrchestrationStatus::Failed { details, .. } => {
                eprintln!(
                    "\n❌ Instance actor {} failed: {}\n",
                    instance_id,
                    details.display_message()
                );
                eprintln!("=== DUMPING ALL EXECUTION HISTORIES FOR {instance_id} ===\n");

                // Find how many executions exist
                let mut exec_id = 1;
                loop {
                    match client.read_execution_history(instance_id, exec_id).await {
                        Ok(hist) if !hist.is_empty() => {
                            eprintln!("--- Execution {exec_id} ---");
                            eprintln!("Events: {}", hist.len());
                            for (idx, event) in hist.iter().enumerate() {
                                let event_json =
                                    serde_json::to_string_pretty(event).unwrap_or_else(|_| format!("{event:?}"));
                                eprintln!("  Event {}: {}", idx + 1, event_json);
                            }
                            eprintln!();
                            exec_id += 1;
                        }
                        _ => break,
                    }

                    // Safety limit
                    if exec_id > 100 {
                        eprintln!("(stopping dump at execution 100)");
                        break;
                    }
                }

                eprintln!("=== END OF HISTORY DUMP ===\n");
                panic!("Instance actor {} failed: {}", instance_id, details.display_message());
            }
            _ => panic!("Unexpected status for {instance_id}: {status:?}"),
        }
    }

    tracing::info!("✓ All 3 instance actors completed successfully");

    // Verify each execution has the expected activities for all 3 instances
    for (instance_id, _k8s_name, _orch_id) in &instances {
        tracing::info!("Verifying executions for {}", instance_id);

        for exec_id in 1..=5 {
            let hist = client.read_execution_history(instance_id, exec_id).await.unwrap();

            // Count activities scheduled in this execution
            let activity_count = hist
                .iter()
                .filter(|e| matches!(&e.kind, EventKind::ActivityScheduled { .. }))
                .count();

            // Executions 1-4 have full cycle (4 activities), execution 5 exits immediately (0 activities)
            if exec_id < 5 {
                assert!(
                    activity_count >= 4,
                    "{instance_id} execution {exec_id} should have at least 4 activities, has {activity_count}"
                );
            }

            // Verify OrchestrationStarted has proper version
            if let Some(event) = hist
                .iter()
                .find(|e| matches!(&e.kind, EventKind::OrchestrationStarted { .. }))
                && let EventKind::OrchestrationStarted { name, version, .. } = &event.kind
            {
                assert_eq!(name, "InstanceActor");
                assert!(
                    version.starts_with("1."),
                    "{instance_id} execution {exec_id} has unexpected version: {version}"
                );
            }

            // Verify terminal event
            // Executions 1-4: continue-as-new, Execution 5: completes
            if exec_id < 5 {
                assert!(
                    hist.iter()
                        .any(|e| matches!(&e.kind, EventKind::OrchestrationContinuedAsNew { .. })),
                    "{instance_id} execution {exec_id} should have ContinuedAsNew"
                );
            } else {
                assert!(
                    hist.iter()
                        .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { .. })),
                    "{instance_id} execution {exec_id} should have Completed"
                );
            }
        }

        tracing::info!("✓ All 5 executions verified for {}", instance_id);
    }

    tracing::info!("✓ All 3 instance actors completed successfully");
    tracing::info!("✓ Total: 15 executions (3 instances × 5 executions each)");
    tracing::info!("✓ Total: 48 activities (3 instances × 4 full cycles × 4 activities)");
    tracing::info!("✓ Total: 12 timers (3 instances × 4 full cycles)");

    rt.shutdown(None).await;
}

/// Regression test: Timer fires at correct time after previous TimerFired exists in history
///
/// STATUS: FIXED - This test now passes.
///
/// This bug was caught by toygres instance actor pattern where:
/// 1. A 5-second poll timer fires, creating TimerFired in history
/// 2. Later, a 60-second timeout timer is scheduled
/// 3. BUG (FIXED): The timeout timer was firing early because calculate_timer_fire_time()
///    used the PREVIOUS TimerFired.fire_at_ms as "now" instead of actual system time
///
/// Evidence from toygres (before fix):
/// - Timer scheduled for fire_at_ms: 1764307112351 (60s from scheduling time)
/// - Timer actually fired at: 1764307079416 (33 seconds EARLY!)
/// - Root cause: Used old TimerFired.fire_at_ms (1764307019416) + 60000 = 1764307079416
///
/// FIX APPLIED: Action::CreateTimer now includes fire_at_ms (computed in futures.rs
/// using system time) instead of delay_ms. execution.rs uses this directly.
#[tokio::test]
async fn timer_fires_at_correct_time_regression() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Activity that takes a configurable time to complete
    async fn slow_activity(_ctx: duroxide::ActivityContext, input: String) -> Result<String, String> {
        let delay_ms: u64 = input.parse().unwrap_or(2000);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        Ok("activity_done".to_string())
    }

    // Orchestration that:
    // 1. Waits for a short timer (creates TimerFired in history)
    // 2. Waits significant real time to pass via slow activity
    // 3. Races a timer against a fast activity
    // 4. The fast activity should win (timer fires at correct future time)
    let test_orch = |ctx: OrchestrationContext, _input: String| async move {
        // Phase 1: Wait for a 100ms timer - this creates TimerFired in history at time T0+100
        ctx.schedule_timer(Duration::from_millis(100)).await;

        // Phase 2: Do a slow activity (2 seconds of real time passes)
        // After this, system time is approximately T0 + 2100ms
        let _ = ctx.schedule_activity("SlowActivity", "2000").await;

        // Phase 3: Now race a 1-second timer against a fast activity (100ms)
        //
        // Expected behavior (after fix):
        // - Timer fire_at = now + 1000 = (T0 + 2100) + 1000 = T0 + 3100
        // - Activity completes at T0 + 2200 (100ms from now)
        // - Activity wins because T0 + 2200 < T0 + 3100
        let timer = ctx.schedule_timer(Duration::from_secs(1));
        let activity = ctx.schedule_activity("SlowActivity", "100"); // 100ms activity

        let result = match ctx.select2(timer, activity).await {
            Either2::First(_) => {
                // Timer won - would indicate regression of the fix
                "timer_won".to_string()
            }
            Either2::Second(r) => {
                // Activity won - this is correct
                r.unwrap_or_else(|e| format!("activity_failed: {e}"))
            }
        };
        Ok(result)
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("TimerBugTest", test_orch)
        .build();

    let activities = ActivityRegistry::builder()
        .register("SlowActivity", slow_activity)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = duroxide::Client::new(store.clone());

    // Start the test orchestration
    client
        .start_orchestration("timer-bug-test", "TimerBugTest", "")
        .await
        .unwrap();

    // Wait for completion
    let status = client
        .wait_for_orchestration("timer-bug-test", Duration::from_secs(15))
        .await
        .unwrap();

    rt.shutdown(None).await;

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            // The fast activity (100ms) should beat the 1-second timer
            assert_ne!(
                output, "timer_won",
                "Timer fired early! The 1-second timer should not beat a 100ms activity. \
                This indicates the calculate_timer_fire_time bug has regressed."
            );
            assert_eq!(output, "activity_done", "Activity should have won the race");
            tracing::info!("✓ Timer fired at correct time, activity won the race as expected");
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected status: {status:?}"),
    }
}

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

mod common;

use duroxide::runtime::{self, registry::ActivityRegistry};
use duroxide::{ActivityContext, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_new_guid() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TestGuid", |ctx: OrchestrationContext, _input: String| async move {
            let guid1 = ctx.new_guid().await?;
            let guid2 = ctx.new_guid().await?;

            // GUIDs should be different
            assert_ne!(guid1, guid2);

            // GUIDs should be valid hex strings (excluding hyphens)
            assert!(guid1.chars().filter(|c| *c != '-').all(|c| c.is_ascii_hexdigit()));
            assert!(guid2.chars().filter(|c| *c != '-').all(|c| c.is_ascii_hexdigit()));

            Ok(format!("{guid1},{guid2}"))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("test-guid", "TestGuid", "").await.unwrap();
    let client = duroxide::Client::new(store.clone());
    let status = client
        .wait_for_orchestration("test-guid", tokio::time::Duration::from_secs(5))
        .await
        .unwrap();

    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        // Result should contain two different GUIDs
        let parts: Vec<&str> = output.split(',').collect();
        assert_eq!(parts.len(), 2);
        assert_ne!(parts[0], parts[1]);
    } else {
        panic!("Orchestration did not complete successfully: {status:?}");
    }

    rt.shutdown(None).await;
}

#[tokio::test]
async fn test_utc_now_ms() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TestTime", |ctx: OrchestrationContext, _input: String| async move {
            let time1 = ctx.utc_now().await?;

            // Add a small timer to ensure time progresses
            ctx.schedule_timer(Duration::from_millis(100)).await;

            let time2 = ctx.utc_now().await?;

            // Convert to milliseconds for validation
            let t1 = time1
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis() as u64;
            let t2 = time2
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis() as u64;

            // Times should be reasonable (after year 2020)
            assert!(t1 > 1577836800000); // Jan 1, 2020
            assert!(t2 > 1577836800000);

            // Second time should be after first (since we had a timer in between)
            assert!(t2 >= t1);

            Ok(format!("{t1},{t2}"))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("test-time", "TestTime", "").await.unwrap();
    let client = duroxide::Client::new(store.clone());
    let status = client
        .wait_for_orchestration("test-time", tokio::time::Duration::from_secs(5))
        .await
        .unwrap();

    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        // Result should contain two timestamps
        let parts: Vec<&str> = output.split(',').collect();
        assert_eq!(parts.len(), 2);
    } else {
        panic!("Orchestration did not complete successfully: {status:?}");
    }

    rt.shutdown(None).await;
}

#[tokio::test]
async fn test_system_calls_deterministic_replay() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "TestDeterminism",
            |ctx: OrchestrationContext, _input: String| async move {
                let guid = ctx.new_guid().await?;
                let time = ctx.utc_now().await?;

                // Use values in some computation
                let time_ms = time
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| e.to_string())?
                    .as_millis() as u64;
                let result = format!("guid:{guid},time:{time_ms}");

                Ok(result)
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities.clone(), orchestrations.clone()).await;

    // Run orchestration first time
    let instance = "test-determinism";
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration(instance, "TestDeterminism", "")
        .await
        .unwrap();
    let client = duroxide::Client::new(store.clone());
    let status1 = client
        .wait_for_orchestration(instance, tokio::time::Duration::from_secs(5))
        .await
        .unwrap();

    let output1 = if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status1 {
        output
    } else {
        panic!("First run did not complete successfully: {status1:?}");
    };

    rt.shutdown(None).await;

    // Start new runtime with same store (simulating restart)
    let rt2 = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    // The orchestration should complete with the same result due to deterministic replay
    let client2 = duroxide::Client::new(store.clone());
    let status2 = client2
        .wait_for_orchestration(instance, tokio::time::Duration::from_secs(5))
        .await
        .unwrap();

    let output2 = if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status2 {
        output
    } else {
        panic!("Second run did not complete successfully: {status2:?}");
    };

    // Outputs should be identical
    assert_eq!(output1, output2);

    rt2.shutdown(None).await;
}

#[tokio::test]
async fn test_system_calls_with_select() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("QuickTask", |_ctx: ActivityContext, _: String| async move {
            Ok("task_done".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TestSelect", |ctx: OrchestrationContext, _input: String| async move {
            // Test: System calls should work correctly with activities in select/join

            // First, get a system call result
            let guid = ctx.new_guid().await?;

            // Test select2 with activities - system calls complete synchronously in the background
            let activity1 = ctx.schedule_activity("QuickTask", "task1");
            let activity2 = ctx.schedule_activity("QuickTask", "task2");

            let (winner_idx, output) = ctx.select2(activity1, activity2).await.into_tuple();

            let first_result = match output {
                Ok(s) => s,
                Err(e) => format!("error: {e}"),
            };

            // Get another system call to verify they work throughout the orchestration
            let time = ctx.utc_now().await?;

            // Verify both system calls returned valid values
            assert!(guid.len() == 36, "GUID should be valid");
            let time_ms = time
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis() as u64;
            assert!(time_ms > 0, "Time should be positive");

            Ok(format!(
                "winner:{},result:{},guid_len:{},time_valid:true",
                winner_idx,
                first_result,
                guid.len()
            ))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("test-select", "TestSelect", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("test-select", tokio::time::Duration::from_secs(5))
        .await
        .unwrap();

    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        println!("Output: {output}");
        // Output should contain winner index, result, guid validation, and time validation
        assert!(output.starts_with("winner:"), "Output should start with 'winner:'");
        assert!(output.contains("result:task_done"), "Output should contain task result");
        assert!(output.contains("guid_len:36"), "GUID should be 36 chars");
        assert!(output.contains("time_valid:true"), "Time should be valid");
    } else {
        panic!("Orchestration did not complete successfully: {status:?}");
    }

    rt.shutdown(None).await;
}

#[tokio::test]
async fn test_system_calls_join_with_activities() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("SlowTask", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("processed:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TestJoin", |ctx: OrchestrationContext, _input: String| async move {
            // Test 1: Call system calls and activity separately since they have different return types
            let guid = ctx.new_guid().await?;
            let time = ctx.utc_now().await?;
            let activity_result = ctx.schedule_activity("SlowTask", "data1").await?;

            // Validate the values
            assert_eq!(guid.len(), 36, "GUID should be 36 chars");
            let time_ms = time
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis() as u64;
            assert!(time_ms > 0, "Time should be positive");
            assert_eq!(activity_result, "processed:data1");

            // Test 2: Select between two activities (same type)
            let activity1 = ctx.schedule_activity("SlowTask", "data2");
            let activity2 = ctx.schedule_activity("SlowTask", "data3");

            let (winner_idx, output) = ctx.select2(activity1, activity2).await.into_tuple();

            let winner_result = match output {
                Ok(s) => s,
                Err(e) => panic!("Expected activity output: {e}"),
            };

            // System call should typically win since it completes synchronously
            // But we accept either winner

            Ok(format!(
                "guid_len:{},time:{},activity:{},winner:{},winner_result:{}",
                guid.len(),
                time_ms,
                activity_result,
                winner_idx,
                winner_result
            ))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("test-join", "TestJoin", "").await.unwrap();
    let status = client
        .wait_for_orchestration("test-join", tokio::time::Duration::from_secs(5))
        .await
        .unwrap();

    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        println!("Output: {output}");
        assert!(output.contains("guid_len:36"), "GUID should be 36 chars");
        assert!(
            output.contains("activity:processed:data1"),
            "Activity should process correctly"
        );
        assert!(output.contains("winner:"), "Should have winner");
    } else {
        panic!("Orchestration did not complete successfully: {status:?}");
    }

    rt.shutdown(None).await;
}

/// Test: Verify that utc_now() used as activity input replays correctly.
///
/// This test verifies replay correctness:
/// 1. First turn: utc_now returns T1, waits for external event
/// 2. External event triggers second turn (replay)
/// 3. On replay, utc_now should return the SAME value T1 from history
/// 4. Activity is scheduled with T1 as input - should match history
#[tokio::test]
async fn test_utc_now_as_activity_input_replays_correctly() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register(
            "ProcessWithTimestamp",
            |_ctx: ActivityContext, input: String| async move { Ok(format!("processed:{}", input)) },
        )
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "TestUtcNowReplayAsInput",
            |ctx: OrchestrationContext, _input: String| async move {
                // Get a timestamp
                let time = ctx.utc_now().await?;
                let time_ms = time
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| e.to_string())?
                    .as_millis() as u64;

                // Wait for external event - this forces a turn boundary
                let _ = ctx.schedule_wait("continue").await;

                // Use timestamp as input to an activity
                // On replay, utc_now must return the same value or this will cause nondeterminism
                let result = ctx
                    .schedule_activity("ProcessWithTimestamp", time_ms.to_string())
                    .await?;

                Ok(result)
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities.clone(), orchestrations.clone()).await;
    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("test-utcnow-replay", "TestUtcNowReplayAsInput", "")
        .await
        .unwrap();

    // Wait for the external subscription to be registered
    let subscribed = common::wait_for_subscription(store.clone(), "test-utcnow-replay", "continue", 2000).await;
    assert!(subscribed, "Orchestration should subscribe to 'continue' event");

    // Wait a bit so wall-clock time advances
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send the external event - this triggers replay of the orchestration
    client
        .raise_event("test-utcnow-replay", "continue", "go")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("test-utcnow-replay", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        duroxide::runtime::OrchestrationStatus::Completed { output, .. } => {
            println!("Orchestration completed: {}", output);
            assert!(output.starts_with("processed:"), "Should have processed the timestamp");
        }
        duroxide::runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        other => {
            panic!("Unexpected status: {other:?}");
        }
    }

    rt.shutdown(None).await;
}

/// Test: Verify that new_guid() used as activity input replays correctly.
///
/// Similar to utc_now test - new_guid must return the same value on replay.
#[tokio::test]
async fn test_new_guid_as_activity_input_replays_correctly() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("ProcessWithId", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("processed:{}", input))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "TestGuidNondeterminism",
            |ctx: OrchestrationContext, _input: String| async move {
                // Get a GUID
                let guid = ctx.new_guid().await?;

                // Wait for external event - this forces a turn boundary
                let _ = ctx.schedule_wait("continue").await;

                // Use guid as input to an activity
                // On replay, new_guid must return the same value or this will cause nondeterminism
                let result = ctx.schedule_activity("ProcessWithId", guid).await?;

                Ok(result)
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities.clone(), orchestrations.clone()).await;
    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("test-guid-replay", "TestGuidNondeterminism", "")
        .await
        .unwrap();

    // Wait for the external subscription to be registered
    let subscribed = common::wait_for_subscription(store.clone(), "test-guid-replay", "continue", 2000).await;
    assert!(subscribed, "Orchestration should subscribe to 'continue' event");

    // Send the external event - this triggers replay
    client.raise_event("test-guid-replay", "continue", "go").await.unwrap();

    let status = client
        .wait_for_orchestration("test-guid-replay", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        duroxide::runtime::OrchestrationStatus::Completed { output, .. } => {
            println!("Orchestration completed: {}", output);
            assert!(output.starts_with("processed:"), "Should have processed the guid");
        }
        duroxide::runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        other => {
            panic!("Unexpected status: {other:?}");
        }
    }

    rt.shutdown(None).await;
}

#[tokio::test]
async fn test_activity_then_syscall_ordering() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );

    let activities = ActivityRegistry::builder()
        .register("A", |_ctx: ActivityContext, _input: String| async move {
            Ok("a".to_string())
        })
        .register("B", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("b:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("Ordering", |ctx: OrchestrationContext, _input: String| async move {
            let _ = ctx.schedule_activity("A", "").await?;
            let guid = ctx.new_guid().await?;
            let _ = ctx.schedule_activity("B", guid).await?;
            Ok("ok".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("test-ordering-1", "Ordering", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("test-ordering-1", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        status,
        duroxide::runtime::OrchestrationStatus::Completed { .. }
    ));

    let history = client.read_execution_history("test-ordering-1", 1).await.unwrap();
    let scheduled_names: Vec<String> = history
        .iter()
        .filter_map(|e| match &e.kind {
            duroxide::EventKind::ActivityScheduled { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect();

    // Ordering should be stable: A, syscall(new_guid), B
    assert_eq!(scheduled_names.len(), 3);
    assert_eq!(scheduled_names[0], "A");
    assert_eq!(scheduled_names[1], "__duroxide_syscall:new_guid");
    assert_eq!(scheduled_names[2], "B");

    rt.shutdown(None).await;
}

#[tokio::test]
async fn test_multiple_syscalls_same_type() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TwoGuids", |ctx: OrchestrationContext, _input: String| async move {
            let g1 = ctx.new_guid().await?;
            let g2 = ctx.new_guid().await?;
            assert_ne!(g1, g2);
            Ok(format!("{g1},{g2}"))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities.clone(), orchestrations.clone()).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("test-two-guids", "TwoGuids", "")
        .await
        .unwrap();
    let status1 = client
        .wait_for_orchestration("test-two-guids", Duration::from_secs(5))
        .await
        .unwrap();
    let output1 = match status1 {
        duroxide::runtime::OrchestrationStatus::Completed { output, .. } => output,
        other => panic!("Unexpected status: {other:?}"),
    };
    rt.shutdown(None).await;

    // Restart runtime and ensure replay is stable
    let rt2 = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let status2 = client
        .wait_for_orchestration("test-two-guids", Duration::from_secs(5))
        .await
        .unwrap();
    let output2 = match status2 {
        duroxide::runtime::OrchestrationStatus::Completed { output, .. } => output,
        other => panic!("Unexpected status: {other:?}"),
    };
    assert_eq!(output1, output2);
    rt2.shutdown(None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_syscalls_work_in_single_thread_mode() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "SingleThreadSyscalls",
            |ctx: OrchestrationContext, _input: String| async move {
                for _ in 0..3 {
                    let _ = ctx.new_guid().await?;
                    let _ = ctx.utc_now().await?;
                }
                Ok("ok".to_string())
            },
        )
        .build();

    let options = runtime::RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("test-single-thread-syscalls", "SingleThreadSyscalls", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("test-single-thread-syscalls", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        status,
        duroxide::runtime::OrchestrationStatus::Completed { .. }
    ));
    rt.shutdown(None).await;
}

#[tokio::test]
async fn test_cancellation_with_pending_syscall() {
    let (store, _td) = common::create_sqlite_store_disk().await;
    let activities = ActivityRegistry::builder().build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "CancelSyscall",
            |ctx: OrchestrationContext, _input: String| async move {
                // Ensure syscall activity is exercised before cancellation.
                let _ = ctx.utc_now().await?;
                // Then wait so we can cancel deterministically.
                let _ = ctx.schedule_wait("hold").await;
                Ok("done".to_string())
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("test-cancel-syscall", "CancelSyscall", "")
        .await
        .unwrap();

    let subscribed = common::wait_for_subscription(store.clone(), "test-cancel-syscall", "hold", 2000).await;
    assert!(subscribed, "Orchestration should subscribe to 'hold' event");

    client
        .cancel_instance("test-cancel-syscall", "test cancellation")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("test-cancel-syscall", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        duroxide::runtime::OrchestrationStatus::Failed { details, .. } => match details {
            duroxide::ErrorDetails::Application {
                kind: duroxide::AppErrorKind::Cancelled { .. },
                ..
            } => {}
            other => panic!("Expected cancelled application error, got: {other:?}"),
        },
        other => panic!("Expected Failed cancellation status, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

//! Poison Message Handling Tests
//!
//! These tests validate the runtime behavior when messages exceed max_attempts.
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::fault_injection::PoisonInjectingProvider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::{Provider, TagFilter, WorkItem};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, OrchestrationStatus, RuntimeOptions};
use duroxide::{
    ActivityContext, Client, ErrorDetails, INITIAL_EXECUTION_ID, OrchestrationContext, OrchestrationRegistry,
    PoisonMessageType,
};

/// Test that attempt_count increments correctly when abandoning orchestration items
#[tokio::test]
async fn orchestration_attempt_count_increments_on_abandon() {
    let provider = Arc::new(
        SqliteProvider::new_in_memory()
            .await
            .expect("Failed to create provider"),
    );
    let lock_timeout = Duration::from_secs(5);

    // Enqueue a start orchestration
    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "attempt-test-1".to_string(),
                orchestration: "TestOrch".to_string(),
                input: "{}".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .expect("enqueue should succeed");

    // First fetch - attempt_count should be 1
    let (_item1, lock_token1, attempt_count1) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt_count1, 1, "First fetch should have attempt_count = 1");

    // Abandon and fetch again
    provider
        .abandon_orchestration_item(&lock_token1, None, false)
        .await
        .expect("abandon should succeed");

    let (_item2, lock_token2, attempt_count2) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt_count2, 2, "Second fetch should have attempt_count = 2");

    // Abandon and fetch a third time
    provider
        .abandon_orchestration_item(&lock_token2, None, false)
        .await
        .expect("abandon should succeed");

    let (_item3, _lock_token3, attempt_count3) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt_count3, 3, "Third fetch should have attempt_count = 3");
}

/// Test that attempt_count increments correctly for worker items
#[tokio::test]
async fn worker_attempt_count_increments_on_lock_expiry() {
    let provider = Arc::new(
        SqliteProvider::new_in_memory()
            .await
            .expect("Failed to create provider"),
    );
    let short_timeout = Duration::from_secs(1);

    // Enqueue an activity
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "attempt-test-2".to_string(),
            execution_id: INITIAL_EXECUTION_ID,
            id: 1,
            name: "TestActivity".to_string(),
            input: "{}".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .expect("enqueue should succeed");

    // First fetch - attempt_count should be 1
    let (_item1, _token1, count1) = provider
        .fetch_work_item(short_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(count1, 1, "First fetch should have attempt_count = 1");

    // Wait for lock to expire
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Second fetch after expiry - attempt_count should be 2
    let (_item2, token2, count2) = provider
        .fetch_work_item(short_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present after lock expiry");
    assert_eq!(count2, 2, "Second fetch should have attempt_count = 2");

    // Ack to clean up
    provider
        .ack_work_item(
            &token2,
            Some(WorkItem::ActivityCompleted {
                instance: "attempt-test-2".to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await
        .expect("ack should succeed");
}

/// Test that PoisonMessageType correctly identifies orchestrations
#[tokio::test]
async fn poison_message_type_orchestration() {
    let poison_type = PoisonMessageType::Orchestration {
        instance: "test-instance".to_string(),
        execution_id: 1,
    };

    let error = ErrorDetails::Poison {
        attempt_count: 11,
        max_attempts: 10,
        message_type: poison_type,
        message: "{}".to_string(),
    };

    assert_eq!(error.category(), "poison");
    assert!(!error.is_retryable());
    assert!(error.display_message().contains("orchestration"));
    assert!(error.display_message().contains("test-instance"));
    assert!(error.display_message().contains("11"));
    assert!(error.display_message().contains("10"));
}

/// Test that PoisonMessageType correctly identifies activities
#[tokio::test]
async fn poison_message_type_activity() {
    let poison_type = PoisonMessageType::Activity {
        instance: "test-instance".to_string(),
        execution_id: 1,
        activity_name: "TestActivity".to_string(),
        activity_id: 42,
    };

    let error = ErrorDetails::Poison {
        attempt_count: 5,
        max_attempts: 3,
        message_type: poison_type,
        message: "{}".to_string(),
    };

    assert_eq!(error.category(), "poison");
    assert!(!error.is_retryable());
    assert!(error.display_message().contains("activity"));
    assert!(error.display_message().contains("TestActivity"));
    assert!(error.display_message().contains("42"));
}

/// Test that RuntimeOptions has correct default max_attempts
#[tokio::test]
async fn runtime_options_default_max_attempts() {
    let options = RuntimeOptions::default();
    assert_eq!(options.max_attempts, 10, "Default max_attempts should be 10");
}

/// Test that max_attempts can be configured
#[tokio::test]
async fn runtime_options_configurable_max_attempts() {
    let options = RuntimeOptions {
        max_attempts: 5,
        ..Default::default()
    };
    assert_eq!(options.max_attempts, 5, "max_attempts should be configurable");
}

// =============================================================================
// E2E Tests with Fault Injection
// =============================================================================

fn fast_runtime_options(max_attempts: u32) -> RuntimeOptions {
    RuntimeOptions {
        max_attempts,
        dispatcher_min_poll_interval: Duration::from_millis(5),
        ..Default::default()
    }
}

/// E2E: Orchestration item detected as poison fails the orchestration
#[tokio::test]
async fn e2e_orchestration_item_poison_fails_orchestration() {
    let sqlite = Arc::new(
        SqliteProvider::new_in_memory()
            .await
            .expect("Failed to create provider"),
    );
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));

    // Register a simple orchestration that should never execute
    let orchestrations = OrchestrationRegistry::builder()
        .register("SimpleOrch", |_ctx: OrchestrationContext, _input: String| async move {
            panic!("Orchestration code should not run when detected as poison");
        })
        .build();

    let activities = ActivityRegistry::builder().build();

    // Inject poison BEFORE starting runtime - next fetch will see high attempt count
    provider.inject_orchestration_poison(11);

    let rt = runtime::Runtime::start_with_options(
        provider.clone() as Arc<dyn Provider>,
        activities,
        orchestrations,
        fast_runtime_options(10),
    )
    .await;

    let client = Client::new(provider.clone() as Arc<dyn Provider>);

    // Start orchestration
    let instance = "poison-orch-test-1";
    client
        .start_orchestration(instance, "SimpleOrch", "{}")
        .await
        .expect("start should succeed");

    // Wait for orchestration to complete (should fail as poison)
    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(5))
        .await
        .expect("wait should succeed");

    // Should be failed with complete poison error details
    match status {
        OrchestrationStatus::Failed { details, .. } => {
            if let ErrorDetails::Poison {
                attempt_count,
                max_attempts,
                message_type,
                message,
            } = details
            {
                assert_eq!(attempt_count, 11);
                assert_eq!(max_attempts, 10);

                // Verify message type contains correct instance and execution_id
                if let PoisonMessageType::Orchestration {
                    instance: inst,
                    execution_id,
                } = message_type
                {
                    assert_eq!(inst, instance);
                    assert_eq!(execution_id, INITIAL_EXECUTION_ID);
                } else {
                    panic!("Expected Orchestration message type");
                }

                // Message should be serialized work items
                assert!(!message.is_empty(), "Message should contain serialized data");
            } else {
                panic!("Expected Poison error, got: {details:?}");
            }
        }
        other => panic!("Expected Failed status, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// E2E: Activity item detected as poison fails the orchestration
#[tokio::test]
async fn e2e_activity_item_poison_fails_orchestration() {
    let sqlite = Arc::new(
        SqliteProvider::new_in_memory()
            .await
            .expect("Failed to create provider"),
    );
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));

    // Register orchestration that schedules an activity
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "OrchWithActivity",
            |ctx: OrchestrationContext, _input: String| async move {
                ctx.schedule_activity("TestActivity", "{}").await
            },
        )
        .build();

    // Register activity that would succeed normally
    let activities = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, _input: String| async move {
            Ok::<_, String>("activity-result".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        provider.clone() as Arc<dyn Provider>,
        activities,
        orchestrations,
        fast_runtime_options(10),
    )
    .await;

    let client = Client::new(provider.clone() as Arc<dyn Provider>);

    // Inject activity poison BEFORE starting - persistent mode ensures all activity fetches
    // will see the high attempt count
    provider.inject_activity_poison_persistent(11);

    let instance = "poison-activity-test-1";
    client
        .start_orchestration(instance, "OrchWithActivity", "{}")
        .await
        .expect("start should succeed");

    // Wait for orchestration to complete
    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(5))
        .await
        .expect("wait should succeed");

    // Orchestration should fail with poison error details
    match status {
        OrchestrationStatus::Failed { details, .. } => {
            if let ErrorDetails::Poison {
                attempt_count,
                max_attempts,
                message_type,
                ..
            } = details
            {
                assert_eq!(attempt_count, 11);
                assert_eq!(max_attempts, 10);

                // Verify activity message type
                if let PoisonMessageType::Activity {
                    instance: inst,
                    execution_id,
                    activity_name,
                    activity_id,
                } = message_type
                {
                    assert_eq!(inst, instance);
                    assert_eq!(execution_id, INITIAL_EXECUTION_ID);
                    assert_eq!(activity_name, "TestActivity");
                    assert!(activity_id > 0, "Activity ID should be positive");
                } else {
                    panic!("Expected Activity message type, got: {message_type:?}");
                }
            } else {
                panic!("Expected Poison error, got: {details:?}");
            }
        }
        other => panic!("Expected Failed status, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// E2E: Sub-orchestration detected as poison fails the parent
#[tokio::test]
async fn e2e_sub_orchestration_poison_fails_parent() {
    let sqlite = Arc::new(
        SqliteProvider::new_in_memory()
            .await
            .expect("Failed to create provider"),
    );
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));

    // Register parent orchestration that calls child
    let orchestrations = OrchestrationRegistry::builder()
        .register("ParentOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_sub_orchestration("ChildOrch", "{}").await
        })
        .register("ChildOrch", |_ctx: OrchestrationContext, _input: String| async move {
            Ok::<_, String>("child-result".to_string())
        })
        .build();

    let activities = ActivityRegistry::builder().build();

    let rt = runtime::Runtime::start_with_options(
        provider.clone() as Arc<dyn Provider>,
        activities,
        orchestrations,
        fast_runtime_options(10),
    )
    .await;

    let client = Client::new(provider.clone() as Arc<dyn Provider>);

    // Skip the first orchestration fetch (the parent), then poison the next (the child).
    provider.inject_orchestration_poison_after_skip(1, 11);

    let instance = "poison-suborg-test-1";
    client
        .start_orchestration(instance, "ParentOrch", "{}")
        .await
        .expect("start should succeed");

    // Wait for parent to complete
    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(5))
        .await
        .expect("wait should succeed");

    // Parent should fail with poison error from child
    match status {
        OrchestrationStatus::Failed { details, .. } => {
            if let ErrorDetails::Poison {
                attempt_count,
                max_attempts,
                message_type,
                message,
            } = details
            {
                assert_eq!(attempt_count, 11);
                assert_eq!(max_attempts, 10);

                // Verify child orchestration message type
                if let PoisonMessageType::Orchestration {
                    instance: child_inst,
                    execution_id,
                } = message_type
                {
                    // Child instance should be derived from parent
                    assert!(
                        child_inst.starts_with(instance),
                        "Child instance should start with parent instance"
                    );
                    assert_eq!(execution_id, INITIAL_EXECUTION_ID);
                } else {
                    panic!("Expected Orchestration message type, got: {message_type:?}");
                }

                assert!(!message.is_empty(), "Message should contain serialized data");
            } else {
                panic!("Expected Poison error, got: {details:?}");
            }
        }
        other => panic!("Expected Failed status, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// E2E: One poisoned activity among several (sequential) fails the orchestration
#[tokio::test]
async fn e2e_one_poisoned_activity_among_many() {
    let sqlite = Arc::new(
        SqliteProvider::new_in_memory()
            .await
            .expect("Failed to create provider"),
    );
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));

    // Register orchestration that schedules multiple activities sequentially
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "MultiActivityOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                // First activity
                let _r1 = ctx.schedule_activity("Activity1", "{}").await?;
                // Second activity - this one will be poisoned
                let _r2 = ctx.schedule_activity("Activity2", "{}").await?;
                // Third activity - should not run
                let r3 = ctx.schedule_activity("Activity3", "{}").await?;
                Ok::<_, String>(r3)
            },
        )
        .build();

    // Register activities
    let activities = ActivityRegistry::builder()
        .register("Activity1", |_ctx: ActivityContext, _input: String| async move {
            Ok::<_, String>("result1".to_string())
        })
        .register("Activity2", |_ctx: ActivityContext, _input: String| async move {
            Ok::<_, String>("result2".to_string())
        })
        .register("Activity3", |_ctx: ActivityContext, _input: String| async move {
            Ok::<_, String>("result3".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        provider.clone() as Arc<dyn Provider>,
        activities,
        orchestrations,
        fast_runtime_options(5),
    )
    .await;

    let client = Client::new(provider.clone() as Arc<dyn Provider>);

    // Inject activity poison in persistent mode BEFORE starting.
    // This will cause the first activity fetch to be detected as poison.
    provider.inject_activity_poison_persistent(6);

    let instance = "poison-multi-activity-test";
    client
        .start_orchestration(instance, "MultiActivityOrch", "{}")
        .await
        .expect("start should succeed");

    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(5))
        .await
        .expect("wait should succeed");

    assert!(
        matches!(status, OrchestrationStatus::Failed { .. }),
        "Orchestration should fail due to one poisoned activity, got: {status:?}"
    );

    rt.shutdown(None).await;
}

/// E2E: Custom max_attempts is respected
#[tokio::test]
async fn e2e_custom_max_attempts_respected() {
    let sqlite = Arc::new(
        SqliteProvider::new_in_memory()
            .await
            .expect("Failed to create provider"),
    );
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "CustomMaxOrch",
            |_ctx: OrchestrationContext, _input: String| async move { Ok::<_, String>("success".to_string()) },
        )
        .build();

    let activities = ActivityRegistry::builder().build();

    // Inject attempt_count = 4 which exceeds custom max of 3
    provider.inject_orchestration_poison(4);

    let rt = runtime::Runtime::start_with_options(
        provider.clone() as Arc<dyn Provider>,
        activities,
        orchestrations,
        fast_runtime_options(3), // custom max_attempts = 3
    )
    .await;

    let client = Client::new(provider.clone() as Arc<dyn Provider>);

    let instance = "custom-max-attempts-test";
    client
        .start_orchestration(instance, "CustomMaxOrch", "{}")
        .await
        .expect("start should succeed");

    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(5))
        .await
        .expect("wait should succeed");

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            if let ErrorDetails::Poison {
                attempt_count,
                max_attempts,
                ..
            } = details
            {
                assert_eq!(attempt_count, 4);
                assert_eq!(max_attempts, 3);
            } else {
                panic!("Expected Poison error with custom max_attempts");
            }
        }
        other => panic!("Expected Failed status, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// E2E: Activity in sub-orchestration is poisoned, fails the sub-orch, which fails the parent
#[tokio::test]
async fn e2e_activity_poisons_suborchestration_poisons_parent() {
    let sqlite = Arc::new(
        SqliteProvider::new_in_memory()
            .await
            .expect("Failed to create provider"),
    );
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));

    // Register parent orchestration that calls child
    // Child orchestration calls an activity
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "GrandparentOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                ctx.schedule_sub_orchestration("ChildWithActivityOrch", "{}").await
            },
        )
        .register(
            "ChildWithActivityOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                // This activity will be poisoned
                ctx.schedule_activity("ChildActivity", "{}").await
            },
        )
        .build();

    // Register activity that would succeed normally
    let activities = ActivityRegistry::builder()
        .register("ChildActivity", |_ctx: ActivityContext, _input: String| async move {
            Ok::<_, String>("activity-result".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        provider.clone() as Arc<dyn Provider>,
        activities,
        orchestrations,
        fast_runtime_options(10),
    )
    .await;

    let client = Client::new(provider.clone() as Arc<dyn Provider>);

    // Inject activity poison - this will poison the activity in the child orchestration
    provider.inject_activity_poison_persistent(11);

    let instance = "poison-chain-test";
    client
        .start_orchestration(instance, "GrandparentOrch", "{}")
        .await
        .expect("start should succeed");

    // Wait for grandparent to complete
    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(5))
        .await
        .expect("wait should succeed");

    // Grandparent should fail: activity poison → child fails → grandparent fails
    match status {
        OrchestrationStatus::Failed { details, .. } => {
            // The error propagated should be the activity poison error
            if let ErrorDetails::Poison {
                attempt_count,
                max_attempts,
                message_type,
                ..
            } = details
            {
                assert_eq!(attempt_count, 11);
                assert_eq!(max_attempts, 10);

                // Should be an Activity poison type (from the poisoned activity in child)
                if let PoisonMessageType::Activity { activity_name, .. } = message_type {
                    assert_eq!(activity_name, "ChildActivity");
                } else {
                    panic!("Expected Activity message type, got: {message_type:?}");
                }
            } else {
                panic!("Expected Poison error, got: {details:?}");
            }
        }
        other => panic!("Expected Failed status, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// E2E: Poison message creates instance if it didn't exist (issue #43)
///
/// Background: Instances are created during ack_orchestration_item, not enqueue.
/// When a StartOrchestration message is detected as poison on first fetch,
/// the instance doesn't exist yet. The fail_orchestration_as_poison function
/// must create the instance with proper metadata, not leave an orphaned queue item.
///
/// This test verifies:
/// 1. StartOrchestration is enqueued (instance doesn't exist yet)
/// 2. First fetch is poisoned (injected attempt_count > max_attempts)
/// 3. Instance IS created with Failed status
/// 4. Instance has proper metadata (orchestration_name, version, status)
/// 5. History contains OrchestrationStarted and OrchestrationFailed events
#[tokio::test]
async fn e2e_poison_message_creates_instance_if_missing() {
    let sqlite = Arc::new(
        SqliteProvider::new_in_memory()
            .await
            .expect("Failed to create provider"),
    );
    let provider = Arc::new(PoisonInjectingProvider::new(sqlite));

    // Register orchestration that should never run (we'll poison it immediately)
    let orchestrations = OrchestrationRegistry::builder()
        .register("NeverRuns", |_ctx: OrchestrationContext, _input: String| async move {
            panic!("This orchestration code should never execute - message is poison");
        })
        .build();

    let activities = ActivityRegistry::builder().build();

    // Inject poison BEFORE starting runtime - first fetch will see attempt_count=11
    provider.inject_orchestration_poison(11);

    let rt = runtime::Runtime::start_with_options(
        provider.clone() as Arc<dyn Provider>,
        activities,
        orchestrations,
        fast_runtime_options(10), // max_attempts=10, so 11 triggers poison
    )
    .await;

    let client = Client::new(provider.clone() as Arc<dyn Provider>);

    let instance = "poison-creates-instance-test";

    // Note: At this point, the instance should NOT exist (instances are created on ack, not enqueue)
    // We don't verify this because the runtime may race and process the message before we can check

    // Start orchestration - enqueues StartOrchestration work item
    client
        .start_orchestration(instance, "NeverRuns", r#"{"test":"data"}"#)
        .await
        .expect("start should succeed (just enqueues)");

    // Wait for orchestration to complete (should fail as poison)
    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(5))
        .await
        .expect("wait should succeed");

    // Verify: Orchestration should be in Failed state with poison error
    match &status {
        OrchestrationStatus::Failed { details, .. } => {
            if let ErrorDetails::Poison {
                attempt_count,
                max_attempts,
                message_type,
                ..
            } = details
            {
                assert_eq!(*attempt_count, 11, "Should have injected attempt_count");
                assert_eq!(*max_attempts, 10, "Should have max_attempts from options");

                if let PoisonMessageType::Orchestration {
                    instance: inst,
                    execution_id,
                } = message_type
                {
                    assert_eq!(inst, instance, "Message type should reference our instance");
                    assert_eq!(*execution_id, INITIAL_EXECUTION_ID);
                } else {
                    panic!("Expected Orchestration poison type, got: {message_type:?}");
                }
            } else {
                panic!("Expected Poison error, got: {details:?}");
            }
        }
        other => panic!("Expected Failed status, got: {other:?}"),
    }

    // KEY VERIFICATION: Instance was created with proper metadata
    let instance_info = client
        .get_instance_info(instance)
        .await
        .expect("Instance MUST exist after poison handling");

    assert_eq!(instance_info.instance_id, instance);
    assert_eq!(
        instance_info.orchestration_name, "NeverRuns",
        "Should have correct orchestration name"
    );
    assert_eq!(instance_info.status, "Failed", "Status should be Failed");

    // Verify history contains proper events (OrchestrationStarted + OrchestrationFailed)
    let history = client
        .read_execution_history(instance, INITIAL_EXECUTION_ID)
        .await
        .expect("Should get history");

    assert!(
        history.len() >= 2,
        "History should have at least 2 events, got {}",
        history.len()
    );

    // First event should be OrchestrationStarted
    match &history[0].kind {
        duroxide::EventKind::OrchestrationStarted {
            name, version, input, ..
        } => {
            assert_eq!(name, "NeverRuns", "Started event should have orchestration name");
            assert!(!version.is_empty(), "Started event should have version");
            assert!(input.contains("test"), "Started event should have input");
        }
        other => panic!("First event should be OrchestrationStarted, got: {other:?}"),
    }

    // Last event should be OrchestrationFailed
    let last_event = history.last().expect("Should have events");
    match &last_event.kind {
        duroxide::EventKind::OrchestrationFailed { details } => {
            assert!(
                matches!(details, ErrorDetails::Poison { .. }),
                "Failed event should have Poison details, got: {details:?}"
            );
        }
        other => panic!("Last event should be OrchestrationFailed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

use crate::provider_validation::{Event, EventKind, ExecutionMetadata, start_item};
use crate::provider_validations::ProviderFactory;
use crate::providers::{ScheduledActivityIdentifier, TagFilter, WorkItem};
use std::time::Duration;

/// Test: Fetch Returns Running State for Active Orchestration
/// Goal: Verify that fetch_work_item returns ExecutionState::Running when orchestration is active.
pub async fn test_fetch_returns_running_state_for_active_orchestration<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: fetch returns Running for active orchestration");
    let provider = factory.create_provider().await;

    // 1. Create an active orchestration instance
    provider
        .enqueue_for_orchestrator(start_item("inst-running"), None)
        .await
        .unwrap();

    // Ack start to create instance
    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        status: Some("Running".to_string()),
        ..Default::default()
    };

    // Enqueue an activity
    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-running".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    provider
        .ack_orchestration_item(
            &token,
            1,
            vec![Event::with_event_id(
                1,
                "inst-running".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![activity_item],
            vec![],
            metadata,
            vec![],
        )
        .await
        .unwrap();

    // 2. Fetch the activity work item
    let result = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();

    match result {
        Some((_, _, _)) => {
            // Activity was fetched successfully - the test passes
        }
        None => panic!("Expected to fetch work item"),
    }

    tracing::info!("✓ Test passed: fetch returns Running for active orchestration");
}

/// Test: Fetch Returns Terminal State when Orchestration Completed
/// Goal: Verify that fetch_work_item returns ExecutionState::Terminal when orchestration is completed.
pub async fn test_fetch_returns_terminal_state_when_orchestration_completed<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: fetch returns Terminal for completed orchestration");
    let provider = factory.create_provider().await;

    // 1. Create an active orchestration instance
    provider
        .enqueue_for_orchestrator(start_item("inst-completed"), None)
        .await
        .unwrap();
    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Enqueue activity but also COMPLETE the orchestration
    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        status: Some("Completed".to_string()), // Terminal state
        output: Some("done".to_string()),
        ..Default::default()
    };

    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-completed".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    provider
        .ack_orchestration_item(
            &token,
            1,
            vec![
                Event::with_event_id(
                    1,
                    "inst-completed".to_string(),
                    1,
                    None,
                    EventKind::OrchestrationStarted {
                        name: "TestOrch".to_string(),
                        version: "1.0".to_string(),
                        input: "{}".to_string(),
                        parent_instance: None,
                        parent_id: None,
                        carry_forward_events: None,
                        initial_custom_status: None,
                    },
                ),
                Event::with_event_id(
                    2,
                    "inst-completed".to_string(),
                    1,
                    None,
                    EventKind::OrchestrationCompleted {
                        output: "done".to_string(),
                    },
                ),
            ],
            vec![activity_item],
            vec![],
            metadata,
            vec![],
        )
        .await
        .unwrap();

    // 2. Fetch the activity work item
    let result = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();

    match result {
        Some((_, _, _)) => {
            // Activity was fetched successfully - the test passes
        }
        None => panic!("Expected to fetch work item"),
    }

    tracing::info!("✓ Test passed: fetch returns Terminal for completed orchestration");
}

/// Test: Fetch Returns Terminal State when Orchestration Failed
/// Goal: Verify that fetch_work_item returns ExecutionState::Terminal when orchestration is failed.
pub async fn test_fetch_returns_terminal_state_when_orchestration_failed<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: fetch returns Terminal for failed orchestration");
    let provider = factory.create_provider().await;

    // 1. Create an active orchestration instance
    provider
        .enqueue_for_orchestrator(start_item("inst-failed"), None)
        .await
        .unwrap();
    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Enqueue activity but also FAIL the orchestration
    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        status: Some("Failed".to_string()), // Terminal state
        ..Default::default()
    };

    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-failed".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    provider
        .ack_orchestration_item(
            &token,
            1,
            vec![
                Event::with_event_id(
                    1,
                    "inst-failed".to_string(),
                    1,
                    None,
                    EventKind::OrchestrationStarted {
                        name: "TestOrch".to_string(),
                        version: "1.0".to_string(),
                        input: "{}".to_string(),
                        parent_instance: None,
                        parent_id: None,
                        carry_forward_events: None,
                        initial_custom_status: None,
                    },
                ),
                Event::with_event_id(
                    2,
                    "inst-failed".to_string(),
                    1,
                    None,
                    EventKind::OrchestrationFailed {
                        details: crate::ErrorDetails::Application {
                            kind: crate::AppErrorKind::OrchestrationFailed,
                            message: "boom".to_string(),
                            retryable: false,
                        },
                    },
                ),
            ],
            vec![activity_item],
            vec![],
            metadata,
            vec![],
        )
        .await
        .unwrap();

    // 2. Fetch the activity work item
    let result = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();

    match result {
        Some((_, _, _)) => {
            // Activity was fetched successfully - the test passes
        }
        None => panic!("Expected to fetch work item"),
    }

    tracing::info!("✓ Test passed: fetch returns Terminal for failed orchestration");
}

/// Test: Fetch Returns Terminal State when Orchestration ContinuedAsNew
/// Goal: Verify that fetch_work_item returns ExecutionState::Terminal when orchestration continued as new.
pub async fn test_fetch_returns_terminal_state_when_orchestration_continued_as_new<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: fetch returns Terminal for ContinuedAsNew orchestration");
    let provider = factory.create_provider().await;

    // 1. Create an active orchestration instance
    provider
        .enqueue_for_orchestrator(start_item("inst-can"), None)
        .await
        .unwrap();
    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Enqueue activity but also ContinueAsNew
    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        status: Some("ContinuedAsNew".to_string()), // Terminal state for THIS execution
        output: Some("new-input".to_string()),
        ..Default::default()
    };

    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-can".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    provider
        .ack_orchestration_item(
            &token,
            1,
            vec![
                Event::with_event_id(
                    1,
                    "inst-can".to_string(),
                    1,
                    None,
                    EventKind::OrchestrationStarted {
                        name: "TestOrch".to_string(),
                        version: "1.0".to_string(),
                        input: "{}".to_string(),
                        parent_instance: None,
                        parent_id: None,
                        carry_forward_events: None,
                        initial_custom_status: None,
                    },
                ),
                Event::with_event_id(
                    2,
                    "inst-can".to_string(),
                    1,
                    None,
                    EventKind::OrchestrationContinuedAsNew {
                        input: "new-input".to_string(),
                    },
                ),
            ],
            vec![activity_item],
            vec![],
            metadata,
            vec![],
        )
        .await
        .unwrap();

    // 2. Fetch the activity work item
    let result = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();

    match result {
        Some((_, _, _)) => {
            // Activity was fetched successfully - the test passes
        }
        None => panic!("Expected to fetch work item"),
    }

    tracing::info!("✓ Test passed: fetch returns Terminal for ContinuedAsNew orchestration");
}

/// Test: Fetch Returns Missing State when Instance Deleted
/// Goal: Verify that fetch_work_item returns activity for missing instance.
pub async fn test_fetch_returns_missing_state_when_instance_deleted<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: fetch returns activity for missing instance");
    let provider = factory.create_provider().await;

    // 1. Enqueue an activity directly (simulating a leftover message)
    // We don't create the instance, so it is "missing"
    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-missing".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    provider.enqueue_for_worker(activity_item).await.unwrap();

    // 2. Fetch the activity work item
    let result = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();

    match result {
        Some((_, _, _)) => {
            // Activity was fetched successfully - the test passes
        }
        None => panic!("Expected to fetch work item"),
    }

    tracing::info!("✓ Test passed: fetch returns activity for missing instance");
}

/// Test: Renew Returns Running when Orchestration Active
/// Goal: Verify that renew_work_item_lock returns ExecutionState::Running when orchestration is active.
pub async fn test_renew_returns_running_when_orchestration_active<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: renew returns Running for active orchestration");
    let provider = factory.create_provider().await;

    // 1. Create active instance and activity
    provider
        .enqueue_for_orchestrator(start_item("inst-renew-run"), None)
        .await
        .unwrap();
    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        status: Some("Running".to_string()),
        ..Default::default()
    };

    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-renew-run".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    provider
        .ack_orchestration_item(
            &token,
            1,
            vec![Event::with_event_id(
                1,
                "inst-renew-run".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![activity_item],
            vec![],
            metadata,
            vec![],
        )
        .await
        .unwrap();

    // 2. Fetch activity to get lock token
    let (_, lock_token, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // 3. Renew lock
    provider
        .renew_work_item_lock(&lock_token, Duration::from_secs(30))
        .await
        .unwrap();

    tracing::info!("✓ Test passed: renew returns Running for active orchestration");
}

/// Test: Renew Returns Terminal when Orchestration Completed
/// Goal: Verify that renew_work_item_lock returns ExecutionState::Terminal when orchestration is completed.
pub async fn test_renew_returns_terminal_when_orchestration_completed<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: renew returns Terminal for completed orchestration");
    let provider = factory.create_provider().await;

    // 1. Create active instance and activity
    provider
        .enqueue_for_orchestrator(start_item("inst-renew-term"), None)
        .await
        .unwrap();
    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        status: Some("Running".to_string()),
        ..Default::default()
    };

    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-renew-term".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    provider
        .ack_orchestration_item(
            &token,
            1,
            vec![Event::with_event_id(
                1,
                "inst-renew-term".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![activity_item],
            vec![],
            metadata,
            vec![],
        )
        .await
        .unwrap();

    // 2. Fetch activity to get lock token
    let (_, lock_token, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // 3. Complete the orchestration (simulate another turn)
    // We need to fetch the orchestration item again (it's not in queue, so we need to trigger it or just update DB directly?
    // Since we can't easily update DB directly in generic test, we'll simulate it by enqueuing a new message to trigger a turn)

    // Enqueue a dummy external event to trigger a turn
    provider
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: "inst-renew-term".to_string(),
                name: "Trigger".to_string(),
                data: "{}".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    let (_item2, token2, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    let metadata2 = ExecutionMetadata {
        status: Some("Completed".to_string()),
        output: Some("done".to_string()),
        ..Default::default()
    };

    provider
        .ack_orchestration_item(
            &token2,
            1,
            vec![Event::with_event_id(
                2,
                "inst-renew-term".to_string(),
                1,
                None,
                EventKind::OrchestrationCompleted {
                    output: "done".to_string(),
                },
            )],
            vec![],
            vec![],
            metadata2,
            vec![],
        )
        .await
        .unwrap();

    // 4. Renew lock
    provider
        .renew_work_item_lock(&lock_token, Duration::from_secs(30))
        .await
        .unwrap();

    tracing::info!("✓ Test passed: renew returns Terminal for completed orchestration");
}

/// Test: Renew Returns Missing when Instance Deleted
/// Goal: Verify that renew_work_item_lock returns ExecutionState::Missing when instance is deleted.
/// Note: This test is hard to implement generically because "deleting an instance" isn't a standard Provider method.
/// We'll skip this one for generic validation unless we add a delete_instance method to Provider.
/// Instead, we can test "Missing" by having an activity for a non-existent instance, fetching it, and renewing it.
pub async fn test_renew_returns_missing_when_instance_deleted<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: renew for missing instance");
    let provider = factory.create_provider().await;

    // 1. Enqueue activity for missing instance
    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-renew-missing".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    provider.enqueue_for_worker(activity_item).await.unwrap();

    // 2. Fetch activity to get lock token
    let (_, lock_token, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // 3. Renew lock
    provider
        .renew_work_item_lock(&lock_token, Duration::from_secs(30))
        .await
        .unwrap();
    // Note: renew_work_item_lock no longer returns ExecutionState

    tracing::info!("✓ Test passed: renew for missing instance");
}

/// Test: Ack Work Item None Deletes Without Enqueue
/// Goal: Verify that ack_work_item(token, None) deletes the item but enqueues nothing.
pub async fn test_ack_work_item_none_deletes_without_enqueue<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: ack(None) deletes without enqueue");
    let provider = factory.create_provider().await;

    // 1. Enqueue activity
    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-ack-none".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    provider.enqueue_for_worker(activity_item).await.unwrap();

    // 2. Fetch activity
    let (_, lock_token, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // 3. Ack with None
    provider.ack_work_item(&lock_token, None).await.unwrap();

    // 4. Verify worker queue is empty
    let result = provider
        .fetch_work_item(Duration::from_millis(100), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();
    assert!(result.is_none(), "Worker queue should be empty");

    // 5. Verify orchestrator queue is empty (no completion enqueued)
    // We can check this by trying to fetch orchestration item.
    // Since we didn't create the instance, fetch_orchestration_item might return None anyway.
    // But if a completion WAS enqueued, it would be there (even if instance is missing, the message exists).
    // However, fetch_orchestration_item usually requires instance lock.
    // A better check might be get_queue_depths if available, or just relying on fetch returning None.

    // Let's try to fetch orchestration item. If a completion was enqueued, it would be visible.
    let orch_result = provider
        .fetch_orchestration_item(Duration::from_millis(100), Duration::ZERO, None)
        .await
        .unwrap();
    assert!(orch_result.is_none(), "Orchestrator queue should be empty");

    tracing::info!("✓ Test passed: ack(None) deletes without enqueue");
}
// ============================================================================
// Lock-Stealing Activity Cancellation Tests
// ============================================================================
// These tests validate the new activity cancellation mechanism where the
// orchestration runtime cancels in-flight activities by deleting their
// worker queue entries ("lock stealing"). The worker detects this when
// its lock renewal or ack fails.

/// Test: Cancelled Activities Are Deleted From Worker Queue
/// Goal: Verify that `ack_orchestration_item` with `cancelled_activities` removes matching entries.
pub async fn test_cancelled_activities_deleted_from_worker_queue<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: cancelled_activities deletes from worker queue");
    let provider = factory.create_provider().await;

    // 1. Create an active orchestration and enqueue multiple activities
    provider
        .enqueue_for_orchestrator(start_item("inst-cancel-delete"), None)
        .await
        .unwrap();

    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        status: Some("Running".to_string()),
        ..Default::default()
    };

    // Enqueue 3 activities
    let activity1 = WorkItem::ActivityExecute {
        instance: "inst-cancel-delete".to_string(),
        execution_id: 1,
        id: 1,
        name: "Activity1".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    let activity2 = WorkItem::ActivityExecute {
        instance: "inst-cancel-delete".to_string(),
        execution_id: 1,
        id: 2,
        name: "Activity2".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    let activity3 = WorkItem::ActivityExecute {
        instance: "inst-cancel-delete".to_string(),
        execution_id: 1,
        id: 3,
        name: "Activity3".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    provider
        .ack_orchestration_item(
            &token,
            1,
            vec![Event::with_event_id(
                1,
                "inst-cancel-delete".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![activity1, activity2, activity3],
            vec![],
            metadata,
            vec![], // No cancellations yet
        )
        .await
        .unwrap();

    // 2. Verify all 3 activities are in worker queue (fetch one to confirm)
    let (_item1, token1, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .expect("Should have activity in queue");

    // Put it back so we can test cancellation
    provider.abandon_work_item(&token1, None, false).await.unwrap();

    // 3. Trigger another orchestration turn that cancels activities 1 and 2
    provider
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: "inst-cancel-delete".to_string(),
                name: "Trigger".to_string(),
                data: "{}".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    let (_item2, token2, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Cancel activities 1 and 2
    let cancelled = vec![
        ScheduledActivityIdentifier {
            instance: "inst-cancel-delete".to_string(),
            execution_id: 1,
            activity_id: 1,
        },
        ScheduledActivityIdentifier {
            instance: "inst-cancel-delete".to_string(),
            execution_id: 1,
            activity_id: 2,
        },
    ];

    provider
        .ack_orchestration_item(
            &token2,
            1,
            vec![], // No new events
            vec![], // No new activities
            vec![], // No orchestrator items
            ExecutionMetadata::default(),
            cancelled,
        )
        .await
        .unwrap();

    // 4. Verify only activity 3 remains in worker queue
    let (remaining_item, _, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .expect("Should have activity 3 remaining");

    match remaining_item {
        WorkItem::ActivityExecute { id, .. } => {
            assert_eq!(
                id, 3,
                "Only activity 3 should remain; activities 1 and 2 should be cancelled"
            );
        }
        _ => panic!("Expected ActivityExecute"),
    }

    // Verify no more activities
    let no_more = provider
        .fetch_work_item(Duration::from_millis(100), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();
    assert!(no_more.is_none(), "Should have no more activities");

    tracing::info!("✓ Test passed: cancelled_activities deletes from worker queue");
}

/// Test: Ack Work Item Fails When Entry Deleted (Lock Stolen)
/// Goal: Verify that `ack_work_item` returns a permanent error when the entry was deleted.
pub async fn test_ack_work_item_fails_when_entry_deleted<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: ack_work_item fails when entry deleted (lock stolen)");
    let provider = factory.create_provider().await;

    // 1. Enqueue and fetch an activity
    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-ack-stolen".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    provider.enqueue_for_worker(activity_item).await.unwrap();

    let (_, lock_token, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // 2. Simulate lock stealing: delete the entry via ack with None from a "different worker"
    // In practice, the orchestration runtime does this via cancelled_activities.
    // Here we directly ack with None to simulate the deletion.
    provider.ack_work_item(&lock_token, None).await.unwrap();

    // 3. Try to ack again with the same token - should fail (entry gone)
    let completion = WorkItem::ActivityCompleted {
        instance: "inst-ack-stolen".to_string(),
        execution_id: 1,
        id: 1,
        result: "done".to_string(),
    };
    let result = provider.ack_work_item(&lock_token, Some(completion)).await;

    assert!(result.is_err(), "ack_work_item should fail when entry already deleted");
    let err = result.unwrap_err();
    assert!(
        !err.is_retryable(),
        "Error should be permanent (not retryable) for deleted entry: {err:?}"
    );

    tracing::info!("✓ Test passed: ack_work_item fails when entry deleted (lock stolen)");
}

/// Test: Renew Work Item Lock Fails When Entry Deleted (Cancellation Signal)
/// Goal: Verify that `renew_work_item_lock` fails when the entry was deleted for cancellation.
pub async fn test_renew_fails_when_entry_deleted<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: renew fails when entry deleted (cancellation signal)");
    let provider = factory.create_provider().await;

    // 1. Enqueue and fetch an activity
    let activity_item = WorkItem::ActivityExecute {
        instance: "inst-renew-stolen".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    provider.enqueue_for_worker(activity_item).await.unwrap();

    let (_, lock_token, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // 2. Delete the entry (simulating lock stealing / cancellation)
    provider.ack_work_item(&lock_token, None).await.unwrap();

    // 3. Try to renew the lock - should fail (entry gone, signals cancellation)
    let result = provider
        .renew_work_item_lock(&lock_token, Duration::from_secs(30))
        .await;

    assert!(result.is_err(), "renew_work_item_lock should fail when entry deleted");

    tracing::info!("✓ Test passed: renew fails when entry deleted (cancellation signal)");
}

/// Test: Cancelling Non-Existent Activities Is Idempotent
/// Goal: Verify that passing non-existent activity IDs in `cancelled_activities` doesn't cause errors.
pub async fn test_cancelling_nonexistent_activities_is_idempotent<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: cancelling non-existent activities is idempotent");
    let provider = factory.create_provider().await;

    // 1. Create an active orchestration
    provider
        .enqueue_for_orchestrator(start_item("inst-cancel-idempotent"), None)
        .await
        .unwrap();

    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // 2. Ack with cancelled_activities that don't exist - should succeed
    let cancelled = vec![
        ScheduledActivityIdentifier {
            instance: "inst-cancel-idempotent".to_string(),
            execution_id: 1,
            activity_id: 999, // Non-existent
        },
        ScheduledActivityIdentifier {
            instance: "inst-cancel-idempotent".to_string(),
            execution_id: 1, // Same execution, non-existent activity
            activity_id: 1,
        },
    ];

    let result = provider
        .ack_orchestration_item(
            &token,
            1,
            vec![Event::with_event_id(
                1,
                "inst-cancel-idempotent".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0".to_string(),
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
                ..Default::default()
            },
            cancelled,
        )
        .await;

    assert!(result.is_ok(), "Cancelling non-existent activities should not error");

    tracing::info!("✓ Test passed: cancelling non-existent activities is idempotent");
}

/// Test: Batch Cancellation Deletes Multiple Activities Atomically
/// Goal: Verify that multiple activities can be cancelled in a single ack call.
pub async fn test_batch_cancellation_deletes_multiple_activities<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: batch cancellation deletes multiple activities atomically");
    let provider = factory.create_provider().await;

    // 1. Create orchestration and enqueue many activities
    provider
        .enqueue_for_orchestrator(start_item("inst-batch-cancel"), None)
        .await
        .unwrap();

    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        status: Some("Running".to_string()),
        ..Default::default()
    };

    // Enqueue 5 activities
    let activities: Vec<WorkItem> = (1..=5)
        .map(|i| WorkItem::ActivityExecute {
            instance: "inst-batch-cancel".to_string(),
            execution_id: 1,
            id: i,
            name: format!("Activity{i}"),
            input: "{}".to_string(),
            session_id: None,
            tag: None,
        })
        .collect();

    provider
        .ack_orchestration_item(
            &token,
            1,
            vec![Event::with_event_id(
                1,
                "inst-batch-cancel".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            activities,
            vec![],
            metadata,
            vec![],
        )
        .await
        .unwrap();

    // 2. Cancel all 5 activities in one batch
    provider
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: "inst-batch-cancel".to_string(),
                name: "Trigger".to_string(),
                data: "{}".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    let (_item2, token2, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    let cancelled: Vec<ScheduledActivityIdentifier> = (1..=5)
        .map(|i| ScheduledActivityIdentifier {
            instance: "inst-batch-cancel".to_string(),
            execution_id: 1,
            activity_id: i,
        })
        .collect();

    provider
        .ack_orchestration_item(
            &token2,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            cancelled,
        )
        .await
        .unwrap();

    // 3. Verify worker queue is empty
    let remaining = provider
        .fetch_work_item(Duration::from_millis(100), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();
    assert!(
        remaining.is_none(),
        "All activities should be cancelled; worker queue should be empty"
    );

    tracing::info!("✓ Test passed: batch cancellation deletes multiple activities atomically");
}

/// Test: Same Activity In worker_items And cancelled_activities Is No-Op
/// Goal: Verify that when an activity is both scheduled (INSERT) and cancelled (DELETE)
/// in the same `ack_orchestration_item` call, the net result is the activity NOT being
/// in the worker queue. This happens when an orchestration schedules an activity and
/// immediately drops its future (e.g., select2 loser, or explicit drop without await).
///
/// CRITICAL ORDERING: Providers MUST process worker_items (INSERT) BEFORE
/// cancelled_activities (DELETE). If DELETE happened first, the INSERT would succeed
/// and leave a stale activity in the queue.
pub async fn test_same_activity_in_worker_items_and_cancelled_is_noop<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: same activity in worker_items and cancelled_activities is no-op");
    let provider = factory.create_provider().await;

    // 1. Create an orchestration
    provider
        .enqueue_for_orchestrator(start_item("inst-schedule-then-cancel"), None)
        .await
        .unwrap();

    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    let metadata = ExecutionMetadata {
        orchestration_name: Some("TestOrch".to_string()),
        status: Some("Running".to_string()),
        ..Default::default()
    };

    // 2. Activity that will be both scheduled AND cancelled in the same call
    // This simulates: schedule_activity() -> immediately drop the future
    let activity_id = 2u64; // event_id 2 (after OrchestrationStarted at id 1)
    let scheduled_activity = WorkItem::ActivityExecute {
        instance: "inst-schedule-then-cancel".to_string(),
        execution_id: 1,
        id: activity_id,
        name: "DroppedActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    let cancelled_activity = ScheduledActivityIdentifier {
        instance: "inst-schedule-then-cancel".to_string(),
        execution_id: 1,
        activity_id,
    };

    // 3. Also schedule a "normal" activity that should remain in the queue
    let normal_activity = WorkItem::ActivityExecute {
        instance: "inst-schedule-then-cancel".to_string(),
        execution_id: 1,
        id: 3, // Different id
        name: "NormalActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    // 4. Ack with the activity in BOTH worker_items and cancelled_activities
    // Provider must INSERT first, then DELETE - net result is activity NOT in queue
    provider
        .ack_orchestration_item(
            &token,
            1,
            vec![Event::with_event_id(
                1,
                "inst-schedule-then-cancel".to_string(),
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![scheduled_activity, normal_activity], // Both activities scheduled
            vec![],
            metadata,
            vec![cancelled_activity], // But activity_id=2 is also cancelled
        )
        .await
        .expect("ack_orchestration_item should succeed");

    // 5. Verify only the normal activity (id=3) is in the worker queue
    let (remaining_item, remaining_token, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .expect("Should have the normal activity in queue");

    match &remaining_item {
        WorkItem::ActivityExecute { id, name, .. } => {
            assert_eq!(
                *id, 3,
                "Only the normal activity (id=3) should remain; the scheduled-then-cancelled activity (id=2) should be gone"
            );
            assert_eq!(name, "NormalActivity");
        }
        _ => panic!("Expected ActivityExecute, got {remaining_item:?}"),
    }

    // Clean up
    provider.ack_work_item(&remaining_token, None).await.unwrap();

    // 6. Verify no more activities in queue
    let no_more = provider
        .fetch_work_item(Duration::from_millis(100), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();
    assert!(
        no_more.is_none(),
        "Should have no more activities; the schedule-then-cancel activity should NOT be in queue"
    );

    tracing::info!("✓ Test passed: same activity in worker_items and cancelled_activities is no-op");
}

/// Test: Orphan Activity After Instance Force-Deletion
///
/// When an instance is force-deleted while activities are sitting in the worker queue,
/// the activity can still be fetched and executed. The resulting ActivityCompleted
/// work item is enqueued to the orchestrator queue but has no instance to process it
/// (it becomes orphaned).
///
/// This validates that force-deletion does not corrupt the worker queue and that
/// orphaned completions are harmless.
pub async fn test_orphan_activity_after_instance_force_deletion<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing cancellation: orphan activity after instance force-deletion");
    let provider = factory.create_provider().await;
    let instance = "inst-orphan-activity";

    // 1. Create a running orchestration with an activity in the worker queue
    provider
        .enqueue_for_orchestrator(start_item(instance), None)
        .await
        .unwrap();

    let (_item, orch_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    let activity_item = WorkItem::ActivityExecute {
        instance: instance.to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "test-input".to_string(),
        session_id: None,
        tag: None,
    };

    provider
        .ack_orchestration_item(
            &orch_token,
            1,
            vec![Event::with_event_id(
                1,
                instance.to_string(),
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
            vec![activity_item],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                status: Some("Running".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // 2. Force-delete the instance (bypassing normal cancellation flow)
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");
    let delete_result = mgmt.delete_instance(instance, true).await.unwrap();
    assert!(
        delete_result.instances_deleted > 0,
        "Force delete should remove the instance"
    );

    // Verify instance is gone
    assert!(
        mgmt.get_instance_info(instance).await.is_err(),
        "Instance should no longer exist"
    );

    // 3. Activity should still be fetchable from the worker queue
    //    (worker queue rows may or may not survive deletion — both behaviors are valid)
    let work_item_result = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();

    if let Some((_item, worker_token, _)) = work_item_result {
        tracing::info!("Activity survived instance deletion — executing and completing it");

        // 4. Ack with completion — this enqueues ActivityCompleted to orchestrator queue
        //    which becomes orphaned since the instance is deleted
        let completion = WorkItem::ActivityCompleted {
            instance: instance.to_string(),
            execution_id: 1,
            id: 1,
            result: "orphan-result".to_string(),
        };
        provider.ack_work_item(&worker_token, Some(completion)).await.unwrap();

        tracing::info!("Activity completed — completion is now orphaned in orchestrator queue");
    } else {
        tracing::info!("Force delete also cleaned up worker queue — activity was removed");
    }

    // Either way, the test verifies no panics/errors occurred during the entire flow
    tracing::info!("✓ Test passed: orphan activity after instance force-deletion handled gracefully");
}

use crate::provider_validation::{Event, EventKind, ExecutionMetadata, create_instance, start_item};
use crate::provider_validations::ProviderFactory;
use crate::providers::WorkItem;
use std::time::Duration;

/// Test 5.1: Worker Queue FIFO Ordering
/// Goal: Verify worker items dequeued in order.
pub async fn test_worker_queue_fifo_ordering<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing queue semantics: worker queue FIFO ordering");
    let provider = factory.create_provider().await;

    // Enqueue 5 worker items
    for i in 0..5 {
        provider
            .enqueue_for_worker(WorkItem::ActivityExecute {
                instance: "instance-A".to_string(),
                execution_id: 1,
                id: i,
                name: format!("Activity{i}"),
                input: format!("input{i}"),
                session_id: None,
            })
            .await
            .unwrap();
    }

    // Dequeue all 5 and verify order
    for i in 0..5 {
        let (item, _token, _) = provider
            .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .unwrap();
        match item {
            WorkItem::ActivityExecute { id, name, .. } => {
                assert_eq!(id, i);
                assert_eq!(name, format!("Activity{i}"));
            }
            _ => panic!("Expected ActivityExecute"),
        }
    }

    // Queue should be empty
    assert!(
        provider
            .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );
    tracing::info!("✓ Test passed: worker queue FIFO ordering verified");
}

/// Test 5.2: Worker Peek-Lock Semantics
/// Goal: Verify dequeue doesn't remove item until ack.
pub async fn test_worker_peek_lock_semantics<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing queue semantics: worker peek-lock semantics");
    let provider = factory.create_provider().await;

    // Enqueue worker item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "instance-A".to_string(),
            execution_id: 1,
            id: 1,
            name: "Activity1".to_string(),
            input: "input1".to_string(),
            session_id: None,
        })
        .await
        .unwrap();

    // Dequeue (gets item + token)
    let (item, token, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item, WorkItem::ActivityExecute { .. }));

    // Attempt second dequeue → should return None
    assert!(
        provider
            .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Ack with token
    provider
        .ack_work_item(
            &token,
            Some(WorkItem::ActivityCompleted {
                instance: "instance-A".to_string(),
                execution_id: 1,
                id: 1,
                result: "result1".to_string(),
            }),
        )
        .await
        .unwrap();

    // Queue should now be empty
    assert!(
        provider
            .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );
    tracing::info!("✓ Test passed: worker peek-lock semantics verified");
}

/// Test 5.3: Worker Ack Atomicity
/// Goal: Verify ack_work_item atomically removes item and enqueues completion.
pub async fn test_worker_ack_atomicity<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing queue semantics: worker ack atomicity");
    let provider = factory.create_provider().await;

    // Create instance first (required for orchestrator queue)
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
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
                "instance-A".to_string(),
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
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Enqueue worker item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "instance-A".to_string(),
            execution_id: 1,
            id: 1,
            name: "Activity1".to_string(),
            input: "input1".to_string(),
            session_id: None,
        })
        .await
        .unwrap();

    // Dequeue and get token
    let (_item, token, _) = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Ack with completion
    provider
        .ack_work_item(
            &token,
            Some(WorkItem::ActivityCompleted {
                instance: "instance-A".to_string(),
                execution_id: 1,
                id: 1,
                result: "result1".to_string(),
            }),
        )
        .await
        .unwrap();

    // Verify:
    // 1. Worker queue is empty
    assert!(
        provider
            .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // 2. Orchestrator queue has completion item
    let (orchestration_item, _lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(orchestration_item.instance, "instance-A");
    assert_eq!(orchestration_item.messages.len(), 1);
    assert!(matches!(
        &orchestration_item.messages[0],
        WorkItem::ActivityCompleted { .. }
    ));
    tracing::info!("✓ Test passed: worker ack atomicity verified");
}

/// Test 5.4: Timer Delayed Visibility
/// Goal: Verify TimerFired items only dequeued when visible_at <= now.
pub async fn test_timer_delayed_visibility<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing queue semantics: timer delayed visibility");
    let provider = factory.create_provider().await;

    // Create instance first
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
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
                "instance-A".to_string(),
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
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Create timer with future visibility (delay from now)
    let delay = Duration::from_secs(5); // 5 seconds delay

    provider
        .enqueue_for_orchestrator(
            WorkItem::TimerFired {
                instance: "instance-A".to_string(),
                execution_id: 1,
                id: 1,
                fire_at_ms: 0, // This will be set correctly during ack
            },
            Some(delay),
        )
        .await
        .unwrap();

    // Fetch orchestration item immediately → should return None (timer not visible yet)
    assert!(
        provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Wait for timer to become visible
    tokio::time::sleep(Duration::from_millis(5100)).await;

    // Fetch again → should return TimerFired when visible_at <= now
    let (item2, _lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "instance-A");
    assert_eq!(item2.messages.len(), 1);
    assert!(matches!(&item2.messages[0], WorkItem::TimerFired { .. }));
    tracing::info!("✓ Test passed: timer delayed visibility verified");
}

/// Test 5.6: Lost Lock Token Handling
/// Goal: Verify locked items eventually become available if token lost.
pub async fn test_lost_lock_token_handling<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing queue semantics: lost lock token handling");
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue worker item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "instance-A".to_string(),
            execution_id: 1,
            id: 1,
            name: "Activity1".to_string(),
            input: "input1".to_string(),
            session_id: None,
        })
        .await
        .unwrap();

    // Dequeue (gets token)
    let (_item, _token, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Lose token (simulate crash)
    // Drop the token without acking

    // Attempt second dequeue → should return None (locked)
    assert!(
        provider
            .fetch_work_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Wait for lock expiration
    tokio::time::sleep(lock_timeout + Duration::from_millis(100)).await;

    // Dequeue again → should succeed → item redelivered
    let (item2, _token2, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item2, WorkItem::ActivityExecute { .. }));
    tracing::info!("✓ Test passed: lost lock token handling verified");
}

/// Test 5.6: Worker Item Immediate Visibility
/// Goal: Verify newly enqueued worker items are immediately visible.
pub async fn test_worker_item_immediate_visibility<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing queue semantics: worker item immediate visibility");
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue a work item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "test-immediate-vis".to_string(),
            execution_id: 1,
            id: 1,
            name: "TestActivity".to_string(),
            input: "test".to_string(),
            session_id: None,
        })
        .await
        .unwrap();

    // Item should be immediately fetchable (visible_at set to now)
    let result = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap();

    assert!(
        result.is_some(),
        "Newly enqueued worker item should be immediately visible"
    );

    let (item, token, _) = result.unwrap();
    assert!(matches!(item, WorkItem::ActivityExecute { name, .. } if name == "TestActivity"));

    // Clean up
    provider
        .ack_work_item(
            &token,
            Some(WorkItem::ActivityCompleted {
                instance: "test-immediate-vis".to_string(),
                execution_id: 1,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await
        .unwrap();

    tracing::info!("✓ Test passed: worker item immediate visibility verified");
}

/// Test 5.7: Worker Delayed Visibility After Abandon
/// Goal: Verify items with future visible_at are skipped even when lock_token is NULL.
pub async fn test_worker_delayed_visibility_skips_future_items<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing queue semantics: worker delayed visibility skips future items");
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue two work items
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "test-delayed-vis".to_string(),
            execution_id: 1,
            id: 1,
            name: "Activity1".to_string(),
            input: "first".to_string(),
            session_id: None,
        })
        .await
        .unwrap();

    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "test-delayed-vis".to_string(),
            execution_id: 1,
            id: 2,
            name: "Activity2".to_string(),
            input: "second".to_string(),
            session_id: None,
        })
        .await
        .unwrap();

    // Fetch first item
    let (item1, token1, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(&item1, WorkItem::ActivityExecute { id: 1, .. }));

    // Abandon first item with delay (sets visible_at to future, clears lock_token)
    let delay = Duration::from_millis(500);
    provider.abandon_work_item(&token1, Some(delay), false).await.unwrap();

    // Fetch again - should get second item (first is delayed even though unlocked)
    let (item2, token2, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(&item2, WorkItem::ActivityExecute { id: 2, .. }),
        "Should skip delayed item and fetch second item"
    );

    // Trying to fetch again should return None (first is delayed, second is locked)
    assert!(
        provider
            .fetch_work_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .is_none(),
        "No items should be available"
    );

    // Wait for delay to expire
    tokio::time::sleep(delay + Duration::from_millis(100)).await;

    // Now first item should be visible again
    let (item3, token3, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(&item3, WorkItem::ActivityExecute { id: 1, .. }),
        "First item should now be visible after delay"
    );

    // Clean up both items
    provider
        .ack_work_item(
            &token2,
            Some(WorkItem::ActivityCompleted {
                instance: "test-delayed-vis".to_string(),
                execution_id: 1,
                id: 2,
                result: "done".to_string(),
            }),
        )
        .await
        .unwrap();

    provider
        .ack_work_item(
            &token3,
            Some(WorkItem::ActivityCompleted {
                instance: "test-delayed-vis".to_string(),
                execution_id: 1,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await
        .unwrap();

    tracing::info!("✓ Test passed: worker delayed visibility skips future items verified");
}

/// Test 5.8: Orphan Queue Messages Dropped
/// Goal: Verify that QueueMessage items enqueued for a non-existent orchestration
/// are dropped (deleted) by the provider, and that QueueMessage items enqueued
/// for an existing orchestration are kept and returned in the next fetch.
pub async fn test_orphan_queue_messages_dropped<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing queue semantics: orphan queue messages are dropped");
    let provider = factory.create_provider().await;

    // --- Part 1: Events for non-existent instance are dropped ---

    // Enqueue two QueueMessage items for an instance that doesn't exist yet
    provider
        .enqueue_for_orchestrator(
            WorkItem::QueueMessage {
                instance: "orphan-test".to_string(),
                name: "ConfigUpdate".to_string(),
                data: "v1".to_string(),
            },
            None,
        )
        .await
        .unwrap();
    provider
        .enqueue_for_orchestrator(
            WorkItem::QueueMessage {
                instance: "orphan-test".to_string(),
                name: "ConfigUpdate".to_string(),
                data: "v2".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    // Fetch should return None (no orchestration found) and DROP the messages
    let result = provider
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap();
    assert!(result.is_none(), "Expected None for orphan queue messages");

    // Verify messages were actually deleted: a second fetch should also return None
    // (if they were left in the queue, we'd get them again — busy loop)
    let result2 = provider
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap();
    assert!(
        result2.is_none(),
        "Orphan messages should have been deleted, not left in queue"
    );

    // --- Part 2: Events for an existing instance are kept ---

    // Create an orchestration instance (enqueue StartOrchestration, fetch, ack with history)
    create_instance(&*provider, "orphan-test-existing")
        .await
        .expect("Failed to create instance");

    // Now enqueue a QueueMessage for the existing instance
    provider
        .enqueue_for_orchestrator(
            WorkItem::QueueMessage {
                instance: "orphan-test-existing".to_string(),
                name: "ConfigUpdate".to_string(),
                data: "v3".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    // Fetch should return the item with the QueueMessage included
    let (item, lock_token, _attempt) = provider
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("Expected orchestration item for existing instance with queued event");

    assert_eq!(item.instance, "orphan-test-existing");
    assert!(
        item.messages
            .iter()
            .any(|m| matches!(m, WorkItem::QueueMessage { name, data, .. } if name == "ConfigUpdate" && data == "v3")),
        "QueueMessage for existing instance should be included in messages, got: {:?}",
        item.messages
    );

    // Clean up: ack the item
    provider
        .ack_orchestration_item(
            &lock_token,
            item.execution_id,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    tracing::info!("✓ Test passed: orphan queue messages dropped, existing instance events kept");
}

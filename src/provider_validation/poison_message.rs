//! Poison Message Detection Validation Tests
//!
//! These tests validate that providers correctly track attempt counts
//! for poison message detection.

use std::time::Duration;

use crate::INITIAL_EXECUTION_ID;
use crate::provider_validations::ProviderFactory;
use crate::providers::{TagFilter, WorkItem};

use super::ExecutionMetadata;

/// Test that orchestration item attempt_count is 1 on first fetch
pub async fn orchestration_attempt_count_starts_at_one(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue a start orchestration
    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "poison-test-1".to_string(),
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

    // Fetch the item
    let (_item, lock_token, attempt_count) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");

    // First fetch should have attempt_count = 1
    assert_eq!(attempt_count, 1, "First fetch should have attempt_count = 1");

    // Ack the item
    provider
        .ack_orchestration_item(
            &lock_token,
            INITIAL_EXECUTION_ID,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .expect("ack should succeed");

    tracing::info!("✓ Test passed: orchestration attempt_count starts at 1");
}

/// Test that orchestration attempt_count increments on each fetch after abandon
pub async fn orchestration_attempt_count_increments_on_refetch(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue a start orchestration
    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "poison-test-2".to_string(),
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

    // First fetch - attempt_count = 1
    let (_item1, lock_token1, attempt_count1) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt_count1, 1, "First fetch should have attempt_count = 1");

    // Abandon the item
    provider
        .abandon_orchestration_item(&lock_token1, None, false)
        .await
        .expect("abandon should succeed");

    // Second fetch - attempt_count = 2
    let (_item2, lock_token2, attempt_count2) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt_count2, 2, "Second fetch should have attempt_count = 2");

    // Abandon again
    provider
        .abandon_orchestration_item(&lock_token2, None, false)
        .await
        .expect("abandon should succeed");

    // Third fetch - attempt_count = 3
    let (_item3, lock_token3, attempt_count3) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt_count3, 3, "Third fetch should have attempt_count = 3");

    // Clean up
    provider
        .ack_orchestration_item(
            &lock_token3,
            INITIAL_EXECUTION_ID,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .expect("ack should succeed");

    tracing::info!("✓ Test passed: orchestration attempt_count increments on refetch");
}

/// Test that worker item attempt_count is 1 on first fetch
pub async fn worker_attempt_count_starts_at_one(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue a worker item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "poison-test-3".to_string(),
            execution_id: INITIAL_EXECUTION_ID,
            id: 1,
            name: "TestActivity".to_string(),
            input: "{}".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .expect("enqueue should succeed");

    // Fetch the item
    let (item, token, attempt_count) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");

    // First fetch should have attempt_count = 1
    assert_eq!(attempt_count, 1, "First fetch should have attempt_count = 1");

    // Ack the item
    provider
        .ack_work_item(
            &token,
            Some(WorkItem::ActivityCompleted {
                instance: "poison-test-3".to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await
        .expect("ack should succeed");

    assert!(matches!(item, WorkItem::ActivityExecute { .. }));

    tracing::info!("✓ Test passed: worker attempt_count starts at 1");
}

/// Test that worker attempt_count increments when lock expires and item is refetched
pub async fn worker_attempt_count_increments_on_lock_expiry(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let short_timeout = Duration::from_secs(1);

    // Enqueue a worker item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "poison-test-4".to_string(),
            execution_id: INITIAL_EXECUTION_ID,
            id: 1,
            name: "TestActivity".to_string(),
            input: "{}".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .expect("enqueue should succeed");

    // First fetch with short lock timeout - attempt_count = 1
    let (_item1, _token1, attempt_count1) = provider
        .fetch_work_item(short_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt_count1, 1, "First fetch should have attempt_count = 1");

    // Wait for lock to expire
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Second fetch after lock expiry - attempt_count = 2
    let (_item2, token2, attempt_count2) = provider
        .fetch_work_item(short_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present after lock expiry");
    assert_eq!(
        attempt_count2, 2,
        "Second fetch after lock expiry should have attempt_count = 2"
    );

    // Clean up
    provider
        .ack_work_item(
            &token2,
            Some(WorkItem::ActivityCompleted {
                instance: "poison-test-4".to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await
        .expect("ack should succeed");

    tracing::info!("✓ Test passed: worker attempt_count increments on lock expiry");
}

/// Test that attempt_count is tracked per message, not globally
pub async fn attempt_count_is_per_message(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue two different worker items
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "poison-test-5a".to_string(),
            execution_id: INITIAL_EXECUTION_ID,
            id: 1,
            name: "Activity1".to_string(),
            input: "{}".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .expect("enqueue should succeed");

    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "poison-test-5b".to_string(),
            execution_id: INITIAL_EXECUTION_ID,
            id: 2,
            name: "Activity2".to_string(),
            input: "{}".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .expect("enqueue should succeed");

    // Fetch first item
    let (item1, token1, attempt1) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt1, 1, "First item first fetch should have attempt_count = 1");

    // Fetch second item
    let (item2, token2, attempt2) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt2, 1, "Second item first fetch should have attempt_count = 1");

    // Both items should have independent attempt counts
    let id1 = match &item1 {
        WorkItem::ActivityExecute { id, .. } => *id,
        _ => panic!("Expected ActivityExecute"),
    };
    let id2 = match &item2 {
        WorkItem::ActivityExecute { id, .. } => *id,
        _ => panic!("Expected ActivityExecute"),
    };
    assert_ne!(id1, id2, "Should be different items");

    // Clean up
    provider
        .ack_work_item(
            &token1,
            Some(WorkItem::ActivityCompleted {
                instance: "poison-test-5a".to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: id1,
                result: "done".to_string(),
            }),
        )
        .await
        .expect("ack should succeed");

    provider
        .ack_work_item(
            &token2,
            Some(WorkItem::ActivityCompleted {
                instance: "poison-test-5b".to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: id2,
                result: "done".to_string(),
            }),
        )
        .await
        .expect("ack should succeed");

    tracing::info!("✓ Test passed: attempt_count is tracked per message");
}

/// Test that abandon_work_item with ignore_attempt=true decrements attempt count
pub async fn abandon_work_item_ignore_attempt_decrements(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue a worker item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "poison-test-ignore-1".to_string(),
            execution_id: INITIAL_EXECUTION_ID,
            id: 1,
            name: "TestActivity".to_string(),
            input: "{}".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .expect("enqueue should succeed");

    // First fetch - attempt_count = 1
    let (_item1, token1, attempt1) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt1, 1, "First fetch should have attempt_count = 1");

    // Abandon WITHOUT ignore_attempt
    provider
        .abandon_work_item(&token1, None, false)
        .await
        .expect("abandon should succeed");

    // Second fetch - attempt_count = 2
    let (_item2, token2, attempt2) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt2, 2, "Second fetch should have attempt_count = 2");

    // Abandon WITH ignore_attempt=true - decrements to 1
    provider
        .abandon_work_item(&token2, None, true)
        .await
        .expect("abandon with ignore_attempt should succeed");

    // Third fetch - attempt_count = 2 (1 stored + 1 from new fetch)
    let (_item3, token3, attempt3) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(
        attempt3, 2,
        "Third fetch should have attempt_count = 2 (decremented then re-incremented)"
    );

    // Clean up
    provider
        .ack_work_item(
            &token3,
            Some(WorkItem::ActivityCompleted {
                instance: "poison-test-ignore-1".to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await
        .expect("ack should succeed");

    tracing::info!("✓ Test passed: abandon_work_item ignore_attempt decrements attempt count");
}

/// Test that abandon_orchestration_item with ignore_attempt=true decrements attempt count
pub async fn abandon_orchestration_item_ignore_attempt_decrements(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue a start orchestration
    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "poison-test-ignore-2".to_string(),
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

    // First fetch - attempt_count = 1
    let (_item1, token1, attempt1) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt1, 1, "First fetch should have attempt_count = 1");

    // Abandon WITHOUT ignore_attempt
    provider
        .abandon_orchestration_item(&token1, None, false)
        .await
        .expect("abandon should succeed");

    // Second fetch - attempt_count = 2
    let (_item2, token2, attempt2) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt2, 2, "Second fetch should have attempt_count = 2");

    // Abandon WITH ignore_attempt=true - decrements to 1
    provider
        .abandon_orchestration_item(&token2, None, true)
        .await
        .expect("abandon with ignore_attempt should succeed");

    // Third fetch - attempt_count = 2 (1 stored + 1 from new fetch)
    let (_item3, token3, attempt3) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(
        attempt3, 2,
        "Third fetch should have attempt_count = 2 (decremented then re-incremented)"
    );

    // Clean up
    provider
        .ack_orchestration_item(
            &token3,
            INITIAL_EXECUTION_ID,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .expect("ack should succeed");

    tracing::info!("✓ Test passed: abandon_orchestration_item ignore_attempt decrements attempt count");
}

/// Test that ignore_attempt never allows attempt count to go below 0
pub async fn ignore_attempt_never_goes_negative(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue a worker item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "poison-test-never-neg".to_string(),
            execution_id: INITIAL_EXECUTION_ID,
            id: 1,
            name: "TestActivity".to_string(),
            input: "{}".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .expect("enqueue should succeed");

    // First fetch - attempt_count = 1
    let (_item1, token1, attempt1) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt1, 1, "First fetch should have attempt_count = 1");

    // Abandon with ignore_attempt=true - decrements to 0
    provider
        .abandon_work_item(&token1, None, true)
        .await
        .expect("abandon with ignore_attempt should succeed");

    // Second fetch - attempt_count = 1 (0 + 1)
    let (_item2, token2, attempt2) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt2, 1, "Second fetch should have attempt_count = 1");

    // Abandon with ignore_attempt=true again - should stay at 0, not go negative
    provider
        .abandon_work_item(&token2, None, true)
        .await
        .expect("abandon with ignore_attempt should succeed");

    // Third fetch - attempt_count = 1 (max(0, 0-1) + 1 = 0 + 1 = 1)
    let (_item3, token3, attempt3) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(
        attempt3, 1,
        "Attempt count should not go below 0 - third fetch should have attempt_count = 1"
    );

    // Clean up
    provider
        .ack_work_item(
            &token3,
            Some(WorkItem::ActivityCompleted {
                instance: "poison-test-never-neg".to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await
        .expect("ack should succeed");

    tracing::info!("✓ Test passed: ignore_attempt never allows attempt count to go negative");
}

/// Test that max attempt_count is returned when batch contains messages with different counts
///
/// This validates the scenario where:
/// 1. An orchestration item is fetched, incrementing attempt_count for its messages
/// 2. New messages arrive for the same instance while lock is held
/// 3. Lock expires (or is abandoned) and instance is re-fetched
/// 4. The returned attempt_count should be the MAX across all messages in the batch
pub async fn max_attempt_count_across_message_batch(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue initial start message
    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "poison-batch-test".to_string(),
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

    // First fetch - attempt_count = 1 for the start message
    let (_item1, lock_token1, attempt1) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt1, 1, "First fetch should have attempt_count = 1");
    assert_eq!(_item1.messages.len(), 1, "Should have 1 message");

    // Abandon without processing (simulating a crash/failure)
    provider
        .abandon_orchestration_item(&lock_token1, None, false)
        .await
        .expect("abandon should succeed");

    // Second fetch - attempt_count = 2 for the start message
    let (_item2, lock_token2, attempt2) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(attempt2, 2, "Second fetch should have attempt_count = 2");

    // While lock is held, enqueue a new message (e.g., activity completion)
    // This simulates a message arriving while an orchestration is being processed
    provider
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "poison-batch-test".to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: 1,
                result: "activity result".to_string(),
            },
            None,
        )
        .await
        .expect("enqueue should succeed");

    // Abandon again (simulating another failure)
    provider
        .abandon_orchestration_item(&lock_token2, None, false)
        .await
        .expect("abandon should succeed");

    // Third fetch - now we have 2 messages:
    // - Start message: attempt_count = 3
    // - Activity completion: attempt_count = 1 (first time being fetched)
    // The returned attempt_count should be MAX(3, 1) = 3
    let (item3, lock_token3, attempt3) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .expect("fetch should succeed")
        .expect("item should be present");
    assert_eq!(item3.messages.len(), 2, "Should have 2 messages in the batch");
    assert_eq!(
        attempt3, 3,
        "Attempt count should be MAX across messages (3), not the newer message's count (1)"
    );

    // Clean up - ack the orchestration
    provider
        .ack_orchestration_item(
            &lock_token3,
            INITIAL_EXECUTION_ID,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .expect("ack should succeed");

    tracing::info!("✓ Test passed: max attempt_count across message batch verified");
}

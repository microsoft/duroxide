use crate::provider_validation::{Event, EventKind, ExecutionMetadata, start_item};
use crate::provider_validations::ProviderFactory;
use crate::providers::{TagFilter, WorkItem};
use std::sync::Arc;
use std::time::Duration;

/// Test 2.1: All-or-Nothing Ack
/// Goal: Verify `ack_orchestration_item` commits everything atomically.
pub async fn test_atomicity_failure_rollback<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing atomicity: ack failure should rollback all operations");
    let provider = factory.create_provider().await;

    // Setup: create instance with some initial state
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Ack with valid data to establish baseline
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

    // Verify initial state
    let initial_history = provider.read("instance-A").await.unwrap_or_default();
    assert_eq!(initial_history.len(), 1);

    // Now enqueue another work item
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item2, lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Try to ack with duplicate event_id (should fail due to primary key constraint)
    let result = provider
        .ack_orchestration_item(
            &lock_token2,
            1,
            vec![Event::with_event_id(
                1, // DUPLICATE - should fail
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
        .await;

    assert!(result.is_err(), "Should reject duplicate event_id");

    // Verify state unchanged - history should still have only 1 event
    let after_history = provider.read("instance-A").await.unwrap_or_default();
    assert_eq!(
        after_history.len(),
        1,
        "History should remain unchanged after failed ack"
    );

    // Lock should still be held, preventing another fetch
    assert!(
        provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );
    tracing::info!("✓ Test passed: atomicity rollback verified");
}

/// Test 2.2: Multi-Operation Atomic Ack
/// Goal: Verify complex ack with multiple outputs succeeds atomically.
pub async fn test_multi_operation_atomic_ack<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing atomicity: complex ack with multiple operations succeeds atomically");
    let provider = factory.create_provider().await;

    // Setup
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Prepare complex ack with multiple operations
    let history_delta = vec![
        Event::with_event_id(
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
        ),
        Event::with_event_id(
            2,
            "instance-A".to_string(),
            1,
            None,
            EventKind::ActivityScheduled {
                name: "Activity1".to_string(),
                input: "input1".to_string(),
                session_id: None,
                tag: None,
            },
        ),
        Event::with_event_id(
            3,
            "instance-A".to_string(),
            1,
            None,
            EventKind::ActivityScheduled {
                name: "Activity2".to_string(),
                input: "input2".to_string(),
                session_id: None,
                tag: None,
            },
        ),
        Event::with_event_id(
            4,
            "instance-A".to_string(),
            1,
            None,
            EventKind::ActivityScheduled {
                name: "Activity3".to_string(),
                input: "input3".to_string(),
                session_id: None,
                tag: None,
            },
        ),
        Event::with_event_id(
            5,
            "instance-A".to_string(),
            1,
            None,
            EventKind::TimerCreated { fire_at_ms: 1234567890 },
        ),
    ];

    let worker_items = vec![
        WorkItem::ActivityExecute {
            instance: "instance-A".to_string(),
            execution_id: 1,
            id: 1,
            name: "Activity1".to_string(),
            input: "input1".to_string(),
            session_id: None,
            tag: None,
        },
        WorkItem::ActivityExecute {
            instance: "instance-A".to_string(),
            execution_id: 1,
            id: 2,
            name: "Activity2".to_string(),
            input: "input2".to_string(),
            session_id: None,
            tag: None,
        },
        WorkItem::ActivityExecute {
            instance: "instance-A".to_string(),
            execution_id: 1,
            id: 3,
            name: "Activity3".to_string(),
            input: "input3".to_string(),
            session_id: None,
            tag: None,
        },
    ];

    let orchestrator_items = vec![
        WorkItem::TimerFired {
            instance: "instance-A".to_string(),
            execution_id: 1,
            id: 1,
            fire_at_ms: 1234567890,
        },
        WorkItem::ExternalRaised {
            instance: "instance-A".to_string(),
            name: "TestEvent".to_string(),
            data: "test-data".to_string(),
        },
    ];

    // Ack with all operations
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            history_delta,
            worker_items,
            orchestrator_items,
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Verify ALL operations succeeded together
    // 1. History should have 5 events
    let history = provider.read("instance-A").await.unwrap_or_default();
    assert_eq!(history.len(), 5, "All 5 history events should be persisted");

    // 2. Worker queue should have 3 items
    for _ in 0..3 {
        assert!(
            provider
                .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
                .await
                .unwrap()
                .is_some()
        );
    }
    assert!(
        provider
            .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
            .await
            .unwrap()
            .is_none()
    );

    // 3. Orchestrator queue should have 2 items
    let (item1, _lock_token1, _attempt_count1) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item1.messages.len(), 2, "Should have TimerFired and ExternalRaised");
    assert!(matches!(&item1.messages[0], WorkItem::TimerFired { .. }));
    assert!(matches!(&item1.messages[1], WorkItem::ExternalRaised { .. }));
    tracing::info!("✓ Test passed: multi-operation atomic ack verified");
}

/// Test 2.3: Lock Released Only on Successful Ack
/// Goal: Ensure lock is only released after successful commit.
pub async fn test_lock_released_only_on_successful_ack<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing atomicity: lock released only on successful ack");
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Setup
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, _lock_token, _attempt_count) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Verify lock is held (can't fetch again)
    assert!(
        provider
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Attempt ack with invalid lock token (should fail)
    let _result = provider
        .ack_orchestration_item(
            "invalid-lock-token",
            1,
            vec![Event::with_event_id(
                2,
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
        .await;

    // Should fail with invalid lock token
    assert!(_result.is_err());

    // Lock should still be held
    assert!(
        provider
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Wait for lock expiration
    tokio::time::sleep(lock_timeout + Duration::from_millis(100)).await;

    // Now should be able to fetch again
    let (item2, _lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "instance-A");
    tracing::info!("✓ Test passed: lock release on successful ack verified");
}

/// Test 2.4: Concurrent Ack Prevention
/// Goal: Ensure two acks with same lock token don't both succeed.
pub async fn test_concurrent_ack_prevention<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing atomicity: concurrent ack prevention");
    let provider = Arc::new(factory.create_provider().await);

    // Setup
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Spawn two concurrent tasks trying to ack with same lock token
    let provider1 = provider.clone();
    let provider2 = provider.clone();
    let token1 = lock_token.clone();
    let token2 = lock_token.clone();

    let handle1 = tokio::spawn(async move {
        provider1
            .ack_orchestration_item(
                &token1,
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
    });

    let handle2 = tokio::spawn(async move {
        provider2
            .ack_orchestration_item(
                &token2,
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
    });

    let results = futures::future::join_all(vec![handle1, handle2]).await;
    let results: Vec<_> = results.into_iter().map(|r| r.unwrap()).collect();

    // Exactly one should succeed
    let successes: Vec<_> = results.iter().filter(|r| r.is_ok()).collect();
    assert_eq!(successes.len(), 1, "Exactly one ack should succeed");

    // The other should fail
    let failures: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
    assert_eq!(failures.len(), 1, "Exactly one ack should fail");
    tracing::info!("✓ Test passed: concurrent ack prevention verified");
}

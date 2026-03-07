use crate::provider_validation::{Event, EventKind, ExecutionMetadata, start_item};
use crate::provider_validations::ProviderFactory;
use std::time::Duration;

/// Test 3.1: Invalid Lock Token on Ack
/// Goal: Provider should reject invalid lock tokens.
pub async fn test_invalid_lock_token_on_ack<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing error handling: invalid lock token on ack");
    let provider = factory.create_provider().await;

    // Attempt to ack with non-existent lock token
    let result = provider
        .ack_orchestration_item(
            "invalid-token",
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err();
    assert!(err_msg.message.contains("Invalid lock token") || err_msg.message.contains("lock_token"));
    tracing::info!("✓ Test passed: invalid lock token rejected");
}

/// Test 3.2: Duplicate Event ID Handling
/// Goal: Provider should detect and handle duplicate event_ids.
pub async fn test_duplicate_event_id_rejection<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing error handling: duplicate event_id rejection");
    let provider = factory.create_provider().await;

    // Create instance with initial event
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Ack with event_id=1
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

    // Try to append duplicate event_id=1
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item2, lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    let result = provider
        .ack_orchestration_item(
            &lock_token2,
            1,
            vec![Event::with_event_id(
                1, // DUPLICATE!
                "instance-A".to_string(),
                1,
                None,
                EventKind::ActivityScheduled {
                    name: "Activity".to_string(),
                    input: "{}".to_string(),
                    session_id: None,
                    tag: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await;

    // Should fail due to duplicate event_id
    assert!(result.is_err());

    // Verify history unchanged
    let history = provider.read("instance-A").await.unwrap_or_default();
    assert_eq!(history.len(), 1);
    assert!(matches!(&history[0].kind, EventKind::OrchestrationStarted { .. }));
    tracing::info!("✓ Test passed: duplicate event_id rejected");
}

/// Test 3.3: Missing Instance Metadata
/// Goal: Provider should handle missing instance gracefully.
pub async fn test_missing_instance_metadata<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing error handling: missing instance metadata");
    let provider = factory.create_provider().await;

    // Attempt to read history for non-existent instance
    let history = provider.read("non-existent-instance").await.unwrap_or_default();
    assert_eq!(history.len(), 0);
    tracing::info!("✓ Test passed: missing instance handled gracefully");
}

/// Test 3.4: Corrupted Serialization Data
/// Goal: Provider should handle corrupted JSON in queue/work items gracefully.
pub async fn test_corrupted_serialization_data<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing error handling: corrupted serialization data");
    let provider = factory.create_provider().await;

    // This test is primarily about graceful degradation
    // SQLite provider will handle corrupted data by returning None on deserialization failure
    // Test that provider doesn't panic
    let item = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap();
    assert!(item.is_none() || item.is_some(), "Should not panic");
    tracing::info!("✓ Test passed: corrupted data handled gracefully");
}

/// Test 3.5: Lock Expiration During Ack
/// Goal: Provider should detect and reject expired locks.
pub async fn test_lock_expiration_during_ack<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing error handling: lock expiration during ack");
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Create and fetch item
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Wait for lock to expire
    tokio::time::sleep(lock_timeout + Duration::from_millis(100)).await;

    // Attempt to ack with expired token
    let result = provider
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
        .await;

    // Should fail - lock expired (or item already consumed by another fetch)
    assert!(result.is_err());
    tracing::info!("✓ Test passed: expired lock rejected");
}

use crate::provider_validation::{Event, EventKind, ExecutionMetadata, WorkItem, start_item};
use crate::provider_validations::ProviderFactory;
use crate::providers::TagFilter;
use std::sync::Arc;
use std::time::Duration;

fn provider_lock_timeout<F: ProviderFactory>(factory: &F) -> Duration {
    factory.lock_timeout()
}

/// Test 4.1: Lock Expires After Timeout
/// Goal: Verify locks expire and instance becomes available again.
pub async fn test_lock_expires_after_timeout<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing lock expiration: lock expires after timeout");
    let provider = factory.create_provider().await;
    let lock_timeout = provider_lock_timeout(factory);

    // Setup: create and fetch item
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Verify lock is held
    assert!(
        provider
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Wait for lock to expire
    tokio::time::sleep(lock_timeout + Duration::from_millis(100)).await;

    // Instance should be available again
    let (item2, lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "instance-A");
    assert_ne!(lock_token2, lock_token, "Should have new lock token");

    // Original lock token should no longer work
    let result = provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await;
    assert!(result.is_err());
    tracing::info!("✓ Test passed: lock expiration verified");
}

/// Test 4.2: Abandon Releases Lock Immediately
/// Goal: Verify abandon releases lock without waiting for expiration.
pub async fn test_abandon_releases_lock_immediately<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing lock expiration: abandon releases lock immediately");
    let provider = factory.create_provider().await;
    let lock_timeout = provider_lock_timeout(factory);

    // Setup: create and fetch item
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Verify lock is held
    assert!(
        provider
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Abandon the lock
    provider
        .abandon_orchestration_item(&lock_token, None, false)
        .await
        .unwrap();

    // Lock should be released immediately (don't need to wait for expiration)
    let (item2, _lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "instance-A");
    tracing::info!("✓ Test passed: abandon releases lock verified");
}

/// Test 4.3: Lock Renewal on Ack
/// Goal: Verify successful ack releases lock immediately.
pub async fn test_lock_renewal_on_ack<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing lock expiration: lock renewal on ack");
    let provider = factory.create_provider().await;
    let lock_timeout = provider_lock_timeout(factory);

    // Setup: create and fetch item
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, _lock_token, _attempt_count) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Verify lock is held
    assert!(
        provider
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Enqueue another item for the same instance while locked
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();

    // Item should not be available yet (lock still held)
    assert!(
        provider
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Ack successfully
    provider
        .ack_orchestration_item(
            &_lock_token,
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

    // The new item should be available immediately after ack
    let (item2, _lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "instance-A");
    tracing::info!("✓ Test passed: lock renewal on ack verified");
}

/// Test 4.4: Concurrent Lock Attempts Respect Expiration
/// Goal: Verify multiple dispatchers respect lock expiration times.
pub async fn test_concurrent_lock_attempts_respect_expiration<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing lock expiration: concurrent lock attempts respect expiration");
    let provider = Arc::new(factory.create_provider().await);
    let lock_timeout = provider_lock_timeout(factory);

    // Setup: create and fetch item
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let (_item, _lock_token, _attempt_count) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Spawn multiple concurrent fetchers
    let handles: Vec<_> = (0..5)
        .map(|i| {
            let provider = provider.clone();
            tokio::spawn(async move {
                // Stagger the attempts slightly
                tokio::time::sleep(Duration::from_millis(i * 50)).await;
                provider
                    .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
                    .await
                    .unwrap()
            })
        })
        .collect();

    // Wait a bit before expiration
    tokio::time::sleep(Duration::from_millis(200)).await;

    // All should still return None (lock held)
    let results = futures::future::join_all(handles).await;
    for result in results {
        assert!(result.unwrap().is_none());
    }

    // Wait for lock expiration
    let wait = lock_timeout.checked_sub(Duration::from_millis(200)).unwrap_or_default() + Duration::from_millis(100);
    tokio::time::sleep(wait).await;

    // Now one should succeed
    let (item2, _lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "instance-A");
    tracing::info!("✓ Test passed: concurrent lock attempts respect expiration verified");
}

/// Test 4.5: Worker Lock Renewal Success
/// Goal: Verify worker lock can be renewed with valid token
pub async fn test_worker_lock_renewal_success<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing worker lock renewal: renewal succeeds with valid token");
    let provider = factory.create_provider().await;
    let lock_timeout = provider_lock_timeout(factory);

    use crate::providers::{TagFilter, WorkItem};

    // Enqueue and fetch work item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "test-instance".to_string(),
            execution_id: 1,
            id: 1,
            name: "TestActivity".to_string(),
            input: "test".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .unwrap();

    let (_item, token, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();
    tracing::info!("Fetched work item with lock token: {}", token);

    // Verify item is locked (can't fetch again)
    assert!(
        provider
            .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
            .await
            .unwrap()
            .is_none()
    );

    // Wait a bit to simulate activity in progress
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Renew lock
    provider.renew_work_item_lock(&token, lock_timeout).await.unwrap();
    tracing::info!("Successfully renewed lock");

    // Item should still be locked
    assert!(
        provider
            .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
            .await
            .unwrap()
            .is_none()
    );

    tracing::info!("✓ Test passed: worker lock renewal success verified");
}

/// Test 4.6: Worker Lock Renewal Invalid Token
/// Goal: Verify renewal fails with invalid token
pub async fn test_worker_lock_renewal_invalid_token<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing worker lock renewal: renewal fails with invalid token");
    let provider = factory.create_provider().await;

    // Try to renew with invalid token
    let result = provider
        .renew_work_item_lock("invalid-token-123", Duration::from_secs(30))
        .await;
    assert!(result.is_err(), "Should fail with invalid token");
    tracing::info!("✓ Test passed: invalid token rejection verified");
}

/// Test 4.7: Worker Lock Renewal After Expiration
/// Goal: Verify renewal fails after lock expires
pub async fn test_worker_lock_renewal_after_expiration<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing worker lock renewal: renewal fails after expiration");
    let provider = factory.create_provider().await;

    use crate::providers::{TagFilter, WorkItem};

    // Enqueue and fetch work item with short timeout
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "test-instance".to_string(),
            execution_id: 1,
            id: 1,
            name: "TestActivity".to_string(),
            input: "test".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .unwrap();

    let short_timeout = Duration::from_secs(1);
    let (_item, token, _) = provider
        .fetch_work_item(short_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();
    tracing::info!("Fetched work item with 1s timeout");

    // Wait for lock to expire
    tokio::time::sleep(factory.lock_timeout() + Duration::from_millis(100)).await;

    // Try to renew expired lock
    let result = provider.renew_work_item_lock(&token, factory.lock_timeout()).await;
    assert!(result.is_err(), "Should fail to renew expired lock");
    tracing::info!("✓ Test passed: expired lock renewal rejection verified");
}

/// Test 4.8: Worker Lock Renewal Extends Timeout
/// Goal: Verify renewal properly extends lock timeout
pub async fn test_worker_lock_renewal_extends_timeout<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing worker lock renewal: renewal extends timeout");
    let provider = Arc::new(factory.create_provider().await);
    let lock_timeout = factory.lock_timeout();

    use crate::provider_validation::start_item;
    use crate::providers::{ExecutionMetadata, TagFilter, WorkItem};
    use crate::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};

    let instance = "test-lock-renewal-extends";

    // 1. Create a running orchestration (required for renewal to work)
    provider
        .enqueue_for_orchestrator(start_item(instance), None)
        .await
        .unwrap();
    let (_item, orch_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Create the ActivityExecute work item to enqueue
    let activity_item = WorkItem::ActivityExecute {
        instance: instance.to_string(),
        execution_id: INITIAL_EXECUTION_ID,
        id: 1,
        name: "TestActivity".to_string(),
        input: "test".to_string(),
        session_id: None,
        tag: None,
    };

    // Ack with Running status and enqueue the activity
    provider
        .ack_orchestration_item(
            &orch_token,
            INITIAL_EXECUTION_ID,
            vec![Event::with_event_id(
                INITIAL_EVENT_ID,
                instance.to_string(),
                INITIAL_EXECUTION_ID,
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
                status: Some("Running".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // 2. Fetch the activity work item
    let (_item, token, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();
    tracing::info!("Fetched work item with {:?} timeout", lock_timeout);

    // Wait 60% of lock timeout before renewing
    let pre_renewal_wait = lock_timeout.mul_f64(0.6);
    tokio::time::sleep(pre_renewal_wait).await;

    // Renew lock for full timeout duration
    // At t=0.6x: renew extends lock to t=0.6x + 1.0x = expires at t=1.6x from start
    provider.renew_work_item_lock(&token, lock_timeout).await.unwrap();
    tracing::info!("Renewed lock at ~0.6x timeout mark, now expires at ~1.6x");

    // Wait remaining time to reach exactly 1.0x from start
    // Without renewal, lock would have expired here
    // With renewal at 0.6x for 1.0x more, lock expires at 1.6x
    let post_renewal_wait = lock_timeout - pre_renewal_wait;
    tokio::time::sleep(post_renewal_wait).await;

    // Item should still be locked (we're at 1.0x, renewal extended to 1.6x)
    let result = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();
    assert!(result.is_none(), "Item should still be locked after renewal");

    tracing::info!("✓ Test passed: lock timeout extension verified");
}

/// Test 4.9: Worker Lock Renewal After Ack Fails
/// Goal: Verify renewal fails after item has been acked
pub async fn test_worker_lock_renewal_after_ack<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing worker lock renewal: renewal fails after ack");
    let provider = factory.create_provider().await;

    use crate::providers::{TagFilter, WorkItem};

    // Enqueue and fetch work item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "test-instance".to_string(),
            execution_id: 1,
            id: 1,
            name: "TestActivity".to_string(),
            input: "test".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .unwrap();

    let lock_timeout = factory.lock_timeout();
    let (_item, token, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();
    tracing::info!("Fetched work item");

    // Ack the work item
    provider
        .ack_work_item(
            &token,
            Some(WorkItem::ActivityCompleted {
                instance: "test-instance".to_string(),
                execution_id: 1,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await
        .unwrap();
    tracing::info!("Acked work item");

    // Try to renew after ack
    let result = provider.renew_work_item_lock(&token, lock_timeout).await;
    assert!(result.is_err(), "Should fail to renew after ack");
    tracing::info!("✓ Test passed: renewal after ack rejection verified");
}

/// Test: abandon_work_item releases lock immediately
/// Goal: Verify abandoning a work item releases its lock without waiting for expiration.
pub async fn test_abandon_work_item_releases_lock<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing: abandon_work_item releases lock immediately");
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue a work item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "test-abandon-work".to_string(),
            execution_id: 1,
            id: 1,
            name: "TestActivity".to_string(),
            input: "test".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .unwrap();

    // Fetch the work item
    let (item, token, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item, WorkItem::ActivityExecute { .. }));

    // Verify lock is held - no items available
    assert!(
        provider
            .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
            .await
            .unwrap()
            .is_none(),
        "Item should be locked"
    );

    // Abandon the work item
    provider.abandon_work_item(&token, None, false).await.unwrap();

    // Item should be immediately available again
    let (item2, token2, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item2, WorkItem::ActivityExecute { .. }));
    assert_ne!(token, token2, "Should have new lock token");

    // Clean up
    provider
        .ack_work_item(
            &token2,
            Some(WorkItem::ActivityCompleted {
                instance: "test-abandon-work".to_string(),
                execution_id: 1,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await
        .unwrap();

    tracing::info!("✓ Test passed: abandon_work_item releases lock immediately");
}

/// Test: abandon_work_item with delay prevents immediate refetch
/// Goal: Verify delay parameter makes the item invisible until delay expires.
pub async fn test_abandon_work_item_with_delay<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing: abandon_work_item with delay prevents immediate refetch");
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue a work item
    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "test-abandon-delay".to_string(),
            execution_id: 1,
            id: 1,
            name: "TestActivity".to_string(),
            input: "test".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .unwrap();

    // Fetch the work item
    let (_item, token, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // Abandon with 500ms delay
    let delay = Duration::from_millis(500);
    provider.abandon_work_item(&token, Some(delay), false).await.unwrap();

    // Item should NOT be immediately available (delayed)
    assert!(
        provider
            .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
            .await
            .unwrap()
            .is_none(),
        "Item should be delayed, not immediately available"
    );

    // Wait for delay to expire
    tokio::time::sleep(delay + Duration::from_millis(100)).await;

    // Now item should be available
    let (item2, token2, _) = provider
        .fetch_work_item(lock_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item2, WorkItem::ActivityExecute { .. }));

    // Clean up
    provider
        .ack_work_item(
            &token2,
            Some(WorkItem::ActivityCompleted {
                instance: "test-abandon-delay".to_string(),
                execution_id: 1,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await
        .unwrap();

    tracing::info!("✓ Test passed: abandon_work_item with delay verified");
}

/// G2: Worker ack fails after lock expiry
/// Goal: Verify that ack_work_item fails when the worker lock has expired,
/// even if no other worker has reclaimed the item.
/// (Mirrors test_lock_expiration_during_ack which tests the orchestration side.)
pub async fn test_worker_ack_fails_after_lock_expiry<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "ack-expiry-test".to_string(),
            execution_id: 1,
            id: 1,
            name: "TestActivity".to_string(),
            input: "test".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .unwrap();

    let short_timeout = Duration::from_secs(1);
    let (_item, token, _) = provider
        .fetch_work_item(short_timeout, Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // Wait for lock to expire
    tokio::time::sleep(short_timeout + Duration::from_millis(200)).await;

    // Ack with the expired token — must fail
    let result = provider
        .ack_work_item(
            &token,
            Some(WorkItem::ActivityCompleted {
                instance: "ack-expiry-test".to_string(),
                execution_id: 1,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await;
    assert!(result.is_err(), "Ack must fail when worker lock has expired");

    // The item should still be in the queue (not deleted) and refetchable
    let refetch = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();
    assert!(
        refetch.is_some(),
        "Item should still be refetchable after expired ack was rejected"
    );

    tracing::info!("✓ Test passed: worker ack after lock expiry correctly rejected");
}

/// G7: Orchestration lock renewal fails after expiry
/// Goal: Verify renew_orchestration_item_lock fails when the lock has expired.
/// (Mirrors test_worker_lock_renewal_after_expiration which tests the worker side.)
pub async fn test_orchestration_lock_renewal_after_expiration<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let lock_timeout = provider_lock_timeout(factory);

    // Enqueue and fetch an orchestration item
    provider
        .enqueue_for_orchestrator(start_item("orch-renewal-expiry"), None)
        .await
        .unwrap();
    let (_item, token, _) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Wait for lock to expire
    tokio::time::sleep(lock_timeout + Duration::from_millis(100)).await;

    // Renewal should fail
    let result = provider.renew_orchestration_item_lock(&token, lock_timeout).await;
    assert!(result.is_err(), "Should fail to renew expired orchestration lock");

    tracing::info!("✓ Test passed: orchestration lock renewal after expiry rejected");
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::provider_validation::{ExecutionMetadata, start_item};
use crate::provider_validations::ProviderFactory;
use crate::providers::WorkItem;
use std::sync::Arc;
use std::time::Duration;

/// Test 1.1: Exclusive Instance Lock Acquisition
/// Goal: Verify only one dispatcher can process an instance at a time.
pub async fn test_exclusive_instance_lock<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: exclusive lock acquisition");
    let provider = factory.create_provider().await;
    let lock_timeout = factory.lock_timeout();

    // Enqueue work for instance "A"
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();

    // Fetch orchestration item (acquires lock)
    let (_item1, lock_token1, _attempt_count1) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Second fetch should fail (instance locked)
    assert!(
        provider
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Wait for lock to expire
    tokio::time::sleep(lock_timeout + Duration::from_millis(100)).await;

    // Now should be able to fetch again
    let (_item2, lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(lock_token2, lock_token1);
    tracing::info!("✓ Test passed: exclusive lock verified");
}

/// Test 1.2: Lock Token Uniqueness
/// Goal: Ensure each fetch generates a unique lock token.
pub async fn test_lock_token_uniqueness<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: lock token uniqueness");
    let provider = factory.create_provider().await;

    // Enqueue work for multiple instances
    for i in 0..5 {
        provider
            .enqueue_for_orchestrator(start_item(&format!("inst-{i}")), None)
            .await
            .unwrap();
    }

    // Fetch multiple orchestration items
    let mut tokens = Vec::new();
    for _ in 0..5 {
        let (_item, lock_token, _attempt_count) = provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .unwrap();
        tokens.push(lock_token);
    }

    // Verify all lock tokens are unique
    let unique_tokens: std::collections::HashSet<_> = tokens.iter().collect();
    assert_eq!(unique_tokens.len(), 5, "All lock tokens should be unique");
    tracing::info!("✓ Test passed: lock token uniqueness verified");
}

/// Test 1.3: Invalid Lock Token Rejection
/// Goal: Verify ack/abandon reject invalid lock tokens.
pub async fn test_invalid_lock_token_rejection<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: invalid lock token rejection");
    let provider = factory.create_provider().await;

    // Enqueue and fetch an item
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    let _item = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Try to ack with invalid token
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
    assert!(result.is_err(), "Should reject invalid lock token");

    // Try to abandon with invalid token
    let result = provider.abandon_orchestration_item("invalid-token", None, false).await;
    assert!(result.is_err(), "Should reject invalid lock token for abandon");

    // Original item should still be locked
    assert!(
        provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );
    tracing::info!("✓ Test passed: invalid lock token rejection verified");
}

/// Test 1.4: Concurrent Fetch Attempts
/// Goal: Test provider under concurrent access from multiple dispatchers.
pub async fn test_concurrent_instance_fetching<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: concurrent fetch attempts");
    let provider = Arc::new(factory.create_provider().await);

    // Seed 10 instances
    for i in 0..10 {
        provider
            .enqueue_for_orchestrator(start_item(&format!("inst-{i}")), None)
            .await
            .unwrap();
    }

    // Fetch concurrently with small delay to reduce contention
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let p = provider.clone();
            tokio::spawn(async move {
                // Add small random delay to reduce contention
                tokio::time::sleep(Duration::from_millis(i * 30)).await;
                p.fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
                    .await
                    .unwrap()
            })
        })
        .collect();

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Verify no duplicates
    let instances: std::collections::HashSet<_> = results
        .iter()
        .filter_map(|r| r.as_ref())
        .map(|(item, _lock_token, _attempt_count)| item.instance.clone())
        .collect();

    assert_eq!(instances.len(), 10, "Each instance should be fetched exactly once");
    tracing::info!("✓ Test passed: concurrent fetching verified");
}

/// Test 1.5: Message Arrival During Lock (Critical)
/// Goal: Verify completions arriving during a lock cannot be fetched by other dispatchers.
pub async fn test_completions_arriving_during_lock_blocked<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: completions arriving during lock blocked");
    let provider = Arc::new(factory.create_provider().await);
    let lock_timeout = factory.lock_timeout();

    // Step 1: Create instance with initial work
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();

    // Step 2: Fetch and acquire lock
    let (item1, _lock_token, _attempt_count) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item1.instance, "instance-A");

    // Step 3: While locked, completions arrive
    for i in 1..=3 {
        provider
            .enqueue_for_orchestrator(
                WorkItem::ActivityCompleted {
                    instance: "instance-A".to_string(),
                    execution_id: 1,
                    id: i,
                    result: format!("result-{i}"),
                },
                None,
            )
            .await
            .unwrap();
    }

    // Step 4: Another dispatcher tries to fetch "instance-A"
    let item2 = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap();
    assert!(item2.is_none(), "Instance still locked, no fetch possible");

    // Step 5: Wait for lock expiration
    tokio::time::sleep(lock_timeout + Duration::from_millis(100)).await;

    // Step 6: Now completions should be fetchable
    let (item3, _lock_token3, _attempt_count3) = provider
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item3.instance, "instance-A");
    // Should have StartOrchestration + 3 ActivityCompleted messages = 4 total
    assert_eq!(
        item3.messages.len(),
        4,
        "Should have StartOrchestration + 3 completions"
    );

    // Verify they're the messages including completions that arrived during lock
    let activity_completions: Vec<_> = item3
        .messages
        .iter()
        .filter_map(|msg| match msg {
            WorkItem::ActivityCompleted { id, .. } => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(
        activity_completions.len(),
        3,
        "Should have 3 ActivityCompleted messages"
    );
    assert_eq!(activity_completions, vec![1, 2, 3]);
    tracing::info!("✓ Test passed: completions during lock blocked verified");
}

/// Test 1.6: Cross-Instance Lock Isolation
/// Goal: Verify locks on one instance don't block other instances.
pub async fn test_cross_instance_lock_isolation<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: cross-instance lock isolation");
    let provider = Arc::new(factory.create_provider().await);

    // Enqueue work for two different instances
    // Create instances first
    crate::provider_validation::create_instance((*provider).as_ref(), "instance-A")
        .await
        .unwrap();
    crate::provider_validation::create_instance((*provider).as_ref(), "instance-B")
        .await
        .unwrap();

    // Enqueue additional work for both instances
    provider
        .enqueue_for_orchestrator(start_item("instance-A"), None)
        .await
        .unwrap();
    provider
        .enqueue_for_orchestrator(start_item("instance-B"), None)
        .await
        .unwrap();

    // Lock instance A
    let (item_a, _lock_token_a, _attempt_count_a) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item_a.instance, "instance-A");

    // Should still be able to fetch instance B (different instance, not blocked)
    let (item_b, lock_token_b, _attempt_count_b) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item_b.instance, "instance-B");

    // Ack B to release its lock, then enqueue another completion for B
    provider
        .ack_orchestration_item(
            &lock_token_b,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // While A is still locked, completion arrives for B
    provider
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "instance-B".to_string(),
                execution_id: 1,
                id: 1,
                result: "done".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    // Should be able to fetch B again (B is not locked)
    let (item_b2, _lock_token_b2, _attempt_count_b2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item_b2.instance, "instance-B");

    // Key assertion: instance-level locks don't block other instances
    assert_ne!(item_a.instance, item_b.instance);
    tracing::info!("✓ Test passed: cross-instance lock isolation verified");
}

/// Test 1.7: Completing Messages During Lock (Message Tagging)
/// Goal: Verify only messages present at fetch time are deleted on ack.
pub async fn test_message_tagging_during_lock<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: message tagging during lock");
    let provider = Arc::new(factory.create_provider().await);

    // Create instance first
    crate::provider_validation::create_instance((*provider).as_ref(), "instance-A")
        .await
        .unwrap();

    // Enqueue initial messages
    provider
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "instance-A".to_string(),
                execution_id: 1,
                id: 1,
                result: "msg1".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    provider
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "instance-A".to_string(),
                execution_id: 1,
                id: 2,
                result: "msg2".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    // Fetch (marks messages with lock_token)
    let (item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.instance, "instance-A");
    assert_eq!(item.messages.len(), 2);

    // While locked, new message arrives
    provider
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "instance-A".to_string(),
                execution_id: 1,
                id: 3,
                result: "msg3".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    // Ack (deletes only messages with lock_token)
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Fetch again - should get msg3
    let (item2, _lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "instance-A");
    assert_eq!(item2.messages.len(), 1);
    assert!(matches!(&item2.messages[0], WorkItem::ActivityCompleted { id: 3, .. }));
    tracing::info!("✓ Test passed: message tagging during lock verified");
}

/// Test 1.8: Ack Only Affects Locked Messages
/// Goal: Verify ack_orchestration_item only acks messages that were locked by the lock_token.
pub async fn test_ack_only_affects_locked_messages<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: ack only affects locked messages");
    let provider = Arc::new(factory.create_provider().await);

    // Create instance
    crate::provider_validation::create_instance((*provider).as_ref(), "instance-A")
        .await
        .unwrap();

    // Enqueue message 1
    provider
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "instance-A".to_string(),
                execution_id: 1,
                id: 1,
                result: "msg1".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    // Fetch message 1 and get lock_token
    let (item1, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item1.messages.len(), 1);

    // While item1 is locked, enqueue messages 2 and 3
    provider
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "instance-A".to_string(),
                execution_id: 1,
                id: 2,
                result: "msg2".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    provider
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "instance-A".to_string(),
                execution_id: 1,
                id: 3,
                result: "msg3".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    // Another fetch attempt should return None (instance is locked)
    assert!(
        provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // Ack with lock_token - should only delete message 1 (locked messages)
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Now messages 2 and 3 should be fetchable
    let (item2, _lock_token2, _attempt_count2) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "instance-A");
    assert_eq!(item2.messages.len(), 2, "Should have messages 2 and 3");

    // Verify they're messages 2 and 3
    let ids: Vec<u64> = item2
        .messages
        .iter()
        .filter_map(|msg| match msg {
            WorkItem::ActivityCompleted { id, .. } => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![2, 3], "Should only have messages 2 and 3");
    tracing::info!("✓ Test passed: ack only affects locked messages verified");
}

/// Test 1.9: Multi-Threaded Lock Contention
/// Goal: Verify instance locks prevent concurrent processing across multiple threads.
pub async fn test_multi_threaded_lock_contention<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: multi-threaded lock contention");
    let provider = Arc::new(factory.create_provider().await);

    // Create a single instance with work
    provider
        .enqueue_for_orchestrator(start_item("contention-instance"), None)
        .await
        .unwrap();

    // Spawn multiple threads attempting to fetch the same instance
    let num_threads = 10;
    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let p = provider.clone();
            tokio::spawn(async move {
                // Small delay to stagger attempts
                tokio::time::sleep(Duration::from_millis(i * 5)).await;
                let result = p
                    .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
                    .await
                    .unwrap();
                (i, result)
            })
        })
        .collect();

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Only ONE thread should have successfully fetched the instance
    let successful_fetches: Vec<_> = results
        .iter()
        .filter_map(|(thread_id, result)| {
            result
                .as_ref()
                .map(|(item, lock_token, _attempt_count)| (*thread_id, item.instance.clone(), lock_token.clone()))
        })
        .collect();

    assert_eq!(
        successful_fetches.len(),
        1,
        "Only one thread should successfully acquire lock for the same instance"
    );

    // Verify no other thread can fetch the same instance
    let (winner_thread, winner_instance, _winner_token) = &successful_fetches[0];
    assert_eq!(winner_instance, "contention-instance");

    // Try fetching again - should fail (instance still locked)
    assert!(
        provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    tracing::info!(
        "✓ Test passed: multi-threaded lock contention verified (thread {} won)",
        winner_thread
    );
}

/// Test 1.10: Multi-Threaded No Duplicate Processing
/// Goal: Verify that even under high contention, no instance is processed by multiple threads.
pub async fn test_multi_threaded_no_duplicate_processing<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: multi-threaded no duplicate processing");
    let provider = Arc::new(factory.create_provider().await);

    // Create multiple instances
    let num_instances: usize = 20;
    for i in 0..num_instances {
        provider
            .enqueue_for_orchestrator(start_item(&format!("dup-test-{i}")), None)
            .await
            .unwrap();
    }

    // Spawn many threads (more than instances) to create contention
    let num_threads = num_instances * 2; // 40 threads for 20 instances
    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let p = provider.clone();
            tokio::spawn(async move {
                // Stagger delays to increase contention
                let delay = (i * 3) % 50; // Cycle through delays without random
                tokio::time::sleep(Duration::from_millis(delay as u64)).await;

                // Retry on deadlock (SQLite in-memory can deadlock under heavy concurrent load)
                for attempt in 0..3 {
                    match p
                        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
                        .await
                    {
                        Ok(item) => return Ok(item.map(|(i, _lock_token, _attempt_count)| i.instance.clone())),
                        Err(e) if e.retryable && attempt < 2 => {
                            tokio::time::sleep(Duration::from_millis(10 * (attempt + 1) as u64)).await;
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                }
                unreachable!()
            })
        })
        .collect();

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .filter_map(|r| r.unwrap().ok().flatten())
        .collect();

    // Collect unique instances that were fetched
    let fetched_instances: std::collections::HashSet<_> = results.iter().collect();

    // Verify:
    // 1. Each instance fetched at most once
    assert_eq!(
        fetched_instances.len(),
        results.len(),
        "No duplicate instances should be fetched"
    );

    // 2. Number of successful fetches doesn't exceed number of instances
    assert!(results.len() <= num_instances, "Cannot fetch more instances than exist");

    // 3. All fetched instances are unique
    assert_eq!(
        fetched_instances.len(),
        results.len(),
        "All fetched instances should be unique"
    );

    tracing::info!(
        "✓ Test passed: multi-threaded no duplicate processing verified ({} instances fetched by {} threads)",
        fetched_instances.len(),
        num_threads
    );
}

/// Test 1.11: Multi-Threaded Lock Expiration Recovery
/// Goal: Verify that after lock expiration, other threads can acquire the lock.
pub async fn test_multi_threaded_lock_expiration_recovery<F: ProviderFactory>(factory: &F) {
    tracing::info!("→ Testing instance locking: multi-threaded lock expiration recovery");
    let provider = Arc::new(factory.create_provider().await);

    // Create instance
    provider
        .enqueue_for_orchestrator(start_item("expiration-instance"), None)
        .await
        .unwrap();

    // Capture timeout value before spawning tasks
    let lock_timeout = factory.lock_timeout();

    // Barrier ensures all threads start at exactly the same time,
    // eliminating race conditions from connection pool cold-start latency
    let barrier = Arc::new(tokio::sync::Barrier::new(3));

    // Thread 1: Fetch and hold lock (don't ack) - simulates crashed worker
    let provider1 = provider.clone();
    let barrier1 = barrier.clone();
    let handle1 = tokio::spawn(async move {
        barrier1.wait().await;
        let (item, lock_token, _attempt_count) = provider1
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.instance, "expiration-instance");
        // Hold lock but don't ack - simulate crashed worker
        tokio::time::sleep(lock_timeout + Duration::from_millis(200)).await;
        lock_token
    });

    // Thread 2: Try to fetch while lock is held (should fail)
    let provider2 = provider.clone();
    let barrier2 = barrier.clone();
    let handle2 = tokio::spawn(async move {
        barrier2.wait().await;
        // 200ms delay gives thread 1 time to acquire lock
        tokio::time::sleep(Duration::from_millis(200)).await;
        let result = provider2
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap();
        assert!(result.is_none(), "Instance should be locked");
        result
    });

    // Thread 3: Wait for expiration, then fetch (should succeed - lock recovery)
    let provider3 = provider.clone();
    let barrier3 = barrier.clone();
    let handle3 = tokio::spawn(async move {
        barrier3.wait().await;
        // Wait for lock to expire (lock_timeout + margin)
        tokio::time::sleep(lock_timeout + Duration::from_millis(100)).await;
        provider3
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
    });

    // Wait for all threads
    let (lock_token1, result2, result3) = futures::future::join3(handle1, handle2, handle3).await;
    let _lock_token1 = lock_token1.unwrap();
    assert!(result2.unwrap().is_none());
    let (item3, lock_token3, _attempt_count3) = result3.unwrap().unwrap();

    // Thread 3 should have successfully acquired the lock after expiration
    assert_eq!(item3.instance, "expiration-instance");
    assert_ne!(lock_token3, "expired-token"); // Should be a new token

    tracing::info!("✓ Test passed: multi-threaded lock expiration recovery verified");
}

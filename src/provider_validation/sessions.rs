//! Session Routing Provider Validation Tests
//!
//! Tests that validate the implicit session routing contract for providers.
//! Sessions pin activities with the same session_id to the same worker.

use crate::provider_validation::ProviderFactory;
use crate::providers::{SessionFetchConfig, TagFilter, WorkItem};
use std::time::Duration;

/// Helper to create a session-bound activity work item
fn session_activity(instance: &str, id: u64, session_id: &str) -> WorkItem {
    WorkItem::ActivityExecute {
        instance: instance.to_string(),
        execution_id: 1,
        id,
        name: "SessionActivity".to_string(),
        input: "{}".to_string(),
        session_id: Some(session_id.to_string()),
        tag: None,
    }
}

/// Helper to create a non-session activity work item
fn plain_activity(instance: &str, id: u64) -> WorkItem {
    WorkItem::ActivityExecute {
        instance: instance.to_string(),
        execution_id: 1,
        id,
        name: "PlainActivity".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    }
}

/// Helper to build a SessionFetchConfig with standard lock timeout
fn session_config(owner_id: &str) -> SessionFetchConfig {
    SessionFetchConfig {
        owner_id: owner_id.to_string(),
        lock_timeout: Duration::from_secs(30),
    }
}

/// Helper to build a SessionFetchConfig with custom lock timeout
fn session_config_with_lock(owner_id: &str, lock_timeout: Duration) -> SessionFetchConfig {
    SessionFetchConfig {
        owner_id: owner_id.to_string(),
        lock_timeout,
    }
}

// ============================================================================
// Basic Session Routing Tests
// ============================================================================

/// Non-session items are always fetchable by any worker
pub async fn test_non_session_items_fetchable_by_any_worker(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    provider.enqueue_for_worker(plain_activity("inst-1", 1)).await.unwrap();

    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result.is_some(), "Non-session item should be fetchable by any worker");
}

/// Session items are fetchable when no session exists (claiming)
pub async fn test_session_item_claimable_when_no_session(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();

    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result.is_some(), "Unclaimed session item should be fetchable");

    // Verify the item has the session_id
    if let Some((WorkItem::ActivityExecute { session_id, .. }, _, _)) = &result {
        assert_eq!(session_id.as_deref(), Some("session-1"));
    } else {
        panic!("Expected ActivityExecute work item");
    }
}

/// After claiming a session, same worker gets subsequent session items
pub async fn test_session_affinity_same_worker(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Enqueue first session item and fetch (claims session)
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let (_, token1, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token1, None).await.unwrap();

    // Enqueue second item with same session
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "session-1"))
        .await
        .unwrap();

    // Same worker should still get it
    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result.is_some(),
        "Owned session item should be fetchable by owning worker"
    );
}

/// A different worker cannot fetch items for a session owned by another worker
pub async fn test_session_affinity_blocks_other_worker(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims session-1
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let (_, token1, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token1, None).await.unwrap();

    // Enqueue another item for same session
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "session-1"))
        .await
        .unwrap();

    // Worker-B should NOT get the session item (session owned by worker-A)
    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result.is_none(),
        "Non-owning worker should not fetch owned session items"
    );
}

/// Different sessions can be owned by different workers
pub async fn test_different_sessions_different_workers(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims session-1
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let (_, token1, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token1, None).await.unwrap();

    // Worker-B claims session-2
    provider
        .enqueue_for_worker(session_activity("inst-2", 1, "session-2"))
        .await
        .unwrap();
    let (_, token2, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token2, None).await.unwrap();

    // Now enqueue more items for each session
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "session-1"))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(session_activity("inst-2", 2, "session-2"))
        .await
        .unwrap();

    // Worker-A gets session-1 item
    let result_a = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result_a.is_some(), "Worker-A should fetch session-1 item");
    if let Some((WorkItem::ActivityExecute { session_id, .. }, _, _)) = &result_a {
        assert_eq!(session_id.as_deref(), Some("session-1"));
    }

    // Worker-B gets session-2 item
    let result_b = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result_b.is_some(), "Worker-B should fetch session-2 item");
    if let Some((WorkItem::ActivityExecute { session_id, .. }, _, _)) = &result_b {
        assert_eq!(session_id.as_deref(), Some("session-2"));
    }
}

/// A mix of session and non-session items works correctly
pub async fn test_mixed_session_and_non_session_items(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A owns session-1
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // Enqueue a session item and a plain item
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "session-1"))
        .await
        .unwrap();
    provider.enqueue_for_worker(plain_activity("inst-2", 1)).await.unwrap();

    // Worker-B should get the plain item (can't get session-1)
    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result.is_some(), "Worker-B should get the plain item");
    if let Some((WorkItem::ActivityExecute { session_id, .. }, _, _)) = &result {
        assert!(session_id.is_none(), "Worker-B should get a non-session item");
    }
}

// ============================================================================
// Session Lock / Expiration Tests
// ============================================================================

/// After session lock expires, another worker can claim the session
pub async fn test_session_claimable_after_lock_expiry(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims session with very short lock
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let cfg = session_config_with_lock("worker-A", Duration::from_millis(50));
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // Wait for session lock to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Enqueue another session-1 item
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "session-1"))
        .await
        .unwrap();

    // Worker-B should now be able to claim session-1
    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result.is_some(), "Worker-B should claim expired session");
}

/// When session=None, fetch_work_item only returns non-session items
pub async fn test_none_session_skips_session_items(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Enqueue a session item and a plain item
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    provider.enqueue_for_worker(plain_activity("inst-2", 2)).await.unwrap();

    // Fetch with session=None — should only get the plain item
    let result = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();
    assert!(result.is_some(), "Should fetch a non-session item with session=None");
    if let Some((WorkItem::ActivityExecute { session_id, id, .. }, token, _)) = result {
        assert!(session_id.is_none(), "Should be a non-session item");
        assert_eq!(id, 2, "Should get the plain activity");
        provider.ack_work_item(&token, None).await.unwrap();
    }

    // Second fetch with session=None — no more non-session items
    let result2 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap();
    assert!(result2.is_none(), "Session item should be invisible to None fetch");

    // Fetch with session=Some — should get the session item
    let result3 = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result3.is_some(), "Session item should be visible with Some config");
    if let Some((WorkItem::ActivityExecute { session_id, .. }, _, _)) = &result3 {
        assert_eq!(session_id.as_deref(), Some("session-1"));
    }
}

/// When session=Some, provider returns both session and non-session items
pub async fn test_some_session_returns_all_items(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Enqueue items for 3 different sessions + one plain item
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "sess-A"))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(session_activity("inst-2", 2, "sess-B"))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(session_activity("inst-3", 3, "sess-C"))
        .await
        .unwrap();
    provider.enqueue_for_worker(plain_activity("inst-4", 4)).await.unwrap();

    // Fetch all 4 with session=Some — provider has no capacity limit, all are claimable
    let mut fetched_ids = Vec::new();
    for _ in 0..4 {
        let (item, token, _) = provider
            .fetch_work_item(
                Duration::from_secs(5),
                Duration::ZERO,
                Some(&session_config("shared-proc")),
                &TagFilter::default(),
            )
            .await
            .unwrap()
            .expect("Should fetch item");
        if let WorkItem::ActivityExecute { id, .. } = &item {
            fetched_ids.push(*id);
        }
        provider.ack_work_item(&token, None).await.unwrap();
    }
    assert_eq!(
        fetched_ids,
        vec![1, 2, 3, 4],
        "All items should be fetched in FIFO order"
    );

    // No more items
    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("shared-proc")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result.is_none(), "Queue should be empty");
}

// ============================================================================
// Session Renewal Tests
// ============================================================================

/// renew_session_lock extends lock for actively-used sessions
pub async fn test_renew_session_lock_active(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims a session
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let cfg = session_config_with_lock("worker-A", Duration::from_secs(5));
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // Renew session lock (idle_timeout = 300s so it's considered active)
    let count = provider
        .renew_session_lock(&["worker-A"], Duration::from_secs(30), Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(count, 1, "Should renew 1 active session");

    // Claim a second session for the same worker
    provider
        .enqueue_for_worker(session_activity("inst-2", 1, "session-2"))
        .await
        .unwrap();
    let (_, token2, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token2, None).await.unwrap();

    // Renew again — should now renew both sessions
    let count2 = provider
        .renew_session_lock(&["worker-A"], Duration::from_secs(30), Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(count2, 2, "Should renew 2 active sessions");
}

/// renew_session_lock skips idle sessions
pub async fn test_renew_session_lock_skips_idle(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims a session
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // Renew with idle_timeout = 0ms (everything is idle)
    let count = provider
        .renew_session_lock(&["worker-A"], Duration::from_secs(30), Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(count, 0, "Should skip idle session");
}

/// renew_session_lock for worker with no sessions returns 0
pub async fn test_renew_session_lock_no_sessions(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    let count = provider
        .renew_session_lock(&["worker-A"], Duration::from_secs(30), Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(count, 0, "Worker with no sessions should renew 0");
}

// ============================================================================
// Session Cleanup Tests
// ============================================================================

/// cleanup_orphaned_sessions removes expired sessions with no pending items
pub async fn test_cleanup_removes_expired_no_items(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims a session with short lock
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let cfg = session_config_with_lock("worker-A", Duration::from_millis(50));
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // Wait for session lock to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cleanup should remove the expired session (no pending items)
    let count = provider
        .cleanup_orphaned_sessions(Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(count, 1, "Should clean up 1 expired session");
}

/// cleanup_orphaned_sessions does not remove sessions with pending items
pub async fn test_cleanup_keeps_sessions_with_pending_items(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims a session with short lock
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let cfg = session_config_with_lock("worker-A", Duration::from_millis(50));
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // Enqueue another item for the same session (pending)
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "session-1"))
        .await
        .unwrap();

    // Wait for session lock to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cleanup should NOT remove session (has pending items)
    let count = provider
        .cleanup_orphaned_sessions(Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(count, 0, "Should not clean up session with pending items");
}

/// cleanup_orphaned_sessions does not remove active sessions
pub async fn test_cleanup_keeps_active_sessions(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims a session with long lock
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let cfg = session_config_with_lock("worker-A", Duration::from_secs(300));
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // Cleanup should NOT remove active session
    let count = provider
        .cleanup_orphaned_sessions(Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(count, 0, "Should not clean up active session");
}

// ============================================================================
// Session + Activity Completion Piggybacking
// ============================================================================

/// Acking a work item with a session updates last_activity_at.
/// Uses a tight idle_timeout to prove ack actually bumped the timestamp —
/// without the piggyback, the session would appear idle and renew would return 0.
pub async fn test_ack_updates_session_last_activity(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let idle_window = Duration::from_millis(200);

    // Enqueue and fetch a session-bound item
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();

    // Wait longer than idle_window so the fetch timestamp becomes stale.
    // If ack doesn't bump last_activity_at, the session will look idle.
    tokio::time::sleep(idle_window + Duration::from_millis(50)).await;

    // Ack — this should piggyback-update last_activity_at to now
    provider
        .ack_work_item(
            &token,
            Some(WorkItem::ActivityCompleted {
                instance: "inst-1".to_string(),
                execution_id: 1,
                id: 1,
                result: "ok".to_string(),
            }),
        )
        .await
        .unwrap();

    // Renew with the tight idle_window.
    // Only passes if ack bumped last_activity_at to a time within the last 200ms.
    let count = provider
        .renew_session_lock(&["worker-A"], Duration::from_secs(30), idle_window)
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "Session should be active — ack must have updated last_activity_at"
    );
}

/// Lock renewal for a session-bound work item updates last_activity_at.
/// Same tight-idle-timeout technique as above to prove the piggyback works.
pub async fn test_renew_work_item_updates_session_last_activity(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;
    let idle_window = Duration::from_millis(200);

    // Enqueue and fetch a session-bound item
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();

    // Wait longer than idle_window so the fetch timestamp becomes stale
    tokio::time::sleep(idle_window + Duration::from_millis(50)).await;

    // Renew the work item lock — should piggyback-update last_activity_at
    provider
        .renew_work_item_lock(&token, Duration::from_secs(30))
        .await
        .unwrap();

    // Renew with tight idle_window — only passes if renew_work_item_lock bumped last_activity_at
    let count = provider
        .renew_session_lock(&["worker-A"], Duration::from_secs(30), idle_window)
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "Session should be active — work item lock renewal must have updated last_activity_at"
    );
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Multiple items for the same session queued in order
pub async fn test_session_items_processed_in_order(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Enqueue 3 items for same session
    for id in 1..=3 {
        provider
            .enqueue_for_worker(session_activity("inst-1", id, "session-1"))
            .await
            .unwrap();
    }

    // Fetch all 3 in order by the same worker
    for expected_id in 1..=3u64 {
        let (item, token, _) = provider
            .fetch_work_item(
                Duration::from_secs(5),
                Duration::ZERO,
                Some(&session_config("worker-A")),
                &TagFilter::default(),
            )
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("Should fetch session item {expected_id}"));

        if let WorkItem::ActivityExecute { id, .. } = &item {
            assert_eq!(*id, expected_id, "Items should be fetched in FIFO order");
        }
        provider.ack_work_item(&token, None).await.unwrap();
    }
}

/// Non-session items are always returned even when using session config
pub async fn test_non_session_items_returned_with_session_config(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Own a session
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "session-1"))
        .await
        .unwrap();
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // Enqueue a plain (non-session) item
    provider.enqueue_for_worker(plain_activity("inst-2", 1)).await.unwrap();

    // With session config, non-session items should still be returned
    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result.is_some(),
        "Non-session items should be returned with session config"
    );
    if let Some((WorkItem::ActivityExecute { session_id, .. }, _, _)) = &result {
        assert!(session_id.is_none(), "Should be a non-session item");
    }
}

// ============================================================================
// Process-Level Session Identity Tests
// ============================================================================

/// When multiple callers use the same worker_id (process-level identity),
/// any of them can fetch items for an owned session.
pub async fn test_shared_worker_id_any_caller_can_fetch_owned_session(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Enqueue two session items for the same session
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "shared-sess"))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "shared-sess"))
        .await
        .unwrap();

    // First fetch claims the session for "process-1" (simulating any slot in that process)
    let (item1, token1, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("process-1")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    if let WorkItem::ActivityExecute { id, .. } = &item1 {
        assert_eq!(*id, 1);
    }
    provider.ack_work_item(&token1, None).await.unwrap();

    // Second fetch with the SAME worker_id should get the second item
    // (simulates a different slot in the same process using the same identity)
    let (item2, token2, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("process-1")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    if let WorkItem::ActivityExecute { id, .. } = &item2 {
        assert_eq!(*id, 2);
    }
    provider.ack_work_item(&token2, None).await.unwrap();

    // A different worker cannot fetch items for this session while owned
    provider
        .enqueue_for_worker(session_activity("inst-1", 3, "shared-sess"))
        .await
        .unwrap();
    let other = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("process-2")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        other.is_none(),
        "Different process should not get items for an owned session"
    );
}

// ============================================================================
// Race Condition / Concurrency Tests
// ============================================================================

/// When two workers race to claim the same unclaimed session, only one succeeds.
/// The loser gets None (the upsert's WHERE clause blocks the second claimant).
pub async fn test_concurrent_session_claim_only_one_wins(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Enqueue two items on the same unclaimed session
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "contested-sess"))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "contested-sess"))
        .await
        .unwrap();

    // Worker-A claims first
    let result_a = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result_a.is_some(), "Worker-A should claim the session");
    // Don't ack yet — worker-A holds the session lock

    // Worker-B tries to get the second item for the same session
    let result_b = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result_b.is_none(),
        "Worker-B should NOT get session items while A holds the session lock"
    );

    // Ack A's item, then B should be able to claim the session
    let (_, token_a, _) = result_a.unwrap();
    provider.ack_work_item(&token_a, None).await.unwrap();
}

/// After a worker claims a session then the lock expires, another worker
/// can claim the same session (takeover). Verifies the upsert WHERE clause
/// allows expired-lock takeover.
pub async fn test_session_takeover_after_lock_expiry(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims session with a very short lock
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "takeover-sess"))
        .await
        .unwrap();
    let cfg_a = session_config_with_lock("worker-A", Duration::from_millis(50));
    let (_, token_a, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg_a),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token_a, None).await.unwrap();

    // Enqueue another item for the same session
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "takeover-sess"))
        .await
        .unwrap();

    // Wait for session lock to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Worker-B should now be able to take over the session
    let result_b = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result_b.is_some(),
        "Worker-B should take over session after A's lock expired"
    );
    if let Some((WorkItem::ActivityExecute { session_id, .. }, _, _)) = &result_b {
        assert_eq!(session_id.as_deref(), Some("takeover-sess"));
    }
}

/// Cleanup can delete a session row, then new items arrive for that session.
/// The next fetch_work_item should re-create the session via upsert.
/// Validates idempotent session re-creation after cleanup.
pub async fn test_cleanup_then_new_item_recreates_session(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims a session with a very short lock
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "ephemeral-sess"))
        .await
        .unwrap();
    let cfg = session_config_with_lock("worker-A", Duration::from_millis(50));
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // Wait for session lock to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cleanup removes the expired session (no pending items)
    let cleaned = provider
        .cleanup_orphaned_sessions(Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(cleaned, 1, "Should clean up the expired session");

    // Now enqueue a new item for the same session_id
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "ephemeral-sess"))
        .await
        .unwrap();

    // Any worker should be able to claim the session fresh (row was deleted)
    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result.is_some(), "Session should be re-creatable after cleanup");
    if let Some((WorkItem::ActivityExecute { session_id, id, .. }, _, _)) = &result {
        assert_eq!(session_id.as_deref(), Some("ephemeral-sess"));
        assert_eq!(*id, 2);
    }
}

/// Abandoned work items are retried and attempt_count reflects at-least-once delivery.
/// When a worker fetches then abandons, the item becomes visible again for re-fetch.
pub async fn test_abandoned_session_item_retryable(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Enqueue a session-bound item
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "retry-sess"))
        .await
        .unwrap();

    // Worker-A fetches it
    let (item1, token1, attempt1) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt1, 1);
    if let WorkItem::ActivityExecute { id, .. } = &item1 {
        assert_eq!(*id, 1);
    }

    // Abandon without completing (simulates ack failure / crash)
    provider.abandon_work_item(&token1, None, false).await.unwrap();

    // Same worker re-fetches — attempt_count should be 2
    let (item2, token2, attempt2) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt2, 2, "Retry should increment attempt count");
    if let WorkItem::ActivityExecute { id, .. } = &item2 {
        assert_eq!(*id, 1, "Should be the same item");
    }

    // Ack successfully this time
    provider.ack_work_item(&token2, None).await.unwrap();

    // No more items
    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result.is_none(), "No items should remain after successful ack");
}

/// When ignore_attempt is true, abandoning a session item does NOT
/// inflate the attempt count, preventing false poison message detection.
pub async fn test_abandoned_session_item_ignore_attempt(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Enqueue a session-bound item
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "ignore-sess"))
        .await
        .unwrap();

    // Fetch (attempt_count becomes 1)
    let (_, token, attempt) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt, 1);

    // Abandon with ignore_attempt=true (e.g., session capacity race)
    provider
        .abandon_work_item(&token, Some(Duration::from_millis(1)), true)
        .await
        .unwrap();

    // Small delay for visible_at
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Re-fetch: attempt_count should be back to 1 (decremented then re-incremented)
    let (_, token2, attempt2) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt2, 1, "ignore_attempt should not inflate attempt count");
    provider.ack_work_item(&token2, None).await.unwrap();
}

/// G3: renew_session_lock after session lock already expired returns zero.
/// The session lock has lapsed, so renewal should not re-acquire it.
pub async fn test_renew_session_lock_after_expiry_returns_zero(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Claim a session with a very short lock
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "expired-sess"))
        .await
        .unwrap();
    let cfg = session_config_with_lock("worker-A", Duration::from_millis(50));
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // Wait for session lock to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Renewal should return 0 — the lock expired, no active session to renew
    let count = provider
        .renew_session_lock(&["worker-A"], Duration::from_secs(30), Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(count, 0, "renew_session_lock should return 0 for expired session");
}

/// G4: Original worker can re-claim its own expired session.
/// After a session lock expires, the original worker should be able to fetch
/// and re-claim the session (treated as an unclaimed session).
pub async fn test_original_worker_reclaims_expired_session(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A claims a session with a short lock
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "reclaim-sess"))
        .await
        .unwrap();
    let cfg = session_config_with_lock("worker-A", Duration::from_millis(50));
    let (_, token1, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token1, None).await.unwrap();

    // Enqueue another item for the same session
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "reclaim-sess"))
        .await
        .unwrap();

    // Wait for session lock to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Worker-A re-fetches — should re-claim the expired session
    let result = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result.is_some(), "Original worker should re-claim expired session");
    if let Some((WorkItem::ActivityExecute { id, session_id, .. }, _, _)) = &result {
        assert_eq!(*id, 2);
        assert_eq!(session_id.as_deref(), Some("reclaim-sess"));
    }
}

/// G5: Activity lock expires while session lock is still valid.
/// The work item lock expires and the item becomes re-fetchable, but the session
/// is still pinned to the original worker. The redelivered item should go to the
/// same worker (session affinity survives activity lock expiry).
pub async fn test_activity_lock_expires_session_lock_valid_same_worker_refetches(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A fetches a session-bound item with a SHORT work item lock (1s)
    // but a LONG session lock (30s via default session_config)
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "sticky-sess"))
        .await
        .unwrap();
    let short_work_lock = Duration::from_millis(500);
    let cfg = session_config("worker-A"); // session lock = 30s
    let (_, _token_a, _) = provider
        .fetch_work_item(short_work_lock, Duration::ZERO, Some(&cfg), &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // Don't ack — let the work item lock expire
    tokio::time::sleep(short_work_lock + Duration::from_millis(100)).await;

    // Worker-B tries to fetch — should NOT get it because session is still
    // owned by Worker-A (session lock has 30s timeout)
    let result_b = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result_b.is_none(),
        "Worker-B should not get session-bound item — session still owned by A"
    );

    // Worker-A re-fetches — should get the redelivered item
    let result_a = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result_a.is_some(),
        "Worker-A should reclaim the redelivered item via session affinity"
    );
    if let Some((WorkItem::ActivityExecute { id, .. }, token, attempt)) = result_a {
        assert_eq!(id, 1, "Should be the same item");
        assert_eq!(attempt, 2, "Attempt count should have incremented");
        provider.ack_work_item(&token, None).await.unwrap();
    }
}

/// G5b: Session lock expires before activity lock — different worker claims session,
/// then the original activity lock expires and the redelivered item goes to the new owner.
pub async fn test_session_lock_expires_new_owner_gets_redelivery(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A: SHORT session lock (200ms), LONG activity lock (5s)
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "takeover2-sess"))
        .await
        .unwrap();
    let short_session = Duration::from_millis(200);
    let long_activity = Duration::from_secs(5);
    let cfg_a = session_config_with_lock("worker-A", short_session);
    let (_, _token_a, _) = provider
        .fetch_work_item(long_activity, Duration::ZERO, Some(&cfg_a), &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // Wait for session lock to expire, activity lock still valid
    tokio::time::sleep(short_session + Duration::from_millis(100)).await;

    // Enqueue a NEW item for the same session
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "takeover2-sess"))
        .await
        .unwrap();

    // Worker-B claims the session (session lock expired) and gets the new item
    let cfg_b = session_config("worker-B");
    let result_b = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg_b),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result_b.is_some(),
        "Worker-B should claim the expired session and get item 2"
    );
    if let Some((WorkItem::ActivityExecute { id, session_id, .. }, token_b, _)) = &result_b {
        assert_eq!(*id, 2, "Worker-B should get the NEW item");
        assert_eq!(session_id.as_deref(), Some("takeover2-sess"));
        provider.ack_work_item(token_b, None).await.unwrap();
    }

    // Now abandon Worker-A's item (simulating activity timeout) so it becomes re-fetchable
    // In real runtime, the activity lock would expire and the item would be retried.
    let _ = provider.abandon_work_item(&_token_a, None, false).await;

    // Worker-B (now session owner) should pick up the redelivered item 1
    let result_b2 = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg_b),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result_b2.is_some(),
        "Worker-B should get redelivered item 1 as new session owner"
    );
    if let Some((WorkItem::ActivityExecute { id, .. }, token, attempt)) = result_b2 {
        assert_eq!(id, 1, "Should be the original item");
        assert_eq!(attempt, 2, "Attempt count should have incremented");
        provider.ack_work_item(&token, None).await.unwrap();
    }
}

/// G5c: Session lock expires — same worker reacquires session on next fetch.
/// Worker-A's session lock expires, but Worker-A is the one that fetches next.
/// It should reclaim the session (upsert updates owner to A again).
pub async fn test_session_lock_expires_same_worker_reacquires(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A: SHORT session lock (200ms)
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "reacq-sess"))
        .await
        .unwrap();
    let short_session = Duration::from_millis(200);
    let cfg_a = session_config_with_lock("worker-A", short_session);
    let (_, token_a, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg_a),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token_a, None).await.unwrap();

    // Wait for session lock to expire
    tokio::time::sleep(short_session + Duration::from_millis(100)).await;

    // Enqueue a new item for the same session
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "reacq-sess"))
        .await
        .unwrap();

    // Worker-A fetches again — should reclaim its own expired session
    let result_a = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg_a),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(result_a.is_some(), "Worker-A should reacquire its own expired session");
    if let Some((WorkItem::ActivityExecute { id, session_id, .. }, token, _)) = result_a {
        assert_eq!(id, 2);
        assert_eq!(session_id.as_deref(), Some("reacq-sess"));
        provider.ack_work_item(&token, None).await.unwrap();
    }
}

/// G6: Both activity lock and session lock expire — different worker claims the item.
/// When both locks have lapsed, the session is unclaimed and any worker can pick it up.
pub async fn test_both_locks_expire_different_worker_claims(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A fetches with short work lock AND short session lock
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "both-expire-sess"))
        .await
        .unwrap();
    let short_lock = Duration::from_millis(200);
    let cfg = session_config_with_lock("worker-A", short_lock);
    let (_, _token_a, _) = provider
        .fetch_work_item(short_lock, Duration::ZERO, Some(&cfg), &TagFilter::default())
        .await
        .unwrap()
        .unwrap();

    // Don't ack — let both locks expire
    tokio::time::sleep(short_lock + Duration::from_millis(200)).await;

    // Worker-B should now be able to claim both the item and the session
    let result_b = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result_b.is_some(),
        "Worker-B should claim item after both locks expired"
    );
    if let Some((WorkItem::ActivityExecute { id, session_id, .. }, token, attempt)) = result_b {
        assert_eq!(id, 1, "Should be the same item");
        assert_eq!(session_id.as_deref(), Some("both-expire-sess"));
        assert_eq!(attempt, 2, "Attempt count should have incremented");
        provider.ack_work_item(&token, None).await.unwrap();
    }
}

/// G7: Session lock expires while activity lock is still valid.
/// The worker should still be able to ack its in-flight activity — session ownership
/// is a routing concern and does not govern individual activity locks.
pub async fn test_session_lock_expires_activity_lock_valid_ack_succeeds(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Worker-A fetches with a SHORT session lock (200ms) but a LONG work item lock (30s)
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "sess-expires"))
        .await
        .unwrap();
    let short_session_lock = Duration::from_millis(200);
    let cfg = session_config_with_lock("worker-A", short_session_lock);
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(30),
            Duration::ZERO,
            Some(&cfg),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();

    // Wait for session lock to expire, but activity lock is still valid
    tokio::time::sleep(short_session_lock + Duration::from_millis(100)).await;

    // Worker-A can still ack — activity lock is independent of session lock
    let result = provider
        .ack_work_item(
            &token,
            Some(WorkItem::ActivityCompleted {
                instance: "inst-1".to_string(),
                execution_id: 1,
                id: 1,
                result: "done".to_string(),
            }),
        )
        .await;
    assert!(
        result.is_ok(),
        "Ack must succeed — activity lock is still valid even though session lock expired"
    );
}

/// G9: Session lock renewal extends timeout past original expiry.
/// Renewal at 60% of the session lock timeout should keep the session alive
/// past the original expiry time.
pub async fn test_session_lock_renewal_extends_past_original_timeout(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    let session_lock_dur = Duration::from_secs(1);

    // Worker-A claims session with 1s session lock
    provider
        .enqueue_for_worker(session_activity("inst-1", 1, "renew-ext-sess"))
        .await
        .unwrap();
    let cfg = session_config_with_lock("worker-A", session_lock_dur);
    let (_, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&cfg),
            &TagFilter::default(),
        )
        .await
        .unwrap()
        .unwrap();
    provider.ack_work_item(&token, None).await.unwrap();

    // At ~0.6s, renew the session lock for another 1s (extends to ~1.6s from start)
    tokio::time::sleep(Duration::from_millis(600)).await;
    let renewed = provider
        .renew_session_lock(&["worker-A"], session_lock_dur, Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(renewed, 1, "Should renew the active session");

    // At ~1.0s (past original expiry), session should STILL be owned by Worker-A
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Enqueue another item for the session
    provider
        .enqueue_for_worker(session_activity("inst-1", 2, "renew-ext-sess"))
        .await
        .unwrap();

    // Worker-B should NOT get it — session still held by Worker-A via renewal
    let result_b = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-B")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result_b.is_none(),
        "Session should still be owned by A after renewal past original timeout"
    );

    // Worker-A should get it
    let result_a = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            Some(&session_config("worker-A")),
            &TagFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        result_a.is_some(),
        "Worker-A should still own the session after renewal"
    );
}

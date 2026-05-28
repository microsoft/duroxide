// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Provider validation tests for custom status.
//!
//! These tests validate that a Provider implementation correctly handles
//! `CustomStatusUpdated` events in history_delta and the
//! `get_custom_status()` method.

use crate::EventKind;
use crate::provider_validation::{Event, ExecutionMetadata, create_instance};
use crate::provider_validations::ProviderFactory;
use crate::providers::WorkItem;
use std::time::Duration;

/// Helper to enqueue an ExternalRaised message to trigger a fetch cycle.
fn poke_item(instance: &str) -> WorkItem {
    WorkItem::ExternalRaised {
        instance: instance.to_string(),
        name: "poke".to_string(),
        data: "{}".to_string(),
    }
}

/// Helper: enqueue → fetch → ack with given metadata, returning the lock token used.
async fn ack_with_metadata(
    provider: &dyn crate::providers::Provider,
    _instance: &str,
    execution_id: u64,
    history_delta: Vec<Event>,
    metadata: ExecutionMetadata,
) {
    let (_, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    provider
        .ack_orchestration_item(
            &lock_token,
            execution_id,
            history_delta,
            vec![],
            vec![],
            metadata,
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// Set custom status
// =============================================================================

/// Acking with a `CustomStatusUpdated { status: Some("progress") }` event writes the custom_status and increments version.
pub async fn test_custom_status_set<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    // Create the instance
    create_instance(&*provider, "cs-set").await.unwrap();

    // Enqueue a completion message so we can fetch again
    provider
        .enqueue_for_orchestrator(poke_item("cs-set"), None)
        .await
        .unwrap();

    // Ack with custom status set via history event
    ack_with_metadata(
        &*provider,
        "cs-set",
        1,
        vec![Event::with_event_id(
            100,
            "cs-set",
            1,
            None,
            EventKind::CustomStatusUpdated {
                status: Some("progress".to_string()),
            },
        )],
        ExecutionMetadata { ..Default::default() },
    )
    .await;

    // Read it back
    let result = provider.get_custom_status("cs-set", 0).await.unwrap();
    assert!(result.is_some(), "expected custom_status to be present");
    let (status, version) = result.unwrap();
    assert_eq!(status, Some("progress".to_string()));
    assert_eq!(version, 1);
}

// =============================================================================
// Clear custom status
// =============================================================================

/// Acking with a `CustomStatusUpdated { status: None }` event resets custom_status to NULL and increments version.
pub async fn test_custom_status_clear<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "cs-clear").await.unwrap();

    // First set a value
    provider
        .enqueue_for_orchestrator(poke_item("cs-clear"), None)
        .await
        .unwrap();

    ack_with_metadata(
        &*provider,
        "cs-clear",
        1,
        vec![Event::with_event_id(
            100,
            "cs-clear",
            1,
            None,
            EventKind::CustomStatusUpdated {
                status: Some("temp".to_string()),
            },
        )],
        ExecutionMetadata { ..Default::default() },
    )
    .await;

    // Verify it was set
    let result = provider.get_custom_status("cs-clear", 0).await.unwrap();
    assert_eq!(result.unwrap().0, Some("temp".to_string()));

    // Now clear it
    provider
        .enqueue_for_orchestrator(poke_item("cs-clear"), None)
        .await
        .unwrap();

    ack_with_metadata(
        &*provider,
        "cs-clear",
        1,
        vec![Event::with_event_id(
            101,
            "cs-clear",
            1,
            None,
            EventKind::CustomStatusUpdated { status: None },
        )],
        ExecutionMetadata { ..Default::default() },
    )
    .await;

    // Read it back — should be None with version incremented
    let result = provider.get_custom_status("cs-clear", 0).await.unwrap();
    assert!(result.is_some(), "version changed, so should return Some");
    let (status, version) = result.unwrap();
    assert_eq!(status, None, "custom_status should be NULL after clear");
    assert_eq!(version, 2, "version should be 2 after set + clear");
}

// =============================================================================
// None preserves existing value
// =============================================================================

/// Acking with `None` (no custom status event) preserves the existing value.
pub async fn test_custom_status_none_preserves<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "cs-noop").await.unwrap();

    // Set a value
    provider
        .enqueue_for_orchestrator(poke_item("cs-noop"), None)
        .await
        .unwrap();

    ack_with_metadata(
        &*provider,
        "cs-noop",
        1,
        vec![Event::with_event_id(
            100,
            "cs-noop",
            1,
            None,
            EventKind::CustomStatusUpdated {
                status: Some("keep-me".to_string()),
            },
        )],
        ExecutionMetadata { ..Default::default() },
    )
    .await;

    // Ack again with no custom status event (no update)
    provider
        .enqueue_for_orchestrator(poke_item("cs-noop"), None)
        .await
        .unwrap();

    ack_with_metadata(
        &*provider,
        "cs-noop",
        1,
        vec![],
        ExecutionMetadata { ..Default::default() },
    )
    .await;

    // Value should be unchanged
    let result = provider.get_custom_status("cs-noop", 0).await.unwrap();
    let (status, version) = result.unwrap();
    assert_eq!(
        status,
        Some("keep-me".to_string()),
        "None should not modify existing value"
    );
    assert_eq!(version, 1, "version should not increment on None");
}

// =============================================================================
// Version monotonicity
// =============================================================================

/// Each Set or Clear increments version by 1.
pub async fn test_custom_status_version_increments<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "cs-ver").await.unwrap();

    // Ack 1: Set
    provider
        .enqueue_for_orchestrator(poke_item("cs-ver"), None)
        .await
        .unwrap();

    ack_with_metadata(
        &*provider,
        "cs-ver",
        1,
        vec![Event::with_event_id(
            100,
            "cs-ver",
            1,
            None,
            EventKind::CustomStatusUpdated {
                status: Some("a".to_string()),
            },
        )],
        ExecutionMetadata { ..Default::default() },
    )
    .await;

    let (_, v1) = provider.get_custom_status("cs-ver", 0).await.unwrap().unwrap();
    assert_eq!(v1, 1);

    // Ack 2: Set again
    provider
        .enqueue_for_orchestrator(poke_item("cs-ver"), None)
        .await
        .unwrap();

    ack_with_metadata(
        &*provider,
        "cs-ver",
        1,
        vec![Event::with_event_id(
            101,
            "cs-ver",
            1,
            None,
            EventKind::CustomStatusUpdated {
                status: Some("b".to_string()),
            },
        )],
        ExecutionMetadata { ..Default::default() },
    )
    .await;

    let (_, v2) = provider.get_custom_status("cs-ver", 0).await.unwrap().unwrap();
    assert_eq!(v2, 2);

    // Ack 3: Clear
    provider
        .enqueue_for_orchestrator(poke_item("cs-ver"), None)
        .await
        .unwrap();

    ack_with_metadata(
        &*provider,
        "cs-ver",
        1,
        vec![Event::with_event_id(
            102,
            "cs-ver",
            1,
            None,
            EventKind::CustomStatusUpdated { status: None },
        )],
        ExecutionMetadata { ..Default::default() },
    )
    .await;

    let (status, v3) = provider.get_custom_status("cs-ver", 0).await.unwrap().unwrap();
    assert_eq!(v3, 3);
    assert_eq!(status, None);
}

// =============================================================================
// get_custom_status polling semantics
// =============================================================================

/// get_custom_status returns None when version hasn't changed.
pub async fn test_custom_status_polling_no_change<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "cs-poll").await.unwrap();

    // Set a value
    provider
        .enqueue_for_orchestrator(poke_item("cs-poll"), None)
        .await
        .unwrap();

    ack_with_metadata(
        &*provider,
        "cs-poll",
        1,
        vec![Event::with_event_id(
            100,
            "cs-poll",
            1,
            None,
            EventKind::CustomStatusUpdated {
                status: Some("v1".to_string()),
            },
        )],
        ExecutionMetadata { ..Default::default() },
    )
    .await;

    // Polling with last_seen_version = 1 should return None (no change)
    let result = provider.get_custom_status("cs-poll", 1).await.unwrap();
    assert!(result.is_none(), "no change since version 1");

    // Polling with last_seen_version = 0 should return Some (version 1 > 0)
    let result = provider.get_custom_status("cs-poll", 0).await.unwrap();
    assert!(result.is_some(), "version 1 > 0");
}

// =============================================================================
// get_custom_status for nonexistent instance
// =============================================================================

/// get_custom_status returns None for a nonexistent instance.
pub async fn test_custom_status_nonexistent_instance<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    let result = provider.get_custom_status("does-not-exist", 0).await.unwrap();
    assert!(result.is_none());
}

// =============================================================================
// Default: fresh instance has version 0 and None status
// =============================================================================

/// A freshly created instance has custom_status = None and version = 0.
pub async fn test_custom_status_default_on_new_instance<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "cs-default").await.unwrap();

    // Polling with version 0 should return None (version 0 is not > 0)
    let result = provider.get_custom_status("cs-default", 0).await.unwrap();
    assert!(result.is_none(), "fresh instance has version 0, not > 0");
}

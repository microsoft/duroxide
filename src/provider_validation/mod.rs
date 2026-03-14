//! Provider Validation Tests
//!
//! Comprehensive test suite for validating provider implementations.
//! These tests are designed to work with any provider through the `ProviderFactory` trait.

#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod atomicity;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod bulk_deletion;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod cancellation;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod capability_filtering;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod custom_status;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod deletion;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod error_handling;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod instance_creation;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod instance_locking;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod kv_store;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod lock_expiration;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod long_polling;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod management;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod multi_execution;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod poison_message;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod prune;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod queue_semantics;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod sessions;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub mod tag_filtering;

#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
use crate::INITIAL_EXECUTION_ID;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
use crate::providers::WorkItem;
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
use std::time::Duration;

#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub use crate::providers::ExecutionMetadata;
/// Re-export common types for use in test modules
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub use crate::{Event, EventKind};

/// Helper function to create a start item for an instance
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub(crate) fn start_item(instance: &str) -> WorkItem {
    WorkItem::StartOrchestration {
        instance: instance.to_string(),
        orchestration: "TestOrch".to_string(),
        input: "{}".to_string(),
        version: Some("1.0.0".to_string()),
        parent_instance: None,
        parent_id: None,
        execution_id: INITIAL_EXECUTION_ID,
    }
}

/// Helper function to create an instance by enqueueing, fetching, and acking with metadata
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub(crate) async fn create_instance(provider: &dyn crate::providers::Provider, instance: &str) -> Result<(), String> {
    provider
        .enqueue_for_orchestrator(start_item(instance), None)
        .await
        .map_err(|e| e.to_string())?;

    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Failed to fetch orchestration item".to_string())?;

    provider
        .ack_orchestration_item(
            &lock_token,
            INITIAL_EXECUTION_ID,
            vec![Event::with_event_id(
                crate::INITIAL_EVENT_ID,
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
        .map_err(|e| e.to_string())?;

    Ok(())
}

/// Helper function to create an instance with a parent relationship.
/// Used for creating sub-orchestration hierarchies in tests.
///
/// Note: This is a low-level helper that only enqueues the instance (doesn't complete it).
/// The `create_child_instance` helper in deletion.rs wraps this and also completes the instance.
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
pub(crate) async fn create_instance_with_parent(
    provider: &dyn crate::providers::Provider,
    instance: &str,
    parent_instance_id: Option<String>,
) -> Result<(), String> {
    // Create start item with parent info
    let start_item = WorkItem::StartOrchestration {
        instance: instance.to_string(),
        orchestration: "TestOrch".to_string(),
        version: Some("1.0.0".to_string()),
        input: "{}".to_string(),
        parent_instance: parent_instance_id,
        parent_id: None,
        execution_id: crate::INITIAL_EXECUTION_ID,
    };

    provider
        .enqueue_for_orchestrator(start_item, None)
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

/// Trait to create fresh provider instances for tests.
/// Essential for multi-threaded/concurrent tests.
#[cfg(any(feature = "provider-test", feature = "provider-test-core"))]
#[async_trait::async_trait]
pub trait ProviderFactory: Sync + Send {
    /// Create a new, isolated provider instance connected to the same backend.
    async fn create_provider(&self) -> std::sync::Arc<dyn crate::providers::Provider>;

    /// Default lock timeout to use in tests
    fn lock_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }

    /// Threshold for short polling tests — how fast a non-long-polling provider
    /// should return when no work is available.
    ///
    /// Default is 100ms, which works for local/in-process providers like SQLite.
    /// Remote-database providers (e.g., PostgreSQL) should override this with a
    /// higher value (e.g., 500ms) to account for network round-trip latency.
    fn short_poll_threshold(&self) -> Duration {
        Duration::from_millis(100)
    }

    /// Corrupt an instance's history so it cannot be deserialized.
    ///
    /// This replaces all stored history event data for the given instance with
    /// content that will fail deserialization (e.g., invalid JSON or an unknown
    /// event variant). Used by the deserialization contract tests (Category I)
    /// to verify that the provider returns `history_error` on fetch and that
    /// ack remains append-only (doesn't re-read corrupted rows).
    ///
    /// The default implementation panics — providers that support the
    /// deserialization contract tests must override this.
    async fn corrupt_instance_history(&self, _instance: &str) {
        panic!("corrupt_instance_history not implemented for this provider factory");
    }

    /// Return the maximum `attempt_count` across all orchestrator queue messages
    /// for the given instance.
    ///
    /// Used by the deserialization contract tests to verify that `attempt_count`
    /// increments on each fetch cycle (even when history can't be deserialized).
    ///
    /// The default implementation panics — providers that support the
    /// deserialization contract tests must override this.
    async fn get_max_attempt_count(&self, _instance: &str) -> u32 {
        panic!("get_max_attempt_count not implemented for this provider factory");
    }
}

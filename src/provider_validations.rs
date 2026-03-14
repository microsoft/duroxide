//! Provider Validation Infrastructure
//!
//! This module provides reusable test infrastructure for validating custom Provider implementations.
//! Enable the `provider-test` feature to use these utilities.
//!
//! # Example
//!
//! ```rust,ignore
//! use duroxide::providers::Provider;
//! use duroxide::provider_validations::ProviderFactory;
//! use std::sync::Arc;
//!
//! struct MyProviderFactory;
//!
//! #[async_trait::async_trait]
//! impl ProviderFactory for MyProviderFactory {
//!     async fn create_provider(&self) -> Arc<dyn Provider> {
//!         Arc::new(MyProvider::new().await.unwrap())
//!     }
//! }
//!
//! #[tokio::test]
//! async fn test_my_provider() {
//!     let factory = MyProviderFactory;
//!     duroxide::provider_validations::run_atomicity_tests(&factory).await;
//! }
//! ```

// Re-export ProviderFactory from the internal module
#[cfg(feature = "provider-test")]
pub use crate::provider_validation::ProviderFactory;

/// ## Individual Test Functions
///
/// For granular control and easier debugging, you can run individual test functions:
///
/// ```rust,ignore
/// use duroxide::provider_validations::{ProviderFactory, test_atomicity_failure_rollback};
///
/// #[tokio::test]
/// async fn test_atomicity_failure_rollback_only() {
///     let factory = MyFactory;
///     test_atomicity_failure_rollback(&factory).await;
/// }
/// ```
///
/// Available test functions:
///
/// **Instance Creation Tests:**
/// - `test_instance_creation_via_metadata` - Verify instances created via ack metadata, not on enqueue
/// - `test_no_instance_creation_on_enqueue` - Verify no instance created when enqueueing
/// - `test_null_version_handling` - Verify NULL version handled correctly
/// - `test_sub_orchestration_instance_creation` - Verify sub-orchestrations follow same pattern
///
/// **Atomicity Tests:**
/// - `test_atomicity_failure_rollback` - Verify ack failure rolls back all operations
/// - `test_multi_operation_atomic_ack` - Verify complex ack succeeds atomically
/// - `test_lock_released_only_on_successful_ack` - Verify lock only released on success
/// - `test_concurrent_ack_prevention` - Verify only one ack succeeds with same token
///
/// **Error Handling Tests:**
/// - `test_invalid_lock_token_on_ack` - Verify invalid lock tokens are rejected
/// - `test_duplicate_event_id_rejection` - Verify duplicate events are rejected
/// - `test_missing_instance_metadata` - Verify missing instances handled gracefully
/// - `test_corrupted_serialization_data` - Verify corrupted data handled gracefully
/// - `test_lock_expiration_during_ack` - Verify expired locks are rejected
///
/// **Instance Locking Tests:**
/// - `test_exclusive_instance_lock` - Verify only one dispatcher can process an instance
/// - `test_lock_token_uniqueness` - Verify each fetch generates unique lock token
/// - `test_invalid_lock_token_rejection` - Verify invalid tokens rejected for ack/abandon
/// - `test_concurrent_instance_fetching` - Verify concurrent fetches don't duplicate instances
/// - `test_completions_arriving_during_lock_blocked` - Verify new messages blocked during lock
/// - `test_cross_instance_lock_isolation` - Verify locks don't block other instances
/// - `test_message_tagging_during_lock` - Verify only fetched messages deleted on ack
/// - `test_ack_only_affects_locked_messages` - Verify ack only affects locked messages
/// - `test_multi_threaded_lock_contention` - Verify locks prevent concurrent processing (multi-threaded)
/// - `test_multi_threaded_no_duplicate_processing` - Verify no duplicate processing (multi-threaded)
/// - `test_multi_threaded_lock_expiration_recovery` - Verify lock expiration recovery (multi-threaded)
///
/// **Lock Expiration Tests:**
/// - `test_lock_expires_after_timeout` - Verify locks expire after timeout
/// - `test_abandon_releases_lock_immediately` - Verify abandon releases lock immediately
/// - `test_abandon_work_item_releases_lock` - Verify abandon_work_item releases lock immediately
/// - `test_abandon_work_item_with_delay` - Verify abandon_work_item with delay defers refetch
/// - `test_lock_renewal_on_ack` - Verify successful ack releases lock immediately
/// - `test_concurrent_lock_attempts_respect_expiration` - Verify concurrent attempts respect expiration
/// - `test_worker_lock_renewal_success` - Verify worker lock can be renewed with valid token
/// - `test_worker_lock_renewal_invalid_token` - Verify renewal fails with invalid token
/// - `test_worker_lock_renewal_after_expiration` - Verify renewal fails after lock expires
/// - `test_worker_lock_renewal_extends_timeout` - Verify renewal properly extends lock timeout
/// - `test_worker_lock_renewal_after_ack` - Verify renewal fails after item has been acked
///
/// **Multi-Execution Tests:**
/// - `test_execution_isolation` - Verify each execution has separate history
/// - `test_latest_execution_detection` - Verify read() returns latest execution
/// - `test_execution_id_sequencing` - Verify execution IDs increment correctly
/// - `test_continue_as_new_creates_new_execution` - Verify ContinueAsNew creates new execution
/// - `test_execution_history_persistence` - Verify all executions' history persists independently
///
/// **Queue Semantics Tests:**
/// - `test_worker_queue_fifo_ordering` - Verify worker items dequeued in FIFO order
/// - `test_worker_peek_lock_semantics` - Verify dequeue doesn't remove item until ack
/// - `test_worker_ack_atomicity` - Verify ack_worker atomically removes item and enqueues completion
/// - `test_timer_delayed_visibility` - Verify TimerFired items only dequeued when visible
/// - `test_lost_lock_token_handling` - Verify locked items become available after expiration
/// - `test_worker_item_immediate_visibility` - Verify newly enqueued items are immediately visible
/// - `test_worker_delayed_visibility_skips_future_items` - Verify items with future visible_at are skipped
/// - `test_orphan_queue_messages_dropped` - Verify QueueMessage for non-existent instance is dropped; kept for existing instance
///
/// **Management Tests:**
/// - `test_list_instances` - Verify list_instances returns all instance IDs
/// - `test_list_instances_by_status` - Verify list_instances_by_status filters correctly
/// - `test_list_executions` - Verify list_executions returns all execution IDs
/// - `test_get_instance_info` - Verify get_instance_info returns metadata
/// - `test_get_execution_info` - Verify get_execution_info returns execution metadata
/// - `test_get_system_metrics` - Verify get_system_metrics returns accurate counts
/// - `test_get_queue_depths` - Verify get_queue_depths returns current queue sizes
///
/// **Poison Message Tests:**
/// - `orchestration_attempt_count_starts_at_one` - Verify first fetch has attempt_count = 1
/// - `orchestration_attempt_count_increments_on_refetch` - Verify attempt_count increments on refetch
/// - `worker_attempt_count_starts_at_one` - Verify worker first fetch has attempt_count = 1
/// - `worker_attempt_count_increments_on_lock_expiry` - Verify worker attempt_count increments on expiry
/// - `attempt_count_is_per_message` - Verify attempt_count is tracked per message
/// - `abandon_work_item_ignore_attempt_decrements` - Verify ignore_attempt decrements count
/// - `abandon_orchestration_item_ignore_attempt_decrements` - Verify ignore_attempt decrements count
/// - `ignore_attempt_never_goes_negative` - Verify attempt_count never goes below 0
/// - `max_attempt_count_across_message_batch` - Verify MAX attempt_count returned for batched messages
#[cfg(feature = "provider-test")]
pub use crate::provider_validation::instance_creation::{
    test_instance_creation_via_metadata, test_no_instance_creation_on_enqueue, test_null_version_handling,
    test_sub_orchestration_instance_creation,
};

#[cfg(feature = "provider-test")]
pub use crate::provider_validation::atomicity::{
    test_atomicity_failure_rollback, test_concurrent_ack_prevention, test_lock_released_only_on_successful_ack,
    test_multi_operation_atomic_ack,
};

#[cfg(feature = "provider-test")]
pub use crate::provider_validation::error_handling::{
    test_corrupted_serialization_data, test_duplicate_event_id_rejection, test_invalid_lock_token_on_ack,
    test_lock_expiration_during_ack, test_missing_instance_metadata,
};

#[cfg(feature = "provider-test")]
pub use crate::provider_validation::instance_locking::{
    test_ack_only_affects_locked_messages, test_completions_arriving_during_lock_blocked,
    test_concurrent_instance_fetching, test_cross_instance_lock_isolation, test_exclusive_instance_lock,
    test_invalid_lock_token_rejection, test_lock_token_uniqueness, test_message_tagging_during_lock,
    test_multi_threaded_lock_contention, test_multi_threaded_lock_expiration_recovery,
    test_multi_threaded_no_duplicate_processing,
};

#[cfg(feature = "provider-test")]
pub use crate::provider_validation::lock_expiration::{
    test_abandon_releases_lock_immediately, test_abandon_work_item_releases_lock, test_abandon_work_item_with_delay,
    test_concurrent_lock_attempts_respect_expiration, test_lock_expires_after_timeout, test_lock_renewal_on_ack,
    test_orchestration_lock_renewal_after_expiration, test_worker_ack_fails_after_lock_expiry,
    test_worker_lock_renewal_after_ack, test_worker_lock_renewal_after_expiration,
    test_worker_lock_renewal_extends_timeout, test_worker_lock_renewal_invalid_token, test_worker_lock_renewal_success,
};

#[cfg(feature = "provider-test")]
pub use crate::provider_validation::multi_execution::{
    test_continue_as_new_creates_new_execution, test_execution_history_persistence, test_execution_id_sequencing,
    test_execution_isolation, test_latest_execution_detection,
};

#[cfg(feature = "provider-test")]
pub use crate::provider_validation::queue_semantics::{
    test_lost_lock_token_handling, test_orphan_queue_messages_dropped, test_timer_delayed_visibility,
    test_worker_ack_atomicity, test_worker_delayed_visibility_skips_future_items,
    test_worker_item_immediate_visibility, test_worker_peek_lock_semantics, test_worker_queue_fifo_ordering,
};

#[cfg(feature = "provider-test")]
pub use crate::provider_validation::management::{
    test_get_execution_info, test_get_instance_info, test_get_queue_depths, test_get_system_metrics,
    test_list_executions, test_list_instances, test_list_instances_by_status,
};

#[cfg(feature = "provider-test")]
pub mod long_polling {
    pub use crate::provider_validation::long_polling::{
        test_fetch_respects_timeout_upper_bound, test_long_poll_waits_for_timeout,
        test_long_poll_work_item_waits_for_timeout, test_short_poll_returns_immediately,
        test_short_poll_work_item_returns_immediately,
    };
}

#[cfg(feature = "provider-test")]
pub mod poison_message {
    pub use crate::provider_validation::poison_message::{
        abandon_orchestration_item_ignore_attempt_decrements, abandon_work_item_ignore_attempt_decrements,
        attempt_count_is_per_message, ignore_attempt_never_goes_negative, max_attempt_count_across_message_batch,
        orchestration_attempt_count_increments_on_refetch, orchestration_attempt_count_starts_at_one,
        worker_attempt_count_increments_on_lock_expiry, worker_attempt_count_starts_at_one,
    };
}

#[cfg(feature = "provider-test")]
pub use crate::provider_validation::cancellation::{
    test_ack_work_item_fails_when_entry_deleted, test_ack_work_item_none_deletes_without_enqueue,
    test_batch_cancellation_deletes_multiple_activities, test_cancelled_activities_deleted_from_worker_queue,
    test_cancelling_nonexistent_activities_is_idempotent, test_fetch_returns_missing_state_when_instance_deleted,
    test_fetch_returns_running_state_for_active_orchestration,
    test_fetch_returns_terminal_state_when_orchestration_completed,
    test_fetch_returns_terminal_state_when_orchestration_continued_as_new,
    test_fetch_returns_terminal_state_when_orchestration_failed, test_orphan_activity_after_instance_force_deletion,
    test_renew_fails_when_entry_deleted, test_renew_returns_missing_when_instance_deleted,
    test_renew_returns_running_when_orchestration_active, test_renew_returns_terminal_when_orchestration_completed,
    test_same_activity_in_worker_items_and_cancelled_is_noop,
};

#[cfg(feature = "provider-test")]
pub mod deletion {
    pub use crate::provider_validation::deletion::{
        test_cascade_delete_hierarchy, test_delete_cleans_queues_and_locks, test_delete_get_instance_tree,
        test_delete_get_parent_id, test_delete_instances_atomic, test_delete_instances_atomic_force,
        test_delete_instances_atomic_orphan_detection, test_delete_nonexistent_instance,
        test_delete_running_rejected_force_succeeds, test_delete_terminal_instances,
        test_force_delete_prevents_ack_recreation, test_list_children, test_stale_activity_after_delete_recreate,
    };
}

#[cfg(feature = "provider-test")]
pub mod prune {
    pub use crate::provider_validation::prune::{
        test_prune_bulk, test_prune_bulk_includes_running_instances, test_prune_options_combinations, test_prune_safety,
    };
}

#[cfg(feature = "provider-test")]
pub mod bulk_deletion {
    pub use crate::provider_validation::bulk_deletion::{
        test_delete_instance_bulk_cascades_to_children, test_delete_instance_bulk_completed_before_filter,
        test_delete_instance_bulk_filter_combinations, test_delete_instance_bulk_safety_and_limits,
    };
}

#[cfg(feature = "provider-test")]
pub mod capability_filtering {
    pub use crate::provider_validation::capability_filtering::{
        test_ack_appends_event_to_corrupted_history, test_ack_stores_pinned_version_via_metadata_update,
        test_concurrent_filtered_fetch_no_double_lock, test_continue_as_new_execution_gets_own_pinned_version,
        test_fetch_corrupted_history_filtered_vs_unfiltered,
        test_fetch_deserialization_error_eventually_reaches_poison,
        test_fetch_deserialization_error_increments_attempt_count,
        test_fetch_filter_applied_before_history_deserialization, test_fetch_filter_boundary_versions,
        test_fetch_filter_does_not_lock_skipped_instances, test_fetch_filter_null_pinned_version_always_compatible,
        test_fetch_filter_skips_incompatible_selects_compatible, test_fetch_single_range_only_uses_first_range,
        test_fetch_with_compatible_filter_returns_item, test_fetch_with_filter_none_returns_any_item,
        test_fetch_with_incompatible_filter_skips_item, test_filter_with_empty_supported_versions_returns_nothing,
        test_pinned_version_immutable_across_ack_cycles, test_pinned_version_stored_via_ack_metadata,
        test_provider_updates_pinned_version_when_told,
    };
}

#[cfg(feature = "provider-test")]
pub mod sessions {
    pub use crate::provider_validation::sessions::{
        test_abandoned_session_item_ignore_attempt, test_abandoned_session_item_retryable,
        test_ack_updates_session_last_activity, test_activity_lock_expires_session_lock_valid_same_worker_refetches,
        test_both_locks_expire_different_worker_claims, test_cleanup_keeps_active_sessions,
        test_cleanup_keeps_sessions_with_pending_items, test_cleanup_removes_expired_no_items,
        test_cleanup_then_new_item_recreates_session, test_concurrent_session_claim_only_one_wins,
        test_different_sessions_different_workers, test_mixed_session_and_non_session_items,
        test_non_session_items_fetchable_by_any_worker, test_non_session_items_returned_with_session_config,
        test_none_session_skips_session_items, test_original_worker_reclaims_expired_session,
        test_renew_session_lock_active, test_renew_session_lock_after_expiry_returns_zero,
        test_renew_session_lock_no_sessions, test_renew_session_lock_skips_idle,
        test_renew_work_item_updates_session_last_activity, test_session_affinity_blocks_other_worker,
        test_session_affinity_same_worker, test_session_claimable_after_lock_expiry,
        test_session_item_claimable_when_no_session, test_session_items_processed_in_order,
        test_session_lock_expires_activity_lock_valid_ack_succeeds,
        test_session_lock_expires_new_owner_gets_redelivery, test_session_lock_expires_same_worker_reacquires,
        test_session_lock_renewal_extends_past_original_timeout, test_session_takeover_after_lock_expiry,
        test_shared_worker_id_any_caller_can_fetch_owned_session, test_some_session_returns_all_items,
    };
}

#[cfg(feature = "provider-test")]
pub mod custom_status {
    pub use crate::provider_validation::custom_status::{
        test_custom_status_clear, test_custom_status_default_on_new_instance, test_custom_status_none_preserves,
        test_custom_status_nonexistent_instance, test_custom_status_polling_no_change, test_custom_status_set,
        test_custom_status_version_increments,
    };
}

#[cfg(feature = "provider-test")]
pub mod tag_filtering {
    pub use crate::provider_validation::tag_filtering::{
        test_any_filter_fetches_everything, test_default_and_fetches_untagged_and_matching,
        test_default_only_fetches_untagged, test_multi_runtime_tag_isolation, test_multi_tag_filter,
        test_none_filter_returns_nothing, test_tag_preserved_through_ack_orchestration_item,
        test_tag_round_trip_preservation, test_tag_survives_abandon_and_refetch, test_tags_fetches_only_matching,
    };
}

#[cfg(feature = "provider-test")]
pub mod kv_store {
    pub use crate::provider_validation::kv_store::{
        test_kv_clear_all, test_kv_clear_isolation, test_kv_clear_nonexistent_key, test_kv_clear_single,
        test_kv_cross_execution_overwrite, test_kv_cross_execution_remove_readd, test_kv_delete_instance_cascades,
        test_kv_delete_instance_with_children, test_kv_empty_value, test_kv_execution_id_tracking,
        test_kv_get_nonexistent, test_kv_get_unknown_instance, test_kv_instance_isolation, test_kv_large_value,
        test_kv_overwrite, test_kv_prune_current_execution_protected, test_kv_prune_preserves_all_keys,
        test_kv_prune_preserves_overwritten, test_kv_set_after_clear, test_kv_set_and_get,
        test_kv_snapshot_after_clear_all, test_kv_snapshot_after_clear_single, test_kv_snapshot_cross_execution,
        test_kv_snapshot_empty, test_kv_snapshot_in_fetch, test_kv_special_chars_in_key,
    };
}

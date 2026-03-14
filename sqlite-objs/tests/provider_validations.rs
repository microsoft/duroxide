//! Provider validation tests for SqliteObjsProvider
//!
//! Runs the same 202-test provider validation suite against SqliteObjsProvider.
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::provider_validations::sessions::{
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
use duroxide::provider_validations::tag_filtering::{
    test_any_filter_fetches_everything, test_default_and_fetches_untagged_and_matching,
    test_default_only_fetches_untagged, test_multi_runtime_tag_isolation, test_multi_tag_filter,
    test_none_filter_returns_nothing, test_tag_preserved_through_ack_orchestration_item,
    test_tag_round_trip_preservation, test_tag_survives_abandon_and_refetch, test_tags_fetches_only_matching,
};
use duroxide::provider_validations::{
    ProviderFactory,
    // Bulk deletion tests
    bulk_deletion::{
        test_delete_instance_bulk_cascades_to_children, test_delete_instance_bulk_completed_before_filter,
        test_delete_instance_bulk_filter_combinations, test_delete_instance_bulk_safety_and_limits,
    },
    // Capability filtering tests
    capability_filtering::{
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
    },
    // Deletion tests
    deletion::{
        test_cascade_delete_hierarchy, test_delete_cleans_queues_and_locks, test_delete_get_instance_tree,
        test_delete_get_parent_id, test_delete_instances_atomic, test_delete_instances_atomic_force,
        test_delete_instances_atomic_orphan_detection, test_delete_nonexistent_instance,
        test_delete_running_rejected_force_succeeds, test_delete_terminal_instances,
        test_force_delete_prevents_ack_recreation, test_list_children, test_stale_activity_after_delete_recreate,
    },
    // Long polling tests
    long_polling::{
        test_fetch_respects_timeout_upper_bound, test_short_poll_returns_immediately,
        test_short_poll_work_item_returns_immediately,
    },
    // Poison message tests
    poison_message::{
        abandon_orchestration_item_ignore_attempt_decrements, abandon_work_item_ignore_attempt_decrements,
        attempt_count_is_per_message, ignore_attempt_never_goes_negative, max_attempt_count_across_message_batch,
        orchestration_attempt_count_increments_on_refetch, orchestration_attempt_count_starts_at_one,
        worker_attempt_count_increments_on_lock_expiry, worker_attempt_count_starts_at_one,
    },
    // Prune tests
    prune::{
        test_prune_bulk, test_prune_bulk_includes_running_instances, test_prune_options_combinations,
        test_prune_safety,
    },
    test_abandon_releases_lock_immediately,
    test_abandon_work_item_releases_lock,
    test_abandon_work_item_with_delay,
    test_ack_only_affects_locked_messages,
    // Cancellation tests
    test_ack_work_item_fails_when_entry_deleted,
    test_ack_work_item_none_deletes_without_enqueue,
    // Atomicity tests
    test_atomicity_failure_rollback,
    test_batch_cancellation_deletes_multiple_activities,
    test_cancelled_activities_deleted_from_worker_queue,
    test_cancelling_nonexistent_activities_is_idempotent,
    test_completions_arriving_during_lock_blocked,
    test_concurrent_ack_prevention,
    test_concurrent_instance_fetching,
    test_concurrent_lock_attempts_respect_expiration,
    test_continue_as_new_creates_new_execution,
    test_corrupted_serialization_data,
    test_cross_instance_lock_isolation,
    test_duplicate_event_id_rejection,
    // Instance locking tests
    test_exclusive_instance_lock,
    test_execution_history_persistence,
    test_execution_id_sequencing,
    // Multi-execution tests
    test_execution_isolation,
    test_fetch_returns_missing_state_when_instance_deleted,
    test_fetch_returns_running_state_for_active_orchestration,
    test_fetch_returns_terminal_state_when_orchestration_completed,
    test_fetch_returns_terminal_state_when_orchestration_continued_as_new,
    test_fetch_returns_terminal_state_when_orchestration_failed,
    test_get_execution_info,
    test_get_instance_info,
    test_get_queue_depths,
    test_get_system_metrics,
    // Instance creation tests
    test_instance_creation_via_metadata,
    // Error handling tests
    test_invalid_lock_token_on_ack,
    test_invalid_lock_token_rejection,
    test_latest_execution_detection,
    test_list_executions,
    // Management tests
    test_list_instances,
    test_list_instances_by_status,
    test_lock_expiration_during_ack,
    // Lock expiration tests
    test_lock_expires_after_timeout,
    test_lock_released_only_on_successful_ack,
    test_lock_renewal_on_ack,
    test_lock_token_uniqueness,
    test_lost_lock_token_handling,
    test_message_tagging_during_lock,
    test_missing_instance_metadata,
    test_multi_operation_atomic_ack,
    test_multi_threaded_lock_contention,
    test_multi_threaded_lock_expiration_recovery,
    test_multi_threaded_no_duplicate_processing,
    test_no_instance_creation_on_enqueue,
    test_null_version_handling,
    test_orchestration_lock_renewal_after_expiration,
    test_orphan_activity_after_instance_force_deletion,
    test_orphan_queue_messages_dropped,
    test_renew_fails_when_entry_deleted,
    test_renew_returns_missing_when_instance_deleted,
    test_renew_returns_running_when_orchestration_active,
    test_renew_returns_terminal_when_orchestration_completed,
    test_same_activity_in_worker_items_and_cancelled_is_noop,
    test_sub_orchestration_instance_creation,
    test_timer_delayed_visibility,
    test_worker_ack_atomicity,
    test_worker_ack_fails_after_lock_expiry,
    test_worker_delayed_visibility_skips_future_items,
    test_worker_item_immediate_visibility,
    // Worker lock renewal tests
    test_worker_lock_renewal_after_ack,
    test_worker_lock_renewal_after_expiration,
    test_worker_lock_renewal_extends_timeout,
    test_worker_lock_renewal_invalid_token,
    test_worker_lock_renewal_success,
    test_worker_peek_lock_semantics,
    // Queue semantics tests
    test_worker_queue_fifo_ordering,
};


/// Standard test factory — each `create_provider()` call gets a fresh in-memory DB.
/// Used by the vast majority of tests that don't need direct DB manipulation.

use duroxide::providers::Provider;
use duroxide_sqlite_objs::SqliteObjsProvider;
use std::sync::Arc;
use std::time::Duration;

const TEST_LOCK_TIMEOUT: Duration = Duration::from_millis(1000);

struct SqliteObjsTestFactory;

#[async_trait::async_trait]
impl ProviderFactory for SqliteObjsTestFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(SqliteObjsProvider::new_in_memory().await.unwrap())
    }

    fn lock_timeout(&self) -> Duration {
        TEST_LOCK_TIMEOUT
    }
}

struct SharedSqliteObjsTestFactory {
    provider: Arc<SqliteObjsProvider>,
}

impl SharedSqliteObjsTestFactory {
    async fn new() -> Self {
        Self {
            provider: Arc::new(SqliteObjsProvider::new_in_memory().await.unwrap()),
        }
    }
}

#[async_trait::async_trait]
impl ProviderFactory for SharedSqliteObjsTestFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        self.provider.clone()
    }

    fn lock_timeout(&self) -> Duration {
        TEST_LOCK_TIMEOUT
    }

    async fn corrupt_instance_history(&self, instance: &str) {
        let instance = instance.to_string();
        let conn = self.provider.get_conn().clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE history SET event_data = 'NOT_VALID_JSON{{{' WHERE instance_id = ?1",
                rusqlite::params![instance],
            ).expect("Failed to corrupt history");
        }).await.unwrap();
    }

    async fn get_max_attempt_count(&self, instance: &str) -> u32 {
        let instance = instance.to_string();
        let conn = self.provider.get_conn().clone();
        let count: i64 = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.query_row(
                "SELECT MAX(attempt_count) FROM orchestrator_queue WHERE instance_id = ?1",
                rusqlite::params![instance],
                |row| row.get(0),
            ).expect("Failed to query attempt_count")
        }).await.unwrap();
        count as u32
    }
}

#[tokio::test]
async fn test_sqlite_objs_atomicity_failure_rollback() {
    test_atomicity_failure_rollback(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_multi_operation_atomic_ack() {
    test_multi_operation_atomic_ack(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_lock_released_only_on_successful_ack() {
    test_lock_released_only_on_successful_ack(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_concurrent_ack_prevention() {
    test_concurrent_ack_prevention(&SqliteObjsTestFactory).await;
}

// Error handling tests
#[tokio::test]
async fn test_sqlite_objs_invalid_lock_token_on_ack() {
    test_invalid_lock_token_on_ack(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_duplicate_event_id_rejection() {
    test_duplicate_event_id_rejection(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_missing_instance_metadata() {
    test_missing_instance_metadata(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_corrupted_serialization_data() {
    test_corrupted_serialization_data(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_lock_expiration_during_ack() {
    test_lock_expiration_during_ack(&SqliteObjsTestFactory).await;
}

// Instance locking tests
#[tokio::test]
async fn test_sqlite_objs_exclusive_instance_lock() {
    test_exclusive_instance_lock(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_lock_token_uniqueness() {
    test_lock_token_uniqueness(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_invalid_lock_token_rejection() {
    test_invalid_lock_token_rejection(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_concurrent_instance_fetching() {
    test_concurrent_instance_fetching(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_completions_arriving_during_lock_blocked() {
    test_completions_arriving_during_lock_blocked(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_cross_instance_lock_isolation() {
    test_cross_instance_lock_isolation(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_message_tagging_during_lock() {
    test_message_tagging_during_lock(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_ack_only_affects_locked_messages() {
    test_ack_only_affects_locked_messages(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_multi_threaded_lock_contention() {
    test_multi_threaded_lock_contention(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_multi_threaded_no_duplicate_processing() {
    test_multi_threaded_no_duplicate_processing(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_multi_threaded_lock_expiration_recovery() {
    test_multi_threaded_lock_expiration_recovery(&SqliteObjsTestFactory).await;
}

// Lock expiration tests
#[tokio::test]
async fn test_sqlite_objs_lock_expires_after_timeout() {
    test_lock_expires_after_timeout(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_abandon_releases_lock_immediately() {
    test_abandon_releases_lock_immediately(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_lock_renewal_on_ack() {
    test_lock_renewal_on_ack(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_concurrent_lock_attempts_respect_expiration() {
    test_concurrent_lock_attempts_respect_expiration(&SqliteObjsTestFactory).await;
}

// Multi-execution tests
#[tokio::test]
async fn test_sqlite_objs_execution_isolation() {
    test_execution_isolation(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_latest_execution_detection() {
    test_latest_execution_detection(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_execution_id_sequencing() {
    test_execution_id_sequencing(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_continue_as_new_creates_new_execution() {
    test_continue_as_new_creates_new_execution(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_execution_history_persistence() {
    test_execution_history_persistence(&SqliteObjsTestFactory).await;
}

// Queue semantics tests
#[tokio::test]
async fn test_sqlite_objs_worker_queue_fifo_ordering() {
    test_worker_queue_fifo_ordering(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_worker_peek_lock_semantics() {
    test_worker_peek_lock_semantics(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_worker_ack_atomicity() {
    test_worker_ack_atomicity(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_timer_delayed_visibility() {
    test_timer_delayed_visibility(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_lost_lock_token_handling() {
    test_lost_lock_token_handling(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_worker_item_immediate_visibility() {
    test_worker_item_immediate_visibility(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_worker_delayed_visibility_skips_future_items() {
    test_worker_delayed_visibility_skips_future_items(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_orphan_queue_messages_dropped() {
    test_orphan_queue_messages_dropped(&SqliteObjsTestFactory).await;
}

// Management tests
#[tokio::test]
async fn test_sqlite_objs_list_instances() {
    test_list_instances(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_list_instances_by_status() {
    test_list_instances_by_status(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_list_executions() {
    test_list_executions(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_get_instance_info() {
    test_get_instance_info(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_get_execution_info() {
    test_get_execution_info(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_get_system_metrics() {
    test_get_system_metrics(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_get_queue_depths() {
    test_get_queue_depths(&SqliteObjsTestFactory).await;
}

// Instance creation tests
#[tokio::test]
async fn test_sqlite_objs_instance_creation_via_metadata() {
    test_instance_creation_via_metadata(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_no_instance_creation_on_enqueue() {
    test_no_instance_creation_on_enqueue(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_null_version_handling() {
    test_null_version_handling(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_sub_orchestration_instance_creation() {
    test_sub_orchestration_instance_creation(&SqliteObjsTestFactory).await;
}

// Long polling tests (SQLite uses short polling)
#[tokio::test]
async fn test_sqlite_objs_short_poll_returns_immediately() {
    let provider = SqliteObjsTestFactory.create_provider().await;
    test_short_poll_returns_immediately(&*provider, SqliteObjsTestFactory.short_poll_threshold()).await;
}

#[tokio::test]
async fn test_sqlite_objs_short_poll_work_item_returns_immediately() {
    let provider = SqliteObjsTestFactory.create_provider().await;
    test_short_poll_work_item_returns_immediately(&*provider, SqliteObjsTestFactory.short_poll_threshold()).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_respects_timeout_upper_bound() {
    let provider = SqliteObjsTestFactory.create_provider().await;
    test_fetch_respects_timeout_upper_bound(&*provider).await;
}

// Poison message tests
#[tokio::test]
async fn test_sqlite_objs_orchestration_attempt_count_starts_at_one() {
    orchestration_attempt_count_starts_at_one(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_orchestration_attempt_count_increments_on_refetch() {
    orchestration_attempt_count_increments_on_refetch(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_worker_attempt_count_starts_at_one() {
    worker_attempt_count_starts_at_one(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_worker_attempt_count_increments_on_lock_expiry() {
    worker_attempt_count_increments_on_lock_expiry(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_attempt_count_is_per_message() {
    attempt_count_is_per_message(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_abandon_work_item_ignore_attempt_decrements() {
    abandon_work_item_ignore_attempt_decrements(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_abandon_orchestration_item_ignore_attempt_decrements() {
    abandon_orchestration_item_ignore_attempt_decrements(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_ignore_attempt_never_goes_negative() {
    ignore_attempt_never_goes_negative(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_max_attempt_count_across_message_batch() {
    max_attempt_count_across_message_batch(&SqliteObjsTestFactory).await;
}

// abandon_work_item tests
#[tokio::test]
async fn test_sqlite_objs_abandon_work_item_releases_lock() {
    test_abandon_work_item_releases_lock(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_abandon_work_item_with_delay() {
    test_abandon_work_item_with_delay(&SqliteObjsTestFactory).await;
}

// Cancellation tests (activity cancellation support)
#[tokio::test]
async fn test_sqlite_objs_fetch_returns_running_state_for_active_orchestration() {
    test_fetch_returns_running_state_for_active_orchestration(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_returns_terminal_state_when_orchestration_completed() {
    test_fetch_returns_terminal_state_when_orchestration_completed(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_returns_terminal_state_when_orchestration_failed() {
    test_fetch_returns_terminal_state_when_orchestration_failed(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_returns_terminal_state_when_orchestration_continued_as_new() {
    test_fetch_returns_terminal_state_when_orchestration_continued_as_new(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_returns_missing_state_when_instance_deleted() {
    test_fetch_returns_missing_state_when_instance_deleted(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_renew_returns_running_when_orchestration_active() {
    test_renew_returns_running_when_orchestration_active(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_renew_returns_terminal_when_orchestration_completed() {
    test_renew_returns_terminal_when_orchestration_completed(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_renew_returns_missing_when_instance_deleted() {
    test_renew_returns_missing_when_instance_deleted(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_ack_work_item_none_deletes_without_enqueue() {
    test_ack_work_item_none_deletes_without_enqueue(&SqliteObjsTestFactory).await;
}

// Lock-stealing activity cancellation tests
#[tokio::test]
async fn test_sqlite_objs_cancelled_activities_deleted_from_worker_queue() {
    test_cancelled_activities_deleted_from_worker_queue(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_ack_work_item_fails_when_entry_deleted() {
    test_ack_work_item_fails_when_entry_deleted(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_renew_fails_when_entry_deleted() {
    test_renew_fails_when_entry_deleted(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_cancelling_nonexistent_activities_is_idempotent() {
    test_cancelling_nonexistent_activities_is_idempotent(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_batch_cancellation_deletes_multiple_activities() {
    test_batch_cancellation_deletes_multiple_activities(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_same_activity_in_worker_items_and_cancelled_is_noop() {
    test_same_activity_in_worker_items_and_cancelled_is_noop(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_orphan_activity_after_instance_force_deletion() {
    test_orphan_activity_after_instance_force_deletion(&SqliteObjsTestFactory).await;
}

// Deletion tests
#[tokio::test]
async fn test_sqlite_objs_delete_terminal_instances() {
    test_delete_terminal_instances(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_running_rejected_force_succeeds() {
    test_delete_running_rejected_force_succeeds(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_nonexistent_instance() {
    test_delete_nonexistent_instance(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_cleans_queues_and_locks() {
    test_delete_cleans_queues_and_locks(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_cascade_delete_hierarchy() {
    test_cascade_delete_hierarchy(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_force_delete_prevents_ack_recreation() {
    test_force_delete_prevents_ack_recreation(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_list_children() {
    test_list_children(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_get_parent_id() {
    test_delete_get_parent_id(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_get_instance_tree() {
    test_delete_get_instance_tree(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_instances_atomic() {
    test_delete_instances_atomic(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_instances_atomic_force() {
    test_delete_instances_atomic_force(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_instances_atomic_orphan_detection() {
    test_delete_instances_atomic_orphan_detection(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_stale_activity_after_delete_recreate() {
    test_stale_activity_after_delete_recreate(&SqliteObjsTestFactory).await;
}

// Worker lock renewal tests
#[tokio::test]
async fn test_sqlite_objs_worker_lock_renewal_success() {
    test_worker_lock_renewal_success(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_worker_lock_renewal_invalid_token() {
    test_worker_lock_renewal_invalid_token(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_worker_lock_renewal_after_expiration() {
    test_worker_lock_renewal_after_expiration(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_worker_lock_renewal_extends_timeout() {
    test_worker_lock_renewal_extends_timeout(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_worker_lock_renewal_after_ack() {
    test_worker_lock_renewal_after_ack(&SqliteObjsTestFactory).await;
}

// Lock expiry boundary tests
#[tokio::test]
async fn test_sqlite_objs_worker_ack_fails_after_lock_expiry() {
    test_worker_ack_fails_after_lock_expiry(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_orchestration_lock_renewal_after_expiration() {
    test_orchestration_lock_renewal_after_expiration(&SqliteObjsTestFactory).await;
}

// Prune tests
#[tokio::test]
async fn test_sqlite_objs_prune_options_combinations() {
    test_prune_options_combinations(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_prune_safety() {
    test_prune_safety(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_prune_bulk() {
    test_prune_bulk(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_prune_bulk_includes_running_instances() {
    test_prune_bulk_includes_running_instances(&SqliteObjsTestFactory).await;
}

// Bulk deletion tests
#[tokio::test]
async fn test_sqlite_objs_delete_instance_bulk_filter_combinations() {
    test_delete_instance_bulk_filter_combinations(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_instance_bulk_safety_and_limits() {
    test_delete_instance_bulk_safety_and_limits(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_instance_bulk_completed_before_filter() {
    test_delete_instance_bulk_completed_before_filter(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_delete_instance_bulk_cascades_to_children() {
    test_delete_instance_bulk_cascades_to_children(&SqliteObjsTestFactory).await;
}

// Capability filtering tests
#[tokio::test]
async fn test_sqlite_objs_fetch_with_filter_none_returns_any_item() {
    test_fetch_with_filter_none_returns_any_item(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_with_compatible_filter_returns_item() {
    test_fetch_with_compatible_filter_returns_item(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_with_incompatible_filter_skips_item() {
    test_fetch_with_incompatible_filter_skips_item(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_filter_skips_incompatible_selects_compatible() {
    test_fetch_filter_skips_incompatible_selects_compatible(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_filter_does_not_lock_skipped_instances() {
    test_fetch_filter_does_not_lock_skipped_instances(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_filter_null_pinned_version_always_compatible() {
    test_fetch_filter_null_pinned_version_always_compatible(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_filter_boundary_versions() {
    test_fetch_filter_boundary_versions(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_pinned_version_stored_via_ack_metadata() {
    test_pinned_version_stored_via_ack_metadata(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_pinned_version_immutable_across_ack_cycles() {
    test_pinned_version_immutable_across_ack_cycles(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_continue_as_new_execution_gets_own_pinned_version() {
    test_continue_as_new_execution_gets_own_pinned_version(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_filter_with_empty_supported_versions_returns_nothing() {
    test_filter_with_empty_supported_versions_returns_nothing(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_concurrent_filtered_fetch_no_double_lock() {
    test_concurrent_filtered_fetch_no_double_lock(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_ack_stores_pinned_version_via_metadata_update() {
    test_ack_stores_pinned_version_via_metadata_update(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_provider_updates_pinned_version_when_told() {
    test_provider_updates_pinned_version_when_told(&SqliteObjsTestFactory).await;
}

// Category I: Deserialization contract tests (provider-agnostic via ProviderFactory)
#[tokio::test]
async fn test_sqlite_objs_fetch_corrupted_history_filtered_vs_unfiltered() {
    test_fetch_corrupted_history_filtered_vs_unfiltered(&SharedSqliteObjsTestFactory::new().await).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_deserialization_error_increments_attempt_count() {
    test_fetch_deserialization_error_increments_attempt_count(&SharedSqliteObjsTestFactory::new().await).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_deserialization_error_eventually_reaches_poison() {
    test_fetch_deserialization_error_eventually_reaches_poison(&SharedSqliteObjsTestFactory::new().await).await;
}

// Category F2: Additional edge cases
#[tokio::test]
async fn test_sqlite_objs_fetch_filter_applied_before_history_deserialization() {
    test_fetch_filter_applied_before_history_deserialization(&SharedSqliteObjsTestFactory::new().await).await;
}

#[tokio::test]
async fn test_sqlite_objs_fetch_single_range_only_uses_first_range() {
    test_fetch_single_range_only_uses_first_range(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_ack_appends_event_to_corrupted_history() {
    test_ack_appends_event_to_corrupted_history(&SharedSqliteObjsTestFactory::new().await).await;
}

// ======================================================================
// Session Routing Validations
// ======================================================================

#[tokio::test]
async fn test_sqlite_objs_non_session_items_fetchable_by_any_worker() {
    test_non_session_items_fetchable_by_any_worker(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_session_item_claimable_when_no_session() {
    test_session_item_claimable_when_no_session(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_session_affinity_same_worker() {
    test_session_affinity_same_worker(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_session_affinity_blocks_other_worker() {
    test_session_affinity_blocks_other_worker(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_different_sessions_different_workers() {
    test_different_sessions_different_workers(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_mixed_session_and_non_session_items() {
    test_mixed_session_and_non_session_items(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_session_claimable_after_lock_expiry() {
    test_session_claimable_after_lock_expiry(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_none_session_skips_session_items() {
    test_none_session_skips_session_items(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_some_session_returns_all_items() {
    test_some_session_returns_all_items(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_renew_session_lock_active() {
    test_renew_session_lock_active(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_renew_session_lock_skips_idle() {
    test_renew_session_lock_skips_idle(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_renew_session_lock_no_sessions() {
    test_renew_session_lock_no_sessions(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_cleanup_removes_expired_no_items() {
    test_cleanup_removes_expired_no_items(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_cleanup_keeps_sessions_with_pending_items() {
    test_cleanup_keeps_sessions_with_pending_items(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_cleanup_keeps_active_sessions() {
    test_cleanup_keeps_active_sessions(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_ack_updates_session_last_activity() {
    test_ack_updates_session_last_activity(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_renew_work_item_updates_session_last_activity() {
    test_renew_work_item_updates_session_last_activity(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_session_items_processed_in_order() {
    test_session_items_processed_in_order(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_non_session_items_returned_with_session_config() {
    test_non_session_items_returned_with_session_config(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_shared_worker_id_any_caller_can_fetch_owned_session() {
    test_shared_worker_id_any_caller_can_fetch_owned_session(&SqliteObjsTestFactory).await;
}

// ======================================================================
// Session Race Condition Validations
// ======================================================================

#[tokio::test]
async fn test_sqlite_objs_concurrent_session_claim_only_one_wins() {
    test_concurrent_session_claim_only_one_wins(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_session_takeover_after_lock_expiry() {
    test_session_takeover_after_lock_expiry(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_cleanup_then_new_item_recreates_session() {
    test_cleanup_then_new_item_recreates_session(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_abandoned_session_item_retryable() {
    test_abandoned_session_item_retryable(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_abandoned_session_item_ignore_attempt() {
    test_abandoned_session_item_ignore_attempt(&SqliteObjsTestFactory).await;
}

// ======================================================================
// Session Lock Expiry Boundary Validations
// ======================================================================

#[tokio::test]
async fn test_sqlite_objs_renew_session_lock_after_expiry_returns_zero() {
    test_renew_session_lock_after_expiry_returns_zero(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_original_worker_reclaims_expired_session() {
    test_original_worker_reclaims_expired_session(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_activity_lock_expires_session_lock_valid_same_worker_refetches() {
    test_activity_lock_expires_session_lock_valid_same_worker_refetches(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_both_locks_expire_different_worker_claims() {
    test_both_locks_expire_different_worker_claims(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_session_lock_expires_activity_lock_valid_ack_succeeds() {
    test_session_lock_expires_activity_lock_valid_ack_succeeds(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_session_lock_expires_new_owner_gets_redelivery() {
    test_session_lock_expires_new_owner_gets_redelivery(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_session_lock_expires_same_worker_reacquires() {
    test_session_lock_expires_same_worker_reacquires(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_session_lock_renewal_extends_past_original_timeout() {
    test_session_lock_renewal_extends_past_original_timeout(&SqliteObjsTestFactory).await;
}

// Custom status tests
use duroxide::provider_validations::custom_status::{
    test_custom_status_clear, test_custom_status_default_on_new_instance, test_custom_status_none_preserves,
    test_custom_status_nonexistent_instance, test_custom_status_polling_no_change, test_custom_status_set,
    test_custom_status_version_increments,
};

#[tokio::test]
async fn test_sqlite_objs_custom_status_set() {
    test_custom_status_set(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_custom_status_clear() {
    test_custom_status_clear(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_custom_status_none_preserves() {
    test_custom_status_none_preserves(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_custom_status_version_increments() {
    test_custom_status_version_increments(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_custom_status_polling_no_change() {
    test_custom_status_polling_no_change(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_custom_status_nonexistent_instance() {
    test_custom_status_nonexistent_instance(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_custom_status_default_on_new_instance() {
    test_custom_status_default_on_new_instance(&SqliteObjsTestFactory).await;
}

// Tag filtering tests
#[tokio::test]
async fn test_sqlite_objs_tag_default_only_fetches_untagged() {
    test_default_only_fetches_untagged(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_tag_tags_fetches_only_matching() {
    test_tags_fetches_only_matching(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_tag_default_and_fetches_untagged_and_matching() {
    test_default_and_fetches_untagged_and_matching(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_tag_none_filter_returns_nothing() {
    test_none_filter_returns_nothing(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_tag_multi_tag_filter() {
    test_multi_tag_filter(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_tag_round_trip_preservation() {
    test_tag_round_trip_preservation(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_tag_any_filter_fetches_everything() {
    test_any_filter_fetches_everything(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_tag_survives_abandon_and_refetch() {
    test_tag_survives_abandon_and_refetch(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_tag_multi_runtime_isolation() {
    test_multi_runtime_tag_isolation(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_tag_preserved_through_ack_orchestration_item() {
    test_tag_preserved_through_ack_orchestration_item(&SqliteObjsTestFactory).await;
}

// KV store tests
use duroxide::provider_validations::kv_store::{
    test_kv_clear_all, test_kv_clear_isolation, test_kv_clear_nonexistent_key, test_kv_clear_single,
    test_kv_cross_execution_overwrite, test_kv_cross_execution_remove_readd, test_kv_delete_instance_cascades,
    test_kv_delete_instance_with_children, test_kv_empty_value, test_kv_execution_id_tracking,
    test_kv_get_nonexistent, test_kv_get_unknown_instance, test_kv_instance_isolation, test_kv_large_value,
    test_kv_overwrite, test_kv_prune_current_execution_protected, test_kv_prune_preserves_overwritten,
    test_kv_prune_removes_orphan_keys, test_kv_set_after_clear, test_kv_set_and_get,
    test_kv_snapshot_after_clear_all, test_kv_snapshot_after_clear_single, test_kv_snapshot_cross_execution,
    test_kv_snapshot_empty, test_kv_snapshot_in_fetch, test_kv_special_chars_in_key,
};

#[tokio::test]
async fn test_sqlite_objs_kv_set_and_get() {
    test_kv_set_and_get(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_overwrite() {
    test_kv_overwrite(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_clear_single() {
    test_kv_clear_single(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_clear_all() {
    test_kv_clear_all(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_get_nonexistent() {
    test_kv_get_nonexistent(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_snapshot_in_fetch() {
    test_kv_snapshot_in_fetch(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_snapshot_after_clear_single() {
    test_kv_snapshot_after_clear_single(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_snapshot_after_clear_all() {
    test_kv_snapshot_after_clear_all(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_execution_id_tracking() {
    test_kv_execution_id_tracking(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_cross_execution_overwrite() {
    test_kv_cross_execution_overwrite(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_cross_execution_remove_readd() {
    test_kv_cross_execution_remove_readd(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_prune_preserves_overwritten() {
    test_kv_prune_preserves_overwritten(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_prune_removes_orphan_keys() {
    test_kv_prune_removes_orphan_keys(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_instance_isolation() {
    test_kv_instance_isolation(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_delete_instance_cascades() {
    test_kv_delete_instance_cascades(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_clear_nonexistent_key() {
    test_kv_clear_nonexistent_key(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_get_unknown_instance() {
    test_kv_get_unknown_instance(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_set_after_clear() {
    test_kv_set_after_clear(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_empty_value() {
    test_kv_empty_value(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_large_value() {
    test_kv_large_value(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_special_chars_in_key() {
    test_kv_special_chars_in_key(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_snapshot_empty() {
    test_kv_snapshot_empty(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_snapshot_cross_execution() {
    test_kv_snapshot_cross_execution(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_prune_current_execution_protected() {
    test_kv_prune_current_execution_protected(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_delete_instance_with_children() {
    test_kv_delete_instance_with_children(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn test_sqlite_objs_kv_clear_isolation() {
    test_kv_clear_isolation(&SqliteObjsTestFactory).await;
}

#[tokio::test]
async fn debug_ack_orchestrator_items() {
    use duroxide::{Event, EventKind};
    use duroxide::providers::{ExecutionMetadata, WorkItem, TagFilter};

    let provider = SqliteObjsProvider::new_in_memory().await.unwrap();

    // Create instance
    provider.enqueue_for_orchestrator(WorkItem::StartOrchestration {
        instance: "inst-1".to_string(),
        orchestration: "Orch".to_string(),
        input: "{}".to_string(),
        version: Some("1.0".to_string()),
        parent_instance: None,
        parent_id: None,
        execution_id: 1,
    }, None).await.unwrap();

    let (_, lock_token, _) = provider.fetch_orchestration_item(
        Duration::from_secs(30), Duration::ZERO, None
    ).await.unwrap().unwrap();

    // Ack with worker + orchestrator items
    provider.ack_orchestration_item(
        &lock_token,
        1,
        vec![Event::with_event_id(1, "inst-1".to_string(), 1, None,
            EventKind::OrchestrationStarted {
                name: "Orch".to_string(), version: "1.0".to_string(), input: "{}".to_string(),
                parent_instance: None, parent_id: None, carry_forward_events: None, initial_custom_status: None,
            })],
        vec![WorkItem::ActivityExecute {
            instance: "inst-1".to_string(), execution_id: 1, id: 1,
            name: "Act".to_string(), input: "in".to_string(), session_id: None, tag: None,
        }],
        vec![WorkItem::TimerFired {
            instance: "inst-1".to_string(), execution_id: 1, id: 1, fire_at_ms: 1234567890,
        }],
        ExecutionMetadata::default(),
        vec![],
    ).await.unwrap();

    // Debug: check the queues
    {
        let conn = provider.get_conn().lock().unwrap();
        let oq_count: i64 = conn.query_row("SELECT COUNT(*) FROM orchestrator_queue", [], |r| r.get(0)).unwrap();
        let wq_count: i64 = conn.query_row("SELECT COUNT(*) FROM worker_queue", [], |r| r.get(0)).unwrap();
        let lock_count: i64 = conn.query_row("SELECT COUNT(*) FROM instance_locks", [], |r| r.get(0)).unwrap();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
        eprintln!("After ack: orchestrator_queue={oq_count}, worker_queue={wq_count}, instance_locks={lock_count}, now_ms={now_ms}");

        let mut stmt = conn.prepare("SELECT id, instance_id, visible_at, lock_token FROM orchestrator_queue").unwrap();
        let rows: Vec<(i64, String, i64, Option<String>)> = stmt.query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        }).unwrap().filter_map(|r| r.ok()).collect();
        for (id, inst, vis, lt) in &rows {
            let visible = *vis <= now_ms;
            eprintln!("  OQ: id={id} instance={inst} visible_at={vis} lock_token={lt:?} is_visible={visible}");
        }

        // Simulate the fetch query
        let candidate: Result<String, _> = conn.query_row(
            "SELECT q.instance_id FROM orchestrator_queue q LEFT JOIN instance_locks il ON q.instance_id = il.instance_id WHERE q.visible_at <= ?1 AND (il.instance_id IS NULL OR il.locked_until <= ?1) ORDER BY q.id LIMIT 1",
            rusqlite::params![now_ms],
            |row| row.get(0),
        );
        eprintln!("  Simulated fetch candidate: {candidate:?}");
    }

    // Fetch worker item
    let (_, w_token, _) = provider.fetch_work_item(
        Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default()
    ).await.unwrap().expect("Should get worker item");
    provider.ack_work_item(&w_token, None).await.unwrap();

    // Try fetch orchestration item
    let result = provider.fetch_orchestration_item(
        Duration::from_secs(30), Duration::ZERO, None
    ).await.unwrap();
    assert!(result.is_some(), "Should get orchestration item with timer");
    let (item, _, _) = result.unwrap();
    assert_eq!(item.messages.len(), 1);
    assert!(matches!(&item.messages[0], WorkItem::TimerFired { .. }));
}

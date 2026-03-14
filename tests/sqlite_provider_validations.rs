//! Provider validation tests for SQLite
//!
//! This test file validates the SQLite provider using the reusable
//! provider validation test suite from `duroxide::provider_validations`.
//!
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

//! These tests automatically enable the `provider-test` feature when running
//! tests within the duroxide repository.

#[cfg(feature = "provider-test")]
mod tests {
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
    use duroxide::providers::Provider;
    use duroxide::providers::sqlite::SqliteProvider;
    use std::sync::Arc;
    use std::time::Duration;

    const TEST_LOCK_TIMEOUT: Duration = Duration::from_millis(1000);

    /// Standard test factory — each `create_provider()` call gets a fresh in-memory DB.
    /// Used by the vast majority of tests that don't need direct DB manipulation.
    struct SqliteTestFactory;

    #[async_trait::async_trait]
    impl ProviderFactory for SqliteTestFactory {
        async fn create_provider(&self) -> Arc<dyn Provider> {
            Arc::new(SqliteProvider::new_in_memory().await.unwrap())
        }

        fn lock_timeout(&self) -> Duration {
            TEST_LOCK_TIMEOUT
        }
    }

    /// Factory backed by a single shared provider. Required for tests that use
    /// `corrupt_instance_history()` or `get_max_attempt_count()`, because those
    /// need direct SQL access to the same DB that `create_provider()` returns.
    struct SharedSqliteTestFactory {
        provider: Arc<SqliteProvider>,
    }

    impl SharedSqliteTestFactory {
        async fn new() -> Self {
            Self {
                provider: Arc::new(SqliteProvider::new_in_memory().await.unwrap()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ProviderFactory for SharedSqliteTestFactory {
        async fn create_provider(&self) -> Arc<dyn Provider> {
            self.provider.clone()
        }

        fn lock_timeout(&self) -> Duration {
            TEST_LOCK_TIMEOUT
        }

        async fn corrupt_instance_history(&self, instance: &str) {
            sqlx::query("UPDATE history SET event_data = 'NOT_VALID_JSON{{{' WHERE instance_id = ?")
                .bind(instance)
                .execute(self.provider.get_pool())
                .await
                .expect("Failed to corrupt history");
        }

        async fn get_max_attempt_count(&self, instance: &str) -> u32 {
            let count: i64 =
                sqlx::query_scalar("SELECT MAX(attempt_count) FROM orchestrator_queue WHERE instance_id = ?")
                    .bind(instance)
                    .fetch_one(self.provider.get_pool())
                    .await
                    .expect("Failed to query attempt_count");
            count as u32
        }
    }

    // Atomicity tests
    #[tokio::test]
    async fn test_sqlite_atomicity_failure_rollback() {
        test_atomicity_failure_rollback(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_multi_operation_atomic_ack() {
        test_multi_operation_atomic_ack(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_lock_released_only_on_successful_ack() {
        test_lock_released_only_on_successful_ack(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_concurrent_ack_prevention() {
        test_concurrent_ack_prevention(&SqliteTestFactory).await;
    }

    // Error handling tests
    #[tokio::test]
    async fn test_sqlite_invalid_lock_token_on_ack() {
        test_invalid_lock_token_on_ack(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_duplicate_event_id_rejection() {
        test_duplicate_event_id_rejection(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_missing_instance_metadata() {
        test_missing_instance_metadata(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_corrupted_serialization_data() {
        test_corrupted_serialization_data(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_lock_expiration_during_ack() {
        test_lock_expiration_during_ack(&SqliteTestFactory).await;
    }

    // Instance locking tests
    #[tokio::test]
    async fn test_sqlite_exclusive_instance_lock() {
        test_exclusive_instance_lock(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_lock_token_uniqueness() {
        test_lock_token_uniqueness(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_invalid_lock_token_rejection() {
        test_invalid_lock_token_rejection(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_concurrent_instance_fetching() {
        test_concurrent_instance_fetching(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_completions_arriving_during_lock_blocked() {
        test_completions_arriving_during_lock_blocked(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_cross_instance_lock_isolation() {
        test_cross_instance_lock_isolation(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_message_tagging_during_lock() {
        test_message_tagging_during_lock(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_ack_only_affects_locked_messages() {
        test_ack_only_affects_locked_messages(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_multi_threaded_lock_contention() {
        test_multi_threaded_lock_contention(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_multi_threaded_no_duplicate_processing() {
        test_multi_threaded_no_duplicate_processing(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_multi_threaded_lock_expiration_recovery() {
        test_multi_threaded_lock_expiration_recovery(&SqliteTestFactory).await;
    }

    // Lock expiration tests
    #[tokio::test]
    async fn test_sqlite_lock_expires_after_timeout() {
        test_lock_expires_after_timeout(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_abandon_releases_lock_immediately() {
        test_abandon_releases_lock_immediately(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_lock_renewal_on_ack() {
        test_lock_renewal_on_ack(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_concurrent_lock_attempts_respect_expiration() {
        test_concurrent_lock_attempts_respect_expiration(&SqliteTestFactory).await;
    }

    // Multi-execution tests
    #[tokio::test]
    async fn test_sqlite_execution_isolation() {
        test_execution_isolation(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_latest_execution_detection() {
        test_latest_execution_detection(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_execution_id_sequencing() {
        test_execution_id_sequencing(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_continue_as_new_creates_new_execution() {
        test_continue_as_new_creates_new_execution(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_execution_history_persistence() {
        test_execution_history_persistence(&SqliteTestFactory).await;
    }

    // Queue semantics tests
    #[tokio::test]
    async fn test_sqlite_worker_queue_fifo_ordering() {
        test_worker_queue_fifo_ordering(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_worker_peek_lock_semantics() {
        test_worker_peek_lock_semantics(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_worker_ack_atomicity() {
        test_worker_ack_atomicity(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_timer_delayed_visibility() {
        test_timer_delayed_visibility(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_lost_lock_token_handling() {
        test_lost_lock_token_handling(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_worker_item_immediate_visibility() {
        test_worker_item_immediate_visibility(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_worker_delayed_visibility_skips_future_items() {
        test_worker_delayed_visibility_skips_future_items(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_orphan_queue_messages_dropped() {
        test_orphan_queue_messages_dropped(&SqliteTestFactory).await;
    }

    // Management tests
    #[tokio::test]
    async fn test_sqlite_list_instances() {
        test_list_instances(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_list_instances_by_status() {
        test_list_instances_by_status(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_list_executions() {
        test_list_executions(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_get_instance_info() {
        test_get_instance_info(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_get_execution_info() {
        test_get_execution_info(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_get_system_metrics() {
        test_get_system_metrics(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_get_queue_depths() {
        test_get_queue_depths(&SqliteTestFactory).await;
    }

    // Instance creation tests
    #[tokio::test]
    async fn test_sqlite_instance_creation_via_metadata() {
        test_instance_creation_via_metadata(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_no_instance_creation_on_enqueue() {
        test_no_instance_creation_on_enqueue(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_null_version_handling() {
        test_null_version_handling(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_sub_orchestration_instance_creation() {
        test_sub_orchestration_instance_creation(&SqliteTestFactory).await;
    }

    // Long polling tests (SQLite uses short polling)
    #[tokio::test]
    async fn test_sqlite_short_poll_returns_immediately() {
        let provider = SqliteTestFactory.create_provider().await;
        test_short_poll_returns_immediately(&*provider, SqliteTestFactory.short_poll_threshold()).await;
    }

    #[tokio::test]
    async fn test_sqlite_short_poll_work_item_returns_immediately() {
        let provider = SqliteTestFactory.create_provider().await;
        test_short_poll_work_item_returns_immediately(&*provider, SqliteTestFactory.short_poll_threshold()).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_respects_timeout_upper_bound() {
        let provider = SqliteTestFactory.create_provider().await;
        test_fetch_respects_timeout_upper_bound(&*provider).await;
    }

    // Poison message tests
    #[tokio::test]
    async fn test_sqlite_orchestration_attempt_count_starts_at_one() {
        orchestration_attempt_count_starts_at_one(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_orchestration_attempt_count_increments_on_refetch() {
        orchestration_attempt_count_increments_on_refetch(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_worker_attempt_count_starts_at_one() {
        worker_attempt_count_starts_at_one(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_worker_attempt_count_increments_on_lock_expiry() {
        worker_attempt_count_increments_on_lock_expiry(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_attempt_count_is_per_message() {
        attempt_count_is_per_message(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_abandon_work_item_ignore_attempt_decrements() {
        abandon_work_item_ignore_attempt_decrements(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_abandon_orchestration_item_ignore_attempt_decrements() {
        abandon_orchestration_item_ignore_attempt_decrements(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_ignore_attempt_never_goes_negative() {
        ignore_attempt_never_goes_negative(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_max_attempt_count_across_message_batch() {
        max_attempt_count_across_message_batch(&SqliteTestFactory).await;
    }

    // abandon_work_item tests
    #[tokio::test]
    async fn test_sqlite_abandon_work_item_releases_lock() {
        test_abandon_work_item_releases_lock(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_abandon_work_item_with_delay() {
        test_abandon_work_item_with_delay(&SqliteTestFactory).await;
    }

    // Cancellation tests (activity cancellation support)
    #[tokio::test]
    async fn test_sqlite_fetch_returns_running_state_for_active_orchestration() {
        test_fetch_returns_running_state_for_active_orchestration(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_returns_terminal_state_when_orchestration_completed() {
        test_fetch_returns_terminal_state_when_orchestration_completed(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_returns_terminal_state_when_orchestration_failed() {
        test_fetch_returns_terminal_state_when_orchestration_failed(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_returns_terminal_state_when_orchestration_continued_as_new() {
        test_fetch_returns_terminal_state_when_orchestration_continued_as_new(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_returns_missing_state_when_instance_deleted() {
        test_fetch_returns_missing_state_when_instance_deleted(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_renew_returns_running_when_orchestration_active() {
        test_renew_returns_running_when_orchestration_active(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_renew_returns_terminal_when_orchestration_completed() {
        test_renew_returns_terminal_when_orchestration_completed(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_renew_returns_missing_when_instance_deleted() {
        test_renew_returns_missing_when_instance_deleted(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_ack_work_item_none_deletes_without_enqueue() {
        test_ack_work_item_none_deletes_without_enqueue(&SqliteTestFactory).await;
    }

    // Lock-stealing activity cancellation tests
    #[tokio::test]
    async fn test_sqlite_cancelled_activities_deleted_from_worker_queue() {
        test_cancelled_activities_deleted_from_worker_queue(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_ack_work_item_fails_when_entry_deleted() {
        test_ack_work_item_fails_when_entry_deleted(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_renew_fails_when_entry_deleted() {
        test_renew_fails_when_entry_deleted(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_cancelling_nonexistent_activities_is_idempotent() {
        test_cancelling_nonexistent_activities_is_idempotent(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_batch_cancellation_deletes_multiple_activities() {
        test_batch_cancellation_deletes_multiple_activities(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_same_activity_in_worker_items_and_cancelled_is_noop() {
        test_same_activity_in_worker_items_and_cancelled_is_noop(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_orphan_activity_after_instance_force_deletion() {
        test_orphan_activity_after_instance_force_deletion(&SqliteTestFactory).await;
    }

    // Deletion tests
    #[tokio::test]
    async fn test_sqlite_delete_terminal_instances() {
        test_delete_terminal_instances(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_running_rejected_force_succeeds() {
        test_delete_running_rejected_force_succeeds(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_nonexistent_instance() {
        test_delete_nonexistent_instance(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_cleans_queues_and_locks() {
        test_delete_cleans_queues_and_locks(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_cascade_delete_hierarchy() {
        test_cascade_delete_hierarchy(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_force_delete_prevents_ack_recreation() {
        test_force_delete_prevents_ack_recreation(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_list_children() {
        test_list_children(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_get_parent_id() {
        test_delete_get_parent_id(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_get_instance_tree() {
        test_delete_get_instance_tree(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_instances_atomic() {
        test_delete_instances_atomic(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_instances_atomic_force() {
        test_delete_instances_atomic_force(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_instances_atomic_orphan_detection() {
        test_delete_instances_atomic_orphan_detection(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_stale_activity_after_delete_recreate() {
        test_stale_activity_after_delete_recreate(&SqliteTestFactory).await;
    }

    // Worker lock renewal tests
    #[tokio::test]
    async fn test_sqlite_worker_lock_renewal_success() {
        test_worker_lock_renewal_success(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_worker_lock_renewal_invalid_token() {
        test_worker_lock_renewal_invalid_token(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_worker_lock_renewal_after_expiration() {
        test_worker_lock_renewal_after_expiration(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_worker_lock_renewal_extends_timeout() {
        test_worker_lock_renewal_extends_timeout(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_worker_lock_renewal_after_ack() {
        test_worker_lock_renewal_after_ack(&SqliteTestFactory).await;
    }

    // Lock expiry boundary tests
    #[tokio::test]
    async fn test_sqlite_worker_ack_fails_after_lock_expiry() {
        test_worker_ack_fails_after_lock_expiry(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_orchestration_lock_renewal_after_expiration() {
        test_orchestration_lock_renewal_after_expiration(&SqliteTestFactory).await;
    }

    // Prune tests
    #[tokio::test]
    async fn test_sqlite_prune_options_combinations() {
        test_prune_options_combinations(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_prune_safety() {
        test_prune_safety(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_prune_bulk() {
        test_prune_bulk(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_prune_bulk_includes_running_instances() {
        test_prune_bulk_includes_running_instances(&SqliteTestFactory).await;
    }

    // Bulk deletion tests
    #[tokio::test]
    async fn test_sqlite_delete_instance_bulk_filter_combinations() {
        test_delete_instance_bulk_filter_combinations(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_instance_bulk_safety_and_limits() {
        test_delete_instance_bulk_safety_and_limits(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_instance_bulk_completed_before_filter() {
        test_delete_instance_bulk_completed_before_filter(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_delete_instance_bulk_cascades_to_children() {
        test_delete_instance_bulk_cascades_to_children(&SqliteTestFactory).await;
    }

    // Capability filtering tests
    #[tokio::test]
    async fn test_sqlite_fetch_with_filter_none_returns_any_item() {
        test_fetch_with_filter_none_returns_any_item(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_with_compatible_filter_returns_item() {
        test_fetch_with_compatible_filter_returns_item(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_with_incompatible_filter_skips_item() {
        test_fetch_with_incompatible_filter_skips_item(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_filter_skips_incompatible_selects_compatible() {
        test_fetch_filter_skips_incompatible_selects_compatible(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_filter_does_not_lock_skipped_instances() {
        test_fetch_filter_does_not_lock_skipped_instances(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_filter_null_pinned_version_always_compatible() {
        test_fetch_filter_null_pinned_version_always_compatible(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_filter_boundary_versions() {
        test_fetch_filter_boundary_versions(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_pinned_version_stored_via_ack_metadata() {
        test_pinned_version_stored_via_ack_metadata(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_pinned_version_immutable_across_ack_cycles() {
        test_pinned_version_immutable_across_ack_cycles(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_continue_as_new_execution_gets_own_pinned_version() {
        test_continue_as_new_execution_gets_own_pinned_version(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_filter_with_empty_supported_versions_returns_nothing() {
        test_filter_with_empty_supported_versions_returns_nothing(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_concurrent_filtered_fetch_no_double_lock() {
        test_concurrent_filtered_fetch_no_double_lock(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_ack_stores_pinned_version_via_metadata_update() {
        test_ack_stores_pinned_version_via_metadata_update(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_provider_updates_pinned_version_when_told() {
        test_provider_updates_pinned_version_when_told(&SqliteTestFactory).await;
    }

    // Category I: Deserialization contract tests (provider-agnostic via ProviderFactory)
    #[tokio::test]
    async fn test_sqlite_fetch_corrupted_history_filtered_vs_unfiltered() {
        test_fetch_corrupted_history_filtered_vs_unfiltered(&SharedSqliteTestFactory::new().await).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_deserialization_error_increments_attempt_count() {
        test_fetch_deserialization_error_increments_attempt_count(&SharedSqliteTestFactory::new().await).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_deserialization_error_eventually_reaches_poison() {
        test_fetch_deserialization_error_eventually_reaches_poison(&SharedSqliteTestFactory::new().await).await;
    }

    // Category F2: Additional edge cases
    #[tokio::test]
    async fn test_sqlite_fetch_filter_applied_before_history_deserialization() {
        test_fetch_filter_applied_before_history_deserialization(&SharedSqliteTestFactory::new().await).await;
    }

    #[tokio::test]
    async fn test_sqlite_fetch_single_range_only_uses_first_range() {
        test_fetch_single_range_only_uses_first_range(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_ack_appends_event_to_corrupted_history() {
        test_ack_appends_event_to_corrupted_history(&SharedSqliteTestFactory::new().await).await;
    }

    // ======================================================================
    // Session Routing Validations
    // ======================================================================

    #[tokio::test]
    async fn test_sqlite_non_session_items_fetchable_by_any_worker() {
        test_non_session_items_fetchable_by_any_worker(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_session_item_claimable_when_no_session() {
        test_session_item_claimable_when_no_session(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_session_affinity_same_worker() {
        test_session_affinity_same_worker(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_session_affinity_blocks_other_worker() {
        test_session_affinity_blocks_other_worker(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_different_sessions_different_workers() {
        test_different_sessions_different_workers(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_mixed_session_and_non_session_items() {
        test_mixed_session_and_non_session_items(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_session_claimable_after_lock_expiry() {
        test_session_claimable_after_lock_expiry(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_none_session_skips_session_items() {
        test_none_session_skips_session_items(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_some_session_returns_all_items() {
        test_some_session_returns_all_items(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_renew_session_lock_active() {
        test_renew_session_lock_active(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_renew_session_lock_skips_idle() {
        test_renew_session_lock_skips_idle(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_renew_session_lock_no_sessions() {
        test_renew_session_lock_no_sessions(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_cleanup_removes_expired_no_items() {
        test_cleanup_removes_expired_no_items(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_cleanup_keeps_sessions_with_pending_items() {
        test_cleanup_keeps_sessions_with_pending_items(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_cleanup_keeps_active_sessions() {
        test_cleanup_keeps_active_sessions(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_ack_updates_session_last_activity() {
        test_ack_updates_session_last_activity(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_renew_work_item_updates_session_last_activity() {
        test_renew_work_item_updates_session_last_activity(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_session_items_processed_in_order() {
        test_session_items_processed_in_order(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_non_session_items_returned_with_session_config() {
        test_non_session_items_returned_with_session_config(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_shared_worker_id_any_caller_can_fetch_owned_session() {
        test_shared_worker_id_any_caller_can_fetch_owned_session(&SqliteTestFactory).await;
    }

    // ======================================================================
    // Session Race Condition Validations
    // ======================================================================

    #[tokio::test]
    async fn test_sqlite_concurrent_session_claim_only_one_wins() {
        test_concurrent_session_claim_only_one_wins(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_session_takeover_after_lock_expiry() {
        test_session_takeover_after_lock_expiry(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_cleanup_then_new_item_recreates_session() {
        test_cleanup_then_new_item_recreates_session(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_abandoned_session_item_retryable() {
        test_abandoned_session_item_retryable(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_abandoned_session_item_ignore_attempt() {
        test_abandoned_session_item_ignore_attempt(&SqliteTestFactory).await;
    }

    // ======================================================================
    // Session Lock Expiry Boundary Validations
    // ======================================================================

    #[tokio::test]
    async fn test_sqlite_renew_session_lock_after_expiry_returns_zero() {
        test_renew_session_lock_after_expiry_returns_zero(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_original_worker_reclaims_expired_session() {
        test_original_worker_reclaims_expired_session(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_activity_lock_expires_session_lock_valid_same_worker_refetches() {
        test_activity_lock_expires_session_lock_valid_same_worker_refetches(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_both_locks_expire_different_worker_claims() {
        test_both_locks_expire_different_worker_claims(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_session_lock_expires_activity_lock_valid_ack_succeeds() {
        test_session_lock_expires_activity_lock_valid_ack_succeeds(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_session_lock_expires_new_owner_gets_redelivery() {
        test_session_lock_expires_new_owner_gets_redelivery(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_session_lock_expires_same_worker_reacquires() {
        test_session_lock_expires_same_worker_reacquires(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_session_lock_renewal_extends_past_original_timeout() {
        test_session_lock_renewal_extends_past_original_timeout(&SqliteTestFactory).await;
    }

    // Custom status tests
    use duroxide::provider_validations::custom_status::{
        test_custom_status_clear, test_custom_status_default_on_new_instance, test_custom_status_none_preserves,
        test_custom_status_nonexistent_instance, test_custom_status_polling_no_change, test_custom_status_set,
        test_custom_status_version_increments,
    };

    #[tokio::test]
    async fn test_sqlite_custom_status_set() {
        test_custom_status_set(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_custom_status_clear() {
        test_custom_status_clear(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_custom_status_none_preserves() {
        test_custom_status_none_preserves(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_custom_status_version_increments() {
        test_custom_status_version_increments(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_custom_status_polling_no_change() {
        test_custom_status_polling_no_change(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_custom_status_nonexistent_instance() {
        test_custom_status_nonexistent_instance(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_custom_status_default_on_new_instance() {
        test_custom_status_default_on_new_instance(&SqliteTestFactory).await;
    }

    // Tag filtering tests
    #[tokio::test]
    async fn test_sqlite_tag_default_only_fetches_untagged() {
        test_default_only_fetches_untagged(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_tag_tags_fetches_only_matching() {
        test_tags_fetches_only_matching(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_tag_default_and_fetches_untagged_and_matching() {
        test_default_and_fetches_untagged_and_matching(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_tag_none_filter_returns_nothing() {
        test_none_filter_returns_nothing(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_tag_multi_tag_filter() {
        test_multi_tag_filter(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_tag_round_trip_preservation() {
        test_tag_round_trip_preservation(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_tag_any_filter_fetches_everything() {
        test_any_filter_fetches_everything(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_tag_survives_abandon_and_refetch() {
        test_tag_survives_abandon_and_refetch(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_tag_multi_runtime_isolation() {
        test_multi_runtime_tag_isolation(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_tag_preserved_through_ack_orchestration_item() {
        test_tag_preserved_through_ack_orchestration_item(&SqliteTestFactory).await;
    }

    // KV store tests
    use duroxide::provider_validations::kv_store::{
        test_kv_clear_all, test_kv_clear_isolation, test_kv_clear_nonexistent_key, test_kv_clear_single,
        test_kv_cross_execution_overwrite, test_kv_cross_execution_remove_readd, test_kv_delete_instance_cascades,
        test_kv_delete_instance_with_children, test_kv_empty_value, test_kv_execution_id_tracking,
        test_kv_get_nonexistent, test_kv_get_unknown_instance, test_kv_instance_isolation, test_kv_large_value,
        test_kv_overwrite, test_kv_prune_current_execution_protected, test_kv_prune_preserves_all_keys,
        test_kv_prune_preserves_overwritten, test_kv_set_after_clear, test_kv_set_and_get,
        test_kv_snapshot_after_clear_all, test_kv_snapshot_after_clear_single, test_kv_snapshot_cross_execution,
        test_kv_snapshot_empty, test_kv_snapshot_in_fetch, test_kv_special_chars_in_key,
    };

    #[tokio::test]
    async fn test_sqlite_kv_set_and_get() {
        test_kv_set_and_get(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_overwrite() {
        test_kv_overwrite(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_clear_single() {
        test_kv_clear_single(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_clear_all() {
        test_kv_clear_all(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_get_nonexistent() {
        test_kv_get_nonexistent(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_snapshot_in_fetch() {
        test_kv_snapshot_in_fetch(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_snapshot_after_clear_single() {
        test_kv_snapshot_after_clear_single(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_snapshot_after_clear_all() {
        test_kv_snapshot_after_clear_all(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_execution_id_tracking() {
        test_kv_execution_id_tracking(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_cross_execution_overwrite() {
        test_kv_cross_execution_overwrite(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_cross_execution_remove_readd() {
        test_kv_cross_execution_remove_readd(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_prune_preserves_overwritten() {
        test_kv_prune_preserves_overwritten(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_prune_preserves_all_keys() {
        test_kv_prune_preserves_all_keys(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_instance_isolation() {
        test_kv_instance_isolation(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_delete_instance_cascades() {
        test_kv_delete_instance_cascades(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_clear_nonexistent_key() {
        test_kv_clear_nonexistent_key(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_get_unknown_instance() {
        test_kv_get_unknown_instance(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_set_after_clear() {
        test_kv_set_after_clear(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_empty_value() {
        test_kv_empty_value(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_large_value() {
        test_kv_large_value(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_special_chars_in_key() {
        test_kv_special_chars_in_key(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_snapshot_empty() {
        test_kv_snapshot_empty(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_snapshot_cross_execution() {
        test_kv_snapshot_cross_execution(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_prune_current_execution_protected() {
        test_kv_prune_current_execution_protected(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_delete_instance_with_children() {
        test_kv_delete_instance_with_children(&SqliteTestFactory).await;
    }

    #[tokio::test]
    async fn test_sqlite_kv_clear_isolation() {
        test_kv_clear_isolation(&SqliteTestFactory).await;
    }
}

# Provider Testing Guide

**For:** Developers testing custom Duroxide providers  
**Reference:** See `sqlite-stress/src/lib.rs` for SQLite stress test implementation

---

## Quick Start

Duroxide provides two types of tests for custom providers:

### 1. Stress Tests (Performance & Throughput)
- Measures: throughput, latency, success rate
- Use `provider-test` feature
- See [Stress Tests](#stress-tests) section below

### 2. Validation Tests (Behavior Validation)
- Validates: atomicity, locking, error handling, queue semantics, management capabilities
- Use `provider-test` feature (same as stress tests)
- See [Provider Validation Tests](#provider-validation-tests) section below

**Recommended Testing Strategy:**
1. Run validation tests first to validate behavior
2. Run stress tests to measure performance
3. Both should pass with 100% success rate

---

## Adding the Dependency

Add Duroxide with the `provider-test` feature to your repository's `Cargo.toml`:

```toml
[dependencies]
duroxide = { path = "../duroxide", features = ["provider-test"] }
```

The `provider-test` feature enables access to the stress testing infrastructure.

---

## Stress Tests

Stress tests validate your provider under load, measuring throughput, latency, and success rate with concurrent orchestrations.

### Basic Example

Implement the `ProviderStressFactory` trait to enable stress testing:

```rust
use duroxide::provider_stress_tests::parallel_orchestrations::{
    ProviderStressFactory, run_parallel_orchestrations_test
};
use duroxide::providers::Provider;
use std::sync::Arc;

struct MyProviderFactory;

#[async_trait::async_trait]
impl ProviderStressFactory for MyProviderFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        // Create a fresh provider instance for stress testing
        Arc::new(MyCustomProvider::new("connection_string").await.unwrap())
    }
}

#[tokio::test]
async fn stress_test_my_provider() {
    let factory = MyProviderFactory;
    let result = run_parallel_orchestrations_test(&factory)
        .await
        .expect("Stress test failed");
    
    // Validate results
    assert!(result.success_rate() > 99.0, "Success rate too low: {:.2}%", result.success_rate());
    assert!(result.orch_throughput > 1.0, "Throughput too low: {:.2} orch/sec", result.orch_throughput);
}
```

The test will:
1. Create a fresh provider instance
2. Launch orchestrations continuously for the configured duration
3. Track completed and failed instances
4. Calculate throughput and latency metrics
5. Return detailed `StressTestResult` with all metrics

---

## Custom Configuration

Override the default configuration by implementing `stress_test_config()`:

```rust
use duroxide::provider_stress_tests::StressTestConfig;

#[async_trait::async_trait]
impl ProviderStressFactory for MyProviderFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(MyCustomProvider::new().await.unwrap())
    }
    
    // Optional: customize the stress test configuration
    fn stress_test_config(&self) -> StressTestConfig {
        StressTestConfig {
            max_concurrent: 10,       // Max concurrent instances at once
            duration_secs: 30,        // How long to run the test
            tasks_per_instance: 3,    // Activities per orchestration
            activity_delay_ms: 50,    // Simulated activity work time
            orch_concurrency: 1,      // Orchestration dispatcher threads
            worker_concurrency: 1,    // Activity worker threads
            wait_timeout_secs: 60,    // Timeout for wait_for_orchestration
        }
    }
}
```

Or pass a custom config directly:

```rust
use duroxide::provider_stress_tests::parallel_orchestrations::run_parallel_orchestrations_test_with_config;

let config = StressTestConfig {
    max_concurrent: 20,
    duration_secs: 10,
    tasks_per_instance: 5,
    activity_delay_ms: 10,
    orch_concurrency: 2,
    worker_concurrency: 2,
    wait_timeout_secs: 60,
};

let result = run_parallel_orchestrations_test_with_config(&factory, config).await?;
```

### Recommended Configurations

**Quick Validation (for CI):**
```rust
StressTestConfig {
    max_concurrent: 5,
    duration_secs: 2,
    tasks_per_instance: 2,
    activity_delay_ms: 5,
    orch_concurrency: 1,
    worker_concurrency: 1,
    wait_timeout_secs: 60,
}
```

**Performance Baseline:**
```rust
StressTestConfig {
    max_concurrent: 20,
    duration_secs: 10,
    tasks_per_instance: 5,
    activity_delay_ms: 10,
    orch_concurrency: 1,
    worker_concurrency: 1,
    wait_timeout_secs: 60,
}
```

**Concurrency Stress Test:**
```rust
StressTestConfig {
    max_concurrent: 20,
    duration_secs: 10,
    tasks_per_instance: 5,
    activity_delay_ms: 10,
    orch_concurrency: 2,
    worker_concurrency: 2,
    wait_timeout_secs: 60,
}
```

---

## Advanced: Custom Orchestrations and Activities

For custom test scenarios, use the lower-level `run_stress_test` function:

```rust
use duroxide::provider_stress_tests::{run_stress_test, create_default_activities, StressTestConfig};
use duroxide::{OrchestrationContext, OrchestrationRegistry, ActivityContext};
use std::sync::Arc;

// Custom orchestration
async fn custom_orchestration(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    let tasks = vec!["task1", "task2", "task3"];
    let mut results = Vec::new();
    
    for task in tasks {
        let result = ctx.schedule_activity("ProcessTask", task.to_string())
            .await?;
        results.push(result);
    }
    
    Ok(format!("completed: {}", results.join(", ")))
}

#[tokio::test]
async fn custom_stress_test() {
    let provider = Arc::new(MyCustomProvider::new().await.unwrap());
    
    // Use default activities or create custom ones
    let activities = create_default_activities(10);
    
    // Register custom orchestration
    let orchestrations = OrchestrationRegistry::builder()
        .register("CustomWorkflow", custom_orchestration)
        .build();
    
    let config = StressTestConfig::default();
    
    let result = run_stress_test(config, provider, activities, orchestrations)
        .await
        .expect("Stress test failed");
    
    assert!(result.success_rate() > 99.0);
}
```

---

## Test Scenarios

Duroxide provides two built-in stress test scenarios:

### Parallel Orchestrations (Fan-Out/Fan-In)

Each orchestration:
1. Fans out to N activities in parallel
2. Waits for all activities to complete
3. Returns a success message

This pattern tests:
- Concurrent activity execution
- Message queue throughput
- Database write concurrency
- Instance-level locking correctness

Use the `ProviderStressFactory` trait:
```rust
use duroxide::provider_stress_tests::parallel_orchestrations::{
    ProviderStressFactory, run_parallel_orchestrations_test
};
```

### Large Payload (Memory Stress)

Each orchestration:
1. Schedules activities with large payloads (10KB, 50KB, 100KB)
2. Creates moderate-length histories (~80-100 events)
3. Spawns sub-orchestrations with large inputs/outputs

This pattern tests:
- Memory allocation efficiency
- History storage and retrieval with large events
- Event serialization/deserialization overhead
- Provider memory footprint under load

Use the same `ProviderStressFactory` trait as parallel orchestrations:
```rust
use duroxide::provider_stress_tests::parallel_orchestrations::ProviderStressFactory;
use duroxide::provider_stress_tests::large_payload::{
    LargePayloadConfig, run_large_payload_test, run_large_payload_test_with_config
};

// Same factory implementation works for both stress tests!
struct MyProviderFactory;

#[async_trait::async_trait]
impl ProviderStressFactory for MyProviderFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(MyProvider::new().await.unwrap())
    }
}

#[tokio::test]
async fn large_payload_stress_test() {
    let factory = MyProviderFactory;
    // Run with default config
    let result = run_large_payload_test(&factory).await.unwrap();
    assert!(result.success_rate() > 99.0);
}

#[tokio::test]
async fn large_payload_stress_test_custom_config() {
    let factory = MyProviderFactory;
    // Or run with custom config
    let config = LargePayloadConfig {
        small_payload_kb: 5,
        medium_payload_kb: 25,
        large_payload_kb: 50,
        ..Default::default()
    };
    let result = run_large_payload_test_with_config(&factory, config).await.unwrap();
    assert!(result.success_rate() > 99.0);
}
```

### Creating Custom Scenarios

To add a new test scenario:

1. **Define the orchestration**:
```rust
async fn my_scenario(ctx: OrchestrationContext, input: String) -> Result<String, Box<dyn std::error::Error>> {
    // Your test scenario logic
    Ok("done".to_string())
}
```

2. **Register it in the test**:
```rust
let orchestrations = OrchestrationRegistry::builder()
    .register("MyScenario", my_scenario)
    .build();
```

3. **Create activities if needed**:
```rust
let activities = Arc::new(
    ActivityRegistry::builder()
        .register("MyActivity", my_activity)
        .build()
);
```

4. **Launch test instances**:
```rust
for i in 0..num_instances {
    let instance_id = format!("test-{}", i);
    client.start_orchestration("MyScenario", instance_id, input.clone()).await?;
}
```

---

## Interpreting Results

The stress test outputs a results table:

```
=== Comparison Table ===
Provider             Config     Completed  Failed     Success %  Orch/sec        Activity/sec    Avg Latency    
------------------------------------------------------------------------------------------------------------------------
In-Memory SQLite     1/1        179        0          100.00     4.63            23.13           216.21         ms
In-Memory SQLite     2/2        278        0          100.00     7.09            35.45           141.04         ms
File SQLite          1/1        167        0          100.00     14.75           73.76           67.78          ms
File SQLite          2/2        281        0          100.00     25.98           129.88          38.49          ms
```

### Key Metrics

**Completed**: Number of orchestrations that finished successfully  
**Failed**: Number of orchestrations that encountered errors  
**Success %**: `(completed / (completed + failed)) * 100` - MUST be 100%  
**Orch/sec**: Orchestrations completed per second (throughput)  
**Activity/sec**: Activities executed per second  
**Avg Latency**: Mean time from start to completion of an orchestration

### Expected Results

✅ **Success Rate = 100%**: All orchestrations complete without errors  
✅ **Consistent Throughput**: Metrics remain stable across runs  
✅ **Deterministic**: Same input produces same output  
✅ **Scalable**: Higher concurrency increases throughput

### Warning Signs

❌ **Success Rate < 100%**: Indicates correctness issues (locks, atomicity, etc.)  
❌ **Throughput = 0**: Provider not committing work  
❌ **Highly Variable Latency**: Lock contention or database issues  
❌ **Decreasing Throughput**: Resource exhaustion or contention

---

## Provider Validation Tests

Duroxide includes a comprehensive suite of validation tests that validate provider behavior. These tests verify critical correctness properties like atomicity, locking, error handling, queue semantics, and management capabilities.

### Quick Start

To run validation tests against your custom provider:

1. Add `duroxide` with `provider-test` feature to your project
2. Implement the `ProviderFactory` trait
3. Run individual test functions for each validation test

### Adding the Dependency

Add Duroxide with the `provider-test` feature:

```toml
[dependencies]
duroxide = { path = "../duroxide", features = ["provider-test"] }
```

> **Note:** The `provider-test` feature enables both stress tests and validation tests. Enable this single feature to get all provider testing infrastructure.

### Basic Example

Run validation tests by calling individual test functions:

```rust
use duroxide::providers::Provider;
use duroxide::provider_validations::{
    ProviderFactory,
    // Atomicity tests
    test_atomicity_failure_rollback,
    test_multi_operation_atomic_ack,
    // Locking tests
    test_exclusive_instance_lock,
    // Queue tests
    test_worker_queue_fifo_ordering,
    // Instance creation tests
    test_instance_creation_via_metadata,
    test_no_instance_creation_on_enqueue,
    test_null_version_handling,
    test_sub_orchestration_instance_creation,
    // Cancellation support tests (execution state)
    test_fetch_returns_running_state_for_active_orchestration,
    test_fetch_returns_terminal_state_when_orchestration_completed,
    test_renew_returns_running_when_orchestration_active,
    test_ack_work_item_none_deletes_without_enqueue,
    // Lock-stealing activity cancellation tests
    test_cancelled_activities_deleted_from_worker_queue,
    test_ack_work_item_fails_when_entry_deleted,
    test_renew_fails_when_entry_deleted,
    test_cancelling_nonexistent_activities_is_idempotent,
    test_batch_cancellation_deletes_multiple_activities,
    test_same_activity_in_worker_items_and_cancelled_is_noop,
    test_orphan_activity_after_instance_force_deletion,
    // ... import other tests as needed
};
use std::sync::Arc;
use std::time::Duration;

const TEST_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

struct MyProviderFactory;

#[async_trait::async_trait]
impl ProviderFactory for MyProviderFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        // Create a fresh provider instance for each test
        Arc::new(MyCustomProvider::new().await?)
    }

    fn lock_timeout(&self) -> Duration {
        // Return the lock timeout configured in your provider
        // This must match the timeout used when creating the provider
        TEST_LOCK_TIMEOUT
    }

    // Optional: Override these for deserialization contract tests (Category I).
    // Required by tests that inject corrupted history and verify attempt counting.

    async fn corrupt_instance_history(&self, instance: &str) {
        // Replace stored history event data with undeserializable content.
        // The exact mechanism is provider-specific (e.g., raw SQL UPDATE for
        // SQL-backed providers, direct key mutation for KV stores).
        my_provider_raw_update_history(instance, "NOT_VALID_JSON{{{").await;
    }

    async fn get_max_attempt_count(&self, instance: &str) -> u32 {
        // Return the max attempt_count from the orchestrator queue for this instance.
        my_provider_query_max_attempt_count(instance).await
    }
}

#[tokio::test]
async fn test_my_provider_atomicity_failure_rollback() {
    let factory = MyProviderFactory;
    test_atomicity_failure_rollback(&factory).await;
}

#[tokio::test]
async fn test_my_provider_exclusive_instance_lock() {
    let factory = MyProviderFactory;
    test_exclusive_instance_lock(&factory).await;
}

#[tokio::test]
async fn test_my_provider_worker_queue_fifo_ordering() {
    let factory = MyProviderFactory;
    test_worker_queue_fifo_ordering(&factory).await;
}
```

> **Important:** Run each test function individually. This provides better test isolation, clearer failure reporting, and allows parallel execution in CI/CD pipelines. When a test fails, you'll know exactly which behavior is broken.

**Note:** All provider methods return `Result<..., ProviderError>` instead of `Result<..., String>`. Tests that check error messages should access the `message` field: `err.message.contains(...)` instead of `err.contains(...)`.

### What the Tests Validate

The validation test suite includes **166 individual test functions** organized into 18 categories:

1. **Atomicity Tests (4 tests)**
   - `test_atomicity_failure_rollback` - All-or-nothing commit semantics, rollback on failure
   - `test_multi_operation_atomic_ack` - Complex ack succeeds atomically
   - `test_lock_released_only_on_successful_ack` - Lock only released on success
   - `test_concurrent_ack_prevention` - Only one ack succeeds with same token

2. **Error Handling Tests (5 tests)**
   - `test_invalid_lock_token_on_ack` - Invalid lock token rejection
   - `test_duplicate_event_id_rejection` - Duplicate event ID detection
   - `test_missing_instance_metadata` - Missing instances handled gracefully
   - `test_corrupted_serialization_data` - Corrupted data handled gracefully
   - `test_lock_expiration_during_ack` - Expired locks are rejected

3. **Instance Locking Tests (11 tests)**
   - `test_exclusive_instance_lock` - Exclusive access to instances
   - `test_lock_token_uniqueness` - Each fetch generates unique lock token
   - `test_invalid_lock_token_rejection` - Invalid tokens rejected for ack/abandon
   - `test_concurrent_instance_fetching` - Concurrent fetches don't duplicate instances
   - `test_completions_arriving_during_lock_blocked` - New messages blocked during lock
   - `test_cross_instance_lock_isolation` - Locks don't block other instances
   - `test_message_tagging_during_lock` - Only fetched messages deleted on ack
   - `test_ack_only_affects_locked_messages` - Ack only affects locked messages
   - `test_multi_threaded_lock_contention` - Locks prevent concurrent processing (multi-threaded)
   - `test_multi_threaded_no_duplicate_processing` - No duplicate processing (multi-threaded)
   - `test_multi_threaded_lock_expiration_recovery` - Lock expiration recovery (multi-threaded)

4. **Lock Expiration Tests (13 tests)**
   - `test_lock_expires_after_timeout` - Automatic lock release after timeout
   - `test_abandon_releases_lock_immediately` - Abandon releases lock immediately
   - `test_lock_renewal_on_ack` - Successful ack releases lock immediately
   - `test_concurrent_lock_attempts_respect_expiration` - Concurrent attempts respect expiration
   - `test_worker_lock_renewal_success` - Worker lock can be renewed with valid token
   - `test_worker_lock_renewal_invalid_token` - Renewal fails with invalid token
   - `test_worker_lock_renewal_after_expiration` - Renewal fails after lock expires
   - `test_worker_lock_renewal_extends_timeout` - Renewal properly extends lock timeout
   - `test_worker_lock_renewal_after_ack` - Renewal fails after item has been acked
   - `test_abandon_work_item_releases_lock` - abandon_work_item releases lock immediately
   - `test_abandon_work_item_with_delay` - abandon_work_item with delay defers visibility
   - `test_worker_ack_fails_after_lock_expiry` - Worker ack rejected after lock expires
   - `test_orchestration_lock_renewal_after_expiration` - Orchestration lock renewal fails after expiry

5. **Multi-Execution Tests (5 tests)**
   - `test_execution_isolation` - Each execution has separate history
   - `test_latest_execution_detection` - read() returns latest execution
   - `test_execution_id_sequencing` - Execution IDs increment correctly
   - `test_continue_as_new_creates_new_execution` - ContinueAsNew creates new execution
   - `test_execution_history_persistence` - All executions' history persists independently

6. **Queue Semantics Tests (8 tests)**
   - `test_worker_queue_fifo_ordering` - Worker items dequeued in FIFO order
   - `test_worker_peek_lock_semantics` - Dequeue doesn't remove item until ack
   - `test_worker_ack_atomicity` - Ack_worker atomically removes item and enqueues completion
   - `test_timer_delayed_visibility` - TimerFired items only dequeued when visible
   - `test_lost_lock_token_handling` - Locked items become available after expiration
   - `test_worker_delayed_visibility_skips_future_items` - Future-visible worker items skipped
   - `test_worker_item_immediate_visibility` - Worker items immediately visible by default
   - `test_orphan_queue_messages_dropped` - QueueMessage for non-existent instance is dropped; QueueMessage for existing instance is kept

7. **Instance Creation Tests (4 tests)**
   - `test_instance_creation_via_metadata` - Instances created via ack metadata, not on enqueue
   - `test_no_instance_creation_on_enqueue` - No instance created when enqueueing work items
   - `test_null_version_handling` - NULL version handled correctly
   - `test_sub_orchestration_instance_creation` - Sub-orchestrations follow same pattern

8. **Management Capability Tests (7 tests)**
   - `test_list_instances` - Instance listing returns all instance IDs
   - `test_list_instances_by_status` - Instance filtering by status works correctly
   - `test_list_executions` - Execution queries return all execution IDs
   - `test_get_instance_info` - Instance metadata retrieval
   - `test_get_execution_info` - Execution metadata retrieval
   - `test_get_system_metrics` - System metrics are accurate
   - `test_get_queue_depths` - Queue depth reporting is correct

9. **Long Polling Tests (5 tests)**
   - `test_short_poll_returns_immediately` - Short-poll providers return immediately when queue is empty
   - `test_short_poll_work_item_returns_immediately` - Worker queue short-poll returns immediately
   - `test_fetch_respects_timeout_upper_bound` - Fetch returns within poll_timeout even if blocking
   - `test_long_poll_waits_for_timeout` - Long-poll orchestration fetch waits for duration
   - `test_long_poll_work_item_waits_for_timeout` - Long-poll worker fetch waits for duration

10. **Poison Message Tests (9 tests)**
   - `orchestration_attempt_count_starts_at_one` - First fetch has attempt_count = 1
   - `orchestration_attempt_count_increments_on_refetch` - Attempt count increments on abandon/refetch
   - `worker_attempt_count_starts_at_one` - Worker items start with attempt_count = 1
   - `worker_attempt_count_increments_on_lock_expiry` - Attempt count increments when lock expires
   - `attempt_count_is_per_message` - Each message has independent attempt count
   - `abandon_work_item_ignore_attempt_decrements` - ignore_attempt=true decrements count
   - `abandon_orchestration_item_ignore_attempt_decrements` - ignore_attempt=true decrements count
   - `ignore_attempt_never_goes_negative` - Attempt count never goes below 0
   - `max_attempt_count_across_message_batch` - MAX attempt_count returned for batched messages

11. **Cancellation Support Tests (16 tests)**
    - `test_fetch_returns_running_state_for_active_orchestration` - Fetching activity for running orchestration proceeds normally
    - `test_fetch_returns_terminal_state_when_orchestration_completed` - Fetching activity for completed orchestration
    - `test_fetch_returns_terminal_state_when_orchestration_failed` - Fetching activity for failed orchestration
    - `test_fetch_returns_terminal_state_when_orchestration_continued_as_new` - Fetching activity for continued-as-new orchestration
    - `test_fetch_returns_missing_state_when_instance_deleted` - Fetching activity when instance deleted
    - `test_renew_returns_running_when_orchestration_active` - Lock renewal succeeds for active orchestration
    - `test_renew_returns_terminal_when_orchestration_completed` - Lock renewal for completed orchestration
    - `test_renew_returns_missing_when_instance_deleted` - Lock renewal when instance deleted
    - `test_ack_work_item_none_deletes_without_enqueue` - ack_work_item(None) deletes item without enqueueing completion
    - **Lock-Stealing Tests (6 tests):**
    - `test_cancelled_activities_deleted_from_worker_queue` - `cancelled_activities` in `ack_orchestration_item` deletes matching worker entries
    - `test_ack_work_item_fails_when_entry_deleted` - `ack_work_item` returns permanent error when entry was deleted (lock stolen)
    - `test_renew_fails_when_entry_deleted` - `renew_work_item_lock` fails when entry was deleted (lock stolen)
    - `test_cancelling_nonexistent_activities_is_idempotent` - Cancelling activities that don't exist is silently ignored
    - `test_batch_cancellation_deletes_multiple_activities` - Multiple activities can be cancelled in a single `ack_orchestration_item`
    - `test_same_activity_in_worker_items_and_cancelled_is_noop` - Activity in both `worker_items` and `cancelled_activities` results in no-op (INSERT then DELETE)
    - `test_orphan_activity_after_instance_force_deletion` - Force-deleting an instance while activities are in the worker queue is handled gracefully

12. **Deletion Tests (13 tests)** - `duroxide::provider_validations::deletion`
    - `test_delete_terminal_instances` - Delete completed/failed instances
    - `test_delete_running_rejected_force_succeeds` - Running instances rejected without force
    - `test_delete_nonexistent_instance` - Deleting non-existent instance is idempotent
    - `test_cascade_delete_hierarchy` - Deleting parent cascades to all descendants
    - `test_delete_cleans_queues_and_locks` - Deletion removes all queue entries and locks
    - `test_delete_instances_atomic` - Atomic batch deletion of multiple instances
    - `test_delete_instances_atomic_force` - Force delete multiple running instances
    - `test_delete_instances_atomic_orphan_detection` - Detect orphan children after deletion
    - `test_delete_get_instance_tree` - Build instance tree for cascade deletion
    - `test_delete_get_parent_id` - Get parent instance ID for sub-orchestrations
    - `test_list_children` - List direct children of an instance
    - `test_force_delete_prevents_ack_recreation` - Force delete prevents ack from recreating
    - `test_stale_activity_after_delete_recreate` - Stale activity completion after delete+recreate doesn't corrupt new instance

13. **Pruning Tests (4 tests)** - `duroxide::provider_validations::prune`
    - `test_prune_options_combinations` - Verify keep_last and completed_before work together
    - `test_prune_safety` - Current execution never pruned, including terminal instances
    - `test_prune_bulk` - Bulk prune across multiple instances
    - `test_prune_bulk_includes_running_instances` - Prune includes Running instances with CAN history (not just terminal)

14. **Bulk Deletion Tests (4 tests)** - `duroxide::provider_validations::bulk_deletion`
    - `test_delete_instance_bulk_completed_before_filter` - Filter by completion timestamp
    - `test_delete_instance_bulk_filter_combinations` - Combined filter options
    - `test_delete_instance_bulk_safety_and_limits` - Respect limit and safety constraints
    - `test_delete_instance_bulk_cascades_to_children` - Bulk delete cascades to sub-orchestrations

15. **Capability Filtering Tests (20 tests)** - `duroxide::provider_validations::capability_filtering`
    - `test_fetch_with_filter_none_returns_any_item` - Legacy behavior: filter=None returns any item
    - `test_fetch_with_compatible_filter_returns_item` - Compatible filter returns matching item
    - `test_fetch_with_incompatible_filter_skips_item` - Incompatible filter returns Ok(None)
    - `test_fetch_filter_skips_incompatible_selects_compatible` - Mixed versions: only compatible returned
    - `test_fetch_filter_does_not_lock_skipped_instances` - Skipped items not locked (fetchable by compatible runtime)
    - `test_fetch_filter_null_pinned_version_always_compatible` - NULL pinned version = always compatible (pre-migration data)
    - `test_fetch_filter_boundary_versions` - Boundary correctness at range edges (inclusive min/max)
    - `test_pinned_version_stored_via_ack_metadata` - Pinned version stored from ExecutionMetadata on ack
    - `test_pinned_version_immutable_across_ack_cycles` - Pinned version persists across ack cycles
    - `test_continue_as_new_execution_gets_own_pinned_version` - ContinueAsNew execution gets independent pinned version (also covers non-inheritance from previous execution)
    - `test_filter_with_empty_supported_versions_returns_nothing` - Empty filter = supports nothing → Ok(None)
    - `test_concurrent_filtered_fetch_no_double_lock` - Filtering doesn't break instance-lock exclusivity
    - `test_ack_stores_pinned_version_via_metadata_update` - Backfill path: ack writes pinned version on existing execution
    - `test_provider_updates_pinned_version_when_told` - Provider overwrites pinned version unconditionally (dumb storage)
    - `test_fetch_single_range_only_uses_first_range` - Phase 1 limitation: multi-range only uses first range
    - `test_fetch_corrupted_history_filtered_vs_unfiltered` - Filter excludes corrupted item = no error; unfiltered = `Ok(Some(...))` with `history_error` *(requires `corrupt_instance_history`)*
    - `test_fetch_deserialization_error_increments_attempt_count` - Attempt count increments across deserialization error cycles, returns `history_error` *(requires `corrupt_instance_history`, `get_max_attempt_count`)*
    - `test_fetch_deserialization_error_eventually_reaches_poison` - Corrupted history reaches max attempts via `history_error` + poison path *(requires `corrupt_instance_history`, `get_max_attempt_count`)*
    - `test_fetch_filter_applied_before_history_deserialization` - Filter applied before history loading (corrupted + excluded = Ok(None)) *(requires `corrupt_instance_history`)*
    - `test_ack_appends_event_to_corrupted_history` - Ack with new event succeeds despite corrupted history rows (append-only contract) *(requires `corrupt_instance_history`)*

16. **Session Routing Tests (33 tests)** - `duroxide::provider_validations::sessions`
    - Basic routing: `test_non_session_items_fetchable_by_any_worker`, `test_session_item_claimable_when_no_session`, `test_session_affinity_same_worker`, `test_session_affinity_blocks_other_worker`, `test_different_sessions_different_workers`, `test_mixed_session_and_non_session_items`
    - Lock/expiration: `test_session_claimable_after_lock_expiry`, `test_none_session_skips_session_items`, `test_some_session_returns_all_items`, `test_session_lock_expires_new_owner_gets_redelivery`, `test_session_lock_expires_same_worker_reacquires`
    - Renewal: `test_renew_session_lock_active`, `test_renew_session_lock_skips_idle`, `test_renew_session_lock_no_sessions`
    - Cleanup: `test_cleanup_removes_expired_no_items`, `test_cleanup_keeps_sessions_with_pending_items`, `test_cleanup_keeps_active_sessions`
    - Piggybacking: `test_ack_updates_session_last_activity`, `test_renew_work_item_updates_session_last_activity`
    - Edge cases: `test_session_items_processed_in_order`, `test_non_session_items_returned_with_session_config`
    - Process-level identity: `test_shared_worker_id_any_caller_can_fetch_owned_session`
    - Race conditions: `test_concurrent_session_claim_only_one_wins`, `test_session_takeover_after_lock_expiry`, `test_cleanup_then_new_item_recreates_session`, `test_abandoned_session_item_retryable`, `test_abandoned_session_item_ignore_attempt`
    - Cross-concern locks: `test_renew_session_lock_after_expiry_returns_zero`, `test_original_worker_reclaims_expired_session`, `test_activity_lock_expires_session_lock_valid_same_worker_refetches`, `test_both_locks_expire_different_worker_claims`, `test_session_lock_expires_activity_lock_valid_ack_succeeds`, `test_session_lock_renewal_extends_past_original_timeout`

17. **Tag Filtering Tests (10 tests)** - `duroxide::provider_validations::tag_filtering`
    - `test_default_only_fetches_untagged` - DefaultOnly filter returns only untagged items
    - `test_tags_fetches_only_matching` - Tags filter returns only matching tagged items
    - `test_default_and_fetches_untagged_and_matching` - DefaultAnd filter returns untagged + matching
    - `test_none_filter_returns_nothing` - None filter never returns items
    - `test_any_filter_fetches_everything` - Any filter returns all items regardless of tag
    - `test_multi_tag_filter` - Multiple tags in filter work correctly
    - `test_tag_round_trip_preservation` - Tag value preserved through enqueue → fetch → dequeue
    - `test_tag_survives_abandon_and_refetch` - Tag preserved after lock → abandon → refetch cycle
    - `test_multi_runtime_tag_isolation` - Concurrent runtimes with different TagFilters (mutual exclusion, partial overlap, full overlap)
    - `test_tag_preserved_through_ack_orchestration_item` - Tags on worker items survive the ack_orchestration_item path (enqueue via orchestrator ack → fetch with tag filter)

### Running Individual Test Functions

Each validation test should be run individually. This provides:

- **Better isolation**: Failures are clearly attributed to specific behaviors
- **Clearer reporting**: Test output shows exactly which test failed
- **Parallel execution**: CI/CD can run tests in parallel
- **Focused debugging**: Fix one behavior at a time without running unrelated tests
- **Easier debugging**: When a test fails, you know exactly which behavior is broken

```rust
use duroxide::provider_validations::{
    ProviderFactory,
    test_atomicity_failure_rollback,
    test_exclusive_instance_lock,
    test_worker_queue_fifo_ordering,
};

#[tokio::test]
async fn test_my_provider_atomicity_failure_rollback() {
    let factory = MyProviderFactory;
    test_atomicity_failure_rollback(&factory).await;
}

#[tokio::test]
async fn test_my_provider_exclusive_instance_lock() {
    let factory = MyProviderFactory;
    test_exclusive_instance_lock(&factory).await;
}

#[tokio::test]
async fn test_my_provider_worker_queue_fifo_ordering() {
    let factory = MyProviderFactory;
    test_worker_queue_fifo_ordering(&factory).await;
}
```

**Available test functions:** See `duroxide::provider_validations` module documentation for the complete list of all test functions, or refer to `tests/sqlite_provider_validations.rs` for a complete example using all tests.

### Creating a Test Provider Factory

Your factory should create fresh, isolated provider instances for each test. **Importantly, you must implement `lock_timeout()` to return the lock timeout used in validation tests** - this ensures validation tests wait for the correct duration when testing lock expiration behavior. Note that in production, lock timeouts are configured via `RuntimeOptions` (`orchestrator_lock_timeout` and `worker_lock_timeout`), not provider options.

```rust
use duroxide::providers::Provider;
use duroxide::provider_validations::ProviderFactory;
use std::sync::Arc;
use std::time::Duration;

const TEST_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

struct MyProviderFactory {
    // Keep temp directory alive
    _temp_dir: TempDir,
}

#[async_trait::async_trait]
impl ProviderFactory for MyProviderFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        // Create a new provider instance with unique path
        // Configure the provider with the same lock timeout
        let db_path = self._temp_dir.path().join(format!("test_{}.db", 
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::File::create(&db_path).unwrap();
        
        let options = MyProviderOptions {
            lock_timeout: TEST_LOCK_TIMEOUT,
        };
        Arc::new(MyProvider::new(&format!("sqlite:{}", db_path.display()), Some(options)).await?)
    }

    fn lock_timeout(&self) -> Duration {
        // CRITICAL: This must match the lock_timeout configured in create_provider()
        // Validation tests use this value to determine sleep durations when waiting
        // for lock expiration. If this doesn't match your provider's timeout,
        // tests will fail with timing issues.
        TEST_LOCK_TIMEOUT
    }
}
```

**Important:** The `lock_timeout()` value should match the timeout you pass to `fetch_orchestration_item()` and `fetch_work_item()` in your tests. In production, lock timeouts are configured via `RuntimeOptions` (`orchestrator_lock_timeout` for orchestrations, `worker_lock_timeout` for activities) and passed to these methods by the runtime dispatchers.

### Integration with CI/CD

Add validation tests to your CI pipeline:

```yaml
# .github/workflows/provider-tests.yml
name: Provider Validation Tests

on: [pull_request]

jobs:
  validation-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      
      - name: Run validation tests
        run: |
          cargo test --features provider-test
```

The tests will run individually, providing granular failure reporting. You can also run specific tests:

```bash
# Run a specific test
cargo test --features provider-test test_my_provider_atomicity_failure_rollback

# Run all atomicity tests
cargo test --features provider-test test_my_provider_atomicity

# Run tests in parallel
cargo test --features provider-test -- --test-threads=4
```

---

## Comparing Providers

Compare multiple providers or configurations side-by-side:

```rust
use duroxide::provider_stress_tests::{
    parallel_orchestrations::run_parallel_orchestrations_test_with_config,
    print_comparison_table, StressTestConfig
};

#[tokio::test]
async fn compare_providers() {
    let mut results = Vec::new();
    
    // Test different concurrency settings
    for (orch, worker) in [(1, 1), (2, 2)] {
        let config = StressTestConfig {
            orch_concurrency: orch,
            worker_concurrency: worker,
            duration_secs: 5,
            ..Default::default()
        };
        
        let result = run_parallel_orchestrations_test_with_config(
            &MyProviderFactory, 
            config
        ).await.unwrap();
        
        results.push((
            "MyProvider".to_string(),
            format!("{}/{}", orch, worker),
            result,
        ));
    }
    
    // Print comparison table
    print_comparison_table(&results);
}
```

Output:
```
Provider             Config     Completed  Failed     Infra    Config   App      Success %  Orch/sec        Activity/sec    Avg Latency    
------------------------------------------------------------------------------------------------------------------------------------------------------
MyProvider           1/1        57         0          0        0        0        100.00     11.40           57.00           87.72          ms
MyProvider           2/2        81         0          0        0        0        100.00     16.20           81.00           61.73          ms
```

---

## Troubleshooting

### Test Fails with Success Rate < 100%

**Possible Causes:**
1. **Non-atomic commits**: Ensure `ack_orchestration_item` uses a single transaction
2. **Lock expiration**: Provider may be releasing locks prematurely
3. **Queue semantics**: Messages lost or duplicated
4. **Database errors**: Deadlocks, connection failures, or constraint violations

**Debug Steps:**
- Enable verbose logging: `RUST_LOG=debug cargo run --release`
- Check provider logs for errors
- Verify transaction boundaries
- Test with lower concurrency first

### Zero Throughput

**Possible Causes:**
1. **Work not committed**: Provider not writing to queues correctly
2. **Lock not released**: Messages stay locked indefinitely
3. **Queue not polling**: `fetch_orchestration_item` not returning work

**Debug Steps:**
- Manually inspect database/queue contents
- Verify `fetch_orchestration_item` returns items
- Check `ack_orchestration_item` completes successfully
- Test with a simple unit test first

### High Latency Variability

**Possible Causes:**
1. **Lock contention**: Too many workers competing for same locks
2. **Database bottleneck**: Insufficient connection pool or slow queries
3. **Missing indexes**: Full table scans on large tables

**Debug Steps:**
- Reduce concurrency to isolate contention
- Profile database queries
- Check for missing indexes
- Verify connection pool size

### Out of Memory

**Possible Causes:**
1. **History not cleaned**: Old executions accumulate in history table
2. **Connection leak**: Connections not properly pooled or closed
3. **Large payloads**: Work items contain excessive data

**Debug Steps:**
- Monitor memory usage during test
- Check history table size
- Verify connection pool limits
- Inspect work item sizes

---

## Running Stress Tests with the Script

Duroxide provides a shell script to run stress tests with resource monitoring.

**Script**: [`run-stress-tests.sh`](https://github.com/microsoft/duroxide/blob/main/run-stress-tests.sh)

### Usage

```bash
./run-stress-tests.sh                    # Run all tests for 10s (default)
./run-stress-tests.sh 60                 # Run all tests for 60 seconds
./run-stress-tests.sh --parallel-only    # Run only parallel orchestrations
./run-stress-tests.sh --large-payload    # Run only large payload test
./run-stress-tests.sh --help             # Show all options
```

### Implementing for Custom Providers

1. Create a stress test binary similar to `sqlite-stress/src/bin/sqlite-stress.rs`
2. Implement `ProviderStressFactory` for your provider (works for both parallel orchestrations and large payload tests)
3. Run with the same configurations to compare performance

---

## Integration with CI/CD

Add stress tests to your CI pipeline:

```yaml
# .github/workflows/stress-tests.yml
name: Stress Tests

on:
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 0 * * 0'  # Weekly

jobs:
  stress-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable

      - name: Run stress tests
        run: |
          ./run-stress-tests.sh 10
```

---

## Performance Targets

Use these benchmarks to validate your provider:

### Minimum Requirements

- ✅ **Success Rate**: 100% under all configurations
- ✅ **Baseline Throughput**: ≥ 10 orch/sec (file-based provider, 1/1 config)
- ✅ **Latency**: Average < 200ms per orchestration
- ✅ **Scalability**: 2/2 config increases throughput by ≥ 30%

### High-Performance Targets

- **Throughput**: ≥ 50 orch/sec (file-based provider, 2/2 config)
- **Latency**: Average < 100ms per orchestration
- **Scalability**: Linear throughput increase with concurrency

---

## Advanced Usage

### Custom Test Runner

For complete control, build your own test runner:

```rust
use duroxide::{Runtime, RuntimeOptions, Client};
use duroxide::providers::Provider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MyCustomProvider::new().await?);
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder().build();
    
    let runtime = Runtime::start_with_options(
        provider.clone(),
        activities,
        orchestrations,
        RuntimeOptions {
            orchestration_concurrency: 2,
            worker_concurrency: 2,
            ..Default::default()
        },
    ).await;
    
    let client = Client::new(provider.clone()).await?;
    
    // Launch orchestrations
    for i in 0..100 {
        client.start_orchestration("TestOrch", format!("instance-{}", i), "input".to_string()).await?;
    }
    
    // Wait for completion
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    
    runtime.shutdown().await;
    
    Ok(())
}
```

### Result Tracking

To track results over time (like Duroxide's own tracking):

```bash
# Run with tracking enabled
./sqlite-stress/track-results.sh
```

This generates `stress-test-results.md` with:
- Commit hash and timestamp
- Changes since last test
- Performance metrics
- Historical trends

---

## Reference

- **Test Implementation**: `src/provider_validation/` (individual test modules)
- **Test API**: `src/provider_validations.rs` (test function exports)
- **Example Usage**: `tests/sqlite_provider_validations.rs` (complete example with all 176 tests)
- **Test Specification**: See individual test function documentation
- **Provider Guide**: `docs/provider-implementation-guide.md`
- **Built-in Providers**: `src/providers/sqlite.rs`

---

**With this guide, you can thoroughly test your custom Duroxide provider!** 🎉


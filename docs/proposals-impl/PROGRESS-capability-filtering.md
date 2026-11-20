# Provider Capability Filtering — Historical Progress

> **Historical record (2026-02-09).** This file records past implementation work.
> Test totals, implementation details, and deferred items describe that snapshot.
> For current contracts, use the [Provider Implementation Guide](../provider-implementation-guide.md)
> and [Provider Testing Guide](../provider-testing-guide.md).

**Spec:** [proposals-impl/provider-capability-filtering.md](provider-capability-filtering.md)  
**Recorded status:** Phase 1 complete

**Snapshot date:** 2026-02-09

---

## Summary

The capability filtering feature allows the orchestration dispatcher to pass a version filter to the provider so it only returns executions whose pinned `duroxide_version` falls within the runtime's supported replay-engine range. This enables safe rolling upgrades in mixed-version clusters without fetch-lock-abandon cycles.

The snapshot passed 733 tests (`cargo nextest run --all-features`).

---

## Implementation Details

### Core types and API (`src/providers/mod.rs`)

- `SemverVersion` — parsed semver (major, minor, patch) with `parse()`, `current_build()`, `Display`
- `SemverRange` — inclusive [min, max] with `contains()`, `default_for_current_build()`
- `DispatcherCapabilityFilter` — filter struct with `supported_duroxide_versions: Vec<SemverRange>`
- `ExecutionMetadata.pinned_duroxide_version: Option<SemverVersion>` — stored by provider unconditionally
- `Provider::fetch_orchestration_item()` — `filter: Option<&DispatcherCapabilityFilter>` parameter added
- Re-exported from `lib.rs`: `DispatcherCapabilityFilter`, `SemverRange`, `SemverVersion`

### SQLite provider (`src/providers/sqlite.rs`)

- Schema: `duroxide_version_major/minor/patch` columns on `executions` table
- `fetch_orchestration_item()`: SQL-level capability filtering via packed integer comparison
- Filter applied BEFORE lock acquisition and history deserialization (provider contract requirement 1)
- `NULL` pinned version treated as always compatible (backward compat with pre-migration data)
- `ack_orchestration_item()`: stores pinned version unconditionally when `Some(v)` provided
- Empty `supported_duroxide_versions` vec returns `Ok(None)` (not an error)
- History deserialization failures return `history_error` field on fetched item (not silent drop)
- Transaction committed (lock + attempt_count incremented) before returning deserialization error, ensuring poison path works
- Phase 1 limitation: only first range in `supported_duroxide_versions` is used

### Migration (`migrations/20240106000000_add_pinned_version.sql`)

- `ALTER TABLE executions ADD COLUMN duroxide_version_major/minor/patch INTEGER`
- In-memory schema creation includes the columns

### Instrumented provider (`src/providers/instrumented.rs`)

- `fetch_orchestration_item()` passes filter through to inner provider

### Runtime — orchestration dispatcher (`src/runtime/dispatchers/orchestration.rs`)

- Builds `DispatcherCapabilityFilter` once at startup from `RuntimeOptions`
- Passes filter to every `fetch_orchestration_item()` call
- Runtime-side compatibility check (defense-in-depth): validates pinned version from `OrchestrationStarted` event after fetch
- Incompatible items abandoned with 1-second delay (prevents tight spin loops)
- `debug_assert` enforcing write-once semantics for `pinned_duroxide_version` in `ack_orchestration_with_changes()`
- Pinned version extracted from `OrchestrationStarted.duroxide_version` field during `compute_execution_metadata()`
- Info log at startup declaring supported version range
- Warning log on incompatible-version abandon

### RuntimeOptions (`src/runtime/mod.rs`)

- `supported_replay_versions: Option<SemverRange>` — override the default range
- Default range: `>=0.0.0, <=CURRENT_BUILD_VERSION`
- `disable_version_filtering` removed (drain mode uses wide `supported_replay_versions` instead)

### ProviderFactory test trait (`src/provider_validation/mod.rs`)

Two optional methods added to `ProviderFactory` for deserialization contract tests:

- `corrupt_instance_history(instance)` — inject undeserializable event data for a given instance. Default panics; providers that support the Category I tests must override.
- `get_max_attempt_count(instance)` — return the max `attempt_count` from the orchestrator queue for a given instance. Default panics; providers that support the Category I tests must override.

These keep the deserialization contract tests fully provider-agnostic — no `sqlx::` or `SqliteProvider` references in `src/provider_validation/`.

SQLite implementation uses `SharedSqliteTestFactory` (in `tests/sqlite_provider_validations.rs`) that holds a single `Arc<SqliteProvider>` so `create_provider()` and the helper methods operate on the same database.

### Test infrastructure updates

- All provider validation tests updated to pass `None` for the filter parameter
- All e2e/integration tests updated: `cancellation_tests.rs`, `long_poll_tests.rs`, `observability_tests.rs`, `poison_message_tests.rs`, `provider_atomic_tests.rs`, `sqlite_tests.rs`
- `common/fault_injection.rs` — `PoisonInjectingProvider`, `FailingProvider`, and `FilterBypassProvider` updated
- `common/mod.rs` — `test_create_execution()` and `seed_instance_with_pinned_version()` helpers

---

## Test Coverage

### Provider validation tests — 20 tests (`src/provider_validation/capability_filtering.rs`)

Wired via `tests/sqlite_provider_validations.rs`. All tests are generic over `ProviderFactory`.

| # | Test | Category | Description |
|---|------|----------|-------------|
| 1 | `test_fetch_with_filter_none_returns_any_item` | A | Legacy behavior: `filter=None` returns any item |
| 2 | `test_fetch_with_compatible_filter_returns_item` | A | Compatible filter returns matching item |
| 3 | `test_fetch_with_incompatible_filter_skips_item` | A | Incompatible filter returns `Ok(None)` |
| 4 | `test_fetch_filter_skips_incompatible_selects_compatible` | A | Two instances, filter selects only matching one |
| 5 | `test_fetch_filter_does_not_lock_skipped_instances` | A | Skipped instance not locked (available for next fetch) |
| 6 | `test_fetch_filter_null_pinned_version_always_compatible` | A | NULL pinned version = always compatible |
| 7 | `test_fetch_filter_boundary_versions` | A | Range boundary correctness (inclusive min/max) |
| 8 | `test_pinned_version_stored_via_ack_metadata` | A | Provider stores/reads pinned version from metadata |
| 9 | `test_pinned_version_immutable_across_ack_cycles` | A | Pinned version persists across ack cycles within execution |
| 10 | `test_continue_as_new_execution_gets_own_pinned_version` | B | ContinueAsNew execution gets its own pinned version |
| 22 | `test_filter_with_empty_supported_versions_returns_nothing` | F | Empty filter = supports nothing → `Ok(None)` |
| 23 | `test_concurrent_filtered_fetch_no_double_lock` | F | Filtering doesn't break instance-lock exclusivity |
| 39 | `test_fetch_corrupted_history_filtered_vs_unfiltered` | I | Filter excludes corrupted = no error; unfiltered = `history_error` |
| 41 | `test_fetch_deserialization_error_increments_attempt_count` | I | Attempt count increments across deserialization error cycles |
| 42 | `test_fetch_deserialization_error_eventually_reaches_poison` | I | Corrupted history → attempt_count reaches max via `history_error` |
| 43 | `test_fetch_filter_applied_before_history_deserialization` | F2 | Filter applied BEFORE deserialization (corrupted + excluded = `Ok(None)`) |
| 44 | `test_fetch_single_range_only_uses_first_range` | F2 | Phase 1 limitation: multi-range only uses first range |
| 45 | `test_ack_stores_pinned_version_via_metadata_update` | F2 | Backfill path: ack writes pinned version on existing execution |
| 46 | `test_provider_updates_pinned_version_when_told` | F2 | Provider overwrites pinned version unconditionally (dumb storage) |
| — | `test_ack_appends_event_to_corrupted_history` | I | Ack appends event to corrupted history (append-only contract) |

### End-to-end scenario tests — 18 tests (`tests/capability_filtering_tests.rs`)

Real `Runtime` + `Client` + SQLite provider. Each test creates a disk-backed SQLite store for isolation.

| # | Test | Category | Description |
|---|------|----------|-------------|
| 12 | `runtime_abandons_incompatible_execution` | C | Incompatible instance stays Running (invisible to filtered runtime) |
| 13 | `runtime_processes_compatible_execution_normally` | C | Compatible orchestration completes successfully |
| 14 | `runtime_abandon_reaches_max_attempts_and_poisons` | C | Incompatible instance stays Running (provider-level filtering) |
| 15 | `runtime_abandon_uses_short_delay` | C | Abandon delay prevents immediate reprocessing |
| 16 | `two_runtimes_different_version_ranges_route_correctly` | D | Runtime A (v1.x) and Runtime B (v2.x) route correctly |
| 17 | `overlapping_version_ranges_both_can_process` | D | Overlapping ranges: v2.5.0 item completes on either runtime |
| 18 | `mixed_cluster_compatible_and_incompatible_items` | D | 5 instances across 2 runtimes, all complete correctly |
| 19 | `execution_metadata_includes_pinned_version_on_new_orchestration` | E | Pinned version matches current build after completion |
| 20 | `existing_executions_without_pinned_version_remain_fetchable` | E | NULL pinned version = always compatible |
| 21 | `pinned_version_extracted_from_orchestration_started_event` | E | Seeded v3.1.4 pinned version correctly extracted e2e |
| 24 | `sub_orchestration_gets_own_pinned_version` | F | Sub-orchestration pinned at current build version |
| 25 | `activity_completion_after_continue_as_new_is_discarded` | F | Stale activity completion from old execution ignored after CAN |
| 26 | `default_supported_range_includes_current_and_older_versions` | G | Current-version orchestration completes with default range |
| 27 | `default_supported_range_excludes_future_versions` | G | v99.0.0 stays invisible to runtime |
| 28 | `custom_supported_replay_versions_narrows_range` | G | Custom range excludes out-of-range items |
| 29 | `wide_supported_range_drains_stuck_items_via_deserialization_error` | G | Drain procedure: wide range + corrupted history → poison path |
| 30 | `runtime_logs_warning_on_incompatible_abandon` | H | Warning log on defense-in-depth abandon (uses FilterBypassProvider) |
| 31 | `runtime_logs_capability_declaration_at_startup` | H | Info log at startup listing supported version ranges |

---

## Production Code Fixes Made During Implementation

### SQLite provider: history deserialization error handling

Fixed `fetch_orchestration_item` to commit the transaction (lock acquisition + attempt_count increment) before returning when history deserialization fails. Previously, the transaction was rolled back on deserialization failure, causing:
- `attempt_count` to never increment for corrupted items
- Corrupted orchestrations to loop infinitely without reaching the poison threshold

The fix ensures `attempt_count` monotonically increases even when deserialization fails, allowing the existing max-attempts poison machinery to terminate stuck items.

### SQLite provider: `history_error` field instead of `ProviderError`

Rather than returning `Err(ProviderError::permanent(...))` on deserialization failure, the provider now returns `Ok(Some(item))` with `item.history_error = Some(error_message)`. This design:
- Preserves the lock and attempt_count atomically (transaction committed)
- Lets the runtime decide what to do (currently: fail as poison if attempt_count > max_attempts)
- Avoids the ambiguity of whether callers should retry permanent errors

---

## Design Decisions

### Provider-level filtering vs runtime-side abandon

The spec originally assumed incompatible items would be fetched by the runtime and then abandoned (defense-in-depth path). In practice, with correct provider-level SQL filtering, incompatible items are never returned to the runtime at all. Tests #12, #14, #27, #28 were adapted to verify items remain *invisible* (still Running) rather than *abandoned/poisoned*. The defense-in-depth path in `orchestration.rs` only fires on provider bugs, verified by test #30 using `FilterBypassProvider`.

### `ProviderFactory` test helper methods

Deserialization contract tests (Category I) need to inject corrupted history data, which is inherently storage-specific. Rather than having tests reference `SqliteProvider` directly (breaking provider-agnosticism), two optional methods were added to the `ProviderFactory` trait:
- `corrupt_instance_history(instance)` — inject undeserializable event data
- `get_max_attempt_count(instance)` — query internal queue state for verification

Default implementations panic (not all providers need these tests). SQLite implements them via `SharedSqliteTestFactory` which holds a single provider instance shared between `create_provider()` and the helpers, ensuring they operate on the same database.

### Spec test #11 merged into #10

Spec test #11 (`continue_as_new_does_not_inherit_previous_pinned_version`) is semantically identical to test #10 (`continue_as_new_execution_gets_own_pinned_version`). Both verify that ContinueAsNew creates an execution with its own pinned version from the new metadata, not inherited from the previous execution. Implemented as a single test.

### Spec test #21 adapted to e2e

Spec test #21 specified a unit test for `compute_execution_metadata()`, but that function is private. Implemented as an end-to-end test instead: seed an instance with pinned v3.1.4, let the runtime process it (with a wide-enough supported range), and verify the pinned version is preserved in history.

### Duplicate scenario test file removed

`tests/scenarios/capability_filtering.rs` was a duplicate of `tests/capability_filtering_tests.rs` (identical tests plus one extra: `runtime_abandon_uses_short_delay`). The extra test was moved to `tests/capability_filtering_tests.rs` and the scenarios file was deleted.

---

## Documentation Updates

- `docs/provider-implementation-guide.md` — capability filtering contract, deserialization contract requirements, `ProviderFactory` helper methods, validation checklist items
- `docs/provider-testing-guide.md` — updated test list (removed "SQLite-specific" tags), `ProviderFactory` example with `corrupt_instance_history` and `get_max_attempt_count`
- `docs/migration-guide.md` — "Draining Stuck Orchestrations After Upgrade" section
- `docs/versioning-best-practices.md` — "Draining Stuck Orchestrations (Version Mismatch)" section

---

## Test artifacts created

- `FilterBypassProvider` in `tests/common/fault_injection.rs` — provider wrapper that ignores capability filter on `fetch_orchestration_item`, enabling defense-in-depth path testing (used by test #30)
- `SharedSqliteTestFactory` in `tests/sqlite_provider_validations.rs` — factory backed by a single shared `SqliteProvider` for tests that need `corrupt_instance_history()` / `get_max_attempt_count()`

---

## Work Deferred at the Snapshot

The following items from the spec were deferred at that time.
This list is not a current roadmap:

- **Multi-range filter support** — phase 1 only uses the first range in `supported_duroxide_versions`
- **Orchestration handler capability filtering** — filter by registered orchestration name/version
- **Activity worker capability filtering** — worker queue filters by registered activity handlers
- **Provider observability counters** — `duroxide.orchestration.incompatible_version_abandoned` etc.
- **`disable_version_filtering` RuntimeOptions flag** — wide-range drain procedure covers the use case
- **Sweeper maintenance loop** — background loop to terminate permanently-stuck items

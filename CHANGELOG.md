# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.24] - 2026-03-12

**Release:** <https://crates.io/crates/duroxide/0.1.24>

**Proposal:** [Orchestration KV Store](https://github.com/microsoft/duroxide/blob/main/docs/proposals/orchestration-kv-store.md)

### Added

- **Durable KV store for per-instance state** — Store key-value pairs scoped to orchestration
  instances via `ctx.set_value(key, value)` / `ctx.get_value(key)`. Values survive replay,
  `continue_as_new`, and are readable by external clients. Cross-instance reads via
  `ctx.get_value_from_instance(instance, key)`.
  - `ctx.set_value()`, `ctx.get_value()`, `ctx.clear_value()`, `ctx.clear_all_values()`
  - Typed variants: `ctx.set_value_typed()`, `ctx.get_value_typed()`
  - Cross-instance: `ctx.get_value_from_instance()`, `ctx.get_value_from_instance_typed()`
  - Client API: `client.get_value()`, `client.get_value_typed()`, `client.wait_for_value()`,
    `client.wait_for_value_typed()`
  - Provider trait: `get_kv_value()` for materialized KV reads
  - SQLite migration `20240110000000_add_kv_store.sql` adding `kv_store` table
  - KV materialization in `ack_orchestration_item` (KeyValueSet, KeyValueCleared, KeyValuesCleared)
  - KV snapshot loading in `fetch_orchestration_item`
  - Replay engine: KV action matching with nondeterminism detection
  - Limits: `MAX_KV_KEYS=10`, `MAX_KV_VALUE_BYTES=16KB`
  - Execution ID tracking (last-writer-wins) for pruning safety
  - Instance deletion cascades to KV cleanup

- **26 provider validation tests** for KV store (`src/provider_validation/kv_store.rs`)

- **15 replay engine tests** for KV action matching and nondeterminism detection

- **45 E2E tests** for KV store including single-thread, stress, cross-instance,
  sub-orchestration isolation, and request/response patterns

- **2 serde backward-compatibility tests** for KV event kinds

### Documentation

- Updated ORCHESTRATION-GUIDE.md with KV Store API reference and Client KV operations
- Updated provider-implementation-guide.md with `kv_store` table schema, `get_kv_value()`,
  KV materialization in ack, and validation checklist
- Updated provider-testing-guide.md with KV test category (176 → 202 total tests)
- Updated README.md with KV store feature

## [0.1.23] - 2026-03-07

**Release:** <https://crates.io/crates/duroxide/0.1.23>

### Added

- **Provider validation test for tag preservation through ack** — New test
  `test_tag_preserved_through_ack_orchestration_item` verifies that tags on worker items
  survive the `ack_orchestration_item` path (orchestrator ack → worker queue fetch with
  tag filter). Catches providers that drop the tag column during worker item insertion.

### Fixed

- **Flaky `sample_config_hot_reload_persistent_events_fs` test** — Replaced fixed
  `tokio::time::sleep(300ms)` delay with `wait_for_history` polling that waits for the
  `cycle_0` activity to be scheduled before sending the mid-flight event. Increased drain
  timeouts (50ms → 100ms) and cycle timer (100ms → 1s) to provide reliable timing margins.

### Documentation

- Updated provider-testing-guide: tag filtering tests 9 → 10, total test count updated to 176

## [0.1.22] - 2026-03-07

**Release:** <https://crates.io/crates/duroxide/0.1.22>

**Proposal:** [Activity Tags](https://github.com/microsoft/duroxide/blob/main/docs/proposals/activity-tags.md)

### Added

- **Activity tag routing for worker specialization** — Route activities to specialized
  worker pools via `.with_tag("gpu")` on `DurableFuture`. Workers subscribe to tags via
  `RuntimeOptions { worker_tag_filter: TagFilter::tags(["gpu"]) }`. Supports five filter
  modes: `DefaultOnly`, `Tags`, `DefaultAnd`, `Any`, and `None` (orchestrator-only).
  Tags compose with sessions, retry, join, select2, and cancellation.
  - `TagFilter` type with `matches()`, constructor validation (1–5 tags, no empty sets)
  - `MAX_WORKER_TAGS` (5) and `MAX_TAG_NAME_BYTES` (256) limits enforced at runtime
  - `ActivityContext::tag()` accessor for activity handlers to inspect their routing tag
  - SQLite migration adding `tag` column + index to `worker_queue`
  - Provider trait `fetch_work_item()` extended with `tag_filter` parameter
  - Tag included in replay determinism checks (tag mismatch → nondeterminism error)
  - Tag propagated through `Action::CallActivity` → `EventKind::ActivityScheduled` → `WorkItem::ActivityExecute`
  - `activity_tag` label added to activity metrics (`duroxide_activity_executions_total`,
    `duroxide_activity_duration_seconds`)
  - `activity_tag` span attribute on all worker tracing

- **9 provider validation tests** for tag filtering (`src/provider_validation/tag_filtering.rs`)

- **11 tag serde + e2e tests** in `tests/tag_serde_tests.rs`
  (serde roundtrip, backward compat, routing, starvation timeout, dual-runtime cooperation,
  oversized tag rejection, boundary tag, tag in `ActivityContext`, multi-worker separation)

- **5 replay engine tests** for tag determinism (tag mismatch, tag change, tag removal → nondeterminism)

- **3 e2e sample tests** in `tests/e2e_samples.rs`
  (heterogeneous workers, starvation-safe timeout pattern, dual-runtime tag cooperation)

- **Orphan event dropping test** (`events_enqueued_before_start_orchestration_are_dropped`)
  in `tests/queue_event_tests.rs`

### Fixed

- **Flaky queue event tests** — `multi_queue_staggered_delivery` and
  `multi_queue_isolation_and_independent_fifo` fixed by moving `start_orchestration` before
  `enqueue_event` (events enqueued before orchestration start are dropped as orphans per 0.1.21).
  `persistent_event_survives_select_cancellation` split into two deterministic tests:
  same-batch (dispatcher stopped during enqueue) and late-extra-discarded (event after terminal).

### Changed

- `Provider::fetch_work_item()` signature now requires `tag_filter: &TagFilter` parameter
  (**breaking for provider implementors**)
- `WorkItem::ActivityExecute` gains `tag: Option<String>` field
- `EventKind::ActivityScheduled` gains `tag: Option<String>` field (backward-compatible via `#[serde(default)]`)
- `ActivityContext::new_with_cancellation()` gains `tag: Option<String>` parameter

### Documentation

- Updated ORCHESTRATION-GUIDE with activity tags section (routing, `.with_tag()`, `TagFilter` variants, starvation warning, replay determinism)
- Updated provider-implementation-guide with `fetch_work_item` tag filtering, validation checklist
- Updated provider-testing-guide with tag filtering test category (18th category, 166 tests)
- Updated metrics-specification with `activity_tag` label on activity metrics
- Updated activity-tags proposal to reflect implementation (`TagFilter::Any`, `worker_tag_filter` naming, empty-set rejection)
- Added flaky test investigation policy to `.github/copilot-instructions.md`

## [0.1.21] - 2026-03-06

**Release:** <https://crates.io/crates/duroxide/0.1.21>

### Fixed

- **Orphan queue message handling** — `QueueMessage` items enqueued before an orchestration
  starts are now dropped (deleted) with a warning instead of being left in the queue (which
  caused a busy-loop in the SQLite provider) or silently lost (in PG providers). Non-QueueMessage
  work items (e.g., `CancelInstance`) that race with `StartOrchestration` are correctly kept
  in the queue for retry.

### Added

- **`test_orphan_queue_messages_dropped`** — New provider validation test verifying that
  QueueMessage items for non-existent instances are dropped, while QueueMessage items for
  existing instances are kept and returned.

### Changed

- **`sample_config_hot_reload_persistent_events_fs`** e2e test adapted to start the
  orchestration before enqueuing events (matching the corrected orphan message semantics).

### Documentation

- Updated ORCHESTRATION-GUIDE.md, external-events.md, provider-implementation-guide.md,
  and provider-testing-guide.md to document pre-start event drop behavior.

## [0.1.20] - 2026-02-21

**Release:** <https://crates.io/crates/duroxide/0.1.20>

### Changed (Breaking for Provider Implementors)

- **Custom status as history events** — `set_custom_status()` and `reset_custom_status()` now emit
  `CustomStatusUpdated` history events instead of writing to `ExecutionMetadata.custom_status`.
  Custom status is now fully durable, replayable, and deterministic across turns.
  - Removed `CustomStatusUpdate` enum from providers
  - Removed `ExecutionMetadata.custom_status` field
  - Provider `ack_orchestration_item()` must now scan `history_delta` for the last `CustomStatusUpdated`
    event and apply it to the instances table (see provider-implementation-guide.md)
  - `initial_custom_status` field added to `OrchestrationStarted` event and `ContinueAsNew` work item
    for carry-forward across continue-as-new boundaries

### Added

- **`get_custom_status()` on OrchestrationContext** — Read the current custom status value,
  reflecting all `set_custom_status` / `reset_custom_status` calls across turns and CAN boundaries
- **`short_poll_threshold()` on ProviderFactory** — Configurable timing for short polling
  validation tests; remote-database providers can override with higher values (closes #51)
- **`test_orphan_activity_after_instance_force_deletion`** — Provider validation test verifying
  graceful handling of activities orphaned by instance force-deletion (closes #37)

### Fixed

- `test_cancelling_nonexistent_activities_is_idempotent` now uses `execution_id: 1` instead of `99`
  to correctly validate same-execution cancellation semantics (closes #40)
- Removed dead `ActivityContext::new` constructor (unused, runtime uses `new_with_cancellation`)
- Removed unused `clippy::clone_on_ref_ptr` suppression from observability.rs (closes #48)

## [0.1.19] - 2026-02-20

**Release:** <https://crates.io/crates/duroxide/0.1.19>

**Proposal:** [Custom Status Progress](https://github.com/microsoft/duroxide/blob/main/docs/proposals/custom-status-progress.md)
**Proposal:** [External Event Semantics](https://github.com/microsoft/duroxide/blob/main/docs/proposals/external-event-semantics.md)
**Proposal:** [Persistent Event Queuing](https://github.com/microsoft/duroxide/blob/main/docs/proposals/persistent-event-queuing.md)

### Added

- **Event Queue API** — Persistent FIFO event queues that survive `continue_as_new`
  - `ctx.dequeue_event(queue)` and `ctx.dequeue_event_typed::<T>(queue)` for orchestrations
  - `client.enqueue_event(instance, queue, data)` and `client.enqueue_event_typed::<T>()` for clients
  - FIFO ordering with buffering — messages can arrive before orchestration subscribes
  - Queue messages carry forward across continue-as-new boundaries

- **Custom Status** — Orchestration progress reporting visible to external clients
  - `ctx.set_custom_status(json)` publishes structured progress from orchestrations
  - `client.wait_for_status_change(instance, version, poll, timeout)` for efficient polling
  - Status persists across continue-as-new boundaries
  - New Provider trait methods: `set_custom_status()`, `get_custom_status()`
  - Schema migration `20240108000000_add_custom_status.sql`

- **Retry on Session** — Combine retry policies with session affinity (closes #56)
  - `ctx.schedule_activity_with_retry_on_session(name, input, policy, session_id)`
  - `ctx.schedule_activity_with_retry_on_session_typed::<In, Out>()`
  - All retry attempts pinned to the same worker session

- **Typed event helpers** — `client.raise_event_typed::<T>()` and `client.enqueue_event_typed::<T>()`

- **Provider validation** — `test_prune_bulk_includes_running_instances` catches providers that
  exclude Running instances from bulk prune (closes #50)

- **Scenario test** — Copilot Chat pattern: multi-turn chat using dequeue_event + set_custom_status + CAN

### Changed

- Renamed persistent event internals: `ExternalRaisedPersistent` → `QueueMessage`, `ExternalSubscribedPersistent` → `QueueSubscribed`

### Deprecated

- `client.raise_event_persistent()` — use `client.enqueue_event()` instead
- `ctx.schedule_wait_persistent()` — use `ctx.dequeue_event()` instead

## [0.1.18] - 2026-02-16

**Release:** <https://crates.io/crates/duroxide/0.1.18>

**Proposal:** [Activity Implicit Sessions v2](https://github.com/microsoft/duroxide/blob/main/docs/proposals-impl/activity-implicit-sessions-v2.md)

### Added

- **Activity Session Affinity** — Route activities to the same worker for in-memory state reuse
  - `ctx.schedule_activity_on_session(name, input, session_id)` pins activities by session ID
  - `ctx.schedule_activity_on_session_typed()` for serde-based typed inputs/outputs
  - `ActivityContext::session_id()` getter for process-local state lookup
  - Two-timeout model: `session_lock_timeout` (heartbeat lease) + `session_idle_timeout` (inactivity expiry)
  - Automatic session lifecycle: implicit creation, heartbeat renewal, idle unpin, crash recovery
  - `SessionTracker` enforces `max_sessions_per_runtime` across all worker slots via RAII guards
  - `worker_node_id` option for stable session identity across restarts (e.g., K8s StatefulSet pods)
  - Session manager background task for lock renewal and orphan cleanup

- **Provider API changes (required)**
  - `fetch_work_item()` gains `session: Option<&SessionFetchConfig>` parameter for session routing
  - New `renew_session_lock()` method — batched heartbeat for owned non-idle sessions
  - New `cleanup_orphaned_sessions()` method — sweep expired session rows with no pending work
  - `ack_work_item()` and `renew_work_item_lock()` piggyback `last_activity_at` updates (guarded by `locked_until`)

- **New `RuntimeOptions` fields**
  - `session_lock_timeout` (default 30s), `session_lock_renewal_buffer` (default 5s)
  - `session_idle_timeout` (default 5min), `session_cleanup_interval` (default 5min)
  - `max_sessions_per_runtime` (default 10), `worker_node_id` (default None)

- **33 provider validation tests** for session routing, locks, races, and cross-concern interactions
- **22 E2E tests** for single/multi-worker, fan-out, CAN, heterogeneous scenarios
- **Schema migration** `20240107000000_add_sessions.sql` — sessions table + worker_queue.session_id

### Changed

- `session_id: Option<String>` added to `Action::CallActivity`, `EventKind::ActivityScheduled`, and `WorkItem::ActivityExecute` (backward compatible via `serde(default)`)
- Existing `schedule_activity` calls are completely unaffected (`session_id = None`)
- Documentation updated across ORCHESTRATION-GUIDE, provider-implementation-guide, provider-testing-guide, README, and migration-guide

## [0.1.17] - 2026-02-09

**Release:** <https://crates.io/crates/duroxide/0.1.17>

**Proposal:** [Provider Capability Filtering](https://github.com/microsoft/duroxide/blob/main/docs/proposals-impl/provider-capability-filtering.md)

### Added

- **Provider Capability Filtering (Phase 1)** — Safe rolling upgrades in mixed-version clusters
  - Orchestration dispatcher passes a version filter to the provider so it only returns
    executions whose pinned `duroxide_version` falls within the runtime's supported range
  - SQL-level filtering applied before lock acquisition and history deserialization
  - NULL pinned version treated as always compatible (backward compat with pre-migration data)
  - `RuntimeOptions::supported_replay_versions` for custom version range configuration
  - Defense-in-depth: runtime-side compatibility check after fetch with 1-second abandon delay
  - Startup log declaring supported version range; warning log on incompatible-version abandon

- **New types:** `SemverVersion`, `SemverRange`, `DispatcherCapabilityFilter`, `current_build_version()`

- **Provider API change:** `fetch_orchestration_item()` gains `filter: Option<&DispatcherCapabilityFilter>` parameter

- **History deserialization contract** — Providers must surface deserialization errors (not silently drop events)
  - `history_error` field on fetched items for deserialization failures
  - Transaction commits lock + attempt_count before returning errors (enables poison path)

- **ProviderFactory test helpers** — `corrupt_instance_history()` and `get_max_attempt_count()`
  optional methods for provider-agnostic deserialization contract tests

- **38 new tests** — 20 provider validation + 18 e2e scenario tests covering filtering,
  rolling deployment routing, metadata/migration, ContinueAsNew isolation, drain procedures,
  and observability

### Changed

- **Migration:** `20240106000000_add_pinned_version.sql` adds `duroxide_version_major/minor/patch`
  columns to `executions` table
- Provider validation test total: 114 tests (up from 94)

## [0.1.16] - 2026-02-02

**Release:** <https://crates.io/crates/duroxide/0.1.16>

**Proposal:** [Persist Cancellation Decisions in History](https://github.com/microsoft/duroxide/blob/main/docs/proposals-impl/history-cancellation-events.md)

### Added

- **Cancellation History Events** - Record cancellation decisions as durable history breadcrumbs
  - New `ActivityCancelRequested` and `SubOrchestrationCancelRequested` event kinds
  - Dropped futures (select losers, terminal cleanup) now recorded in history
  - Enables observability: history answers "was this cancelled?"
  - Enables replay determinism: detect when cancellation decisions differ on replay
  - Idempotent: side-channel cancellations only emitted once per decision

- **Nondeterminism Tests for Completion Validation** - 12 new tests covering:
  - Completion kind mismatches (timer/activity/sub-orchestration cross-checks)
  - Duplicate completion detection for closed schedules
  - OrchestrationChained mismatch validation

### Fixed

- **Duplicate Completion Detection** - Fixed oversight where `open_schedules.remove()` was never called after delivering completions in replay engine
  - Previously, duplicate completions in history were silently accepted
  - Now properly triggers nondeterminism error on duplicate completions

### Changed

- **Housekeeping** - Moved implemented/rejected proposals to `docs/proposals-impl/`:
  - `metrics-facade-migration.md` (implemented)
  - `replay-simplification-PROGRESS.md` (completed)
  - `activity-cancellation-queue-flag.md` (rejected/superseded by lock-stealing)

---

## [0.1.15] - 2026-01-30

**Release:** <https://crates.io/crates/duroxide/0.1.15>

### Changed

- **Simplified Metrics Facade** - Internal observability uses consistent atomic counters with a cleaner facade pattern

### Added

- **Code Coverage Improvements** - Test coverage improved to 91.9% with better organization
  - New provider validation tests for error handling, management interface, and observability
  - Removed duplicate tests that overlapped with provider validation suite
- **Code Coverage Guide** - New `docs/code-coverage-guide.md` with llvm-cov setup instructions
- **Copilot Skill for Coverage** - AI assistant skill for code coverage workflows

### Fixed

- **README Markdown Formatting** - Fixed section heading syntax for better rendering

---

## [0.1.14] - 2026-01-24

**Release:** <https://crates.io/crates/duroxide/0.1.14>

### Fixed

- **Fire-and-Forget Orchestrations Now Record History Events** - `ctx.schedule_orchestration()` (detached/chained orchestrations) now correctly creates `OrchestrationChained` events in history
  - Previously, these fire-and-forget calls were not recorded, breaking determinism detection on replay
  - If an orchestration scheduled a detached orchestration followed by an activity, replay would fail with nondeterminism error
  - Added proper action-to-event conversion and event matching in replay engine

### Added

- **Action-to-Event Recording Tests** - New test module `tests/replay_engine/action_to_event.rs` verifying all scheduling actions create corresponding history events
- **E2E Test for Detached + Activity Pattern** - `sample_detached_then_activity_fs` validates the fix end-to-end

---

## [0.1.13] - 2026-01-24 [YANKED]

**Release:** <https://crates.io/crates/duroxide/0.1.13>

**Proposal:** [System Calls as Real Activities](https://github.com/microsoft/duroxide/blob/main/docs/proposals-impl/system-calls-as-activities.md)

### Changed

- **System Calls Reimplemented as Regular Activities** - `ctx.new_guid()` and `ctx.utc_now()` now use normal activity infrastructure
  - Simplifies replay engine by removing special-case SystemCall handling
  - Fixes determinism bugs where syscalls returned fresh values on replay
  - Reserved activity prefix `__duroxide_syscall:` prevents user collisions
  - Builtin activities injected automatically at runtime startup

- **API Rename: `utcnow()` → `utc_now()`** - Consistent with Rust naming conventions

### Added

- **Reserved Activity Prefix Validation** - `ActivityRegistry` rejects names starting with `__duroxide_syscall:`
- **Comprehensive Syscall Tests** - Replay determinism, ordering, single-thread mode, cancellation

### Removed

- `SystemCall` variants from `Action`, `EventKind`, `CompletionResult`
- SystemCall handling from replay engine (no more re-poll loop)
- `EVENT_TYPE_SYSTEM_CALL` from sqlite provider

### Documentation

- Updated ORCHESTRATION-GUIDE and durable-futures-internals for new syscall semantics
- Reorganized proposals: moved 11 implemented proposals to `docs/proposals-impl/`
- Updated merge prompt to require squash-only merges

### Breaking Changes

- `utcnow()` renamed to `utc_now()` - update all call sites
- Histories containing `SystemCall` events will not replay (pre-1.0, acceptable)

---

## [0.1.12] - 2026-01-23

**Release:** <https://crates.io/crates/duroxide/0.1.12>

### Added

- **Unobserved Future Cancellation** - Futures that are scheduled but never awaited are now properly cancelled
  - New `DurableFuture` implementation with proper drop semantics
  - Cancellation events recorded in history for deterministic replay
  - Comprehensive test coverage in `tests/replay_engine/unobserved_futures.rs`

- **AI Skills System** - New `docs/skills/` folder for AI coding assistant context
  - Installation instructions for VS Code Copilot, Claude Code, and Cursor
  - `duroxide-provider-implementation` skill for provider developers

- **Provider Validation** - New cancellation validation tests
  - `test_activity_cancellation_via_lock_stealing`
  - Additional lock stealing edge case tests

### Changed

- **Major Documentation Refactor**
  - Rewrote `provider-implementation-guide.md` with better structure and pedagogy
  - Rewrote `architecture.md` with cleaner ASCII diagrams (removed mermaid)
  - Merged `replay-engine.md` into `durable-futures-internals.md`
  - Added long-polling vs short-polling explanation
  - Added Performance Considerations and ProviderAdmin sections

- **Simplified ActivityRegistry API** - Now takes value instead of Arc

- **Improved Dispatcher Backoff Logic** - Better stale activity handling

### Fixed

- **Polling Model Documentation** - Corrected to multi-poll (not single-poll) model

### Code Statistics (vs v0.1.11)

| Area | Files | Insertions | Deletions | Net |
|------|-------|------------|-----------|-----|
| Core (src/) | 15 | +2,355 | -1,999 | +356 |
| Tests (tests/) | 63 | +7,616 | -3,353 | +4,263 |
| Docs (docs/) | 43 | +17,477 | -4,004 | +13,473 |
| **Total** | 134 | +17,459 | -9,536 | **+7,923** |

---

## [0.1.11] - 2026-01-07

**Release:** <https://crates.io/crates/duroxide/0.1.11>

### Fixed

- **Issue #49: WorkItemReader version extraction during completion-only replay**
  
  Fixed a bug where `WorkItemReader` did not extract all fields from history during
  completion-only replay (when no `Start` or `ContinueAsNew` item is present in the
  work item batch). This caused nondeterminism errors when:
  
  - A versioned orchestration was started
  - The runtime restarted (or lock expired) mid-execution
  - Activity completion arrived without a start item
  - The runtime incorrectly used the `Latest` version policy instead of the
    version recorded in history
  
  The fix extracts all tuple fields (`orchestration_name`, `input`, `version`,
  `parent_instance`, `parent_id`) from `HistoryManager` during completion-only replay,
  ensuring deterministic handler resolution.

### Added

- **New scenario tests** for issue #49 regression prevention:
  - `e2e_replay_completion_only_must_use_version_from_history` - First execution replay
  - `e2e_replay_completion_only_after_can_must_use_version_from_history` - Nth execution (after CAN) replay
  - Unit tests verifying all `WorkItemReader` tuple fields are correctly extracted

## [0.1.10] - 2026-01-06

**Release:** <https://crates.io/crates/duroxide/0.1.10>

### Added

- **Rolling Deployment Support** - Exponential backoff for unregistered handlers
  
  Unregistered orchestrations and activities now use exponential backoff instead of
  immediate failure, enabling graceful rolling deployments in multi-node clusters:
  
  - Messages abandoned with backoff (1s → 2s → 4s → ... up to 60s max)
  - Bounce between nodes until one with the handler registered picks it up
  - Eventually fail as `ErrorDetails::Poison` if handler never becomes available
  - Configurable via `UnregisteredBackoffConfig` (defaults: 1s base, 60s max, 6 exponent cap)

- **New scenario tests** for rolling deployments
  - `e2e_rolling_deployment_new_activity` - Multi-node deployment with new activity
  - `e2e_rolling_deployment_version_upgrade` - Version upgrade via continue-as-new

- **Consolidated unregistered handler tests** in `tests/unregistered_backoff_tests.rs`
  - `unknown_version_fails_with_poison` - Version mismatch handling
  - `continue_as_new_to_missing_version_fails_with_poison` - CAN to missing version
  - `delete_poisoned_orchestration` - Cleanup after poison
  - Plus existing backoff behavior tests

### Changed

- **BREAKING:** `ConfigErrorKind::MissingVersion` removed - unregistered handlers now use backoff/poison path
- `config_error` metric now only tracks nondeterminism (unregistered handlers result in `poison`)
- Updated `docs/metrics-specification.md` with new error type behaviors
- Updated `docs/ORCHESTRATION-GUIDE.md` error handling section

### Removed

- `tests/unknown_activity_tests.rs` - consolidated into `unregistered_backoff_tests.rs`
- `tests/unknown_orchestration_tests.rs` - consolidated into `unregistered_backoff_tests.rs`

## [0.1.9] - 2026-01-05

**Release:** <https://crates.io/crates/duroxide/0.1.9>

### Added

- **Management API for Instance Deletion and Pruning** - Comprehensive instance lifecycle management

  **Client API:**
  - `delete_instance(id, force)` - Delete single instance with cascading
  - `delete_instance_bulk(filter)` - Bulk delete with filters (IDs, timestamp, limit)
  - `prune_executions(id, options)` - Prune old executions from long-running instances
  - `prune_executions_bulk(filter, options)` - Bulk prune across multiple instances
  - `get_instance_tree(id)` - Inspect instance hierarchy before deletion

  **Provider API (ProviderAdmin trait):**
  - `delete_instance(id, force)` - Provider-level single deletion
  - `delete_instance_bulk(filter)` - Provider-level bulk deletion
  - `delete_instances_atomic(ids)` - Atomic batch deletion for cascading
  - `prune_executions(id, options)` - Provider-level pruning
  - `prune_executions_bulk(filter, options)` - Provider-level bulk pruning
  - `get_instance_tree(id)` - Provider-level tree traversal
  - `list_children(id)` - List direct child sub-orchestrations
  - `get_parent_id(id)` - Get parent instance ID

  **Safety Guarantees:**
  - Running instances protected (skip or error based on API)
  - Current execution never pruned
  - Sub-orchestrations cannot be deleted directly (must delete root)
  - Atomic cascading deletes (all-or-nothing)
  - Force delete available for stuck instances

- **102 new provider validation tests** - Deletion, bulk deletion, pruning, cascading deletes, filter combinations, safety tests

### Changed

- Provider implementation guide with deletion/pruning contracts
- Provider testing guide updates
- Continue-as-new docs with pruning section
- README instance management section
- Enhanced management-api-deletion proposal with force delete semantics

## [0.1.8] - 2026-01-02

**Release:** <https://crates.io/crates/duroxide/0.1.8>

### Added

- **Lock-stealing activity cancellation** - New mechanism for cancelling in-flight activities
  - Activities are cancelled by deleting their worker queue entries ("lock stealing")
  - Workers detect cancellation when lock renewal fails (entry missing)
  - More efficient than polling execution state on every renewal
  - Enables batch cancellation of multiple activities atomically

- **`ScheduledActivityIdentifier`** - New struct for identifying activities in worker queue
  - Fields: `instance` (String), `execution_id` (u64), `activity_id` (u64)
  - Used by `ack_orchestration_item` to specify activities to cancel
  - Exported from `duroxide::providers`

- **Provider validation tests for lock-stealing** - 5 new tests
  - `test_cancelled_activities_deleted_from_worker_queue` - Verify deletion during ack
  - `test_ack_work_item_fails_when_entry_deleted` - Verify permanent error on stolen lock
  - `test_renew_fails_when_entry_deleted` - Verify renewal fails on stolen lock
  - `test_cancelling_nonexistent_activities_is_idempotent` - Verify no error for missing entries
  - `test_batch_cancellation_deletes_multiple_activities` - Verify batch deletion

- **Worker queue activity identity columns** - Store activity identity for cancellation
  - New migration: `20240104000000_add_worker_activity_identity.sql`
  - SQLite provider stores `instance_id`, `execution_id`, `activity_id` on ActivityExecute items

### Changed

- **BREAKING:** `Provider::ack_orchestration_item` signature changed
  - Added 7th parameter: `cancelled_activities: Vec<ScheduledActivityIdentifier>`
  - Provider must delete matching worker queue entries atomically in same transaction

- **BREAKING:** `Provider::fetch_work_item` return type simplified
  - Changed from `(WorkItem, String, u32, ExecutionState)` to `(WorkItem, String, u32)`
  - Removed `ExecutionState` - cancellation detected via lock renewal failure instead

- **BREAKING:** `Provider::renew_work_item_lock` return type changed
  - Changed from `Result<ExecutionState, ProviderError>` to `Result<(), ProviderError>`
  - Failure indicates lock was stolen (activity cancelled) or expired

- **BREAKING:** `Provider::ack_work_item` must fail when entry missing
  - Returns permanent error if work item entry was deleted (lock stolen)
  - Signals to worker that activity was cancelled

- Provider validation test count: 80 tests (up from 75)

### Removed

- **`ExecutionState` enum removed from Provider API** - No longer needed
  - Was used for state-polling cancellation approach
  - Lock-stealing provides more efficient cancellation mechanism
  - Provider validation tests for ExecutionState still exist (legacy support during migration)

### Migration Guide

**Provider implementers - Required changes:**

1. Update `ack_orchestration_item` signature:
```rust
async fn ack_orchestration_item(
    &self,
    lock_token: &str,
    execution_id: u64,
    history_delta: Vec<Event>,
    worker_items: Vec<WorkItem>,
    orchestrator_items: Vec<WorkItem>,
    metadata: ExecutionMetadata,
    cancelled_activities: Vec<ScheduledActivityIdentifier>,  // NEW
) -> Result<(), ProviderError>;
```

2. Update `fetch_work_item` return type:
```rust
async fn fetch_work_item(...) -> Result<Option<(WorkItem, String, u32)>, ProviderError>;
// Removed ExecutionState from tuple
```

3. Update `renew_work_item_lock` return type:
```rust
async fn renew_work_item_lock(...) -> Result<(), ProviderError>;
// Returns () instead of ExecutionState
```

4. Update `ack_work_item` to fail on missing entry:
```rust
// Return error if entry not found (lock was stolen)
if rows_affected == 0 {
    return Err(ProviderError::permanent("ack_work_item", "Entry not found (lock stolen)"));
}
```

5. Store activity identity on worker queue entries:
```sql
-- Add columns to worker_queue table
ALTER TABLE worker_queue ADD COLUMN instance_id TEXT;
ALTER TABLE worker_queue ADD COLUMN execution_id INTEGER;
ALTER TABLE worker_queue ADD COLUMN activity_id INTEGER;

-- Add index for efficient cancellation
CREATE INDEX idx_worker_queue_activity ON worker_queue(instance_id, execution_id, activity_id);
```

6. Implement batch deletion in `ack_orchestration_item`:
```rust
// Delete cancelled activities atomically within the ack transaction
for activity in cancelled_activities {
    DELETE FROM worker_queue 
    WHERE instance_id = activity.instance 
      AND execution_id = activity.execution_id 
      AND activity_id = activity.activity_id;
}
```

## [0.1.7] - 2025-12-28

**Release:** <https://crates.io/crates/duroxide/0.1.7>

### Added

- **Cooperative activity cancellation** - Activities can detect when their parent orchestration has been cancelled or completed
  - `ActivityContext` now provides cancellation awareness via `is_cancelled()` and `cancelled()` methods
  - Activities can cooperatively respond to cancellation by checking the cancellation token
  - Use `tokio::select!` with `ctx.cancelled()` for responsive cancellation in async activities
  - Configurable grace period before forced activity termination

- **ExecutionState enum** - Providers now report orchestration state with activity work items
  - `ExecutionState::Running` - Orchestration is active, activity should proceed
  - `ExecutionState::Terminal { status }` - Orchestration completed/failed/continued, activity result won't be observed
  - `ExecutionState::Missing` - Orchestration instance deleted, activity should abort

- **Provider validation tests for cancellation** - 13 new tests in `provider_validation::cancellation`
  - Verifies `ExecutionState` is correctly returned by `fetch_work_item` and `renew_work_item_lock`
  - Tests for Running, Terminal (Completed/Failed/ContinuedAsNew), and Missing states
  - Tests for state transitions during activity execution

- **Single-threaded runtime support** - Full compatibility with `tokio::runtime::Builder::new_current_thread()`
  - Essential for embedding in single-threaded environments (e.g., pgrx PostgreSQL extensions)
  - New scenario tests in `tests/scenarios/single_thread.rs`
  - Use `RuntimeOptions { orchestration_concurrency: 1, worker_concurrency: 1, .. }` for 1x1 mode

- **Configurable wait timeout for stress tests** - `StressTestConfig::wait_timeout_secs` field
  - Default: 60 seconds
  - Increase for high-latency remote database providers
  - Uses `#[serde(default)]` for backward compatibility with existing configs

### Changed

- **BREAKING:** `Provider::fetch_work_item` now returns 4-tuple: `(WorkItem, String, u32, ExecutionState)`
  - Added `ExecutionState` as fourth element to report parent orchestration state
  - Required for activity cancellation support

- **BREAKING:** `Provider::renew_work_item_lock` now returns `ExecutionState` instead of `()`
  - Allows runtime to detect orchestration state changes during long-running activities
  - Triggers cancellation token when orchestration becomes terminal

- Provider validation test count increased from 62 to 75

- Documentation updates:
  - Added "Runtime Polling Configuration" section to provider-implementation-guide
  - Default polling interval (10ms) is aggressive; configure for remote/cloud providers
  - Updated provider-testing-guide with new test count and wait_timeout_secs examples

### Fixed

- **test_worker_lock_renewal_extends_timeout** - Fixed timing sensitivity (GitHub #34)
  - Test now creates proper orchestration with Running status before testing renewal
  - Uses 0.6x pre-renewal wait + 0.4x post-renewal wait for reliable timing

- **test_multi_threaded_lock_expiration_recovery** - Fixed race condition (GitHub #32)
  - Uses `tokio::sync::Barrier` to synchronize thread start times
  - Eliminates false failures from connection pool cold-start latency

### Migration Guide

**Provider implementers:**
```rust
// fetch_work_item now returns ExecutionState
async fn fetch_work_item(
    &self,
    lock_timeout: Duration,
    poll_timeout: Duration,
) -> Result<Option<(WorkItem, String, u32, ExecutionState)>, ProviderError>;

// renew_work_item_lock now returns ExecutionState
async fn renew_work_item_lock(
    &self,
    token: &str,
    extend_for: Duration,
) -> Result<ExecutionState, ProviderError>;
```

**Determining ExecutionState:**
```rust
// Query the execution status for the work item's instance/execution_id
let state = match (instance_exists, execution_status) {
    (false, _) => ExecutionState::Missing,
    (true, None) => ExecutionState::Missing,
    (true, Some(status)) if status == "Running" => ExecutionState::Running,
    (true, Some(status)) => ExecutionState::Terminal { status },
};
```

**Activity authors (using cancellation):**
```rust
activities.register("LongTask", |ctx: ActivityContext, input: String| async move {
    for item in items {
        // Check cancellation periodically
        if ctx.is_cancelled() {
            return Err("Cancelled".into());
        }
        process(item).await;
    }
    Ok("done".into())
});

// Or use select! for responsive cancellation
activities.register("AsyncTask", |ctx: ActivityContext, input: String| async move {
    tokio::select! {
        result = do_work(input) => result,
        _ = ctx.cancelled() => Err("Cancelled".into()),
    }
});
```

## [0.1.6] - 2025-12-21

**Release:** <https://crates.io/crates/duroxide/0.1.6>

### Added

- **Large payload stress test** - New memory-intensive stress test scenario
  - Tests large event payloads (10KB, 50KB, 100KB) and longer histories (~80-100 events)
  - New binary: `large-payload-stress` for running the test standalone
  - Uses the same `ProviderStressFactory` trait as parallel orchestrations test
  - Configurable payload sizes and activity/sub-orchestration counts
  - See `docs/provider-testing-guide.md` for usage

- **Stress test monitoring** - Resource usage tracking in `run-stress-tests.sh`
  - Peak RSS (Resident Set Size) measurement
  - Average CPU usage tracking
  - Sampling every 500ms during test execution
  - New documentation: `STRESS_TEST_MONITORING.md`
  - Supports `--parallel-only` and `--large-payload` flags

### Changed

- **Memory optimization** - Reduced allocations in history processing
  - Added `HistoryManager::full_history_len()` - get count without allocation
  - Added `HistoryManager::is_full_history_empty()` - check emptiness without allocation
  - Added `HistoryManager::full_history_iter()` - iterate without allocation
  - Updated runtime to use efficient methods in hot paths
  - Improved child cancellation to use iterator instead of collecting full history

- **Orchestration naming** - Renamed "FanoutWorkflow" to "FanoutOrchestration" for consistency

### Fixed

- Child sub-orchestration cancellation now uses iterator-based approach for better memory efficiency

## [0.1.5] - 2025-12-18

**Release:** <https://crates.io/crates/duroxide/0.1.5>

### Added

- **Provider identity API** - Providers now expose `name()` and `version()` methods
  - `Provider::name()` returns provider name (e.g., "sqlite")
  - `Provider::version()` returns provider version
  - Default implementations return "unknown" and "0.0.0"
  - SQLite provider returns "sqlite" and the crate version

- **Runtime startup banner** - Version information logged on startup
  - Logs duroxide version and provider name/version
  - Example: `duroxide runtime (0.1.4) starting with provider sqlite (0.1.4)`

- **Worker queue visibility control** - Worker queue now uses `visible_at` for delayed visibility
  - Added `visible_at` column to worker_queue (matches orchestrator queue pattern)
  - `abandon_work_item` with delay now sets `visible_at` instead of keeping `locked_until`
  - Cleaner semantics: `visible_at` controls when item becomes visible, `locked_until` only for lock expiry
  - Migration file included for existing databases

- **New provider validation tests** - 2 additional queue semantics tests
  - `test_worker_item_immediate_visibility` - Verify newly enqueued items are immediately visible
  - `test_worker_delayed_visibility_skips_future_items` - Verify items with future visible_at are skipped

### Changed

- **Reduced default `dispatcher_long_poll_timeout`** from 5 minutes to 30 seconds
  - More responsive shutdown behavior
  - Better suited for typical workloads

## [0.1.3] - 2025-12-14

### Added

- **Provider validation tests** - 4 new tests for abandon and poison handling
  - `test_abandon_work_item_releases_lock` - Verify abandon_work_item releases lock immediately
  - `test_abandon_work_item_with_delay` - Verify abandon_work_item with delay defers refetch
  - `max_attempt_count_across_message_batch` - Verify MAX attempt_count returned for batched messages

### Changed

- Provider validation test count increased from 58 to 62

### Fixed

- `abandon_work_item` with delay now correctly keeps lock_token to prevent immediate refetch

## [0.1.2] - 2025-12-14

### Added

- **Poison message handling** - Automatic detection and failure of messages that exceed `max_attempts` (default: 10)
  - `RuntimeOptions::max_attempts` configuration option
  - `ErrorDetails::Poison` variant with detailed context
  - `PoisonMessageType` enum distinguishing orchestration vs activity poison
  - Dedicated metrics: `duroxide_orchestration_poison_total`, `duroxide_activity_poison_total`

- **Lock renewal for orchestrations** - Prevents lock expiration during long orchestration turns
  - `Provider::renew_orchestration_item_lock()` method
  - `RuntimeOptions::orchestrator_lock_renewal_buffer` configuration (default: 2s)
  - Automatic background renewal task in orchestration dispatcher

- **Work item abandon with retry** - Explicit lock release for failed activities
  - `Provider::abandon_work_item()` method with optional delay
  - Called automatically when `ack_work_item` fails

- **Attempt count management** - `ignore_attempt` parameter for abandon methods
  - `abandon_work_item(..., ignore_attempt: bool)` - decrement count on transient failures
  - `abandon_orchestration_item(..., ignore_attempt: bool)` - same for orchestrations
  - Prevents false poison detection from infrastructure errors

- **Provider validation tests** - 8 new poison message tests
  - `orchestration_attempt_count_starts_at_one`
  - `orchestration_attempt_count_increments_on_refetch`
  - `worker_attempt_count_starts_at_one`
  - `worker_attempt_count_increments_on_lock_expiry`
  - `attempt_count_is_per_message`
  - `abandon_work_item_ignore_attempt_decrements`
  - `abandon_orchestration_item_ignore_attempt_decrements`
  - `ignore_attempt_never_goes_negative`

### Changed

- **BREAKING:** SQLite provider is now optional - enable with `features = ["sqlite"]`
- **BREAKING:** `Provider::fetch_work_item` now returns `(WorkItem, String, u32)` tuple (added `attempt_count`)
- **BREAKING:** `Provider::fetch_orchestration_item` now returns `(OrchestrationItem, String, u32)` tuple (added `attempt_count`)
- **BREAKING:** `Provider::abandon_work_item` now requires `ignore_attempt: bool` parameter
- **BREAKING:** `Provider::abandon_orchestration_item` now requires `ignore_attempt: bool` parameter
- `OrchestrationItem` struct no longer contains `lock_token` (moved to return tuple)
- Provider validation test count increased from 50 to 58

### Migration Guide

**Cargo.toml (if using SQLite provider):**
```toml
# Before
duroxide = "0.1.1"

# After - SQLite now requires explicit feature
duroxide = { version = "0.1.2", features = ["sqlite"] }
```

**Provider implementers:**
```rust
// fetch_work_item now returns attempt_count
async fn fetch_work_item(...) -> Result<Option<(WorkItem, String, u32)>, ProviderError>;

// fetch_orchestration_item now returns attempt_count
async fn fetch_orchestration_item(...) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError>;

// abandon methods now have ignore_attempt parameter
async fn abandon_work_item(&self, token: &str, delay: Option<Duration>, ignore_attempt: bool) -> Result<(), ProviderError>;
async fn abandon_orchestration_item(&self, token: &str, delay: Option<Duration>, ignore_attempt: bool) -> Result<(), ProviderError>;

// New method for orchestration lock renewal
async fn renew_orchestration_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError>;
```

**Runtime users:**
```rust
RuntimeOptions {
    max_attempts: 10,  // NEW - poison threshold
    orchestrator_lock_renewal_buffer: Duration::from_secs(2),  // NEW
    ..Default::default()
}
```

## [0.1.1] - 2025-12-10

### Added

- **Long polling support** - Providers can now block waiting for work, reducing CPU usage and latency
- `dispatcher_long_poll_timeout` configuration option (default: 5 minutes)
- `poll_timeout: Duration` parameter to `Provider::fetch_orchestration_item` and `Provider::fetch_work_item`
- Long polling validation tests in `duroxide::provider_validations::long_polling`

### Changed

- **BREAKING:** `Provider::fetch_orchestration_item` now requires `poll_timeout: Duration` parameter
- **BREAKING:** `Provider::fetch_work_item` now requires `poll_timeout: Duration` parameter
- **BREAKING:** `RuntimeOptions::dispatcher_idle_sleep` renamed to `dispatcher_min_poll_interval`
- **BREAKING:** `continue_as_new()` now returns an awaitable future (use `return ctx.continue_as_new(input).await`)

### Migration Guide

**Provider implementers:**
```rust
// Add poll_timeout parameter to both fetch methods
async fn fetch_orchestration_item(
    &self,
    lock_timeout: Duration,
    poll_timeout: Duration,  // NEW - ignore for short-polling, block for long-polling
) -> Result<Option<OrchestrationItem>, ProviderError>;
```

**Runtime users:**
```rust
// Rename dispatcher_idle_sleep to dispatcher_min_poll_interval
RuntimeOptions {
    dispatcher_min_poll_interval: Duration::from_millis(100),
    dispatcher_long_poll_timeout: Duration::from_secs(300),  // NEW
    ..Default::default()
}
```

**Orchestration authors using continue_as_new:**
```rust
// Before: ctx.continue_as_new(input);
// After:
return ctx.continue_as_new(input).await;
```

## [0.1.0] - 2025-12-01

### Added

- Initial release
- Deterministic orchestration execution with replay
- Activity scheduling with automatic retries
- Timer support (create_timer)
- Sub-orchestration support
- External event handling
- Continue-as-new for long-running workflows
- SQLite provider implementation
- OpenTelemetry metrics and structured logging
- Provider validation test suite
- Comprehensive documentation



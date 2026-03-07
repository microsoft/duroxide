# Duroxide AI Agent Instructions

A durable execution runtime for Rust. Key docs: `docs/ORCHESTRATION-GUIDE.md`, `docs/provider-implementation-guide.md`.

## ⚠️ CRITICAL: Git Operations

**NEVER commit or push without explicit user permission.** Always ask before running `git commit` or `git push`.

## ⚠️ CRITICAL: Test Execution

**ALWAYS use nextest for running tests.** This is mandatory - nextest provides faster execution and better output.

```bash
cargo nt                              # Run all tests (nextest alias)
cargo nt -E 'test(/pattern/)'         # Filter tests by pattern
cargo nt --test specific_test_file    # Run specific test file
```

Only fall back to `cargo test` if nextest is not available. Never use `cargo test` when `cargo nt` works.

## ⚠️ CRITICAL: Flaky Tests

**NEVER dismiss a failing test as a "pre-existing flake" and move on.** Flaky tests block progress and must be investigated.

- When a failure is **unrelated to current changes**, state that explicitly — then offer to investigate the root cause and fix it.
- **Always investigate deeply**: trace the exact code path, build a plausible theory for the failure mechanism, and propose a concrete fix.
- Do not hand-wave "timing" or "contention" as an explanation without proving the specific race condition.
- A test that passes in isolation but fails under parallel execution has a real bug — either in the test (ordering assumptions, shared state) or in the runtime (contention handling).

## Architecture Overview

**Two-queue message-driven runtime:**
- **OrchestrationDispatcher** (orchestrator queue): Processes workflow turns, durable timers, and external events
- **WorkDispatcher** (worker queue): Executes activities with automatic lock renewal for long-running work

**Core types:**
- `OrchestrationContext` - Schedules work via `schedule_activity()`, `schedule_timer()`, `schedule_wait()`, `schedule_sub_orchestration()` - all return futures you can `.await` directly
- `Event`/`Action` - Immutable history entries; providers only store, never generate IDs
- `Provider` trait - Storage abstraction with peek-lock semantics (`src/providers/mod.rs`)

## Critical Patterns

**Awaiting schedule_* methods:**
```rust
// schedule_* methods return futures that can be awaited directly
let result = ctx.schedule_activity("Task", input).await?;
ctx.schedule_timer(Duration::from_secs(5)).await;
let event_data = ctx.schedule_wait("MyEvent").await;
let sub_result = ctx.schedule_sub_orchestration("Child", input).await?;
```

**Use ctx.join/select, NOT tokio::join/select:**
```rust
// ✅ CORRECT - deterministic, history-ordered resolution
match ctx.select2(timer, activity).await {
    Either2::First(()) => { /* timer won */ }
    Either2::Second(result) => { /* activity won */ }
}
let results = ctx.join(vec![f1, f2, f3]).await;

// ❌ WRONG - non-deterministic, breaks replay
let (a, b) = tokio::join!(f1, f2);  // NEVER use tokio::join in orchestrations!
tokio::select! { ... }              // NEVER use tokio::select in orchestrations!
```

**Orchestrations vs Activities:**
- Orchestrations: Deterministic coordination only (no I/O, no randomness, no time)
- Activities: Single-purpose execution (can do anything - DB, HTTP, sleep, poll)
- Use `ctx.schedule_timer()` for orchestration delays, NOT activity sleep

**Activity cancellation (cooperative):**
```rust
// Activities check ctx.is_cancelled() or use tokio::select! with ctx.cancelled()
.register("LongTask", |ctx: ActivityContext, input: String| async move {
    for item in items {
        if ctx.is_cancelled() { return Err("Cancelled".into()); }
        process(item).await;
    }
    Ok("done".into())
})
```

## Single-Threaded Runtime Support

**Duroxide MUST work in single-threaded tokio runtime** (e.g., pgrx PostgreSQL extensions with SPI calls).
- All tests in `tests/scenarios/single_thread.rs` use `#[tokio::test(flavor = "current_thread")]`
- Use `RuntimeOptions { orchestration_concurrency: 1, worker_concurrency: 1, .. }` (1x1 mode)
- Never spawn blocking tasks that assume multi-threaded runtime

## Build & Test Commands

```bash
cargo nt                               # ⚠️ PREFERRED: Run all tests with nextest (--all-features)
cargo nt -E 'test(/pattern/)'          # Filter tests by pattern
cargo nt --test specific_test          # Run specific test file
./run-tests.sh                         # ⚠️ COMPREHENSIVE: Two-pass (with + without feature flags)
./run-tests.sh -E 'test(/pattern/)'    # Two-pass with filter
cargo build --all-targets              # Build everything
cargo clippy --all-targets --all-features  # Full lint check
cargo test --doc                       # Doctest validation (nextest doesn't run these)
cargo run --example hello_world        # Run example
```

`cargo nt` runs `nextest run --all-features` (single pass). `./run-tests.sh` adds a second pass
without features to catch behaviors hidden under `--all-features` — feature-gated code paths,
serde boundary enforcement (v2 event rejection), and conditional compilation differences.
Use `./run-tests.sh` before committing.

## Key Directories

- `src/runtime/dispatchers/` - Orchestration and worker dispatchers
- `src/providers/sqlite.rs` - Reference provider implementation
- `tests/scenarios/` - Production pattern regression tests (add real-world patterns here)
- `tests/e2e_samples.rs` - Comprehensive usage examples
- `prompts/` - AI task prompts (use `duroxide-*.md` for specific workflows)

## Provider Implementation

Providers are pure storage - runtime owns all orchestration logic:
- `fetch_work_item()` returns 4-tuple: `(WorkItem, String, u32, ExecutionState)`
- `ack_work_item()` takes `Option<WorkItem>` - `None` skips enqueueing completion
- `renew_work_item_lock()` returns `ExecutionState` for cancellation support
- See `docs/provider-implementation-guide.md` for complete contract

**When changing Provider trait:**
1. Add validation tests in `src/provider_validation/` 
2. Add tests to `tests/sqlite_provider_validations.rs`
3. Update `docs/provider-implementation-guide.md` (signatures, pseudocode, semantics)
4. Update `docs/provider-testing-guide.md` (test count, new test category if needed)

## Determinism Rules

Orchestrations must be deterministic for replay:
- ✅ Use: `ctx.new_guid()`, `ctx.utcnow()`, control flow, `continue_as_new()`
- ❌ Avoid: `Uuid::new_v4()`, `SystemTime::now()`, `rand::random()`, `std::process::id()`, direct I/O, `tokio::join!/select!`

## Version Compatibility (Rolling Upgrades)

Users upgrade duroxide by rolling out new versions one node at a time. This means:
- **Flag any change** that would break mixed-version clusters (old runtime + new runtime)
- Event schema changes must be backward compatible (new fields optional, old fields preserved)
- Provider API changes need migration paths
- Never remove event variants - deprecate first

## Error Handling

- Orchestrations return `Result<String, String>`
- Activities return `Result<String, String>` 
- Use `ErrorDetails` enum for structured errors (Infrastructure/Configuration/Application)
- Provider methods return `Result<_, ProviderError>` with retryable classification

## Scenario Tests

`tests/scenarios/` captures real-world patterns discovered in production (toygres, duroxide-pg):
- **Instance Actor Pattern**: Long-running orchestration with `continue_as_new()` for health monitoring
- **Continue-As-New Chains**: Prevent unbounded history growth
- **Single-Thread Mode**: Validates pgrx/embedded use cases

When adding features, consider if they warrant a scenario test based on real-world usage.

**Pre-seeded history:** Tests that need history without a running runtime should use helpers in `tests/common/mod.rs`:
- `seed_history_turn()` — enqueue→fetch→ack cycle in one call (builds one orchestrator turn)
- `seed_instance_with_pinned_version()` — creates an instance stamped with a specific duroxide version
- `make_versioned_event()` — creates an event with a custom `duroxide_version` stamp
- Never duplicate the raw enqueue→fetch→ack pattern inline — use these helpers instead

## AI Workflow Prompts

Reference `prompts/duroxide-*.md` for structured tasks:
- `duroxide-clean-warnings.md` - Eliminate compiler/clippy warnings
- `duroxide-update-docs-tests.md` - Documentation and test updates with OTel audit
- `duroxide-create-scenario-test.md` - Convert patterns to regression tests
- `duroxide-stress-test.md` - Performance testing

## Workflow Rules

- **Never commit or push without explicit user permission.** Always ask before running `git commit` or `git push`.

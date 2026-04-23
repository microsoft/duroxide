# ContinueAsNew Semantics

ContinueAsNew (CAN) terminates the current execution and starts a new execution for the same instance with fresh input and empty history.

## API

```rust
// Returns a future that never resolves - use with `return` and `await`
fn continue_as_new(&self, input: impl Into<String>) -> impl Future<Output = Result<String, String>>
fn continue_as_new_typed<T: Serialize>(&self, input: &T) -> impl Future<Output = Result<String, String>>
fn continue_as_new_versioned(&self, version: &str, input: &str) -> impl Future<Output = Result<String, String>>
```

**Usage pattern:**
```rust
async fn my_orchestration(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    // Do some work...
    let result = ctx.schedule_activity("DoWork", input.clone()).await?;
    
    if should_continue(&result) {
        // MUST use `return` and `.await` - future never resolves
        return ctx.continue_as_new(result).await;
    }
    
    Ok(result)
}
```

**Important:** `continue_as_new()` returns a future that **never resolves**. You must:
1. Call `.await` on it
2. Use `return` to ensure code after is unreachable

The compiler will warn about unreachable code if you forget `return`.

## Key Points

- **Terminal for current execution:** The current execution appends `OrchestrationContinuedAsNew { input }` and stops.
- **New execution history:** The runtime creates a new execution (incremented execution_id) and stamps a fresh `OrchestrationStarted` with the provided input. The provider simply persists what the runtime sends.
- **Event targeting:** Activity/timer/external events always target the currently active execution. After CAN, the next execution must resubscribe/re-arm as needed.
- **Result propagation:** The initial start handle resolves with an empty output when the first execution continues-as-new; the runtime then automatically starts the next execution.

## Flow

1. Orchestrator calls `return ctx.continue_as_new(new_input).await`.
2. Runtime extracts pending actions (including `ContinueAsNew`) before checking future state.
3. Runtime appends terminal `OrchestrationContinuedAsNew` to the current execution.
4. Runtime enqueues a `WorkItem::ContinueAsNew` and later processes it as a new execution with empty history, stamping `OrchestrationStarted { name, version, input: new_input, parent_instance?, parent_id? }`.
5. Provider receives `ack_orchestration_item(lock_token, execution_id = prev + 1, ...)` and atomically persists the new execution row, updates `instances.current_execution_id = MAX(..., execution_id)`, and appends events.

## Practical Tips

- Re-create subscriptions and timers in the new execution; carry forward any needed state via the new input.
- External events sent before the new execution subscribes will be dropped; raise them after the new execution has subscribed.
- Use deterministic correlation IDs if you coordinate sub-orchestrations across executions.
- **Sessions survive ContinueAsNew** — the session row persists, and the new execution
  can schedule activities on the same `session_id`. Activities route to the same worker.

## Pruning Old Executions

Long-running workflows using ContinueAsNew will accumulate execution history over time. Use `prune_executions` to clean up old executions while preserving the current execution:

```rust
use duroxide::PruneOptions;

// Prune to only the current execution (recommended)
client.prune_executions("my-instance", PruneOptions {
    keep_last: None,  // No count-based retention
    ..Default::default()
}).await?;

// Keep the last 10 executions
client.prune_executions("my-instance", PruneOptions {
    keep_last: Some(10),
    ..Default::default()
}).await?;

// Or prune by age (executions older than 30 days)
let thirty_days_ago = now_ms - (30 * 24 * 60 * 60 * 1000);
client.prune_executions("my-instance", PruneOptions {
    completed_before: Some(thirty_days_ago),
    ..Default::default()
}).await?;
```

**Safety guarantees:**
- The **current execution** (highest execution_id) is **never** pruned
- This protection applies to both running and terminal instances
- Pruning can be done while the workflow is actively running

**Note on `keep_last` semantics:** Since the current execution is always protected and is always the highest execution_id, `keep_last: None`, `Some(0)`, and `Some(1)` are all equivalent—they all prune down to exactly the current execution. Use `None` for clarity when you want to prune all historical executions.

## Anticipating the History Limit

Every event appended to an orchestration's history consumes space. The runtime hard-caps
history at **5 MiB** (`MAX_HISTORY_BYTES`). When an orchestration's persisted history
reaches that cap its next turn is terminated with an error — so it's better to roll over
before reaching the limit.

`OrchestrationContext` exposes two accessors for this purpose:

```rust
/// Serialized byte size of history at the start of this turn (replay-safe).
pub fn history_size_bytes(&self) -> usize

/// history_size_bytes() / MAX_HISTORY_BYTES, clamped to 0.0..=1.0.
pub fn history_pressure(&self) -> f32
```

Both values are set once per turn from the baseline history that existed when the turn
started. They are **deterministic across replay** — the replay engine sets them before
invoking the orchestration function, so every replay sees the same snapshot.

### Choosing a rollover threshold

A common pattern is to roll over at 80 % pressure, leaving a comfortable margin for the
events that will be appended during the current turn:

```rust
async fn eternal_monitor(ctx: OrchestrationContext, state_json: String) -> Result<String, String> {
    let state: State = serde_json::from_str(&state_json).unwrap_or_default();

    // Roll over proactively before the hard cap is hit.
    if ctx.history_pressure() >= 0.80 {
        let trimmed = serde_json::to_string(&state.trim_for_handoff()).unwrap();
        return ctx.continue_as_new(trimmed).await;
    }

    // Normal turn logic ...
    let result = ctx.schedule_activity("DoWork", state_json.clone()).await?;
    ctx.schedule_timer(std::time::Duration::from_secs(60)).await;
    return ctx.continue_as_new(result).await;
}
```

### Enforcement and error shape (opt-in)

By default the hard cap is **observed but not enforced** — the accessors are always
available but no turn is failed purely because of history size. Two `RuntimeOptions`
toggles control enforcement:

| Toggle | Default | Effect |
|---|---|---|
| `enforce_size_limits` | `false` | When `true`, terminates turns that would exceed 5 MiB |
| `emit_limit_exceeded_errors` | `false` | When `true`, failed turns produce `ConfigErrorKind::LimitExceeded` instead of `Application::OrchestrationFailed` |

Enable enforcement once you're confident your orchestrations roll over in time:

```rust
let runtime = Runtime::builder(provider)
    .with_options(RuntimeOptions {
        enforce_size_limits: true,
        emit_limit_exceeded_errors: true,
        ..Default::default()
    })
    .build();
```

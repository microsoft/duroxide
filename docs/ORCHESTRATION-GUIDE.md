# Duroxide Orchestration Writing Guide

**For:** LLMs and humans building durable workflows with Duroxide  
**Reference:** See `tests/e2e_samples.rs` and `examples/` for complete working examples

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Concepts](#core-concepts)
3. [Determinism Rules](#determinism-rules)
4. [API Reference](#api-reference)
5. [Client API Reference](#client-api-reference)
6. [Common Patterns](#common-patterns)
7. [Anti-Patterns (What to Avoid)](#anti-patterns)
8. [Complete Examples](#complete-examples)
9. [Orchestration Versioning](#orchestration-versioning)
10. [Testing Your Orchestrations](#testing)

---

## Quick Start

### Minimum Viable Orchestration

```rust
use duroxide::{ActivityContext, OrchestrationContext, OrchestrationRegistry, Client};
use duroxide::runtime::{self, registry::ActivityRegistry};
use duroxide::providers::sqlite::SqliteProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create storage
    let store = Arc::new(SqliteProvider::new("sqlite::memory:", None).await?);
    
    // 2. Register activities (side-effecting work)
    let activities = ActivityRegistry::builder()
        .register("Greet", |ctx: ActivityContext, name: String| async move {
            Ok(format!("Hello, {}!", name))
        })
        .build();
    
    // 3. Define orchestration (deterministic coordination)
    let orchestration = |ctx: OrchestrationContext, name: String| async move {
        let greeting = ctx.schedule_activity("Greet", name).await?;
        Ok(greeting)
    };
    
    // 4. Register and start runtime
    let orchestrations = OrchestrationRegistry::builder()
        .register("HelloWorld", orchestration)
        .build();
    
    let rt = runtime::Runtime::start_with_store(
        store.clone(), 
        activities, 
        orchestrations
    ).await;
    
    // 5. Start an instance
    let client = Client::new(store);
    client.start_orchestration("my-instance-1", "HelloWorld", "World").await?;
    
    // 6. Wait for completion
    let status = client.wait_for_orchestration(
        "my-instance-1", 
        std::time::Duration::from_secs(5)
    ).await?;
    
    println!("Result: {:?}", status);
    
    rt.shutdown(None).await;
    Ok(())
}
```

---

## Core Concepts

### Orchestrations vs Activities

**Orchestrations** (deterministic coordination logic):
- Written as async functions: `|ctx: OrchestrationContext, input: T| async move { ... }`
- Must be **deterministic** (same input → same decisions)
- **Only** schedule work, don't do work themselves
- Can use: if/else, loops, match, any Rust control flow
- **CANNOT** use: random, time, I/O, non-deterministic APIs
- **Purpose:** Coordinate multi-step workflows, contain business logic

**Activities** (single-purpose execution units):
- Stateless async functions: `|ctx: ActivityContext, input: T| async move { ... }`
- Can do **anything**: database, HTTP, file I/O, polling, VM provisioning, etc.
- **CAN sleep/poll** internally as part of their work (e.g., waiting for VM to provision)
- Should be **idempotent** (tolerate retries)
- Should be **single-purpose** (do ONE thing well)
- Executed by worker dispatcher (can fail and retry)
- Return `Result<String, String>` (success or error)
- First parameter `ActivityContext` provides logging and metadata access

**Key Principle: Separation of Concerns**
- **Activities** = What to do (execution)
- **Orchestrations** = When and how to do it (coordination)

**Mental Model:**
- Orchestration = conductor (coordinates the orchestra, decides when each musician plays)
- Activity = musician (actually plays the music, focuses on playing well)
- Pull multi-step logic UP to orchestrations, keep activities focused

**Example:**
```rust
// ✅ GOOD: Activity polls for VM readiness (single-purpose: provision VM)
// Note: Long-running activities (minutes to hours) are fully supported via automatic lock renewal
activities.register("ProvisionVM", |ctx: ActivityContext, config| async move {
    let vm = create_vm(config).await?;
    while !vm_ready(&vm).await {
        tokio::time::sleep(Duration::from_secs(5)).await;  // ✅ Internal polling is fine
    }
    Ok(vm.id)
});

// ✅ GOOD: Orchestration coordinates multiple activities
async fn deploy_app(ctx: OrchestrationContext, config: String) -> Result<String, String> {
    let vm_id = ctx.schedule_activity("ProvisionVM", config.clone()).await?;
    let app_id = ctx.schedule_activity("DeployApp", vm_id).await?;
    let health = ctx.schedule_activity("HealthCheck", app_id.clone()).await?;
    
    if health != "healthy" {
        // ✅ Business logic in orchestration
        ctx.schedule_activity("Rollback", app_id).await?;
        return Err("Deployment unhealthy".to_string());
    }
    
    Ok(app_id)
}
```

### Cooperative Activity Cancellation

When an orchestration is cancelled or reaches a terminal state, in-flight activities receive a **cancellation signal** via `ActivityContext`. Activities can choose how to respond:

**Cooperative Activities (Recommended)**

Activities should check for cancellation during long-running work:

```rust
use duroxide::ActivityContext;

activities.register("LongRunningTask", |ctx: ActivityContext, input: String| async move {
    for i in 0..100 {
        // ✅ BEST PRACTICE: Check cancellation before each unit of work
        if ctx.is_cancelled() {
            ctx.trace_info("Received cancellation, cleaning up...".to_string());
            cleanup_partial_work().await;
            return Err("Cancelled".to_string());
        }
        
        do_work_chunk(i).await;
    }
    Ok("completed".to_string())
});
```

**Using `select!` for Cancellation-Aware I/O**

For activities that wait on external resources, use `select!` with `ctx.cancelled()`:

```rust
activities.register("WaitForResource", |ctx: ActivityContext, resource_id: String| async move {
    tokio::select! {
        // Wait for cancellation signal
        _ = ctx.cancelled() => {
            ctx.trace_info("Cancelled while waiting for resource".to_string());
            Err("Cancelled".to_string())
        }
        // Or complete the actual work
        result = fetch_resource(&resource_id) => {
            result.map_err(|e| e.to_string())
        }
    }
});
```

**Non-Cooperative Activities**

If an activity ignores the cancellation signal:
1. The runtime waits for a configurable **grace period** (default: 30 seconds)
2. After the grace period, the activity is **forcibly aborted**
3. The activity's result (success or failure) is **dropped** since the orchestration is already terminal

**Best Practices for Long-Running Activities**

```rust
// ✅ GOOD: Polling activity with cancellation support
activities.register("ProvisionVM", |ctx: ActivityContext, config: String| async move {
    let vm = create_vm(&config).await?;
    
    // Poll for readiness with cancellation check
    loop {
        if ctx.is_cancelled() {
            ctx.trace_info("Cancelling VM provisioning".to_string());
            cleanup_vm(&vm.id).await;
            return Err("Cancelled during provisioning".to_string());
        }
        
        if vm_ready(&vm.id).await {
            return Ok(vm.id);
        }
        
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
});

// ✅ GOOD: HTTP client with cancellation-aware timeout
activities.register("CallExternalAPI", |ctx: ActivityContext, request: String| async move {
    let client = reqwest::Client::new();
    
    tokio::select! {
        _ = ctx.cancelled() => {
            Err("Request cancelled".to_string())
        }
        response = client.post("https://api.example.com")
            .body(request)
            .timeout(Duration::from_secs(30))
            .send() => {
            response
                .map_err(|e| e.to_string())?
                .text()
                .await
                .map_err(|e| e.to_string())
        }
    }
});
```

**ActivityContext Cancellation API**

| Method | Description |
|--------|-------------|
| `ctx.is_cancelled()` | Returns `true` if cancellation was requested |
| `ctx.cancelled()` | Returns a `Future` that completes when cancelled (use in `select!`) |

### The Replay Model


**Why replay?**
Orchestrations can run for hours/days/months and must survive crashes.

**How it works:**
1. **First execution**: Orchestration runs, schedules activities, decisions recorded as events
2. **After crash/restart**: Orchestration code runs again, but replays from history
3. **Awaits resolve from history**: No duplicate side effects, fast forward to where we left off

**Example:**
```rust
async fn my_orchestration(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    // Turn 1 (first execution):
    let result = ctx.schedule_activity("Step1", input).await?;
    // ↑ Schedules Step1, waits for completion
    // [CRASH happens here]
    
    // Turn 2 (after restart - REPLAY):
    let result = ctx.schedule_activity("Step1", input).await?;
    // ↑ Doesn't re-schedule! Reads Step1 completion from history
    
    let result2 = ctx.schedule_activity("Step2", result).await?;
    // ↑ Schedules Step2 for FIRST time (not in history yet)
    
    Ok(result2)
}
```

### Scheduling Work

All `schedule_*` methods return futures that can be awaited directly:

```rust
// Activities return Result<String, String>
let result = ctx.schedule_activity("Task", "input").await?;

// Timers return ()
ctx.schedule_timer(Duration::from_secs(5)).await;

// External events return String
let data = ctx.schedule_wait("EventName").await;

// Sub-orchestrations return Result<String, String>
let child_result = ctx.schedule_sub_orchestration("Child", "input").await?;
```

---

## Determinism Rules

### ✅ DO (Safe in Orchestrations)

```rust
async fn safe_orchestration(ctx: OrchestrationContext, count: i32) -> Result<String, String> {
    // ✅ Control flow
    if count > 10 {
        return Ok("too many".to_string());
    }
    
    // ✅ Loops
    for i in 0..count {
        ctx.schedule_activity("Process", i.to_string()).await?;
    }
    
    // ✅ Pattern matching
    let status = ctx.schedule_activity("GetStatus", "").await?;
    match status.as_str() {
        "ready" => { /* ... */ }
        "pending" => { /* ... */ }
        _ => { /* ... */ }
    }
    
    // ✅ Timers for delays
    ctx.schedule_timer(std::time::Duration::from_secs(5)).await;
    
    // ✅ External events for signals
    let approval = ctx.schedule_wait("ApprovalEvent").await;
    
    // ✅ Logging (replay-safe)
    ctx.trace_info("Step completed");
    
    // ✅ Deterministic GUIDs
    let id = ctx.new_guid().await?;
    
    // ✅ Deterministic timestamps
    let now = ctx.utc_now().await?;
    let now_ms = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_millis() as u64;
    
    // ✅ KV store (durable state)
    ctx.set_value("progress", "step_3");
    let val = ctx.get_value("progress");
    
    Ok("done".to_string())
}
```

### ❌ DON'T (Non-Deterministic)

```rust
async fn unsafe_orchestration(ctx: OrchestrationContext, _input: String) -> Result<String, String> {
    // ❌ Random numbers
    let random = rand::random::<u64>();  // Different on each replay!
    
    // ❌ System time
    let now = std::time::SystemTime::now();  // Different on each replay!
    
    // ❌ I/O in orchestration
    let file_contents = std::fs::read_to_string("data.txt")?;  // Side effect!
    
    // ❌ HTTP in orchestration
    let response = reqwest::get("https://api.example.com").await?;  // Side effect!
    
    // ❌ Sleep in orchestration
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;  // Use ctx.schedule_timer() instead!
    
    Ok("unsafe".to_string())
}
```

**Instead, use:**
```rust
async fn safe_version(ctx: OrchestrationContext, _input: String) -> Result<String, String> {
    // ✅ Random → Use ctx.new_guid() or activity
    let id = ctx.new_guid().await?;
    
    // ✅ Time → Use ctx.utc_now()
    let now = ctx.utc_now().await?;
    
    // ✅ I/O → Use activities
    let file_contents = ctx.schedule_activity("ReadFile", "data.txt").await?;
    
    // ✅ HTTP → Use activities
    let response = ctx.schedule_activity("HttpGet", "https://api.example.com").await?;
    
    // ✅ Sleep → Use timers
    ctx.schedule_timer(std::time::Duration::from_secs(5)).await;
    
    // ✅ Activity
    let result = ctx.schedule_activity("Task", "input").await?;
    
    Ok(result)
}
```

---

## API Reference

### OrchestrationContext Methods

#### Scheduling Activities

```rust
// String I/O (basic)
fn schedule_activity(&self, name: impl Into<String>, input: impl Into<String>) 
    -> impl Future<Output = Result<String, String>>

// Typed I/O (with serde)
fn schedule_activity_typed<In: Serialize, Out: DeserializeOwned>(
    &self, 
    name: impl Into<String>, 
    input: &In
) -> impl Future<Output = Result<Out, String>>

// Usage:
let result = ctx.schedule_activity("ProcessOrder", order_json).await?;

let result: OrderResult = ctx.schedule_activity_typed("ProcessOrder", &order_data).await?;
```

#### Scheduling Activities with Session Affinity

Route activities to the worker process that owns the given session. All activities
with the same `session_id` are dispatched to the same worker for as long as the
session is owned. This enables in-memory state reuse across activity invocations.

```rust
// String I/O
fn schedule_activity_on_session(
    &self,
    name: impl Into<String>,
    input: impl Into<String>,
    session_id: impl Into<String>,
) -> DurableFuture<Result<String, String>>

// Typed I/O (with serde)
fn schedule_activity_on_session_typed<In: Serialize, Out: DeserializeOwned>(
    &self,
    name: impl Into<String>,
    input: &In,
    session_id: impl Into<String>,
) -> impl Future<Output = Result<Out, String>>

// Usage:
let session_id = ctx.new_guid().await?;  // Deterministic, replay-safe
let result = ctx.schedule_activity_on_session("RunTurn", &input, &session_id).await?;
```

**Key properties of sessions:**
- **Pure routing** — no ordering guarantees, no transactional semantics
- **Re-claimable** — if the worker dies, another worker can claim the session after lock expiry
- **Implicit lifecycle** — no create/close APIs; sessions are created on first use, cleaned up when idle
- **Independent of activity locks** — an in-flight activity can still complete even if the session was reassigned (like an in-flight network packet surviving a flow table expiry)

See the [Session Affinity Pattern](#session-affinity-pattern) section below for usage patterns.

#### Activity Tags (Worker Specialization)

Tags route activities to specialized worker pools. An orchestration marks an
activity with a tag using `.with_tag()`, and the worker runtime filters its
queue to only dequeue matching tags.

```rust
// Tag an activity for a GPU worker pool
let result = ctx.schedule_activity("RenderFrame", input)
    .with_tag("gpu")
    .await?;

// Tags work with typed activities too
let result: RenderResult = ctx.schedule_activity_typed("RenderFrame", &input)
    .with_tag("gpu")
    .await?;

// Tags can be combined with sessions
let result = ctx.schedule_activity_on_session("CacheLookup", input, &session_id)
    .with_tag("cache-node")
    .await?;
```

**Worker-side configuration:**

```rust
use duroxide::{RuntimeOptions, TagFilter};

// Worker that only processes GPU-tagged activities
let opts = RuntimeOptions {
    worker_tag_filter: TagFilter::tags(["gpu"]),
    ..Default::default()
};

// Worker that processes untagged + GPU-tagged activities
let opts = RuntimeOptions {
    worker_tag_filter: TagFilter::default_and(["gpu"]),
    ..Default::default()
};
```

**`TagFilter` variants:**
| Variant | Behavior |
|---------|----------|
| `TagFilter::DefaultOnly` (default) | Only untagged activities |
| `TagFilter::tags(["a", "b"])` | Only activities tagged `"a"` or `"b"` |
| `TagFilter::default_and(["a"])` | Untagged **or** tagged `"a"` |
| `TagFilter::Any` | All activities regardless of tag (generalist worker) |
| `TagFilter::None` | Never matches (no activities fetched) |

**Key properties:**
- Tags are **persisted in event history** — replays see the exact same tag
- `.with_tag()` is a **mutate-after-emit** pattern that updates the already-emitted action
- Tags are purely a **routing concern** — they don't affect activity behavior
- Tags are **per-activity, not per-orchestration** — different activities in the same orchestration can have different tags
- Maximum 5 distinct tags per `TagFilter` (enforced at construction)

**⚠️ Starvation warning:** If you tag an activity `"gpu"` but no running worker has a matching `TagFilter`, the activity sits in the queue **indefinitely** — no timeout, no error. Protect against this with a `select2` timeout:

```rust
let activity = ctx.schedule_activity("GpuRender", input).with_tag("gpu");
let timeout = ctx.schedule_timer(Duration::from_secs(30));

match ctx.select2(activity, timeout).await {
    Either2::First(Ok(result)) => Ok(result),
    Either2::First(Err(e)) => Err(e),
    Either2::Second(()) => Err("No GPU worker available".to_string()),
}
```

**⚠️ Replay determinism:** Changing, adding, or removing a `.with_tag()` call in orchestration code will cause a **nondeterminism error** on replay of any in-flight instance whose history was recorded with the old tag. This follows the same rules as renaming activities — the tag is part of the event identity.

#### Scheduling Activities with Retry

```rust
// With retry policy (string I/O)
async fn schedule_activity_with_retry(
    &self,
    name: impl Into<String>,
    input: impl Into<String>,
    policy: RetryPolicy,
) -> Result<String, String>

// Typed variant
async fn schedule_activity_with_retry_typed<In, Out>(
    &self,
    name: impl Into<String>,
    input: &In,
    policy: RetryPolicy,
) -> Result<Out, String>

// Usage:
use duroxide::{RetryPolicy, BackoffStrategy};

// Simple: 3 attempts with default exponential backoff
let result = ctx.schedule_activity_with_retry("Task", input, RetryPolicy::new(3)).await?;

// Custom: 5 attempts, fixed 1s backoff, 30s per-attempt timeout
let policy = RetryPolicy::new(5)
    .with_timeout(Duration::from_secs(30))
    .with_backoff(BackoffStrategy::Fixed { delay: Duration::from_secs(1) });
let result = ctx.schedule_activity_with_retry("Task", input, policy).await?;
```

#### Scheduling Activities with Retry on Session

Combines retry semantics with session affinity — all retry attempts are pinned
to the same worker session.

```rust
// String I/O
async fn schedule_activity_with_retry_on_session(
    &self,
    name: impl Into<String>,
    input: impl Into<String>,
    policy: RetryPolicy,
    session_id: impl Into<String>,
) -> Result<String, String>

// Typed variant
async fn schedule_activity_with_retry_on_session_typed<In, Out>(
    &self,
    name: impl Into<String>,
    input: &In,
    policy: RetryPolicy,
    session_id: impl Into<String>,
) -> Result<Out, String>

// Usage:
let session = ctx.new_guid().await?;
let policy = RetryPolicy::new(3)
    .with_backoff(BackoffStrategy::Fixed { delay: Duration::from_secs(1) });
let result = ctx.schedule_activity_with_retry_on_session(
    "RunQuery", "SELECT 1", policy, &session,
).await?;
```

#### Scheduling Timers

```rust
// Delay as Duration
fn schedule_timer(&self, delay: Duration) -> impl Future<Output = ()>

// Usage:
ctx.schedule_timer(Duration::from_secs(5)).await;  // Wait 5 seconds
ctx.schedule_timer(Duration::from_secs(60)).await;  // Wait 1 minute
```

#### External Events (Ephemeral)

```rust
// Wait for external signal by name (one-shot, positional matching)
fn schedule_wait(&self, name: impl Into<String>) -> impl Future<Output = String>

// Usage:
let approval_data = ctx.schedule_wait("ApprovalEvent").await;

// Typed version
let approval: ApprovalData = ctx.schedule_wait_typed("ApprovalEvent").await;
```

#### Event Queues (Persistent/FIFO)

Durable FIFO queues that survive `continue_as_new` boundaries. Unlike ephemeral
events, queue messages are consumed in order and don't require a matching
subscription before the event is raised.

> **Important:** Events must be enqueued *after* the orchestration has started.
> Events enqueued before `start_orchestration` is called may be dropped by the
> provider (orphan message semantics). Once the orchestration is running, queued
> events are buffered until consumed — even if no `dequeue_event` call is active.

```rust
// Dequeue next message from a named queue (blocks until available)
fn dequeue_event(&self, queue: impl Into<String>) -> impl Future<Output = String>

// Typed variant (auto-deserializes)
fn dequeue_event_typed<T: DeserializeOwned>(&self, queue: impl Into<String>) -> impl Future<Output = T>

// Usage:
let msg = ctx.dequeue_event("inbox").await;

// Typed:
let msg: ChatMessage = ctx.dequeue_event_typed("inbox").await;
```

**Queue vs Ephemeral events:**

| Feature | `schedule_wait` | `dequeue_event` |
|---------|-----------------|------------------|
| Semantics | Positional (Nth wait matches Nth raise) | FIFO queue |
| Survives CAN | No | Yes |
| Pre-subscription required | Yes | No |
| Pre-start messages | N/A | Dropped (enqueue only after orchestration starts) |
| Use case | One-shot signals, approvals | Chat messages, command streams |

#### Custom Status

Publish progress or structured state visible to external clients via polling.

```rust
// Set custom status (string payload, typically JSON)
fn set_custom_status(&self, status: impl Into<String>)

// Clear custom status back to None
fn reset_custom_status(&self)

// Read current custom status (reflects all set/reset calls across turns)
fn get_custom_status(&self) -> Option<String>

// Usage:
ctx.set_custom_status(&serde_json::to_string(&MyProgress { pct: 50 }).unwrap());
```

Custom status changes are recorded as `CustomStatusUpdated` history events, making them
durable and replayable. The status survives `continue_as_new` — the last value is
carried forward to the new execution. Clients read it via `wait_for_status_change()`
(see Client API).

#### KV Store (Durable Key-Value State)

Store per-instance key-value pairs that survive replay, `continue_as_new`, and are
visible to external clients. Use for progress tracking, configuration, or cross-instance
communication.

```rust
// Set a value (fire-and-forget, no await needed)
ctx.set_value("progress", "step_3_of_5");

// Set with automatic JSON serialization
ctx.set_value_typed("config", &MyConfig { retries: 3 });

// Read local KV state (replay-safe, returns current snapshot)
let val: Option<String> = ctx.get_value("progress");

// Read with automatic deserialization
let config: Option<MyConfig> = ctx.get_value_typed::<MyConfig>("config").unwrap();

// Read KV from another running instance (cross-instance, requires await)
let other_val = ctx.get_value_from_instance("other-instance-id", "status").await?;

// Clear a single key
ctx.clear_value("progress");

// Clear all keys for this instance
ctx.clear_all_values();
```

**Behavior:**
- KV operations (`set_value`, `clear_value`, `clear_all_values`) are fire-and-forget — they don't suspend the orchestration
- Values survive `continue_as_new` (carried to the new execution)
- Values are visible to external clients via `client.get_value(instance, key)`
- Local reads (`get_value`) return the current in-memory snapshot (replay-safe)
- Cross-instance reads (`get_value_from_instance`) are async and hit the provider
- **Limits:** Max 10 keys per instance (`MAX_KV_KEYS`), max 16KB per value (`MAX_KV_VALUE_BYTES`)
- Exceeding limits fails the orchestration turn

#### Sub-Orchestrations

```rust
// Start child orchestration
fn schedule_sub_orchestration(&self, name: impl Into<String>, input: impl Into<String>) 
    -> impl Future<Output = Result<String, String>>

// Usage:
let child_result = ctx.schedule_sub_orchestration("ChildWorkflow", input_json).await?;

// Typed version
let child_result: ChildOutput = ctx.schedule_sub_orchestration_typed("ChildWorkflow", &child_input).await?;
```

#### Detached Orchestrations (Fire-and-Forget)

```rust
// Start orchestration without waiting for result
fn schedule_orchestration(&self, name: impl Into<String>, instance: impl Into<String>, input: impl Into<String>)

// Usage:
ctx.schedule_orchestration("EmailNotification", "email-123", email_json);
// Returns immediately, doesn't wait for completion
```

#### Composition (Select/Join)

```rust
// Select: first to complete wins, returns Either2 or Either3
async fn select2<T1, T2, F1, F2>(&self, f1: F1, f2: F2) -> Either2<T1, T2>
async fn select3<T1, T2, T3, F1, F2, F3>(&self, f1: F1, f2: F2, f3: F3) -> Either3<T1, T2, T3>

// Join: wait for all
async fn join<T, F>(&self, futures: Vec<F>) -> Vec<T>
async fn join3<T1, T2, T3, F1, F2, F3>(&self, f1: F1, f2: F2, f3: F3) -> (T1, T2, T3)

// Usage:
let timer = ctx.schedule_timer(Duration::from_secs(30));
let approval = ctx.schedule_wait("Approval");

match ctx.select2(timer, approval).await {
    Either2::First(()) => println!("Timed out"),
    Either2::Second(data) => println!("Approved: {}", data),
}

// Join example
let f1 = ctx.schedule_activity("Task1", "a");
let f2 = ctx.schedule_activity("Task2", "b");
let f3 = ctx.schedule_activity("Task3", "c");
let results = ctx.join(vec![f1, f2, f3]).await;  // Wait for all 3
```

#### Continue As New

```rust
// End current execution, start fresh with new input
// Returns a future that never resolves - use with `return` to terminate
fn continue_as_new(&self, input: impl Into<String>) -> impl Future<Output = Result<String, String>>

// Usage:
async fn pagination(ctx: OrchestrationContext, cursor: String) -> Result<String, String> {
    let next_cursor = ctx.schedule_activity("ProcessPage", cursor).await?;
    
    if next_cursor == "EOF" {
        Ok("done".to_string())
    } else {
        return ctx.continue_as_new(next_cursor).await;  // Terminates execution
    }
}
```

**Important:** `continue_as_new()` returns a future that never resolves. You must `await` it and use `return` to ensure code after is unreachable. The compiler will warn about unreachable code if you forget `return`.

Note: Each Continue As New starts a new execution with an empty history. The runtime stamps a fresh `OrchestrationStarted { event_id: 1 }` for the new execution; activities, timers, and external events always target the currently active execution only.

#### Logging (Replay-Safe)

```rust
fn trace_info(&self, message: impl Into<String>)
fn trace_warn(&self, message: impl Into<String>)
fn trace_error(&self, message: impl Into<String>)
fn trace_debug(&self, message: impl Into<String>)

// Usage:
ctx.trace_info("Processing order 12345");
ctx.trace_warn(format!("Retry attempt {}", attempt));
ctx.trace_error("Payment failed");
```

> **Default log targets**
> The runtime installs a filter equivalent to `error,duroxide::orchestration={level},duroxide::activity={level}` (where `{level}` comes from `ObservabilityConfig.log_level`). This keeps stdout focused on orchestration and activity trace helpers. To opt into additional targets—such as the internal runtime dispatcher logs—override `RUST_LOG` before starting your app:
>
> ```bash
> RUST_LOG=error,duroxide::orchestration=info,duroxide::activity=info,duroxide::runtime=debug cargo run --example with_observability
> ```
>
> You can add any other targets in the same way (for example `duroxide::providers::sqlite=trace`). Setting `RUST_LOG` always takes precedence over the runtime defaults.

#### System Calls (Deterministic Non-Determinism)

```rust
// Generate deterministic GUID
async fn new_guid(&self) -> Result<String, String>

// Get deterministic UTC timestamp
async fn utc_now(&self) -> Result<std::time::SystemTime, String>

// Usage:
let correlation_id = ctx.new_guid().await?;
let created_at = ctx.utc_now().await?;
let created_at_ms = created_at
    .duration_since(std::time::UNIX_EPOCH)
    .map_err(|e| e.to_string())?
    .as_millis() as u64;
```

---

## Client API Reference

The `Client` provides control-plane operations for managing orchestration instances. It communicates with the runtime through the shared `Provider` (no direct coupling).

### Creating a Client

```rust
use duroxide::Client;
use duroxide::providers::sqlite::SqliteProvider;
use std::sync::Arc;

let store = Arc::new(SqliteProvider::new("sqlite:./data.db", None).await?);
let client = Client::new(store);
```

**Key Points:**
- Client shares the same Provider instance as the Runtime
- Client is `Clone` and thread-safe
- Can be used from any process, even without a running Runtime

### Core Operations

#### Start an Orchestration

```rust
// Basic string input
client.start_orchestration("order-123", "ProcessOrder", r#"{"customer_id": "c1"}"#).await?;

// With explicit version
client.start_orchestration_versioned("order-456", "ProcessOrder", "2.0.0", "{}").await?;

// With typed input
#[derive(Serialize)]
struct OrderInput {
    customer_id: String,
    amount: f64,
}

let input = OrderInput {
    customer_id: "c1".to_string(),
    amount: 99.99,
};
client.start_orchestration_typed("order-789", "ProcessOrder", &input).await?;
```

**Behavior:**
- Enqueues a `StartOrchestration` work item
- Runtime picks it up and begins execution
- Returns immediately (non-blocking)
- Use `wait_for_orchestration()` to wait for completion

#### Raise External Event

```rust
// String payload
client.raise_event("order-123", "PaymentReceived", r#"{"amount": 99.99}"#).await?;

// Typed payload
#[derive(Serialize)]
struct PaymentEvent {
    transaction_id: String,
    amount: f64,
}

let event = PaymentEvent {
    transaction_id: "tx-456".to_string(),
    amount: 99.99,
};
client.raise_event_typed("order-123", "PaymentReceived", &event).await?;
```

**Use Cases:**
- Human approvals
- Webhook callbacks
- External system notifications
- Timer-based triggers from external schedulers

#### Enqueue Event (Queue Semantics)

```rust
// Send a message into an orchestration's named queue
client.enqueue_event("chat-42", "inbox", r#"{"text": "Hello"}"#).await?;

// Typed variant (auto-serializes)
client.enqueue_event_typed("chat-42", "inbox", &ChatMessage { text: "Hello".into() }).await?;
```

**Use Cases:**
- Chat / conversational orchestrations
- Command streams to long-running orchestrations
- Any pattern where messages arrive before the orchestration subscribes

#### Poll Custom Status

```rust
let mut version = 0u64;
loop {
    match client.wait_for_status_change("order-123", version, poll_interval, timeout).await? {
        OrchestrationStatus::Running { custom_status: Some(s), custom_status_version: v, .. } => {
            version = v;
            let progress: MyProgress = serde_json::from_str(&s).unwrap();
            println!("Progress: {}%", progress.pct);
        }
        OrchestrationStatus::Completed { .. } => break,
        _ => break,
    }
}
```

**Tip:** `wait_for_status_change` blocks until the status version advances past
`last_seen_version`, making it efficient for progress polling without tight loops.

#### Cancel an Orchestration

```rust
client.cancel_instance("order-123", "Customer requested cancellation").await?;
```

**Behavior:**
- Enqueues a `CancelInstance` work item
- Runtime processes it on next turn
- Orchestration moves to `Failed` status with cancellation error
- **Cascading cancellation**: Child sub-orchestrations are also cancelled
- **Activity cancellation**: In-flight activities receive cancellation signal (see [Cooperative Activity Cancellation](#cooperative-activity-cancellation))

**What happens to in-flight activities:**
1. The activity's `ActivityContext` receives the cancellation signal
2. Cooperative activities can check `ctx.is_cancelled()` and clean up gracefully
3. The runtime waits up to `activity_cancellation_grace_period` (default 30s) for the activity to finish
4. If the activity doesn't finish in time, it's forcibly aborted
5. The activity result (success or failure) is **dropped** since the orchestration is already terminal

**Example with graceful cleanup:**
```rust
// Activity that responds to cancellation
activities.register("ProcessBatch", |ctx: ActivityContext, batch: String| async move {
    let items: Vec<Item> = serde_json::from_str(&batch)?;
    
    for (i, item) in items.iter().enumerate() {
        // Check cancellation between items
        if ctx.is_cancelled() {
            ctx.trace_info(format!("Cancelled after processing {} of {} items", i, items.len()));
            return Err("Batch processing cancelled".to_string());
        }
        process_item(item).await?;
    }
    
    Ok(format!("Processed {} items", items.len()))
});
```

#### Check Status

```rust
use duroxide::OrchestrationStatus;

let status = client.get_orchestration_status("order-123").await?; // Returns Result<OrchestrationStatus, ClientError>

match status {
    OrchestrationStatus::Running { .. } => println!("Still processing..."),
    OrchestrationStatus::Completed { output, .. } => println!("Done: {}", output),
    OrchestrationStatus::Failed { details, .. } => {
        eprintln!("Failed: {}", details.display_message());
        eprintln!("Category: {}", details.category());
    }
    OrchestrationStatus::NotFound => println!("Instance doesn't exist"),
}
```

**Returns:**
- `Running` - Orchestration in progress
- `Completed { output }` - Successful completion with result
- `Failed { details }` - Failed with error details
- `NotFound` - Instance doesn't exist or no history

#### Wait for Completion

```rust
use std::time::Duration;

// Wait up to 30 seconds
let status = client.wait_for_orchestration("order-123", Duration::from_secs(30)).await?;

match status {
    OrchestrationStatus::Completed { output, .. } => {
        println!("Success: {}", output);
    }
    OrchestrationStatus::Failed { details, .. } => {
        eprintln!("Failed: {}", details.display_message());
    }
    _ => unreachable!("wait_for_orchestration only returns terminal states"),
}

// Typed output
#[derive(Deserialize)]
struct OrderResult {
    order_id: String,
    status: String,
}

let result: OrderResult = client
    .wait_for_orchestration_typed("order-123", Duration::from_secs(30))
    .await?;
```

**Behavior:**
- Polls status with exponential backoff (5ms → 100ms)
- Returns when terminal state reached or timeout
- Only returns `Completed` or `Failed`, never `Running` or `NotFound`

#### Read KV Values

Read durable key-value state from any orchestration instance, even while running.

```rust
// Read a single KV value
let val: Option<String> = client.get_value("order-123", "status").await?;

// Read with automatic deserialization
let config: MyConfig = client.get_value_typed::<MyConfig>("order-123", "config").await?
    .expect("config should exist");

// Poll until a value appears (with timeout)
let val = client.wait_for_value("order-123", "ready_flag", Duration::from_secs(10)).await?;

// Poll with typed deserialization
let result: MyResult = client
    .wait_for_value_typed::<MyResult>("order-123", "result", Duration::from_secs(30))
    .await?
    .expect("result should exist");
```

**Behavior:**
- `get_value` reads directly from the provider (no history replay needed)
- `wait_for_value` polls with exponential backoff until the key exists or timeout
- Values survive `continue_as_new` and are readable after orchestration completion
- Returns `None` for nonexistent keys or instances

### Management Operations (Optional)

These methods require a provider that implements `ProviderAdmin`. Check availability first:

```rust
if client.has_management_capability() {
    // Management operations available
} else {
    // Provider doesn't support management
}
```

#### List Instances

```rust
// List all instances
let instances = client.list_all_instances().await?;
for instance in instances {
    println!("Instance: {}", instance);
}

// Filter by status
let completed = client.list_instances_by_status("Completed").await?;
let running = client.list_instances_by_status("Running").await?;
let failed = client.list_instances_by_status("Failed").await?;
```

#### Get Instance Metadata

```rust
let info = client.get_instance_info("order-123").await?;

println!("Instance: {}", info.instance_id);
println!("Orchestration: {} v{}", info.orchestration_name, info.orchestration_version);
println!("Status: {}", info.status);
println!("Current Execution: {}", info.current_execution_id);
if let Some(output) = info.output {
    println!("Output: {}", output);
}
```

#### Inspect Executions

```rust
// List all execution IDs for an instance
let executions = client.list_executions("order-123").await?;
println!("Executions: {:?}", executions);  // [1, 2, 3] if ContinueAsNew used

// Get execution metadata
let exec_info = client.get_execution_info("order-123", 1).await?;
println!("Status: {}", exec_info.status);
println!("Events: {}", exec_info.event_count);

// Read execution history
let history = client.read_execution_history("order-123", 1).await?;
for event in history {
    println!("Event: {:?}", event);
}
```

#### System Metrics

```rust
let metrics = client.get_system_metrics().await?;
println!("Total instances: {}", metrics.total_instances);
println!("Running: {}", metrics.running_instances);
println!("Completed: {}", metrics.completed_instances);
println!("Failed: {}", metrics.failed_instances);

let queues = client.get_queue_depths().await?;
println!("Orchestrator queue: {}", queues.orchestrator_queue);
println!("Worker queue: {}", queues.worker_queue);
```

### Client Usage Patterns

#### Fire-and-Forget

```rust
// Start orchestration and don't wait
client.start_orchestration("background-job-1", "ProcessData", "input").await?;
// Continue with other work immediately
```

#### Synchronous Wait

```rust
// Start and wait for completion
client.start_orchestration("sync-job-1", "ProcessData", "input").await?;
let status = client.wait_for_orchestration("sync-job-1", Duration::from_secs(30)).await?;
```

#### Polling for Progress

```rust
client.start_orchestration("long-job-1", "ProcessData", "input").await?;

loop {
    let status = client.get_orchestration_status("long-job-1").await?; // Returns Result<OrchestrationStatus, ClientError>
    match status {
        OrchestrationStatus::Running { .. } => {
            println!("Still running...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        OrchestrationStatus::Completed { output, .. } => {
            println!("Completed: {}", output);
            break;
        }
        OrchestrationStatus::Failed { details, .. } => {
            eprintln!("Failed: {}", details.display_message());
            break;
        }
        _ => break,
    }
}
```

#### Human-in-the-Loop

```rust
// In your orchestration:
async fn approval_workflow(ctx: OrchestrationContext, order_id: String) -> Result<String, String> {
    let result = ctx.schedule_activity("SubmitForApproval", order_id).await?;
    
    // Wait for approval event
    let approval = ctx.schedule_wait("ManagerApproval").await;
    
    Ok(approval)
}

// External system receives approval and raises event:
async fn handle_approval(client: &Client, order_id: &str, approved: bool) {
    let event_data = serde_json::json!({
        "approved": approved,
        "approver": "manager@company.com",
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });
    
    client.raise_event(
        &format!("order-{}", order_id),
        "ManagerApproval",
        event_data.to_string()
    ).await.unwrap();
}
```

#### Dashboard / Monitoring

```rust
async fn show_dashboard(client: &Client) {
    if !client.has_management_capability() {
        println!("Management features not available");
        return;
    }
    
    // System overview
    let metrics = client.get_system_metrics().await.unwrap();
    println!("=== System Status ===");
    println!("Total: {} | Running: {} | Completed: {} | Failed: {}",
        metrics.total_instances,
        metrics.running_instances,
        metrics.completed_instances,
        metrics.failed_instances
    );
    
    // Queue health
    let queues = client.get_queue_depths().await.unwrap();
    println!("\n=== Queue Depths ===");
    println!("Orchestrator: {} | Worker: {}", 
        queues.orchestrator_queue, 
        queues.worker_queue
    );
    
    // Recent failures
    let failed = client.list_instances_by_status("Failed").await.unwrap();
    println!("\n=== Recent Failures ===");
    for instance in failed.iter().take(10) {
        let info = client.get_instance_info(instance).await.unwrap();
        println!("{}: {} ({})", instance, info.orchestration_name, info.status);
    }
}
```

### Error Handling

```rust
use duroxide::OrchestrationStatus;

match client.start_orchestration("test", "MyOrch", "input").await {
    Ok(()) => println!("Started successfully"),
    Err(e) => eprintln!("Failed to start: {}", e),
}

// Wait with timeout handling
match client.wait_for_orchestration("test", Duration::from_secs(10)).await {
    Ok(OrchestrationStatus::Completed { output, .. }) => println!("Done: {}", output),
    Ok(OrchestrationStatus::Failed { details, .. }) => {
        eprintln!("Orchestration failed: {}", details.display_message());
        
        // Check error category for handling
        match details.category() {
            "application" => {
                // Business logic error - may want to retry with different input
            }
            "configuration" => {
                // Nondeterminism detected - requires code fix
            }
            "infrastructure" => {
                // Provider/database error - may be transient
                // Note: Poison errors (unregistered handlers after backoff) also fall here
            }
            _ => {}
        }
    }
    Err(e) => eprintln!("Timeout or error: {:?}", e),
    _ => {}
}
```

### Client API Summary

| Method | Purpose | Returns |
|--------|---------|---------|
| `start_orchestration()` | Start new instance | `Result<(), ClientError>` |
| `raise_event()` | Send ephemeral external event | `Result<(), ClientError>` |
| `enqueue_event()` | Send queue message (FIFO) | `Result<(), ClientError>` |
| `cancel_instance()` | Request cancellation | `Result<(), ClientError>` |
| `get_orchestration_status()` | Check status | `Result<OrchestrationStatus, ClientError>` |
| `wait_for_orchestration()` | Wait for completion | `Result<OrchestrationStatus, ClientError>` |
| `wait_for_status_change()` | Poll custom status | `Result<OrchestrationStatus, ClientError>` |
| `get_value()` | Read KV value | `Result<Option<String>, ClientError>` |
| `get_value_typed()` | Read KV value (deserialized) | `Result<Option<T>, ClientError>` |
| `wait_for_value()` | Poll for KV value | `Result<Option<String>, ClientError>` |
| `wait_for_value_typed()` | Poll for KV value (deserialized) | `Result<Option<T>, ClientError>` |
| `has_management_capability()` | Check feature availability | `bool` |
| `list_all_instances()` | List instances | `Result<Vec<String>, ClientError>` |
| `list_instances_by_status()` | Filter by status | `Result<Vec<String>, ClientError>` |
| `get_instance_info()` | Instance metadata | `Result<InstanceInfo, ClientError>` |
| `get_execution_info()` | Execution metadata | `Result<ExecutionInfo, ClientError>` |
| `list_executions()` | List execution IDs | `Result<Vec<u64>, ClientError>` |
| `read_execution_history()` | Read history | `Result<Vec<Event>, ClientError>` |
| `get_system_metrics()` | System stats | `Result<SystemMetrics, ClientError>` |
| `get_queue_depths()` | Queue depths | `Result<QueueDepths, ClientError>` |

---

## Common Patterns

### Session Affinity Pattern

Use `schedule_activity_on_session` when you need multiple activities to run on the
same worker process — typically to share in-memory state like caches, connection pools,
or loaded ML models.

```rust
async fn copilot_conversation(ctx: OrchestrationContext, config: String) -> Result<String, String> {
    // Generate a deterministic session ID (replay-safe)
    let session_id = ctx.new_guid().await?;
    
    // First turn: hydrates session state on the claiming worker
    ctx.schedule_activity_on_session("HydrateSession", &config, &session_id).await?;
    
    // Subsequent turns: routed to the same worker, reusing in-memory state
    loop {
        let user_input = ctx.schedule_wait("user_message").await;
        
        let response = ctx.schedule_activity_on_session(
            "RunTurn", &user_input, &session_id
        ).await?;
        
        if response == "done" {
            break;
        }
    }
    
    Ok("conversation complete".to_string())
}
```

**Worker side — managing session state:**
```rust
// Use an LRU cache keyed by session_id
static SESSION_CACHE: LazyLock<Mutex<LruCache<String, SessionState>>> = ...;

.register("RunTurn", |ctx: ActivityContext, input: String| async move {
    let session_id = ctx.session_id().expect("must be session-bound");
    let mut cache = SESSION_CACHE.lock().unwrap();
    let state = cache.get_mut(session_id)
        .ok_or("unknown_session — needs re-hydration")?;
    
    let result = state.process(&input).await;
    Ok(result)
})
```

**Session migration handling:**
When a session migrates (worker death, idle timeout), the new worker won't have the
in-memory state. Handle this reactively:

```rust
match ctx.schedule_activity_on_session("RunTurn", &input, &session_id).await {
    Ok(result) => { /* success */ }
    Err(e) if e.contains("unknown_session") => {
        // Re-hydrate on the new worker
        ctx.schedule_activity_on_session("HydrateSession", &config, &session_id).await?;
        ctx.schedule_activity_on_session("RunTurn", &input, &session_id).await?;
    }
    Err(e) => return Err(e),
}
```

**Rolling upgrade caveat:** Session-bound activities are pinned to the owning worker
until the session unpins. During rolling upgrades, ensure all workers have the session
activity handlers registered before orchestrations schedule session activities.

### 1. Function Chaining (Sequential Steps)

```rust
async fn process_order(ctx: OrchestrationContext, order_json: String) -> Result<String, String> {
    ctx.trace_info("Starting order processing");
    
    // Step 1: Validate
    let validated = ctx.schedule_activity("ValidateOrder", order_json)
        .await?;
    
    // Step 2: Charge payment
    let payment_result = ctx.schedule_activity("ChargePayment", validated)
        .await?;
    
    // Step 3: Ship order
    let tracking = ctx.schedule_activity("ShipOrder", payment_result)
        .await?;
    
    // Step 4: Send confirmation
    let confirmation = ctx.schedule_activity("SendConfirmation", tracking)
        .await?;
    
    ctx.trace_info("Order processing completed");
    Ok(confirmation)
}
```

### 2. Fan-Out/Fan-In (Parallel Processing)

```rust
async fn process_users(ctx: OrchestrationContext, user_ids_json: String) -> Result<String, String> {
    // Parse input
    let user_ids: Vec<String> = serde_json::from_str(&user_ids_json)?;
    
    // Fan-out: Schedule all tasks in parallel
    let futures: Vec<_> = user_ids
        .into_iter()
        .map(|user_id| ctx.schedule_activity("ProcessUser", user_id))
        .collect();
    
    // Fan-in: Wait for all to complete (deterministic order)
    let results = ctx.join(futures).await;
    
    // Process results - each result is Result<String, String>
    let mut successes = 0;
    for result in results {
        match result {
            Ok(_) => successes += 1,
            Err(e) => {
                ctx.trace_warn(format!("User processing failed: {}", e));
            }
        }
    }
    
    Ok(format!("Processed {} users successfully", successes))
}
```

### 3. Human-in-the-Loop with Timeout

```rust
async fn approval_workflow(ctx: OrchestrationContext, request_json: String) -> Result<String, String> {
    // Submit approval request
    let request_id = ctx.schedule_activity("SubmitApprovalRequest", request_json)
        .await?;
    
    // Wait for approval or timeout
    let timeout = ctx.schedule_timer(std::time::Duration::from_millis(86_400_000));  // 24 hours
    let approval = ctx.schedule_wait("ApprovalEvent");
    
    match ctx.select2(timeout, approval).await {
        Either2::First(()) => {
            // Timed out - escalate
            ctx.trace_warn("Approval timed out, escalating");
            ctx.schedule_activity("EscalateApproval", request_id)
                .await
        }
        Either2::Second(approval_data) => {
            // Approved - use the data
            ctx.trace_info("Approval received");
            Ok(approval_data)
        }
    }
}
```

### 4. Error Handling and Compensation (Saga Pattern)

```rust
async fn saga_orchestration(ctx: OrchestrationContext, order_json: String) -> Result<String, String> {
    // Step 1: Reserve inventory
    let reservation = match ctx.schedule_activity("ReserveInventory", order_json.clone())
        .await 
    {
        Ok(r) => r,
        Err(e) => {
            ctx.trace_error(format!("Reservation failed: {}", e));
            return Err("Reservation failed".to_string());
        }
    };
    
    // Step 2: Charge payment
    match ctx.schedule_activity("ChargePayment", reservation.clone())
        .await 
    {
        Ok(payment) => Ok(payment),
        Err(e) => {
            // Compensation: Release inventory
            ctx.trace_warn(format!("Payment failed: {}, releasing inventory", e));
            ctx.schedule_activity("ReleaseInventory", reservation)
                .await?;
            Err("Payment failed, order cancelled".to_string())
        }
    }
}
```

### 5. Retry with Backoff

**Preferred approach** - Use the built-in `schedule_activity_with_retry()`:

```rust
use duroxide::{RetryPolicy, BackoffStrategy};

async fn retry_orchestration(ctx: OrchestrationContext, task_input: String) -> Result<String, String> {
    // Simple: 3 attempts with default exponential backoff
    let result = ctx.schedule_activity_with_retry(
        "UnreliableTask",
        task_input,
        RetryPolicy::new(3),
    ).await?;
    
    Ok(result)
}

// Custom policy with timeout and fixed backoff
async fn custom_retry(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    let policy = RetryPolicy::new(5)
        .with_timeout(std::time::Duration::from_secs(30))  // Per-attempt timeout
        .with_backoff(BackoffStrategy::Exponential {
            base: std::time::Duration::from_millis(100),
            multiplier: 2.0,
            max: std::time::Duration::from_secs(10),
        });
    
    ctx.schedule_activity_with_retry("Task", input, policy).await
}
```

**Backoff strategies available:**
- `BackoffStrategy::None` - No delay between retries
- `BackoffStrategy::Fixed { delay }` - Same delay every retry
- `BackoffStrategy::Linear { base, max }` - delay = base × attempt
- `BackoffStrategy::Exponential { base, multiplier, max }` - delay = base × multiplier^(attempt-1)

**Behavior notes:**
- Activity errors trigger retries (with backoff)
- Timeouts exit immediately (no retry)
- A warning trace is logged on each retry attempt

**Manual retry loop** (for advanced customization):

```rust
async fn manual_retry(ctx: OrchestrationContext, task_input: String) -> Result<String, String> {
    let max_attempts = 5;
    let mut delay = std::time::Duration::from_secs(1);
    
    for attempt in 1..=max_attempts {
        ctx.trace_info(format!("Attempt {} of {}", attempt, max_attempts));
        
        match ctx.schedule_activity("UnreliableTask", task_input.clone())
            .await 
        {
            Ok(result) => return Ok(result),
            Err(e) => {
                ctx.trace_warn(format!("Attempt {} failed: {}", attempt, e));
                if attempt < max_attempts {
                    ctx.schedule_timer(delay).await;
                    delay *= 2;
                } else {
                    return Err(format!("Failed after {} attempts", max_attempts));
                }
            }
        }
    }
    unreachable!()
}
```

### 6. Understanding Error Types

Duroxide classifies errors into three categories for proper handling and metrics:

**Application Errors** - Business logic failures that your orchestration code sees and handles:
```rust
async fn order_workflow(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    match ctx.schedule_activity("ProcessPayment", input).await {
        Ok(txn_id) => Ok(format!("paid:{txn_id}")),
        Err(e) => {
            // This is an Application error - you handle it
            // Examples: "insufficient_funds", "invalid_card"
            ctx.trace_warn(format!("Payment failed: {e}"));
            Err(format!("payment_error:{e}"))
        }
    }
}
```

**Configuration Errors** - Deployment issues that abort orchestration execution before your code runs:
- **Nondeterminism** (code changed between replays) - Immediate failure
- **Unregistered orchestrations/activities** - Uses exponential backoff, eventually fails as Poison

For unregistered handlers, the runtime automatically backs off to support **rolling deployments**:
- Message is abandoned with exponential backoff (1s → 2s → 4s → ... up to 60s)
- Eventually picked up by a node with the handler registered
- If all retry attempts exhaust, fails as `ErrorDetails::Poison`

Your orchestration code **never sees** these errors. The runtime:
1. Records the failure in history (ActivityFailed + OrchestrationFailed events)
2. Marks the orchestration as failed with ErrorDetails::Configuration (nondeterminism) or ErrorDetails::Poison (unregistered)
3. Requires deployment fix (register missing handler, fix nondeterministic code)

**Infrastructure Errors** - Provider/system failures that abort execution:
- Database connection failures
- Data corruption
- Queue operation failures

These also abort before your code runs, may be retryable by the runtime.

**Checking Error Category** (for metrics/logging):
```rust
match client.get_orchestration_status("inst-1").await? { // Returns Result<OrchestrationStatus, ClientError>
    OrchestrationStatus::Failed { details, .. } => {
        match details.category() {
            "infrastructure" => alert_ops_team(),      // System issue
            "configuration" => alert_dev_team(),       // Deployment issue  
            "application" => log_business_error(),     // Expected failures
        }
    }
    _ => {}
}
```

### 7. Loop and Accumulate

```rust
async fn accumulate_results(ctx: OrchestrationContext, items_json: String) -> Result<String, String> {
    let items: Vec<String> = serde_json::from_str(&items_json)?;
    let mut accumulator = Vec::new();
    
    for (i, item) in items.iter().enumerate() {
        ctx.trace_info(format!("Processing item {} of {}", i + 1, items.len()));
        
        let result = ctx.schedule_activity("ProcessItem", item.clone())
            .await?;
        
        accumulator.push(result);
    }
    
    let summary = serde_json::to_string(&accumulator).unwrap();
    Ok(summary)
}
```

### 7. Conditional Workflow

```rust
async fn conditional_workflow(ctx: OrchestrationContext, order_json: String) -> Result<String, String> {
    // Get order value
    let order_value: f64 = ctx.schedule_activity("GetOrderValue", order_json.clone())
        .await?;
    
    if order_value > 1000.0 {
        // High value - require approval
        ctx.trace_info("High-value order, requiring approval");
        
        let approval_request = ctx.schedule_activity("CreateApprovalRequest", order_json.clone())
            .await?;
        
        let timeout = ctx.schedule_timer(std::time::Duration::from_millis(3600_000));  // 1 hour
        let approval = ctx.schedule_wait("ManagerApproval");
        
        let (winner, _) = ctx.select2(timeout, approval).await;
        if winner == 0 {
            ctx.trace_warn("Approval timeout");
            return Err("Approval timeout".to_string());
        }
    }
    
    // Process the order
    ctx.schedule_activity("ProcessOrder", order_json).await
}
```

### 8. Sub-Orchestrations (Composition)

```rust
async fn parent_workflow(ctx: OrchestrationContext, batch_json: String) -> Result<String, String> {
    let batches: Vec<String> = serde_json::from_str(&batch_json)?;
    
    // Process each batch as a sub-orchestration
    let mut results = Vec::new();
    for batch in batches {
        let result = ctx.schedule_sub_orchestration("ProcessBatch", batch)
            .await?;
        results.push(result);
    }
    
    Ok(serde_json::to_string(&results).unwrap())
}

async fn child_workflow(ctx: OrchestrationContext, batch: String) -> Result<String, String> {
    // Child has its own event history, isolated from parent
    let processed = ctx.schedule_activity("ProcessBatchItems", batch)
        .await?;
    
    ctx.trace_info("Batch processing completed");
    Ok(processed)
}
```

### 9. Continue As New (Long-Running Workflows)

```rust
async fn eternal_monitor(ctx: OrchestrationContext, state_json: String) -> Result<(), String> {
    let state: MonitorState = serde_json::from_str(&state_json)?;
    
    // Do one iteration of work
    let check_result = ctx.schedule_activity("CheckSystem", state_json)
        .await?;
    
    // Wait before next check
    ctx.schedule_timer(std::time::Duration::from_millis(60_000)).await;  // 1 minute
    
    // Continue with updated state - await and return to terminate this execution
    return ctx.continue_as_new(check_result).await;
}
```

---

## Anti-Patterns (What to Avoid)

### ❌ Anti-Pattern 1: Business Logic / Control Flow in Activities

```rust
// WRONG - Activity contains business logic and branching
activities.register("ProcessOrderComplex", |ctx: ActivityContext, order_json: String| async move {
    let order: Order = serde_json::from_str(&order_json)?;
    
    // ❌ Business logic in activity
    if order.total > 1000.0 {
        // Send for approval
        send_approval_request(&order).await?;
        wait_for_approval(&order.id).await?;  // Polling/waiting in activity
    }
    
    // ❌ Control flow in activity
    if order.customer_type == "premium" {
        process_premium(&order).await?;
    } else {
        process_standard(&order).await?;
    }
    
    Ok("processed".to_string())
});
```

```rust
// CORRECT - Business logic in orchestration, activities are single-purpose
// Activities: single-purpose, can sleep/poll internally if needed
activities.register("GetOrderValue", |ctx: ActivityContext, order_json: String| async move {
    let order: Order = serde_json::from_str(&order_json)?;
    Ok(order.total.to_string())
});

activities.register("SendApprovalRequest", |ctx: ActivityContext, order_json: String| async move {
    let order: Order = serde_json::from_str(&order_json)?;
    send_to_approval_system(&order).await?;
    Ok(order.id)
});

activities.register("ProcessPremiumOrder", |ctx: ActivityContext, order_json: String| async move {
    let order: Order = serde_json::from_str(&order_json)?;
    // Activity can poll/sleep internally - that's fine!
    let vm = provision_vm(&order).await?;
    while !vm_ready(&vm.id).await {
        tokio::time::sleep(Duration::from_secs(5)).await;  // ✅ OK in activity
    }
    process_on_vm(&order, &vm).await?;
    Ok("processed".to_string())
});

// Orchestration: contains business logic and control flow
async fn good_orch(ctx: OrchestrationContext, order_json: String) -> Result<String, String> {
    // Get order value
    let value_str = ctx.schedule_activity("GetOrderValue", order_json.clone())
        .await?;
    let value: f64 = value_str.parse().unwrap();
    
    // ✅ Business logic in orchestration
    if value > 1000.0 {
        // Send for approval
        ctx.schedule_activity("SendApprovalRequest", order_json.clone())
            .await?;
        
        // ✅ Orchestration-level timeout control
        let timeout = ctx.schedule_timer(std::time::Duration::from_millis(3600_000));  // 1 hour
        let approval = ctx.schedule_wait("ApprovalEvent");
        let (winner, _) = ctx.select2(timeout, approval).await;
        
        if winner == 0 {
            return Err("Approval timeout".to_string());
        }
    }
    
    // ✅ Control flow in orchestration
    let customer_type = ctx.schedule_activity("GetCustomerType", order_json.clone())
        .await?;
    
    if customer_type == "premium" {
        ctx.schedule_activity("ProcessPremiumOrder", order_json).await
    } else {
        ctx.schedule_activity("ProcessStandardOrder", order_json).await
    }
}
```

**Why:** 
- Activities are **execution units** - they do ONE thing well
- Orchestrations are **coordination units** - they contain business logic and control flow
- Control flow in activities breaks composability and replay benefits
- An activity CAN sleep/poll internally (e.g., provisioning a VM) - that's perfectly fine!
- What matters: keep activities single-purpose, pull multi-step logic into orchestrations

### ❌ Anti-Pattern 2: Non-Deterministic Branching

```rust
// WRONG - Random branching
async fn bad_orch(ctx: OrchestrationContext) -> Result<String, String> {
    if rand::random::<bool>() {  // Different on each replay!
        ctx.schedule_activity("PathA", "").await
    } else {
        ctx.schedule_activity("PathB", "").await
    }
}
```

```rust
// CORRECT - Deterministic branching based on activity result
async fn good_orch(ctx: OrchestrationContext) -> Result<String, String> {
    let decision = ctx.schedule_activity("MakeDecision", "").await?;
    
    if decision == "A" {
        ctx.schedule_activity("PathA", "").await
    } else {
        ctx.schedule_activity("PathB", "").await
    }
}
```

### ❌ Anti-Pattern 3: Using tokio::select! or tokio::join!
    let result = ctx.schedule_activity("Task", "input").await?;
    Ok(result)
}
```

### ❌ Anti-Pattern 4: Using Activity for Pure Delays/Timeouts

```rust
// WRONG - Activity that only sleeps (no business logic)
activities.register("Wait30Seconds", |ctx: ActivityContext, _: String| async move {
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    Ok("done".to_string())
});

async fn bad_delay(ctx: OrchestrationContext) -> Result<String, String> {
    ctx.schedule_activity("Wait30Seconds", "").await?;
    // Just wasted a worker slot for 30 seconds!
    Ok("done".to_string())
}
```

```rust
// CORRECT - Use timer for pure delays
async fn good_delay(ctx: OrchestrationContext) -> Result<String, String> {
    ctx.schedule_timer(std::time::Duration::from_millis(30_000)).await;
    // Timer doesn't block workers, handled by timer dispatcher
    Ok("done".to_string())
}
```

**Why:**
- If the ONLY purpose is to delay, use `schedule_timer()` - it's designed for that
- Activities are for doing work (DB, API, computation), not just waiting
- **BUT:** It's perfectly fine for an activity to sleep/poll AS PART of doing work:
  ```rust
  // ✅ FINE - Activity polls as part of provisioning work
  activities.register("ProvisionVM", |ctx: ActivityContext, config: String| async move {
      let vm_id = start_vm_creation(config).await?;
      
      // Polling is part of the provisioning logic
      while !vm_ready(&vm_id).await {
          tokio::time::sleep(Duration::from_secs(5)).await;  // ✅ OK - part of work
      }
      
      Ok(vm_id)
  });
  ```
- The rule: Use timers for orchestration-level delays, not for activity-internal polling

### ❌ Anti-Pattern 5: Infinite Loop Without Delay

```rust
// WRONG - Infinite loop hammering activities
async fn bad_poll(ctx: OrchestrationContext, task_id: String) -> Result<String, String> {
    loop {
        let status = ctx.schedule_activity("CheckStatus", task_id.clone())
            .await?;
        
        if status == "complete" {
            return Ok(status);
        }
        // No delay - creates unlimited activities!
    }
}
```

```rust
// CORRECT - Loop with timer delays
async fn good_poll(ctx: OrchestrationContext, task_id: String) -> Result<String, String> {
    for _ in 0..100 {  // Max iterations to prevent infinite history
        let status = ctx.schedule_activity("CheckStatus", task_id.clone())
            .await?;
        
        if status == "complete" {
            return Ok(status);
        }
        
        ctx.schedule_timer(std::time::Duration::from_millis(5000)).await;  // Wait 5s before retry
    }
    
    Err("Polling timeout".to_string())
}
```

**Better:** Use Continue As New for eternal polling:
```rust
async fn eternal_poll(ctx: OrchestrationContext, task_id: String) -> Result<String, String> {
    let status = ctx.schedule_activity("CheckStatus", task_id.clone())
        .await?;
    
    if status == "complete" {
        Ok(status)
    } else {
        ctx.schedule_timer(std::time::Duration::from_millis(5000)).await;
        return ctx.continue_as_new(task_id).await;  // Fresh history for next iteration
    }
}
```

### ❌ Anti-Pattern 6: Non-Deterministic Data Structures

```rust
use std::collections::HashMap;

// WRONG - HashMap iteration order is not guaranteed
async fn bad_graph(ctx: OrchestrationContext, _input: String) -> Result<String, String> {
    let mut nodes: HashMap<String, Node> = HashMap::new();
    nodes.insert("a".to_string(), Node { ... });
    nodes.insert("b".to_string(), Node { ... });
    nodes.insert("c".to_string(), Node { ... });
    
    // Serializing to JSON - order varies between runs!
    let graph_json = serde_json::to_string(&nodes)?;
    
    // This activity may receive different JSON on replay,
    // causing nondeterminism detection to fail
    ctx.schedule_activity("ProcessGraph", graph_json)
        .await
}
```

```rust
use std::collections::BTreeMap;

// CORRECT - BTreeMap maintains consistent ordering
async fn good_graph(ctx: OrchestrationContext, _input: String) -> Result<String, String> {
    let mut nodes: BTreeMap<String, Node> = BTreeMap::new();
    nodes.insert("a".to_string(), Node { ... });
    nodes.insert("b".to_string(), Node { ... });
    nodes.insert("c".to_string(), Node { ... });
    
    // BTreeMap serializes in consistent key order
    let graph_json = serde_json::to_string(&nodes)?;
    
    ctx.schedule_activity("ProcessGraph", graph_json)
        .await
}
```

**Data structures to AVOID in orchestration code:**

| Avoid | Use Instead | Why |
|-------|-------------|-----|
| `HashMap<K, V>` | `BTreeMap<K, V>` | HashMap iteration order varies between runs |
| `HashSet<T>` | `BTreeSet<T>` | HashSet iteration order is non-deterministic |
| `IndexMap` with `into_iter()` after remove | `BTreeMap` | IndexMap preserves insertion order but remove+insert can change it |
| `dashmap`, `flurry`, etc. | `BTreeMap` | Concurrent maps have non-deterministic iteration |

**Why this matters:**
When an orchestration replays, it must make the same decisions as the original run. If you serialize a `HashMap` to pass to an activity, the JSON key order can differ between the original run and replay. The runtime detects this as a nondeterminism violation because the activity input changed.

**Safe patterns:**
- Use `BTreeMap`/`BTreeSet` when order matters for serialization
- `Vec` is fine—maintains insertion order
- `IndexMap` is safe IF you only insert (never remove then re-insert)
- Build ordered structures in activities if you need hash-based performance

---

## Complete Examples

### E-Commerce Order Processing

```rust
use duroxide::OrchestrationContext;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Order {
    order_id: String,
    customer_id: String,
    items: Vec<OrderItem>,
    total: f64,
}

#[derive(Serialize, Deserialize)]
struct OrderItem {
    product_id: String,
    quantity: u32,
}

async fn process_order_orchestration(
    ctx: OrchestrationContext,
    order_json: String,
) -> Result<String, String> {
    let order: Order = serde_json::from_str(&order_json)
        .map_err(|e| format!("Invalid order JSON: {}", e))?;
    
    ctx.trace_info(format!("Processing order {}", order.order_id));
    
    // 1. Validate order
    let validation_result = ctx.schedule_activity("ValidateOrder", order_json.clone())
        .await?;
    
    // 2. Reserve inventory for all items (fan-out)
    let reservation_futures: Vec<_> = order.items.iter()
        .map(|item| {
            let item_json = serde_json::to_string(item).unwrap();
            ctx.schedule_activity("ReserveInventory", item_json)
        })
        .collect();
    
    let reservation_results = ctx.join(reservation_futures).await;
    
    // Check all reservations succeeded - each result is Result<String, String>
    let all_reserved = reservation_results.iter().all(|r| r.is_ok());
    
    if !all_reserved {
        ctx.trace_error("Inventory reservation failed");
        return Err("Insufficient inventory".to_string());
    }
    
    // 3. Charge payment
    let payment_result = ctx.schedule_activity("ChargePayment", order_json.clone())
        .await;
    
    match payment_result {
        Ok(payment_id) => {
            // 4. Fulfill order
            ctx.trace_info("Payment successful, fulfilling order");
            let fulfillment = ctx.schedule_activity("FulfillOrder", order_json)
                .await?;
            
            Ok(fulfillment)
        }
        Err(payment_error) => {
            // Compensation: Release all reservations
            ctx.trace_warn("Payment failed, compensating");
            
            let release_futures: Vec<_> = order.items.iter()
                .map(|item| {
                    let item_json = serde_json::to_string(item).unwrap();
                    ctx.schedule_activity("ReleaseInventory", item_json)
                })
                .collect();
            
            ctx.join(release_futures).await;
            
            Err(format!("Payment failed: {}", payment_error))
        }
    }
}
```

### Document Approval Workflow

```rust
async fn document_approval(
    ctx: OrchestrationContext,
    document_id: String,
) -> Result<String, String> {
    ctx.trace_info(format!("Starting approval for document {}", document_id));
    
    // 1. Request manager approval
    let approval_request = ctx.schedule_activity(
        "SendApprovalRequest",
        format!("{{\"document_id\": \"{}\", \"level\": \"manager\"}}", document_id)
    ).await?;
    
    // 2. Wait for approval or timeout (24 hours)
    let manager_timeout = ctx.schedule_timer(std::time::Duration::from_millis(86_400_000));
    let manager_approval = ctx.schedule_wait(format!("ManagerApproval_{}", document_id));
    
    let approval_data = match ctx.select2(manager_timeout, manager_approval).await {
        Either2::First(()) => {
            // Manager didn't respond - auto-escalate to director
            ctx.trace_warn("Manager approval timeout, escalating to director");
            
            ctx.schedule_activity(
                "SendApprovalRequest",
                format!("{{\"document_id\": \"{}\", \"level\": \"director\"}}", document_id)
            ).await?;
            
            // Wait for director (no timeout)
            ctx.schedule_wait(format!("DirectorApproval_{}", document_id))
                .await
        }
        Either2::Second(data) => {
            // Manager approved
            ctx.trace_info("Manager approved document");
            data
        }
    };
    
    // 3. Finalize document
    let finalize_input = format!("{{\"document_id\": \"{}\", \"approval\": {}}}", 
        document_id, approval_data);
    
    ctx.schedule_activity("FinalizeDocument", finalize_input)
        .await
}
```

### Batch Processing with Continue As New

```rust
async fn batch_processor(
    ctx: OrchestrationContext,
    cursor: String,
) -> Result<String, String> {
    ctx.trace_info(format!("Processing batch starting at cursor: {}", cursor));
    
    // Fetch next batch
    let batch_result = ctx.schedule_activity("FetchBatch", cursor)
        .await?;
    
    let batch: BatchResult = serde_json::from_str(&batch_result)?;
    
    // Process items in this batch (fan-out)
    let process_futures: Vec<_> = batch.items.iter()
        .map(|item| ctx.schedule_activity("ProcessItem", item.clone()))
        .collect();
    
    let results = ctx.join(process_futures).await;
    
    // Count successes - each result is Result<String, String>
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    
    ctx.trace_info(format!("Processed {} of {} items", success_count, batch.items.len()));
    
    // Continue to next batch or complete
    if let Some(next_cursor) = batch.next_cursor {
        ctx.trace_info("More batches to process, continuing");
        return ctx.continue_as_new(next_cursor).await;  // Start fresh with new cursor
    } else {
        ctx.trace_info("All batches processed");
        Ok(format!("Batch processing complete"))
    }
}

#[derive(Deserialize)]
struct BatchResult {
    items: Vec<String>,
    next_cursor: Option<String>,
}
```

---

## Testing Your Orchestrations

### Unit Testing with Replay Engine

For testing replay behavior, action emission, and completion delivery without the full runtime,
see the replay engine test suite in `tests/replay_engine/`. These tests use a minimal harness
that verifies orchestration logic against synthetic history.

Example pattern from `tests/replay_engine/sequential_progress.rs`:

```rust
/// Tests a simple activity schedule and completion.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_activity("Task", "input").await?;
///     Ok(result)
/// }
/// ```
#[test]
fn activity_scheduled_and_completed() {
    let history = vec![
        started_event(1),                        // OrchestrationStarted
        activity_scheduled(2, "Task", "input"),  // schedule_activity()
        activity_completed(3, 2, "result"),      // activity done
    ];

    let handler = SingleActivityHandler::new("Task", "input");
    let result = run_replay(history, handler);

    assert_eq!(result.status, TurnStatus::Completed(Ok("result".to_string())));
}
```

For comprehensive replay tests, run:
```bash
cargo nt -E 'test(/replay_engine::/)'   # 113 tests covering replay behavior
```

### Integration Testing (With Runtime)

```rust
#[tokio::test]
async fn test_full_workflow() {
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    
    let activities = ActivityRegistry::builder()
        .register("Task", |ctx: ActivityContext, input: String| async move {
            Ok(format!("processed: {}", input))
        })
        .build();
    
    let orch = |ctx: OrchestrationContext, input: String| async move {
        ctx.schedule_activity("Task", input).await
    };
    
    let orchestrations = OrchestrationRegistry::builder()
        .register("TestOrch", orch)
        .build();
    
    let rt = runtime::Runtime::start_with_store(
        store.clone(),
        activities,
        orchestrations,
    ).await;
    
    let client = Client::new(store);
    client.start_orchestration("test-1", "TestOrch", "input").await.unwrap();
    
    let status = client.wait_for_orchestration("test-1", std::time::Duration::from_secs(5))
        .await
        .unwrap();
    
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "processed: input");
        }
        _ => panic!("Expected completion"),
    }
    
    rt.shutdown(None).await;
}
```

---

## Best Practices

### 1. Keep Activities Idempotent

```rust
// Good: Idempotent database update
activities.register("UpdateUserStatus", |ctx: ActivityContext, user_json: String| async move {
    let user: User = serde_json::from_str(&user_json)?;
    
    // Upsert is idempotent
    db.execute("INSERT INTO users (id, status) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET status = ?",
        [&user.id, &user.status, &user.status])?;
    
    Ok(user.id)
});
```

### 2. Use Correlation IDs

```rust
async fn order_with_tracking(ctx: OrchestrationContext, order_json: String) -> Result<String, String> {
    // Generate correlation ID for entire workflow
    let correlation_id = ctx.new_guid().await?;
    
    // Pass correlation ID to all activities
    let validated = ctx.schedule_activity(
        "ValidateOrder",
        format!("{{\"order\": {}, \"correlation_id\": \"{}\"}}", order_json, correlation_id)
    ).await?;
    
    // Activities can log with correlation ID for tracing
    // ...
    
    Ok(correlation_id)
}
```

### 3. Handle Partial Failures

```rust
async fn robust_batch_processing(ctx: OrchestrationContext, items_json: String) -> Result<String, String> {
    let items: Vec<String> = serde_json::from_str(&items_json)?;
    
    // Process all items (don't stop on first failure)
    let futures: Vec<_> = items.iter()
        .map(|item| ctx.schedule_activity("ProcessItem", item.clone()))
        .collect();
    
    let results = ctx.join(futures).await;
    
    // Collect successes and failures - each result is Result<String, String>
    let mut successes = Vec::new();
    let mut failures = Vec::new();
    
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(output) => {
                successes.push((i, output.clone()));
            }
            Err(error) => {
                failures.push((i, error.clone()));
                ctx.trace_warn(format!("Item {} failed: {}", i, error));
            }
        }
    }
    
    if failures.is_empty() {
        Ok(format!("All {} items succeeded", successes.len()))
    } else {
        Ok(format!("{} succeeded, {} failed", successes.len(), failures.len()))
    }
}
```

### 4. Use Typed APIs When Possible

```rust
#[derive(Serialize, Deserialize)]
struct OrderInput {
    customer_id: String,
    items: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct OrderOutput {
    order_id: String,
    status: String,
}

async fn typed_workflow(
    ctx: OrchestrationContext,
    order: OrderInput,
) -> Result<OrderOutput, String> {
    // Compile-time type safety
    let validation: ValidationResult = ctx
        .schedule_activity_typed("ValidateOrder", &order)
        .await?;
    
    let order_id = ctx.new_guid().await?;
    
    Ok(OrderOutput {
        order_id,
        status: "completed".to_string(),
    })
}
```

---

## Orchestration Versioning

When running long-lived orchestrations in production, you'll need to evolve their logic over time while instances are still running. Duroxide supports orchestration versioning to handle this safely.

### Key Concepts

**Version Registration:**
```rust
// Default version (1.0.0)
OrchestrationRegistry::builder()
    .register_typed(MY_ORCHESTRATION, my_orchestration)
    
// Explicit versions  
    .register_versioned_typed(MY_ORCHESTRATION, "1.0.1", my_orchestration_v1_0_1)
    .register_versioned_typed(MY_ORCHESTRATION, "1.0.2", my_orchestration_v1_0_2)
    .build();
```

**Version Upgrade Timing (Critical):**  
Version upgrades happen at `continue_as_new` time, **not** when the server restarts. If an old version has a timer running, it must complete before the upgrade occurs.

```
T+0:00  Server restarts with v1.0.2 registered
        But v1.0.1 was mid-cycle with ~1 min left on its timer
T+1:00  v1.0.1's timer expires, completes its work
T+1:01  v1.0.1 calls continue_as_new() → resolves to v1.0.2
        Database updated: orchestration_version = "1.0.2"
```

**Best Practices:**
- Keep the orchestration NAME constant stable across all versions
- Create separate functions per version: `my_orch()`, `my_orch_v1_0_1()`, `my_orch_v1_0_2()`
- Add version prefix to trace logs: `ctx.trace_info("[v1.0.2] Starting cycle...")`
- Never remove old version registrations while instances might still be running them

📚 **See [versioning-best-practices.md](versioning-best-practices.md) for comprehensive patterns including code organization, common scenarios, and a complete best practices checklist.**

---


## Debugging Tips

### 1. Use Trace Statements

```rust
async fn debuggable_orch(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    ctx.trace_info(format!("Starting with input: {}", input));
    
    let step1 = ctx.schedule_activity("Step1", input).await?;
    ctx.trace_info(format!("Step1 completed: {}", step1));
    
    let step2 = ctx.schedule_activity("Step2", step1).await?;
    ctx.trace_info(format!("Step2 completed: {}", step2));
    
    ctx.trace_info("Workflow completed successfully");
    Ok(step2)
}
```

### 2. Inspect History

```rust
// Get orchestration history (requires management capability)
let client = Client::new(store);
let history = client.read_execution_history("my-instance", 1).await.unwrap();

for event in history {
    println!("{:?}", event);
}
```

### 3. Inspect Deterministic Flow

Use trace statements and history inspection to understand orchestration progress across turns. Each turn is triggered by new completions and is replay-safe; there is no public turn index.

---

## Advanced Patterns

### Dynamic Activity Scheduling

```rust
async fn dynamic_workflow(ctx: OrchestrationContext, config_json: String) -> Result<String, String> {
    let config: WorkflowConfig = serde_json::from_str(&config_json)?;
    
    // Determine activities to run based on input
    let activity_names = match config.workflow_type.as_str() {
        "standard" => vec!["Step1", "Step2", "Step3"],
        "expedited" => vec!["FastStep1", "FastStep2"],
        "manual" => vec!["ManualReview"],
        _ => return Err("Unknown workflow type".to_string()),
    };
    
    // Schedule determined activities
    let mut result = config_json;
    for activity_name in activity_names {
        result = ctx.schedule_activity(activity_name, result)
            .await?;
    }
    
    Ok(result)
}
```

### Actor-Style Orchestrations

```rust
// Orchestration as a long-lived actor
async fn order_actor(ctx: OrchestrationContext, state_json: String) -> Result<(), String> {
    let mut state: OrderState = serde_json::from_str(&state_json)?;
    
    // Wait for next command
    let command = ctx.schedule_wait("OrderCommand").await;
    let cmd: Command = serde_json::from_str(&command)?;
    
    match cmd.action.as_str() {
        "cancel" => {
            state.status = "cancelled".to_string();
            ctx.schedule_activity("SendCancellationEmail", state_json).await?;
            Ok(())  // Terminal
        }
        "ship" => {
            state.status = "shipped".to_string();
            ctx.schedule_activity("ShipOrder", serde_json::to_string(&state)?).await?;
            
            // Continue waiting for more commands
            return ctx.continue_as_new(serde_json::to_string(&state)?).await;
        }
        _ => {
            // Unknown command, continue waiting
            return ctx.continue_as_new(state_json).await;
        }
    }
}
```

---

## Performance Considerations

### 1. Minimize History Size

Each orchestration turn appends events. Keep history manageable:

```rust
// ❌ Bad - Creates millions of events
async fn bad_loop(ctx: OrchestrationContext, _input: String) -> Result<String, String> {
    for i in 0..1_000_000 {  // Too many!
        ctx.schedule_activity("Process", i.to_string()).await?;
    }
    Ok(())
}
```

```rust
// ✅ Good - Use Continue As New to reset history
async fn good_loop(ctx: OrchestrationContext, state_json: String) -> Result<String, String> {
    let state: State = serde_json::from_str(&state_json)?;
    
    // Process batch of 100
    for i in 0..100 {
        ctx.schedule_activity("Process", (state.offset + i).to_string())
            .await?;
    }
    
    state.offset += 100;
    
    if state.offset < state.total {
        return ctx.continue_as_new(serde_json::to_string(&state)?).await;  // Fresh history
    } else {
        Ok("All processed".to_string())
    }
}
```

### 2. Batch Operations

```rust
// ❌ Slow - One DB call per item
async fn slow_updates(ctx: OrchestrationContext, items: Vec<String>) -> Result<(), String> {
    for item in items {
        ctx.schedule_activity("UpdateItem", item).await?;
    }
    Ok(())
}
```

```rust
// ✅ Fast - Batch update in single activity
async fn fast_updates(ctx: OrchestrationContext, items_json: String) -> Result<(), String> {
    ctx.schedule_activity("UpdateItemsBatch", items_json).await?;
    Ok(())
}
```

---

## Common Gotchas

### 1. Non-Deterministic Control Flow

**Symptom:** Orchestration replays differently, causes nondeterminism errors

**Fix:** Base all decisions on activity results or external events, never on time/random

### 2. Activities That Only Sleep

**Symptom:** Activity that only sleeps without doing actual work

**Fix:** Use `ctx.schedule_timer()` in orchestration; activities can sleep/poll as part of their work (e.g., provisioning resources)

### 3. Using tokio::select! or tokio::join! in Orchestrations

**Symptom:** Non-deterministic behavior on replay

**Fix:** Use `ctx.select2()` / `ctx.select3()` and `ctx.join()` instead—these use `futures::select_biased!` for determinism

---

## Checklist for Production Orchestrations

- [ ] All side effects in activities (no I/O in orchestration code)
- [ ] Orchestration delays use `schedule_timer()` (activities can sleep/poll as needed)
- [ ] All branching is deterministic (based on activity results)
- [ ] Activities are idempotent (can retry safely)
- [ ] Error handling for all activities (match on Result)
- [ ] Trace statements for observability (`ctx.trace_*()` in both orchestrations and activities)
- [ ] Continue As New for long-running workflows (avoid unbounded history)
- [ ] Timeouts on external events (use select2 with timer)
- [ ] Compensation logic for failures (saga pattern)
- [ ] Tests written (unit + integration)

---

## Quick Reference

### OrchestrationContext API

| Method | Returns | Use For |
|--------|---------|---------|
| `schedule_activity(name, input)` | `impl Future<Output = Result<String, String>>` | Database, HTTP, file I/O |
| `schedule_timer(delay)` | `impl Future<Output = ()>` | Delays, timeouts, scheduling |
| `schedule_wait(event_name)` | `impl Future<Output = String>` | External signals, human input |
| `schedule_sub_orchestration(name, input)` | `impl Future<Output = Result<String, String>>` | Child workflows |
| `schedule_orchestration(name, instance, input)` | `()` | Fire-and-forget |
| `select2(a, b)` | `Either2<T1, T2>` | First to complete |
| `join(vec)` | `Vec<T>` | Wait for all |
| `continue_as_new(input)` | `impl Future` (never resolves) | Reset history, keep running |
| `new_guid()` | `impl Future<Output = Result<String, String>>` | Correlation IDs |
| `utc_now()` | `impl Future<Output = Result<u64, String>>` | Timestamps |
| `set_value(key, value)` | `()` | Store durable KV pair |
| `get_value(key)` | `Option<String>` | Read local KV |
| `clear_value(key)` | `()` | Remove a KV entry |
| `clear_all_values()` | `()` | Remove all KV entries |
| `get_value_from_instance(id, key)` | `impl Future<Output = Result<Option<String>, String>>` | Cross-instance KV read |
| `trace_info/warn/error(msg)` | `()` | Logging |

---

## Getting Help

- **Examples**: `examples/` directory - runnable samples
- **Tests**: `tests/e2e_samples.rs` - comprehensive patterns
- **API Docs**: Run `cargo doc --open`
- **Replay Debugging**: Check history with `client.read_execution_history()` (management capability required)
- **Cross-Crate Composition**: See `docs/cross-crate-registry-pattern.md` for organizing orchestrations across multiple crates

---

**You now have everything needed to build production-grade durable workflows!** 🎉


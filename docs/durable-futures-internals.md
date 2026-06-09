# Durable Futures Internals: How Duroxide Makes Your Code Survive Crashes

This document explains how Duroxide creates the illusion of **durable code**—code that continues executing through process crashes, restarts, and even machine failures. It's written for Rust developers who are new to the concept of durable execution.

If you want the user-facing programming model, start with [ORCHESTRATION-GUIDE.md](ORCHESTRATION-GUIDE.md).

## Table of Contents

1. [What is Durable Execution?](#what-is-durable-execution)
2. [Orchestrations: Your Durable Functions](#orchestrations-your-durable-functions)
3. [Activities: Where the Real Work Happens](#activities-where-the-real-work-happens)
4. [How It Works: Making Futures Durable](#how-it-works-making-futures-durable)
5. [The Replay Engine: Heart of Durability](#the-replay-engine-heart-of-durability)
6. [The Replay Algorithm in Detail](#the-replay-algorithm-in-detail)
7. [Special Operations](#special-operations)
8. [Termination and Cancellation](#termination-and-cancellation)
9. [Nondeterminism Detection](#nondeterminism-detection)
10. [Implementation Details (For Maintainers)](#implementation-details-for-maintainers)

---

## What is Durable Execution?

Imagine you're running a multi-step workflow that takes hours or days to complete:

1. Charge a customer's credit card
2. Wait for warehouse confirmation (might take hours)
3. Send a shipping notification
4. Wait 30 days
5. Send a follow-up survey

In traditional code, if your process crashes at step 3, you'd lose all your progress. You'd need to build complex checkpoint systems, handle partial failures, and manually resume from where you left off.

**Durable execution** solves this problem: your code *continues executing* through process boundaries—crashes, restarts, deployments, even moving between machines. From the programmer's perspective, it's as if the code never stopped running.

### Why Async/Futures?

Rust's async/await model turns out to be a natural fit for durable execution:

- You already think of futures as "work that will complete eventually"
- The syntax for waiting on long-running operations (`.await`) already exists
- Futures naturally compose with control flow, error handling, and concurrency primitives

Duroxide makes your futures **virtually long-running or durable**. A `.await` that takes 30 days looks exactly like one that takes 30 milliseconds:

```rust
// This works even if the process restarts 1000 times during those 30 days!
ctx.schedule_timer(Duration::from_days(30)).await;
```

**The catch**: to enable this magic, your orchestration code must be **deterministic**. Given the same history of events, it must make the same decisions. This constraint is what allows Duroxide to "replay" your code and recreate its state after a restart.

---

## Orchestrations: Your Durable Functions

An **orchestration** is a special async function that coordinates work. It's the durable "glue" that schedules activities, waits for events, and makes decisions.

Here's what an orchestration looks like:

```rust
async fn order_workflow(ctx: OrchestrationContext, order_id: String) -> Result<String, String> {
    // Schedule an activity to process payment
    let payment = ctx.schedule_activity("ProcessPayment", &order_id).await?;
    
    // Schedule another activity to reserve inventory
    let reservation = ctx.schedule_activity("ReserveInventory", &order_id).await?;
    
    // Return the combined result
    Ok(format!("Order {order_id} completed: {payment}, {reservation}"))
}
```

### The schedule_*() Methods

Inside orchestrations, you use `ctx.schedule_*()` methods to schedule work. These return ordinary Rust futures that you can `.await`:

| Method | Purpose | Returns |
|--------|---------|---------|
| `schedule_activity(name, input)` | Execute a unit of work | `impl Future<Output = Result<String, String>>` |
| `schedule_timer(duration)` | Wait for a duration | `impl Future<Output = ()>` |
| `schedule_wait(event_name)` | Wait for an external event | `impl Future<Output = String>` |
| `schedule_sub_orchestration(name, input)` | Start a child orchestration | `impl Future<Output = Result<String, String>>` |

**Important**: These are the *only* async operations allowed in orchestrations. You cannot call `tokio::time::sleep()`, make HTTP requests, or do any I/O directly—those must go in activities.

### Example: Control Flow and Branching

Orchestrations use normal Rust control flow. Here's an example with branching:

```rust
async fn approval_workflow(ctx: OrchestrationContext, request: String) -> Result<String, String> {
    // Get the request amount
    let amount: u32 = ctx.schedule_activity("GetAmount", &request).await?
        .parse()
        .unwrap_or(0);
    
    if amount > 10_000 {
        // Large amounts need manager approval
        let approved = ctx.schedule_activity("RequestManagerApproval", &request).await?;
        if approved == "rejected" {
            return Err("Manager rejected the request".into());
        }
    }
    
    // Process the approved request
    let result = ctx.schedule_activity("ProcessRequest", &request).await?;
    Ok(result)
}
```

### Example: Error Handling and Recovery

Standard Rust error handling works as expected:

```rust
async fn resilient_workflow(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    match ctx.schedule_activity("RiskyOperation", &input).await {
        Ok(result) => Ok(result),
        Err(e) => {
            // Log the failure (using replay-safe tracing)
            ctx.trace_warn(format!("RiskyOperation failed: {e}, attempting recovery"));
            
            // Try a recovery activity
            let recovered = ctx.schedule_activity("RecoveryOperation", &input).await?;
            Ok(format!("recovered: {recovered}"))
        }
    }
}
```

### Example: Long-Running Timers

Timers can span arbitrary durations—even across process restarts:

```rust
async fn subscription_reminder(ctx: OrchestrationContext, user_id: String) -> Result<String, String> {
    // Wait 30 days (survives any number of restarts!)
    ctx.schedule_timer(Duration::from_secs(30 * 24 * 60 * 60)).await;
    
    // Send a reminder
    ctx.schedule_activity("SendRenewalReminder", &user_id).await?;
    
    // Wait another 30 days
    ctx.schedule_timer(Duration::from_secs(30 * 24 * 60 * 60)).await;
    
    // Check if they renewed
    let status = ctx.schedule_activity("CheckSubscriptionStatus", &user_id).await?;
    Ok(status)
}
```

### Example: Timeout Patterns with select

Use `ctx.select2()` to race operations—this is essential for timeouts:

```rust
async fn with_timeout(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    let work = ctx.schedule_activity("SlowOperation", &input);
    let timeout = async {
        ctx.schedule_timer(Duration::from_secs(30)).await;
        Err::<String, String>("Operation timed out".into())
    };
    
    // Race the two futures—whichever completes first wins
    match ctx.select2(work, timeout).await {
        Either2::First(result) => result,
        Either2::Second(timeout_err) => timeout_err,
    }
}
```

**Critical**: Always use `ctx.select2()` / `ctx.join()`, never `tokio::select!` / `tokio::join!`. The context methods are deterministic; tokio's are not.

---

## Activities: Where the Real Work Happens

**Activities** are where you do real work—I/O, API calls, database operations, anything with side effects:

```rust
let activity_registry = ActivityRegistry::builder()
    .register("ProcessPayment", |ctx: ActivityContext, order_id: String| async move {
        // This runs in a worker, NOT in the orchestration
        let result = payment_api::charge(&order_id).await?;
        Ok(format!("charged:{}", result.transaction_id))
    })
    .register("SendEmail", |ctx: ActivityContext, email: String| async move {
        email_service::send(&email).await?;
        Ok("sent".into())
    })
    .build();
```

### Activity Guarantees: At-Least-Once Execution

Activities have **at-least-once** execution semantics. This means:

- If the worker crashes mid-execution, the activity **will be retried**
- Your activity code must handle being called multiple times for the same logical operation

Design activities to be **idempotent** when possible:

```rust
// ✅ Good: Uses idempotency key
.register("ChargeCard", |ctx: ActivityContext, input: String| async move {
    let req: ChargeRequest = serde_json::from_str(&input)?;
    // Use order_id as idempotency key—charging twice is safe
    payment_api::charge_idempotent(&req.order_id, req.amount).await
})

// ⚠️ Risky: Not idempotent—could send duplicate emails
.register("SendEmail", |ctx: ActivityContext, input: String| async move {
    email_service::send(&input).await  // If retried, sends twice!
})
```

### What Can Activities Do?

Activities can do *anything*:
- HTTP requests
- Database operations
- File I/O
- Sleep (`tokio::time::sleep` is fine here!)
- Call external APIs
- Use randomness
- Access system time

The only limit is that they return `Result<String, String>`.

---

## How It Works: Making Futures Durable

Now we get to the heart of the matter: **how does Duroxide make futures survive crashes?**

The key insight: **we don't checkpoint memory.** Instead, we:

1. **Record** what the orchestration scheduled (as events in persistent storage)
2. **Replay** the orchestration from the beginning after any restart
3. **Feed** previously-recorded results back to futures during replay

The orchestration code runs *multiple times*, but it sees the same results each time—giving the illusion that it ran once, continuously.

### The Event History

Every orchestration instance has an **event history**—a persistent log of what happened. Here's what a simple orchestration's history might look like:

```
Event History for instance "order-123":
─────────────────────────────────────────────────────────────────
[1] OrchestrationStarted { name: "OrderWorkflow", input: "order-123" }
[2] ActivityScheduled { name: "ProcessPayment", input: "order-123" }
[3] ActivityCompleted { source_event_id: 2, result: "txn:abc123" }
[4] ActivityScheduled { name: "ReserveInventory", input: "order-123" }
[5] ActivityCompleted { source_event_id: 4, result: "reserved:xyz" }
[6] OrchestrationCompleted { output: "Order complete: txn:abc123, reserved:xyz" }
```

This history is the **source of truth**. When replaying, the orchestration will:
- See the same `ActivityCompleted` result for "ProcessPayment"
- Make the same decisions it made before
- Schedule the same subsequent activities

### Tokens: The Bridge Between Futures and History

Here's the challenge: when you call `schedule_activity()`, you get back a future. But that future is created *fresh* on every replay. How do we connect it to its persisted result?

The answer: **tokens**.

```
┌─────────────────────────────────────────────────────────────────────┐
│  What happens when you call schedule_activity("Greet", "Alice"):   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. ALLOCATE TOKEN                                                   │
│     token = 1 (incrementing counter, in-memory only)                │
│                                                                      │
│  2. EMIT ACTION                                                      │
│     emitted_actions.push((token=1, CallActivity{name, input}))      │
│                                                                      │
│  3. RETURN FUTURE                                                    │
│     Return a future that polls for completion_results[token=1]       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

The **replay engine** then:
1. Matches emitted actions to events in history
2. **Binds** tokens to event IDs: `token_bindings[1] = event_id_10`
3. Finds completions for those events
4. **Delivers** results back to the token: `completion_results[1] = Ok("Hello!")`

When the future polls, it finds its result and resolves.

---

## The Replay Engine: Heart of Durability

Let's trace through a concrete example to understand how replay works.

### Example Orchestration

```rust
async fn greet(ctx: OrchestrationContext, name: String) -> Result<String, String> {
    let greeting = ctx.schedule_activity("Greet", &name).await?;
    Ok(greeting)
}
```

### Turn 1: Fresh Execution (No History Yet)

The orchestration starts for the first time. The only history is `OrchestrationStarted`:

```
┌─────────────────────────────────────────────────────────────────────┐
│ History: [OrchestrationStarted]                                      │
├─────────────────────────────────────────────────────────────────────┤
│ Open Futures Table (in-memory):                                      │
│ ┌─────────┬───────────────────┬─────────┬────────────┐              │
│ │ Token   │ Action            │ EventID │ Result     │              │
│ ├─────────┼───────────────────┼─────────┼────────────┤              │
│ │ (empty) │                   │         │            │              │
│ └─────────┴───────────────────┴─────────┴────────────┘              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  STEP 1: Orchestration runs, hits schedule_activity("Greet", "Alice")│
│                                                                      │
│  → Token 1 allocated                                                 │
│  → Action emitted: (1, CallActivity{name:"Greet", input:"Alice"})    │
│                                                                      │
│  Table now:                                                          │
│  ┌─────────┬───────────────────────────┬─────────┬────────────┐     │
│  │ Token   │ Action                    │ EventID │ Result     │     │
│  ├─────────┼───────────────────────────┼─────────┼────────────┤     │
│  │ 1       │ CallActivity{Greet,Alice} │ (none)  │ (pending)  │     │
│  └─────────┴───────────────────────────┴─────────┴────────────┘     │
│                                                                      │
│  STEP 2: Future is awaited → polls → no result → returns Pending     │
│                                                                      │
│  STEP 3: Engine processes new action (no matching history):          │
│    → Allocates event_id = 2                                          │
│    → Binds: token_bindings[1] = 2                                    │
│    → Persists: ActivityScheduled{event_id:2, name:"Greet", ...}      │
│    → Dispatches work to worker queue                                 │
│                                                                      │
│  Table now:                                                          │
│  ┌─────────┬───────────────────────────┬─────────┬────────────┐     │
│  │ Token   │ Action                    │ EventID │ Result     │     │
│  ├─────────┼───────────────────────────┼─────────┼────────────┤     │
│  │ 1       │ CallActivity{Greet,Alice} │ 2       │ (pending)  │     │
│  └─────────┴───────────────────────────┴─────────┴────────────┘     │
│                                                                      │
│  STEP 4: Turn ends. Future is dropped. Only history persists.        │
│                                                                      │
├─────────────────────────────────────────────────────────────────────┤
│ History after turn: [OrchestrationStarted, ActivityScheduled{id=2}]  │
└─────────────────────────────────────────────────────────────────────┘
```

Meanwhile, a worker picks up the activity, executes it, and records the completion.

### Turn 2: Replay After Activity Completes

The orchestration wakes up again. Now history includes the completion:

```
┌─────────────────────────────────────────────────────────────────────┐
│ History: [OrchestrationStarted,                                      │
│           ActivityScheduled{id=2, name:"Greet", input:"Alice"},      │
│           ActivityCompleted{id=3, source_id=2, result:"Hello!"}]     │
├─────────────────────────────────────────────────────────────────────┤
│ Open Futures Table: (starts empty—new turn, new future)              │
│ ┌─────────┬───────────────────┬─────────┬────────────┐              │
│ │ Token   │ Action            │ EventID │ Result     │              │
│ └─────────┴───────────────────┴─────────┴────────────┘              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  STEP 1: Create fresh orchestration future (replay from beginning)   │
│                                                                      │
│  STEP 2: Poll orchestration → hits schedule_activity("Greet","Alice")│
│    → Token 1 allocated (same as before—deterministic!)               │
│    → Action emitted: (1, CallActivity{Greet, Alice})                 │
│                                                                      │
│  Table now:                                                          │
│  ┌─────────┬───────────────────────────┬─────────┬────────────┐     │
│  │ Token   │ Action                    │ EventID │ Result     │     │
│  ├─────────┼───────────────────────────┼─────────┼────────────┤     │
│  │ 1       │ CallActivity{Greet,Alice} │ (none)  │ (pending)  │     │
│  └─────────┴───────────────────────────┴─────────┴────────────┘     │
│                                                                      │
│  STEP 3: Replay engine walks history:                                │
│                                                                      │
│    → Sees ActivityScheduled{id=2}                                    │
│    → Pops emitted action, validates it matches                       │
│    → Binds: token_bindings[1] = 2                                    │
│                                                                      │
│  Table now:                                                          │
│  ┌─────────┬───────────────────────────┬─────────┬────────────┐     │
│  │ Token   │ Action                    │ EventID │ Result     │     │
│  ├─────────┼───────────────────────────┼─────────┼────────────┤     │
│  │ 1       │ CallActivity{Greet,Alice} │ 2       │ (pending)  │     │
│  └─────────┴───────────────────────────┴─────────┴────────────┘     │
│                                                                      │
│    → Sees ActivityCompleted{source_id=2, result:"Hello!"}            │
│    → Finds token bound to event 2 → token 1                          │
│    → Delivers: completion_results[1] = Ok("Hello!")                  │
│                                                                      │
│  Table now:                                                          │
│  ┌─────────┬───────────────────────────┬─────────┬────────────┐     │
│  │ Token   │ Action                    │ EventID │ Result     │     │
│  ├─────────┼───────────────────────────┼─────────┼────────────┤     │
│  │ 1       │ CallActivity{Greet,Alice} │ 2       │ Ok("Hello!")│    │
│  └─────────┴───────────────────────────┴─────────┴────────────┘     │
│                                                                      │
│  STEP 4: Poll orchestration again                                    │
│    → Future checks completion_results[1] → finds Ok("Hello!")        │
│    → Returns Poll::Ready(Ok("Hello!"))                               │
│    → Orchestration returns Ok("Hello!")                              │
│                                                                      │
│  STEP 5: Engine records OrchestrationCompleted{output:"Hello!"}      │
│                                                                      │
├─────────────────────────────────────────────────────────────────────┤
│ History after: [..., OrchestrationCompleted{output:"Hello!"}]        │
└─────────────────────────────────────────────────────────────────────┘
```

The orchestration completed successfully—and it would produce the *exact same result* even if the process crashed and restarted 100 times between Turn 1 and Turn 2.

---

## The Replay Algorithm in Detail

Here's the complete algorithm the replay engine follows:

### Turn Execution Pseudocode

```
function execute_turn(instance_id, history, completion_messages):
    
    # 1. PREPARE: Add new completions to working history
    working_history = history + convert_to_events(completion_messages)
    
    # 2. CREATE: Instantiate fresh orchestration context and future
    ctx = new OrchestrationContext()
    future = create_orchestration_future(ctx, input)
    
    # 3. REPLAY LOOP: Walk history, poll orchestration
    must_poll = true
    history_cursor = 0
    
    while history_cursor < working_history.length:
        
        # 3a. Poll orchestration if we might make progress
        if must_poll:
            must_poll = false
            result = poll_once(future)
            
            if result == Ready(output):
                return TurnResult::Completed(output)
            if result == Ready(Err(e)):
                return TurnResult::Failed(e)
        
        # 3b. Process next history event
        event = working_history[history_cursor]
        history_cursor += 1
        
        # Update is_replaying state
        ctx.set_replaying(history_cursor <= persisted_history_length)
        
        match event.kind:
            
            # Schedule events: bind tokens to event IDs
            ActivityScheduled | TimerCreated | ExternalSubscribed | SubOrchestrationScheduled:
                action = ctx.pop_next_emitted_action()
                validate_action_matches_event(action, event)
                ctx.bind_token(action.token, event.event_id)
            
            # Completion events: deliver results to tokens
            ActivityCompleted { result }:
                token = ctx.find_token_for_event(event.source_event_id)
                ctx.deliver_result(token, ActivityOk(result))
                must_poll = true  # Might unblock orchestration
            
            ActivityFailed { details }:
                token = ctx.find_token_for_event(event.source_event_id)
                ctx.deliver_result(token, ActivityErr(details))
                must_poll = true
            
            TimerFired:
                token = ctx.find_token_for_event(event.source_event_id)
                ctx.deliver_result(token, TimerComplete)
                must_poll = true
            
            ExternalEvent { name, data }:
                ctx.record_external_arrival(name, data)
                must_poll = true  # Might satisfy a schedule_wait()
            
            SubOrchestrationCompleted | SubOrchestrationFailed:
                # Similar to activities...
                
            OrchestrationCancelRequested:
                ctx.mark_cancellation_requested()
            
            SystemCall { op, value }:
                # System calls are schedule + complete in one event
                action = ctx.pop_next_emitted_action()
                ctx.bind_token(action.token, event.event_id)
                ctx.deliver_result(action.token, SystemCallValue(value))
                must_poll = true
    
    # 4. FINAL POLL: Process any remaining work
    result = poll_once(future)
    if result == Ready(output):
        return TurnResult::Completed(output)
    
    # 5. NEW SCHEDULES: Process actions that weren't in history
    for (token, action) in ctx.remaining_emitted_actions():
        event_id = allocate_next_event_id()
        ctx.bind_token(token, event_id)
        
        schedule_event = create_schedule_event(action, event_id)
        persist_event(schedule_event)
        dispatch_action(action)
    
    # 6. CHECK CANCELLATION: Takes precedence over normal completion
    if ctx.is_cancellation_requested():
        return TurnResult::Cancelled(reason)
    
    return TurnResult::Continue  # Orchestration is suspended, waiting for more events
```

### Key Invariants

1. **Actions emitted in order**: The orchestration emits schedule actions in deterministic order. The replay engine validates they match history in the same order.

2. **Binding before delivery**: A token must be bound to an event ID before a completion can be delivered to it.

3. **Poll only when progress is possible**: The engine doesn't busy-poll. It sets `must_poll = true` only after delivering a completion or external event.

4. **Completions keyed by token, not event ID**: Futures poll using their token, not the event ID. The engine translates via `token_bindings`.

---

## Special Operations

### System Activities: Replay-Safe Time and IDs

Some operations are provided as **built-in activities** (under reserved names) so they follow the same
`ActivityScheduled` + `ActivityCompleted` event flow as user activities. This ensures replay determinism
without special-case logic in the replay engine.

- `ctx.new_guid()` – Generate a random UUID (recorded in history, so replay-stable)
- `ctx.utc_now()` – Get the current time (replay-safe)

Example usage:

```rust
let guid = ctx.new_guid().await?;
```

In history, these appear as normal activity events (with a reserved activity name):

```
[10] ActivityScheduled { name: "__duroxide_syscall:new_guid", input: "" }
[11] ActivityCompleted { source_event_id: 10, result: "550e8400-e29b-41d4-a716-446655440000" }
```

On replay, the engine re-delivers the recorded completion value, so the orchestration observes the same GUID/time.

### Session Affinity: Routing Activities to Specific Workers

`Action::CallActivity` includes an optional `session_id`. When present, the replay engine
flows it through `ActivityScheduled` events and `WorkItem::ActivityExecute`. The replay
engine treats `session_id` as opaque data — it participates in action matching (ensuring
replay determinism) but doesn't affect the replay algorithm itself. Session routing is
handled entirely by the provider's `fetch_work_item` implementation.

### continue_as_new: Preventing Unbounded History Growth

Long-running orchestrations (like actors or state machines) can accumulate huge histories. `continue_as_new()` resets the history:

```rust
async fn long_running_actor(ctx: OrchestrationContext, state: String) -> Result<String, String> {
    let new_state = ctx.schedule_activity("ProcessBatch", &state).await?;
    
    // After processing, start fresh with new state
    // This creates a new execution with empty history
    ctx.continue_as_new(new_state).await
}
```

The old execution's history ends with `OrchestrationContinuedAsNew`. A new execution starts fresh.

### External Events: Out-of-Band Data

External events allow outside systems to send data into a waiting orchestration:

```rust
// Orchestration waits for approval
let approval = ctx.schedule_wait("approval").await;
if approval == "approved" {
    ctx.schedule_activity("ProcessOrder", &order_id).await?;
}
```

```rust
// External system sends the event
client.raise_event("order-123", "approval", "approved").await;
```

External events are matched by **name and subscription index**, not by source event ID. The first `schedule_wait("approval")` gets the first external event named "approval", the second gets the second, etc.

---

## Termination and Cancellation

### Normal Completion

An orchestration completes when it returns:

```rust
Ok("success".into())  // → OrchestrationCompleted { output: "success" }
Err("failed".into())  // → OrchestrationFailed { details: ... }
```

### Cancellation

Cancellation is **cooperative**. When you cancel an instance:

```rust
client.cancel_instance("order-123", "user_requested").await;
```

The engine:
1. Records `OrchestrationCancelRequested { reason: "user_requested" }`
2. On the next turn, returns `TurnResult::Cancelled` after replay
3. Records `OrchestrationFailed { details: Cancelled { reason } }`

Orchestrations can check cancellation and clean up:

```rust
if ctx.is_cancellation_requested() {
    // Clean up resources before terminating
    ctx.schedule_activity("Cleanup", "").await?;
}
```

### Cascading Cancellation

When a parent orchestration is cancelled, its child sub-orchestrations are also cancelled:

```
Parent cancelled
    → Child 1 receives CancelInstance work item
    → Child 2 receives CancelInstance work item
    → Both children terminate with Cancelled status
```

### Activity Cancellation

Activities can also respond to cancellation cooperatively:

```rust
.register("LongRunning", |ctx: ActivityContext, input: String| async move {
    for item in items {
        if ctx.is_cancelled() {
            return Err("Activity cancelled".into());
        }
        process(item).await;
    }
    Ok("done".into())
})
```

---

## Nondeterminism Detection

The replay engine **validates** that your orchestration is deterministic. If it detects a mismatch, the turn fails with a nondeterminism error.

### Schedule Mismatch

If the orchestration emits a different schedule than history expects:

```rust
// Version 1: schedules ActivityA first
let a = ctx.schedule_activity("ActivityA", "").await?;
let b = ctx.schedule_activity("ActivityB", "").await?;

// Version 2 (BREAKS REPLAY): schedules ActivityB first  
let b = ctx.schedule_activity("ActivityB", "").await?;  // Error!
let a = ctx.schedule_activity("ActivityA", "").await?;
```

The engine will fail with: "expected ActivityA, got ActivityB"

### Common Causes of Nondeterminism

| Problem | Why It Breaks | Solution |
|---------|--------------|----------|
| `Uuid::new_v4()` | Different UUID each replay | Use `ctx.new_guid()` |
| `SystemTime::now()` | Different time each replay | Use `ctx.utc_now()` |
| `rand::random()` | Different value each replay | Use activities for randomness |
| `tokio::select!` | Nondeterministic poll order | Use `ctx.select2()` |
| Changing code | Different schedule order | Use versioning |
| Conditional on external state | External state may change | Pass state as input |

### Defensive Programming Tips

1. **Keep orchestrations pure**: No I/O, no randomness, no system time
2. **Use version guards** when changing orchestration logic:
   ```rust
   if ctx.version() < "2.0" {
       // Old behavior
   } else {
       // New behavior
   }
   ```
3. **Test with replay**: Run your orchestrations twice and verify they produce the same schedule

---

## Summary

Duroxide makes your async code durable through a simple but powerful mechanism:

1. **Record** what the orchestration schedules as events
2. **Replay** the orchestration from the beginning after any restart
3. **Feed** recorded results back to futures during replay

The key abstractions:

- **Tokens**: In-memory identifiers that bridge futures to persisted events
- **Binding**: The replay engine connects tokens to event IDs by matching actions to history
- **Delivery**: Completions are routed back to tokens, resolving the waiting futures

The contract you must uphold:

- **Determinism**: Given the same history, emit the same schedules in the same order
- **Activities for side effects**: Keep orchestrations pure; do I/O in activities
- **Use context helpers**: `ctx.select2()` not `tokio::select!`, `ctx.new_guid()` not `Uuid::new_v4()`

When you follow these rules, your code genuinely survives crashes—running for days, weeks, or months as if it never stopped.

---

## Implementation Details (For Maintainers)

This section covers implementation specifics for developers modifying the replay engine.

### Responsibility Boundaries

The replay engine is **pure with respect to external side-effects**. It only transforms history and emits decisions. The runtime owns everything else.

**ReplayEngine owns:**
- Input assembly: combine baseline history with this-turn completion events
- Deterministic replay: run orchestration code once with `OrchestrationContext`
- Decision capture: collect `Action`s recorded during the poll
- Event materialization: append new events produced during the poll
- Nondeterminism guard: detect mismatches (completion kind, missing schedules)
- Terminal detection: surface outcomes (Completed/Failed/Cancelled/ContinueAsNew)

**Runtime owns (NOT ReplayEngine):**
- Persisting history and actions atomically
- Enqueuing worker/timer/orchestrator work items
- Acknowledging queue messages
- Version resolution and execution-id management

### Turn Lifecycle (Three Phases)

**Phase 1: Prep completions** (convert work items to events)
- Validate each completion belongs to the current execution_id
- Drop duplicates already persisted or staged this turn
- Detect mismatches (e.g., completion kind doesn't match scheduled kind)
- Stage converted events in `history_delta` with assigned `event_id`s

**Phase 2: Execute orchestration** (replay loop)
- Build `working_history = baseline_history + history_delta`
- Walk through history events, polling the orchestration as needed:
  - On schedule events: match against emitted actions, bind tokens to event IDs
  - On completion events: deliver results to waiting futures, then re-poll
  - On system calls: deliver result immediately, then re-poll
- Continue until orchestration returns Ready or all history is processed
- Final poll after all history to capture any new schedules

**Phase 3: Apply results**
- If nondeterminism flagged: return Failed with message
- Convert remaining emitted actions to pending_actions and history_delta
- Determine terminal state: Continue, Completed, Failed, ContinueAsNew, or Cancelled

### Data Flow

```
Inputs                              Outputs
────────────────────────────────    ────────────────────────────────
instance: String                    history_delta: Vec<Event>
execution_id: u64                   pending_actions: Vec<Action>
baseline_history: Vec<Event>        TurnResult:
completion_messages: Vec<WorkItem>    • Continue
                                      • Completed(String)
                                      • Failed(String)
                                      • ContinueAsNew { input, version }
                                      • Cancelled(String)
```

### Polling Model

The replay engine polls the orchestration **multiple times per turn**:

1. **Initial poll**: Start the orchestration, capture first emitted actions
2. **After each completion delivery**: Re-poll so the orchestration can advance
3. **Final poll**: After all history is processed, capture any remaining actions

This multi-poll approach allows the orchestration to make progress through multiple `.await` points within a single turn, as long as the completions are available in history.

```
History: [Started, ActivityScheduled, ActivityCompleted, TimerScheduled, TimerFired]
                     ↓                      ↓                    ↓            ↓
Polls:          [poll 1]              [poll 2]             [poll 3]      [poll 4]
                emit A1               (advances)            emit T1      (advances)
```

### Error Handling

- **Nondeterminism**: Returns `Failed` with a descriptive message (e.g., "expected ActivityA, got ActivityB")
- **Panics**: Caught and returned as `Failed("nondeterministic: ...")`

### File Reference

- **Implementation**: `src/runtime/replay_engine.rs`
- **Entry points**:
  - `ReplayEngine::new(instance, execution_id, baseline_history)`
  - `ReplayEngine::prep_completions(messages)`
  - `ReplayEngine::execute_orchestration(handler, input)`

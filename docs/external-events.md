# External Event Semantics

This runtime delivers external events to orchestrations by name, correlated to the most recent matching subscription recorded in history.

Key points
- Subscriptions are explicit: a subscription is recorded when the orchestrator awaits `schedule_wait("Name")` (history: `ExternalSubscribed { id, name }`).
- Delivery requires a subscription: when an external event arrives (`ExternalRaised`), the runtime looks up the latest `ExternalSubscribed` for that name in the current execution and only then appends `ExternalEvent { id, name, data }`.
- Early events are dropped: if no subscription exists yet at the time of delivery, the event is dropped (with a warning).
- Execution-scoped: events are delivered to the currently active execution. After ContinueAsNew, the new execution must subscribe again.
- At-least-once: raising the same external event again after subscription is idempotent at the app layer if you handle duplicates; the runtime does not dedupe by payload.

Flow
1. Sender calls `Runtime::raise_event(instance, name, data)`.
2. Provider enqueues `ExternalRaised` in the work queue.
3. Poller forwards to the active instance as `ExternalByName`.
4. Instance loop (append_completion) resolves name → subscription id and appends `ExternalEvent` if found; otherwise logs a warning and drops the event.

Recommendations
- Subscribe early in your orchestrator before signaling external parties.
- If you need buffering before subscription, use **event queues** (see below) instead of ephemeral events.
- Include correlation data in `data` and implement idempotency in your orchestrator for resilience.

## Event Queues (Persistent/FIFO)

For use cases where FIFO ordering matters — use the queue-based API:

> **Important:** Events must be enqueued *after* the orchestration has started
> (i.e., after `start_orchestration` is called). Events enqueued for a
> non-existent orchestration instance are dropped by the provider. Once the
> orchestration is running, queued events are buffered until consumed — even
> if no `dequeue_event` subscription is currently active.

**Orchestration side:**
```rust
// Dequeue next message (blocks until available)
let msg = ctx.dequeue_event("inbox").await;

// Typed variant
let msg: ChatMessage = ctx.dequeue_event_typed("inbox").await;
```

**Client side:**
```rust
// Send a message into the queue
client.enqueue_event("instance-1", "inbox", payload).await?;

// Typed variant
client.enqueue_event_typed("instance-1", "inbox", &data).await?;
```

Key differences from ephemeral events:

| Feature | Ephemeral (`schedule_wait`/`raise_event`) | Queue (`dequeue_event`/`enqueue_event`) |
|---------|------------------------------------------|----------------------------------------|
| Matching | Positional (Nth wait ↔ Nth raise) | FIFO queue |
| Early messages | Dropped if no subscription | Buffered once orchestration is running; dropped if enqueued before start |
| Survives CAN | No (must re-subscribe) | Yes (queue persists) |
| Use case | One-shot signals, approvals | Chat, command streams, iterative loops |

Deprecated aliases: `raise_event_persistent()` → `enqueue_event()`, `schedule_wait_persistent()` → `dequeue_event()`.

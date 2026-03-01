# Activity Tags

**Status:** Proposal  
**Created:** 2026-01-12  
**Updated:** 2026-03-01  
**Author:** AI Assistant  

## Summary

Add support for activity tags that allow routing activities to specific workers. This enables specialized worker pools for activities with different resource requirements (e.g., GPU workers, build machines, LLM workers), as well as dynamic worker affinity for session-based workloads.

Tags are orthogonal to sessions — an activity can have both a tag AND a session_id for layered routing (e.g., route to GPU pool, pin to specific GPU worker).

## Motivation

Currently, all activity workers share a single queue and can process any registered activity. This doesn't support scenarios where:

- Some activities require specialized hardware (GPUs, high memory)
- Build activities must run on dedicated build machines with compilers, SDKs, and caches
- Activities need to run in specific regions/zones for data locality
- Different SLA tiers require dedicated worker pools
- Resource isolation between teams or tenants
- LLM inference needs workers with models loaded in GPU memory

## Design Decisions

1. **Tags specified at schedule time only** — The orchestration decides the tag when calling `.with_tag()`.

2. **No default tags at registration** — Activities are registered without tags; routing is purely a scheduling concern.

3. **Workers subscribe to tags via RuntimeOptions** — Workers declare which tags they can process.

4. **No tag = default** — Activities scheduled without `.with_tag()` go to the default queue (None).

5. **Exact matches only** — No wildcard or pattern matching. Single tag per activity.

6. **Full observability** — Tag appears in metrics, traces, and logs.

7. **Backward compatible** — Existing code works unchanged (no `.with_tag()` = default).

8. **Tags and sessions are orthogonal** — An activity can have both a tag AND a session_id. Provider query composes both filters with AND.

9. **No provider-level TTL for stranded activities** — If a tagged activity has no matching worker, it sits in queue indefinitely. Users protect against this with orchestration-level timeouts via `schedule_activity_with_retry(..., RetryPolicy::new(1).with_timeout(...))` or `select2(activity, timer)`.

10. **Mutate-after-emit pattern** — `.with_tag()` on `DurableFuture` patches the already-emitted `Action` before the replay engine drains it. This preserves full composability with `join`, `select2`, `map`, and Drop-based cancellation.

## Terminology

- **Tag**: A routing label specified at schedule time (e.g., "gpu", "build-machine", "llm-worker")
- **Default**: Activities without a tag (represented as `None`)
- **Stranded Activity**: A tagged activity with no matching worker — sits in queue indefinitely

---

## System Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SHARED PROVIDER                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         worker_queue                                 │   │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────────┐ │   │
│  │  │ tag: NULL    │ │ tag: "gpu"   │ │ tag: "gpu"   │ │ tag: "w-01" │ │   │
│  │  │ SendEmail    │ │ GPUInference │ │ VideoEncode  │ │ ProcessData │ │   │
│  │  └──────────────┘ └──────────────┘ └──────────────┘ └─────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
         │                      │                              │
         │ WHERE tag IS NULL    │ WHERE tag = 'gpu'           │ WHERE tag = 'w-01'
         ▼                      ▼                              ▼
┌─────────────────┐    ┌─────────────────┐            ┌─────────────────┐
│ DEFAULT WORKER  │    │   GPU WORKER    │            │  WORKER w-01    │
│                 │    │                 │            │  (with cache)   │
│ TagFilter::     │    │ TagFilter::     │            │ TagFilter::     │
│  default_only() │    │  tags(["gpu"])  │            │  tags(["w-01"]) │
└─────────────────┘    └─────────────────┘            └─────────────────┘
```

### Tag Flow: Schedule Time → Worker Queue → Worker

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATION                                    │
│                                                                          │
│  // No tag - goes to default queue                                       │
│  ctx.schedule_activity("SendEmail", input).await?;    // tag = None      │
│                                                                          │
│  // With tag - goes to "gpu" queue                                       │
│  ctx.schedule_activity("GPUInference", input)                            │
│      .with_tag("gpu")                                 // tag = "gpu"     │
│      .await?;                                                            │
│                                                                          │
│  // Dynamic tag for worker affinity                                      │
│  ctx.schedule_activity("ProcessData", input)                             │
│      .with_tag(&session.worker_id)                    // tag = "w-01"    │
│      .await?;                                                            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │     WorkItem::ActivityExecute │
                    │     {                         │
                    │       name: "GPUInference",   │
                    │       tag: Some("gpu"),       │
                    │       ...                     │
                    │     }                         │
                    └───────────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │         worker_queue          │
                    │   INSERT ... tag = 'gpu'      │
                    └───────────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │        GPU WORKER             │
                    │  SELECT ... WHERE tag = 'gpu' │
                    └───────────────────────────────┘
```

### Worker Tag Filtering

```
                         ┌──────────────────┐
                         │   worker_queue   │
                         └──────────────────┘
                                    │
            ┌───────────────────────┼───────────────────────┐
            │                       │                       │
            ▼                       ▼                       ▼
   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
   │  DefaultOnly    │    │  Tags(["gpu"])  │    │ DefaultAnd(     │
   │                 │    │                 │    │   ["gpu"])      │
   │ WHERE tag IS    │    │ WHERE tag IN    │    │ WHERE tag IS    │
   │       NULL      │    │   ('gpu')       │    │   NULL OR tag   │
   │                 │    │                 │    │   IN ('gpu')    │
   └─────────────────┘    └─────────────────┘    └─────────────────┘
            │                       │                       │
            ▼                       ▼                       ▼
   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
   │ Gets: SendEmail │    │ Gets: GPUTask   │    │ Gets: Both      │
   │ Skips: GPUTask  │    │ Skips: SendEmail│    │                 │
   └─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## API Design

### Activity Registration (Unchanged)

```rust
// No tags at registration - all activities registered the same way
ActivityRegistry::builder()
    .register("SendEmail", handler)
    .register("GPUInference", handler)
    .register("ProcessData", handler)
    .register_typed::<Input, Output, _, _>("TypedActivity", typed_handler)
    .build();
```

### Worker Configuration

```rust
// Only default (no tag) - backward compatible default
RuntimeOptions::default()

// Explicit default only
RuntimeOptions::default()
    .with_worker_tags(TagFilter::default_only())

// Only specific tags (NOT default)
RuntimeOptions::default()
    .with_worker_tags(TagFilter::tags(["gpu", "high-memory"]))

// Default + specific tags
RuntimeOptions::default()
    .with_worker_tags(TagFilter::default_and(["gpu"]))

// Multi-tag worker (up to 5 tags)
RuntimeOptions::default()
    .with_worker_tags(TagFilter::tags(["gpu", "llm", "video-encode"]))

// No activities - orchestrator-only mode
RuntimeOptions::default()
    .with_worker_tags(TagFilter::none())
```

### Scheduling with Tags

```rust
// No tag - goes to default queue (backward compatible)
let result = ctx.schedule_activity("SendEmail", input).await?;

// With tag - routes to workers subscribed to "gpu"
let result = ctx.schedule_activity("GPUInference", input)
    .with_tag("gpu")
    .await?;

// Dynamic tag for worker affinity (e.g., pin to a specific build machine)
let result = ctx.schedule_activity("CompileProject", input)
    .with_tag("build-machine-03")
    .await?;

// Typed variant - .with_tag() chains before .await
let embeddings: Vec<f32> = ctx.schedule_activity_typed("GenerateEmbeddings", &text)
    .with_tag("llm")
    .await?;
```

### Tags + Sessions (Orthogonal)

```rust
// Route to GPU pool (tag) AND pin to a specific GPU worker (session)
let result = ctx.schedule_activity_on_session("TrainModel", input, "session-42")
    .with_tag("gpu")
    .await?;
```

### Tags + Retry

```rust
// Retry with tag - internally each attempt uses .with_tag()
let result = ctx.schedule_activity_with_retry_tagged(
    "CompileProject",
    input,
    "build-machine",
    RetryPolicy::new(3),
).await?;
```

### Tags + Join / Select (Full Composability)

```rust
// Fan-out to different worker pools
let gpu_task = ctx.schedule_activity("GPUInference", model_input).with_tag("gpu");
let cpu_task = ctx.schedule_activity("CPUPostProcess", raw_data);  // default queue

// join works - both are DurableFuture
let (gpu_result, cpu_result) = ctx.join2(gpu_task, cpu_task).await;

// select2 works - race GPU vs timeout
let fast_path = ctx.schedule_activity("GPUInference", input).with_tag("gpu");
let deadline = ctx.schedule_timer(Duration::from_secs(60));
match ctx.select2(fast_path, deadline).await {
    Either2::First(result) => { /* GPU finished */ }
    Either2::Second(()) => { /* timed out, GPU activity auto-cancelled via Drop */ }
}
```

### Protecting Against Stranded Activities

```rust
// If no "build-machine" worker exists, the activity sits in queue forever.
// Protect with an orchestration-level timeout:
let build = ctx.schedule_activity("CompileProject", input)
    .with_tag("build-machine");
let timeout = async {
    ctx.schedule_timer(Duration::from_secs(300)).await;
    Err::<String, String>("timeout: no build worker responded in 5 minutes".into())
};

match ctx.select2(build, timeout).await {
    Either2::First(result) => result?,
    Either2::Second(Err(e)) => return Err(e),
    _ => unreachable!(),
};
```

### TagFilter Type

```rust
/// Maximum number of tags a worker can subscribe to.
pub const MAX_WORKER_TAGS: usize = 5;

/// Filter for which activity tags a worker will process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagFilter {
    /// Process only activities with no tag (default).
    DefaultOnly,
    
    /// Process only activities with the specified tags (NOT default).
    /// Limited to MAX_WORKER_TAGS (5) tags.
    Tags(HashSet<String>),
    
    /// Process activities with no tag AND the specified tags.
    /// Limited to MAX_WORKER_TAGS (5) tags.
    DefaultAnd(HashSet<String>),
    
    /// Don't process any activities (orchestrator-only mode).
    None,
}

impl Default for TagFilter {
    fn default() -> Self {
        TagFilter::DefaultOnly  // Backward compatible
    }
}

impl TagFilter {
    /// Create a filter for specific tags only.
    /// 
    /// # Panics
    /// Panics if more than MAX_WORKER_TAGS (5) tags are provided.
    pub fn tags<I, S>(tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let set: HashSet<String> = tags.into_iter().map(Into::into).collect();
        assert!(
            set.len() <= MAX_WORKER_TAGS,
            "Worker can subscribe to at most {} tags, got {}",
            MAX_WORKER_TAGS,
            set.len()
        );
        TagFilter::Tags(set)
    }
    
    /// Create a filter for default plus specific tags.
    /// 
    /// # Panics
    /// Panics if more than MAX_WORKER_TAGS (5) tags are provided.
    pub fn default_and<I, S>(tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let set: HashSet<String> = tags.into_iter().map(Into::into).collect();
        assert!(
            set.len() <= MAX_WORKER_TAGS,
            "Worker can subscribe to at most {} tags, got {}",
            MAX_WORKER_TAGS,
            set.len()
        );
        TagFilter::DefaultAnd(set)
    }
    
    pub fn default_only() -> Self { TagFilter::DefaultOnly }
    pub fn none() -> Self { TagFilter::None }
    
    pub fn matches(&self, tag: Option<&str>) -> bool {
        match (self, tag) {
            (TagFilter::None, _) => false,
            (TagFilter::DefaultOnly, None) => true,
            (TagFilter::DefaultOnly, Some(_)) => false,
            (TagFilter::Tags(set), None) => false,
            (TagFilter::Tags(set), Some(t)) => set.contains(t),
            (TagFilter::DefaultAnd(set), None) => true,
            (TagFilter::DefaultAnd(set), Some(t)) => set.contains(t),
        }
    }
}
```

---

## Implementation Mechanism: Mutate-After-Emit

### Why Not a Builder?

`DurableFuture` directly implements `Future` and is used with `ctx.join()` and `ctx.select2()`, which accept any `Future` (not `IntoFuture`). A builder that only implements `IntoFuture` wouldn't compose with these methods. Making a builder lazily emit actions on first `poll()` would break deterministic replay — action emission order would depend on poll order, not program order.

### How with_tag() Works

When `schedule_activity()` is called, it pushes an `Action::CallActivity` into `emitted_actions: Vec<(u64, Action)>` on `CtxInner` and returns a `DurableFuture` holding the `token`. The replay engine doesn't drain `emitted_actions` until the orchestration future yields `Poll::Pending`. So there's a safe mutation window:

```
schedule_activity("Build", input)     .with_tag("gpu")              .await
         │                                    │                        │
         ▼                                    ▼                        ▼
  1. Lock CtxInner                   3. Lock CtxInner           5. Future::poll
  2. Push Action::CallActivity       4. Find action by             returns
     { tag: None } ◄── initial          self.token in              Poll::Pending
     token = 7                          emitted_actions,
     Return DurableFuture               set tag to              6. Replay engine
       { token: 7 }                     Some("gpu") ◄── ✓         drains
                                     Return self                   emitted_actions
                                                                   (tag is set)
```

```rust
impl<T> DurableFuture<T> {
    pub fn with_tag(self, tag: impl Into<String>) -> Self {
        let tag = tag.into();
        let mut inner = self.ctx.inner.lock().expect("Mutex should not be poisoned");
        
        // Find the action we emitted (by our token) and patch its tag
        for (token, action) in inner.emitted_actions.iter_mut() {
            if *token == self.token {
                match action {
                    Action::CallActivity { tag: t, .. } => {
                        *t = Some(tag);
                    }
                    _ => panic!("with_tag() called on non-activity DurableFuture"),
                }
                break;
            }
        }
        drop(inner);
        self  // return self — same token, same future, full composability
    }
}
```

### Safety Properties

1. **Timing** — `emitted_actions` is drained only when `drain_emitted_actions()` is called after the orchestration future returns `Poll::Pending`. The `.with_tag()` call happens during the same poll cycle, before yield.

2. **Ordering** — `with_tag()` modifies the already-emitted action in-place. It doesn't change emission order. The replay engine processes actions in FIFO order, so determinism is preserved.

3. **Replay safety** — On replay, the stored `ActivityScheduled` event already has `tag: Some("gpu")`. The replayed code emits the same action with the same tag → match succeeds.

4. **Composability** — Returns `self`, so it chains with everything:
   - `schedule_activity(...).with_tag("gpu").await?` — direct await
   - `schedule_activity(...).with_tag("gpu").map(...)` — transform output
   - `ctx.join2(a.with_tag("gpu"), b.with_tag("cpu"))` — fan-out
   - `ctx.select2(a.with_tag("gpu"), timer)` — race with timeout
   - Drop cancellation still works (same token, same `ScheduleKind`)

### Future Opportunity: Unified Builder Pattern

The `with_tag()` mutate-after-emit pattern could also be applied to sessions (`.with_session()`) and potentially retries (`.with_retry()`). This would collapse the current 8-method combinatorial explosion:

```rust
// Current: 8 methods
schedule_activity(name, input)
schedule_activity_typed(name, input)
schedule_activity_on_session(name, input, session)
schedule_activity_on_session_typed(name, input, session)
schedule_activity_with_retry(name, input, policy)
schedule_activity_with_retry_typed(name, input, policy)
schedule_activity_with_retry_on_session(name, input, policy, session)
schedule_activity_with_retry_on_session_typed(name, input, policy, session)

// Future: 2 base methods + chainable modifiers
schedule_activity(name, input)          // → DurableFuture<Result<String, String>>
schedule_activity_typed(name, input)    // → DurableFuture<Result<Out, String>>
    .with_tag("gpu")                    // → DurableFuture (non-terminal, any order)
    .with_session("s1")                 // → DurableFuture (non-terminal, any order)
    .with_retry(policy)                 // → impl Future (terminal, must be last)
    .await?;
```

This unification is out of scope for this proposal but is a natural follow-up.

---

## Data Model Changes

### Action::CallActivity

```rust
CallActivity {
    scheduling_event_id: u64,
    name: String,
    input: String,
    session_id: Option<String>,
    tag: Option<String>,  // NEW — set by .with_tag(), default None
},
```

### EventKind::ActivityScheduled

```rust
#[serde(rename = "ActivityScheduled")]
ActivityScheduled { 
    name: String, 
    input: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    tag: Option<String>,  // NEW — from .with_tag() at schedule time
},
```

### WorkItem::ActivityExecute

```rust
ActivityExecute {
    instance: String,
    execution_id: u64,
    id: u64,
    name: String,
    input: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    tag: Option<String>,  // NEW — for queue routing
},
```

### ActivityContext

```rust
pub struct ActivityContext {
    // ... existing fields ...
    tag: Option<String>,  // NEW — plumbed from WorkItem::ActivityExecute
}

impl ActivityContext {
    /// Returns the tag this activity was routed with.
    pub fn tag(&self) -> Option<&str> {
        self.tag.as_deref()
    }
}
```

Note: No changes to `ActivityRegistry` — tags are not stored at registration.

---

## Provider Changes

### Schema Migration

```sql
-- Migration: 20240109000000_add_worker_tag.sql

-- Add tag column to worker_queue (nullable = default tag)
ALTER TABLE worker_queue ADD COLUMN tag TEXT;

-- Index for efficient tag filtering
CREATE INDEX IF NOT EXISTS idx_worker_tag ON worker_queue(tag, visible_at, lock_token);
```

### Provider Trait Changes

```rust
/// Fetch a work item matching the session config and tag filter.
async fn fetch_work_item(
    &self,
    lock_timeout: Duration,
    poll_timeout: Duration,
    session: Option<&SessionFetchConfig>,
    tag_filter: &TagFilter,  // NEW parameter
) -> Result<Option<(WorkItem, String, u32)>, ProviderError>;
```

### SQLite Implementation

Tag filter composes with session filter via AND in the WHERE clause:

```rust
async fn fetch_work_item(
    &self,
    lock_timeout: Duration,
    _poll_timeout: Duration,
    session: Option<&SessionFetchConfig>,
    tag_filter: &TagFilter,
) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
    // Short-circuit for orchestrator-only mode
    if matches!(tag_filter, TagFilter::None) {
        return Ok(None);
    }
    
    let tag_condition = match tag_filter {
        TagFilter::None => unreachable!(),
        TagFilter::DefaultOnly => "q.tag IS NULL".to_string(),
        TagFilter::Tags(tags) => {
            let placeholders: Vec<_> = tags.iter()
                .map(|t| format!("'{}'", t.replace('\'', "''")))
                .collect();
            format!("q.tag IN ({})", placeholders.join(", "))
        }
        TagFilter::DefaultAnd(tags) => {
            let placeholders: Vec<_> = tags.iter()
                .map(|t| format!("'{}'", t.replace('\'', "''")))
                .collect();
            format!("(q.tag IS NULL OR q.tag IN ({}))", placeholders.join(", "))
        }
    };
    
    // Session-aware query (session: Some(config)):
    // SELECT ... FROM worker_queue q
    //   LEFT JOIN sessions s ON s.session_id = q.session_id AND s.locked_until > ?1
    //   WHERE q.visible_at <= ?1
    //     AND (q.lock_token IS NULL OR q.locked_until <= ?1)
    //     AND (q.session_id IS NULL OR s.worker_id = ?2 OR s.session_id IS NULL)
    //     AND {tag_condition}           ◄── NEW: composed via AND
    //   ORDER BY q.id LIMIT 1
    
    // Non-session query (session: None):
    // SELECT ... FROM worker_queue q
    //   WHERE q.visible_at <= ?1
    //     AND (q.lock_token IS NULL OR q.locked_until <= ?1)
    //     AND q.session_id IS NULL
    //     AND {tag_condition}           ◄── NEW: composed via AND
    //   ORDER BY q.id LIMIT 1
}
```

When inserting `WorkItem::ActivityExecute` into `worker_queue` (in `ack_orchestration_item`), extract `tag` from the work item and store it in the dedicated column — same pattern as `session_id`, `instance_id`, `activity_id`.

---

## Runtime Changes

### RuntimeOptions

```rust
pub struct RuntimeOptions {
    // ... existing fields ...
    
    /// Tag filter for worker dispatcher.
    /// Default: `TagFilter::DefaultOnly`
    pub worker_tags: TagFilter,
}

impl RuntimeOptions {
    pub fn with_worker_tags(mut self, filter: TagFilter) -> Self {
        self.worker_tags = filter;
        self
    }
}
```

### Worker Dispatcher

Pass `&rt.options.worker_tags` to `fetch_work_item`:

```rust
let (item, token, attempt_count) = match rt.history_store.fetch_work_item(
    rt.options.worker_lock_timeout,
    rt.options.dispatcher_long_poll_timeout,
    session_config.as_ref(),
    &rt.options.worker_tags,  // NEW
).await? { ... };
```

### Replay Engine

Propagate `tag` from `Action::CallActivity` at two points:

1. **`action_to_event`** — `Action::CallActivity { tag, .. }` → `EventKind::ActivityScheduled { tag, .. }`
2. **Action→WorkItem conversion** — `Action::CallActivity { tag, .. }` → `WorkItem::ActivityExecute { tag, .. }`

---

## Stranded Activities

### The Problem

Unlike unregistered activities (which get picked up by a worker, abandoned, and eventually poisoned), tagged activities with **no matching worker** are never fetched — they sit in the queue indefinitely. No worker sees them, so no lock timeout, no poison detection.

### Recommended Mitigation

Use orchestration-level timeouts:

```rust
// Option 1: select2 with timer
let build = ctx.schedule_activity("Build", input).with_tag("build-machine");
let timeout = async {
    ctx.schedule_timer(Duration::from_secs(300)).await;
    Err::<String, String>("timeout: no build worker responded".into())
};
match ctx.select2(build, timeout).await {
    Either2::First(result) => result?,
    Either2::Second(Err(e)) => return Err(e),
    _ => unreachable!(),
};

// Option 2: retry with per-attempt timeout
let result = ctx.schedule_activity_with_retry_tagged(
    "Build", input, "build-machine",
    RetryPolicy::new(1).with_timeout(Duration::from_secs(300)),
).await?;
```

### Why Not Provider-Level TTL?

A provider-level `expires_at` column with background sweep would add complexity to the Provider trait, require a sweep task, and create edge cases around timing. The orchestration-level approach is:
- Simpler (no provider changes)
- More flexible (different timeouts per activity)
- Consistent with existing timeout patterns
- Provides clear error messages

Operators should monitor queue depth by tag to detect stranded activities proactively.

---

## Observability

### Metrics

```rust
duroxide_activity_started_total{activity_name="GPUInference", tag="gpu"}
duroxide_activity_completed_total{activity_name="SendEmail", tag="default"}
duroxide_activity_duration_seconds{activity_name="Process", tag="worker-001"}
```

### Tracing

```rust
tracing::debug!(
    target: "duroxide::runtime",
    instance_id = %ctx.instance,
    activity_name = %ctx.activity_name,
    tag = %ctx.tag.as_deref().unwrap_or("default"),
    "Activity started"
);
```

### ActivityContext

```rust
impl ActivityContext {
    /// Returns the tag this activity was routed with.
    pub fn tag(&self) -> Option<&str> {
        self.tag.as_deref()
    }
}
```

---

## Backward Compatibility

| Scenario | Behavior |
|----------|----------|
| Existing activity registration | No change needed |
| Existing RuntimeOptions | `worker_tags` = `TagFilter::DefaultOnly` |
| Existing orchestration code | No `.with_tag()` = None (default queue) |
| Old events without `tag` field | Deserialize as `tag: None` via `#[serde(default)]` |
| Existing Provider implementations | Must update `fetch_work_item` signature |

**Migration sequence:**
1. Deploy schema migration (add `tag` column, defaults to NULL)
2. Deploy new code — all existing activities have NULL tag, workers use DefaultOnly
3. Start adding `.with_tag()` calls and deploy specialized workers

---

## Cross-SDK Adoption

All SDKs share a JSON wire format with `#[serde(tag = "type")]`. Tag is added as an optional field on existing variants — no new type variants needed.

### Wire Format

```json
{ "type": "activity", "name": "Build", "input": "...", "tag": "gpu" }
{ "type": "activity", "name": "Build", "input": "...", "tag": "gpu", "sessionId": "s1" }
{ "type": "activityWithRetry", "name": "Build", "input": "...", "retry": {...}, "tag": "gpu" }
```

### Per-SDK APIs

| SDK | Mechanism | Example |
|-----|-----------|---------|
| **Rust** | `.with_tag()` on `DurableFuture` (mutate-after-emit) | `.with_tag("gpu").await?` |
| **Java** | `.withTag()` on `DuroxideTask` (existing fluent pattern) | `.withTag("gpu").get()` |
| **.NET** | `.WithTag()` on `DuroxideTask` (new fluent method) | `await .WithTag("gpu")` |
| **Node** | Options bag `{ tag: "gpu" }` | `yield ctx.scheduleActivity("A", input, { tag: "gpu" })` |
| **Python** | Keyword arg `tag="gpu"` | `yield ctx.schedule_activity("A", input, tag="gpu")` |

Node and Python use generator functions where `scheduleActivity` returns a plain descriptor object (dict) that is immediately `yield`ed. There's no live handle to configure — the options bag / kwarg is the idiomatic approach.

Java already has fluent methods on `DuroxideTask` (`.withRetry()`, `.onSession()`), so `.withTag()` is a natural fit.

### Rust-Side SDK Type Changes

Each SDK's `types.rs` adds `tag: Option<String>` to the `Activity` and `ActivityWithRetry` variants:

```rust
// .NET & Node (already use optional session_id on variant):
Activity { name, input, session_id: Option<String>, tag: Option<String> }
ActivityWithRetry { name, input, retry, session_id: Option<String>, tag: Option<String> }

// Java & Python (have separate session variants — add tag to all):
Activity { name, input, tag: Option<String> }
ActivityWithSession { name, input, sessionId, tag: Option<String> }
ActivityWithRetry { name, input, retry, tag: Option<String> }
ActivityWithRetryOnSession { name, input, retry, sessionId, tag: Option<String> }
```

---

## Build Machine Example

```rust
// === ORCHESTRATION ===
let orchestrations = OrchestrationRegistry::builder()
    .register("CI-Pipeline", |ctx: OrchestrationContext, repo: String| async move {
        // Lint runs anywhere (default worker)
        let lint = ctx.schedule_activity("RunLint", repo.clone()).await?;

        // Build MUST run on a build machine
        let artifact = ctx.schedule_activity("CompileRelease", repo.clone())
            .with_tag("build-machine")
            .await?;

        // Tests run on build machines too
        let tests = ctx.schedule_activity("RunTests", artifact.clone())
            .with_tag("build-machine")
            .await?;

        // Deploy runs anywhere (default worker)
        let deployed = ctx.schedule_activity("Deploy", artifact).await?;
        Ok(deployed)
    })
    .build();

// === BUILD WORKER (dedicated machine with compilers, SDKs, caches) ===
let build_activities = ActivityRegistry::builder()
    .register("CompileRelease", |_ctx, repo: String| async move {
        Ok("artifact-v1.2.3".into())
    })
    .register("RunTests", |_ctx, artifact: String| async move {
        Ok("all tests passed".into())
    })
    .build();

let build_runtime = Runtime::start_with_options(
    store.clone(),
    Arc::new(build_activities),
    OrchestrationRegistry::builder().build(),
    RuntimeOptions::default()
        .with_worker_tags(TagFilter::tags(["build-machine"])),
).await;

// === DEFAULT WORKER (general purpose) ===
let general_activities = ActivityRegistry::builder()
    .register("RunLint", |_ctx, repo: String| async move { Ok("clean".into()) })
    .register("Deploy", |_ctx, artifact: String| async move { Ok("deployed".into()) })
    .build();

let general_runtime = Runtime::start_with_options(
    store.clone(),
    Arc::new(general_activities),
    orchestrations,
    RuntimeOptions::default(),  // TagFilter::DefaultOnly
).await;
```

---

## Test Plan

### Test File Layout

| File | Scope | Count |
|------|-------|-------|
| `src/providers/mod.rs` (inline `#[cfg(test)]`) | TagFilter unit tests | 6 |
| `src/lib.rs` (inline `#[cfg(test)]`) | DurableFuture with_tag + event serde | 6 |
| `src/provider_validation/tag_routing.rs` | Provider contract tests | 17 |
| `tests/sqlite_provider_validations.rs` | SQLite runner for above | 17 |
| `tests/activity_tag_tests.rs` | Integration (end-to-end) | 38 |
| **Total** | | **~67** |

### Unit Tests: TagFilter (`src/providers/mod.rs`)

| Test | Description |
|------|-------------|
| `default_only_matches_none_rejects_tag` | `DefaultOnly.matches(None)` → true, `.matches(Some("gpu"))` → false |
| `tags_matches_member_rejects_others` | `Tags(["gpu"]).matches(Some("gpu"))` → true, `(Some("cpu"))` → false, `(None)` → false |
| `default_and_matches_both` | `DefaultAnd(["gpu"]).matches(None)` → true, `(Some("gpu"))` → true, `(Some("cpu"))` → false |
| `none_matches_nothing` | `None.matches(None)` → false, `None.matches(Some("x"))` → false |
| `default_impl_is_default_only` | `TagFilter::default() == DefaultOnly` |
| `panics_over_max_tags` | `Tags` with 6 tags panics, `Tags` with 5 succeeds |

### Unit Tests: DurableFuture + Event Serde (`src/lib.rs`)

| Test | Description |
|------|-------------|
| `with_tag_sets_tag_on_emitted_action` | After `.with_tag("gpu")`, action in `emitted_actions` has `tag: Some("gpu")` |
| `no_with_tag_leaves_none` | Default activity has `tag: None` |
| `with_tag_on_non_activity_panics` | `.with_tag()` on timer DurableFuture panics |
| `activity_scheduled_with_tag_roundtrips` | Serialize→deserialize preserves `tag: Some("gpu")` |
| `activity_scheduled_none_tag_omitted_in_json` | `tag: None` → no `tag` key in JSON |
| `activity_scheduled_missing_tag_deserializes_as_none` | Old JSON without `tag` → `tag: None` |

### Provider Validation Tests (`src/provider_validation/tag_routing.rs`)

**Basic tag storage & filtering:**

| Test | Description |
|------|-------------|
| `enqueue_with_tag_stores_tag` | `WorkItem { tag: Some("gpu") }` → fetchable with `Tags(["gpu"])` |
| `enqueue_null_tag_stores_null` | `WorkItem { tag: None }` → fetchable with `DefaultOnly` |
| `fetch_default_only_skips_tagged` | Enqueue tagged + untagged. `DefaultOnly` → only untagged |
| `fetch_tags_skips_untagged` | Enqueue tagged + untagged. `Tags(["gpu"])` → only tagged |
| `fetch_tags_skips_non_matching` | Enqueue "gpu" + "cpu". `Tags(["gpu"])` → only "gpu" |
| `fetch_default_and_returns_both` | Enqueue tagged + untagged. `DefaultAnd(["gpu"])` → both |
| `fetch_none_returns_nothing` | `TagFilter::None` → `Ok(None)` always |

**Multi-tag worker (OR within tag set):**

| Test | Description |
|------|-------------|
| `multi_tag_worker_matches_any` | Enqueue "gpu", "build", "cpu". `Tags(["gpu", "build"])` → gets "gpu" and "build", skips "cpu" |
| `multi_tag_default_and_matches_any_plus_null` | Enqueue "gpu", "build", None. `DefaultAnd(["gpu", "build"])` → gets all three |
| `multi_tag_ordering_is_fifo` | `Tags(["gpu", "build"])` → items returned in insertion order |

**Tag + session composition (AND):**

| Test | Description |
|------|-------------|
| `tag_and_session_both_filter` | Tag filter AND session filter both apply |
| `tag_filter_and_session_filter_reject_independently` | Tag matches but session rejects → NOT returned |
| `tag_and_no_session_config` | `Tags(["gpu"])` with session=None → only non-session tagged items |
| `tag_and_session_null_tag_with_session` | `DefaultOnly` + session → gets non-tagged session items |

**Tag + lock/visibility composition (AND):**

| Test | Description |
|------|-------------|
| `tag_filter_respects_lock` | Tagged locked item → not returned |
| `tag_filter_respects_visibility` | Tagged item with future `visible_at` → not returned |
| `tagged_item_preserves_attempt_count` | Fetch + abandon tagged → attempt_count increments |

### Integration Tests (`tests/activity_tag_tests.rs`)

**Basic routing (7 tests):**

| Test | Description |
|------|-------------|
| `default_worker_processes_untagged` | `DefaultOnly` worker completes untagged activity |
| `default_worker_ignores_tagged` | `DefaultOnly` worker does NOT see tagged activity |
| `tagged_worker_processes_matching` | `Tags(["gpu"])` worker completes `tag: Some("gpu")` |
| `tagged_worker_ignores_untagged` | `Tags(["gpu"])` does NOT see `tag: None` |
| `tagged_worker_ignores_non_matching` | `Tags(["gpu"])` does NOT see `tag: Some("cpu")` |
| `mixed_worker_processes_both` | `DefaultAnd(["gpu"])` handles both |
| `orchestrator_only_processes_nothing` | `TagFilter::None` — no activities dispatched |

**Multi-tag worker (3 tests):**

| Test | Description |
|------|-------------|
| `worker_with_multiple_tags_gets_all_matching` | `Tags(["gpu", "build"])` processes both |
| `worker_with_multiple_tags_skips_non_matching` | Same worker ignores `tag: Some("deploy")` |
| `default_and_multiple_tags` | `DefaultAnd(["gpu", "build"])` gets untagged + gpu + build |

**Multi-worker separation (2 tests):**

| Test | Description |
|------|-------------|
| `two_workers_different_tags_no_overlap` | GPU + build workers, each gets only their own |
| `three_pool_separation` | Default + GPU + build, each gets exactly one |

**Tags + sessions — orthogonality (3 tests):**

| Test | Description |
|------|-------------|
| `tag_and_session_route_correctly` | `.with_tag("gpu")` + `.on_session("s1")` → GPU worker, pinned |
| `same_session_different_tags` | Same session, different tags → correct workers |
| `session_without_tag_unaffected` | Session only, no tag → DefaultOnly worker |

**Tags + retry (3 tests):**

| Test | Description |
|------|-------------|
| `retry_preserves_tag_across_attempts` | Both retry attempts go to tagged worker |
| `retry_with_tag_and_session` | All attempts: same tag, same session |
| `retry_timeout_with_tag` | No matching worker → timeout fires, error returned |

**Tags + poison handling (3 tests):**

| Test | Description |
|------|-------------|
| `tagged_unregistered_activity_poisons` | Tagged, worker has no handler → abandon → poison |
| `tagged_activity_max_attempts_exhausted` | Handler fails repeatedly → poison error |
| `poison_error_includes_tag_context` | Poison error tracing shows tag |

**Tags + cancellation (2 tests):**

| Test | Description |
|------|-------------|
| `dropped_tagged_future_triggers_cancellation` | `select2(tagged, timer)` → timer wins → cancel |
| `cancelled_tagged_activity_frees_worker` | After cancel, worker picks up next item |

**Tags + replay (3 tests):**

| Test | Description |
|------|-------------|
| `tag_in_event_history` | `ActivityScheduled` has `tag: Some("gpu")` |
| `tag_preserved_on_replay` | Restart → replay → re-routes to correct worker |
| `backward_compat_no_tag_in_old_events` | Old event without `tag` field → `None` |

**Tags + join/select composition (3 tests):**

| Test | Description |
|------|-------------|
| `join_with_mixed_tags` | `join2(a.with_tag("gpu"), b.with_tag("build"))` → both complete |
| `select2_tagged_vs_timer` | GPU worker wins → `Either2::First` |
| `select2_tagged_timeout` | No worker → timer wins → `Either2::Second` |

**Tags + typed activities (1 test):**

| Test | Description |
|------|-------------|
| `typed_activity_with_tag` | `schedule_activity_typed(...).with_tag("gpu")` → routes, deserializes |

**Tags + continue_as_new (1 test):**

| Test | Description |
|------|-------------|
| `tag_routing_survives_continue_as_new` | New execution uses `.with_tag()` → still routes |

**Edge cases (3 tests):**

| Test | Description |
|------|-------------|
| `empty_string_tag_treated_as_tag` | `.with_tag("")` → NOT treated as None |
| `with_tag_on_non_activity_panics` | Timer/sub-orch DurableFuture → panics |
| `tag_with_special_characters` | `"us-east/zone-1"` → stored and filtered correctly |

---

## Implementation Order

1. Add `TagFilter` type with `MAX_WORKER_TAGS` limit to `src/providers/mod.rs`
2. Add `tag: Option<String>` to `Action::CallActivity` in `src/lib.rs`
3. Add `tag: Option<String>` to `EventKind::ActivityScheduled` in `src/lib.rs`
4. Add `tag: Option<String>` to `WorkItem::ActivityExecute` in `src/providers/mod.rs`
5. Implement `DurableFuture::with_tag()` (mutate-after-emit) in `src/lib.rs`
6. Add `worker_tags: TagFilter` to `RuntimeOptions` in `src/runtime/mod.rs`
7. Update `Provider::fetch_work_item` signature — add `tag_filter` parameter
8. Add SQLite migration `20240109000000_add_worker_tag.sql`
9. Update SQLite `fetch_work_item` — compose tag filter in WHERE clause
10. Update SQLite `ack_orchestration_item` — extract tag to `worker_queue` column
11. Update worker dispatcher — pass `worker_tags` to `fetch_work_item`
12. Update `action_to_event` in replay engine — propagate tag
13. Update Action→WorkItem conversion in execution — propagate tag
14. Add `tag` to `ActivityContext` and expose accessor
15. Add observability (metrics label, tracing span attribute)
16. Update all existing call sites (`tag: None`, `&TagFilter::DefaultOnly`)
17. Write unit tests (TagFilter, DurableFuture, serde)
18. Write provider validation tests (`src/provider_validation/tag_routing.rs`)
19. Write integration tests (`tests/activity_tag_tests.rs`)
20. Update documentation (ORCHESTRATION-GUIDE, provider guides)
21. Update SDK type definitions (all 4 SDKs)

---

## Call Sites That Must Update

| Location | Change |
|----------|--------|
| `schedule_activity_internal` in `src/lib.rs` | `tag: None` in `Action::CallActivity` |
| `action_to_event` in `src/runtime/replay_engine.rs` | Propagate `tag` |
| `CallActivity` match in `src/runtime/execution.rs` | Propagate `tag` to `WorkItem` |
| `match_and_bind_schedule` in `src/runtime/replay_engine.rs` | Destructure `tag` in `ActivityScheduled` match |
| Worker dispatcher `fetch_work_item` call in `src/runtime/dispatchers/worker.rs` | Add `&rt.options.worker_tags` |
| Every test calling `fetch_work_item` | Add `&TagFilter::DefaultOnly` parameter |
| Every test constructing `ActivityScheduled` events | Add `tag: None` |
| Every test constructing `WorkItem::ActivityExecute` | Add `tag: None` |
| Every pattern match on `Action::CallActivity` | Add `tag` |

---

## Implementation Checklist

> **⚠️ MANDATORY:** An LLM agent implementing this proposal MUST complete every checkbox below, in order, before declaring the work complete. Skipping a phase or deferring a review is not permitted. Each phase ends with an adversarial code review — the agent must re-read its own changes critically, list problems, and fix them before proceeding.

---

### Phase 1: Core Types & Data Model

**Scope:** Add `TagFilter`, update `Action`, `EventKind`, `WorkItem` structs. No runtime changes yet — code should compile but tags have no effect.

- [ ] 1.1 Add `TagFilter` enum, `MAX_WORKER_TAGS`, constructors, `matches()`, `Default` impl to `src/providers/mod.rs`
- [ ] 1.2 Add `tag: Option<String>` to `Action::CallActivity` in `src/lib.rs`
- [ ] 1.3 Add `tag: Option<String>` to `EventKind::ActivityScheduled` in `src/lib.rs` (with `#[serde(skip_serializing_if, default)]`)
- [ ] 1.4 Add `tag: Option<String>` to `WorkItem::ActivityExecute` in `src/providers/mod.rs` (with `#[serde(skip_serializing_if, default)]`)
- [ ] 1.5 Update ALL pattern matches on `Action::CallActivity` across the codebase (set `tag: None` at construction, destructure `tag` at match sites)
- [ ] 1.6 Update ALL pattern matches on `EventKind::ActivityScheduled` across the codebase
- [ ] 1.7 Update ALL pattern matches on `WorkItem::ActivityExecute` across the codebase
- [ ] 1.8 Write TagFilter unit tests (6 tests in `src/providers/mod.rs` inline `#[cfg(test)]`)
- [ ] 1.9 Write event serde tests (3 tests — roundtrip with tag, None omits key, missing field deserializes as None)
- [ ] 1.10 Run: `cargo build --all-targets` — zero errors
- [ ] 1.11 Run: `cargo nt` — all existing tests pass (backward compat)
- [ ] 1.12 Run: `cargo clippy --all-targets --all-features` — zero warnings

**Phase 1 — Adversarial Code Review:**
- [ ] Re-read every changed file. For each change, ask: "Is there a pattern match I missed? A constructor I forgot? A test that constructs this type?"
- [ ] `grep -rn 'CallActivity' src/ tests/` — verify every occurrence has `tag`
- [ ] `grep -rn 'ActivityScheduled' src/ tests/` — verify every occurrence has `tag`
- [ ] `grep -rn 'ActivityExecute' src/ tests/` — verify every occurrence has `tag`
- [ ] Verify serde backward compat: construct JSON without `tag` field, deserialize, assert `tag == None`
- [ ] List all problems found, fix them, re-run `cargo nt` and `cargo clippy`

---

### Phase 2: DurableFuture Mutation & Replay Engine Propagation

**Scope:** Implement `with_tag()` on `DurableFuture`, propagate tag through `action_to_event` and Action→WorkItem conversion. Tags now flow from orchestration code into event history and work items.

- [ ] 2.1 Implement `DurableFuture::with_tag(self, tag: impl Into<String>) -> Self` in `src/lib.rs` (mutate-after-emit: lock `ctx.inner`, find action by `self.token` in `emitted_actions`, set tag, return self)
- [ ] 2.2 Update `action_to_event` in `src/runtime/replay_engine.rs` — propagate `tag` from `Action::CallActivity` into `EventKind::ActivityScheduled`
- [ ] 2.3 Update Action→WorkItem conversion in `src/runtime/execution.rs` — propagate `tag` from `Action::CallActivity` into `WorkItem::ActivityExecute`
- [ ] 2.4 Update `match_and_bind_schedule` in `src/runtime/replay_engine.rs` — destructure `tag` in `ActivityScheduled` match arm
- [ ] 2.5 Write DurableFuture unit tests (3 tests — with_tag sets tag, no with_tag is None, with_tag on non-activity panics)
- [ ] 2.6 Run: `cargo build --all-targets` — zero errors
- [ ] 2.7 Run: `cargo nt` — all existing tests pass
- [ ] 2.8 Run: `cargo clippy --all-targets --all-features` — zero warnings

**Phase 2 — Adversarial Code Review:**
- [ ] Re-read `DurableFuture::with_tag()`: Does it handle the case where `emitted_actions` has already been drained? (It shouldn't be possible, but verify the panic path)
- [ ] Re-read `action_to_event`: Is `tag` propagated for ALL activity-related events, not just `ActivityScheduled`?
- [ ] Re-read Action→WorkItem: Does the tag survive serialization into the work item JSON?
- [ ] Verify `with_tag` returns `self` with same token (composability preserved)
- [ ] Check: does `with_tag` work when called on a `DurableFuture` returned by `schedule_activity_typed`?
- [ ] List all problems found, fix them, re-run `cargo nt` and `cargo clippy`

---

### Phase 3: Provider & SQLite Implementation

**Scope:** Schema migration, `Provider::fetch_work_item` signature change, SQLite tag filtering, tag extraction on enqueue. Tags are now stored and filtered at the provider level.

- [ ] 3.1 Create migration file `migrations/20240109000000_add_worker_tag.sql` (`ALTER TABLE worker_queue ADD COLUMN tag TEXT; CREATE INDEX ...`)
- [ ] 3.2 Update `Provider::fetch_work_item` trait signature — add `tag_filter: &TagFilter` parameter after `session`
- [ ] 3.3 Update SQLite `fetch_work_item` — compose `tag_condition` with existing session/lock/visibility WHERE clauses via AND
- [ ] 3.4 Update SQLite `ack_orchestration_item` — extract `tag` from `WorkItem::ActivityExecute` JSON and store in `worker_queue.tag` column
- [ ] 3.5 Update ALL existing call sites of `fetch_work_item` (production code + tests) — pass `&TagFilter::DefaultOnly`
- [ ] 3.6 Write provider validation tests in `src/provider_validation/tag_routing.rs` (17 tests: basic filtering, multi-tag worker, tag+session AND, tag+lock AND, tag+visibility AND, attempt_count)
- [ ] 3.7 Register tag_routing module in `src/provider_validation/mod.rs` and re-export in `src/provider_validations.rs`
- [ ] 3.8 Add SQLite runner tests in `tests/sqlite_provider_validations.rs` — call all 17 validation functions
- [ ] 3.9 Run: `cargo build --all-targets` — zero errors
- [ ] 3.10 Run: `cargo nt` — all tests pass (existing + new provider validations)
- [ ] 3.11 Run: `cargo clippy --all-targets --all-features` — zero warnings

**Phase 3 — Adversarial Code Review:**
- [ ] Re-read SQLite `fetch_work_item`: Does `TagFilter::None` short-circuit BEFORE the query? Does `DefaultOnly` produce `q.tag IS NULL`? Does `Tags` use proper SQL escaping (single-quote doubling)?
- [ ] Re-read `ack_orchestration_item`: Is the `tag` column populated from the WorkItem JSON on INSERT? What happens if `tag` is None — does it INSERT NULL?
- [ ] Check: Do the provider validation tests cover the case where multiple items exist with different tags and the filter should return them in FIFO order?
- [ ] Check: Does the migration apply cleanly on top of `20240108_add_custom_status.sql`?
- [ ] Check: Is there SQL injection risk via tag values? (Tags are user-provided strings)
- [ ] Check: Does the session-aware query path AND the non-session query path both include `tag_condition`?
- [ ] `grep -rn 'fetch_work_item' src/ tests/` — verify every call site passes `tag_filter`
- [ ] List all problems found, fix them, re-run `cargo nt` and `cargo clippy`

---

### Phase 4: Runtime Wiring

**Scope:** `RuntimeOptions.worker_tags`, worker dispatcher plumbing, `ActivityContext.tag()` accessor, observability. Tags are now fully functional end-to-end.

- [ ] 4.1 Add `worker_tags: TagFilter` to `RuntimeOptions` in `src/runtime/mod.rs` (default `TagFilter::DefaultOnly`)
- [ ] 4.2 Add `with_worker_tags(mut self, filter: TagFilter) -> Self` builder method
- [ ] 4.3 Update worker dispatcher in `src/runtime/dispatchers/worker.rs` — pass `&rt.options.worker_tags` to `fetch_work_item`
- [ ] 4.4 Add `tag: Option<String>` to `ActivityContext` in `src/lib.rs`, expose via `pub fn tag(&self) -> Option<&str>`
- [ ] 4.5 Update worker dispatcher — plumb `tag` from `WorkItem::ActivityExecute` into `ActivityContext` construction
- [ ] 4.6 Add `tag` label to activity metrics (`duroxide_activity_started_total`, `duroxide_activity_completed_total`, `duroxide_activity_duration_seconds`) — use `tag.as_deref().unwrap_or("default")`
- [ ] 4.7 Add `tag` attribute to activity tracing spans in worker dispatcher
- [ ] 4.8 Run: `cargo build --all-targets` — zero errors
- [ ] 4.9 Run: `cargo nt` — all tests pass
- [ ] 4.10 Run: `cargo clippy --all-targets --all-features` — zero warnings

**Phase 4 — Adversarial Code Review:**
- [ ] Re-read worker dispatcher: Is `worker_tags` cloned or borrowed? Is it passed by reference to avoid unnecessary allocation per poll cycle?
- [ ] Re-read `ActivityContext` construction: Is `tag` threaded through from the `WorkItem` destructure? Or is it lost somewhere?
- [ ] Check: Does `TagFilter::None` actually prevent the worker dispatcher from polling? Or does it poll and get `Ok(None)` every cycle? (Both work, but the short-circuit is more efficient)
- [ ] Check: Are metrics labels consistent — does the tracing span use the same `tag` value as the metric label?
- [ ] Check: Does `RuntimeOptions::default()` produce `worker_tags: TagFilter::DefaultOnly`?
- [ ] Check: If observability feature is disabled (`#[cfg(feature = "observability")]`), do the metric/tracing additions compile out cleanly?
- [ ] List all problems found, fix them, re-run `cargo nt` and `cargo clippy`

---

### Phase 5: Integration Tests

**Scope:** Write all 38 integration tests in `tests/activity_tag_tests.rs`. These exercise the full runtime: orchestration → dispatch → worker → completion.

- [ ] 5.1 Basic routing tests (7): default processes untagged, default ignores tagged, tagged processes matching, tagged ignores untagged, tagged ignores non-matching, mixed processes both, orchestrator-only processes nothing
- [ ] 5.2 Multi-tag worker tests (3): worker with multiple tags gets all matching, skips non-matching, DefaultAnd with multiple tags
- [ ] 5.3 Multi-worker separation tests (2): two workers different tags no overlap, three pool separation
- [ ] 5.4 Tags + sessions orthogonality tests (3): tag and session route correctly, same session different tags, session without tag unaffected
- [ ] 5.5 Tags + retry tests (3): retry preserves tag across attempts, retry with tag and session, retry timeout with tag
- [ ] 5.6 Tags + poison handling tests (3): tagged unregistered activity poisons, max attempts exhausted, poison error includes tag context
- [ ] 5.7 Tags + cancellation tests (2): dropped tagged future triggers cancellation, cancelled tagged activity frees worker
- [ ] 5.8 Tags + replay tests (3): tag in event history, tag preserved on replay, backward compat no tag in old events
- [ ] 5.9 Tags + join/select composition tests (3): join with mixed tags, select2 tagged vs timer, select2 tagged timeout
- [ ] 5.10 Tags + typed activities test (1): typed activity with tag
- [ ] 5.11 Tags + continue_as_new test (1): tag routing survives continue_as_new
- [ ] 5.12 Edge case tests (3): empty string tag treated as tag, with_tag on non-activity panics, tag with special characters
- [ ] 5.13 Run: `cargo nt` — all 67+ tests pass (unit + provider + integration)
- [ ] 5.14 Run: `./run-tests.sh` — two-pass (with + without feature flags) passes

**Phase 5 — Adversarial Code Review:**
- [ ] Read every test: Does it assert the RIGHT thing? Are there tests that pass trivially (e.g., asserting `Ok(...)` without checking routing)?
- [ ] Check: Do multi-worker tests actually start separate runtimes with different `TagFilter` configs? Or do they use one runtime (which wouldn't test routing)?
- [ ] Check: Do retry tests verify that ALL attempts went to the tagged worker, not just the final one?
- [ ] Check: Do poison tests wait long enough for the poison threshold to be reached?
- [ ] Check: Do replay tests actually kill and restart the runtime, or do they just test serialization?
- [ ] Check: Are there any race conditions in multi-worker tests? (Use `wait_for_completion` with timeout, not sleep)
- [ ] Check: Do cancel tests verify the worker is free by scheduling a SECOND activity and seeing it complete?
- [ ] List all problems found, fix them, re-run `cargo nt` and `./run-tests.sh`

---

### Phase 6: Clean Warnings (`prompts/duroxide-clean-warnings.md`)

**Scope:** Execute the full compiler and linter cleanup protocol from `prompts/duroxide-clean-warnings.md`.

- [ ] 6.1 Run `cargo build --all-targets 2>&1` — capture all warnings
- [ ] 6.2 Run `cargo clippy --all-targets --all-features 2>&1` — capture all clippy lints
- [ ] 6.3 Fix every warning — no `#[allow(unused)]` shortcuts, no `_` prefixing to silence, investigate each one
- [ ] 6.4 Run `cargo fmt --all` — apply formatting
- [ ] 6.5 Run `cargo test --doc` — all doc examples compile
- [ ] 6.6 Run `cargo build --examples` — all examples compile
- [ ] 6.7 Verify: `cargo build --all-targets` — zero warnings
- [ ] 6.8 Verify: `cargo clippy --all-targets --all-features` — zero warnings
- [ ] 6.9 Verify: `cargo fmt --all --check` — zero diff
- [ ] 6.10 Verify: `cargo nt` — all tests pass
- [ ] 6.11 Verify: `./run-tests.sh` — two-pass passes

**Phase 6 — Adversarial Code Review:**
- [ ] `grep -rn '#\[allow(unused' src/ tests/` — no new `allow(unused)` or `allow(dead_code)` added by this implementation
- [ ] `grep -rn '^use ' src/lib.rs | head -30` — no unused imports
- [ ] Spot-check 3 test files for unused variables or imports
- [ ] List all problems found, fix them, re-run all verification commands

---

### Phase 7: Documentation & Tests Audit (`prompts/duroxide-update-docs-tests.md`)

**Scope:** Execute the full documentation and test audit protocol from `prompts/duroxide-update-docs-tests.md`.

- [ ] 7.1 Run `git diff` — identify all changed files for documentation impact
- [ ] 7.2 Update `docs/ORCHESTRATION-GUIDE.md` — add activity tags section (routing, `.with_tag()`, worker configuration, stranded activity guidance)
- [ ] 7.3 Update `docs/provider-implementation-guide.md` — updated `fetch_work_item` signature, SQL pseudocode for tag filtering, migration requirement
- [ ] 7.4 Update `docs/provider-testing-guide.md` — new test count, `tag_routing` category
- [ ] 7.5 Review `src/lib.rs` doc comments — `DurableFuture::with_tag()` has doc comment with example
- [ ] 7.6 Review `src/providers/mod.rs` doc comments — `TagFilter` enum and all methods have doc comments
- [ ] 7.7 Review `src/runtime/mod.rs` doc comments — `RuntimeOptions::with_worker_tags()` has doc comment
- [ ] 7.8 Review `src/lib.rs` doc comments — `ActivityContext::tag()` has doc comment
- [ ] 7.9 Update `docs/metrics-specification.md` if activity metrics gained a `tag` label — add label to spec, update OTel audit status
- [ ] 7.10 Verify all examples in `examples/` still compile: `cargo build --examples`
- [ ] 7.11 Verify doc tests compile: `cargo test --doc`
- [ ] 7.12 Spot-check: Run `cargo run --example hello_world` — works
- [ ] 7.13 Search for outdated references: `grep -rn 'into_activity' docs/ examples/` — zero hits (old API pattern)

**Phase 7 — Adversarial Code Review:**
- [ ] Re-read `docs/ORCHESTRATION-GUIDE.md` tags section: Do examples use `.await?` (not `.into_activity().await`)? Are they copy-pasteable?
- [ ] Re-read `docs/provider-implementation-guide.md`: Does the `fetch_work_item` pseudocode show tag filtering composed with session filtering?
- [ ] Re-read doc comments on `with_tag()`: Does the example show composability with `join2`/`select2`?
- [ ] Check: Does `docs/metrics-specification.md` list `tag` as a label on all activity metrics?
- [ ] Check: Are there broken `[TypeName]` cross-references in any updated doc comments?
- [ ] List all problems found, fix them, re-verify

---

### Phase 8: Final Validation

**Scope:** Full end-to-end verification. Nothing else changes after this phase — only verification.

- [ ] 8.1 `cargo build --all-targets` — zero errors, zero warnings
- [ ] 8.2 `cargo clippy --all-targets --all-features` — zero warnings
- [ ] 8.3 `cargo fmt --all --check` — zero diff
- [ ] 8.4 `cargo nt` — all tests pass
- [ ] 8.5 `./run-tests.sh` — two-pass (with + without features) passes
- [ ] 8.6 `cargo test --doc` — all doc tests pass
- [ ] 8.7 `cargo build --examples` — all examples compile
- [ ] 8.8 `cargo run --example hello_world` — runs successfully
- [ ] 8.9 Verify test count: `cargo nt --list 2>&1 | grep -c 'test'` — confirm ~67 new tag-related tests exist
- [ ] 8.10 Verify backward compat: Run existing pre-tag tests in isolation — `cargo nt -E 'not test(/tag/)' ` — all pass

**Phase 8 — Final Adversarial Code Review:**
- [ ] Re-read the entire `git diff` from start of implementation. For every hunk, ask: "Could this break existing users? Could this break rolling upgrades? Could this break replay of old events?"
- [ ] Check version compatibility: Old events without `tag` field deserialize correctly (serde `#[default]`)
- [ ] Check version compatibility: Old work items without `tag` column — does SQLite migration handle NULL gracefully?
- [ ] Check: Is `TagFilter` exported from the crate's public API? Can users access it?
- [ ] Check: Is `MAX_WORKER_TAGS` exported? Should it be?
- [ ] Check: Can a user construct `TagFilter::Tags(HashSet::new())` (empty set)? What happens? Should it be prevented?
- [ ] Verify: No files were accidentally deleted or corrupted
- [ ] Verify: No TODO/FIXME/HACK comments left behind from implementation
- [ ] `grep -rn 'TODO\|FIXME\|HACK\|XXX' src/ tests/` — review any new occurrences (old ones are OK)
- [ ] List all problems found, fix them, re-run Phase 8 verification commands
- [ ] **Only after ALL Phase 8 checks pass with zero issues may the implementation be declared COMPLETE**

---

## References

- [Temporal Task Queues](https://docs.temporal.io/workers#task-queue)
- [Azure Durable Functions Task Hubs](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-task-hubs)

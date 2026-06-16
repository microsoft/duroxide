## Sub-orchestrations (Design)

Sub-orchestrations allow an orchestration to schedule another orchestration, await its completion, compose multiple child results, and handle failures — all deterministically.

### Goals

- Deterministic replay: parent histories use correlation IDs; replays adopt the same child instance IDs and resolve by completion events.
- Idempotence: retries or replays must not start duplicate children nor append duplicate completions.
- Same programming model: sub-orchestrations compose like activities via `join`/`select`.

### Public API

New methods and outputs:

```rust
impl OrchestrationContext {
    /// Start a sub-orchestration; child instance id is derived deterministically.
    pub fn schedule_sub_orchestration(
        &self,
        name: impl Into<String>,
        input: impl Into<String>,
    ) -> impl Future<Output = Result<String, String>>;
}
```
```

### History model

Parent history gains the following events:

```rust
SubOrchestrationScheduled { id, name, instance, input }
SubOrchestrationCompleted { id, result }
SubOrchestrationFailed { id, error }
```

Child histories record parent linkage in the `OrchestrationStarted` event via `parent_instance` and `parent_id`.

Existing terminal events remain:

```rust
OrchestrationCompleted { output }
OrchestrationFailed { error }
```

### Actions

Host-visible decisions now include:

```rust
StartSubOrchestration { id, name, instance, input }
```

### Child instance ID

There are two modes for child instance ID assignment:

**Auto-generated (default):**
- Use `schedule_sub_orchestration()` - no instance ID parameter
- Runtime generates `instance = "sub::{event_id}"` for execution 1, and `instance = "sub::{execution_id}.{event_id}"` for later continue-as-new executions; then prefixes with parent.
- This guarantees the same child id on replays and prevents collisions across parents and continue-as-new generations.

**Explicit:**
- Use `schedule_sub_orchestration_with_id("Child", "my-instance-id", input)`
- The provided instance ID is used **exactly as provided** - no parent prefix
- You control the full instance ID for external visibility or correlation

### Runtime flow

1) Parent schedules child: pushes `SubOrchestrationScheduled` and records `StartSubOrchestration` action.
2) Runtime dispatches `StartSubOrchestration`:
   - If parent history already has `SubOrchestrationCompleted/Failed` with same `id`, skip dispatch (idempotent).
   - For auto-generated IDs (starting with `sub::`): derive `child_full = "{parent_instance}::{instance}"`
   - For explicit IDs: use the instance ID exactly as provided
   - Call `start_orchestration(child_full, name, input)` in a background task.
   - When the child finishes, route completion back to parent via internal router messages:
     - `SubOrchCompleted { instance: parent, id, result }`
     - `SubOrchFailed { instance: parent, id, error }`
3) Router appends the corresponding completion event in the parent history.
4) The parent future resolves by finding the completion by `id` in its own history.

### Idempotence and retries

- Parent dispatch checks for an existing terminal sub-orch event to avoid duplicate child starts.
- If the parent crashes and restarts:
  - The child instance continues independently and eventually completes.
  - The background poller delivers the `SubOrch*` router messages after restart, appending completion in the parent.
- If the child start fails to enqueue or join, a `SubOrchFailed` is routed to the parent with the error reason.

### Failure semantics

- Parent awaits a sub-orchestration via `.await` and receives:
  - `Ok(String)` — child completed with output
  - `Err(String)` — child failed with error
- Orchestrators can compensate or branch on error just like activity failures.

### Examples

Basic parent/child:

```rust
let child_upper = |ctx: OrchestrationContext, input: String| async move {
    let up = ctx.schedule_activity("Upper", input).await?;
    Ok(up)
};

let parent = |ctx: OrchestrationContext, input: String| async move {
    let r = ctx.schedule_sub_orchestration("ChildUpper", input).await?;
    Ok(format!("parent:{r}"))
};
```

Fan-out and join:

```rust
let a = ctx.schedule_sub_orchestration("ChildSum", "1,2");
let b = ctx.schedule_sub_orchestration("ChildSum", "3,4");
let results = ctx.join(vec![a, b]).await;
let total = results[0].as_ref().unwrap().parse::<i64>().unwrap() 
          + results[1].as_ref().unwrap().parse::<i64>().unwrap();
Ok(total.to_string())
```

Chained sub-orchestrations (root -> mid -> leaf):

```rust
let leaf = |ctx: OrchestrationContext, input: String| async move {
    Ok(ctx.schedule_activity("AppendX", input).await?)
};
let mid = |ctx: OrchestrationContext, input: String| async move {
    let r = ctx.schedule_sub_orchestration("Leaf", input).await?;
    Ok(format!("{r}-mid"))
};
let root = |ctx: OrchestrationContext, input: String| async move {
    let r = ctx.schedule_sub_orchestration("Mid", input).await?;
    Ok(format!("root:{r}"))
};
```

### Provider interaction

- Sub-orchestration completion routing uses the runtime’s internal router; no additional provider `WorkItem` types are required for the happy path.
- If we later want durable routing of child completions across process restarts, we can add provider `WorkItem::SubOrchCompleted/Failed` and enqueue on child completion (the code is already shaped for this and easy to toggle).

### Recovery

- On startup, the runtime scans instances and starts any that are not terminal.
- Children are started deterministically from their parents; if a parent is resumed and a child is still running, the child will complete and route its result to the parent, which then appends the terminal sub-orch event.

### Testing strategy

- Parent->child happy path: parent output and parent history contain sub-orch events.
- Fan-out and join two children; stable replay.
- Child failure path and parent compensation.
- Restart with running children: both complete and parent records both results after restart.
- Idempotence: ensure the parent does not start a child twice if a terminal sub-orch event exists.

### Future enhancements

  
- API to specify explicit child instance ids.
- Provider-backed durable completion routing (enqueue `SubOrchCompleted/Failed` items), leasing, DLQ.
- Visualization: render sub-orchestration edges in Mermaid diagrams under each parent.


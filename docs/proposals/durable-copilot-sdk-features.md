# Duroxide Feature Requests

Collected from building `durable-copilot-sdk` — a framework that wraps the GitHub Copilot SDK with duroxide for durable AI agent sessions.

---

## 1. Worker / Activity Affinity (Activity Tags)

**Problem:** In a multi-node deployment, activities for the same Copilot session must route to the same worker node because the CLI subprocess and session state live in that worker's memory.

**Proposal:** Activity tags — when scheduling an activity, attach a tag (e.g., the worker ID). The dispatcher routes tagged activities to workers that advertise matching tags.

```rust
// Scheduling side (orchestration)
ctx.schedule_activity_with_tag("runAgentTurn", input, "worker-abc123")

// Worker side (runtime startup)
runtime.set_worker_tags(vec!["worker-abc123"])
```

**Status:** Proposal exists at `duroxide/docs/proposals/activity-tags.md`

---

## 2. Orchestration Status Details / Custom Metadata

**Problem:** An orchestration may be "Running" but in a meaningful sub-state that the client needs to know about — e.g., "waiting for user input", "waiting for approval", "processing step 3 of 5". Currently, `getStatus()` only returns `Running | Completed | Failed` with no way to attach custom metadata.

**Proposal:** Add an optional `details` field to orchestration status that the orchestration can set at any point during execution.

```rust
// Inside orchestration
ctx.set_status_details(serde_json::json!({
    "phase": "waiting_for_user_input",
    "question": "Which city?",
    "choices": ["Tokyo", "Paris", "London"]
}));
yield ctx.wait_for_event("user-input");

// Client side
let info = client.get_instance_info("my-orch-123").await?;
// info.status == Running
// info.details == Some({"phase": "waiting_for_user_input", "question": "Which city?", ...})
```

**Use case:** Durable Copilot SDK — when the LLM asks the user a question (via `ask_user`), the orchestration needs to surface the question to the client process so it can prompt the user and raise the event with the answer.

**Alternatives considered:**
- Side table in Postgres — works but requires schema outside duroxide
- In-memory Map — only works single-process
- Sub-orchestration that "publishes" — over-engineered

---

## 3. WaitForEvent with Description

**Problem:** `wait_for_event("event-name")` is opaque — there's no way to know *why* an orchestration is waiting or what data it expects. This makes it impossible for management tools or client code to discover outstanding waits and provide the right data.

**Proposal:** Allow `wait_for_event` to take an optional description/metadata that is stored in the history and queryable via the management API.

```rust
// Inside orchestration
yield ctx.wait_for_event_with_details("user-input", serde_json::json!({
    "question": "Which city would you like to know the population of?",
    "choices": ["Tokyo", "Paris", "London"],
    "allow_freeform": true
}));

// Management API — query outstanding waits
let waits = client.get_pending_events("my-orch-123").await?;
// waits == [PendingEvent { name: "user-input", details: {...}, waited_since: ... }]
```

**Use case:** Same as above — the client polls for pending events, discovers the question, calls the user's callback, and raises the event with the answer. No side tables needed.

**Relationship to #2:** These are complementary. `set_status_details` is general-purpose (any metadata at any time). `wait_for_event_with_details` is specific to the wait pattern and is more discoverable. Could implement just one:

- If only #2: orchestration calls `set_status_details` before `wait_for_event`. Client polls status.
- If only #3: client queries pending events directly. More targeted.
- Both: #2 for general status, #3 for structured event queries.

---

## 4. Event Emission (Orchestration → External)

**Problem:** Events currently flow only inward: `client.raise_event()` → `ctx.wait_for_event()`. There's no way for an orchestration to emit events outward to external listeners.

**Proposal:** Allow orchestrations to emit events that external consumers can subscribe to or poll.

```rust
// Inside orchestration
ctx.emit_event("progress", serde_json::json!({ "step": 3, "total": 5 }));
ctx.emit_event("user-input-needed", serde_json::json!({ "question": "..." }));

// Client side
let events = client.get_emitted_events("my-orch-123", since_timestamp).await?;
// Or: client.subscribe_events("my-orch-123", |event| { ... })
```

**Relationship to #2 and #3:** This is the most general solution. #2 is a single mutable slot (latest status). #3 is scoped to waits. #4 is an append-only event stream — most flexible but heaviest.

**Recommendation:** Start with #2 (status details) — it's the simplest, covers the immediate use case, and the `details` field can be cleared/updated as the orchestration progresses. #3 and #4 can be added later if needed.

---

## Priority

| Feature | Urgency | Complexity | Notes |
|---------|---------|------------|-------|
| Activity Tags (#1) | High | Medium | Required for multi-node Phase 2 |
| Status Details (#2) | High | Low | Required for durable user input |
| WaitForEvent Description (#3) | Medium | Low | Nice-to-have, #2 covers the use case |
| Event Emission (#4) | Low | High | General solution, not needed yet |
| Orchestration Output Streaming (#5) | High | Medium | Eliminates polling hacks for intermediate results |
| ContinuedAsNew-aware wait (#6) | High | Low | Current waitForOrchestration is useless for loops |
| Rich Status with Timer/Event Info (#7) | Medium | Low | Can't distinguish timer-wait from active processing |
| Client-side LISTEN/NOTIFY (#8) | Medium | Medium | Eliminates client-side polling entirely |
| Cross-platform CI Publishing (#9) | Medium | Low | Manual npm binary publishing is error-prone |
| Activity Streaming (#10) | High | High | Activities can only return one value; intermediate results lost on abort |
| Activity Yield & Resume (#11) | High | High | Eliminates abort hack, continueAsNew loops, session re-creation |
| Per-Instance Shared State (#12) | Medium | Low | Config duplication in every TurnInput is a workaround |
| Activity Heartbeats with Data (#13) | Medium | Low | No visibility into long-running activity progress |

---

## 5. Orchestration Output Streaming

**Problem:** Long-running orchestrations (e.g., "every 60s fetch headlines") produce intermediate results on each cycle, but `waitForOrchestration` only returns on terminal states. Clients must poll `listExecutions` + `readExecutionHistory`, parse `ActivityCompleted` events, track which executions they've already seen, and handle `continueAsNew` boundaries. This is fragile — we shipped a bug where `lastSeenExecution` was advanced before `ActivityCompleted` appeared, causing all intermediate content to be missed.

**Proposal:** Allow orchestrations to emit intermediate outputs via `ctx.emitOutput(data)`. Clients consume via `client.readOutputs(instanceId)` or a streaming `client.subscribe(instanceId, callback)`.

```rust
// Inside orchestration
let summary = yield ctx.schedule_activity("fetchHeadlines", input);
ctx.emit_output(summary);  // Client sees this immediately
yield ctx.schedule_timer(60_000);
yield ctx.continue_as_new(input);

// Client side — poll-based
let outputs = client.read_outputs("my-orch", since_sequence).await?;
// returns [{ sequence: 1, data: "**Headlines...**", timestamp: ... }, ...]

// Client side — streaming (if LISTEN/NOTIFY available)
client.subscribe_outputs("my-orch", |output| {
    println!("Got: {}", output.data);
});
```

**Use case:** Durable Copilot SDK TUI — each wait cycle produces a response (e.g., headline summary) that must appear in the chat UI in real time. Currently requires ~40 lines of fragile polling code that reads raw execution history.

---

## 6. `waitForOrchestration` that handles `continueAsNew`

**Problem:** `waitForOrchestration` returns when the orchestration reaches a terminal state. But orchestrations using `continueAsNew` in loops never truly "complete" — each cycle ends with `ContinuedAsNew` (terminal for that execution) and a new execution starts. The client must implement its own polling loop, which duplicates logic that should live in the SDK.

**Proposal:** Add `waitForNextResult(instanceId, timeout)` that returns each time the orchestration produces a result, working across `continueAsNew` boundaries.

```rust
// Yields on each completed execution's output, follows continueAsNew chain
loop {
    match client.wait_for_next_result("my-orch", Duration::from_secs(120)).await {
        NextResult::Output(data) => println!("Intermediate: {}", data),
        NextResult::Completed(data) => { println!("Final: {}", data); break; }
        NextResult::Failed(err) => { eprintln!("Error: {}", err); break; }
        NextResult::Timeout => continue,
    }
}
```

**Relationship to #5:** If #5 (output streaming) is implemented, this becomes a convenience wrapper. Without #5, this at least encapsulates the `listExecutions` + `readExecutionHistory` + execution tracking logic inside duroxide.

---

## 7. Rich Status with Timer/Event Info

**Problem:** `getStatus()` returns `Running | Completed | Failed | NotFound`. During a durable timer wait, status is `Running` — indistinguishable from "activity in progress". The TUI can't show "⏳ Waiting 35s" vs "🔄 Processing..." because the status is the same.

**Proposal:** Enrich the status response with sub-state information parsed from the latest execution history.

```rust
let status = client.get_orchestration_status("my-orch").await?;
match status {
    Status::Running { sub_state } => match sub_state {
        SubState::Active,                              // Activity running
        SubState::WaitingForTimer { fires_at },        // Durable timer
        SubState::WaitingForEvent { event_name, details }, // External event
    },
    Status::Completed { output } => ...,
    Status::Failed { details } => ...,
}
```

**Implementation:** This doesn't require schema changes — the runtime already writes `TimerCreated` and `EventRaised`/`EventSent` events to history. `getStatus` just needs to check the last few events to determine the sub-state.

---

## 8. Client-side LISTEN/NOTIFY

**Problem:** `duroxide-pg-opt` uses PostgreSQL LISTEN/NOTIFY for the runtime dispatcher (long-polling), but the client API still polls with `getStatus()` every N milliseconds. The TUI polls every 500ms — that's 2 queries/second per orchestration hitting PostgreSQL, with most returning unchanged data.

**Proposal:** Extend the PostgreSQL provider's client interface to support LISTEN/NOTIFY for state changes.

```rust
// Client subscribes to instance state changes
let mut stream = client.subscribe("my-orch").await?;
while let Some(event) = stream.next().await {
    match event {
        StateChange::ActivityCompleted { data } => ...,
        StateChange::TimerFired => ...,
        StateChange::Completed { output } => break,
    }
}
```

**Implementation:** The provider would `LISTEN duroxide_instance_{id}` and the runtime would `NOTIFY` on state transitions. Falls back to polling for SQLite provider.

**Relationship to #5:** This is the transport layer; #5 is the API layer. Together they eliminate all client-side polling.

---

## 9. Cross-platform CI Publishing (npm)

> [!WARNING]
> This publishing approach is historical and must not be implemented. Official
> packages are published through Microsoft-managed internal pipelines; see
> [RELEASE_POLICY.md](../../RELEASE_POLICY.md). The cross-platform build and
> validation requirements remain applicable.

**Problem:** `duroxide-node` uses napi-rs with separate platform packages (`duroxide-darwin-arm64`, `duroxide-linux-x64-gnu`, etc.). Publishing requires building the native binary on each target platform and uploading to npm individually. We forgot to publish the linux binary for v0.1.5, causing Docker builds to fail. We had to manually extract the binary from a Docker image and publish it.

**Proposal:** GitHub Actions workflow that builds all platform binaries and publishes them on release tag push.

```yaml
# .github/workflows/release.yml
on:
  push:
    tags: ['v*']
jobs:
  build:
    strategy:
      matrix:
        include:
          - os: macos-latest
            target: aarch64-apple-darwin
            package: duroxide-darwin-arm64
          - os: ubuntu-latest
            target: x86_64-unknown-linux-gnu
            package: duroxide-linux-x64-gnu
    steps:
      - uses: actions/checkout@v4
      - run: npx napi build --platform --release --target ${{ matrix.target }}
      - run: cd npm/npm/${{ matrix.package }} && npm publish
  publish:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - run: npm publish  # main package
```

**Status:** Superseded by [RELEASE_POLICY.md](../../RELEASE_POLICY.md). The
workflow above is retained as historical context only and must not be adopted.

---

# Activity ↔ Orchestration Coordination

The following features address how activities and orchestrations communicate during execution. Currently, activities are black boxes that return a single value — all coordination must be encoded in that return value and decoded by the orchestration.

---

## 10. Activity Streaming / Partial Results

**Problem:** An activity returns **one value** when it completes. In durable-copilot-sdk, when the LLM generates a response and then calls the `wait` tool, the activity must abort the Copilot session to return control to the orchestration for durable timer scheduling. The abort kills `sendAndWait`, losing the LLM's response content. We hacked around this by calling `session.getMessages()` in the catch block to scrape the last assistant message before returning the wait signal — fragile and semantically wrong.

**Proposal:** Allow activities to emit intermediate results that the orchestration (and clients) can observe.

```rust
// Activity implementation
async fn run_agent_turn(ctx: ActivityContext, input: TurnInput) -> Result<TurnResult> {
    let session = get_or_create_session(&input);
    let response = session.send_and_wait(input.prompt).await?;

    // Emit the content as a partial result — orchestration sees it
    ctx.emit_progress(serde_json::json!({
        "content": response.content,
        "type": "assistant_message"
    }));

    // Now if the wait tool fired, return the wait signal
    // The content is already emitted — no data loss
    if let Some(wait) = pending_wait.take() {
        return Ok(TurnResult::Wait { seconds: wait.seconds, reason: wait.reason });
    }

    Ok(TurnResult::Completed { content: response.content })
}

// Orchestration sees progress events
let result = yield ctx.schedule_activity("runAgentTurn", input);
// ctx.last_activity_progress() returns the emitted data
```

**Use case:** Every wait cycle in the TUI's "fetch headlines every 35s" pattern. Currently requires the orchestration to parse raw execution history to find intermediate content.

---

## 11. Activity Yield & Resume

**Problem:** The entire abort → return → `continueAsNew` → new activity → re-create session pattern exists because activities cannot yield control to the orchestration and be resumed. Each "wait and continue" cycle requires:

1. Activity aborts the Copilot session (hack)
2. Activity returns `{ type: "wait" }` to orchestration
3. Orchestration schedules durable timer
4. Orchestration calls `continueAsNew` (to bound history)
5. New execution starts, schedules a new activity
6. New activity creates/finds the CopilotSession again
7. LLM receives "the wait is complete, continue" prompt

Steps 1-7 should be a single operation: "pause this activity for 60 seconds, then resume."

**Proposal:** Activities can yield control to the orchestration for durable operations (timers, events) and be resumed with the same context.

```rust
// Activity
async fn run_agent_turn(ctx: ActivityContext, input: TurnInput) -> Result<String> {
    let session = create_session(&input);

    loop {
        let response = session.send_and_wait(input.prompt).await?;

        if let Some(wait) = check_pending_wait() {
            // Yield to orchestration — durable timer scheduled automatically
            // Activity is suspended, resources preserved
            ctx.yield_timer(Duration::from_secs(wait.seconds)).await;
            // Resumed here after timer fires — same session, same context
            continue;
        }

        return Ok(response.content);
    }
}
```

**Implementation complexity:** High. Requires activity checkpointing (serialize/restore the activity's async state) or keeping the activity task alive across timer boundaries (which conflicts with the scale-to-zero model). Possible approaches:
- **Cooperative checkpointing:** Activity opts in to save/restore via a trait
- **Worker pinning:** Activity stays alive on the same worker (requires activity tags)
- **Continuation passing:** Activity returns a continuation closure that the runtime invokes after the timer

**Relationship to other features:**
- Makes #1 (Activity Tags) more important — resumed activities need the same worker
- Eliminates the need for #5 (Orchestration Output Streaming) in many cases — the activity handles the full lifecycle
- Subsumes the `continueAsNew` pattern for timer loops

---

## 12. Per-Instance Shared State

**Problem:** In scaled mode (client on laptop, workers on AKS), the worker doesn't have the client's in-memory configuration. We pass `systemMessage` and `model` through every `TurnInput`, duplicating config in every orchestration input and every `continueAsNew` call. If we add more config fields (tools, hooks, working directory), the duplication grows.

**Proposal:** A per-instance key-value store that persists in the durable backend, readable by activities and orchestrations.

```rust
// Client sets metadata when starting the orchestration
client.start_orchestration_with_metadata(
    "my-session",
    "durable-turn",
    input,
    json!({ "systemMessage": "You are helpful.", "model": "gpt-4" })
).await?;

// Activity reads it — no need to pass through TurnInput
async fn run_agent_turn(ctx: ActivityContext, input: TurnInput) -> Result<TurnResult> {
    let metadata = ctx.get_instance_metadata().await?;
    let system_msg = metadata["systemMessage"].as_str().unwrap_or("default");
    // ...
}

// Metadata survives continueAsNew — it's per-instance, not per-execution
```

**Implementation:** Store as a JSON column on the instance row. Read via the activity context (which already has access to the provider). Low complexity — it's just a CRUD operation on the instance table.

**Use case:** Any multi-node deployment where config originates on the client but is consumed by remote workers.

---

## 13. Activity Heartbeats with Data

**Problem:** Long-running activities (LLM turns can take 30+ seconds) are opaque. The orchestration and client have no visibility into what the activity is doing. `is_cancelled()` exists for cancellation checking, but there's no progress reporting.

**Proposal:** Activities can send heartbeats with arbitrary data, queryable by orchestrations and clients.

```rust
// Activity
async fn run_agent_turn(ctx: ActivityContext, input: TurnInput) -> Result<TurnResult> {
    ctx.heartbeat(json!({ "phase": "creating_session" })).await;
    let session = create_session(&input);

    ctx.heartbeat(json!({ "phase": "calling_llm", "prompt_length": input.prompt.len() })).await;
    let response = session.send_and_wait(input.prompt).await?;

    ctx.heartbeat(json!({ "phase": "processing_response" })).await;
    // ...
}

// Client side
let info = client.get_instance_info("my-orch").await?;
// info.last_heartbeat == Some({ "phase": "calling_llm", "prompt_length": 42 })
// info.last_heartbeat_at == Some(2026-02-13T08:15:00Z)
```

**Implementation:** Low complexity. Heartbeats can be stored as a single mutable field on the work item row (overwritten on each heartbeat). Alternatively, use the existing `is_cancelled` polling mechanism but add a data payload.

**Use case:** TUI status bar showing "🔄 Calling LLM..." vs "📥 Fetching URL..." vs "⏳ Processing...". Also useful for timeout detection — if heartbeats stop, the activity may be stuck.

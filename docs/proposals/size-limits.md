# Proposal: Size & Shape Limits (Simplified)

**Status:** Implemented (slices 1–10 complete, 2026-05-06)  
**Author:** AI-assisted  
**Date:** 2026-05-05  
**Tracking:** TODO.md "Size limits" / "Limits everywhere…"  
**Relates to:** `docs/proposals/core-improvements-roadmap.md` §8 *Event Size Limits*  

---

## 1. Problem

Every value the user can hand to duroxide — an orchestration name, an activity
input, an external event payload, a KV value, a custom status string — ends up
persisted in history and shipped over the provider transport. Today the runtime
caps a small uneven subset (`MAX_CUSTOM_STATUS_BYTES`, `MAX_KV_VALUE_BYTES`,
`MAX_KV_KEYS`, `MAX_TAG_NAME_BYTES`) and leaves the rest unbounded. As a
result:

- A 50 MiB activity output silently lands in history, gets re-read on every
  replay, and may exceed provider row/column caps (Cosmos 2 MiB, DynamoDB 400
  KiB, SQLite/Postgres operator-imposed `CHECK` constraints).
- Replay cost grows with serialized history size; one oversized event can
  push replay from sub-second to seconds.
- Provider-side rejection surfaces as an opaque infrastructure error far from
  the schedule call that caused it.
- Orchestrations that grow their history without bound (long-running monitors,
  unbounded retry loops) eventually fail in production with no warning that
  the cliff was approaching.

## 2. Goals & Non-Goals

**Goals**
- Add **three** new size constants and one history-pressure accessor. That's
  the entire surface change.
- Defensible numbers that fit inside SQLite, Postgres, and Cosmos provider
  envelopes.
- Failures surface at the earliest deterministic point, with a clear error,
  and oversized data **never reaches the provider**.
- Orchestration code can deterministically anticipate the history cap and
  `continue_as_new()` before it fails.

**Non-Goals**
- **Limit values are not configurable.** The three new caps and every existing
  cap are `pub const`. Operators who need different numbers fork. Rationale:
  per-cluster cap configurability introduces a per-instance pinning question
  (does an instance carry the cap that was in effect when it started? what
  about `continue_as_new`?), a mixed-cluster question (which cap wins?), and
  a validation matrix. Forking three constants is cheaper than getting all of
  that right.
- **No new constants beyond the three below.** Existing constants stay as-is
  with their current values. We do not introduce per-call-site knobs.
- **No name shape validation** (control chars, reserved prefixes, whitespace
  trimming). Out of scope; can be a separate proposal if a real bug demands it.
- **No claim-check / compression / chunking.** Document the pattern; runtime
  does not implement it.
- **No provider trait changes.** All enforcement is runtime-side.

Two runtime toggles **are** introduced (see §5.4) to manage rollout risk —
not to tune the cap values themselves.

## 3. Current State (Baseline)

Existing constants in [`src/runtime/limits.rs`](../../src/runtime/limits.rs)
remain **unchanged**:

| Constant | Value | Scope |
|---|---|---|
| `MAX_CARRY_FORWARD_EVENTS` | 100 | unmatched persistent events across `continue_as_new` |
| `MAX_CUSTOM_STATUS_BYTES` | 256 KiB | `ctx.set_custom_status()` |
| `MAX_WORKER_TAGS` | 5 | tags per `TagFilter` |
| `MAX_TAG_NAME_BYTES` | 256 | activity tag name |
| `MAX_KV_KEYS` | 150 | user KV keys per instance |
| `MAX_KV_VALUE_BYTES` | 64 KiB | single KV value |

## 4. The Three New Limits

```rust
// src/runtime/limits.rs (additions)

/// Maximum total serialized size of an execution's history, in bytes.
///
/// Enforced before each ack: if appending the proposed `history_delta` would
/// push `total_history_bytes` past this cap, the delta is **discarded** and
/// the orchestration is failed with `Configuration::LimitExceeded { resource:
/// "history" }`. The oversized delta is never persisted.
///
/// Orchestration code can read the running total via
/// `OrchestrationContext::history_size_bytes()` and roll over with
/// `continue_as_new()` before hitting this cap.
///
/// 5 MiB — comfortably below every reference provider's per-row aggregate
/// limits, and large enough that typical workflows never approach it.
pub const MAX_HISTORY_BYTES: usize = 5 * 1024 * 1024;

/// Maximum size of a "large payload" — the values that flow through
/// orchestration logic and live as a single event in history.
///
/// Covers: activity input/output, orchestration input/output,
/// sub-orchestration input/output, `continue_as_new` carry-forward input.
///
/// 3 MiB — over half of `MAX_HISTORY_BYTES` so a single big payload is
/// permitted, but two of them in the same execution will trip the history
/// cap and force a `continue_as_new` decision.
pub const MAX_PAYLOAD_BYTES: usize = 3 * 1024 * 1024;

/// Maximum size for "small values" — short strings and discrete signals
/// that are not currently capped by an existing constant.
///
/// Covers: orchestration name, activity name, sub-orchestration name,
/// event name, queue name, instance ID, session ID, external event payload,
/// queue message, error message, cancel reason.
///
/// Does **not** apply to values already governed by an existing constant
/// (`MAX_CUSTOM_STATUS_BYTES`, `MAX_KV_VALUE_BYTES`, `MAX_TAG_NAME_BYTES`).
///
/// 64 KiB — same order as `MAX_KV_VALUE_BYTES`, generous enough that
/// reasonable names/IDs (bytes-to-hundreds-of-bytes) are never near the cap,
/// and large enough for stack traces and moderate event payloads.
pub const MAX_SMALL_VALUE_BYTES: usize = 64 * 1024;
```

### 4.1 Per-call-site mapping

| Call site | Limit | Tier |
|---|---|---|
| `Client::start_orchestration` input | `MAX_PAYLOAD_BYTES` | 1 (API) |
| `Client::start_orchestration` instance ID | `MAX_SMALL_VALUE_BYTES` | 1 |
| `Client::start_orchestration` orchestration name | `MAX_SMALL_VALUE_BYTES` | 1 |
| `Client::raise_event` payload | `MAX_SMALL_VALUE_BYTES` | 1 |
| `Client::raise_event` event name | `MAX_SMALL_VALUE_BYTES` | 1 |
| `Client::enqueue_event` message | `MAX_SMALL_VALUE_BYTES` | 1 |
| `Client::enqueue_event` queue name | `MAX_SMALL_VALUE_BYTES` | 1 |
| `Client::cancel_instance` reason | `MAX_SMALL_VALUE_BYTES` | 1 |
| `ctx.schedule_activity*` input | `MAX_PAYLOAD_BYTES` | 2 (validate_limits) |
| `ctx.schedule_activity*` activity name | `MAX_SMALL_VALUE_BYTES` | 2 |
| `ctx.schedule_activity_on_session*` session ID | `MAX_SMALL_VALUE_BYTES` | 2 |
| `ctx.schedule_sub_orchestration*` input | `MAX_PAYLOAD_BYTES` | 2 |
| `ctx.schedule_sub_orchestration*` name | `MAX_SMALL_VALUE_BYTES` | 2 |
| `ctx.continue_as_new(input)` | `MAX_PAYLOAD_BYTES` | 2 |
| Orchestration completion output | `MAX_PAYLOAD_BYTES` | 2 |
| Sub-orchestration result returned to parent | `MAX_PAYLOAD_BYTES` | 2 |
| Activity output | `MAX_PAYLOAD_BYTES` | 3 (worker) |
| Activity error string | `MAX_SMALL_VALUE_BYTES` | 3 |
| Orchestration error string | `MAX_SMALL_VALUE_BYTES` | 2 |
| **Aggregate**: total history bytes (per execution) | `MAX_HISTORY_BYTES` | 2 |

Existing constants continue to govern: `MAX_CUSTOM_STATUS_BYTES` for
`ctx.set_custom_status()`, `MAX_KV_VALUE_BYTES` and `MAX_KV_KEYS` for KV,
`MAX_TAG_NAME_BYTES` for activity tag names, `MAX_CARRY_FORWARD_EVENTS` for
`continue_as_new`, and `MAX_WORKER_TAGS` for `TagFilter`_.
_
## 5. Failure Mode

**One terminal outcome. No retries. Two runtime toggles control rollout risk.**

A new variant on the existing `ConfigErrorKind`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConfigErrorKind {
    Nondeterminism,
    LimitExceeded,           // NEW
}
```

The variant is *defined* unconditionally, but whether the runtime ever
*produces* it is gated by `RuntimeOptions::emit_limit_exceeded_errors`
(see §5.4). Whether the runtime *enforces* the new caps at all is gated
by `RuntimeOptions::enforce_size_limits`.

The `resource: String` field on `ErrorDetails::Configuration` carries a
short stable identifier of the offending site:

- `"history"` — total history size cap exceeded
- `"activity_input:MyActivity"`, `"activity_output:MyActivity"`
- `"orchestration_input"`, `"orchestration_output"`
- `"sub_orch_input:Child"`, `"sub_orch_output:Child"`
- `"external_event:OrderCancelled"`, `"queue_message:notifications"`
- `"name:activity"`, `"name:orchestration"`, `"name:event"`, `"name:queue"`,
  `"name:session"`, `"identifier:instance_id"`
- `"error_message"`, `"cancel_reason"`

The `message: Option<String>` field is human-readable
(`"size 4194304 bytes exceeds limit 3145728 bytes"`), bounded to
`MAX_SMALL_VALUE_BYTES`, and not parsed by tooling.

### 5.1 Three enforcement tiers

All three tiers run only when `RuntimeOptions::enforce_size_limits == true`.
When the toggle is `false`, the runtime measures (counters, metrics, the
`history_size_bytes()` accessor) but never fails for a new-cap violation —
oversized values flow through to the provider as they do today.

**Tier 1 — Client API (preferred).** `Client` methods that accept
user-controlled bytes validate before writing to the provider and return
`Err(ClientError::Configuration(...))`. Nothing reaches history.

**Tier 2 — Orchestration turn (`validate_limits()`).** For values produced
*inside* an orchestration, the dispatcher inspects the proposed `history_delta`
**before** appending it. On any violation:

1. The delta is **dropped in memory**. No part of it is persisted.
2. A minimal terminal delta is built: a single
   `OrchestrationFailed { details }` event, where the error shape obeys
   `emit_limit_exceeded_errors` (see §5.4). By construction the event is
   well under any plausible cap.
3. The work item is acked with the minimal delta. Side effects from the
   rejected delta (worker enqueues, sub-orch starts, external events) are
   **not** enqueued — they were part of the same atomic ack that we replaced.
4. `duroxide.limit.violations { resource }` increments.

**Tier 3 — Worker (activity output).** The activity worker checks the
serialized output size before enqueueing `ActivityCompleted`. On violation,
it enqueues `ActivityFailed { details }` instead, with `details` again
shaped per §5.4. The original output is dropped before reaching the
provider; the worker logs (WARN) `output_size_bytes` and `output_blake3_hex`
(16 hex chars) for forensic correlation. **Raw payload bytes are never
logged.**

On the next orchestrator turn, the replay engine sees the failure and
treats it as terminal regardless of which shape was emitted: `Configuration`
errors abort the turn at the replay-engine level (existing behavior in
`replay_engine.rs`); `Application { retryable: false }` errors are
non-retryable application failures that today's `fail_orchestration_for_limit()`
path already produces. In both shapes the instance ends `Failed` and
orchestration code does **not** catch the failure via `?` on
`schedule_activity`.

### 5.2 The history-cap check is pre-persist

This is the central correctness point: **the runtime never persists a
history that exceeds `MAX_HISTORY_BYTES`.**

Implementation:

- `HistoryManager` maintains an incremental counter `total_history_bytes:
  u64`, updated when events are appended or replayed (`O(1)` per event).
  No full-history walks on the hot path.
- Before each ack, `validate_limits()` computes
  `proposed_total = total_history_bytes + sum(serialized_size(e) for e in delta)`.
- If `proposed_total > MAX_HISTORY_BYTES`, the §5.1 tier-2 conversion
  runs with `resource = "history"`. The minimal terminal delta is one
  small event; appending it cannot itself exceed the cap (5 MiB minus a
  few hundred bytes of headroom is a safe assumption).
- Per-event size violations (e.g., a 4 MiB activity input that fits the
  per-payload cap but combined with existing history would push past 5
  MiB) are caught by the same check.

The serialized-size measurement uses `String::len()` on the JSON-encoded
event (the same form the provider stores). This is `O(event)` once at
append time, then cached on the in-memory event record, so the running
total is always cheap to update.

### 5.3 Behavior change vs. today (gated)

Existing limit failures (`MAX_CUSTOM_STATUS_BYTES`, `MAX_KV_VALUE_BYTES`,
`MAX_KV_KEYS`, `MAX_TAG_NAME_BYTES`) currently emit
`Application { OrchestrationFailed, retryable: false }`. The long-term shape
is `Configuration { LimitExceeded }` for consistency with the new limits — a
hardcoded limit is a deployment fact, not an application failure, and
retrying through orchestration `?` cannot fix it.

The migration is **gated by `emit_limit_exceeded_errors`** (§5.4):

- **Toggle off (default initially)**: pre-existing limits keep emitting
  `Application { OrchestrationFailed, retryable: false }`, exactly as today.
  No mixed-cluster wire-format risk; no test breakage for assertions on the
  `Application` shape.
- **Toggle on**: pre-existing limits emit `Configuration { LimitExceeded }`,
  same shape as the new limits. Tests asserting `Application` for these
  specific failures must be updated. Operators relying on metric labels see
  these failures move from the application bucket to the configuration
  bucket.

When `enforce_size_limits` is on but `emit_limit_exceeded_errors` is off,
the new limits also emit `Application { OrchestrationFailed, retryable:
false }` rather than `Configuration { LimitExceeded }`. The wire format
remains a shape every existing runtime understands; the operator can adopt
the protective enforcement without committing to the wire-format change
until the cluster is fully on a `LimitExceeded`-aware version.

### 5.4 Two runtime toggles

```rust
// src/runtime/mod.rs (RuntimeOptions additions)
pub struct RuntimeOptions {
    // ... existing fields ...

    /// When `true`, the runtime enforces `MAX_HISTORY_BYTES`,
    /// `MAX_PAYLOAD_BYTES`, and `MAX_SMALL_VALUE_BYTES` at all three
    /// tiers (Client API, orchestration turn, worker output).
    ///
    /// When `false` (default for the introducing release), the runtime
    /// **measures** every value-bearing call site and updates metrics
    /// (`duroxide.payload.bytes`, `duroxide.history.bytes`) but never
    /// fails for a new-cap violation. Use the off state to observe
    /// population pressure (via `duroxide.history.bytes` and
    /// `OrchestrationContext::history_size_bytes()`) and refactor at-risk
    /// orchestrations *before* turning enforcement on.
    ///
    /// Independent of `emit_limit_exceeded_errors`.
    pub enforce_size_limits: bool,

    /// When `true`, every limit failure (both pre-existing and new) emits
    /// `ErrorDetails::Configuration { kind: ConfigErrorKind::LimitExceeded,
    /// resource, message }`.
    ///
    /// When `false` (default for the introducing release), every limit
    /// failure emits `ErrorDetails::Application { kind: OrchestrationFailed,
    /// retryable: false, message }` — the shape pre-existing limits use
    /// today, recognizable to every prior duroxide version. Leave off
    /// during a rolling upgrade until every node in the cluster has been
    /// upgraded to a duroxide version that recognizes the `LimitExceeded`
    /// variant; otherwise older nodes hit the existing
    /// `FailedDeserialization` poison path on the unknown variant.
    ///
    /// Independent of `enforce_size_limits`.
    pub emit_limit_exceeded_errors: bool,
}
```

The four toggle combinations all have a clear meaning:

| `enforce` | `emit_le` | Behavior |
|---|---|---|
| `false` | `false` | Today's behavior, plus measurement metrics and `history_size_bytes()`. No failures for new caps; pre-existing limits emit `Application`. **Safe upgrade default.** |
| `true` | `false` | New caps enforced; all failures (new and pre-existing) emit `Application`. **Aggressive: protective enforcement without wire-format change.** Safe in mixed clusters. |
| `false` | `true` | New caps not enforced; pre-existing limits emit `Configuration::LimitExceeded`. Useful only for testing the new error shape; not a long-term posture. |
| `true` | `true` | Long-term target: full enforcement, consistent error shape. **Requires every cluster node to recognize the variant.** |

The defaults flip in a future release (§11).

A single canary node can flip either toggle to validate behavior, with
a config-change rollback path if anything goes sideways. The two-release
phased-rollout pattern is unnecessary because the wire-format risk is
gated by the toggle, not by the binary version.

## 6. Anticipating the History Cap

Hardcoded limits are only humane if orchestrations can see how close they
are. Three pieces:

### 6.1 Orchestration accessor (deterministic)

```rust
impl OrchestrationContext {
    /// Bytes of serialized history for this execution at the current turn
    /// boundary. Deterministic across replay (computed from already-replayed
    /// events; does not include the in-flight delta of the current turn).
    pub fn history_size_bytes(&self) -> usize;

    /// `history_size_bytes() / MAX_HISTORY_BYTES` as a 0.0..=1.0 ratio.
    /// Convenience for "am I getting close to the cap?" checks.
    pub fn history_pressure(&self) -> f32;
}
```

Canonical usage at a safe checkpoint:

```rust
// Inside an orchestration loop that processes batches.
loop {
    let batch = ctx.schedule_activity("FetchBatch", &cursor).await?;
    if batch.is_empty() { return Ok("done".into()); }
    process_batch(&ctx, &batch).await?;
    cursor = next_cursor(&batch);

    // Roll over before history grows too large.
    if ctx.history_pressure() > 0.75 {
        return ctx.continue_as_new(cursor).await;
    }
}
```

**Determinism.** Both methods are pure functions of replayed history at
the current turn boundary. Replays produce identical values. The current
turn's delta is excluded so the value is stable across the turn.

### 6.2 Where the accessor can be called

- **Inside orchestration code (`OrchestrationContext`)**: yes, always. This
  is the only place it's both useful and deterministic.
- **Inside activity code (`ActivityContext`)**: not exposed. Activities
  don't see history, can't `continue_as_new`, and produce exactly one
  history event (their result). The check would be meaningless and
  non-deterministic — activities aren't replayed, so a value read at run
  time wouldn't be reproducible.
- **From orchestration code at any await point**: yes. The value reflects
  history-as-of-this-turn-boundary.
- **From inside an activity scheduled by an orchestration**: no. The
  orchestration must check before scheduling and pass any decision (e.g.,
  "this is the last batch") into the activity input.

In short: **orchestration code only.** Activities ask their orchestration
to make the call and pass results in.

### 6.3 Operator visibility (non-deterministic, runtime-side)

Independent of orchestration code:

- **Metric** `duroxide.history.bytes` (gauge / histogram, label
  `{orchestration_name}`) — emitted on every ack with the post-ack
  `total_history_bytes`. Gives operators a population view of how close
  instances run to the cap.
- **Metric** `duroxide.history.terminated_oversize` (counter, label
  `{orchestration_name}`) — increments when an instance fails with
  `resource = "history"`.
- **WARN log** at 75 % and 90 % thresholds, with `instance_id`,
  `orchestration_name`, `total_history_bytes`. Logged at most once per
  threshold per execution to avoid spam.

Operators who see population pressure rising act by either (i) asking the
app team to add a `continue_as_new` checkpoint, or (ii) shipping a code
change. The runtime does not auto-roll-over — only the orchestration
knows what state to carry forward.

### 6.4 Documentation

`docs/continue-as-new.md` gains a new section "Anticipating the history
limit" that walks through the `history_pressure()` checkpoint pattern,
shows a worked example, and links back to this proposal.

## 7. Determinism

Limits are baked into the runtime binary and never enter history. Replay
of an old history that was within the cap when written succeeds on a newer
binary regardless of any limit change (because the events already exist
and the per-event check only runs at append time, not on replay). Replay
of a history that contains a terminal limit-failure event (under either
emitted shape) always reaches the same terminal state.

**The toggles are not part of replay state.** They are runtime-binary
configuration, identical in spirit to today's `MAX_CUSTOM_STATUS_BYTES`
constant. Two replicas of the same orchestration **must** be configured
identically — the same way they must run the same duroxide version. A
cluster with mixed toggle settings on the same instance pool can produce
different failure shapes for the same input, which is unsupported (and
identical to running mixed binary versions). Operators set the toggles
as part of their deployment configuration and roll them out the same way
they roll out a code change.

`history_size_bytes()` and `history_pressure()` are deterministic because
they are computed from the same replayed history that all replicas see —
independent of the toggles.

## 8. Testing Plan

Unit tests in `src/runtime/limits.rs` cover the constants and the
`history_pressure()` arithmetic.

Integration tests in `tests/scenarios/limits.rs`:

| Test | Scenario | Expected |
|---|---|---|
| `activity_input_too_large` | Schedule activity with 4 MiB input | Tier-2 conversion: instance terminal with `Configuration::LimitExceeded { resource: "activity_input:..." }`; oversized delta never persisted |
| `activity_output_too_large` | Activity returns 4 MiB | Tier-3: `ActivityFailed { Configuration::LimitExceeded }`; instance terminal; raw output never logged; WARN contains size + BLAKE3 hash |
| `orchestration_input_too_large` | `start_orchestration` with 4 MiB input | Tier-1: `Err(ClientError::Configuration)`; nothing written to provider |
| `external_event_too_large` | `raise_event` with 100 KiB payload | Tier-1: `Err(ClientError::Configuration)` |
| `instance_id_too_long` | `start_orchestration` with 65 KiB instance ID | Tier-1: rejected |
| `orchestration_name_too_long` | Register orchestration with 65 KiB name | Rejected at registration (panic with `LimitViolation` payload) |
| `history_cap_terminates_instance` | Loop scheduling activities with 1 MiB outputs until total > 5 MiB | Instance terminal at the offending turn; oversized delta never persisted; counter `duroxide.history.terminated_oversize` increments |
| `history_size_bytes_is_deterministic` | Run orchestration to a known state, replay, assert `history_size_bytes()` returns the same value on every replay | Equal across replays |
| `history_pressure_drives_continue_as_new` | Orchestration that calls `continue_as_new` when `history_pressure() > 0.75`, runs through 10 generations | Each generation stays under cap; instance never fails for size |
| `existing_limit_failures_keep_application_when_emit_off` | Trigger `MAX_CUSTOM_STATUS_BYTES` etc. with `emit_limit_exceeded_errors = false` | Each produces `Application { OrchestrationFailed, retryable: false }` (preserves today's shape; verifies §5.3 toggle behavior) |
| `existing_limit_failures_become_configuration_when_emit_on` | Trigger same limits with `emit_limit_exceeded_errors = true` | Each produces `Configuration::LimitExceeded` |
| `enforce_off_measures_but_does_not_fail` | Schedule activity with 4 MiB input, `enforce_size_limits = false` | Orchestration succeeds; `duroxide.payload.bytes` records the size; no failure event |
| `enforce_on_emit_off_uses_application_shape` | Trigger any new-cap violation with `enforce_size_limits = true`, `emit_limit_exceeded_errors = false` | Failure event uses `Application { OrchestrationFailed, retryable: false }` shape (mixed-cluster-safe) |
| `tier2_failure_drops_side_effects` | Orchestration that schedules an activity *and* sets oversized custom status in the same turn (enforcement on) | Activity is **not** enqueued (rejected delta drops side effects); instance terminal |
| `replay_of_pre_limit_history_succeeds` | Hand-craft a history that contains a 4 MiB activity output (allowed when written), replay it under the new code | Replay succeeds; `history_size_bytes()` reports the (large) value; no spurious limit failure |
| `activity_output_log_no_payload_prefix` | Activity returns oversized output | WARN log contains `output_size_bytes` and `output_blake3_hex`; does **not** contain raw payload bytes |
| `multibyte_utf8_name_length` | Schedule activity with a 64-emoji name (256 UTF-8 bytes) and a 16385-emoji name (~64 KiB + 4 bytes) | First accepted, second rejected — confirms byte-length not char-length |

## 9. Files Touched

| File | Change |
|---|---|
| `src/runtime/limits.rs` | Add `MAX_HISTORY_BYTES`, `MAX_PAYLOAD_BYTES`, `MAX_SMALL_VALUE_BYTES`; no other changes to existing constants |
| `src/lib.rs` | Add `ConfigErrorKind::LimitExceeded` variant |
| `src/runtime/mod.rs` (`RuntimeOptions`) | Add `enforce_size_limits: bool` and `emit_limit_exceeded_errors: bool` fields (both default `false` in the introducing release; defaults flip in a later release per §11) |
| `src/runtime/dispatchers/orchestration.rs` | Extend `validate_limits()` with the new checks (gated by `enforce_size_limits`); implement the pre-persist history-cap check (drop oversized delta, ack with minimal terminal event); make `fail_orchestration_for_limit()` choose error shape per `emit_limit_exceeded_errors` |
| `src/runtime/dispatchers/worker.rs` | Activity output size check (Tier 3, gated by `enforce_size_limits`); error shape per `emit_limit_exceeded_errors`; BLAKE3-hash + size WARN log on oversized output; no raw bytes |
| `src/runtime/history_manager.rs` | Maintain `total_history_bytes: u64` incremental counter (`O(1)` per event, **always** — independent of toggles, since `history_size_bytes()` reads it); cache per-event serialized size on the in-memory record |
| `src/runtime/registry.rs` | Reject orchestration/activity names exceeding `MAX_SMALL_VALUE_BYTES` at register-time when `enforce_size_limits` is on (panic, matches existing duplicate-name behavior) |
| `src/client/mod.rs` | Tier-1 checks on every input-accepting client method (gated by `enforce_size_limits`); error shape per `emit_limit_exceeded_errors` |
| `src/lib.rs` (or `src/futures.rs`) | New `OrchestrationContext::history_size_bytes()` and `history_pressure()`; surfaced from the existing per-turn history snapshot. **Always available** (independent of toggles) |
| `docs/continue-as-new.md` | New "Anticipating the history limit" section with the `history_pressure()` pattern |
| `docs/ORCHESTRATION-GUIDE.md` | Refresh limits section; document both toggles |
| `docs/proposals/core-improvements-roadmap.md` | Mark §8 superseded by this doc |
| `TODO.md` | Strike "Size limits" / "Limits everywhere…" lines |
| `tests/scenarios/limits.rs` | New scenario file (see §8) — must cover all four toggle combinations |

No provider trait changes. The only event-schema addition is the
`ConfigErrorKind::LimitExceeded` variant, and it is *defined* in the
introducing release but only *produced* when `emit_limit_exceeded_errors`
is on — an operator-controlled gate, not a binary-version gate.

## 10. Rolling Upgrade

The two toggles in §5.4 manage the two distinct rolling-upgrade risks:

- **Wire-format risk** — old runtimes can't deserialize
  `Configuration::LimitExceeded` and would hit the existing
  `FailedDeserialization` poison path on the unknown variant. Gated by
  `emit_limit_exceeded_errors`. Leave off during the upgrade window;
  flip on once every node recognizes the variant.
- **In-flight-orchestration risk** — orchestrations that have been
  quietly accumulating large history or large payloads under the
  pre-upgrade runtime will start failing the moment caps are enforced.
  Gated by `enforce_size_limits`. Leave off after upgrade, observe
  population pressure via `duroxide.history.bytes` and the
  `history_size_bytes()` accessor, refactor at-risk orchestrations,
  *then* flip on.

Other rules:

- **A history written under an older runtime is always replayable** by a
  newer runtime, regardless of cap changes or toggle settings. Per-event
  size checks run only at append time; replay does not re-validate.
- **Toggle settings must be uniform within an instance pool.** Mixed
  toggle settings on nodes processing the same instance produce different
  failure shapes for the same input — same caveat as running mixed
  binary versions. Treat the toggles as part of deployment configuration.
- **Tightening a cap in a future release** would invalidate previously-OK
  in-flight orchestrations on their next turn. We do not plan to tighten
  these caps after shipping; if we ever do, it is a major-version event
  with explicit release notes.

This is consistent with the established pattern in CHANGELOG.md (e.g.,
0.1.18 "Provider Capability Filtering" — a feature gated and rolled out
the same way) and with the documented rolling-upgrade conventions in
`.github/copilot-instructions.md` ("flag any change that would break
mixed-version clusters" — flagged here, with operator-controlled gates
for each risk).

## 11. Rollout

A single release ships all the machinery; operators flip toggles when
their cluster is ready. A later release flips defaults.

**Release N (the introducing release):**
- Add the three constants and `ConfigErrorKind::LimitExceeded` variant.
- Add `OrchestrationContext::history_size_bytes()` /
  `history_pressure()`.
- Add `HistoryManager::total_history_bytes` counter.
- Add `RuntimeOptions::enforce_size_limits` (default `false`) and
  `RuntimeOptions::emit_limit_exceeded_errors` (default `false`).
- Wire all tier-1 / tier-2 / tier-3 checks behind `enforce_size_limits`.
- Wire the `Application` ↔ `Configuration::LimitExceeded` shape choice
  behind `emit_limit_exceeded_errors`.
- Add `duroxide.history.bytes` (always emitted, useful even with
  enforcement off), `duroxide.payload.bytes` (always emitted),
  `duroxide.history.terminated_oversize` (only fires when enforcement
  on), `duroxide.limit.violations` (only fires when enforcement on),
  75 % / 90 % WARN logs.
- Update `docs/continue-as-new.md`, `docs/ORCHESTRATION-GUIDE.md`,
  `TODO.md`. Document both toggles and the recommended rollout sequence.
- Land the test suite from §8 covering all four toggle combinations.
- Changelog entry under `### Added`. **No `### Breaking Changes` entry**
  — defaults preserve all existing behavior.

**Operator rollout sequence (recommended, single release):**

1. Upgrade every cluster node to Release N. Toggles default off; nothing
   changes behaviorally.
2. Observe `duroxide.history.bytes` and `duroxide.payload.bytes` metrics
   for at least one full duty cycle. Identify any orchestrations
   approaching `MAX_HISTORY_BYTES` or producing oversized payloads.
3. Refactor at-risk orchestrations to add `continue_as_new` checkpoints
   using `history_pressure()`.
4. Flip `enforce_size_limits = true` cluster-wide (canary first if
   desired). Errors still emit `Application`, so mixed-cluster
   wire-format remains safe.
5. Once every node is on Release N (or later), flip
   `emit_limit_exceeded_errors = true` cluster-wide.

**Release N+K (some future minor version):** flip both defaults to
`true`. *That* release ships with a `### Breaking Changes` entry
covering (a) the new caps becoming enforced by default, and (b) the
migration of existing limit failures from `Application` to
`Configuration::LimitExceeded`. By then, the variant has been
deserializable for K releases.

## 12. Decision Log

- **Why are the cap *values* hardcoded?** Configurable values introduce a
  per-instance pinning question (does an instance carry the cap that was
  in effect when it started? what about `continue_as_new`?), a
  mixed-cluster question (which cap wins?), and a validation matrix.
  Forking three constants is cheaper than getting all of that right.
- **Why two on/off toggles, then?** The two rollout risks are independent
  — the wire-format change (`emit_limit_exceeded_errors`) and the
  in-flight enforcement change (`enforce_size_limits`). A single toggle
  forces operators to take both at once or neither; two toggles let each
  cluster pick its sequence. The toggle surface is two bools — much
  smaller than a per-cap configuration story.
- **Why default both toggles to `false` in the introducing release?** A
  duroxide upgrade should never silently fail in-flight orchestrations
  or change wire format. Defaults preserve today's behavior; operators
  opt in when ready. The defaults flip together in a later release whose
  changelog entry calls out the breaking change.
- **Why allow the `(enforce=true, emit_le=false)` combination?** It lets
  operators get the protective benefit of the new caps immediately on
  upgrade while deferring the wire-format change until cluster rollout
  is complete. Mixed-cluster safe by construction. Without this
  combination, conservative operators would have to wait through a full
  rolling upgrade *before* any size protection kicks in.
- **Why three constants and not one per call site?** Three covers every
  meaningful tier (history aggregate, large per-event payload, small
  per-event value) without inviting per-call-site bikeshedding. If a
  real bug ever demands a tighter cap on a specific call site, splitting
  one constant later is backward-compatible.
- **Why 5 / 3 / 64 KiB?** 5 MiB sits comfortably below every reference
  provider's per-row aggregate cap. 3 MiB lets a single big payload through
  but forces a `continue_as_new` decision before the second one. 64 KiB
  matches the existing `MAX_KV_VALUE_BYTES` and is generous enough that
  reasonable names/IDs are nowhere near the cap.
- **Why pre-persist history check?** Persisting a delta and then failing
  the orchestration would bake the oversized event into history, making
  replay even more expensive and the failure permanently visible. Dropping
  the delta keeps history at the last known-good state plus one small
  failure event.
- **Why `Configuration` and not `Application` (long-term)?** A hardcoded
  limit is a deployment fact, not an application failure. `Configuration`
  errors abort the turn at the replay-engine level (existing behavior),
  which is what we want — application code should not retry-loop on a
  non-retryable condition. The `Application` shape is the *transitional*
  shape during rollout, controlled by `emit_limit_exceeded_errors`.
- **Why is `history_pressure()` orchestration-only?** It's deterministic
  only inside replay (which is where `OrchestrationContext` lives). Inside
  an activity it would be non-deterministic (activities aren't replayed)
  and meaningless (activities can't `continue_as_new`).
- **Why no shape validation (control chars, reserved prefixes)?** Out of
  scope; not motivated by a current bug; can be a separate proposal.

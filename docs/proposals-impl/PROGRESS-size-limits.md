# Size & Shape Limits — Implementation Progress

**Spec:** [proposals/size-limits.md](../proposals/size-limits.md)
**Status:** In progress
**Last updated:** 2026-05-06

---

## Summary

Implements three new hardcoded size limits (`MAX_HISTORY_BYTES = 5 MiB`,
`MAX_PAYLOAD_BYTES = 3 MiB`, `MAX_SMALL_VALUE_BYTES = 64 KiB`), a deterministic
`OrchestrationContext::history_size_bytes()` accessor, observability metrics,
and a `ConfigErrorKind::LimitExceeded` error variant. Enforcement and error
shape are gated by two `RuntimeOptions` toggles
(`enforce_size_limits`, `emit_limit_exceeded_errors`), both defaulting `false`
in the introducing release so upgrades preserve today's behavior.

---

## Implementation Slices

Each slice is a logical unit; multiple slices may land in a single PR.

- [x] **Slice 1 — Constants + RuntimeOptions toggles**
  - `MAX_HISTORY_BYTES`, `MAX_PAYLOAD_BYTES`, `MAX_SMALL_VALUE_BYTES`
    in `src/runtime/limits.rs`
  - `RuntimeOptions::enforce_size_limits: bool` (default `false`)
  - `RuntimeOptions::emit_limit_exceeded_errors: bool` (default `false`)
  - No behavior change yet; constants/fields purely additive.

- [x] **Slice 2 — `HistoryManager::total_history_bytes` counter**
  - Maintain `O(1)` running total, updated on append and during replay.
  - Cache per-event serialized size on the in-memory `Event` record (or in
    a parallel `Vec<u32>`) so the increment is `O(1)`.
  - Always-on; foundational for slices 3 and 6.

- [x] **Slice 3 — `OrchestrationContext` accessors**
  - `history_size_bytes() -> usize` reads the value computed at this
    turn boundary (excluding in-flight delta).
  - `history_pressure() -> f32` returns `bytes / MAX_HISTORY_BYTES`
    clamped to `0.0..=1.0`.
  - Always-on; deterministic across replay.

- [x] **Slice 4 — `ConfigErrorKind::LimitExceeded` variant**
  - Add the variant to `src/lib.rs`.
  - Add `display_message` arm.
  - Update `fail_orchestration_for_limit()` (or equivalent) in
    `src/runtime/dispatchers/orchestration.rs` to choose the error shape
    based on `RuntimeOptions::emit_limit_exceeded_errors`.
  - Pre-existing limit failures (`MAX_CUSTOM_STATUS_BYTES`,
    `MAX_KV_VALUE_BYTES`, `MAX_KV_KEYS`, `MAX_TAG_NAME_BYTES`) continue
    to emit `Application` when toggle is off (today's shape preserved).

- [x] **Slice 5 — Tier-1 client checks**
  - `Client::start_orchestration*`, `raise_event`, `enqueue_event*`,
    `cancel_instance`: validate sizes and return
    `Err(ClientError::Configuration(...))` when `enforce_size_limits` is on.
  - Identifier checks (instance ID, orchestration name, event name,
    queue name, cancel reason) use `MAX_SMALL_VALUE_BYTES`.
  - Payload checks (start input, raise event payload, queue message)
    use `MAX_PAYLOAD_BYTES` / `MAX_SMALL_VALUE_BYTES` per §4.1 mapping.

- [x] **Slice 6 — Tier-2 `validate_limits()` extensions**
  - Per-event checks for activity input, sub-orch input, sub-orch name,
    activity name, session ID, continue-as-new input, orchestration
    completion output, sub-orch result, orchestration error string.
  - Aggregate history-cap check: pre-persist comparison of
    `total_history_bytes + sum(serialized_size(delta))` against
    `MAX_HISTORY_BYTES`. On violation, drop delta in memory, ack with
    minimal terminal `OrchestrationFailed` event, drop side effects.
  - All gated by `enforce_size_limits`.

- [x] **Slice 7 — Tier-3 worker output check**
  - Activity output size check before enqueueing `ActivityCompleted`.
  - On violation, enqueue `ActivityFailed { details }` (shape per
    `emit_limit_exceeded_errors`) and WARN-log size + BLAKE3 hash.
  - Error string also bounded to `MAX_SMALL_VALUE_BYTES`; truncate
    rather than fail.
  - Add `blake3` dependency.

- [x] **Slice 8 — Metrics + WARN logs**
  - `duroxide.history.bytes` (always emitted, gauge per-instance on ack).
  - `duroxide.payload.bytes { kind }` (always emitted, histogram per
    payload site).
  - `duroxide.limit.violations { resource }` (emitted on tier-1/2/3 fail).
  - `duroxide.history.terminated_oversize { orchestration_name }`
    (emitted when an instance terminates with `resource = "history"`).
  - 75 % / 90 % WARN log thresholds, once per execution per threshold.

- [x] **Slice 9 — Tests**
  - `tests/scenarios/limits.rs` covering all 4 toggle combinations
    and every test row in spec §8.
  - Existing test updates where assertions on the `Application` shape
    break under `(enforce=true, emit_le=true)` (these test the new
    `Configuration::LimitExceeded` shape; old shape covered by
    `existing_limit_failures_keep_application_when_emit_off`).

- [x] **Slice 10 — Docs**
  - `docs/continue-as-new.md` — new "Anticipating the history limit"
    section with `history_pressure()` worked example.
  - `docs/ORCHESTRATION-GUIDE.md` — refresh limits section, document
    both toggles.
  - `docs/proposals/core-improvements-roadmap.md` — mark §8 superseded.
  - `TODO.md` — strike "Size limits" / "Limits everywhere…" lines.

- [ ] **Slice 11 — Release housekeeping** *(at release time, not now)*
  - `git mv docs/proposals/size-limits.md docs/proposals-impl/`
  - Delete this PROGRESS file.
  - Add `### Added` changelog entry; **no `### Breaking Changes`**
    entry (defaults preserve today's behavior).

---

## Implementation Notes

### Slice 1

- Cargo is not installed in this development environment; build validation
  was done via the language server (no errors reported in `limits.rs` or
  `runtime/mod.rs`). User should run `cargo build --all-targets
  --all-features` and `cargo nt` before merging.
- The proposal originally claimed there was a magic number `20` in
  `prep_completions()` to be promoted to a `MAX_PENDING_EXTERNAL_EVENTS`
  constant. Code inspection found no such constant — the only related cap
  is `MAX_CARRY_FORWARD_EVENTS = 100`, which is already named. Spec was
  corrected to drop that claim.

### Slices 2–9

- Implemented and 1092/1092 tests pass (`cargo nt`, 2026-05-06).
- `blake3 = "1"` added to `Cargo.toml` for tier-3 forensic hashing.
- `HistoryManager` tracks `history_bytes` (baseline) and `delta_bytes`
  (incremental) using `serialized_event_size()` (serde_json byte length).
- `OrchestrationContext` exposes `history_size_bytes()` and
  `history_pressure()` as always-on, deterministic accessors. The value
  is set once per turn from the working history before the orchestration
  function runs, so it is stable and replay-safe within a turn.
- `ConfigErrorKind::LimitExceeded` is additive; `emit_limit_exceeded_errors`
  defaults `false`, preserving the `Application` shape for all existing callers.
- Tier-2 `check_tier2_size_limits()` drops the in-memory delta before acking
  a terminal failure event, which required clearing `metadata.pinned_duroxide_version`
  to avoid a version-pinning invariant assertion when `OrchestrationStarted`
  was no longer present in the delta.
- Metrics are always emitted regardless of `enforce_size_limits`; enforcement
  only gates fail/truncate behavior.


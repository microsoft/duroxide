//! Runtime limits and constants.
//!
//! Collect all hard limits in one place so they're easy to find, document,
//! and reference from both runtime code and provider validators.

/// Maximum number of unmatched persistent events that can be carried forward
/// across a `continue_as_new()` boundary.
///
/// When the list exceeds this limit the oldest events (by history order) are
/// dropped and a warning is logged.
pub const MAX_CARRY_FORWARD_EVENTS: usize = 100;

/// Maximum size in bytes for the custom status string set via
/// `ctx.set_custom_status()`.
///
/// If the orchestration sets a custom status that exceeds this limit, the
/// runtime will fail the orchestration with an `Infrastructure` error
/// before the ack is committed.
///
/// 256 KiB — generous for progress/status strings while preventing unbounded
/// growth in the execution metadata row.
pub const MAX_CUSTOM_STATUS_BYTES: usize = 256 * 1024;

/// Maximum number of tags a worker can subscribe to in a [`TagFilter`].
///
/// Keeps the SQL `IN (...)` clause and CosmosDB query predicates bounded.
pub const MAX_WORKER_TAGS: usize = 5;

/// Maximum size in bytes for a single activity tag name.
///
/// Enforced at the orchestration dispatcher level (before ack) following
/// the same pattern as [`MAX_CUSTOM_STATUS_BYTES`]. If exceeded, the
/// orchestration is failed with an Infrastructure error.
pub const MAX_TAG_NAME_BYTES: usize = 256;

/// Maximum number of **user** KV keys per orchestration instance.
///
/// Enforced in `validate_limits()` after the orchestration turn completes.
/// If exceeded, the orchestration is failed with a non-retryable application error.
pub const MAX_KV_KEYS: usize = 150;

/// Maximum size of a single KV value in bytes (64 KiB).
///
/// Enforced in `validate_limits()` by scanning `KeyValueSet` events in the history delta.
pub const MAX_KV_VALUE_BYTES: usize = 64 * 1024;

// =============================================================================
// Size limits — see `docs/proposals/size-limits.md`
//
// These three constants cover every value-bearing call site not already
// governed by an existing constant above. Enforcement and error shape are
// gated by `RuntimeOptions::enforce_size_limits` and
// `RuntimeOptions::emit_limit_exceeded_errors` respectively (defaults `false`
// in the introducing release; defaults flip in a later release).
// =============================================================================

/// Maximum total serialized size of an execution's history, in bytes.
///
/// When `RuntimeOptions::enforce_size_limits` is true, the orchestration
/// dispatcher checks before each ack whether appending the proposed
/// `history_delta` would push `total_history_bytes` past this cap. If it
/// would, the delta is **discarded in memory** and the orchestration is
/// failed with a single terminal `OrchestrationFailed` event whose
/// `details.resource = "history"`. The oversized delta never reaches the
/// provider.
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

//! Runtime limits and constants.
//!
//! Collect all hard limits in one place so they're easy to find, document,
//! and reference from both runtime code and provider validators.

/// Maximum number of unmatched persistent events that can be carried forward
/// across a `continue_as_new()` boundary.
///
/// When the list exceeds this limit the oldest events (by history order) are
/// dropped and a warning is logged.
pub const MAX_CARRY_FORWARD_EVENTS: usize = 20;

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

/// Maximum number of KV keys per orchestration instance.
///
/// Enforced in `validate_limits()` after the orchestration turn completes.
/// If exceeded, the orchestration is failed with a non-retryable application error.
pub const MAX_KV_KEYS: usize = 100;

/// Maximum size of a single KV value in bytes (16 KiB).
///
/// Enforced in `validate_limits()` by scanning `KeyValueSet` events in the history delta.
pub const MAX_KV_VALUE_BYTES: usize = 16 * 1024;

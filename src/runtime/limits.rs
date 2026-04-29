//! Runtime limits and constants.
//!
//! Collect all hard limits in one place so they're easy to find, document,
//! and reference from both runtime code and provider validators.
//!
//! # Two layers of limit constants
//!
//! - **Struct-based (`Limits`)**: runtime-configurable via `RuntimeOptions::limits`.
//!   Use `Limits::recommended()` for the documented defaults and
//!   `Limits::permissive()` to disable all checks (the current `Default`).
//! - **Legacy `pub const`s**: kept for backward compatibility. Each is a
//!   `#[deprecated]` alias pointing at the corresponding `Limits::recommended()`
//!   value. External code that references them continues to compile unchanged.

// ============================================================================
// Runtime-configurable limits struct
// ============================================================================

/// Runtime-configurable size and shape limits.
///
/// Override via [`crate::runtime::RuntimeOptions::limits`] to tighten or
/// loosen for a deployment.
///
/// # Phase defaults
///
/// [`Limits::default()`] currently returns [`Limits::permissive()`] (all
/// checks effectively disabled). This will flip to [`Limits::recommended()`]
/// in a future "Phase 7" release. See the size-limits proposal for details.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Maximum UTF-8 byte length for a *name*: orchestration name, activity
    /// name, sub-orchestration name, event name, queue name, tag name, or
    /// session ID.
    ///
    /// Default (`recommended`): 256 bytes.
    pub max_name_bytes: usize,

    /// Maximum UTF-8 byte length for an *identifier*: instance ID or KV key.
    ///
    /// These values end up as primary keys / index columns in provider storage
    /// and benefit from being independently tunable.
    ///
    /// Default (`recommended`): 256 bytes.
    pub max_identifier_bytes: usize,

    // ---- payload/aggregate fields reserved for future phases ----
    // (payloads: max_payload_bytes, max_message_bytes, max_diagnostic_bytes)
    // (shape: max_fanout_per_turn, max_history_delta_events, …)
}

impl Limits {
    /// The recommended production limits.
    ///
    /// These values are the documented defaults for all limit constants and
    /// are appropriate for most deployments. `Default` will return these
    /// values from Phase 7 onward.
    pub fn recommended() -> Self {
        Self {
            max_name_bytes: 256,
            max_identifier_bytes: 256,
        }
    }

    /// Permissive limits — all size/shape checks effectively disabled.
    ///
    /// Used as [`Default`] in Phases 1–6 so no existing orchestration can
    /// regress on upgrade. Tests that exercise specific limits should
    /// override individual fields explicitly rather than relying on the
    /// default.
    pub fn permissive() -> Self {
        Self {
            max_name_bytes: usize::MAX,
            max_identifier_bytes: usize::MAX,
        }
    }
}

impl Default for Limits {
    /// Returns [`Limits::permissive()`].
    ///
    /// Will flip to [`Limits::recommended()`] in Phase 7 (a future minor
    /// version with explicit release notes). Until then, all new limit checks
    /// are inert unless the operator explicitly sets `RuntimeOptions::limits`.
    fn default() -> Self {
        Self::permissive()
    }
}

// ============================================================================
// Name / identifier validation types (used by Phase 2 enforcement)
// ============================================================================

/// Identifies which call site produced a name that violated a limit.
///
/// Carried inside [`LimitViolation::NameTooLong`] to produce per-call-site
/// error messages without requiring a separate constant per call site.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NameKind {
    /// Orchestration type name (passed to `start_orchestration*` or registered
    /// in `OrchestrationRegistry`).
    OrchestrationName,
    /// Activity type name (passed to `schedule_activity*` or registered in
    /// `ActivityRegistry`).
    ActivityName,
    /// Sub-orchestration type name (passed to `schedule_sub_orchestration*`).
    SubOrchestrationName,
    /// External event name (passed to `raise_event`, `schedule_wait`).
    EventName,
    /// Named queue identifier (passed to `enqueue_event*`,
    /// `dequeue_event*`).
    QueueName,
    /// Activity routing tag (passed via `.with_tag()`).
    TagName,
    /// Session identifier (passed to `schedule_activity_on_session*`).
    SessionId,
    /// Orchestration instance ID (passed to `start_orchestration*`,
    /// `schedule_sub_orchestration*`).
    InstanceId,
    /// KV store key.
    KvKey,
    /// Pinned version string (passed to `start_orchestration_versioned*`).
    PinnedVersion,
}

impl NameKind {
    /// Human-readable label used in error messages and metrics.
    pub fn label(&self) -> &'static str {
        match self {
            NameKind::OrchestrationName => "orchestration_name",
            NameKind::ActivityName => "activity_name",
            NameKind::SubOrchestrationName => "sub_orchestration_name",
            NameKind::EventName => "event_name",
            NameKind::QueueName => "queue_name",
            NameKind::TagName => "tag_name",
            NameKind::SessionId => "session_id",
            NameKind::InstanceId => "instance_id",
            NameKind::KvKey => "kv_key",
            NameKind::PinnedVersion => "pinned_version",
        }
    }
}

/// A structured description of a limit violation.
///
/// Carried inside [`crate::ConfigErrorKind::LimitExceeded`] error details and
/// used by registry-time panics. Serialize/deserialize support is provided so
/// the payload can be embedded in the `message` field of
/// `ErrorDetails::Configuration`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LimitViolation {
    /// A name or identifier exceeded its configured byte-length limit.
    NameTooLong {
        /// Which call site / field was checked.
        kind: NameKind,
        /// The offending name (truncated to 128 bytes for log safety).
        name: String,
        /// Actual UTF-8 byte length of the name.
        size: usize,
        /// Configured limit that was exceeded.
        limit: usize,
    },
    // Future variants: InvalidName, PayloadTooLarge, TooManyEvents, …
}

impl LimitViolation {
    /// Stable marker prefix used when embedding a `LimitViolation` inside the
    /// `message: Option<String>` field of `ErrorDetails::Configuration`.
    ///
    /// Older runtimes that do not understand this format will still surface
    /// the string as a human-readable error message.
    pub const MESSAGE_PREFIX: &'static str = "__duroxide.limit_violation:";

    /// Serialize this violation into a `message` string suitable for storing
    /// in `ErrorDetails::Configuration { message }`.
    pub fn encode_into_message(&self) -> String {
        let json = match self {
            LimitViolation::NameTooLong { kind, name, size, limit } => {
                let safe_name = truncate_at_char_boundary(name, 128);
                format!(
                    r#"{{"v":"NameTooLong","kind":"{}","name":{},"size":{},"limit":{}}}"#,
                    kind.label(),
                    serde_json::Value::String(safe_name.to_string()),
                    size,
                    limit,
                )
            }
        };
        format!("{}{}", Self::MESSAGE_PREFIX, json)
    }

    /// Try to parse a `LimitViolation` from a message string produced by
    /// [`encode_into_message`]. Returns `None` if the string was not produced
    /// by this method (e.g., a legacy plain-text error message).
    pub fn parse_from_message(msg: &str) -> Option<Self> {
        let json_str = msg.strip_prefix(Self::MESSAGE_PREFIX)?;
        let v: serde_json::Value = serde_json::from_str(json_str).ok()?;
        match v["v"].as_str()? {
            "NameTooLong" => {
                let kind_str = v["kind"].as_str()?;
                let kind = NameKind::from_label(kind_str)?;
                let name = v["name"].as_str()?.to_string();
                let size = v["size"].as_u64()? as usize;
                let limit = v["limit"].as_u64()? as usize;
                Some(LimitViolation::NameTooLong { kind, name, size, limit })
            }
            _ => None,
        }
    }

    /// Human-readable summary for error messages and log fields.
    pub fn display_message(&self) -> String {
        match self {
            LimitViolation::NameTooLong { kind, name, size, limit } => {
                let safe_name = if name.len() > 64 {
                    format!("{}…", truncate_at_char_boundary(name, 64))
                } else {
                    name.clone()
                };
                format!(
                    "{} '{}' is {} bytes, exceeds limit of {} bytes",
                    kind.label(),
                    safe_name,
                    size,
                    limit,
                )
            }
        }
    }
}

impl NameKind {
    fn from_label(label: &str) -> Option<Self> {
        match label {
            "orchestration_name" => Some(NameKind::OrchestrationName),
            "activity_name" => Some(NameKind::ActivityName),
            "sub_orchestration_name" => Some(NameKind::SubOrchestrationName),
            "event_name" => Some(NameKind::EventName),
            "queue_name" => Some(NameKind::QueueName),
            "tag_name" => Some(NameKind::TagName),
            "session_id" => Some(NameKind::SessionId),
            "instance_id" => Some(NameKind::InstanceId),
            "kv_key" => Some(NameKind::KvKey),
            "pinned_version" => Some(NameKind::PinnedVersion),
            _ => None,
        }
    }
}

// ============================================================================
// Byte-counting helper
// ============================================================================

/// Measure the byte length of a string for limit checks.
///
/// Returns the raw UTF-8 byte length (`str::len()`). All limit comparisons go
/// through this helper so the measurement strategy is easy to change in one
/// place.
///
/// Note: This measures the *raw* string, not the JSON-escaped form. The
/// catalog defaults include ~2× headroom over typical provider envelopes to
/// absorb JSON escaping overhead.
#[inline]
pub fn measured_len(s: &str) -> usize {
    s.len()
}

/// Truncate a string to at most `max_bytes` UTF-8 bytes, ensuring the cut
/// falls on a character boundary.
///
/// Returns a `&str` slice of the original string — always valid UTF-8.
fn truncate_at_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    // Walk backward from max_bytes until we find a valid character boundary.
    let mut boundary = max_bytes;
    while boundary > 0 && !s.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &s[..boundary]
}

// ============================================================================
// Legacy `pub const` aliases (kept for backward compatibility)
// ============================================================================

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
/// # Deprecation
///
/// This constant is a legacy alias for [`Limits::recommended().max_name_bytes`](Limits::recommended).
/// Prefer accessing the limit through [`RuntimeOptions::limits`](crate::runtime::RuntimeOptions::limits)
/// at runtime, or through [`Limits::recommended()`] for static defaults.
#[deprecated(since = "0.1.29", note = "use Limits::recommended().max_name_bytes or RuntimeOptions::limits.max_name_bytes")]
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

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permissive_is_effectively_unlimited() {
        let l = Limits::permissive();
        assert_eq!(l.max_name_bytes, usize::MAX);
        assert_eq!(l.max_identifier_bytes, usize::MAX);
    }

    #[test]
    fn recommended_has_256_byte_limits() {
        let l = Limits::recommended();
        assert_eq!(l.max_name_bytes, 256);
        assert_eq!(l.max_identifier_bytes, 256);
    }

    #[test]
    fn default_is_permissive() {
        let l = Limits::default();
        assert_eq!(l.max_name_bytes, usize::MAX);
    }

    #[test]
    fn measured_len_counts_utf8_bytes_not_chars() {
        // Each emoji is 4 UTF-8 bytes
        let emoji = "🦀".repeat(64); // 64 * 4 = 256 bytes, 64 chars
        assert_eq!(measured_len(&emoji), 256);
        assert_eq!(emoji.chars().count(), 64);

        let one_more = "🦀".repeat(65); // 260 bytes
        assert_eq!(measured_len(&one_more), 260);
    }

    #[test]
    fn limit_violation_encode_decode_roundtrip() {
        let v = LimitViolation::NameTooLong {
            kind: NameKind::ActivityName,
            name: "my_activity".to_string(),
            size: 512,
            limit: 256,
        };
        let encoded = v.encode_into_message();
        assert!(encoded.starts_with(LimitViolation::MESSAGE_PREFIX));
        let decoded = LimitViolation::parse_from_message(&encoded).expect("should decode");
        assert_eq!(decoded, v);
    }

    #[test]
    fn limit_violation_display_truncates_long_names() {
        let long_name = "a".repeat(200);
        let v = LimitViolation::NameTooLong {
            kind: NameKind::InstanceId,
            name: long_name,
            size: 200,
            limit: 256,
        };
        let msg = v.display_message();
        // Should not panic, and should contain the kind label
        assert!(msg.contains("instance_id"));
    }

    #[test]
    fn parse_from_message_ignores_non_prefixed_strings() {
        assert!(LimitViolation::parse_from_message("plain error message").is_none());
        assert!(LimitViolation::parse_from_message("").is_none());
    }

    #[test]
    fn name_kind_labels_are_stable() {
        assert_eq!(NameKind::OrchestrationName.label(), "orchestration_name");
        assert_eq!(NameKind::ActivityName.label(), "activity_name");
        assert_eq!(NameKind::InstanceId.label(), "instance_id");
        assert_eq!(NameKind::KvKey.label(), "kv_key");
    }
}

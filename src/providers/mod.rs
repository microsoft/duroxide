use crate::Event;
use std::any::Any;
use std::collections::HashSet;
use std::time::Duration;

pub mod error;
pub use error::ProviderError;

/// Maximum number of tags a worker can subscribe to.
pub use crate::runtime::limits::MAX_WORKER_TAGS;

/// Filter for which activity tags a worker will process.
///
/// Activity tags route work items to specialized workers. A tag is an
/// opaque string label set at schedule time via `.with_tag()`. Workers
/// declare which tags they accept via [`crate::RuntimeOptions`]`.worker_tag_filter`.
///
/// # Variants
///
/// - `DefaultOnly` (default) — process only activities with no tag.
/// - `Tags(["gpu"])` — process only activities tagged `"gpu"`.
/// - `DefaultAnd(["gpu"])` — process activities with no tag OR tagged `"gpu"`.
/// - `Any` — process all activities regardless of tag.
/// - `None` — don't process any activities (orchestrator-only mode).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TagFilter {
    /// Process only activities with no tag (default).
    #[default]
    DefaultOnly,

    /// Process only activities with the specified tags (NOT default).
    /// Limited to [`MAX_WORKER_TAGS`] (5) tags.
    Tags(HashSet<String>),

    /// Process activities with no tag AND the specified tags.
    /// Limited to [`MAX_WORKER_TAGS`] (5) tags.
    DefaultAnd(HashSet<String>),

    /// Process all activities regardless of tag (generalist worker).
    Any,

    /// Don't process any activities (orchestrator-only mode).
    None,
}

impl TagFilter {
    /// Create a filter for specific tags only.
    ///
    /// # Panics
    /// Panics if more than [`MAX_WORKER_TAGS`] (5) tags are provided.
    pub fn tags<I, S>(tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let set: HashSet<String> = tags.into_iter().map(Into::into).collect();
        assert!(
            !set.is_empty(),
            "TagFilter::tags() requires at least one tag; use TagFilter::None for no activities"
        );
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
    /// Panics if more than [`MAX_WORKER_TAGS`] (5) tags are provided.
    pub fn default_and<I, S>(tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let set: HashSet<String> = tags.into_iter().map(Into::into).collect();
        assert!(
            !set.is_empty(),
            "TagFilter::default_and() requires at least one tag; use TagFilter::DefaultOnly for untagged only"
        );
        assert!(
            set.len() <= MAX_WORKER_TAGS,
            "Worker can subscribe to at most {} tags, got {}",
            MAX_WORKER_TAGS,
            set.len()
        );
        TagFilter::DefaultAnd(set)
    }

    /// Create a filter for only untagged activities.
    pub fn default_only() -> Self {
        TagFilter::DefaultOnly
    }

    /// Create a filter that processes no activities.
    pub fn none() -> Self {
        TagFilter::None
    }

    /// Create a filter that processes all activities regardless of tag.
    pub fn any() -> Self {
        TagFilter::Any
    }

    /// Check if an activity with the given tag matches this filter.
    pub fn matches(&self, tag: Option<&str>) -> bool {
        match (self, tag) {
            (TagFilter::Any, _) => true,
            (TagFilter::None, _) => false,
            (TagFilter::DefaultOnly, Option::None) => true,
            (TagFilter::DefaultOnly, Some(_)) => false,
            (TagFilter::Tags(_), Option::None) => false,
            (TagFilter::Tags(set), Some(t)) => set.contains(t),
            (TagFilter::DefaultAnd(_), Option::None) => true,
            (TagFilter::DefaultAnd(set), Some(t)) => set.contains(t),
        }
    }
}

/// Returns the current build version of the duroxide crate.
pub fn current_build_version() -> semver::Version {
    semver::Version::parse(env!("CARGO_PKG_VERSION")).expect("CARGO_PKG_VERSION must be valid semver")
}

/// An inclusive version range: [min, max].
///
/// Both bounds are inclusive. For example, `SemverRange { min: (0,0,0), max: (1,5,0) }`
/// matches any version `v` where `0.0.0 <= v <= 1.5.0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemverRange {
    pub min: semver::Version,
    pub max: semver::Version,
}

impl SemverRange {
    pub fn new(min: semver::Version, max: semver::Version) -> Self {
        Self { min, max }
    }

    /// Check if a version falls within this range (inclusive on both ends).
    pub fn contains(&self, version: &semver::Version) -> bool {
        version >= &self.min && version <= &self.max
    }

    /// Default range: `>=0.0.0, <=CURRENT_BUILD_VERSION`.
    ///
    /// This means the runtime can replay any execution pinned at or below its own
    /// build version. Replay engines are backward-compatible.
    pub fn default_for_current_build() -> Self {
        Self {
            min: semver::Version::new(0, 0, 0),
            max: current_build_version(),
        }
    }
}

/// Capability filter passed by the orchestration dispatcher to the provider.
///
/// The provider uses this to return only orchestration items whose pinned
/// `duroxide_version` falls within one of the supported ranges.
///
/// If no filter is supplied (`None`), the provider returns any available item
/// (legacy behavior, useful for admin tooling).
#[derive(Debug, Clone)]
pub struct DispatcherCapabilityFilter {
    /// Supported duroxide version ranges. An execution is compatible if its
    /// pinned duroxide version falls within ANY of these ranges.
    ///
    /// Phase 1 typically has a single range: `[>=0.0.0, <=CURRENT_BUILD_VERSION]`.
    pub supported_duroxide_versions: Vec<SemverRange>,
}

impl DispatcherCapabilityFilter {
    /// Check if a pinned version is compatible with this filter.
    pub fn is_compatible(&self, version: &semver::Version) -> bool {
        self.supported_duroxide_versions.iter().any(|r| r.contains(version))
    }

    /// Build the default filter for the current build version.
    pub fn default_for_current_build() -> Self {
        Self {
            supported_duroxide_versions: vec![SemverRange::default_for_current_build()],
        }
    }
}

/// Identity of an activity for cancellation purposes.
///
/// Used by the runtime to specify which activities should be cancelled
/// during an orchestration turn (e.g., select losers, orchestration termination).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledActivityIdentifier {
    /// Instance ID of the orchestration
    pub instance: String,
    /// Execution ID within the instance
    pub execution_id: u64,
    /// Activity ID (the event_id of the ActivityScheduled event)
    pub activity_id: u64,
}

/// Configuration for session-aware work item fetching.
///
/// When passed to [`Provider::fetch_work_item`], enables session routing:
/// - Non-session items (`session_id IS NULL`) are always eligible
/// - Items for sessions already owned by `owner_id` are eligible
/// - Items for unowned (claimable) sessions are eligible
///
/// When `fetch_work_item` is called with `session: None`, only non-session
/// items are returned.
#[derive(Debug, Clone)]
pub struct SessionFetchConfig {
    /// Identity tag for session ownership. All worker slots sharing this
    /// value are treated as a single owner for session affinity purposes.
    /// Typically set to `RuntimeOptions::worker_node_id` (process-level)
    /// or a per-slot UUID (ephemeral).
    pub owner_id: String,
    /// How long to hold the session lock when claiming a new session.
    pub lock_timeout: Duration,
}

/// Orchestration item containing all data needed to process an instance atomically.
///
/// This represents a locked batch of work for a single orchestration instance.
/// The provider must guarantee that no other process can modify this instance
/// until `ack_orchestration_item()` or `abandon_orchestration_item()` is called.
///
/// # Fields
///
/// * `instance` - Unique identifier for the orchestration instance (e.g., "order-123")
/// * `orchestration_name` - Name of the orchestration being executed (e.g., "ProcessOrder")
/// * `execution_id` - Current execution ID (starts at 1, increments with ContinueAsNew)
/// * `version` - Orchestration version string (e.g., "1.0.0")
/// * `history` - Complete event history for the current execution (ordered by event_id)
/// * `messages` - Batch of WorkItems to process (may include Start, completions, external events)
/// * `lock_token` - Unique token that must be used to ack or abandon this batch
/// * `history_error` - If set, history deserialization failed; `history` may be empty or partial
///
/// # Implementation Notes
///
/// - The provider must ensure `history` contains ALL events for `execution_id` in order
/// - For multi-execution instances (ContinueAsNew), only return the LATEST execution's history
/// - The `lock_token` must be unique and prevent concurrent processing of the same instance
/// - All messages in the batch should belong to the same instance
/// - The lock should expire after a timeout (e.g., 30s) to handle worker crashes
/// - If history deserialization fails, set `history_error` with the error message and return
///   `Ok(Some(...))` instead of `Err(...)`. This allows the runtime to reach the poison check
///   and terminate the orchestration after `max_attempts`.
///
/// # Example from SQLite Provider
///
/// ```text
/// // 1. Find available instance (check instance_locks for active locks)
/// SELECT q.instance_id FROM orchestrator_queue q
/// LEFT JOIN instance_locks il ON q.instance_id = il.instance_id
/// WHERE q.visible_at <= now()
///   AND (il.instance_id IS NULL OR il.locked_until <= now())
/// ORDER BY q.id LIMIT 1
///
/// // 2. Atomically acquire instance-level lock
/// INSERT INTO instance_locks (instance_id, lock_token, locked_until, locked_at)
/// VALUES (?, ?, ?, ?)
/// ON CONFLICT(instance_id) DO UPDATE
/// SET lock_token = excluded.lock_token, locked_until = excluded.locked_until
/// WHERE locked_until <= excluded.locked_at
///
/// // 3. Lock all visible messages for that instance
/// UPDATE orchestrator_queue SET lock_token = ?, locked_until = ?
/// WHERE instance_id = ? AND visible_at <= now()
///
/// // 4. Load instance metadata
/// SELECT orchestration_name, orchestration_version, current_execution_id
/// FROM instances WHERE instance_id = ?
///
/// // 5. Load history for current execution
/// SELECT event_data FROM history
/// WHERE instance_id = ? AND execution_id = ?
/// ORDER BY event_id
///
/// // 6. Return OrchestrationItem with lock_token
/// ```
#[derive(Debug, Clone)]
pub struct OrchestrationItem {
    pub instance: String,
    pub orchestration_name: String,
    pub execution_id: u64,
    pub version: String,
    pub history: Vec<Event>,
    pub messages: Vec<WorkItem>,
    /// If set, history deserialization failed. `history` may be empty or partial.
    ///
    /// The runtime treats this as a terminal error: it abandons the item with backoff,
    /// and once `attempt_count > max_attempts`, poisons the orchestration.
    ///
    /// Providers SHOULD return `Ok(Some(...))` with this field set instead of
    /// `Err(ProviderError::permanent(...))` when deserialization fails after acquiring
    /// a lock. This ensures the lock lifecycle stays clean (Ok = lock held, Err = no lock).
    pub history_error: Option<String>,

    /// KV snapshot loaded from the materialized `kv_store` table at fetch time.
    /// Seeds the orchestration's in-memory `kv_state` before the turn executes.
    pub kv_snapshot: std::collections::HashMap<String, String>,
}

/// Execution metadata computed by the runtime to be persisted by the provider.
///
/// The runtime inspects the `history_delta` and `orchestrator_items` to compute this metadata.
/// **Providers must NOT inspect event contents themselves** - they should blindly store this metadata.
///
/// This design ensures the provider remains a pure storage abstraction without orchestration knowledge.
///
/// # Fields
///
/// * `status` - New execution status: `Some("Completed")`, `Some("Failed")`, `Some("ContinuedAsNew")`, or `None`
///   - `None` means the execution is still running (no status update needed)
///   - Provider should update the stored execution status when `Some(...)`
///
/// * `output` - The terminal value to store (depends on status):
///   - `Completed`: The orchestration's successful result
///   - `Failed`: The error message
///   - `ContinuedAsNew`: The input that was passed to continue_as_new()
///   - `None`: No output (execution still running)
///
/// # Example Usage in Provider
///
/// ```text
/// async fn ack_orchestration_item(..., metadata: ExecutionMetadata) {
///     // Store metadata without understanding what it means
///     if let Some(status) = &metadata.status {
///         UPDATE executions SET status = ?, output = ? WHERE instance_id = ? AND execution_id = ?
///     }
/// }
/// ```
///
/// # ContinueAsNew Handling
///
/// ContinueAsNew is handled entirely by the runtime. Providers must NOT try to
/// synthesize new executions in `fetch_orchestration_item`.
///
/// Runtime behavior:
/// - When an orchestration calls `continue_as_new(input)`, the runtime stamps
///   `OrchestrationContinuedAsNew` into the current execution's history and enqueues
///   a `WorkItem::ContinueAsNew`.
/// - When processing that work item, the runtime starts a fresh execution with
///   `execution_id = current + 1`, passes an empty `existing_history`, and stamps an
///   `OrchestrationStarted { event_id: 1, .. }` event for the new execution.
/// - The runtime then calls `ack_orchestration_item(lock_token, execution_id, ...)` with
///   the explicit execution id to persist history and queue operations.
///
/// Provider responsibilities:
/// - Use the explicit `execution_id` given to `ack_orchestration_item`.
/// - Idempotently create the execution record/entry if it doesn't already exist.
/// - Update the instance's “current execution” pointer to be at least `execution_id`.
/// - Append all `history_delta` events to the specified `execution_id`.
/// - Update `executions.status, executions.output` from `ExecutionMetadata` when provided.
#[derive(Debug, Clone, Default)]
pub struct ExecutionMetadata {
    /// New status for the execution ('Completed', 'Failed', 'ContinuedAsNew', or None to keep current)
    pub status: Option<String>,
    /// Output/error/input to store (for Completed/Failed/ContinuedAsNew)
    pub output: Option<String>,
    /// Orchestration name (for new instances or updates)
    pub orchestration_name: Option<String>,
    /// Orchestration version (for new instances or updates)
    pub orchestration_version: Option<String>,
    /// Parent instance ID (for sub-orchestrations, used for cascading delete)
    pub parent_instance_id: Option<String>,
    /// Pinned duroxide version for this execution (set from OrchestrationStarted event).
    ///
    /// The provider stores this alongside the execution record for efficient
    /// capability filtering.
    ///
    /// - `Some(v)`: Store `v` as the execution's pinned version. The provider should
    ///   update the stored version unconditionally when provided.
    /// - `None`: No version update requested. The provider should not modify the
    ///   existing stored value.
    ///
    /// The **runtime** guarantees this is only set on the first turn of a new execution
    /// (when `OrchestrationStarted` is in the history delta), enforced by a `debug_assert`
    /// in the orchestration dispatcher. The provider does not need to enforce write-once
    /// semantics — it simply stores what it's told.
    pub pinned_duroxide_version: Option<semver::Version>,
}

/// Provider-backed work queue items the runtime consumes continually.
///
/// WorkItems represent messages that flow through provider-managed queues.
/// They are serialized/deserialized using serde_json for storage.
///
/// # Queue Routing
///
/// Different WorkItem types go to different queues:
/// - `StartOrchestration, ContinueAsNew, ActivityCompleted/Failed, TimerFired, ExternalRaised, SubOrchCompleted/Failed, CancelInstance` → **Orchestrator queue**
///   - TimerFired items use `visible_at = fire_at_ms` for delayed visibility
/// - `ActivityExecute` → **Worker queue**
///
/// # Instance ID Extraction
///
/// Most WorkItems have an `instance` field. Sub-orchestration completions use `parent_instance`.
/// Providers need to extract the instance ID for routing. SQLite example:
///
/// ```ignore
/// let instance = match &item {
///     WorkItem::StartOrchestration { instance, .. } |
///     WorkItem::ActivityCompleted { instance, .. } |
///     WorkItem::CancelInstance { instance, .. } => instance,
///     WorkItem::SubOrchCompleted { parent_instance, .. } => parent_instance,
///     _ => return Err("unexpected item type"),
/// };
/// ```
///
/// # Execution ID Tracking
///
/// WorkItems for activities, timers, and sub-orchestrations include `execution_id`.
/// This allows providers to route completions to the correct execution when ContinueAsNew creates multiple executions.
///
/// **Critical:** Completions with mismatched execution_id should still be enqueued (the runtime filters them).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum WorkItem {
    /// Start a new orchestration instance
    /// - Instance metadata is created by runtime via ack_orchestration_item metadata (not on enqueue)
    /// - `execution_id`: The execution ID for this start (usually INITIAL_EXECUTION_ID=1)
    /// - `version`: None means runtime will resolve from registry
    StartOrchestration {
        instance: String,
        orchestration: String,
        input: String,
        version: Option<String>,
        parent_instance: Option<String>,
        parent_id: Option<u64>,
        execution_id: u64,
    },

    /// Execute an activity (goes to worker queue)
    /// - `id`: event_id from ActivityScheduled (for correlation)
    /// - Worker will enqueue ActivityCompleted or ActivityFailed
    ActivityExecute {
        instance: String,
        execution_id: u64,
        id: u64, // scheduling_event_id from ActivityScheduled
        name: String,
        input: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        session_id: Option<String>,
        /// Routing tag for directing this activity to specific workers.
        /// `None` means default (untagged) queue.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        tag: Option<String>,
    },

    /// Activity completed successfully (goes to orchestrator queue)
    /// - `id`: source_event_id referencing the ActivityScheduled event
    /// - Triggers next orchestration turn
    ActivityCompleted {
        instance: String,
        execution_id: u64,
        id: u64, // source_event_id referencing ActivityScheduled
        result: String,
    },

    /// Activity failed with error (goes to orchestrator queue)
    /// - `id`: source_event_id referencing the ActivityScheduled event
    /// - Triggers next orchestration turn
    ActivityFailed {
        instance: String,
        execution_id: u64,
        id: u64, // source_event_id referencing ActivityScheduled
        details: crate::ErrorDetails,
    },

    /// Timer fired (goes to orchestrator queue with delayed visibility)
    /// - Created directly by runtime when timer is scheduled
    /// - Enqueued to orchestrator queue with `visible_at = fire_at_ms`
    /// - Orchestrator dispatcher processes when `visible_at <= now()`
    TimerFired {
        instance: String,
        execution_id: u64,
        id: u64, // source_event_id referencing TimerCreated
        fire_at_ms: u64,
    },

    /// External event raised (goes to orchestrator queue)
    /// - Matched by `name` to ExternalSubscribed events
    /// - `data`: JSON payload from external system
    ExternalRaised {
        instance: String,
        name: String,
        data: String,
    },

    /// Sub-orchestration completed (goes to parent's orchestrator queue)
    /// - Routes to `parent_instance`, not the child
    /// - `parent_id`: event_id from parent's SubOrchestrationScheduled event
    SubOrchCompleted {
        parent_instance: String,
        parent_execution_id: u64,
        parent_id: u64, // source_event_id referencing SubOrchestrationScheduled
        result: String,
    },

    /// Sub-orchestration failed (goes to parent's orchestration queue)
    /// - Routes to `parent_instance`, not the child
    /// - `parent_id`: event_id from parent's SubOrchestrationScheduled event
    SubOrchFailed {
        parent_instance: String,
        parent_execution_id: u64,
        parent_id: u64, // source_event_id referencing SubOrchestrationScheduled
        details: crate::ErrorDetails,
    },

    /// Request orchestration cancellation (goes to orchestrator queue)
    /// - Runtime will append OrchestrationCancelRequested event
    /// - Eventually results in OrchestrationFailed with "canceled: {reason}"
    CancelInstance { instance: String, reason: String },

    /// Continue orchestration as new execution (goes to orchestrator queue)
    /// - Signals the end of current execution and start of next
    /// - Runtime will create Event::OrchestrationStarted for next execution
    /// - Provider should create new execution (see ExecutionMetadata.create_next_execution)
    ContinueAsNew {
        instance: String,
        orchestration: String,
        input: String,
        version: Option<String>,
        /// Persistent events carried forward from the previous execution.
        /// These are seeded into the new execution's history before any new
        /// externally-raised events, preserving FIFO order across CAN boundaries.
        #[serde(default)]
        carry_forward_events: Vec<(String, String)>,
        /// Custom status accumulated from the previous execution, if any.
        /// Carried forward so `get_custom_status()` works immediately in the new execution.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        initial_custom_status: Option<String>,
    },

    /// Persistent external event raised (goes to orchestrator queue).
    /// Matched by `name` using FIFO mailbox semantics — events stick around until consumed.
    QueueMessage {
        instance: String,
        name: String,
        data: String,
    },

    /// V2: External event with topic-based pub/sub matching (goes to orchestrator queue).
    ///
    /// Matched by `name` AND `topic` to ExternalSubscribed2 events.
    /// Feature-gated for replay engine extensibility verification.
    #[cfg(feature = "replay-version-test")]
    ExternalRaised2 {
        instance: String,
        name: String,
        topic: String,
        data: String,
    },
}

/// Provider abstraction for durable orchestration execution (persistence + queues).
///
/// # Overview
///
/// A Provider is responsible for:
/// 1. **Persistence**: Storing orchestration history (append-only event log)
/// 2. **Queueing**: Managing two work queues (orchestrator, worker)
/// 3. **Locking**: Implementing peek-lock semantics to prevent concurrent processing
/// 4. **Atomicity**: Ensuring transactional consistency across operations
///
/// # Architecture: Two-Queue Model
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────┐
/// │                     RUNTIME (2 Dispatchers)                 │
/// ├─────────────────────────────────────────────────────────────┤
/// │                                                             │
/// │  [Orchestration Dispatcher]  ←─── fetch_orchestration_item │
/// │           ↓                                                 │
/// │      Process Turn                                           │
/// │           ↓                                                 │
/// │  ack_orchestration_item ────┬──► Orchestrator Queue        │
/// │                             │   (TimerFired with delayed   │
/// │                             │    visibility for timers)   │
/// │                             └──► Worker Queue              │
/// │                                                             │
/// │  [Worker Dispatcher]  ←────────── fetch_work_item │
/// │       Execute Activity                                      │
/// │           ↓                                                 │
/// │  Completion ────────────────────► Orchestrator Queue        │
/// │                                                             │
/// └─────────────────────────────────────────────────────────────┘
///                           ↕
///              ┌────────────────────────┐
///              │   PROVIDER (Storage)   │
///              │  ┌──────────────────┐  │
///              │  │ History (Events) │  │
///              │  ├──────────────────┤  │
///              │  │ Orch Queue       │  │
///              │  │ Worker Queue     │  │
///              │  └──────────────────┘  │
///              └────────────────────────┘
/// ```
///
/// # Design Principles
///
/// **Providers should be storage abstractions, NOT orchestration engines:**
/// - ✅ Store and retrieve events as opaque data (don't inspect Event contents)
/// - ✅ Manage queues and locks (generic queue operations)
/// - ✅ Provide ACID guarantees where possible
/// - ❌ DON'T interpret orchestration semantics (use ExecutionMetadata from runtime)
/// - ❌ DON'T create events (runtime creates all events)
/// - ❌ DON'T make orchestration decisions (runtime decides control flow)
///
/// # Multi-Execution Support (ContinueAsNew)
///
/// Orchestration instances can have multiple executions (execution_id 1, 2, 3, ...):
/// - Each execution has its own event history
/// - `read()` should return the LATEST execution's history
/// - Provider tracks current_execution_id to know which execution is active
/// - When metadata.create_next_execution=true, create a new execution record/entry
///
/// **Example:**
/// ```text
/// Instance "order-123":
///   Execution 1: [OrchestrationStarted, ActivityScheduled, OrchestrationContinuedAsNew]
///   Execution 2: [OrchestrationStarted, ActivityScheduled, OrchestrationCompleted]
///   
///   read("order-123") → Returns Execution 2's events (latest)
///   read_with_execution("order-123", 1) → Returns Execution 1's events
///   latest_execution_id("order-123") → Returns Some(2)
/// ```
///
/// # Concurrency Model
///
/// The runtime runs 3 background dispatchers polling your queues:
/// 1. **Orchestration Dispatcher**: Polls fetch_orchestration_item() continuously
/// 2. **Work Dispatcher**: Polls fetch_work_item() continuously
/// 3. **Timer Dispatcher**: Polls dequeue_timer_peek_lock() continuously
///
/// **Your implementation must be thread-safe** and support concurrent access from multiple dispatchers.
///
/// # Peek-Lock Pattern (Critical)
///
/// All dequeue operations use peek-lock semantics:
/// 1. **Peek**: Select and lock a message (message stays in queue)
/// 2. **Process**: Runtime processes the locked message
/// 3. **Ack**: Delete message from queue (success) OR
/// 4. **Abandon**: Release lock for retry (failure)
///
/// Benefits:
/// - At-least-once delivery (messages survive crashes)
/// - Automatic retry on worker failure (lock expires)
/// - Prevents duplicate processing (locked messages invisible to others)
///
/// # Transactional Guarantees
///
/// **`ack_orchestration_item()` is the atomic boundary:**
/// - ALL operations in ack must succeed or fail together
/// - History append + queue enqueues + lock release = atomic
/// - If commit fails, entire turn is retried
/// - This ensures exactly-once semantics for orchestration turns
///
/// # Queue Message Flow
///
/// **Orchestrator Queue:**
/// - Inputs: StartOrchestration, ActivityCompleted/Failed, TimerFired, ExternalRaised, SubOrchCompleted/Failed, CancelInstance, ContinueAsNew
/// - Output: Processed by orchestration dispatcher → ack_orchestration_item
/// - Batching: All messages for an instance processed together
/// - Timers: TimerFired items are enqueued with `visible_at = fire_at_ms` for delayed visibility
///
/// **Worker Queue:**
/// - Inputs: ActivityExecute (from ack_orchestration_item)
/// - Output: Processed by work dispatcher → ActivityCompleted/Failed to orch queue
/// - Batching: One message at a time (activities executed independently)
///
/// **Note:** There is no separate timer queue. Timers are handled by enqueuing TimerFired items
/// directly to the orchestrator queue with delayed visibility (`visible_at` set to `fire_at_ms`).
/// The orchestrator dispatcher processes them when `visible_at <= now()`.
///
/// # Instance Metadata Management
///
/// Providers typically maintain metadata about instances:
/// - instance_id (primary key)
/// - orchestration_name, orchestration_version
/// - current_execution_id (for multi-execution support)
/// - status, output (optional, for quick queries)
/// - created_at, updated_at timestamps
///
/// This metadata is updated via:
/// - `enqueue_for_orchestrator()` with StartOrchestration
/// - `ack_orchestration_item()` with history changes
/// - `ExecutionMetadata` for status/output updates
///
/// # Required vs Optional Methods
///
/// **REQUIRED** (must implement):
/// - fetch_orchestration_item, ack_orchestration_item, abandon_orchestration_item
/// - read, append_with_execution
/// - enqueue_for_worker, fetch_work_item, ack_work_item
/// - enqueue_for_orchestrator
///
/// **OPTIONAL** (has defaults):
/// - latest_execution_id, read_with_execution
/// - list_instances, list_executions
///
/// # Recommended Database Schema (SQL Example)
///
/// ```sql
/// -- Instance metadata
/// CREATE TABLE instances (
///     instance_id TEXT PRIMARY KEY,
///     orchestration_name TEXT NOT NULL,
///     orchestration_version TEXT,  -- NULLable, set by runtime via metadata
///     current_execution_id INTEGER DEFAULT 1,
///     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
/// );
///
/// -- Execution tracking
/// CREATE TABLE executions (
///     instance_id TEXT NOT NULL,
///     execution_id INTEGER NOT NULL,
///     status TEXT DEFAULT 'Running',
///     output TEXT,
///     started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
///     completed_at TIMESTAMP,
///     PRIMARY KEY (instance_id, execution_id)
/// );
///
/// -- Event history (append-only)
/// CREATE TABLE history (
///     instance_id TEXT NOT NULL,
///     execution_id INTEGER NOT NULL,
///     event_id INTEGER NOT NULL,
///     event_type TEXT NOT NULL,
///     event_data TEXT NOT NULL,  -- JSON serialized Event
///     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
///     PRIMARY KEY (instance_id, execution_id, event_id)
/// );
///
/// -- Orchestrator queue (peek-lock)
/// CREATE TABLE orchestrator_queue (
///     id INTEGER PRIMARY KEY AUTOINCREMENT,
///     instance_id TEXT NOT NULL,
///     work_item TEXT NOT NULL,  -- JSON serialized WorkItem
///     visible_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
///     lock_token TEXT,
///     locked_until TIMESTAMP,
///     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
/// );
///
/// -- Worker queue (peek-lock with visibility control)
/// CREATE TABLE worker_queue (
///     id INTEGER PRIMARY KEY AUTOINCREMENT,
///     work_item TEXT NOT NULL,
///     visible_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
///     lock_token TEXT,
///     locked_until TIMESTAMP,
///     attempt_count INTEGER NOT NULL DEFAULT 0,
///     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
/// );
///
/// -- Instance-level locks (CRITICAL: Prevents concurrent processing)
/// CREATE TABLE instance_locks (
///     instance_id TEXT PRIMARY KEY,
///     lock_token TEXT NOT NULL,
///     locked_until INTEGER NOT NULL,  -- Unix timestamp (milliseconds)
///     locked_at INTEGER NOT NULL      -- Unix timestamp (milliseconds)
/// );
/// ```
///
/// # Implementing a New Provider: Checklist
///
/// 1. ✅ **Storage Layer**: Choose backing store (PostgreSQL, Redis, DynamoDB, etc.)
/// 2. ✅ **Serialization**: Use serde_json for Event and WorkItem (or compatible format)
/// 3. ✅ **Instance Locking**: Implement instance-level locks to prevent concurrent processing
/// 4. ✅ **Locking**: Implement peek-lock with unique tokens and expiration
/// 5. ✅ **Transactions**: Ensure `ack_orchestration_item` is atomic
/// 6. ✅ **Indexes**: Add indexes on `instance_id`, `lock_token`, `visible_at` for orchestrator queue
/// 7. ✅ **Delayed Visibility**: Support `visible_at` timestamps for TimerFired items in orchestrator queue
/// 8. ✅ **Testing**: Use `tests/sqlite_provider_validations.rs` as a template, or see `docs/provider-implementation-guide.md`
/// 9. ✅ **Multi-execution**: Support execution_id partitioning for ContinueAsNew
/// 10. ⚠️ **DO NOT**: Inspect event contents (use ExecutionMetadata)
/// 11. ⚠️ **DO NOT**: Create events (runtime owns event creation)
/// 12. ⚠️ **DO NOT**: Make orchestration decisions (runtime owns logic)
///
/// # Example: Minimal Redis Provider Sketch
///
/// ```ignore
/// struct RedisProvider {
///     client: redis::Client,
///     lock_timeout: Duration,
/// }
///
/// impl Provider for RedisProvider {
///     async fn fetch_orchestration_item(&self) -> Option<OrchestrationItem> {
///         // 1. RPOPLPUSH from "orch_queue" to "orch_processing"
///         let instance = self.client.rpoplpush("orch_queue", "orch_processing")?;
///         
///         // 2. Load history from "history:{instance}:{exec_id}" (sorted set)
///         let exec_id = self.client.get(&format!("instance:{instance}:exec_id")).unwrap_or(1);
///         let history = self.client.zrange(&format!("history:{instance}:{exec_id}"), 0, -1);
///         
///         // 3. Generate lock token and set expiration
///         let lock_token = uuid::Uuid::new_v4().to_string();
///         self.client.setex(&format!("lock:{lock_token}"), self.lock_timeout.as_secs() as usize, &instance);
///         
///         Some(OrchestrationItem { instance, history, lock_token, ... })
///     }
///     
///     async fn ack_orchestration_item(..., metadata: ExecutionMetadata) -> Result<(), String> {
///         let mut pipe = redis::pipe();
///         pipe.atomic(); // Use Redis transaction
///         
///         // Append history (ZADD to sorted set with event_id as score)
///         for event in history_delta {
///             pipe.zadd(&format!("history:{instance}:{exec_id}"), event_json, event.event_id());
///         }
///         
///         // Update metadata (HSET)
///         if let Some(status) = &metadata.status {
///             pipe.hset(&format!("exec:{instance}:{exec_id}"), "status", status);
///             pipe.hset(&format!("exec:{instance}:{exec_id}"), "output", &metadata.output);
///         }
///         
///         // Create next execution if needed
///         if metadata.create_next_execution {
///             pipe.incr(&format!("instance:{instance}:exec_id"), 1);
///         }
///         
///         // Enqueue worker/orchestrator items
///         for item in worker_items { pipe.lpush("worker_queue", serialize(item)); }
///         for item in orch_items {
///             let visible_at = match &item {
///                 WorkItem::TimerFired { fire_at_ms, .. } => *fire_at_ms,
///                 _ => now()
///             };
///             pipe.zadd("orch_queue", serialize(item), visible_at);
///         }
///         
///         // Release lock
///         pipe.del(&format!("lock:{lock_token}"));
///         pipe.lrem("orch_processing", 1, instance);
///         
///         pipe.execute()?;
///         Ok(())
///     }
/// }
/// ```
///
/// # Design Principles
///
/// **Providers should be storage abstractions, NOT orchestration engines:**
/// - ✅ Store and retrieve events as opaque data (don't inspect Event contents)
/// - ✅ Manage queues and locks (generic queue operations)
/// - ✅ Provide ACID guarantees where possible
/// - ❌ DON'T interpret orchestration semantics (use ExecutionMetadata from runtime)
/// - ❌ DON'T create events (runtime creates all events)
/// - ❌ DON'T make orchestration decisions (runtime decides control flow)
///
/// # Multi-Execution Support (ContinueAsNew)
///
/// Orchestration instances can have multiple executions (execution_id 1, 2, 3, ...):
/// - Each execution has its own event history
/// - `read()` should return the LATEST execution's history
/// - Provider tracks current_execution_id to know which execution is active
/// - When metadata.create_next_execution=true, create a new execution record/entry
///
/// **Example:**
/// ```text
/// Instance "order-123":
///   Execution 1: [OrchestrationStarted, ActivityScheduled, OrchestrationContinuedAsNew]
///   Execution 2: [OrchestrationStarted, ActivityScheduled, OrchestrationCompleted]
///   
///   read("order-123") → Returns Execution 2's events (latest)
///   read_with_execution("order-123", 1) → Returns Execution 1's events
///   latest_execution_id("order-123") → Returns Some(2)
///   list_executions("order-123") → Returns vec!\[1, 2\]
/// ```
///
/// # Concurrency Model
///
/// The runtime runs 3 background dispatchers polling your queues:
/// 1. **Orchestration Dispatcher**: Polls fetch_orchestration_item() continuously
/// 2. **Work Dispatcher**: Polls fetch_work_item() continuously
/// 3. **Timer Dispatcher**: Polls dequeue_timer_peek_lock() continuously
///
/// **Your implementation must be thread-safe** and support concurrent access from multiple dispatchers.
///
/// # Peek-Lock Pattern (Critical)
///
/// All dequeue operations use peek-lock semantics:
/// 1. **Peek**: Select and lock a message (message stays in queue)
/// 2. **Process**: Runtime processes the locked message
/// 3. **Ack**: Delete message from queue (success) OR
/// 4. **Abandon**: Release lock for retry (failure)
///
/// Benefits:
/// - At-least-once delivery (messages survive crashes)
/// - Automatic retry on worker failure (lock expires)
/// - Prevents duplicate processing (locked messages invisible to others)
///
/// # Transactional Guarantees
///
/// **`ack_orchestration_item()` is the atomic boundary:**
/// - ALL operations in ack must succeed or fail together
/// - History append + queue enqueues + lock release = atomic
/// - If commit fails, entire turn is retried
/// - This ensures exactly-once semantics for orchestration turns
///
/// # Queue Message Flow
///
/// **Orchestrator Queue:**
/// - Inputs: StartOrchestration, ActivityCompleted/Failed, TimerFired, ExternalRaised, SubOrchCompleted/Failed, CancelInstance, ContinueAsNew
/// - Output: Processed by orchestration dispatcher → ack_orchestration_item
/// - Batching: All messages for an instance processed together
/// - Ordering: FIFO per instance preferred
/// - Timers: TimerFired items are enqueued with `visible_at = fire_at_ms` for delayed visibility
///
/// **Worker Queue:**
/// - Inputs: ActivityExecute (from ack_orchestration_item)
/// - Output: Processed by work dispatcher → ActivityCompleted/Failed to orch queue
/// - Batching: One message at a time (activities executed independently)
/// - Ordering: FIFO preferred but not required
///
/// **Note:** There is no separate timer queue. Timers are handled by enqueuing TimerFired items
/// directly to the orchestrator queue with delayed visibility (`visible_at` set to `fire_at_ms`).
/// The orchestrator dispatcher processes them when `visible_at <= now()`.
///
/// # Instance Metadata Management
///
/// Providers typically maintain metadata about instances:
/// - instance_id (primary key)
/// - orchestration_name, orchestration_version (from StartOrchestration)
/// - current_execution_id (for multi-execution support)
/// - status, output (optional, from ExecutionMetadata)
/// - created_at, updated_at timestamps
///
/// This metadata is updated via:
/// - `ack_orchestration_item()` with ExecutionMetadata (creates instance and updates status/output)
/// - `ack_orchestration_item()` with metadata.create_next_execution (increments current_execution_id)
///
/// **Note:** Instances are NOT created in `enqueue_for_orchestrator()`. Instance creation happens
/// when the runtime acknowledges the first turn via `ack_orchestration_item()` with metadata.
///
/// # Error Handling Philosophy
///
/// - **Transient errors** (busy, timeout): Return error, let runtime retry
/// - **Invalid tokens**: Return Ok (idempotent - already processed)
/// - **Corruption** (missing instance, invalid data): Return error
/// - **Concurrency conflicts**: Handle via locks, return error if deadlock
///
/// # Required vs Optional Methods
///
/// **REQUIRED** (must implement):
/// - fetch_orchestration_item, ack_orchestration_item, abandon_orchestration_item
/// - read, append_with_execution
/// - enqueue_for_worker, fetch_work_item, ack_work_item
/// - enqueue_for_orchestrator
///
/// **OPTIONAL** (has defaults):
/// - latest_execution_id, read_with_execution
/// - list_instances, list_executions
///
/// # Testing Your Provider
///
/// See `tests/sqlite_provider_validations.rs` for comprehensive provider validation tests:
/// - Basic enqueue/dequeue operations
/// - Transactional atomicity
/// - Instance locking correctness (including multi-threaded tests)
/// - Lock expiration and redelivery
/// - Multi-execution support
/// - Execution status and output persistence
///
/// All tests use the Provider trait directly (not runtime), so they're portable to new providers.
/// See `docs/provider-implementation-guide.md` for detailed implementation guidance including instance locks.
///
/// Core provider trait for runtime orchestration operations.
///
/// This trait defines the essential methods required for durable orchestration execution.
/// It focuses on runtime-critical operations: fetching work items, processing history,
/// and managing queues. Management and observability features are provided through
/// optional capability traits.
///
/// # Capability Discovery
///
/// Providers can implement additional capability traits (like `ProviderAdmin`)
/// to expose richer features. The `Client` automatically discovers these capabilities
/// through the `as_management_capability()` method.
///
/// # Implementation Guide for LLMs
///
/// When implementing a new provider, focus on these core methods:
///
/// ## Required Methods (9 total)
///
/// 1. **Orchestration Processing (3 methods)**
///    - `fetch_orchestration_item()` - Atomic batch processing
///    - `ack_orchestration_item()` - Commit processing results
///    - `abandon_orchestration_item()` - Release locks and retry
///
/// 2. **History Access (1 method)**
///    - `read()` - Get event history for status checks
///
/// 3. **Worker Queue (2 methods)**
///    - `fetch_work_item()` - Get activity work items
///    - `ack_work_item()` - Acknowledge activity completion
///
/// 4. **Orchestrator Queue (1 method)**
///    - `enqueue_for_orchestrator()` - Enqueue control messages (including TimerFired with delayed visibility)
///
/// ## Optional Management Methods (2 methods)
///
/// These are included for backward compatibility but will be moved to
/// `ProviderAdmin` in future versions:
///
/// - `list_instances()` - List all instance IDs
/// - `list_executions()` - List execution IDs for an instance
///
/// # Capability Detection
///
/// Implement `as_management_capability()` to expose management features:
///
/// ```ignore
/// impl Provider for MyProvider {
///     // ... implement required methods
///     
///     fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
///         Some(self as &dyn ProviderAdmin)
///     }
/// }
///
/// impl ProviderAdmin for MyProvider {
///     // ... implement management methods
/// }
/// ```
///
/// # Testing Your Provider
///
/// See `tests/sqlite_provider_validations.rs` for comprehensive provider validation tests:
/// - Basic enqueue/dequeue operations
/// - Transactional atomicity
/// - Instance locking correctness (including multi-threaded tests)
/// - Lock expiration and redelivery
/// - Multi-execution support
/// - Execution status and output persistence
///
/// All tests use the Provider trait directly (not runtime), so they're portable to new providers.
/// See `docs/provider-implementation-guide.md` for detailed implementation guidance including instance locks.
///
#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
pub trait Provider: Any + Send + Sync {
    // ===== Provider Identity =====

    /// Returns the name of this provider implementation.
    ///
    /// Used for logging and diagnostics. Override to provide a meaningful name.
    ///
    /// Default: "unknown"
    fn name(&self) -> &str {
        "unknown"
    }

    /// Returns the version of this provider implementation.
    ///
    /// Used for logging and diagnostics. Override to provide the version.
    ///
    /// Default: "0.0.0"
    fn version(&self) -> &str {
        "0.0.0"
    }

    // ===== Core Atomic Orchestration Methods (REQUIRED) =====
    // These three methods form the heart of reliable orchestration execution.
    //
    // ⚠️ CRITICAL ID GENERATION CONTRACT:
    //
    // The provider MUST NOT generate execution_id or event_id values.
    // All IDs are generated by the runtime and passed to the provider:
    //   - execution_id: Passed to ack_orchestration_item()
    //   - event_id: Set in each Event in the history_delta
    //
    // The provider's role is to STORE these IDs, not generate them.

    /// Fetch the next orchestration work item atomically.
    ///
    /// # What This Does
    ///
    /// 1. **Select an instance to process**: Find the next available message in orchestrator queue that is not locked
    /// 2. **Acquire instance-level lock**: Atomically claim the instance lock to prevent concurrent processing
    /// 3. **Lock ALL messages** for that instance (batch processing)
    /// 4. **Load history**: Get complete event history for the current execution
    /// 5. **Load metadata**: Get orchestration_name, version, execution_id
    /// 6. **Return locked batch**: Provider must prevent other processes from touching this instance
    ///
    /// # ⚠️ CRITICAL: Instance-Level Locking
    ///
    /// **You MUST acquire an instance-level lock BEFORE fetching messages.** This prevents concurrent
    /// dispatchers from processing the same instance simultaneously, which would cause race conditions
    /// and data corruption.
    ///
    /// **Required Implementation Pattern:**
    /// 1. Find available instance (check no other lock is held for it)
    /// 2. Atomically acquire instance lock (with conflict handling)
    /// 3. Verify lock acquisition succeeded
    /// 4. Only then proceed to lock and fetch messages
    ///
    /// See `docs/provider-implementation-guide.md` for detailed implementation guidance.
    ///
    /// # Peek-Lock Semantics
    ///
    /// - Messages remain in queue until `ack_orchestration_item()` is called
    /// - Generate a unique `lock_token` (e.g., UUID)
    /// - Set `locked_until` timestamp (e.g., now + 30 seconds)
    /// - If lock expires before ack, messages become available again (automatic retry)
    ///
    /// # Visibility Filtering
    ///
    /// Only return messages where `visible_at <= now()`:
    /// - Normal messages: visible immediately
    /// - Delayed messages: `visible_at = now + delay` (used for timer backpressure)
    ///
    /// # Instance Batching
    ///
    /// **CRITICAL:** All messages in the batch must belong to the SAME instance.
    ///
    /// Implementation steps:
    /// 1. Find the first visible, unlocked instance in the orchestrator queue
    /// 2. Atomically acquire the instance lock (prevent concurrent processing)
    /// 3. Lock ALL queued messages for that instance
    /// 4. Fetch all locked messages for processing
    ///
    /// # History Loading
    ///
    /// - For new instances (no history yet): return empty Vec
    /// - For existing instances: return ALL events for current execution_id, ordered by event_id
    /// - For multi-execution instances: return ONLY the LATEST execution's history
    ///
    /// SQLite example:
    /// ```text
    /// // Get current execution ID
    /// let exec_id = SELECT current_execution_id FROM instances WHERE instance_id = ?
    ///
    /// // Load history for that execution
    /// SELECT event_data FROM history
    /// WHERE instance_id = ? AND execution_id = ?
    /// ORDER BY event_id
    /// ```
    ///
    /// # Return Value
    ///
    /// - `Some(OrchestrationItem)` - Work is available and locked
    /// - `None` - No work available (dispatcher will sleep and retry)
    ///
    /// # Error Handling
    ///
    /// Don't panic on transient errors - return None and let dispatcher retry.
    /// Only panic on unrecoverable errors (e.g., corrupted data or schema).
    ///
    /// # Concurrency
    ///
    /// This method is called continuously by the orchestration dispatcher.
    /// Must be thread-safe and handle concurrent calls gracefully.
    ///
    /// # Example Implementation Pattern
    ///
    /// ```ignore
    /// async fn fetch_orchestration_item(&self) -> Option<OrchestrationItem> {
    ///     let tx = begin_transaction()?;
    ///     
    ///     // Step 1: Find next available instance (check instance_locks)
    ///     let instance_id = SELECT q.instance_id FROM orch_queue q
    ///         LEFT JOIN instance_locks il ON q.instance_id = il.instance_id
    ///         WHERE q.visible_at <= now()
    ///           AND (il.instance_id IS NULL OR il.locked_until <= now())
    ///         ORDER BY q.id LIMIT 1;
    ///     
    ///     if instance_id.is_none() { return None; }
    ///     
    ///     // Step 2: Atomically acquire instance lock
    ///     let lock_token = generate_uuid();
    ///     let lock_result = INSERT INTO instance_locks (instance_id, lock_token, locked_until, locked_at)
    ///         VALUES (?, ?, ?, ?)
    ///         ON CONFLICT(instance_id) DO UPDATE
    ///         SET lock_token = excluded.lock_token, locked_until = excluded.locked_until
    ///         WHERE locked_until <= excluded.locked_at;
    ///     
    ///     if lock_result.rows_affected == 0 {
    ///         // Lock acquisition failed - another dispatcher has the lock
    ///         return None;
    ///     }
    ///     
    ///     // Step 3: Lock all messages for this instance
    ///     UPDATE orch_queue SET lock_token = ?, locked_until = now() + 30s
    ///         WHERE instance_id = ? AND visible_at <= now();
    ///     
    ///     // Step 4: Fetch locked messages
    ///     let messages = SELECT work_item FROM orch_queue WHERE lock_token = ?;
    ///     
    ///     // Step 5: Load instance metadata
    ///     let (name, version, exec_id) = SELECT ... FROM instances WHERE instance_id = ?;
    ///     
    ///     // Step 6: Load history for current execution
    ///     let history = SELECT event_data FROM history
    ///         WHERE instance_id = ? AND execution_id = ?
    ///         ORDER BY event_id;
    ///     
    ///     commit_transaction();
    ///     Some((OrchestrationItem { instance, orchestration_name, execution_id, version, history, messages }, lock_token, attempt_count))
    /// }
    /// ```
    ///
    /// # Return Value
    ///
    /// * `Ok(Some((item, lock_token, attempt_count)))` - Orchestration item with lock token and attempt count
    ///   - `item`: The orchestration item to process
    ///   - `lock_token`: Unique token for ack/abandon operations
    ///   - `attempt_count`: Number of times this item has been fetched (for poison detection)
    /// * `Ok(None)` - No work available
    /// * `Err(ProviderError)` - Storage error
    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError>;

    /// Acknowledge successful orchestration processing atomically.
    ///
    /// This is the most critical method - it commits all changes from an orchestration turn atomically.
    ///
    /// # What This Does (ALL must be atomic)
    ///
    /// 1. **Validate lock token**: Verify instance lock is still valid and matches lock_token
    /// 2. **Remove instance lock**: Release the instance lock (processing complete)
    /// 3. **Append new events** to history for current execution
    /// 4. **Update execution metadata** (status, output) using pre-computed metadata
    /// 5. **Create new execution** if metadata.create_next_execution=true (ContinueAsNew)
    /// 6. **Enqueue worker_items** to worker queue (activity executions)
    /// 7. **Enqueue orchestrator_items** to orchestrator queue (completions, new instances, TimerFired with delayed visibility)
    /// 8. **Remove acknowledged messages** from orchestrator queue (release lock)
    ///
    /// # Atomicity Requirements
    ///
    /// **CRITICAL:** All 8 operations above must succeed or fail together.
    ///
    /// - **If ANY operation fails**: Roll back all changes
    /// - **If all operations succeed**: All changes are durable and visible
    ///
    /// This prevents:
    /// - Duplicate activity execution (history saved but worker item lost)
    /// - Lost completions (worker item enqueued but history not saved)
    /// - Orphaned locks (messages deleted but history append failed)
    /// - Stale instance locks (instance lock not removed)
    ///
    /// # Parameters
    ///
    /// * `lock_token` - Token from fetch_orchestration_item() - identifies locked messages
    /// * `history_delta` - New events to append (runtime assigns event_ids, provider stores as-is)
    /// * `worker_items` - Activity executions to enqueue (WorkItem::ActivityExecute)
    /// * `orchestrator_items` - Orchestrator messages to enqueue (completions, starts, TimerFired with delayed visibility)
    /// * `metadata` - Pre-computed execution state (DO NOT inspect events yourself!)
    ///
    /// # Event Storage (history_delta)
    ///
    /// **Important:** Store events exactly as provided. DO NOT:
    /// - Modify event_id (runtime assigns these)
    /// - Inspect event contents to make decisions
    /// - Filter or reorder events
    ///
    /// SQLite example:
    /// ```text
    /// for event in &history_delta {
    ///     let event_json = serde_json::to_string(&event)?;
    ///     let event_type = extract_discriminant_name(&event); // For indexing only
    ///     INSERT INTO history (instance_id, execution_id, event_id, event_data, event_type)
    ///     VALUES (?, ?, event.event_id(), event_json, event_type)
    /// }
    /// ```
    ///
    /// # Metadata Storage (ExecutionMetadata)
    ///
    /// **The runtime has ALREADY inspected events and computed metadata.**
    /// Provider just stores it:
    ///
    /// ```text
    /// // Update execution status/output from metadata
    /// if let Some(status) = &metadata.status {
    ///     UPDATE executions
    ///     SET status = ?, output = ?, completed_at = CURRENT_TIMESTAMP
    ///     WHERE instance_id = ? AND execution_id = ?
    /// }
    ///
    /// // Create next execution if requested
    /// if metadata.create_next_execution {
    ///     if let Some(next_id) = metadata.next_execution_id {
    ///         INSERT INTO executions (instance_id, execution_id, status)
    ///         VALUES (?, next_id, 'Running')
    ///         
    ///         UPDATE instances SET current_execution_id = next_id
    ///     }
    /// }
    /// ```
    ///
    /// # Queue Item Enqueuing
    ///
    /// Worker items and orchestrator items must be enqueued within the same transaction:
    ///
    /// ```text
    /// // Worker queue (no special handling)
    /// for item in worker_items {
    ///     INSERT INTO worker_queue (work_item) VALUES (serde_json::to_string(&item))
    /// }
    ///
    /// // Orchestrator queue (may have delayed visibility for TimerFired)
    /// for item in orchestrator_items {
    ///     let visible_at = match &item {
    ///         WorkItem::TimerFired { fire_at_ms, .. } => *fire_at_ms,  // Delayed visibility
    ///         _ => now()  // Immediate visibility
    ///     };
    ///     
    ///     INSERT INTO orchestrator_queue (instance_id, work_item, visible_at)
    ///     VALUES (extract_instance(&item), serde_json::to_string(&item), visible_at)
    /// }
    /// ```
    ///
    /// # Lock Release
    ///
    /// Remove all acknowledged messages associated with this lock_token.
    ///
    /// # Error Handling
    ///
    /// - Return `Ok(())` if all operations succeeded
    /// - Return `Err(msg)` if any operation failed (all changes rolled back)
    /// - On error, runtime will call `abandon_orchestration_item()` to release lock
    ///
    /// # Special Cases
    ///
    /// **StartOrchestration in orchestrator_items:**
    /// - May need to create instance metadata record/entry (idempotent create-if-not-exists)
    /// - Should create execution record/entry with ID=1 if new instance
    ///
    /// **Empty history_delta:**
    /// - Valid case (e.g., terminal instance being acked with no changes)
    /// - Still process queues and release lock
    ///
    /// # Parameters
    ///
    /// * `lock_token` - Token from `fetch_orchestration_item` identifying the batch
    /// * `execution_id` - **The execution ID this history belongs to** (runtime decides this)
    /// * `history_delta` - Events to append to the specified execution
    /// * `worker_items` - Activity work items to enqueue
    /// * `orchestrator_items` - Orchestration work items to enqueue (StartOrchestration, ContinueAsNew, TimerFired, etc.)
    ///   - TimerFired items should be enqueued with `visible_at = fire_at_ms` for delayed visibility
    /// * `metadata` - Pre-computed execution metadata (status, output)
    ///
    /// # TimerFired Items
    ///
    /// When `TimerFired` items are present in `orchestrator_items`, they should be enqueued with
    /// delayed visibility using the `fire_at_ms` field for the `visible_at` timestamp.
    /// This allows timers to fire at the correct logical time.
    ///
    /// # execution_id Parameter
    ///
    /// The `execution_id` parameter tells the provider **which execution this history belongs to**.
    /// The runtime is responsible for:
    /// - Deciding when to create new executions (e.g., for ContinueAsNew)
    /// - Managing execution ID sequencing
    /// - Ensuring each execution has its own isolated history
    ///
    /// The provider should:
    /// - **Create the execution record if it doesn't exist** (idempotent create-if-not-exists)
    /// - Append `history_delta` to the specified `execution_id`
    /// - Update the instance's current execution pointer if this execution_id is newer
    /// - NOT inspect WorkItems to decide execution IDs
    ///
    /// # SQLite Implementation Pattern
    ///
    /// ```text
    /// async fn ack_orchestration_item(execution_id, ...) -> Result<(), String> {
    ///     let tx = begin_transaction()?;
    ///     
    ///     // Step 1: Validate lock token and get instance_id from instance_locks
    ///     let instance_id = SELECT instance_id FROM instance_locks
    ///         WHERE lock_token = ? AND locked_until > now();
    ///     if instance_id.is_none() {
    ///         return Err("Invalid or expired lock token");
    ///     }
    ///     
    ///     // Step 2: Remove instance lock (processing complete)
    ///     DELETE FROM instance_locks WHERE instance_id = ? AND lock_token = ?;
    ///     
    ///     // Step 3: Create execution record if it doesn't exist (idempotent)
    ///     INSERT OR IGNORE INTO executions (instance_id, execution_id, status)
    ///     VALUES (?, ?, 'Running');
    ///     
    ///     // Step 4: Append history to the SPECIFIED execution_id
    ///     for event in history_delta {
    ///         INSERT INTO history (...) VALUES (instance_id, execution_id, event.event_id(), ...)
    ///     }
    ///     
    ///     // Step 5: Update execution metadata (no event inspection!)
    ///     if let Some(status) = &metadata.status {
    ///         UPDATE executions SET status = ?, output = ?, completed_at = NOW()
    ///         WHERE instance_id = ? AND execution_id = ?
    ///     }
    ///     
    ///     // Step 6: Update current_execution_id if this is a newer execution
    ///     UPDATE instances SET current_execution_id = GREATEST(current_execution_id, ?)
    ///     WHERE instance_id = ?;
    ///     
    ///     // Step 7: Enqueue worker/orchestrator items
    ///     // Worker items go to worker queue (populate identity columns)
    ///     for item in worker_items {
    ///         if let WorkItem::ActivityExecute { instance, execution_id, id, .. } = &item {
    ///             INSERT INTO worker_queue (work_item, instance_id, execution_id, activity_id)
    ///             VALUES (serialize(item), instance, execution_id, id)
    ///         }
    ///     }
    ///     
    ///     // Orchestrator items go to orchestrator queue (TimerFired uses fire_at_ms for visible_at)
    ///     for item in orchestrator_items {
    ///         let visible_at = match &item {
    ///             WorkItem::TimerFired { fire_at_ms, .. } => *fire_at_ms,
    ///             _ => now()
    ///         };
    ///         INSERT INTO orchestrator_queue (instance_id, work_item, visible_at)
    ///         VALUES (extract_instance(&item), serialize(item), visible_at)
    ///     }
    ///     
    ///     // Step 8: Delete cancelled activities from worker queue
    ///     DELETE FROM worker_queue
    ///     WHERE (instance_id = ? AND execution_id = ? AND activity_id = ?)
    ///        OR (instance_id = ? AND execution_id = ? AND activity_id = ?)
    ///        ...
    ///     
    ///     // Step 9: Release lock: DELETE FROM orch_queue WHERE lock_token = ?;
    ///     
    ///     commit_transaction()?;
    ///     Ok(())
    /// }
    /// ```
    async fn ack_orchestration_item(
        &self,
        _lock_token: &str,
        _execution_id: u64,
        _history_delta: Vec<Event>,
        _worker_items: Vec<WorkItem>,
        _orchestrator_items: Vec<WorkItem>,
        _metadata: ExecutionMetadata,
        _cancelled_activities: Vec<ScheduledActivityIdentifier>,
    ) -> Result<(), ProviderError>;

    /// Abandon orchestration processing (used for errors/retries).
    ///
    /// Called when orchestration processing fails (e.g., storage contention, runtime crash).
    /// The messages must be made available for reprocessing.
    ///
    /// # What This Does
    ///
    /// 1. **Clear lock_token** from messages (make available again)
    /// 2. **Remove instance lock** (release instance-level lock)
    /// 3. **Optionally delay** retry by setting visibility timestamp
    /// 4. **Preserve message order** (don't reorder or modify messages)
    ///
    /// # Parameters
    ///
    /// * `lock_token` - Token from fetch_orchestration_item()
    /// * `delay` - Optional delay before messages become visible again
    ///   - `None`: immediate retry (visible_at = now)
    ///   - `Some(duration)`: delayed retry (visible_at = now + duration)
    ///
    /// # Implementation Pattern
    ///
    /// ```ignore
    /// async fn abandon_orchestration_item(lock_token: &str, delay: Option<Duration>) -> Result<(), String> {
    ///     let visible_at = if let Some(delay) = delay {
    ///         now() + delay
    ///     } else {
    ///         now()
    ///     };
    ///     
    ///     BEGIN TRANSACTION
    ///         // Get instance_id from instance_locks
    ///         let instance_id = SELECT instance_id FROM instance_locks WHERE lock_token = ?;
    ///         
    ///         IF instance_id IS NULL:
    ///             // Invalid lock token - return error
    ///             ROLLBACK
    ///             RETURN Err("Invalid lock token")
    ///         
    ///         // Clear lock_token from messages
    ///         UPDATE orchestrator_queue
    ///         SET lock_token = NULL, locked_until = NULL, visible_at = ?
    ///         WHERE lock_token = ?;
    ///         
    ///         // Remove instance lock
    ///         DELETE FROM instance_locks WHERE lock_token = ?;
    ///     COMMIT
    ///     
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Use Cases
    ///
    /// - Storage contention → delay = Some(Duration::from_millis(50)) for backoff
    /// - Orchestration turn failed → delay = None for immediate retry
    /// - Runtime shutdown during processing → messages auto-recover when lock expires
    ///
    /// # Error Handling
    ///
    /// **Invalid lock tokens MUST return an error.** Unlike `ack_orchestration_item()`, this method
    /// should not be idempotent for invalid tokens. Invalid tokens indicate a programming error or
    /// state corruption and should be surfaced as errors.
    ///
    /// Return `Err("Invalid lock token")` if the lock token is not recognized or has expired.
    ///
    /// # Parameters
    ///
    /// * `lock_token` - The lock token from `fetch_orchestration_item`
    /// * `delay` - Optional delay before message becomes visible again
    /// * `ignore_attempt` - If true, decrement the attempt count (never below 0)
    async fn abandon_orchestration_item(
        &self,
        _lock_token: &str,
        _delay: Option<Duration>,
        _ignore_attempt: bool,
    ) -> Result<(), ProviderError>;

    /// Read the full history for the latest execution of an instance.
    ///
    /// # What This Does
    ///
    /// Returns ALL events for the LATEST execution of an instance, ordered by event_id.
    ///
    /// # Multi-Execution Behavior
    ///
    /// For instances with multiple executions (from ContinueAsNew):
    /// - Find the latest execution_id (MAX(execution_id))
    /// - Return events for ONLY that execution
    /// - DO NOT return events from earlier executions
    ///
    /// # Return Value
    ///
    /// - `Ok(Vec<Event>)` - Events ordered by event_id ascending (event_id 1, 2, 3, ...)
    /// - `Err(ProviderError)` - Storage error
    ///
    /// # Usage
    ///
    /// **NOT called in runtime hot path.** Used by:
    /// - Client.get_orchestration_status() - to determine current state
    /// - Runtime.get_orchestration_descriptor() - to get orchestration metadata
    /// - Testing and validation suites
    ///
    /// The runtime hot path uses `fetch_orchestration_item()` which loads history internally.
    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError>;

    // ===== History Reading and Testing Methods (NOT used by runtime hot path) =====
    // These methods are NOT called by the main runtime during orchestration execution.
    // They are used by testing, validation suites, client APIs, and debugging tools.

    /// Read the full event history for a specific execution within an instance.
    ///
    /// **NOT called in runtime hot path.** Used by testing, validation suites, and debugging tools.
    ///
    /// # Parameters
    ///
    /// * `instance` - The ID of the orchestration instance.
    /// * `execution_id` - The specific execution ID to read history for.
    ///
    /// # Returns
    ///
    /// Vector of events in chronological order (oldest first).
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn read_with_execution(&self, instance: &str, execution_id: u64) -> Result<Vec<Event>, ProviderError> {
    ///     SELECT event_data FROM history
    ///     WHERE instance_id = ? AND execution_id = ?
    ///     ORDER BY event_id
    /// }
    /// ```
    async fn read_with_execution(&self, _instance: &str, _execution_id: u64) -> Result<Vec<Event>, ProviderError>;

    /// Append events to a specific execution.
    ///
    /// **NOT called in runtime hot path.** Used by testing and validation suites for fault injection
    /// and test setup. The runtime uses `ack_orchestration_item()` to append history atomically.
    ///
    /// # What This Does
    ///
    /// Add new events to the history log for a specific execution.
    ///
    /// # CRITICAL: Event ID Assignment
    ///
    /// **The runtime assigns event_ids BEFORE calling this method.**
    /// - DO NOT modify event.event_id()
    /// - DO NOT renumber events
    /// - Store events exactly as provided
    ///
    /// # Duplicate Detection
    ///
    /// Events with duplicate (instance_id, execution_id, event_id) should:
    /// - Either: Reject with error (let runtime handle)
    /// - Or: IGNORE (idempotent append)
    /// - NEVER: Overwrite existing event (corrupts history)
    ///
    /// # Parameters
    ///
    /// * `instance` - The ID of the orchestration instance.
    /// * `execution_id` - The specific execution ID to append events to.
    /// * `new_events` - Vector of events to append (event_ids already assigned).
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, `Err(ProviderError)` on failure.
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn append_with_execution(
    ///     &self,
    ///     instance: &str,
    ///     execution_id: u64,
    ///     new_events: Vec<Event>,
    /// ) -> Result<(), ProviderError> {
    ///     for event in &new_events {
    ///         INSERT INTO history (instance_id, execution_id, event_id, event_data)
    ///         VALUES (?, ?, event.event_id(), serialize(event))
    ///     }
    ///     Ok(())
    /// }
    /// ```
    async fn append_with_execution(
        &self,
        _instance: &str,
        _execution_id: u64,
        _new_events: Vec<Event>,
    ) -> Result<(), ProviderError>;

    // ===== Worker Queue Operations (REQUIRED) =====
    // Worker queue processes activity executions.

    /// Enqueue an activity execution request.
    ///
    /// # What This Does
    ///
    /// Add a WorkItem::ActivityExecute to the worker queue for background processing.
    ///
    /// # Implementation
    ///
    /// ```ignore
    /// async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), String> {
    ///     INSERT INTO worker_queue (work_item)
    ///     VALUES (serde_json::to_string(&item)?)
    /// }
    /// ```
    ///
    /// # Locking
    ///
    /// New messages should have lock_token = NULL (available for dequeue).
    ///
    /// # Ordering
    ///
    /// FIFO order preferred but not strictly required.
    async fn enqueue_for_worker(&self, _item: WorkItem) -> Result<(), ProviderError>;

    /// Dequeue a single work item with peek-lock semantics.
    ///
    /// # What This Does
    ///
    /// Fetch next available worker task.
    ///
    /// 1. Find an available task in the worker queue (visible_at <= now, not locked)
    /// 2. Lock it with a unique token
    /// 3. Return item + token (item stays in queue until ack)
    ///
    /// # Parameters
    ///
    /// - `lock_timeout`: Duration to lock the item for processing.
    /// - `poll_timeout`: Maximum time to wait for work. Provider MAY wait up to this
    ///   duration if it supports long polling, or return immediately if it doesn't.
    /// - `session`: Session routing configuration.
    ///   - `Some(config)`: Session-aware fetch — returns non-session + owned + claimable session items.
    ///   - `None`: Non-session only — returns only items with `session_id IS NULL`.
    ///
    /// # Return Value
    ///
    /// - `Ok(Some((WorkItem, String, u32)))` - Item is locked and ready to process
    ///   - WorkItem: The work item to process
    ///   - String: Lock token for ack/abandon
    ///   - u32: Attempt count (number of times this item has been fetched)
    /// - `Ok(None)` - No work available
    /// - `Err(ProviderError)` - Storage error (allows runtime to distinguish empty queue from failures)
    ///
    /// # Concurrency
    ///
    /// Called continuously by work dispatcher. Must prevent double-dequeue.
    ///
    /// # Session Routing
    ///
    /// When `session` is `Some(config)`, the provider must filter eligible items:
    /// - Non-session items (`session_id IS NULL`): always eligible
    /// - Owned-session items: items whose `session_id` has an active session owned by `config.owner_id`
    /// - Claimable-session items: items whose `session_id` has no active session (unowned)
    ///
    /// When fetching a claimable session-bound item, atomically create or update the session
    /// record with `config.owner_id` and `config.lock_timeout`.
    ///
    /// When `session` is `None`, only non-session items (`session_id IS NULL`) are returned.
    /// Session-bound items are skipped entirely.
    ///
    /// # Tag Filtering
    ///
    /// The `tag_filter` parameter controls which activities this worker processes:
    /// - `DefaultOnly`: Only items with `tag IS NULL`
    /// - `Tags(set)`: Only items whose `tag` is in `set`
    /// - `DefaultAnd(set)`: Items with `tag IS NULL` OR `tag` in `set`
    /// - `None`: No items (orchestrator-only mode)
    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError>;

    /// Acknowledge successful processing of a work item.
    ///
    /// # What This Does
    ///
    /// Atomically acknowledge worker item and optionally enqueue completion to orchestrator queue.
    ///
    /// # Purpose
    ///
    /// Ensures completion delivery and worker ack happen atomically.
    /// Prevents lost completions if enqueue succeeds but ack fails.
    /// Prevents duplicate work if ack succeeds but enqueue fails.
    ///
    /// # Parameters
    ///
    /// * `token` - Lock token from fetch_work_item
    /// * `completion` - Optional completion work item:
    ///   - `Some(WorkItem)`: Remove worker item AND enqueue completion to orchestrator queue
    ///   - `None`: Remove worker item WITHOUT enqueueing anything (used when activity was cancelled)
    ///
    /// # Error on Missing Entry
    ///
    /// **CRITICAL**: This method MUST return a non-retryable error if the locked worker work-item
    /// entry does not exist anymore (was already removed) or cannot be acknowledged as expected.
    /// This can happen when:
    /// - The activity was cancelled (the backing entry was removed/invalidated by orchestration)
    /// - The lock was stolen by another worker (lock expired and work-item was re-fetched/completed)
    /// - The lock expired (worker took too long without renewing)
    ///
    /// The worker dispatcher uses this error to detect cancellation races and log appropriately.
    ///
    /// # Implementation
    ///
    /// ```ignore
    /// async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) -> Result<(), ProviderError> {
    ///     BEGIN TRANSACTION
    ///         // Relational/SQL example:
    ///         let result = DELETE FROM worker_queue WHERE lock_token = ?token AND locked_until > now();
    ///         
    ///         // CRITICAL: Check if the locked entry existed and lock was still valid
    ///         if result.rows_affected == 0 {
    ///             ROLLBACK
    ///             return Err(ProviderError::permanent("Work item not found - cancelled, stolen, or lock expired"))
    ///         }
    ///         
    ///         if let Some(completion) = completion {
    ///             INSERT INTO orchestrator_queue (instance_id, work_item, visible_at)
    ///             VALUES (completion.instance, serialize(completion), now)
    ///         }
    ///     COMMIT
    /// }
    /// ```
    async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) -> Result<(), ProviderError>;

    /// Renew the lock on a worker queue item.
    ///
    /// # What This Does
    ///
    /// Extends the lock timeout for an in-progress activity execution, preventing
    /// the lock from expiring while the activity is still being processed.
    ///
    /// # Purpose
    ///
    /// Enables long-running activities to complete without lock timeout:
    /// - Worker fetches activity with initial lock timeout (e.g., 30s)
    /// - Background renewal task periodically extends the lock
    /// - If worker crashes, renewal stops and lock expires naturally
    /// - Another worker can then pick up the abandoned activity
    ///
    /// # Activity Cancellation
    ///
    /// This method also serves as the cancellation detection mechanism. When the
    /// orchestration runtime decides an activity should be cancelled, it removes or invalidates
    /// the backing entry for that locked work-item (e.g., by
    /// removing the corresponding entry). The next renewal attempt will fail, signaling to the
    /// worker that the activity should be cancelled.
    ///
    /// # Parameters
    ///
    /// * `token` - Lock token from fetch_work_item
    /// * `extend_for` - [`Duration`] to extend lock from now (typically matches `RuntimeOptions::worker_lock_timeout`)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Lock renewed successfully
    /// * `Err(ProviderError)` - Lock renewal failed:
    ///   - Permanent error: Token invalid, expired, entry removed (cancelled), or already acked
    ///   - Retryable error: Storage connection issues
    ///
    /// # Implementation Pattern
    ///
    /// ```ignore
    /// async fn renew_work_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
    ///     // Relational/SQL example:
    ///     let result = UPDATE worker_queue
    ///                  SET locked_until = now() + extend_for
    ///                  WHERE lock_token = ?token AND locked_until > now();
    ///     
    ///     if result.rows_affected == 0:
    ///         // Entry doesn't exist (cancelled), lock expired, or invalid token
    ///         return Err(ProviderError::permanent("Lock token invalid, expired, or entry removed"))
    ///     
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Concurrency
    ///
    /// - Safe to call while activity is executing
    /// - Idempotent: Multiple renewals with same token are safe
    /// - Renewal fails gracefully after ack_work_item (token deleted)
    ///
    /// # Usage in Runtime
    ///
    /// Called by worker dispatcher's activity manager task:
    /// - Renewal interval calculated based on lock timeout
    /// - On error: triggers cancellation token (entry may have been removed/invalidated for cancellation)
    /// - Stops automatically when activity completes or worker crashes
    async fn renew_work_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError>;

    /// Heartbeat all non-idle sessions owned by the given workers.
    ///
    /// Extends `locked_until` for sessions where:
    /// - `owner_id` matches any of the provided `owner_ids`
    /// - `locked_until > now` (still owned)
    /// - `last_activity_at + idle_timeout > now` (not idle)
    ///
    /// Sessions that are idle (no recent activity flow) are NOT renewed,
    /// causing their locks to naturally expire.
    ///
    /// Accepts a slice of owner IDs so the provider can batch the operation
    /// into a single storage call.
    ///
    /// # Returns
    ///
    /// Total count of sessions renewed across all owners.
    async fn renew_session_lock(
        &self,
        owner_ids: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError>;

    /// Sweep orphaned session entries.
    ///
    /// This is a convenience hook so providers don't need background threads for
    /// a common cleanup scenario. The runtime calls it periodically on behalf of
    /// all workers.
    ///
    /// Removes sessions where:
    /// - `locked_until < now` (lock expired)
    /// - No pending work items reference this session
    ///
    /// Any worker can sweep any worker's orphans.
    ///
    /// Providers that already clean up expired/ownerless sessions internally
    /// (e.g., via TTL, background sweeps, or eager eviction on fetch) may
    /// return `Ok(0)` here.
    ///
    /// # Returns
    ///
    /// Count of sessions removed (0 is valid if no orphans exist or the
    /// provider handles cleanup through other means).
    async fn cleanup_orphaned_sessions(&self, idle_timeout: Duration) -> Result<usize, ProviderError>;

    /// Abandon work item processing (release lock without completing).
    ///
    /// Called when activity processing fails with a retryable error (e.g., storage contention)
    /// and the work item should be made available for another worker to pick up.
    ///
    /// # What This Does
    ///
    /// 1. **Clear lock** - Remove lock_token and reset locked_until
    /// 2. **Optionally delay** - Set visible_at to defer retry
    /// 3. **Preserve message** - Don't delete or modify the work item content
    ///
    /// # Parameters
    ///
    /// * `token` - Lock token from fetch_work_item
    /// * `delay` - Optional delay before message becomes visible again
    ///   - `None`: Immediate visibility (visible_at = now)
    ///   - `Some(duration)`: Delayed visibility (visible_at = now + duration)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Work item abandoned, lock released
    /// * `Err(ProviderError)` - Abandon failed (invalid token, already acked, etc.)
    ///
    /// # Implementation Pattern
    ///
    /// ```ignore
    /// async fn abandon_work_item(&self, token: &str, delay: Option<Duration>) -> Result<(), ProviderError> {
    ///     let visible_at = delay.map(|d| now() + d).unwrap_or(now());
    ///     
    ///     let result = UPDATE worker_queue
    ///                  SET lock_token = NULL, locked_until = NULL, visible_at = ?visible_at
    ///                  WHERE lock_token = ?token;
    ///     
    ///     if result.rows_affected == 0:
    ///         return Err(ProviderError::permanent("Invalid lock token"))
    ///     
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Parameters
    ///
    /// * `token` - Lock token from fetch_work_item
    /// * `delay` - Optional delay before work item becomes visible again
    /// * `ignore_attempt` - If true, decrement the attempt count (never below 0)
    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError>;

    /// Renew the lock on an orchestration item.
    ///
    /// Extends the instance lock timeout, preventing lock expiration during
    /// long-running orchestration turns.
    ///
    /// # Parameters
    ///
    /// * `token` - Lock token from fetch_orchestration_item
    /// * `extend_for` - Duration to extend lock from now
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Lock renewed successfully
    /// * `Err(ProviderError)` - Lock renewal failed (invalid token, expired, etc.)
    ///
    /// # Implementation Pattern
    ///
    /// ```ignore
    /// async fn renew_orchestration_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
    ///     let locked_until = now() + extend_for;
    ///     
    ///     let result = UPDATE instance_locks
    ///                  SET locked_until = ?locked_until
    ///                  WHERE lock_token = ?token
    ///                    AND locked_until > now();
    ///     
    ///     if result.rows_affected == 0:
    ///         return Err(ProviderError::permanent("Lock token invalid or expired"))
    ///     
    ///     Ok(())
    /// }
    /// ```
    async fn renew_orchestration_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError>;

    // ===== Optional Management APIs =====
    // These have default implementations and are primarily used for testing/debugging.

    /// Enqueue a work item to the orchestrator queue.
    ///
    /// # Purpose
    ///
    /// Used by runtime for:
    /// - External events: `Client.raise_event()` → enqueues WorkItem::ExternalRaised
    /// - Cancellation: `Client.cancel_instance()` → enqueues WorkItem::CancelInstance
    /// - Testing: Direct queue manipulation
    ///
    /// **Note:** In normal operation, orchestrator items are enqueued via `ack_orchestration_item`.
    ///
    /// # Parameters
    ///
    /// * `item` - WorkItem to enqueue (usually ExternalRaised or CancelInstance)
    /// * `delay` - Optional visibility delay
    ///   - `None`: Immediate visibility (common case)
    ///   - `Some(duration)`: Delay visibility by the specified duration
    ///   - Providers without delayed_visibility support can ignore this (treat as None)
    ///
    /// # Implementation Pattern
    ///
    /// ```ignore
    /// async fn enqueue_for_orchestrator(&self, item: WorkItem, delay: Option<Duration>) -> Result<(), String> {
    ///     let instance = extract_instance(&item);  // See WorkItem docs for extraction
    ///     let work_json = serde_json::to_string(&item)?;
    ///     
    ///     let visible_at = if let Some(delay) = delay {
    ///         now() + delay
    ///     } else {
    ///         now()
    ///     };
    ///     
    ///     // ⚠️ DO NOT create instance here - runtime will create it via ack_orchestration_item metadata
    ///     // Instance creation happens when runtime acknowledges the first turn with ExecutionMetadata
    ///     
    ///     INSERT INTO orchestrator_queue (instance_id, work_item, visible_at, lock_token, locked_until)
    ///     VALUES (instance, work_json, visible_at, NULL, NULL);
    ///     
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Special Cases
    ///
    /// **StartOrchestration:**
    /// - ⚠️ DO NOT create instance here - instance creation happens via `ack_orchestration_item()` metadata
    /// - Just enqueue the work item; runtime will create instance when acknowledging the first turn
    ///
    /// **ExternalRaised:**
    /// - Can arrive before instance is started (race condition)
    /// - Should still enqueue - runtime will handle gracefully
    ///
    /// # Error Handling
    ///
    /// Return Err if storage fails. Return Ok if item was enqueued successfully.
    async fn enqueue_for_orchestrator(&self, _item: WorkItem, _delay: Option<Duration>) -> Result<(), ProviderError>;

    // - Add timeout parameter to fetch_work_item and dequeue_timer_peek_lock
    // - Add refresh_worker_lock(token, extend_ms) and refresh_timer_lock(token, extend_ms)
    // - Provider should auto-abandon messages if lock expires without ack
    // This would enable graceful handling of worker crashes and long-running activities

    // ===== Capability Discovery =====

    /// Check if this provider implements management capabilities.
    ///
    /// # Purpose
    ///
    /// This method enables automatic capability discovery by the `Client`.
    /// When a provider implements `ProviderAdmin`, it should return
    /// `Some(self as &dyn ProviderAdmin)` to expose management features.
    ///
    /// # Default Implementation
    ///
    /// Returns `None` (no management capabilities). Override to expose capabilities:
    ///
    /// ```ignore
    /// impl Provider for MyProvider {
    ///     fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
    ///         Some(self as &dyn ProviderAdmin)
    ///     }
    /// }
    /// ```
    ///
    /// # Usage
    ///
    /// The `Client` automatically discovers capabilities:
    ///
    /// ```ignore
    /// let client = Client::new(provider);
    /// if client.has_management_capability() {
    ///     let instances = client.list_all_instances().await?;
    /// }
    /// ```
    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        None
    }

    /// Lightweight check for custom status changes.
    ///
    /// Returns `Some((custom_status, version))` if `custom_status_version > last_seen_version`,
    /// `None` if unchanged. Used by `Client::wait_for_status_change()` polling loop.
    ///
    /// # Parameters
    ///
    /// * `instance` - Instance ID to check
    /// * `last_seen_version` - The version the caller last observed
    ///
    /// # Returns
    ///
    /// * `Ok(Some((custom_status, version)))` - Status has changed
    /// * `Ok(None)` - No change since `last_seen_version`
    /// * `Err(ProviderError)` - Storage error
    async fn get_custom_status(
        &self,
        _instance: &str,
        _last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError>;

    /// Read a single KV entry for the given instance.
    ///
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` otherwise.
    /// Reads directly from the materialized `kv_store` table (not history events).
    async fn get_kv_value(&self, instance: &str, key: &str) -> Result<Option<String>, ProviderError>;
}

/// Management and observability provider interface.
pub mod management;

pub mod instrumented;

/// SQLite-backed provider with full transactional support.
///
/// Enable with the `sqlite` feature:
/// ```toml
/// duroxide = { version = "0.1", features = ["sqlite"] }
/// ```
#[cfg(feature = "sqlite")]
pub mod sqlite;

// Re-export management types for convenience
pub use management::{
    DeleteInstanceResult, ExecutionInfo, InstanceFilter, InstanceInfo, InstanceTree, ManagementProvider, PruneOptions,
    PruneResult, QueueDepths, SystemMetrics,
};

/// Administrative capability trait for observability and management operations.
///
/// This trait provides rich management and observability features that extend
/// the core `Provider` functionality. Providers can implement this trait to
/// expose administrative capabilities to the `Client`.
///
/// # Automatic Discovery
///
/// The `Client` automatically discovers this capability through the
/// `Provider::as_management_capability()` method. When available, management
/// methods become accessible through the client.
///
/// # Implementation Guide for LLMs
///
/// When implementing a new provider, you can optionally implement this trait
/// to expose management features:
///
/// ```ignore
/// impl Provider for MyProvider {
///     // ... implement required Provider methods
///     
///     fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
///         Some(self as &dyn ProviderAdmin)
///     }
/// }
///
/// impl ProviderAdmin for MyProvider {
///     async fn list_instances(&self) -> Result<Vec<String>, String> {
///         // Query your storage for all instance IDs
///         Ok(vec!["instance-1".to_string(), "instance-2".to_string()])
///     }
///     
///     async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, String> {
///         // Query instance metadata from your storage
///         Ok(InstanceInfo {
///             instance_id: instance.to_string(),
///             orchestration_name: "ProcessOrder".to_string(),
///             orchestration_version: "1.0.0".to_string(),
///             current_execution_id: 1,
///             status: "Running".to_string(),
///             output: None,
///             created_at: 1234567890,
///             updated_at: 1234567890,
///         })
///     }
///     
///     // ... implement other management methods
/// }
/// ```
///
/// # Required Methods (8 total)
///
/// 1. **Instance Discovery (2 methods)**
///    - `list_instances()` - List all instance IDs
///    - `list_instances_by_status()` - Filter instances by status
///
/// 2. **Execution Inspection (3 methods)**
///    - `list_executions()` - List execution IDs for an instance
///    - `read_execution()` - Read history for a specific execution
///    - `latest_execution_id()` - Get the latest execution ID
///
/// 3. **Metadata Access (2 methods)**
///    - `get_instance_info()` - Get comprehensive instance information
///    - `get_execution_info()` - Get detailed execution information
///
/// 4. **System Metrics (2 methods)**
///    - `get_system_metrics()` - Get system-wide metrics
///    - `get_queue_depths()` - Get current queue depths
///
/// # Usage
///
/// ```ignore
/// let client = Client::new(provider);
///
/// // Check if management features are available
/// if client.has_management_capability() {
///     let instances = client.list_all_instances().await?;
///     let metrics = client.get_system_metrics().await?;
///     println!("System has {} instances", metrics.total_instances);
/// } else {
///     println!("Management features not available");
/// }
/// ```
#[async_trait::async_trait]
pub trait ProviderAdmin: Any + Send + Sync {
    // ===== Instance Discovery =====

    /// List all known instance IDs.
    ///
    /// # Returns
    ///
    /// Vector of instance IDs, typically sorted by creation time (newest first).
    ///
    /// # Use Cases
    ///
    /// - Admin dashboards showing all workflows
    /// - Bulk operations across instances
    /// - Testing (verify instance creation)
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn list_instances(&self) -> Result<Vec<String>, String> {
    ///     SELECT instance_id FROM instances ORDER BY created_at DESC
    /// }
    /// ```
    async fn list_instances(&self) -> Result<Vec<String>, ProviderError>;

    /// List instances matching a status filter.
    ///
    /// # Parameters
    ///
    /// * `status` - Filter by execution status: "Running", "Completed", "Failed", "ContinuedAsNew"
    ///
    /// # Returns
    ///
    /// Vector of instance IDs with the specified status.
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn list_instances_by_status(&self, status: &str) -> Result<Vec<String>, String> {
    ///     SELECT i.instance_id FROM instances i
    ///     JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
    ///     WHERE e.status = ?
    ///     ORDER BY i.created_at DESC
    /// }
    /// ```
    async fn list_instances_by_status(&self, status: &str) -> Result<Vec<String>, ProviderError>;

    // ===== Execution Inspection =====

    /// List all execution IDs for an instance.
    ///
    /// # Returns
    ///
    /// Vector of execution IDs in ascending order: `[1]`, `[1, 2]`, `[1, 2, 3]`, etc.
    ///
    /// # Multi-Execution Context
    ///
    /// When an orchestration uses ContinueAsNew, multiple executions exist:
    /// - Execution 1: Original execution
    /// - Execution 2: First ContinueAsNew
    /// - Execution 3: Second ContinueAsNew
    /// - etc.
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn list_executions(&self, instance: &str) -> Result<Vec<u64>, String> {
    ///     SELECT execution_id FROM executions
    ///     WHERE instance_id = ?
    ///     ORDER BY execution_id
    /// }
    /// ```
    async fn list_executions(&self, instance: &str) -> Result<Vec<u64>, ProviderError>;

    /// Read the full event history for a specific execution within an instance.
    ///
    /// # Parameters
    ///
    /// * `instance` - The ID of the orchestration instance.
    /// * `execution_id` - The specific execution ID to read history for.
    ///
    /// # Returns
    ///
    /// Vector of events in chronological order (oldest first).
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn read_execution(&self, instance: &str, execution_id: u64) -> Result<Vec<Event>, String> {
    ///     SELECT event_data FROM history
    ///     WHERE instance_id = ? AND execution_id = ?
    ///     ORDER BY event_id
    /// }
    /// ```
    async fn read_history_with_execution_id(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError>;

    /// Read the full event history for the latest execution of an instance.
    ///
    /// # Parameters
    ///
    /// * `instance` - The ID of the orchestration instance.
    ///
    /// # Returns
    ///
    /// Vector of events in chronological order (oldest first) for the latest execution.
    ///
    /// # Implementation
    ///
    /// This method gets the latest execution ID and delegates to `read_history_with_execution_id`.
    ///
    /// ```ignore
    /// async fn read_history(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
    ///     let execution_id = self.latest_execution_id(instance).await?;
    ///     self.read_history_with_execution_id(instance, execution_id).await
    /// }
    /// ```
    async fn read_history(&self, instance: &str) -> Result<Vec<Event>, ProviderError>;

    /// Get the latest (current) execution ID for an instance.
    ///
    /// # Parameters
    ///
    /// * `instance` - The ID of the orchestration instance.
    ///
    /// # Implementation Pattern
    ///
    /// ```ignore
    /// async fn latest_execution_id(&self, instance: &str) -> Result<u64, String> {
    ///     SELECT COALESCE(MAX(execution_id), 1) FROM executions WHERE instance_id = ?
    /// }
    /// ```
    async fn latest_execution_id(&self, instance: &str) -> Result<u64, ProviderError>;

    // ===== Instance Metadata =====

    /// Get comprehensive information about an instance.
    ///
    /// # Parameters
    ///
    /// * `instance` - The ID of the orchestration instance.
    ///
    /// # Returns
    ///
    /// Detailed instance information including status, output, and metadata.
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, String> {
    ///     SELECT i.*, e.status, e.output
    ///     FROM instances i
    ///     JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
    ///     WHERE i.instance_id = ?
    /// }
    /// ```
    async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, ProviderError>;

    /// Get detailed information about a specific execution.
    ///
    /// # Parameters
    ///
    /// * `instance` - The ID of the orchestration instance.
    /// * `execution_id` - The specific execution ID.
    ///
    /// # Returns
    ///
    /// Detailed execution information including status, output, and event count.
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn get_execution_info(&self, instance: &str, execution_id: u64) -> Result<ExecutionInfo, String> {
    ///     SELECT e.*, COUNT(h.event_id) as event_count
    ///     FROM executions e
    ///     LEFT JOIN history h ON e.instance_id = h.instance_id AND e.execution_id = h.execution_id
    ///     WHERE e.instance_id = ? AND e.execution_id = ?
    ///     GROUP BY e.execution_id
    /// }
    /// ```
    async fn get_execution_info(&self, instance: &str, execution_id: u64) -> Result<ExecutionInfo, ProviderError>;

    // ===== System Metrics =====

    /// Get system-wide metrics for the orchestration engine.
    ///
    /// # Returns
    ///
    /// System metrics including instance counts, execution counts, and status breakdown.
    ///
    /// # Implementation Example
    ///
    /// ```text
    /// async fn get_system_metrics(&self) -> Result<SystemMetrics, String> {
    ///     SELECT
    ///         COUNT(*) as total_instances,
    ///         SUM(CASE WHEN e.status = 'Running' THEN 1 ELSE 0 END) as running_instances,
    ///         SUM(CASE WHEN e.status = 'Completed' THEN 1 ELSE 0 END) as completed_instances,
    ///         SUM(CASE WHEN e.status = 'Failed' THEN 1 ELSE 0 END) as failed_instances
    ///     FROM instances i
    ///     JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
    /// }
    /// ```
    async fn get_system_metrics(&self) -> Result<SystemMetrics, ProviderError>;

    /// Get the current depths of the internal work queues.
    ///
    /// # Returns
    ///
    /// Queue depths for orchestrator and worker queues.
    ///
    /// **Note:** Timer queue depth is not applicable since timers are handled via
    /// delayed visibility in the orchestrator queue.
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn get_queue_depths(&self) -> Result<QueueDepths, String> {
    ///     SELECT
    ///         (SELECT COUNT(*) FROM orchestrator_queue WHERE lock_token IS NULL) as orchestrator_queue,
    ///         (SELECT COUNT(*) FROM worker_queue WHERE lock_token IS NULL) as worker_queue
    /// }
    /// ```
    async fn get_queue_depths(&self) -> Result<QueueDepths, ProviderError>;

    // ===== Hierarchy Primitive Operations =====
    // These are simple database operations that providers MUST implement.
    // Composite operations like get_instance_tree and delete_instance have
    // default implementations that use these primitives.

    /// List direct children of an instance.
    ///
    /// Returns instance IDs that have `parent_instance_id = instance_id`.
    /// Returns empty vec if instance has no children or doesn't exist.
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn list_children(&self, instance_id: &str) -> Result<Vec<String>, ProviderError> {
    ///     SELECT instance_id FROM instances WHERE parent_instance_id = ?
    /// }
    /// ```
    async fn list_children(&self, instance_id: &str) -> Result<Vec<String>, ProviderError>;

    /// Get the parent instance ID.
    ///
    /// Returns `Some(parent_id)` for sub-orchestrations, `None` for root orchestrations.
    /// Returns `Err` if instance doesn't exist.
    ///
    /// # Implementation Example
    ///
    /// ```ignore
    /// async fn get_parent_id(&self, instance_id: &str) -> Result<Option<String>, ProviderError> {
    ///     SELECT parent_instance_id FROM instances WHERE instance_id = ?
    /// }
    /// ```
    async fn get_parent_id(&self, instance_id: &str) -> Result<Option<String>, ProviderError>;

    /// Atomically delete a batch of instances.
    ///
    /// # Parameters
    ///
    /// * `ids` - Instance IDs to delete.
    /// * `force` - If true, delete regardless of status. If false, all instances must be terminal.
    ///
    /// # Provider Contract (MUST implement)
    ///
    /// 1. **Atomicity**: All deletions MUST be atomic (all-or-nothing).
    ///    If any instance fails validation, the entire batch MUST be rolled back.
    ///
    /// 2. **Force semantics**:
    ///    - `force=false`: Return error if ANY instance has `status = 'Running'`
    ///    - `force=true`: Delete regardless of status (for stuck/abandoned instances)
    ///
    /// 3. **Orphan detection**: Atomically check for children not in `ids`:
    ///    ```sql
    ///    SELECT instance_id FROM instances
    ///    WHERE parent_instance_id IN (?) AND instance_id NOT IN (?)
    ///    ```
    ///    If any exist, return error (race condition: new child spawned after get_instance_tree).
    ///
    /// 4. **Complete cleanup**: Remove ALL related data in a single atomic operation:
    ///    - event history
    ///    - executions
    ///    - orchestrator queue entries
    ///    - worker queue entries
    ///    - instance locks
    ///    - instance metadata
    ///
    /// 5. **Prevent ack recreation**: When force-deleting Running instances, the removal
    ///    of the instance lock prevents in-flight `ack_orchestration_item` from recreating state.
    ///
    /// # Race Condition Protection
    ///
    /// This method MUST validate within the transaction that no orphans would be created:
    /// - If any instance has `parent_instance_id` pointing to an instance in `ids`,
    ///   but that child is NOT also in `ids`, return an error.
    /// - This prevents orphans from children spawned between `get_instance_tree()` and
    ///   this call.
    ///
    /// # Transaction Semantics
    ///
    /// All instances must be deleted atomically (all-or-nothing).
    /// The orphan check must be done within the transaction to prevent TOCTOU races.
    ///
    /// # Implementation Notes
    ///
    /// - Delete all related data: event history, executions, orchestrator queue, worker queue, instance locks, instance metadata
    /// - Order within transaction doesn't matter for correctness (single atomic transaction)
    /// - Count deletions and aggregate into result
    async fn delete_instances_atomic(&self, ids: &[String], force: bool)
    -> Result<DeleteInstanceResult, ProviderError>;

    // ===== Hierarchy Composite Operations =====
    // These have default implementations using the primitives above.
    // Providers can override for better performance if needed.

    /// Get the full instance tree rooted at the given instance.
    ///
    /// Returns all instances in the tree: the root, all children, grandchildren, etc.
    ///
    /// Default implementation uses `list_children` recursively.
    async fn get_instance_tree(&self, instance_id: &str) -> Result<InstanceTree, ProviderError> {
        let mut all_ids = vec![];

        // BFS to collect all descendants
        let mut to_process = vec![instance_id.to_string()];
        while let Some(parent_id) = to_process.pop() {
            all_ids.push(parent_id.clone());
            let children = self.list_children(&parent_id).await?;
            to_process.extend(children);
        }

        Ok(InstanceTree {
            root_id: instance_id.to_string(),
            all_ids,
        })
    }

    // ===== Deletion/Pruning Operations =====

    /// Delete a single orchestration instance and all its associated data.
    ///
    /// This removes the instance, all executions, all history events, and any
    /// pending queue messages (orchestrator, worker, timer).
    ///
    /// # Default Implementation
    ///
    /// Uses primitives: `get_parent_id`, `get_instance_tree`, `delete_instances_atomic`.
    /// Providers can override for better performance if needed.
    ///
    /// # Parameters
    ///
    /// * `instance_id` - The ID of the instance to delete.
    /// * `force` - If true, delete even if the instance is in Running state.
    ///   WARNING: Force delete only removes stored state; it does NOT cancel
    ///   in-flight tokio tasks. Use `cancel_instance` first for graceful termination.
    ///
    /// # Returns
    ///
    /// * `Ok(DeleteResult)` - Details of what was deleted.
    /// * `Err(ProviderError)` with `is_retryable() = false` for:
    ///   - Instance is running and force=false
    ///   - Instance is a sub-orchestration (must delete root instead)
    ///   - Instance not found
    ///
    /// # Safety
    ///
    /// - Deleting a running instance with force=true may cause in-flight operations
    ///   to fail when they try to persist state.
    /// - Sub-orchestrations cannot be deleted directly; delete the root to cascade.
    /// - The instance lock entry is removed to prevent zombie recreation.
    async fn delete_instance(&self, instance_id: &str, force: bool) -> Result<DeleteInstanceResult, ProviderError> {
        // Step 1: Check if this is a sub-orchestration
        let parent = self.get_parent_id(instance_id).await?;
        if parent.is_some() {
            return Err(ProviderError::permanent(
                "delete_instance",
                format!("Cannot delete sub-orchestration {instance_id} directly. Delete root instance instead."),
            ));
        }

        // Step 2: Get full tree (includes all descendants)
        let tree = self.get_instance_tree(instance_id).await?;

        // Step 3: Atomic delete of entire tree
        self.delete_instances_atomic(&tree.all_ids, force).await
    }

    /// Delete multiple orchestration instances matching the filter criteria.
    ///
    /// Only instances in terminal states (Completed, Failed) are eligible.
    /// Running instances are silently skipped (not an error).
    ///
    /// # Parameters
    ///
    /// * `filter` - Criteria for selecting instances to delete. All criteria are ANDed.
    ///
    /// # Filter Behavior
    ///
    /// - `instance_ids`: Allowlist of specific IDs to consider
    /// - `completed_before`: Only delete instances completed before this timestamp (ms)
    /// - `limit`: Maximum number of instances to delete (applied last)
    ///
    /// # Returns
    ///
    /// Aggregated counts of all deleted data across all deleted instances.
    ///
    /// # Safety
    ///
    /// - Running instances are NEVER deleted (silently skipped)
    /// - Use `limit` to avoid long-running transactions
    /// - Sub-orchestrations are skipped (only roots are deleted with cascade)
    async fn delete_instance_bulk(&self, filter: InstanceFilter) -> Result<DeleteInstanceResult, ProviderError>;

    /// Prune old executions from a single instance.
    ///
    /// Use this for `ContinueAsNew` workflows that accumulate many executions.
    ///
    /// # Parameters
    ///
    /// * `instance_id` - The instance to prune executions from.
    /// * `options` - Criteria for selecting executions to delete. All criteria are ANDed.
    ///
    /// # Provider Contract (MUST implement)
    ///
    /// 1. **Current execution protection**: The current execution of the instance
    ///    MUST NEVER be pruned, regardless of options.
    ///
    /// 2. **Running execution protection**: Executions with status 'Running'
    ///    MUST NEVER be pruned.
    ///
    /// 3. **Atomicity**: All deletions for a single prune call MUST be atomic
    ///    (all-or-nothing).
    ///
    /// # `keep_last` Semantics
    ///
    /// Since current_execution_id is always the highest execution_id:
    /// - `keep_last: None` → prune all except current
    /// - `keep_last: Some(0)` → same as None (current is protected)
    /// - `keep_last: Some(1)` → same as above (top 1 = current)
    /// - `keep_last: Some(N)` → keep current + (N-1) most recent
    ///
    /// **All three (`None`, `Some(0)`, `Some(1)`) are equivalent** because the
    /// current execution is always protected regardless of this setting.
    ///
    /// # Implementation Example
    ///
    /// ```sql
    /// DELETE FROM executions
    /// WHERE instance_id = ?
    ///   AND execution_id != ?  -- current_execution_id (CRITICAL)
    ///   AND status != 'Running'
    ///   AND execution_id NOT IN (
    ///     SELECT execution_id FROM executions
    ///     WHERE instance_id = ?
    ///     ORDER BY execution_id DESC
    ///     LIMIT ?  -- keep_last
    ///   )
    /// ```
    async fn prune_executions(&self, instance_id: &str, options: PruneOptions) -> Result<PruneResult, ProviderError>;

    /// Prune old executions from multiple instances matching the filter.
    ///
    /// Applies the same prune options to all matching instances.
    ///
    /// # Parameters
    ///
    /// * `filter` - Criteria for selecting instances to process.
    /// * `options` - Criteria for selecting executions to delete within each instance.
    ///
    /// # Returns
    ///
    /// Aggregated counts including how many instances were processed.
    async fn prune_executions_bulk(
        &self,
        filter: InstanceFilter,
        options: PruneOptions,
    ) -> Result<PruneResult, ProviderError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_only_matches_none_rejects_tag() {
        let f = TagFilter::DefaultOnly;
        assert!(f.matches(None));
        assert!(!f.matches(Some("gpu")));
    }

    #[test]
    fn tags_matches_member_rejects_others() {
        let f = TagFilter::tags(["gpu"]);
        assert!(f.matches(Some("gpu")));
        assert!(!f.matches(Some("cpu")));
        assert!(!f.matches(None));
    }

    #[test]
    fn default_and_matches_both() {
        let f = TagFilter::default_and(["gpu"]);
        assert!(f.matches(None));
        assert!(f.matches(Some("gpu")));
        assert!(!f.matches(Some("cpu")));
    }

    #[test]
    fn none_matches_nothing() {
        let f = TagFilter::None;
        assert!(!f.matches(None));
        assert!(!f.matches(Some("x")));
    }

    #[test]
    fn any_matches_everything() {
        let f = TagFilter::Any;
        assert!(f.matches(None));
        assert!(f.matches(Some("gpu")));
        assert!(f.matches(Some("cpu")));
        assert!(f.matches(Some("anything")));
    }

    #[test]
    fn default_impl_is_default_only() {
        assert_eq!(TagFilter::default(), TagFilter::DefaultOnly);
    }

    #[test]
    #[should_panic(expected = "at most 5 tags")]
    fn panics_over_max_tags() {
        TagFilter::tags(["a", "b", "c", "d", "e", "f"]);
    }

    #[test]
    #[should_panic(expected = "requires at least one tag")]
    fn panics_on_empty_tags() {
        TagFilter::tags(Vec::<String>::new());
    }

    #[test]
    #[should_panic(expected = "requires at least one tag")]
    fn panics_on_empty_default_and() {
        TagFilter::default_and(Vec::<String>::new());
    }
}

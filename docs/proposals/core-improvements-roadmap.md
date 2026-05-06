# Core Duroxide Improvements Roadmap

This document captures needed improvements to the core duroxide framework. Each item represents a significant enhancement to reliability, performance, or developer experience.

## Summary

| # | Improvement | Issue | Description |
|---|-------------|-------|-------------|
| 1 | [Pub/Sub for RaiseEvent](#1-pubsub-for-raiseevent) | [#8](https://github.com/microsoft/duroxide/issues/8) | MQTT/Redis-style topic subscriptions for broadcasting events to multiple orchestrations |
| 2 | [Dispatcher Improvements](#2-dispatcher-improvements) | [#9](https://github.com/microsoft/duroxide/issues/9) | Spin off executions, prefetch from queue, sticky message routing via mpsc channels |
| 3 | [Management API Improvements](#3-management-api-improvements) | [#10](https://github.com/microsoft/duroxide/issues/10) | Enriched list responses, truncate APIs, pagination, and performance fixes |
| 4 | [General Performance Work](#4-general-performance-work) | [#11](https://github.com/microsoft/duroxide/issues/11) | Reduce redundant history scans, replay optimizations, serialization overhead |
| 5 | [Macro Support](#5-macro-support) | [#12](https://github.com/microsoft/duroxide/issues/12) | `register_activity!()`, `call_durable!()` macros to eliminate boilerplate |
| 6 | [Poison Message Detection](#6-poison-message-and-instance-detection) | [#13](https://github.com/microsoft/duroxide/issues/13) | Quarantine instances after N consecutive failures with configurable policy |
| 7 | [Registry Discovery API](#7-registry-discovery-api) | [#14](https://github.com/microsoft/duroxide/issues/14) | Query registered orchestrations/activities with metadata and descriptions |
| 8 | [Event Size Limits](#8-event-size-limits) | [#15](https://github.com/microsoft/duroxide/issues/15) | Configurable limits on payload sizes to prevent memory/storage issues |
| 9 | [Runtime-Provider Contract Versioning](#9-runtime-provider-contract-versioning) | [#16](https://github.com/microsoft/duroxide/issues/16) | Version and capability negotiation between runtime and provider |
| 10 | [Managed Long-Running Activities](#10-managed-long-running-activities) | [#17](https://github.com/microsoft/duroxide/issues/17) | Bidirectional communication for progress updates and control signals |

---

## 1. Pub/Sub for RaiseEvent

### Problem

Currently, `raise_event` targets a single orchestration instance by instance ID. There's no mechanism for broadcasting events to multiple orchestrations that share a common interest.

### Proposed Solution

Implement MQTT/Redis-style topic subscriptions. Orchestrations subscribe to topic patterns, and clients can publish events to topics that fan out to all matching subscribers.

**Topic Pattern Examples:**
- `provisioning/#` — matches `provisioning/start`, `provisioning/vm/create`, etc.
- `region/+/feature-flags` — matches `region/us-east/feature-flags`, `region/eu-west/feature-flags`
- `alerts/critical` — exact match

**Use Cases:**
- **"Stop all provisioning operations"** — publish to `provisioning/stop`, all provisioning orchestrations subscribed to `provisioning/#` receive the signal
- **"Enable feature switch for a region"** — publish to `region/us-east/feature-flags`, all orchestrations in that region react
- **Global circuit breakers** — publish to `system/circuit-breaker/open`, all orchestrations pause external calls

**API Sketch:**
```rust
// Orchestration subscribes to a topic pattern
let event = ctx.subscribe_topic("provisioning/#").await;

// Client publishes to a topic (fans out to all subscribers)
client.publish_topic("provisioning/stop", payload).await?;
```

**Implementation Considerations:**
- Topic subscriptions stored in provider with pattern matching
- Provider needs efficient topic → subscriber lookup (trie or regex-based)
- History event: `TopicSubscribed { pattern }` and `TopicEventReceived { topic, data }`
- Must handle subscription before publish (late subscribers don't receive past events)

---

## 2. Dispatcher Improvements

### Problem

Current dispatcher model blocks on orchestration execution. While executing, new messages for the same or other orchestrations queue up. This creates head-of-line blocking and limits throughput.

### Proposed Solution

Dispatchers should:
1. **Spin off executions** — dispatch work to a task/thread pool and immediately return to polling the queue
2. **Prefetch messages** — while executions run, continue pulling messages from the queue
3. **Sticky message routing** — if more messages arrive for an already-running orchestration, route them to that execution via an mpsc channel instead of creating a new execution context

**Architecture:**
```
Queue → [Dispatcher] → mpsc channels → [Execution Tasks]
              ↑                              |
              └──────── prefetch ────────────┘
```

**Benefits:**
- Higher throughput — dispatchers don't block on slow orchestrations
- Better resource utilization — prefetching reduces queue round-trips
- Reduced context switching — sticky routing keeps orchestration state hot
- Enables back-pressure — mpsc channels can buffer or reject when overloaded

**Implementation Considerations:**
- Dispatcher maintains `HashMap<InstanceId, mpsc::Sender<WorkItem>>`
- When execution completes, sender is dropped and channel closes
- Configurable prefetch depth and channel buffer sizes
- Need to handle execution panics cleanly (remove from map, allow re-dispatch)

---

## 3. Management API Improvements

### Problem

The management API has several gaps:
- `list_instances` returns minimal info (just IDs), requiring N+1 queries for details
- No API to truncate instance data (cleanup after testing/development)
- No API to truncate execution history (critical for eternal orchestrations)
- `list_instances` is slow on large datasets (lacks pagination, filtering optimizations)

### Proposed Solution

**3.1 Enriched List Responses:**

```rust
pub struct InstanceInfo {
    pub instance_id: String,
    pub orchestration_name: String,
    pub orchestration_version: String,
    pub status: OrchestrationStatus,
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
    pub current_execution_id: u64,
    pub execution_count: u64,
}

// Returns basic info for all instances without needing separate queries
fn list_instances_with_info(&self) -> Result<Vec<InstanceInfo>>;
```

**3.2 Truncate APIs:**

```rust
// Delete instance and all its executions/history
fn truncate_instance(&self, instance_id: &str) -> Result<()>;

// Delete old executions, keeping only the most recent N
fn truncate_executions(
    &self,
    instance_id: &str,
    keep_last_n: u64,
) -> Result<u64>; // Returns number deleted

// Automatic policy for eternal orchestrations
pub struct ExecutionRetentionPolicy {
    pub keep_last_n: u64,           // e.g., 5
    pub keep_duration: Duration,     // e.g., 7 days
}
```

**3.3 Performance Improvements:**

- Add pagination: `list_instances_paginated(cursor, limit)`
- Add filtering: `list_instances_filtered(status, name_prefix, created_after)`
- Add indexes on `(orchestration_name, status)` and `(created_at)`
- Use covering indexes where possible to avoid table lookups
- Consider materialized views for dashboard queries

---

## 4. General Performance Work

### Problem

History scanning is performed redundantly in several code paths:
- Replay engine scans full history on every turn
- Status checks scan history to determine current state
- Multiple functions iterate over history looking for specific events

### Areas to Address

**4.1 Redundant History Scans:**
- Profile and identify hot paths with repeated scans
- Cache computed state (e.g., pending activities, last event ID)
- Use indexes/hashmaps for event lookups instead of linear scans

**4.2 Replay Engine Optimizations:**
- Start replay from checkpoint instead of beginning
- Skip already-processed events on subsequent turns
- Batch event fetching instead of per-event queries

**4.3 Serialization Overhead:**
- Evaluate faster JSON libraries (simd-json, sonic-rs)
- Consider binary format for internal storage (MessagePack, bincode)
- Cache serialized forms where possible

**4.4 Database Access Patterns:**
- Batch inserts for multiple events
- Use prepared statements consistently
- Connection pooling tuning
- Analyze query plans for common operations

---

## 5. Macro Support

### Problem

Current API requires verbose boilerplate for activity/orchestration registration and invocation:

```rust
// Registration boilerplate
registry.register_activity("my_activity", |ctx, input| {
    Box::pin(async move { my_activity_fn(ctx, input).await })
});

// Invocation boilerplate  
let result: MyOutput = ctx.schedule_activity_typed::<MyInput, MyOutput>(
    "my_activity",
    &my_input,
).into_activity().await?;
```

### Proposed Solution

Provide ergonomic macros that eliminate string-based naming and boilerplate:

**5.1 Registration Macro:**

```rust
// Before
registry.register_activity("process_payment", |ctx, input| {
    Box::pin(async move { process_payment(ctx, input).await })
});

// After
register_activity!(registry, process_payment);
// Automatically uses function name as activity name
// Handles async wrapping internally
```

**5.2 Invocation Macro:**

```rust
// Before
let result: PaymentResult = ctx.schedule_activity_typed::<PaymentInput, PaymentResult>(
    "process_payment",
    &payment_input,
).into_activity().await?;

// After
let result = call_durable!(ctx, process_payment, &payment_input).await?;
// Type inference from function signature
// Automatic serialization/deserialization
```

**5.3 Orchestration Registration:**

```rust
// Before
registry.register_orchestration("order_workflow", "1.0.0", |ctx| {
    Box::pin(async move { order_workflow(ctx).await })
});

// After
register_orchestration!(registry, order_workflow, "1.0.0");
```

**Implementation Considerations:**
- Procedural macros for compile-time validation
- Preserve function signature information for type checking
- Generate unique names to avoid collisions
- Support both sync and async activities
- Allow custom name override: `register_activity!(registry, my_fn, name = "custom_name")`

---

## 6. Poison Message and Instance Detection

### Problem

Some orchestrations may fail repeatedly due to:
- Corrupt input data
- Unrecoverable external dependencies
- Logic bugs triggered by specific inputs

Currently, these instances retry indefinitely (at infrastructure level), consuming resources and polluting logs without making progress.

### Proposed Solution

Implement application-level poison detection that quarantines problematic instances after N consecutive failures.

**6.1 Retry Tracking:**

```rust
pub struct InstanceRetryState {
    pub consecutive_failures: u32,
    pub last_failure_reason: String,
    pub first_failure_at: DateTime<Utc>,
    pub last_failure_at: DateTime<Utc>,
}
```

**6.2 Quarantine Behavior:**

After `max_retries` consecutive failures:
1. Instance status set to `Quarantined` (new status variant)
2. Cancel reason: `"Quarantined: exceeded {N} consecutive failures"`
3. Instance removed from work queue
4. Event logged: `InstanceQuarantined { instance_id, failure_count, last_error }`

**6.3 Configuration:**

```rust
pub struct PoisonMessagePolicy {
    pub max_consecutive_failures: u32,  // Default: 5
    pub failure_window: Duration,        // Reset counter if no failures within window
    pub quarantine_action: QuarantineAction,
}

pub enum QuarantineAction {
    Cancel,                    // Cancel with quarantine reason
    Suspend,                   // Pause but allow manual resume
    MoveToDeadLetter,          // Move to separate queue for analysis
    Callback(Box<dyn Fn>),     // Custom handling
}
```

**6.4 Recovery:**

```rust
// Manual recovery API
client.release_from_quarantine(instance_id).await?;

// Or with retry
client.retry_quarantined(instance_id).await?;
```

**Distinction from Infrastructure Retries:**
- **Infrastructure retries**: Handle transient failures (network, lock contention) — already implemented
- **Application poison detection**: Handle persistent failures that survive infrastructure retries — this proposal

---

## 7. Registry Discovery API

### Problem

Clients and management tools have no way to discover what orchestrations and activities are registered with a runtime. This makes it difficult to:
- Build generalized management UIs that show available orchestrations
- Validate inputs before starting an orchestration
- Generate documentation or API catalogs
- Provide IDE/tooling support for orchestration invocation

Additionally, there's no mechanism for developers to attach metadata (descriptions, input/output schemas, tags) to their orchestrations and activities.

### Proposed Solution

**7.1 Metadata Structs:**

```rust
pub struct OrchestrationMetadata {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub input_schema: Option<String>,    // JSON Schema
    pub output_schema: Option<String>,   // JSON Schema
    pub tags: Vec<String>,               // e.g., ["billing", "critical"]
    pub deprecated: bool,
    pub deprecation_message: Option<String>,
}

pub struct ActivityMetadata {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Option<String>,    // JSON Schema
    pub output_schema: Option<String>,   // JSON Schema
    pub tags: Vec<String>,
    pub estimated_duration: Option<Duration>,
    pub idempotent: bool,                // Hint for retry behavior
}
```

**7.2 Registration with Metadata:**

```rust
// Current registration (unchanged, metadata optional)
registry.register_orchestration("order_workflow", "1.0.0", handler);

// New: registration with metadata
registry.register_orchestration_with_metadata(
    "order_workflow",
    "1.0.0",
    handler,
    OrchestrationMetadata {
        description: Some("Processes customer orders end-to-end".into()),
        input_schema: Some(include_str!("schemas/order_input.json").into()),
        tags: vec!["orders".into(), "critical".into()],
        ..Default::default()
    },
);

// Or with builder pattern
registry.register_orchestration("order_workflow", "1.0.0", handler)
    .with_description("Processes customer orders end-to-end")
    .with_input_schema(include_str!("schemas/order_input.json"))
    .with_tags(&["orders", "critical"]);
```

**7.3 Discovery API (Provider-Level):**

The provider must persist and serve registry metadata so that management clients can query it without access to the runtime process.

```rust
// Provider trait additions
trait OrchestrationProvider {
    // Called by runtime on startup to sync registered items
    fn sync_registry(&self, orchestrations: &[OrchestrationMetadata], activities: &[ActivityMetadata]) -> Result<()>;
    
    // Query registered orchestrations
    fn list_registered_orchestrations(&self) -> Result<Vec<OrchestrationMetadata>>;
    
    // Query registered activities  
    fn list_registered_activities(&self) -> Result<Vec<ActivityMetadata>>;
    
    // Filter by tags
    fn list_orchestrations_by_tag(&self, tag: &str) -> Result<Vec<OrchestrationMetadata>>;
}
```

**7.4 Client API:**

```rust
// Client can discover what's available
let orchestrations = client.list_orchestrations().await?;
for orch in orchestrations {
    println!("{} v{}: {}", orch.name, orch.version, orch.description.unwrap_or_default());
}

let activities = client.list_activities().await?;

// Filter by tag
let billing_orchs = client.list_orchestrations_by_tag("billing").await?;
```

**7.5 Storage Schema:**

```sql
CREATE TABLE registered_orchestrations (
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    description TEXT,
    input_schema TEXT,
    output_schema TEXT,
    tags TEXT,  -- JSON array
    deprecated BOOLEAN DEFAULT FALSE,
    deprecation_message TEXT,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (name, version)
);

CREATE TABLE registered_activities (
    name TEXT PRIMARY KEY,
    description TEXT,
    input_schema TEXT,
    output_schema TEXT,
    tags TEXT,  -- JSON array
    estimated_duration_ms INTEGER,
    idempotent BOOLEAN DEFAULT FALSE,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Implementation Considerations:**
- Runtime syncs registry to provider on startup (upsert semantics)
- `last_seen_at` tracks which items are still active across restarts
- Stale entries (not seen in N days) can be flagged or cleaned up
- Metadata is optional — existing registrations continue to work
- Consider versioning the metadata schema itself for future extensions

---

## 8. Event Size Limits

> **Status: Superseded — implemented in `docs/proposals/size-limits.md` (2026-05-06).**
>
> The final design differs from the sketch below: limits are hardcoded constants
> (not configurable per-runtime), enforcement is gated by two `RuntimeOptions` toggles
> (`enforce_size_limits`, `emit_limit_exceeded_errors`), and the error type uses
> `ConfigErrorKind::LimitExceeded` rather than a custom `SizeLimitError` enum.
> See [`docs/ORCHESTRATION-GUIDE.md#size-limits`](../ORCHESTRATION-GUIDE.md#size-limits)
> for the authoritative reference.

### Problem (historical)

Currently, there's no enforcement on the size of individual events. Large payloads in activity inputs/outputs, orchestration inputs, or external events can:
- Cause memory pressure during replay (all events loaded into memory)
- Slow down serialization/deserialization
- Bloat the database and degrade query performance
- Create issues with provider-specific limits (e.g., SQLite row size, network transfer limits)

History length limits exist, but a single oversized event can still cause problems.

### Proposed Solution

**8.1 Size Limits Configuration:**

```rust
pub struct EventSizeLimits {
    /// Max size for activity input payload (bytes). Default: 256KB
    pub activity_input_max: usize,
    /// Max size for activity output payload (bytes). Default: 256KB
    pub activity_output_max: usize,
    /// Max size for orchestration input (bytes). Default: 256KB
    pub orchestration_input_max: usize,
    /// Max size for orchestration output (bytes). Default: 256KB
    pub orchestration_output_max: usize,
    /// Max size for external event data (bytes). Default: 64KB
    pub external_event_max: usize,
    /// Max size for any single event (bytes). Default: 1MB
    pub event_max: usize,
}

impl Default for EventSizeLimits {
    fn default() -> Self {
        Self {
            activity_input_max: 256 * 1024,      // 256KB
            activity_output_max: 256 * 1024,     // 256KB
            orchestration_input_max: 256 * 1024, // 256KB
            orchestration_output_max: 256 * 1024,// 256KB
            external_event_max: 64 * 1024,       // 64KB
            event_max: 1024 * 1024,              // 1MB
        }
    }
}
```

**8.2 Enforcement Points:**

| Location | Check | Action on Violation |
|----------|-------|---------------------|
| `schedule_activity()` | Input size | Return error before scheduling |
| Activity worker | Output size | Return `ActivityFailed` with size error |
| `start_orchestration()` | Input size | Reject start request |
| Orchestration completion | Output size | Fail orchestration with size error |
| `raise_event()` | Event data size | Reject event |
| Provider append | Total event size | Reject append (safety net) |

**8.3 Error Handling:**

```rust
#[derive(Debug)]
pub enum SizeLimitError {
    ActivityInputTooLarge { name: String, size: usize, limit: usize },
    ActivityOutputTooLarge { name: String, size: usize, limit: usize },
    OrchestrationInputTooLarge { size: usize, limit: usize },
    ExternalEventTooLarge { name: String, size: usize, limit: usize },
    EventTooLarge { event_type: String, size: usize, limit: usize },
}
```

**8.4 Large Payload Pattern:**

For use cases requiring large data, recommend the "claim check" pattern:
- Store large payload in external storage (S3, blob store)
- Pass reference/URL in the event
- Activity/orchestration retrieves payload when needed

```rust
// Instead of passing large data directly:
let result = ctx.schedule_activity("process", &huge_payload).await; // ❌ May exceed limit

// Use claim check pattern:
let reference = store_payload(&huge_payload).await?;
let result = ctx.schedule_activity("process", &reference).await; // ✅ Small reference
```

**Implementation Considerations:**
- Check sizes after serialization (JSON size, not in-memory size)
- Configurable per-runtime via `RuntimeOptions`
- Clear error messages indicating which limit was exceeded
- Metrics for tracking payload sizes (p50, p99, max)
- Consider compression for storage while still enforcing limits on uncompressed size

---

## 9. Runtime-Provider Contract Versioning

### Problem

The runtime and provider communicate through a trait contract (`OrchestrationProvider`). As duroxide evolves:
- New trait methods may be added (e.g., `list_orchestrations_by_tag`)
- Existing method signatures may change (new parameters, different return types)
- Event format may evolve (new fields, different serialization)
- A provider built for runtime v0.1 may not work with runtime v0.2

Currently there's no mechanism to:
- Detect contract version incompatibility at startup
- Negotiate capabilities between runtime and provider
- Gracefully degrade when provider doesn't support new features

### Proposed Solution

**9.1 Contract Version:**

```rust
/// Version of the runtime-provider contract
/// Increment when making breaking changes to:
/// - OrchestrationProvider trait methods
/// - Event enum variants or field changes
/// - WorkItem structure
/// - Any shared types between runtime and provider
pub const PROVIDER_CONTRACT_VERSION: u32 = 1;

/// Capabilities that may or may not be supported
#[derive(Debug, Clone, Default)]
pub struct ProviderCapabilities {
    pub supports_topic_subscriptions: bool,
    pub supports_registry_discovery: bool,
    pub supports_execution_truncation: bool,
    pub supports_batch_operations: bool,
    pub max_event_size: Option<usize>,
}
```

**9.2 Provider Trait Addition:**

```rust
pub trait OrchestrationProvider {
    // Existing methods...
    
    /// Returns the contract version this provider implements
    fn contract_version(&self) -> u32;
    
    /// Returns capabilities this provider supports
    fn capabilities(&self) -> ProviderCapabilities;
}
```

**9.3 Runtime Startup Validation:**

```rust
impl Runtime {
    pub fn new(provider: Arc<dyn OrchestrationProvider>) -> Result<Self, RuntimeError> {
        let provider_version = provider.contract_version();
        let runtime_version = PROVIDER_CONTRACT_VERSION;
        
        if provider_version != runtime_version {
            return Err(RuntimeError::ContractVersionMismatch {
                runtime_version,
                provider_version,
                message: format!(
                    "Runtime requires contract v{}, but provider implements v{}. \
                     Please upgrade your {}.",
                    runtime_version,
                    provider_version,
                    if provider_version < runtime_version { "provider" } else { "runtime" }
                ),
            });
        }
        
        // Log capabilities for debugging
        let caps = provider.capabilities();
        tracing::info!(?caps, "Provider capabilities detected");
        
        Ok(Self { provider, capabilities: caps })
    }
}
```

**9.4 Capability-Based Feature Gating:**

```rust
impl OrchestrationContext {
    pub async fn subscribe_topic(&self, pattern: &str) -> Result<TopicEvent, Error> {
        if !self.runtime.capabilities.supports_topic_subscriptions {
            return Err(Error::FeatureNotSupported {
                feature: "topic subscriptions",
                reason: "Provider does not support pub/sub",
            });
        }
        // ... implementation
    }
}
```

**9.5 Version Negotiation (Future):**

For more complex scenarios, support version ranges:

```rust
pub struct ContractCompatibility {
    /// Minimum contract version this component supports
    pub min_version: u32,
    /// Maximum contract version this component supports  
    pub max_version: u32,
    /// Preferred version
    pub preferred_version: u32,
}

// Runtime and provider negotiate to highest common version
fn negotiate_version(runtime: &ContractCompatibility, provider: &ContractCompatibility) -> Option<u32> {
    let min = runtime.min_version.max(provider.min_version);
    let max = runtime.max_version.min(provider.max_version);
    if min <= max { Some(max) } else { None }
}
```

**Implementation Considerations:**
- Start with strict version matching (v1), add negotiation later if needed
- Capabilities allow graceful degradation for optional features
- Contract version is separate from crate version (semver)
- Document what changes require contract version bump vs capability flag
- Provider implementations should fail clearly when asked for unsupported operations

---

## 10. Managed Long-Running Activities

> ⚠️ **Note**: This is a high-level concept that requires significant design work to flesh out.

### Problem

Current activities are fire-and-forget from the orchestration's perspective:
- No way to receive progress updates during execution
- No way to send control signals (pause, cancel, adjust parameters) mid-flight
- Long-running activities (hours/days) provide no visibility until completion
- Orchestration must poll external systems for status updates

### Concept

Introduce "managed activities" with bidirectional communication between orchestration and activity:
- Activity can send progress/status updates back to orchestration
- Orchestration can send control signals (cancel, pause, custom) to activity
- All communication happens through async/await on durable futures

Example: Activity sends "10% complete" → orchestration receives update → orchestration sends "cancel" → activity receives and aborts.

### Replay Model

The key insight is **asymmetric replay safety**:

- **Orchestration side**: Uses replay-safe durable futures. Communications from the activity are captured as history events. On replay, the orchestration sees the same sequence deterministically.

- **Activity side**: Is NOT replay-safe. If the activity worker crashes mid-execution, it cannot resume. The entire managed activity block fails with an infrastructure error, and the orchestration must handle it (retry, fail, or continue-as-new).

This trade-off is acceptable because managed activities target scenarios where **visibility matters more than crash recovery**.

### Target Use Case: Data-Intensive Operations

Managed activities are ideal for **data-intensive streaming operations** where:
- Large datasets are loaded, processed, and exhausted in a single pass
- Progress visibility is critical for operational monitoring
- The operation is inherently non-idempotent (e.g., consuming a queue, processing a stream)
- Crash recovery would mean restarting the entire pipeline anyway

Examples:
- ETL jobs: Load millions of records → transform → write to destination
- Batch exports: Query database → format → upload to S3
- Queue draining: Consume messages → process → acknowledge
- Report generation: Aggregate data → render → deliver

---

## Open Questions

1. **Pub/Sub**: Should late subscribers receive a "snapshot" of current state, or only future events?
2. **Dispatcher**: What's the right default prefetch depth? Should it be adaptive?
3. **Truncate**: Should truncation be synchronous or queue a background job?
4. **Macros**: How to handle versioning when function signatures change?
5. **Poison**: Should quarantine state survive `continue_as_new`?
6. **Registry Discovery**: How to handle multiple runtime instances registering different versions? Merge or last-write-wins?
7. **Event Size**: Should limits be configurable per-orchestration, or global only? What about compression?
8. **Contract Versioning**: When should we bump contract version vs add a capability flag?


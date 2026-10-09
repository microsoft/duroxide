# Orchestration Versioning Best Practices

This guide covers practical patterns for managing versioned orchestrations in production, particularly for long-running workflows that use `continue_as_new`.

## Table of Contents

1. [Code Organization Pattern](#code-organization-pattern)
2. [Registration Pattern](#registration-pattern)
3. [Version Upgrade Timing](#version-upgrade-timing)
4. [Best Practices Checklist](#best-practices-checklist)
5. [Version Info Flow](#version-info-flow)
6. [Common Scenarios](#common-scenarios)

---

## Code Organization Pattern

Keep the orchestration NAME constant stable across all versions. Create separate functions for each version with a version suffix:

```rust
// names.rs - Keep NAME constant stable
pub const MY_ORCHESTRATION: &str = "my-app::orchestration::my-orch";

// my_orchestration.rs
use duroxide::OrchestrationContext;

/// Version 1.0.0 - Original implementation
pub async fn my_orchestration(
    ctx: OrchestrationContext, 
    input: String
) -> Result<String, String> {
    ctx.trace_info("Starting...");
    // Original logic
    ctx.schedule_timer(Duration::from_secs(60)).await;
    ctx.continue_as_new(input).await
}

/// Version 1.0.1 - Added version prefix to traces for debugging
pub async fn my_orchestration_1_0_1(
    ctx: OrchestrationContext, 
    input: String
) -> Result<String, String> {
    ctx.trace_info("[v1.0.1] Starting...");
    // Same logic with improved observability
    ctx.schedule_timer(Duration::from_secs(60)).await;
    ctx.continue_as_new(input).await
}

/// Version 1.0.2 - Increased timer interval
pub async fn my_orchestration_1_0_2(
    ctx: OrchestrationContext, 
    input: String
) -> Result<String, String> {
    ctx.trace_info("[v1.0.2] Starting...");
    // Updated logic with longer interval
    ctx.schedule_timer(Duration::from_secs(300)).await;
    ctx.continue_as_new(input).await
}
```

**Why this pattern works:**
- The NAME constant provides stable identity for the orchestration
- Separate functions allow different implementations without breaking replay
- Version suffixes make code navigation easy (`my_orch_1_0_1`, `my_orch_1_0_2`)
- Trace prefixes like `[v1.0.2]` make log analysis straightforward

---

## Registration Pattern

Register all versions in your registry, with the default version using `register_typed` and explicit versions using `register_versioned_typed`:

```rust
use crate::orchestrations::{
    MY_ORCHESTRATION,
    my_orchestration,
    my_orchestration_1_0_1, 
    my_orchestration_1_0_2,
};

let orchestrations = OrchestrationRegistry::builder()
    // Default version (1.0.0) - use register_typed
    .register_typed(MY_ORCHESTRATION, my_orchestration)
    
    // Explicit versions - use register_versioned_typed
    .register_versioned_typed(MY_ORCHESTRATION, "1.0.1", my_orchestration_1_0_1)
    .register_versioned_typed(MY_ORCHESTRATION, "1.0.2", my_orchestration_1_0_2)
    
    .build();
```

**Key points:**
- `register_typed()` registers at version `1.0.0` by default
- `register_versioned_typed()` registers at the specified semver version
- The default version policy is `Latest` - new starts and `continue_as_new` use the highest version
- Use `set_version_policy()` to pin new instances to a specific version if needed

---

## Version Upgrade Timing

**Critical concept:** Version upgrades happen at `continue_as_new` time, NOT when the server restarts.

This explains why you might see a delay between deploying a new version and seeing it reflected in your UI/monitoring:

```
Timeline example:

T+0:00  Server restarts with v1.0.2 registered
        But v1.0.1 was mid-cycle with ~1 min left on its timer
        
T+1:00  v1.0.1's timer expires, orchestration does its work
        
T+1:01  v1.0.1 calls continue_as_new()
        → Duroxide resolves "Latest" policy → v1.0.2
        → Database updated: orchestration_version = "1.0.2"
        → New execution starts with v1.0.2 code
        
T+1:02  UI/API refreshes → now shows v1.0.2
```

**Why this matters:**
- Duroxide doesn't interrupt running orchestrations
- The version policy (`Latest`) is evaluated when a new execution starts
- If you have a long timer (e.g., 5 minutes), the old version runs until that timer expires
- This is intentional - it ensures orchestration determinism and clean execution boundaries

---

## Best Practices Checklist

### ✅ DO

1. **Keep NAME constants stable** across all versions
   ```rust
   pub const MY_ORCH: &str = "myapp::orchestration::my-orch"; // Never changes
   ```

2. **Use version suffix on function names**
   ```rust
   my_orch()       // v1.0.0
   my_orch_1_0_1() // v1.0.1
   my_orch_1_0_2() // v1.0.2
   ```

3. **Add version prefix to trace logs** for easier debugging
   ```rust
   ctx.trace_info("[v1.0.2] Starting health check cycle");
   ctx.trace_info("[v1.0.2] Completed iteration, continuing...");
   ```

4. **Document version changes** in code comments
   ```rust
   /// Version 1.0.2 - Changes from 1.0.1:
   /// - Increased timer interval from 2 min to 5 min
   /// - Added retry logic for health checks
   ```

5. **Test version transitions** in development before production
   - Deploy new version
   - Verify old version completes its cycle
   - Verify new version starts after `continue_as_new`

### ❌ DON'T

1. **Don't change the NAME constant** when creating new versions
2. **Don't expect immediate version switch** after server restart
3. **Don't use `continue_as_new_versioned()`** unless you specifically need to pin to a version
4. **Don't delete old version code** until all instances have upgraded

---

## Version Info Flow

Understanding how version information flows through the system:

```
                      Duroxide Runtime
                            │
                            ▼
┌─────────────────────────────────────────────────┐
│  Provider (instances table)                      │
│  ├── orchestration_name: "my-app::my-orch"      │
│  └── orchestration_version: "1.0.2"             │
└─────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────┐
│  Client.get_instance_info()                      │
│  └── Returns InstanceInfo {                      │
│        orchestration_version: "1.0.2"           │
│      }                                           │
└─────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────┐
│  Your Backend API                                │
│  └── /api/orchestrations/:id                    │
│      { "version": "1.0.2", ... }                │
└─────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────┐
│  Your UI / Dashboard                             │
│  └── Displays: "Version: 1.0.2"                 │
└─────────────────────────────────────────────────┘
```

To display version in your UI:
1. Call `client.get_instance_info(instance_id)` 
2. Access `info.orchestration_version`
3. Include in your API response
4. Display in your dashboard

---

## Common Scenarios

### Scenario 1: Hot-fix deployment

You need to deploy a critical fix to a long-running orchestration:

```rust
// v1.0.1 - Critical fix for health check timeout
pub async fn health_monitor_1_0_1(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    ctx.trace_info("[v1.0.1] Starting with increased timeout fix");
    
    // Fixed: Increased timeout from 5s to 30s
    let result = ctx.schedule_activity_with_retry(
        "HealthCheck",
        input.clone(),
        RetryPolicy::new(3).with_timeout(Duration::from_secs(30)),
    ).await?;
    
    ctx.schedule_timer(Duration::from_secs(60)).await;
    ctx.continue_as_new(input).await
}
```

**What happens:**
1. Deploy server with v1.0.1 registered
2. Running v1.0.0 instances complete their current timer
3. On `continue_as_new`, they automatically upgrade to v1.0.1
4. New fix takes effect without manual intervention

### Scenario 2: Graceful migration with state changes

If you need to transform state between versions:

```rust
// v2.0.0 - New state format
pub async fn my_orch_2_0_0(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    ctx.trace_info("[v2.0.0] Starting with new state format");
    
    // Handle migration from v1 state format
    let state: MyState = if input.starts_with("{\"v1\":") {
        ctx.trace_info("[v2.0.0] Migrating from v1 state format");
        migrate_v1_to_v2(&input)?
    } else {
        serde_json::from_str(&input)?
    };
    
    // Continue with v2 logic...
    ctx.continue_as_new(serde_json::to_string(&state)?).await
}
```

### Scenario 3: Pinning to exact version

If you need to prevent automatic upgrades (e.g., during testing):

```rust
let orchestrations = OrchestrationRegistry::builder()
    .register_typed(MY_ORCH, my_orchestration)
    .register_versioned_typed(MY_ORCH, "1.0.1", my_orchestration_1_0_1)
    .set_policy(MY_ORCH, VersionPolicy::Exact(Version::parse("1.0.0").unwrap()))
    .build();
```

Or use `continue_as_new_versioned` for explicit control:

```rust
// Stay on current version explicitly
ctx.continue_as_new_versioned("1.0.0", input).await
```

---

## Draining Stuck Orchestrations (Version Mismatch)

An orchestration stays in the queue when no running node supports its pinned duroxide version.
With the default range (`0.0.0` up to the node's own build version) every node supports all
older versions, so stuck items are pinned to a version **newer** than every running node (for
example after a runtime rollback), or the nodes run a narrowed range.

**Do not widen `supported_replay_versions` above the node's own build version.** History that
deserializes is not necessarily replay-compatible. Duroxide 0.1.31 changed race-cancellation
semantics without adding event types: a pre-0.1.31 node that admits a 0.1.31+ execution
deserializes its history and replays it with the old rules, which can change recorded decisions
(nondeterminism) or deliver a different message or signal.

To clear the backlog:

1. **Process the items on a compatible node.** Run at least one node whose build version is at
   least the pinned version, with the default range.
2. **Or abandon them explicitly.** Delete the affected instances through the client
   (`delete_instance`, `delete_instance_bulk`; pass `force` for a running instance).
   `cancel_instance` only queues a request that a compatible node must still process.
3. **Restore a narrowed node** only down to older versions:

```rust
let runtime = Runtime::builder()
    .with_provider(provider)
    .with_options(RuntimeOptions {
        // Older versions only: the upper bound is this node's own build version.
        supported_replay_versions: Some(SemverRange::new(
            semver::Version::new(0, 0, 0),
            duroxide::providers::current_build_version(),
        )),
        ..Default::default()
    })
    .build()
    .await?;
```

**After draining:** revert `supported_replay_versions` to `None` (the default) or the
appropriate range for your cluster.

> **Limitation:** `supported_replay_versions` is not clamped to the build version; an override
> replaces both the provider filter and the runtime-side check. Operators own that boundary.
> Items whose history contains unknown event types still fail provider-level deserialization,
> but that protects only against new event types, not against semantic changes such as 0.1.31.

---

## See Also

- [Migration Guide](migration-guide.md) - For major version migrations
- [Continue As New Semantics](continue-as-new.md) - Deep dive into CAN behavior
- [ORCHESTRATION-GUIDE.md](ORCHESTRATION-GUIDE.md) - Complete orchestration reference

### Rolling Upgrades with Session Activities

Session-bound activities are pinned to the owning worker until the session unpins.
During rolling upgrades, ensure session activity handlers are registered on all workers
before orchestrations schedule activities on sessions. If a session activity is not
registered on the owning worker, it will poison after `max_attempts` backoff cycles
rather than failing over to an upgraded worker.

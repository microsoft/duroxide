# Migration Guide

This guide helps you migrate between Duroxide versions and handle orchestration versioning.

## Reserved `sub::` instance-id marker (Unreleased)

The `sub::` marker is now reserved for runtime-generated sub-orchestration instance ids.
`Client::start_orchestration` and `Client::start_orchestration_versioned` reject root
instance ids that:

- start with `sub::`, or
- contain the `::sub::` infix.

Such ids return `ClientError::InvalidInput`. Ordinary uses of `::` in instance ids remain
valid (e.g. `tenant-7::order-42`); only the `sub::` marker is reserved.

This prevents a root instance id from pre-occupying an auto-generated child id. Child
sub-orchestration ids take the form `{parent}::sub::{event_id}` on the first parent
execution and `{parent}::sub::{execution_id}_{event_id}` after `continue_as_new`.

Before upgrading client code, audit your root instance-id scheme for the reserved marker:

```text
# Reject — start with `sub::` or contain `::sub::`
sub::job-1
tenant-7::sub::order-42

# Accept — ordinary `::` is fine
tenant-7::order-42
order-2026-06-09
```

Rename any root instance ids that use the reserved marker before upgrading.

### Explicit sub-orchestration ids use a narrower rule

`ctx.schedule_sub_orchestration_with_id()` and
`ctx.schedule_sub_orchestration_versioned_with_id()` reject only ids that **start with**
`sub::`. The returned future resolves immediately to an `Err`; nothing is scheduled and no
`SubOrchestrationScheduled` event is written.

| | starts with `sub::` | contains `::sub::` |
| --- | --- | --- |
| `Client::start_orchestration` | reject | reject |
| `ctx.schedule_orchestration` (detached root) | reject | reject |
| `ctx.schedule_sub_orchestration_with_id` | reject | **allow** |

The two rules answer different questions:

- A **root** id must not occupy a name some future child will need. Child names carry the
  marker anywhere in the string, so the infix has to be reserved. This applies to both
  top-level client starts and detached starts via `ctx.schedule_orchestration()`, whose id is
  also used verbatim as a root id. Because that method returns `()`, a violation panics
  rather than returning an error.
- An **explicit child** id only has to avoid the runtime's control signals. A leading
  `sub::` is read by `build_child_instance_id` as "auto-generated suffix, add the parent
  prefix", and `sub::pending_` is an internal placeholder that gets replaced outright — so
  those ids were silently rewritten instead of used verbatim. Everything else is safe.

The infix **must** stay legal for child ids because the runtime generates it itself: a
grandchild of `root` is named `root::sub::2::sub::2`, and deriving a child id from
`ctx.instance_id()` inside a sub-orchestration naturally produces ids like
`root::sub::2::worker-1`.

```rust
// Rejected — leading marker, previously rewritten to "{parent}::sub::my-child"
ctx.schedule_sub_orchestration_with_id("Child", "sub::my-child", input);

// Rejected — internal placeholder shape, previously discarded entirely
ctx.schedule_sub_orchestration_with_id("Child", "sub::pending_99", input);

// Accepted — used verbatim
ctx.schedule_sub_orchestration_with_id("Child", "tenant::sub::99", input);
ctx.schedule_sub_orchestration_with_id("Child", format!("{}::worker-1", ctx.instance_id()), input);
```

If you have in-flight instances that scheduled a child with a leading-`sub::` explicit id,
rename the id before upgrading: replaying that history against the new validation produces a
nondeterminism failure, because history records a scheduling event the new code no longer
emits.

## Durable sub-orchestration routing (`parent_execution_id`)

Sub-orchestration completion and failure notifications are now routed to the exact parent
execution that scheduled the child. To do this, the scheduling parent's execution id is
stamped onto the child's start and persisted in the child's history:

- `WorkItem::StartOrchestration` gains an optional `parent_execution_id` field.
- `EventKind::OrchestrationStarted` gains an optional `parent_execution_id` field.

Both fields are `Option<u64>`, serialized with `#[serde(default, skip_serializing_if = "Option::is_none")]`,
so the wire and history formats remain backward compatible:

- **Old → new:** A new runtime reading an old child history (or an old work item) sees
  `parent_execution_id = None` and falls back to a durable provider read of the parent's
  current execution — the previous behavior.
- **New → old:** An old runtime ignores the extra field (it is skipped when absent and not
  required when deserializing).

No action is required to upgrade. Mixed-version clusters route correctly during a rolling
upgrade. The provider-read fallback is retained only for histories/work items created before
this change.

## Orchestration Versioning

Duroxide supports versioning to handle code evolution while maintaining compatibility with running instances.

### When to Version

You need to version your orchestration when:

1. **Adding/removing activities**: Changes the execution flow
2. **Reordering operations**: Affects correlation IDs
3. **Changing conditional logic**: Alters execution paths
4. **Modifying data structures**: Input/output format changes

You DON'T need to version when:

1. **Fixing bugs in activities**: Activities are stateless
2. **Improving activity performance**: No behavior change
3. **Adding logging**: Using `ctx.trace_*` is replay-safe
4. **Refactoring activity internals**: Interface remains the same

### Versioning Strategy

```rust
// Version 1.0.0
let orchestration_v1 = |ctx: OrchestrationContext, input: String| async move {
    let result = ctx.schedule_activity("ProcessV1", input).await?;
    Ok(result)
};

// Version 2.0.0 - Added validation step
let orchestration_v2 = |ctx: OrchestrationContext, input: String| async move {
    // New validation step
    let validated = ctx.schedule_activity("Validate", &input).await?;
    let result = ctx.schedule_activity("ProcessV2", validated).await?;
    Ok(result)
};

// Register both versions
let orchestrations = OrchestrationRegistry::builder()
    .register_versioned("MyOrchestration", "1.0.0", orchestration_v1)
    .register_versioned("MyOrchestration", "2.0.0", orchestration_v2)
    .with_version_policy(VersionPolicy::Latest) // New instances use latest
    .build();
```

### Version Policies

1. **Latest** (default): New instances use the latest registered version
2. **Exact**: Must specify exact version when starting
3. **Compatible**: Use semantic versioning rules

### Handling Running Instances

When you deploy a new version:

1. **Running instances continue with their version**: Pinned at start
2. **New instances use the latest version**: Based on policy
3. **ContinueAsNew can change versions**: Explicitly specify

```rust
// Migrate running instance to new version via ContinueAsNew
ctx.continue_as_new_versioned("2.0.0", new_input);
```

## Breaking Changes Between Versions

### Duroxide 0.1.0 → 0.2.0 (Hypothetical)

#### API Changes

1. **Activity Registration**:
   ```rust
   // Old (0.1.0)
   .register("MyActivity", |ctx: ActivityContext, input: String| async move { Ok(result) })
   
   // New (0.2.0) - Explicit error type
   .register("MyActivity", |ctx: ActivityContext, input: String| async move -> Result<String, ActivityError> { 
       Ok(result) 
   })
   ```

2. **Orchestration Context**:
   ```rust
   // Old (0.1.0)
   ctx.new_guid() // Removed
   
   // New (0.2.0)
   ctx.system_new_guid().await // Async system activity
   ```

3. **Runtime Creation**:
   ```rust
   // Old (0.1.0)
   Runtime::start(activities, orchestrations).await
   
   // New (0.2.0) - Explicit store
   Runtime::start_with_store(store, activities, orchestrations).await
   ```

#### Migration Steps

1. **Update Dependencies**:
   ```toml
   [dependencies]
   duroxide = "0.2"
   ```

2. **Update Activity Signatures**:
   - Add explicit error types
   - Update return types if changed

3. **Update Orchestration Code**:
   - Replace deprecated methods
   - Update to new async APIs

4. **Test Thoroughly**:
   - Run existing tests
   - Test with production-like data
   - Verify determinism

## Data Migration

### Handling Input/Output Format Changes

When changing data structures:

1. **Support both formats temporarily**:
   ```rust
   #[derive(Serialize, Deserialize)]
   #[serde(untagged)]
   enum InputCompat {
       V1(InputV1),
       V2(InputV2),
   }
   
   let orchestration = |ctx: OrchestrationContext, input_json: String| async move {
       let input: InputCompat = serde_json::from_str(&input_json)?;
       
       match input {
           InputCompat::V1(v1) => {
               // Handle old format
               let v2 = migrate_v1_to_v2(v1);
               process_v2(ctx, v2).await
           }
           InputCompat::V2(v2) => {
               // Handle new format
               process_v2(ctx, v2).await
           }
       }
   };
   ```

2. **Gradual migration**:
   - Deploy version supporting both formats
   - Migrate data at your pace
   - Remove old format support later

### Storage Provider Migration

When switching providers:

```rust
// 1. Export from old provider
let old_store = InMemoryHistoryStore::new();
let instances = old_store.list_instances().await;

for instance in instances {
    let history = old_store.read(&instance).await;
    // Save history to new provider
}

// 2. Import to new provider
let new_store = SqliteProvider::new("sqlite:./data.db", None).await?;
for (instance, history) in saved_data {
    // Recreate instance in new store
    new_store.create_instance(&instance).await?;
    new_store.append(&instance, history).await?;
}

// 3. Switch runtime to new provider
let rt = Runtime::start_with_store(Arc::new(new_store), activities, orchestrations).await;
```

## Best Practices for Versioning

1. **Semantic Versioning**: Use major.minor.patch
   - Major: Breaking changes
   - Minor: New features, backward compatible
   - Patch: Bug fixes

2. **Deployment Strategy**:
   - Deploy new version alongside old
   - Monitor both versions
   - Gradually migrate instances
   - Remove old version when safe

3. **Testing Strategy**:
   ```rust
   #[test]
   async fn test_version_compatibility() {
       // Test that v1 instances complete successfully
       let v1_result = run_with_version("1.0.0", v1_input).await;
       
       // Test that v2 instances work with new features
       let v2_result = run_with_version("2.0.0", v2_input).await;
       
       // Test migration path
       let migrated = migrate_v1_to_v2(v1_result);
       assert_eq!(migrated, expected);
   }
   ```

4. **Documentation**:
   - Document what changed
   - Provide migration examples
   - List breaking changes clearly
   - Include compatibility matrix

## Rollback Strategy

If issues arise after deployment:

1. **Leave running instances**: They continue with their pinned version
2. **Revert new instances**: Change version policy or registration
3. **Monitor and fix**: Address issues without affecting running work

```rust
// Emergency rollback configuration
let orchestrations = OrchestrationRegistry::builder()
    .register_versioned("MyOrchestration", "1.0.0", orchestration_v1)
    .register_versioned("MyOrchestration", "2.0.0", orchestration_v2)
    .with_version_policy(VersionPolicy::Exact("1.0.0")) // Force v1 for new instances
    .build();
```

## Draining Stuck Orchestrations After Upgrade

If after a full upgrade some orchestrations remain pinned to an old duroxide version that no
running node supports, they will sit in the queue indefinitely. To clear them, temporarily
widen `supported_replay_versions` in `RuntimeOptions`:

```rust
RuntimeOptions {
    supported_replay_versions: Some(SemverRange::new(
        semver::Version::new(0, 0, 0),
        semver::Version::new(99, 0, 0),
    )),
    max_attempts: 5,
    ..Default::default()
}
```

The wide range causes the provider filter to pass for all items. When the provider fetches
an item whose history contains unknown event types (from a newer duroxide version),
deserialization fails at the provider level, returning a permanent error. Each fetch cycle
increments the item's `attempt_count`. The item remains in the queue but is effectively
drained — it never reaches the runtime's replay engine because the provider cannot
deserialize its history. Compatible items whose history deserializes successfully are
processed normally. Revert to the default after draining.

See [Versioning Best Practices](versioning-best-practices.md#draining-stuck-orchestrations-version-mismatch)
for details.

## Future Compatibility

To make future migrations easier:

1. **Use typed inputs/outputs** with serde
2. **Version your APIs** from the start
3. **Keep orchestrations simple** - complex logic in activities
4. **Document assumptions** and invariants
5. **Test with multiple versions** in CI/CD

## Session Affinity Notes

Sessions are backward-compatible by design:
- Existing `schedule_activity` calls are unaffected (`session_id = None`)
- Old `ActivityScheduled` events without `session_id` deserialize with `session_id = None` via `#[serde(default)]`
- Provider schema migration: add `session_id` column to `worker_queue`, create `sessions` table
- No changes required to existing orchestration or activity code

## Getting Help

For migration assistance:

1. Review the [changelog](../CHANGELOG.md) for detailed changes
2. Check [examples](../examples/) for updated patterns
3. Run tests to verify compatibility
4. Open an issue for migration problems

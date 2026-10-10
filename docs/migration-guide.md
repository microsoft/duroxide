# Migration Guide

This guide helps you migrate between Duroxide versions and handle orchestration versioning.

## Race cancellation semantics (reserved 0.1.31)

The queue, positional-signal, and continue-as-new fixes share one execution
version switch. Applying those changes to an old execution can change a
recorded race winner, accepted signal, or carry-forward payload.

The runtime selects the policy from the `duroxide_version` of the first
`OrchestrationStarted` event and keeps it for the entire execution:

| Execution started by | Queue cancellation policy |
| --- | --- |
| Core before 0.1.31 | Original queue, positional, and CAN behavior |
| Core 0.1.31 or later | Immediate cancellation at drop/bind, bound-active-slot signal admission, and exact unread-arrival carry-forward |

Upgrading a runtime does not change an existing execution's policy. Old
histories and subsequent turns retain their original scheduling decisions.
This also retains the known old queue, signal, and CAN defects for those executions until
they finish or continue as new. A replacement execution started by a new
runtime uses the fixed policy.

Do not rewrite the initial version stamp to enable the fix in place. A
recorded race winner may then change, causing nondeterminism. No history or
provider schema migration is needed.

An execution whose stamp is present but is not a valid version runs with the
legacy policy and logs "Invalid pinned version" on each turn. A missing stamp
field fails deserialization, and the instance is poisoned.

With the default range in mixed-version deployments, the dispatcher capability filter
prevents an old runtime from replaying an execution stamped 0.1.31. A start
handled by an old runtime still creates a legacy execution. Upgrade all
runtimes before expecting every new execution to use the fix.

Do not widen `supported_replay_versions` on a pre-0.1.31 node beyond 0.1.30
while 0.1.31+ executions exist. The override replaces both compatibility checks,
and the unchanged event schema does not prevent a legacy runtime from
misinterpreting new queue decisions. Deserialization compatibility is not
replay-semantic compatibility.

No release numbered 0.1.31 or later may omit this semantic cutover: its number
is the recorded contract, not a runtime upgrade switch.

Version 0.1.31 is reserved for the semantic cutover. The
subsequent release PR uses 0.1.32, which still satisfies the
same threshold. Never publish 0.1.31 from an older branch without these fixes.

### Positional signal contract

The implemented design-document contract discards a signal addressed to a
dropped positional wait's slot; its replacement receives a fresh signal.
This includes a value accepted into that slot while the future was not polled.
Signals are admitted only for a bound active wait. A signal applied in the same
batch after the completion that emits a replacement is still early until
the replacement is bound. That signal is dropped; the next fresh signal
can resolve the bound replacement. Batch insertion order does not establish
when a signal was raised relative to creation of an unbound wait.
Queued messages instead remain persistent after a dequeue is dropped.

Retaining the old answer for a later wait is a documented alternative, not
the implemented contract. Such an alternative would require its own versioned
policy for future executions, not a reinterpretation of existing history.

Continue-as-new carries up to 100 queued arrivals that no dequeue returned, in
history order across names. Newer unread arrivals beyond that limit are
dropped with a warning (`MAX_CARRY_FORWARD`). A held, unresolved dequeue is not a consumer;
consumption need not form a prefix when newer dequeues are polled first.
Activity and child cleanup keep their existing continue-as-new behavior.

### Continue-as-new input admission

The dispatcher also fixes a pre-existing ingestion window: an old execution
has committed continue-as-new, but the current fetch cutoff cannot yet see
the successor's start. Instance-scoped inputs in that batch are preserved
by abandoning them with the unregistered-handler backoff, not acknowledged as
terminal work. When the start is visible, carried messages
precede the preserved messages. A deferred input can be delivered after an
input that arrived later: queue order across abandons is best effort. Stale
activity, timer and child completions for the previous execution wait with the
other inputs and the successor discards them.
Positional signals still follow the successor's signal-admission contract;
preserving the work item does not make positional signals persistent.

This correction changes no previously recorded replay decision and applies
to every version stamp. Waiting batches use the same backoff and attempt
budget as an unregistered handler: each deferral keeps its attempt, and a
start that never becomes visible ends in poison after `max_attempts` (about
five minutes with the defaults). Deferred inputs carry their attempts into the
successor's first turn. If the abandon fails, the lease expires and the
inputs are fetched again.
SQLite restores
only locked rows' attempts, matching PostgreSQL, not hidden unregistered starts.
A mixed deployment remains exposed while any unpatched worker can
fetch the affected instance; upgrade all orchestration workers to close it.
No schema migration or rewrite of the previous execution's history is needed.
The CAN start also wins over a duplicate client start independently of queue
order. A duplicate start for an instance with history does not replace its
name, version or input. A sub-orchestration start that collides with an
existing instance fails the incoming parent, whether that instance is finished
or still running. The exception is a running instance's own parent re-sending
the same call (same parent instance, call id and a known, equal parent
execution id), which is ignored as before.

If the successor's first turn is poisoned, its failure can currently be
recorded after CAN in the previous execution. This history is terminal:
the poison transaction consumed the start, so later queue, signal and cancel
inputs are acknowledged rather than deferred.

While a successor start stays hidden (for example behind an unregistered
handler's backoff during a rolling upgrade), each waiting instance is fetched
and abandoned on the unregistered-handler backoff schedule. A provider-level
instance cooldown and a
continue-as-new start that a fetch cutoff cannot exclude are separate
follow-ups.

The CAN start exists after the atomic terminal acknowledgement. Successful
fetches eventually observe it once its visibility delay or registration
backoff ends. Each deferral logs a warning with the batch size, the attempt
count and the chosen backoff. A start that never becomes visible (permanent
deletion, corruption or an indefinitely unavailable handler) poisons the
waiting inputs once the attempt budget is exhausted. A warning is not evidence
that an input was delivered or that an existing deleted input was recovered.

### Retained legacy hazards

An old execution that already recorded an unsafe race ordering is not
repaired by upgrade. Do not rewrite its initial stamp. Operators must use
application-specific restart/recovery or let a healthy execution finish or
continue as new on an upgraded runtime. The library provides no automatic
in-place repair of a failed or hanging legacy history.

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

These steps roll back application handlers on the same duroxide runtime.

### Rolling back the duroxide runtime itself

Rolling the duroxide crate back across the 0.1.31 race-cancellation cutover is
not a supported routine operation: fix forward instead. Executions started by
0.1.31 or later must be replayed by a 0.1.31+ runtime.

- By default a pre-0.1.31 worker does not fetch them (its supported range ends
  at its own build version), so they wait until a 0.1.31+ worker runs again.
- An instance that keeps continuing as new on 0.1.31+ workers stays on the new
  policy, because each successor is stamped by the runtime that starts it.
- If a core downgrade is unavoidable, keep at least one 0.1.31+ worker with the
  required handlers running until every new-policy execution has finished.
  Do not widen an old worker's `supported_replay_versions` to cover them
  (see [Draining Stuck Orchestrations](#draining-stuck-orchestrations-after-upgrade)).
- Downgraded workers also reopen the continue-as-new input-loss window and the
  duplicate-start defect fixed in 0.1.31; those fixes apply only on patched workers.

## Draining Stuck Orchestrations After Upgrade

Orchestrations sit in the queue when no running node supports their pinned duroxide version.
With the default range (`0.0.0` up to the node's own build version) every node supports all
older versions, so this happens only when executions are pinned to a version **newer** than
every running node (for example after a runtime rollback), or when nodes run a narrowed range.

- **Never set the upper bound above the node's own build version**
  (`duroxide::providers::current_build_version()`). History that still deserializes can carry
  different replay semantics: executions stamped 0.1.31 or later replay with the race-cancellation
  rules, and a pre-0.1.31 node would replay them with the old rules and change recorded decisions.
- **To process them**, run at least one node whose build version is at least the pinned version,
  with the default range (or a range that ends at that node's build version).
- **To abandon them**, delete the instances explicitly (`Client::delete_instance`,
  `Client::delete_instance_bulk`; pass `force` for an instance that is still running).
  `Client::cancel_instance` only queues a cancel request, which a compatible node must still
  process. Do not route them to an older node.

```rust
// A narrowed node can be widened back down to older versions only; the upper
// bound stays at this node's own build version.
RuntimeOptions {
    supported_replay_versions: Some(SemverRange::new(
        semver::Version::new(0, 0, 0),
        duroxide::providers::current_build_version(),
    )),
    ..Default::default()
}
```

Deserialization succeeding is not evidence of replay compatibility. Revert any temporary
range change after the backlog drains.

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

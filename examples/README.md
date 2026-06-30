# Duroxide Examples

This directory contains complete, runnable examples that demonstrate how to use Duroxide for various orchestration patterns. These examples are designed to be the first place LLMs and developers look when learning how to use Duroxide.

## Quick Start Examples

### 1. Delays and Timeouts (`delays_and_timeouts.rs`) ⚠️ IMPORTANT
**Start here if you're confused about timers vs activities!**

```bash
cargo run --example delays_and_timeouts
```

**What you'll learn:**
- ✅ CORRECT: Use `ctx.schedule_timer()` for orchestration delays and timeouts
- ✅ CORRECT: Use `ctx.select2()` for timeout patterns
- ✅ CORRECT: Activities can use any async operations (sleep, HTTP, DB, etc.)
- The key rule: Orchestrations = deterministic, Activities = any async work

### 2. Hello World (`hello_world.rs`)
Basic orchestration setup and execution.

```bash
cargo run --example hello_world
```

**What you'll learn:**
- Setting up a basic orchestration with activities
- Using the SQLite provider for persistence
- Running orchestrations with the in-process runtime
- Basic error handling and completion waiting

### 3. Fan-Out/Fan-In (`fan_out_fan_in.rs`)
Parallel processing pattern for handling multiple items concurrently.

```bash
cargo run --example fan_out_fan_in
```

**What you'll learn:**
- Scheduling multiple activities in parallel
- Using `ctx.join()` for deterministic result aggregation
- Typed activity inputs and outputs with Serde
- Processing collections of data efficiently

### 4. Timers and External Events (`timers_and_events.rs`)
Human-in-the-loop workflows with timeouts and external approvals.

```bash
cargo run --example timers_and_events
```

**What you'll learn:**
- Using durable timers for delays and timeouts
- Waiting for external events (approvals, webhooks)
- Race conditions with `ctx.select2()`
- Building approval workflows

## Advanced Patterns

### Function Chaining
Sequential operations where each step depends on the previous result:

```rust
async fn chain_example(ctx: OrchestrationContext) -> Result<String, String> {
    let step1 = ctx.schedule_activity("Step1", "input").await?;
    let step2 = ctx.schedule_activity("Step2", &step1).await?;
    let step3 = ctx.schedule_activity("Step3", &step2).await?;
    Ok(step3)
}
```

### Saga Pattern (Compensation)
Rollback operations on failure:

```rust
async fn saga_example(ctx: OrchestrationContext) -> Result<String, String> {
    let result1 = ctx.schedule_activity("ReserveInventory", "item1").await?;
    
    match ctx.schedule_activity("ProcessPayment", "card123").await {
        Ok(payment) => {
            ctx.schedule_activity("ShipItem", &result1).await?;
            Ok("Order completed".to_string())
        }
        Err(e) => {
            // Compensate: release inventory
            ctx.schedule_activity("ReleaseInventory", &result1).await?;
            Err(format!("Payment failed, inventory released: {}", e))
        }
    }
}
```

### Sub-Orchestrations
Breaking complex workflows into smaller, reusable orchestrations:

```rust
async fn parent_orchestration(ctx: OrchestrationContext) -> Result<String, String> {
    // Start multiple sub-orchestrations
    let sub1 = ctx.schedule_sub_orchestration("ProcessOrder", "order1");
    let sub2 = ctx.schedule_sub_orchestration("ProcessOrder", "order2");
    
    // Wait for all to complete
    let results = ctx.join(vec![sub1, sub2]).await;
    
    // Aggregate results - each result is Result<String, String>
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    
    Ok(format!("Processed {} orders successfully", success_count))
}
```

## Common Use Cases

### 1. **E-commerce Order Processing**
- Reserve inventory → Process payment → Ship item → Send confirmation
- Compensation: Release inventory if payment fails

### 2. **Document Approval Workflows**
- Submit document → Notify approvers → Wait for approval → Archive
- Timeout handling: Send reminders, escalate after delays

### 3. **Data Processing Pipelines**
- Fan-out: Process multiple files in parallel
- Fan-in: Aggregate results and generate reports
- Error handling: Retry failed items, continue with successful ones

### 4. **Resource Provisioning**
- Provision infrastructure → Configure services → Run health checks → Notify completion
- Rollback: Clean up resources if any step fails

## Best Practices

### 1. **Activity Design**
- Keep activities stateless and idempotent
- Use meaningful activity names
- Handle errors gracefully within activities

### 2. **Orchestration Design**
- Keep orchestrations deterministic (no random numbers, current time, etc.)
- Use `ctx.trace_*` for logging instead of `println!`
- Handle all possible error cases

### 3. **Error Handling**
- Use `Result<String, String>` for activity outputs
- Implement compensation logic for critical operations
- Use timeouts to prevent hanging orchestrations

### 4. **Performance**
- Use fan-out/fan-in for parallel processing
- Consider sub-orchestrations for complex workflows
- Use appropriate timeout values

## Running Examples

All examples can be run with:

```bash
# Run a specific example
cargo run --example hello_world

# Run with logging
RUST_LOG=debug cargo run --example fan_out_fan_in

# Run all examples (if you have a script)
for example in hello_world fan_out_fan_in timers_and_events; do
    echo "Running $example..."
    cargo run --example $example
done
```

## Next Steps

After running these examples:

1. **Read the documentation**: Check `docs/ORCHESTRATION-GUIDE.md` for the complete guide
2. **Explore the test suite**: Look at `tests/e2e_samples.rs` for more complex scenarios
3. **Build your own**: Start with a simple orchestration and gradually add complexity
4. **Join the community**: Duroxide is currently in preview - contributions welcome!

## Troubleshooting

### Common Issues

1. **"Activity not found"**: Make sure you've registered the activity in the `ActivityRegistry`
2. **"Orchestration not found"**: Ensure the orchestration is registered in the `OrchestrationRegistry`
3. **Timeout errors**: Increase timeout values or check if external events are being raised
4. **Compilation errors**: Make sure you're using the latest version of Duroxide

### Getting Help

- Check the main README.md for overview information
- Look at the test files for working examples
- Review the architecture documentation in `docs/`
- Open an issue if you find bugs or need help

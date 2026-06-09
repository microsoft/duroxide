![Banner](duroxide_banner.jpg)
## duroxide

[![Crates.io](https://img.shields.io/crates/v/duroxide.svg)](https://crates.io/crates/duroxide)
[![Documentation](https://docs.rs/duroxide/badge.svg)](https://docs.rs/duroxide)

> Notice: This is a "preview" version.

**Duroxide is a lightweight, embeddable durable execution runtime for Rust.**

Write ordinary `async` Rust. Duroxide makes it *durable*: your code keeps
running across process crashes, restarts, and deployments. A workflow that
waits 30 days looks exactly like one that waits 30 milliseconds — and if the
process dies in the middle, it resumes right where it left off, without
re-running the work it already finished.

Inspired by the [Durable Task Framework](https://aka.ms/durabletask)
and [Temporal](https://temporal.io/).

### What you get

- **Durable by default** — every step is recorded; crashes resume from the last completed step.
- **Plain async Rust** — orchestrate with `.await`, control flow, and error handling you already know.
- **Embeddable** — runs in-process on Tokio. No separate server to operate.
- **Storage-agnostic** — a `Provider` trait backs persistence; a SQLite provider (in-memory or file) is built in.

### What you can build

- **Function chaining** — sequential steps where each depends on the last.
- **Fan-out / fan-in** — run many activities in parallel, then aggregate deterministically.
- **Human-in-the-loop** — wait for approvals, callbacks, or webhooks, then resume.
- **Durable timers** — sleep for minutes, hours, or days without holding a thread.
- **Saga compensation** — roll back prior steps on failure.
- **Built-in retries** — configurable backoff and per-attempt timeouts.
- **Cancellation** — in-flight activities receive cooperative cancellation signals.
- **Worker specialization** — route activities to dedicated pools with tags (e.g. `gpu`).
- **Durable KV** — per-instance key/value state that survives replay.

### Install

```toml
[dependencies]
duroxide = { version = "0.1", features = ["sqlite"] }  # With the bundled SQLite provider
# OR
duroxide = "0.1"  # Core only — bring your own Provider
```

## Examples

### Hello world

```rust
use std::sync::Arc;
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use duroxide::runtime::{self};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::providers::sqlite::SqliteProvider;

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let store = Arc::new(SqliteProvider::new("sqlite:./data.db", None).await?);

// An activity does the real work (I/O, HTTP, DB — anything).
let activities = ActivityRegistry::builder()
    .register("Hello", |_ctx: ActivityContext, name: String| async move {
        Ok(format!("Hello, {name}!"))
    })
    .build();

// An orchestration coordinates activities deterministically.
let orchestrations = OrchestrationRegistry::builder()
    .register("HelloWorld", |ctx: OrchestrationContext, name: String| async move {
        let result = ctx.schedule_activity("Hello", name).await?;
        Ok::<_, String>(result)
    })
    .build();

let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
let client = Client::new(store);

client.start_orchestration("inst-1", "HelloWorld", "Rust").await?;
match client.wait_for_orchestration("inst-1", std::time::Duration::from_secs(5)).await? {
    OrchestrationStatus::Completed { output } => assert_eq!(output, "Hello, Rust!"),
    other => panic!("unexpected: {other:?}"),
}

rt.shutdown(None).await;
# Ok(())
# }
```

### Surviving crashes

Each completed step is durably recorded, so a restart replays history and
resumes from exactly where it stopped.

```rust
use duroxide::OrchestrationContext;

async fn fulfill_order(ctx: OrchestrationContext, order_id: String) -> Result<String, String> {
    // Step 1: charge the card. When this completes, the result is written to history.
    let receipt = ctx.schedule_activity("ChargeCard", order_id.clone()).await?;

    // 💥 If the process crashes HERE, Duroxide replays on restart:
    //    `ChargeCard` is NOT charged again — its recorded result is returned
    //    instantly and execution continues from this exact line.

    // Step 2: wait for the warehouse. This can take hours or days; no thread is held.
    let _ = ctx.schedule_wait("WarehouseShipped").await;

    // 💥 Crash anywhere in this wait? On restart we resume still waiting —
    //    the timer/event state is durable.

    // Step 3: notify the customer.
    ctx.schedule_activity("SendShippingEmail", order_id).await?;

    Ok(receipt)
}
```

### Fan-out / fan-in

```rust
use duroxide::OrchestrationContext;

async fn fanout(ctx: OrchestrationContext) -> Vec<String> {
    let f1 = ctx.schedule_activity("Greet", "Gabbar");
    let f2 = ctx.schedule_activity("Greet", "Samba");
    // join resolves deterministically by history order, not polling order.
    ctx.join(vec![f1, f2])
        .await
        .into_iter()
        .map(|o| o.unwrap_or_else(|e| panic!("activity failed: {e}")))
        .collect()
}
```

### Timers and external events

```rust
use duroxide::{Either2, OrchestrationContext};

async fn wait_with_timeout(ctx: OrchestrationContext) -> String {
    let timer = ctx.schedule_timer(std::time::Duration::from_secs(60));
    let event = ctx.schedule_wait("Approval");
    // Use ctx.select2 — NOT tokio::select! — so the outcome is replay-safe.
    match ctx.select2(timer, event).await {
        Either2::First(()) => "timed out".to_string(),
        Either2::Second(data) => data,
    }
}
```

### Error handling and compensation

```rust
use duroxide::OrchestrationContext;

async fn with_recovery(ctx: OrchestrationContext) -> String {
    match ctx.schedule_activity("Fragile", "input").await {
        Ok(v) => v,
        Err(e) => {
            ctx.trace_warn(format!("fragile failed: {e}"));
            ctx.schedule_activity("Compensate", "").await.unwrap()
        }
    }
}
```

## How it works

Duroxide runs each orchestration **turn by turn**. Every operation gets a
correlation id; scheduling is recorded as a history event (e.g.
`ActivityScheduled`) and completions are matched back by id (e.g.
`ActivityCompleted`). On restart, the runtime **replays** that history to
rebuild in-memory state: completed steps return their recorded results without
re-executing, and the orchestration continues from the first unfinished step.

This is why orchestrations must be **deterministic** — they coordinate, they
don't do I/O. Activities are where side effects happen, and they run at most
once per logical step. A few consequences worth knowing:

- Use `ctx.join` / `ctx.select2` (not `tokio::join!` / `tokio::select!`) so
  concurrency resolves by history order, not wall-clock polling.
- Use `ctx.schedule_timer()`, `ctx.new_guid()`, `ctx.utcnow()` instead of
  `std::time`, `rand`, or `Uuid::new_v4()` directly.

📖 **For the full story** — how futures are made durable, the replay algorithm
step by step, and nondeterminism detection — read
[Durable Futures Internals](docs/durable-futures-internals.md).

## The Duroxide family

Several related projects share Duroxide's durable-execution model. Pick the one
that fits how you want to author and host your workflows:

- **[pg_durable](https://github.com/microsoft/pg_durable)** — PostgreSQL
  extension. Use this when you want durable pipelines and functions
  **directly in PostgreSQL**, with no other moving parts.
- **[duroxide](https://github.com/microsoft/duroxide)** _(this repo)_ —
  Rust durable-execution runtime. Use this when you want to author workflows in
  **Rust** and embed the runtime in your service. Multiple storage providers
  are available (SQLite built-in, PostgreSQL via
  [duroxide-pg](https://github.com/microsoft/duroxide-pg), or bring your own).
- **[duroxide-python](https://github.com/microsoft/duroxide-python)** — Python
  SDK over the duroxide runtime. Use this when you want to author workflows in
  **Python**.
- **[duroxide-node](https://github.com/microsoft/duroxide-node)** — Node.js /
  TypeScript SDK over the duroxide runtime. Use this when you want to author
  workflows in **JavaScript / TypeScript**.
- **[duroxide-pg](https://github.com/microsoft/duroxide-pg)** — PostgreSQL
  provider for the duroxide runtime. Plug this into duroxide / duroxide-python /
  duroxide-node when you want **PostgreSQL** as the durable store.

## Learn more

- **[Orchestration Guide](docs/ORCHESTRATION-GUIDE.md)** — the complete guide to writing workflows.
- **[Durable Futures Internals](docs/durable-futures-internals.md)** — how replay and durability work under the hood.
- **[Provider Implementation](docs/provider-implementation-guide.md)** / **[Provider Testing](docs/provider-testing-guide.md)** — build and test a custom storage backend.
- **[Observability Guide](docs/observability-guide.md)** — structured logging and metrics.
- **[AI Skills](docs/skills/)** — context files for AI assistants (Copilot, Cursor, etc.).
- **Examples** — `cargo run --example hello_world`, plus more in [`examples/`](examples/) and [`tests/e2e_samples.rs`](tests/e2e_samples.rs).

## Development

```bash
cargo build                          # Build
cargo test --all -- --nocapture      # Run all tests
./run-stress-tests.sh                # Stress tests (see STRESS_TEST_MONITORING.md)
```

See [CHANGELOG.md](CHANGELOG.md) for release notes and
[CONTRIBUTING.md](CONTRIBUTING.md) to get involved.

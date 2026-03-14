//! # Duroxide: Durable execution framework in Rust
//!
//! Duroxide is a framework for building reliable, long-running code based workflows that can survive
//! failures and restarts. For a deep dive into how durable execution works, see the
//! [Durable Futures Internals](https://github.com/affandar/duroxide/blob/main/docs/durable-futures-internals.md) documentation.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use duroxide::providers::sqlite::SqliteProvider;
//! use duroxide::runtime::registry::ActivityRegistry;
//! use duroxide::runtime::{self};
//! use duroxide::{ActivityContext, OrchestrationContext, OrchestrationRegistry, Client};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // 1. Create a storage provider
//! let store = Arc::new(SqliteProvider::new("sqlite:./data.db", None).await.unwrap());
//!
//! // 2. Register activities (your business logic)
//! let activities = ActivityRegistry::builder()
//!     .register("Greet", |_ctx: ActivityContext, name: String| async move {
//!         Ok(format!("Hello, {}!", name))
//!     })
//!     .build();
//!
//! // 3. Define your orchestration
//! let orchestration = |ctx: OrchestrationContext, name: String| async move {
//!     let greeting = ctx.schedule_activity("Greet", name).await?;

// Mutex poisoning indicates a panic in another thread - a critical error.
// All expect()/unwrap() calls on mutex locks in this module are intentional:
// poisoned mutexes should panic as they indicate corrupted state.
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
// Arc::clone() vs .clone() is a style preference - we use .clone() for brevity
#![allow(clippy::clone_on_ref_ptr)]
//!     Ok(greeting)
//! };
//!
//! // 4. Register and start the runtime
//! let orchestrations = OrchestrationRegistry::builder()
//!     .register("HelloWorld", orchestration)
//!     .build();
//!
//! let rt = runtime::Runtime::start_with_store(
//!     store.clone(), activities, orchestrations
//! ).await;
//!
//! // 5. Create a client and start an orchestration instance
//! let client = Client::new(store.clone());
//! client.start_orchestration("inst-1", "HelloWorld", "World").await?;
//! let result = client.wait_for_orchestration("inst-1", std::time::Duration::from_secs(5)).await
//!     .map_err(|e| format!("Wait error: {:?}", e))?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Key Concepts
//!
//! - **Orchestrations**: Long-running workflows written as async functions (coordination logic)
//! - **Activities**: Single-purpose work units (can do anything - DB, API, polling, etc.)
//!   - Supports long-running activities via automatic lock renewal (minutes to hours)
//! - **Timers**: Use `ctx.schedule_timer(ms)` for orchestration-level delays and timeouts
//! - **Deterministic Replay**: Orchestrations are replayed from history to ensure consistency
//! - **Durable Futures**: Composable futures for activities, timers, and external events
//! - **ContinueAsNew (Multi-Execution)**: An orchestration can end the current execution and
//!   immediately start a new one with fresh input. Each execution has its own isolated history
//!   that starts with `OrchestrationStarted { event_id: 1 }`.
//!
//! ## ⚠️ Important: Orchestrations vs Activities
//!
//! **Orchestrations = Coordination (control flow, business logic)**
//! **Activities = Execution (single-purpose work units)**
//!
//! ```rust,no_run
//! # use duroxide::OrchestrationContext;
//! # use std::time::Duration;
//! # async fn example(ctx: OrchestrationContext) -> Result<(), String> {
//! // ✅ CORRECT: Orchestration-level delay using timer
//! ctx.schedule_timer(Duration::from_secs(5)).await;  // Wait 5 seconds
//!
//! // ✅ ALSO CORRECT: Activity can poll/sleep as part of its work
//! // Example: Activity that provisions a VM and polls for readiness
//! // activities.register("ProvisionVM", |config| async move {
//! //     let vm = create_vm(config).await?;
//! //     while !vm_ready(&vm).await {
//! //         tokio::time::sleep(Duration::from_secs(5)).await;  // ✅ OK - part of provisioning
//! //     }
//! //     Ok(vm.id)
//! // });
//!
//! // ❌ WRONG: Activity that ONLY sleeps (use timer instead)
//! // ctx.schedule_activity("Sleep5Seconds", "").await;
//! # Ok(())
//! # }
//! ```
//!
//! **Put in Activities (single-purpose execution units):**
//! - Database operations
//! - API calls (can include retries/polling)
//! - Data transformations
//! - File I/O
//! - VM provisioning (with internal polling)
//!
//! **Put in Orchestrations (coordination and business logic):**
//! - Control flow (if/else, match, loops)
//! - Business decisions
//! - Multi-step workflows
//! - Error handling and compensation
//! - Timeouts and deadlines (use timers)
//! - Waiting for external events
//!
//! ## ContinueAsNew (Multi-Execution) Semantics
//!
//! ContinueAsNew (CAN) allows an orchestration to end its current execution and start a new
//! one with fresh input (useful for loops, pagination, long-running workflows).
//!
//! - Orchestration calls `ctx.continue_as_new(new_input)`
//! - Runtime stamps `OrchestrationContinuedAsNew` in the CURRENT execution's history
//! - Runtime enqueues a `WorkItem::ContinueAsNew`
//! - When processing that work item, the runtime starts a NEW execution with:
//!   - `execution_id = previous_execution_id + 1`
//!   - `existing_history = []` (fresh history)
//!   - `OrchestrationStarted { event_id: 1, input = new_input }` is stamped automatically
//! - Each execution's history is independent; `duroxide::Client::read_execution_history(instance, id)`
//!   returns events for that execution only
//!
//! Provider responsibilities are strictly storage-level (see below). The runtime owns all
//! orchestration semantics, including execution boundaries and starting the new execution.
//!
//! ## Provider Responsibilities (At a Glance)
//!
//! Providers are pure storage abstractions. The runtime computes orchestration semantics
//! and passes explicit instructions to the provider.
//!
//! - `fetch_orchestration_item()`
//!   - Return a locked batch of work for ONE instance
//!   - Include full history for the CURRENT `execution_id`
//!   - Do NOT create/synthesize new executions here (even for ContinueAsNew)
//!
//! - `ack_orchestration_item(lock_token, execution_id, history_delta, ..., metadata)`
//!   - Atomic commit of one orchestration turn
//!   - Idempotently `INSERT OR IGNORE` execution row for the explicit `execution_id`
//!   - `UPDATE instances.current_execution_id = MAX(current_execution_id, execution_id)`
//!   - Append `history_delta` to the specified execution
//!   - Update `executions.status` and `executions.output` from `metadata` (no event inspection)
//!
//! - Worker/Timer queues
//!   - Peek-lock semantics (dequeue with lock token; ack by deleting)
//!   - Automatic lock renewal for long-running activities (no configuration needed)
//!   - Orchestrator, Worker, Timer queues are independent but committed atomically with history
//!
//! See `docs/provider-implementation-guide.md` and `src/providers/sqlite.rs` for a complete,
//! production-grade provider implementation.
//!
//! ## Simplified API
//!
//! All schedule methods return typed futures that can be awaited directly:
//!
//! ```rust,no_run
//! # use duroxide::OrchestrationContext;
//! # use std::time::Duration;
//! # async fn example(ctx: OrchestrationContext) -> Result<(), String> {
//! // Activities return Result<String, String>
//! let result = ctx.schedule_activity("Task", "input").await?;
//!
//! // Timers return ()
//! ctx.schedule_timer(Duration::from_secs(5)).await;
//!
//! // External events return String
//! let event = ctx.schedule_wait("Event").await;
//!
//! // Sub-orchestrations return Result<String, String>
//! let sub_result = ctx.schedule_sub_orchestration("Sub", "input").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Common Patterns
//!
//! ### Function Chaining
//! ```rust,no_run
//! # use duroxide::OrchestrationContext;
//! async fn chain_example(ctx: OrchestrationContext) -> Result<String, String> {
//!     let step1 = ctx.schedule_activity("Step1", "input").await?;
//!     let step2 = ctx.schedule_activity("Step2", &step1).await?;
//!     Ok(step2)
//! }
//! ```
//!
//! ### Fan-Out/Fan-In
//! ```rust,no_run
//! # use duroxide::OrchestrationContext;
//! async fn fanout_example(ctx: OrchestrationContext) -> Vec<String> {
//!     let futures = vec![
//!         ctx.schedule_activity("Process", "item1"),
//!         ctx.schedule_activity("Process", "item2"),
//!         ctx.schedule_activity("Process", "item3"),
//!     ];
//!     let results = ctx.join(futures).await;
//!     results.into_iter().filter_map(|r| r.ok()).collect()
//! }
//! ```
//!
//! ### Human-in-the-Loop (Timeout Pattern)
//! ```rust,no_run
//! # use duroxide::{OrchestrationContext, Either2};
//! # use std::time::Duration;
//! async fn approval_example(ctx: OrchestrationContext) -> String {
//!     let timeout = ctx.schedule_timer(Duration::from_secs(30));
//!     let approval = ctx.schedule_wait("ApprovalEvent");
//!     
//!     match ctx.select2(approval, timeout).await {
//!         Either2::First(data) => data,
//!         Either2::Second(()) => "timeout".to_string(),
//!     }
//! }
//! ```
//!
//! ### Delays and Timeouts
//! ```rust,no_run
//! # use duroxide::{OrchestrationContext, Either2};
//! # use std::time::Duration;
//! async fn delay_example(ctx: OrchestrationContext) -> Result<String, String> {
//!     // Use timer for orchestration-level delays
//!     ctx.schedule_timer(Duration::from_secs(5)).await;
//!     
//!     // Process after delay
//!     let result = ctx.schedule_activity("ProcessData", "input").await?;
//!     Ok(result)
//! }
//!
//! async fn timeout_example(ctx: OrchestrationContext) -> Result<String, String> {
//!     // Race work against timeout
//!     let work = ctx.schedule_activity("SlowOperation", "input");
//!     let timeout = ctx.schedule_timer(Duration::from_secs(30));
//!     
//!     match ctx.select2(work, timeout).await {
//!         Either2::First(result) => result,
//!         Either2::Second(()) => Err("Operation timed out".to_string()),
//!     }
//! }
//! ```
//!
//! ### Fan-Out/Fan-In with Error Handling
//! ```rust,no_run
//! # use duroxide::OrchestrationContext;
//! async fn fanout_with_errors(ctx: OrchestrationContext, items: Vec<String>) -> Result<Vec<String>, String> {
//!     // Schedule all work in parallel
//!     let futures: Vec<_> = items.iter()
//!         .map(|item| ctx.schedule_activity("ProcessItem", item.clone()))
//!         .collect();
//!     
//!     // Wait for all to complete (deterministic order preserved)
//!     let results = ctx.join(futures).await;
//!     
//!     // Process results with error handling
//!     let mut successes = Vec::new();
//!     for result in results {
//!         match result {
//!             Ok(value) => successes.push(value),
//!             Err(e) => {
//!                 // Log error but continue processing other items
//!                 ctx.trace_error(format!("Item processing failed: {e}"));
//!             }
//!         }
//!     }
//!     
//!     Ok(successes)
//! }
//! ```
//!
//! ### Retry Pattern
//! ```rust,no_run
//! # use duroxide::{OrchestrationContext, RetryPolicy, BackoffStrategy};
//! # use std::time::Duration;
//! async fn retry_example(ctx: OrchestrationContext) -> Result<String, String> {
//!     // Retry with linear backoff: 5 attempts, delay increases linearly (1s, 2s, 3s, 4s)
//!     let result = ctx.schedule_activity_with_retry(
//!         "UnreliableOperation",
//!         "input",
//!         RetryPolicy::new(5)
//!             .with_backoff(BackoffStrategy::Linear {
//!                 base: Duration::from_secs(1),
//!                 max: Duration::from_secs(10),
//!             }),
//!     ).await?;
//!     
//!     Ok(result)
//! }
//! ```
//!
//! ## Examples
//!
//! See the `examples/` directory for complete, runnable examples:
//! - `hello_world.rs` - Basic orchestration setup
//! - `fan_out_fan_in.rs` - Parallel processing pattern with error handling
//! - `timers_and_events.rs` - Human-in-the-loop workflows with timeouts
//! - `delays_and_timeouts.rs` - Correct usage of timers for delays and timeouts
//! - `with_observability.rs` - Using observability features (tracing, metrics)
//! - `metrics_cli.rs` - Querying system metrics via CLI
//!
//! Run examples with: `cargo run --example <name>`
//!
//! ## Architecture
//!
//! This crate provides:
//! - **Public data model**: `Event`, `Action` for history and decisions
//! - **Orchestration driver**: `run_turn`, `run_turn_with`, and `Executor`
//! - **OrchestrationContext**: Schedule activities, timers, and external events
//! - **Deterministic futures**: `schedule_*()` return standard `Future`s that can be composed with `join`/`select`
//! - **Runtime**: In-process execution engine with dispatchers and workers
//! - **Providers**: Pluggable storage backends (filesystem, in-memory)
//!
//! ### End-to-End System Architecture
//!
//! ```text
//! +-------------------------------------------------------------------------+
//! |                           Application Layer                             |
//! +-------------------------------------------------------------------------+
//! |                                                                         |
//! |  +--------------+         +------------------------------------+        |
//! |  |    Client    |-------->|  start_orchestration()             |        |
//! |  |              |         |  raise_event()                     |        |
//! |  |              |         |  wait_for_orchestration()          |        |
//! |  +--------------+         +------------------------------------+        |
//! |                                                                         |
//! +-------------------------------------------------------------------------+
//!                                    |
//!                                    v
//! +-------------------------------------------------------------------------+
//! |                            Runtime Layer                                |
//! +-------------------------------------------------------------------------+
//! |                                                                         |
//! |  +-------------------------------------------------------------------+  |
//! |  |                         Runtime                                   |  |
//! |  |  +----------------------+         +----------------------+        |  |
//! |  |  | Orchestration        |         | Work                 |        |  |
//! |  |  | Dispatcher           |         | Dispatcher           |        |  |
//! |  |  | (N concurrent)       |         | (N concurrent)       |        |  |
//! |  |  +----------+-----------+         +----------+-----------+        |  |
//! |  |             |                                |                    |  |
//! |  |             | Processes turns                | Executes activities|  |
//! |  |             |                                |                    |  |
//! |  +-------------+--------------------------------+--------------------+  |
//! |                |                                |                       |
//! |  +-------------v--------------------------------v--------------------+  |
//! |  |  OrchestrationRegistry: maps names -> orchestration handlers     |  |
//! |  +-------------------------------------------------------------------+  |
//! |                                                                         |
//! |  +-------------------------------------------------------------------+  |
//! |  |  ActivityRegistry: maps names -> activity handlers               |  |
//! |  +-------------------------------------------------------------------+  |
//! |                                                                         |
//! +-------------------------------------------------------------------------+
//!                |                                |
//!                | Fetches work items             | Fetches work items
//!                | (peek-lock)                    | (peek-lock)
//!                v                                v
//! +-------------------------------------------------------------------------+
//! |                          Provider Layer                                 |
//! +-------------------------------------------------------------------------+
//! |                                                                         |
//! |  +----------------------------+    +----------------------------+       |
//! |  |  Orchestrator Queue        |    |  Worker Queue              |       |
//! |  |  - StartOrchestration      |    |  - ActivityExecute         |       |
//! |  |  - ActivityCompleted       |    |                            |       |
//! |  |  - ActivityFailed          |    |                            |       |
//! |  |  - TimerFired (delayed)    |    |                            |       |
//! |  |  - ExternalRaised          |    |                            |       |
//! |  |  - ContinueAsNew           |    |                            |       |
//! |  +----------------------------+    +----------------------------+       |
//! |                                                                         |
//! |  +-------------------------------------------------------------------+  |
//! |  |                     Provider (Storage)                            |  |
//! |  |  - History (Events per instance/execution)                        |  |
//! |  |  - Instance metadata                                              |  |
//! |  |  - Execution metadata                                             |  |
//! |  |  - Instance locks (peek-lock semantics)                           |  |
//! |  |  - Queue management (enqueue/dequeue with visibility)             |  |
//! |  +-------------------------------------------------------------------+  |
//! |                                                                         |
//! |  +-------------------------------------------------------------------+  |
//! |  |                Storage Backend (SQLite, etc.)                     |  |
//! |  +-------------------------------------------------------------------+  |
//! |                                                                         |
//! +-------------------------------------------------------------------------+
//!
//! ### Execution Flow
//!
//! 1. **Client** starts orchestration → enqueues `StartOrchestration` to orchestrator queue
//! 2. **OrchestrationDispatcher** fetches work item (peek-lock), loads history from Provider
//! 3. **Runtime** calls user's orchestration function with `OrchestrationContext`
//! 4. **Orchestration** schedules activities/timers → Runtime appends `Event`s to history
//! 5. **Runtime** enqueues `ActivityExecute` to worker queue, `TimerFired` (delayed) to orchestrator queue
//! 6. **WorkDispatcher** fetches activity work item, executes via `ActivityRegistry`
//! 7. **Activity** completes → enqueues `ActivityCompleted`/`ActivityFailed` to orchestrator queue
//! 8. **OrchestrationDispatcher** processes completion → next orchestration turn
//! 9. **Runtime** atomically commits history + queue changes via `ack_orchestration_item()`
//!
//! All operations are deterministic and replayable from history.
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

// Public orchestration primitives and executor

pub mod client;
pub mod runtime;
// Re-export descriptor type for public API ergonomics
pub use runtime::OrchestrationDescriptor;
pub mod providers;

#[cfg(feature = "provider-test")]
pub mod provider_validations;

#[cfg(feature = "provider-test")]
pub mod provider_validation;

#[cfg(feature = "provider-test")]
pub mod provider_stress_tests;

#[cfg(feature = "provider-test")]
pub mod provider_stress_test;

// Re-export key runtime types for convenience
pub use client::{Client, ClientError};
pub use runtime::{
    OrchestrationHandler, OrchestrationRegistry, OrchestrationRegistryBuilder, OrchestrationStatus, RuntimeOptions,
};

// Re-export management types for convenience
pub use providers::{
    ExecutionInfo, InstanceInfo, ProviderAdmin, QueueDepths, ScheduledActivityIdentifier, SessionFetchConfig,
    SystemMetrics, TagFilter,
};

// Re-export capability filtering types
pub use providers::{DispatcherCapabilityFilter, SemverRange, current_build_version};

// Re-export deletion/pruning types for Client API users
pub use providers::{DeleteInstanceResult, InstanceFilter, InstanceTree, PruneOptions, PruneResult};

// Type aliases for improved readability and maintainability
/// Shared reference to a Provider implementation
pub type ProviderRef = Arc<dyn providers::Provider>;

/// Shared reference to an OrchestrationHandler
pub type OrchestrationHandlerRef = Arc<dyn runtime::OrchestrationHandler>;

// ============================================================================
// Heterogeneous Select Result Types
// ============================================================================

// ============================================================================
// Schedule Kind and DurableFuture (Cancellation Support)
// ============================================================================

/// Identifies the kind of scheduled work for cancellation purposes.
///
/// When a `DurableFuture` is dropped without completing, the runtime uses
/// this discriminator to determine how to cancel the underlying work:
/// - **Activity**: Lock stealing via provider (DELETE from worker_queue)
/// - **Timer**: No-op (virtual construct, no external state)
/// - **ExternalWait**: No-op (virtual construct, no external state)  
/// - **SubOrchestration**: Enqueue `CancelInstance` work item for child
#[derive(Debug, Clone)]
pub enum ScheduleKind {
    /// A scheduled activity execution
    Activity {
        /// Activity name for debugging/logging
        name: String,
    },
    /// A durable timer
    Timer,
    /// Waiting for an external event
    ExternalWait {
        /// Event name for debugging/logging
        event_name: String,
    },
    /// Waiting for a persistent external event (mailbox semantics)
    QueueDequeue {
        /// Event name for debugging/logging
        event_name: String,
    },
    /// A sub-orchestration
    SubOrchestration {
        /// Token for this schedule (used to look up resolved instance ID)
        token: u64,
    },
}

/// A wrapper around scheduled futures that supports cancellation on drop.
///
/// When a `DurableFuture` is dropped without completing (e.g., as a select loser,
/// or when going out of scope without being awaited), the underlying scheduled work
/// is cancelled:
///
/// - **Activities**: Lock stealing via provider (removes from worker queue)
/// - **Sub-orchestrations**: `CancelInstance` work item enqueued for child
/// - **Timers/External waits**: No-op (virtual constructs with no external state)
///
/// # Examples
///
/// ```rust,no_run
/// # use duroxide::OrchestrationContext;
/// # use std::time::Duration;
/// # async fn example(ctx: OrchestrationContext) -> Result<String, String> {
/// // Activity scheduled - if timer wins, activity gets cancelled
/// let activity = ctx.schedule_activity("SlowWork", "input");
/// let timeout = ctx.schedule_timer(Duration::from_secs(5));
///
/// match ctx.select2(activity, timeout).await {
///     duroxide::Either2::First(result) => result,
///     duroxide::Either2::Second(()) => Err("Timed out - activity cancelled".to_string()),
/// }
/// # }
/// ```
///
/// # Drop Semantics
///
/// Unlike regular Rust futures which are inert on drop, `DurableFuture` has
/// meaningful drop semantics similar to `File` (closes on drop) or `MutexGuard`
/// (releases lock on drop). This is intentional - we want unobserved scheduled
/// work to be cancelled rather than leaked.
///
/// **Note:** Using `std::mem::forget()` on a `DurableFuture` will bypass
/// cancellation, causing the scheduled work to run but its result to be lost.
pub struct DurableFuture<T> {
    /// Token assigned at creation (before schedule_id is known)
    token: u64,
    /// What kind of schedule this represents
    kind: ScheduleKind,
    /// Reference to context for cancellation registration
    ctx: OrchestrationContext,
    /// Whether the future has completed (to skip cancellation)
    completed: bool,
    /// The underlying future
    inner: std::pin::Pin<Box<dyn Future<Output = T> + Send>>,
}

impl<T: Send + 'static> DurableFuture<T> {
    /// Create a new `DurableFuture` wrapping an inner future.
    fn new(
        token: u64,
        kind: ScheduleKind,
        ctx: OrchestrationContext,
        inner: impl Future<Output = T> + Send + 'static,
    ) -> Self {
        Self {
            token,
            kind,
            ctx,
            completed: false,
            inner: Box::pin(inner),
        }
    }

    /// Transform the output of this `DurableFuture` while preserving its
    /// identity for cancellation, `join`, and `select` composability.
    ///
    /// The mapping function runs synchronously after the underlying future
    /// completes. Cancellation semantics are fully preserved: dropping the
    /// returned `DurableFuture` cancels the original scheduled work.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) -> Result<String, String> {
    /// // Map an activity result to extract a field
    /// let length = ctx.schedule_activity("Greet", "World")
    ///     .map(|r| r.map(|s| s.len().to_string()));
    /// let len_result = length.await?;
    /// # Ok(len_result)
    /// # }
    /// ```
    pub fn map<U: Send + 'static>(self, f: impl FnOnce(T) -> U + Send + 'static) -> DurableFuture<U> {
        // Transfer ownership of cancellation guard to the new DurableFuture.
        // We must defuse `self` so its Drop doesn't fire cancellation.
        let token = self.token;
        let kind = self.kind.clone();
        let ctx = self.ctx.clone();

        // Defuse: take the inner future out without running Drop.
        let inner = unsafe {
            let inner = std::ptr::read(&self.inner);
            // Prevent Drop from running on the original (would cancel the token)
            std::mem::forget(self);
            inner
        };

        let mapped = async move {
            let value = inner.await;
            f(value)
        };

        DurableFuture::new(token, kind, ctx, mapped)
    }

    /// Set a routing tag on this scheduled activity.
    ///
    /// Tags direct activities to specialized workers. A worker configured with
    /// [`crate::providers::TagFilter::tags`]`(["gpu"])` will only process activities
    /// tagged `"gpu"`.
    ///
    /// This method uses **mutate-after-emit**: the action has already been emitted to
    /// the context's action list, and `with_tag` reaches back to modify it in place.
    /// This is safe because actions are not drained until the replay engine polls
    /// the orchestration future (which hasn't happened yet — we're still inside the
    /// user's `async` block).
    ///
    /// # Panics
    ///
    /// Panics if called on a non-activity `DurableFuture` (e.g., timer or sub-orchestration).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) -> Result<String, String> {
    /// let result = ctx.schedule_activity("CompileRelease", "repo-url")
    ///     .with_tag("build-machine")
    ///     .await?;
    /// # Ok(result)
    /// # }
    /// ```
    pub fn with_tag(self, tag: impl Into<String>) -> Self {
        match &self.kind {
            ScheduleKind::Activity { .. } => {}
            other => panic!("with_tag() can only be called on activity futures, got {:?}", other),
        }

        let tag_value = tag.into();
        let mut inner = self.ctx.inner.lock().expect("Mutex should not be poisoned");
        // Find the action with our token and set its tag.
        // The token must exist: it was just emitted by schedule_activity_internal
        // and emitted_actions is only drained when the replay engine polls
        // (which hasn't happened yet — we're still in the user's async block).
        let mut found = false;
        for (token, action) in inner.emitted_actions.iter_mut() {
            if *token == self.token {
                match action {
                    Action::CallActivity { tag, .. } => {
                        *tag = Some(tag_value);
                    }
                    _ => panic!("Token matched but action is not CallActivity"),
                }
                found = true;
                break;
            }
        }
        assert!(
            found,
            "with_tag(): token {} not found in emitted_actions — actions were drained before with_tag() was called",
            self.token
        );
        drop(inner);
        self
    }
}

impl<T> Future for DurableFuture<T> {
    type Output = T;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<T> {
        match self.inner.as_mut().poll(cx) {
            Poll::Ready(value) => {
                self.completed = true;
                Poll::Ready(value)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for DurableFuture<T> {
    fn drop(&mut self) {
        if !self.completed {
            // Future dropped without completing - trigger cancellation.
            // Note: During dehydration (TurnResult::Continue), the orchestration future
            // is dropped after collect_cancelled_from_context() has already run, so these
            // cancellations go into a context that's about to be dropped. This is safe
            // because the next turn creates a fresh context.
            self.ctx.mark_token_cancelled(self.token, &self.kind);
        }
    }
}

// DurableFuture is Send if T is Send (inner is already Send-boxed)
unsafe impl<T: Send> Send for DurableFuture<T> {}

/// Result type for `select2` - represents which of two futures completed first.
///
/// Use this when racing two futures with different output types:
/// ```rust,no_run
/// # use duroxide::{OrchestrationContext, Either2};
/// # use std::time::Duration;
/// # async fn example(ctx: OrchestrationContext) -> Result<String, String> {
/// let activity = ctx.schedule_activity("Work", "input");
/// let timeout = ctx.schedule_timer(Duration::from_secs(30));
///
/// match ctx.select2(activity, timeout).await {
///     Either2::First(result) => result,
///     Either2::Second(()) => Err("Timed out".to_string()),
/// }
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Either2<A, B> {
    /// First future completed first
    First(A),
    /// Second future completed first  
    Second(B),
}

impl<A, B> Either2<A, B> {
    /// Returns true if this is the First variant
    pub fn is_first(&self) -> bool {
        matches!(self, Either2::First(_))
    }

    /// Returns true if this is the Second variant
    pub fn is_second(&self) -> bool {
        matches!(self, Either2::Second(_))
    }

    /// Returns the index of the winner (0 for First, 1 for Second)
    pub fn index(&self) -> usize {
        match self {
            Either2::First(_) => 0,
            Either2::Second(_) => 1,
        }
    }
}

impl<T> Either2<T, T> {
    /// For homogeneous Either2 (both types are the same), extract as (index, value).
    /// This is useful for migration from the old `(usize, T)` return type.
    pub fn into_tuple(self) -> (usize, T) {
        match self {
            Either2::First(v) => (0, v),
            Either2::Second(v) => (1, v),
        }
    }
}

/// Result type for `select3` - represents which of three futures completed first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Either3<A, B, C> {
    /// First future completed first
    First(A),
    /// Second future completed first
    Second(B),
    /// Third future completed first
    Third(C),
}

impl<A, B, C> Either3<A, B, C> {
    /// Returns the index of the winner (0 for First, 1 for Second, 2 for Third)
    pub fn index(&self) -> usize {
        match self {
            Either3::First(_) => 0,
            Either3::Second(_) => 1,
            Either3::Third(_) => 2,
        }
    }
}

impl<T> Either3<T, T, T> {
    /// For homogeneous Either3 (all types are the same), extract as (index, value).
    pub fn into_tuple(self) -> (usize, T) {
        match self {
            Either3::First(v) => (0, v),
            Either3::Second(v) => (1, v),
            Either3::Third(v) => (2, v),
        }
    }
}

// Reserved prefix for built-in system activities.
// User-registered activities cannot use names starting with this prefix.
pub(crate) const SYSCALL_ACTIVITY_PREFIX: &str = "__duroxide_syscall:";

// Built-in system activity names (constructed from prefix)
pub(crate) const SYSCALL_ACTIVITY_NEW_GUID: &str = "__duroxide_syscall:new_guid";
pub(crate) const SYSCALL_ACTIVITY_UTC_NOW_MS: &str = "__duroxide_syscall:utc_now_ms";
pub(crate) const SYSCALL_ACTIVITY_GET_KV_VALUE: &str = "__duroxide_syscall:get_kv_value";

use crate::_typed_codec::Codec;
// LogLevel is now defined locally in this file
use serde::{Deserialize, Serialize};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

// Internal codec utilities for typed I/O (kept private; public API remains ergonomic)
mod _typed_codec {
    use serde::{Serialize, de::DeserializeOwned};
    use serde_json::Value;
    pub trait Codec {
        fn encode<T: Serialize>(v: &T) -> Result<String, String>;
        fn decode<T: DeserializeOwned>(s: &str) -> Result<T, String>;
    }
    pub struct Json;
    impl Codec for Json {
        fn encode<T: Serialize>(v: &T) -> Result<String, String> {
            // If the value is a JSON string, return raw content to preserve historic behavior
            match serde_json::to_value(v) {
                Ok(Value::String(s)) => Ok(s),
                Ok(val) => serde_json::to_string(&val).map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            }
        }
        fn decode<T: DeserializeOwned>(s: &str) -> Result<T, String> {
            // Try parse as JSON first
            match serde_json::from_str::<T>(s) {
                Ok(v) => Ok(v),
                Err(_) => {
                    // Fallback: treat raw string as JSON string value
                    let val = Value::String(s.to_string());
                    serde_json::from_value(val).map_err(|e| e.to_string())
                }
            }
        }
    }
}

/// Initial execution ID for new orchestration instances.
/// All orchestrations start with execution_id = 1.
pub const INITIAL_EXECUTION_ID: u64 = 1;

/// Initial event ID for new executions.
/// The first event (OrchestrationStarted) always has event_id = 1.
pub const INITIAL_EVENT_ID: u64 = 1;

// =============================================================================
// Sub-orchestration instance ID conventions
// =============================================================================

/// Prefix for auto-generated sub-orchestration instance IDs.
/// IDs starting with this prefix will have parent prefix added: `{parent}::{sub::N}`
pub const SUB_ORCH_AUTO_PREFIX: &str = "sub::";

/// Prefix for placeholder instance IDs before event ID assignment.
/// These are replaced with `sub::{event_id}` during action processing.
pub(crate) const SUB_ORCH_PENDING_PREFIX: &str = "sub::pending_";

/// Determine if a sub-orchestration instance ID is auto-generated (needs parent prefix).
///
/// Auto-generated IDs start with "sub::" and will have the parent instance prefixed
/// to create a globally unique ID: `{parent_instance}::{child_instance}`.
///
/// Explicit IDs (those not starting with "sub::") are used exactly as provided.
#[inline]
pub fn is_auto_generated_sub_orch_id(instance: &str) -> bool {
    instance.starts_with(SUB_ORCH_AUTO_PREFIX)
}

/// Build the full child instance ID, adding parent prefix only for auto-generated IDs.
///
/// - Auto-generated IDs (starting with "sub::"): `{parent}::{child}` (e.g., `parent-1::sub::5`)
/// - Explicit IDs: used exactly as provided (e.g., `my-custom-child-id`)
#[inline]
pub fn build_child_instance_id(parent_instance: &str, child_instance: &str) -> String {
    if is_auto_generated_sub_orch_id(child_instance) {
        format!("{parent_instance}::{child_instance}")
    } else {
        child_instance.to_string()
    }
}

/// Structured error details for orchestration failures.
///
/// Errors are categorized into three types for proper metrics and logging:
/// - **Infrastructure**: Provider failures, data corruption (abort turn, never reach user code)
/// - **Configuration**: Deployment issues like unregistered activities, nondeterminism (abort turn)
/// - **Application**: Business logic failures (flow through normal orchestration code)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErrorDetails {
    /// Infrastructure failure (provider errors, data corruption).
    /// These errors abort orchestration execution and never reach user code.
    Infrastructure {
        operation: String,
        message: String,
        retryable: bool,
    },

    /// Configuration error (unregistered orchestrations/activities, nondeterminism).
    /// These errors abort orchestration execution and never reach user code.
    Configuration {
        kind: ConfigErrorKind,
        resource: String,
        message: Option<String>,
    },

    /// Application error (business logic failures).
    /// These are the ONLY errors that orchestration code sees.
    Application {
        kind: AppErrorKind,
        message: String,
        retryable: bool,
    },

    /// Poison message error - message exceeded max fetch attempts.
    ///
    /// This indicates a message that repeatedly fails to process.
    /// Could be caused by:
    /// - Malformed message data causing deserialization failures
    /// - Message triggering bugs that crash the worker
    /// - Transient infrastructure issues that became permanent
    /// - Application code bugs triggered by specific input patterns
    Poison {
        /// Number of times the message was fetched
        attempt_count: u32,
        /// Maximum allowed attempts
        max_attempts: u32,
        /// Message type and identity
        message_type: PoisonMessageType,
        /// The poisoned message content (serialized JSON for debugging)
        message: String,
    },
}

/// Poison message type identification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PoisonMessageType {
    /// Orchestration work item batch
    Orchestration { instance: String, execution_id: u64 },
    /// Activity execution
    Activity {
        instance: String,
        execution_id: u64,
        activity_name: String,
        activity_id: u64,
    },
    /// History deserialization failure (e.g., unknown event types from a newer duroxide version)
    FailedDeserialization {
        instance: String,
        execution_id: u64,
        error: String,
    },
}

/// Configuration error kinds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConfigErrorKind {
    Nondeterminism,
}

/// Application error kinds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AppErrorKind {
    ActivityFailed,
    OrchestrationFailed,
    Panicked,
    Cancelled { reason: String },
}

impl ErrorDetails {
    /// Get failure category for metrics/logging.
    pub fn category(&self) -> &'static str {
        match self {
            ErrorDetails::Infrastructure { .. } => "infrastructure",
            ErrorDetails::Configuration { .. } => "configuration",
            ErrorDetails::Application { .. } => "application",
            ErrorDetails::Poison { .. } => "poison",
        }
    }

    /// Check if failure is retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            ErrorDetails::Infrastructure { retryable, .. } => *retryable,
            ErrorDetails::Application { retryable, .. } => *retryable,
            ErrorDetails::Configuration { .. } => false,
            ErrorDetails::Poison { .. } => false, // Never retryable
        }
    }

    /// Get display message for logging/UI (backward compatible format).
    pub fn display_message(&self) -> String {
        match self {
            ErrorDetails::Infrastructure { operation, message, .. } => {
                format!("infrastructure:{operation}: {message}")
            }
            ErrorDetails::Configuration {
                kind,
                resource,
                message,
            } => match kind {
                ConfigErrorKind::Nondeterminism => message
                    .as_ref()
                    .map(|m| format!("nondeterministic: {m}"))
                    .unwrap_or_else(|| format!("nondeterministic in {resource}")),
            },
            ErrorDetails::Application { kind, message, .. } => match kind {
                AppErrorKind::Cancelled { reason } => format!("canceled: {reason}"),
                AppErrorKind::Panicked => format!("orchestration panicked: {message}"),
                _ => message.clone(),
            },
            ErrorDetails::Poison {
                attempt_count,
                max_attempts,
                message_type,
                ..
            } => match message_type {
                PoisonMessageType::Orchestration { instance, .. } => {
                    format!("poison: orchestration {instance} exceeded {attempt_count} attempts (max {max_attempts})")
                }
                PoisonMessageType::Activity {
                    activity_name,
                    activity_id,
                    ..
                } => {
                    format!(
                        "poison: activity {activity_name}#{activity_id} exceeded {attempt_count} attempts (max {max_attempts})"
                    )
                }
                PoisonMessageType::FailedDeserialization { instance, error, .. } => {
                    format!(
                        "poison: orchestration {instance} history deserialization failed after {attempt_count} attempts (max {max_attempts}): {error}"
                    )
                }
            },
        }
    }
}

/// Unified event with common metadata and type-specific payload.
///
/// All events have common fields (event_id, source_event_id, instance_id, etc.)
/// plus type-specific data in the `kind` field.
///
/// Events are append-only history entries persisted by a provider and consumed during replay.
/// The `event_id` is a monotonically increasing position in history.
/// Scheduling and completion events are linked via `source_event_id`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    /// Sequential position in history (monotonically increasing per execution)
    pub event_id: u64,

    /// For completion events: references the scheduling event this completes.
    /// None for lifecycle events (OrchestrationStarted, etc.) and scheduling events.
    /// Some(id) for completion events (ActivityCompleted, TimerFired, etc.).
    pub source_event_id: Option<u64>,

    /// Instance this event belongs to.
    /// Denormalized from DB key for self-contained events.
    pub instance_id: String,

    /// Execution this event belongs to.
    /// Denormalized from DB key for self-contained events.
    pub execution_id: u64,

    /// Timestamp when event was created (milliseconds since Unix epoch).
    pub timestamp_ms: u64,

    /// Crate semver version that generated this event.
    /// Format: "0.1.0", "0.2.0", etc.
    pub duroxide_version: String,

    /// Event type and associated data.
    #[serde(flatten)]
    pub kind: EventKind,
}

/// Event-specific payloads.
///
/// Common fields have been extracted to the Event struct:
/// - event_id: moved to Event.event_id
/// - source_event_id: moved to Event.source_event_id (`Option<u64>`)
/// - execution_id: moved to Event.execution_id (was in 4 variants)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum EventKind {
    /// Orchestration instance was created and started by name with input.
    /// Version is required; parent linkage is present when this is a child orchestration.
    #[serde(rename = "OrchestrationStarted")]
    OrchestrationStarted {
        name: String,
        version: String,
        input: String,
        parent_instance: Option<String>,
        parent_id: Option<u64>,
        /// Persistent events carried forward from the previous execution during continue-as-new.
        /// Present only on CAN-initiated executions for audit trail. Each tuple is (event_name, data).
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        carry_forward_events: Option<Vec<(String, String)>>,
        /// Custom status carried forward from the previous execution during continue-as-new.
        /// Initialized from the last `CustomStatusUpdated` event in the previous execution's history.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        initial_custom_status: Option<String>,
    },

    /// Orchestration completed with a final result.
    #[serde(rename = "OrchestrationCompleted")]
    OrchestrationCompleted { output: String },

    /// Orchestration failed with a final error.
    #[serde(rename = "OrchestrationFailed")]
    OrchestrationFailed { details: ErrorDetails },

    /// Activity was scheduled.
    #[serde(rename = "ActivityScheduled")]
    ActivityScheduled {
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

    /// Activity completed successfully with a result.
    #[serde(rename = "ActivityCompleted")]
    ActivityCompleted { result: String },

    /// Activity failed with error details.
    #[serde(rename = "ActivityFailed")]
    ActivityFailed { details: ErrorDetails },

    /// Cancellation was requested for an activity (best-effort; completion may still arrive).
    /// Correlates to the ActivityScheduled event via Event.source_event_id.
    #[serde(rename = "ActivityCancelRequested")]
    ActivityCancelRequested { reason: String },

    /// Timer was created and will logically fire at `fire_at_ms`.
    #[serde(rename = "TimerCreated")]
    TimerCreated { fire_at_ms: u64 },

    /// Timer fired at logical time `fire_at_ms`.
    #[serde(rename = "TimerFired")]
    TimerFired { fire_at_ms: u64 },

    /// Subscription to an external event by name was recorded.
    #[serde(rename = "ExternalSubscribed")]
    ExternalSubscribed { name: String },

    /// An external event was raised. Matched by name (no source_event_id).
    #[serde(rename = "ExternalEvent")]
    ExternalEvent { name: String, data: String },

    /// Fire-and-forget orchestration scheduling (detached).
    #[serde(rename = "OrchestrationChained")]
    OrchestrationChained {
        name: String,
        instance: String,
        input: String,
    },

    /// Sub-orchestration was scheduled with deterministic child instance id.
    #[serde(rename = "SubOrchestrationScheduled")]
    SubOrchestrationScheduled {
        name: String,
        instance: String,
        input: String,
    },

    /// Sub-orchestration completed and returned a result to the parent.
    #[serde(rename = "SubOrchestrationCompleted")]
    SubOrchestrationCompleted { result: String },

    /// Sub-orchestration failed and returned error details to the parent.
    #[serde(rename = "SubOrchestrationFailed")]
    SubOrchestrationFailed { details: ErrorDetails },

    /// Cancellation was requested for a sub-orchestration (best-effort; completion may still arrive).
    /// Correlates to the SubOrchestrationScheduled event via Event.source_event_id.
    #[serde(rename = "SubOrchestrationCancelRequested")]
    SubOrchestrationCancelRequested { reason: String },

    /// Orchestration continued as new with fresh input (terminal for this execution).
    #[serde(rename = "OrchestrationContinuedAsNew")]
    OrchestrationContinuedAsNew { input: String },

    /// Cancellation has been requested for the orchestration (terminal will follow deterministically).
    #[serde(rename = "OrchestrationCancelRequested")]
    OrchestrationCancelRequested { reason: String },

    /// An external event subscription was cancelled (e.g., lost a select2 race).
    /// Correlates to the ExternalSubscribed event via Event.source_event_id.
    /// This breadcrumb ensures deterministic replay — cancelled subscriptions
    /// are skipped during index-based matching.
    #[serde(rename = "ExternalSubscribedCancelled")]
    ExternalSubscribedCancelled { reason: String },

    /// Persistent subscription to an external event by name was recorded.
    /// Unlike positional ExternalSubscribed, persistent subscriptions use
    /// mailbox semantics: FIFO matching, no positional pairing.
    #[serde(rename = "ExternalSubscribedPersistent")]
    QueueSubscribed { name: String },

    /// A persistent external event was raised. Matched by name using FIFO
    /// mailbox semantics (not positional). Events stick around until consumed.
    #[serde(rename = "ExternalEventPersistent")]
    QueueEventDelivered { name: String, data: String },

    /// A persistent external event subscription was cancelled (e.g., lost a select2 race).
    /// Correlates to the QueueSubscribed event via Event.source_event_id.
    /// This breadcrumb ensures correct CAN carry-forward — cancelled subscriptions
    /// are not counted as having consumed an arrival.
    #[serde(rename = "ExternalSubscribedPersistentCancelled")]
    QueueSubscriptionCancelled { reason: String },

    /// Custom status was updated via `ctx.set_custom_status()` or `ctx.reset_custom_status()`.
    /// `Some(s)` means set to `s`; `None` means cleared back to NULL.
    /// This event makes custom status changes durable and replayable.
    #[serde(rename = "CustomStatusUpdated")]
    CustomStatusUpdated { status: Option<String> },

    /// V2 subscription: includes a topic filter for pub/sub matching.
    /// Feature-gated for replay engine extensibility verification.
    #[cfg(feature = "replay-version-test")]
    #[serde(rename = "ExternalSubscribed2")]
    ExternalSubscribed2 { name: String, topic: String },

    /// V2 event: includes the actual topic it was published on.
    /// Feature-gated for replay engine extensibility verification.
    #[cfg(feature = "replay-version-test")]
    #[serde(rename = "ExternalEvent2")]
    ExternalEvent2 { name: String, topic: String, data: String },

    // === Key-Value Store Events ===
    /// A key-value pair was set via `ctx.set_kv_value()`.
    /// Fire-and-forget metadata action (like `CustomStatusUpdated`).
    #[serde(rename = "KeyValueSet")]
    KeyValueSet {
        key: String,
        value: String,
        /// Timestamp (ms since epoch) when this key was written.
        /// Set by the runtime, persisted by the provider. Defaults to 0 for pre-upgrade events.
        #[serde(default)]
        last_updated_at_ms: u64,
    },

    /// A single key was cleared via `ctx.clear_kv_value()`.
    #[serde(rename = "KeyValueCleared")]
    KeyValueCleared { key: String },

    /// All key-value pairs were cleared via `ctx.clear_all_kv_values()`.
    #[serde(rename = "KeyValuesCleared")]
    KeyValuesCleared,
}

impl Event {
    /// Create a new event with common fields populated and a specific event_id.
    ///
    /// Use this when you know the event_id upfront (e.g., during replay or when
    /// creating events inline).
    pub fn with_event_id(
        event_id: u64,
        instance_id: impl Into<String>,
        execution_id: u64,
        source_event_id: Option<u64>,
        kind: EventKind,
    ) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        Event {
            event_id,
            source_event_id,
            instance_id: instance_id.into(),
            execution_id,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            duroxide_version: env!("CARGO_PKG_VERSION").to_string(),
            kind,
        }
    }

    /// Create a new event with common fields populated.
    ///
    /// The event_id will be 0 and should be set by the history manager.
    pub fn new(
        instance_id: impl Into<String>,
        execution_id: u64,
        source_event_id: Option<u64>,
        kind: EventKind,
    ) -> Self {
        Self::with_event_id(0, instance_id, execution_id, source_event_id, kind)
    }

    /// Get the event_id (position in history).
    #[inline]
    pub fn event_id(&self) -> u64 {
        self.event_id
    }

    /// Set the event_id (used by runtime when adding events to history).
    #[inline]
    pub(crate) fn set_event_id(&mut self, id: u64) {
        self.event_id = id;
    }

    /// Get the source_event_id if this is a completion event.
    /// Returns None for lifecycle and scheduling events.
    #[inline]
    pub fn source_event_id(&self) -> Option<u64> {
        self.source_event_id
    }

    /// Check if this event is a terminal event (ends the orchestration).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.kind,
            EventKind::OrchestrationCompleted { .. }
                | EventKind::OrchestrationFailed { .. }
                | EventKind::OrchestrationContinuedAsNew { .. }
        )
    }
}

/// Log levels for orchestration context logging.
#[derive(Debug, Clone)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

/// Backoff strategy for computing delay between retry attempts.
#[derive(Debug, Clone)]
pub enum BackoffStrategy {
    /// No delay between retries.
    None,
    /// Fixed delay between all retries.
    Fixed {
        /// Delay duration between each retry.
        delay: std::time::Duration,
    },
    /// Linear backoff: delay = base * attempt, capped at max.
    Linear {
        /// Base delay multiplied by attempt number.
        base: std::time::Duration,
        /// Maximum delay cap.
        max: std::time::Duration,
    },
    /// Exponential backoff: delay = base * multiplier^(attempt-1), capped at max.
    Exponential {
        /// Initial delay for first retry.
        base: std::time::Duration,
        /// Multiplier applied each attempt.
        multiplier: f64,
        /// Maximum delay cap.
        max: std::time::Duration,
    },
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        BackoffStrategy::Exponential {
            base: std::time::Duration::from_millis(100),
            multiplier: 2.0,
            max: std::time::Duration::from_secs(30),
        }
    }
}

impl BackoffStrategy {
    /// Compute delay for given attempt (1-indexed).
    /// Attempt 1 is after first failure, so delay_for_attempt(1) is the first backoff.
    pub fn delay_for_attempt(&self, attempt: u32) -> std::time::Duration {
        match self {
            BackoffStrategy::None => std::time::Duration::ZERO,
            BackoffStrategy::Fixed { delay } => *delay,
            BackoffStrategy::Linear { base, max } => {
                let delay = base.saturating_mul(attempt);
                std::cmp::min(delay, *max)
            }
            BackoffStrategy::Exponential { base, multiplier, max } => {
                // delay = base * multiplier^(attempt-1)
                let factor = multiplier.powi(attempt.saturating_sub(1) as i32);
                let delay_nanos = (base.as_nanos() as f64 * factor) as u128;
                let delay = std::time::Duration::from_nanos(delay_nanos.min(u64::MAX as u128) as u64);
                std::cmp::min(delay, *max)
            }
        }
    }
}

/// Retry policy for activities.
///
/// Configures automatic retry behavior including maximum attempts, backoff strategy,
/// and optional total timeout spanning all attempts.
///
/// # Example
///
/// ```rust
/// use std::time::Duration;
/// use duroxide::{RetryPolicy, BackoffStrategy};
///
/// // Simple retry with defaults (3 attempts, exponential backoff)
/// let policy = RetryPolicy::new(3);
///
/// // Custom policy with timeout and fixed backoff
/// let policy = RetryPolicy::new(5)
///     .with_timeout(Duration::from_secs(30))
///     .with_backoff(BackoffStrategy::Fixed {
///         delay: Duration::from_secs(1),
///     });
/// ```
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including initial). Must be >= 1.
    pub max_attempts: u32,
    /// Backoff strategy between retries.
    pub backoff: BackoffStrategy,
    /// Per-attempt timeout. If set, each activity attempt is raced against this
    /// timeout. If timeout fires, returns error immediately (no retry).
    /// Retries only occur for activity errors, not timeouts. None = no timeout.
    pub timeout: Option<std::time::Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: BackoffStrategy::default(),
            timeout: None,
        }
    }
}

impl RetryPolicy {
    /// Create a new retry policy with specified max attempts and default backoff.
    ///
    /// # Panics
    /// Panics if `max_attempts` is 0.
    pub fn new(max_attempts: u32) -> Self {
        assert!(max_attempts >= 1, "max_attempts must be at least 1");
        Self {
            max_attempts,
            ..Default::default()
        }
    }

    /// Set per-attempt timeout.
    ///
    /// Each activity attempt is raced against this timeout. If the timeout fires
    /// before the activity completes, returns an error immediately (no retry).
    /// Retries only occur for activity errors, not timeouts.
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Alias for `with_timeout` for backwards compatibility.
    #[doc(hidden)]
    pub fn with_total_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set backoff strategy.
    pub fn with_backoff(mut self, backoff: BackoffStrategy) -> Self {
        self.backoff = backoff;
        self
    }

    /// Compute delay for given attempt using the configured backoff strategy.
    pub fn delay_for_attempt(&self, attempt: u32) -> std::time::Duration {
        self.backoff.delay_for_attempt(attempt)
    }
}

/// Declarative decisions produced by an orchestration turn. The host/provider
/// is responsible for materializing these into corresponding `Event`s.
#[derive(Debug, Clone)]
pub enum Action {
    /// Schedule an activity invocation. scheduling_event_id is the event_id of the ActivityScheduled event.
    CallActivity {
        scheduling_event_id: u64,
        name: String,
        input: String,
        /// Optional session ID for worker affinity routing.
        /// When set, the activity is routed to the worker owning this session.
        session_id: Option<String>,
        /// Optional routing tag for directing this activity to specific workers.
        /// Set via `.with_tag()` on the returned `DurableFuture`.
        tag: Option<String>,
    },
    /// Create a timer that will fire at the specified absolute time.
    /// scheduling_event_id is the event_id of the TimerCreated event.
    /// fire_at_ms is the absolute timestamp (ms since epoch) when the timer should fire.
    CreateTimer { scheduling_event_id: u64, fire_at_ms: u64 },
    /// Subscribe to an external event by name. scheduling_event_id is the event_id of the ExternalSubscribed event.
    WaitExternal { scheduling_event_id: u64, name: String },
    /// Start a detached orchestration (no result routing back to parent).
    StartOrchestrationDetached {
        scheduling_event_id: u64,
        name: String,
        version: Option<String>,
        instance: String,
        input: String,
    },
    /// Start a sub-orchestration by name and child instance id. scheduling_event_id is the event_id of the SubOrchestrationScheduled event.
    StartSubOrchestration {
        scheduling_event_id: u64,
        name: String,
        version: Option<String>,
        instance: String,
        input: String,
    },

    /// Continue the current orchestration as a new execution with new input (terminal for current execution).
    /// Optional version string selects the target orchestration version for the new execution.
    ContinueAsNew { input: String, version: Option<String> },

    /// Update the custom status of the orchestration.
    /// `Some(s)` means set to `s`; `None` means cleared back to NULL.
    UpdateCustomStatus { status: Option<String> },

    /// Subscribe to a persistent external event by name (mailbox semantics).
    /// Unlike WaitExternal, persistent events use FIFO matching and survive cancellation.
    DequeueEvent { scheduling_event_id: u64, name: String },

    /// V2: Subscribe to an external event with topic-based pub/sub matching.
    /// Feature-gated for replay engine extensibility verification.
    #[cfg(feature = "replay-version-test")]
    WaitExternal2 {
        scheduling_event_id: u64,
        name: String,
        topic: String,
    },

    // === Key-Value Store Actions ===
    /// Set a key-value pair on this orchestration instance.
    /// Fire-and-forget metadata action (like `UpdateCustomStatus`).
    SetKeyValue {
        key: String,
        value: String,
        last_updated_at_ms: u64,
    },

    /// Clear a single key from the KV store.
    ClearKeyValue { key: String },

    /// Clear all keys from the KV store.
    ClearKeyValues,
}

/// Result delivered to a durable future upon completion.
///
/// This enum represents the completion states for various durable operations.
#[doc(hidden)]
#[derive(Debug, Clone)]
#[allow(dead_code)] // ExternalData is part of the design but delivered via get_external_event
pub enum CompletionResult {
    /// Activity completed successfully
    ActivityOk(String),
    /// Activity failed with error
    ActivityErr(String),
    /// Timer fired
    TimerFired,
    /// Sub-orchestration completed successfully
    SubOrchOk(String),
    /// Sub-orchestration failed with error
    SubOrchErr(String),
    /// External event data (NOTE: External events delivered via get_external_event, not CompletionResult)
    ExternalData(String),
}

#[derive(Debug)]
struct CtxInner {
    /// Whether we're currently replaying history (true) or processing new events (false).
    /// True while processing baseline_history events, false after.
    /// Users can check this via `ctx.is_replaying()` to skip side effects during replay.
    is_replaying: bool,

    // === Replay Engine State ===
    /// Token counter (each schedule_*() call gets a unique token)
    next_token: u64,
    /// Emitted actions (token -> Action kind info)
    /// Token is used to correlate with schedule events during replay
    emitted_actions: Vec<(u64, Action)>,
    /// Results map: token -> completion result (populated by replay engine)
    completion_results: std::collections::HashMap<u64, CompletionResult>,
    /// Token -> schedule_id binding (set when replay engine matches action to history)
    token_bindings: std::collections::HashMap<u64, u64>,
    /// External subscriptions: schedule_id -> (name, subscription_index)
    external_subscriptions: std::collections::HashMap<u64, (String, usize)>,
    /// External arrivals: name -> list of payloads in arrival order
    external_arrivals: std::collections::HashMap<String, Vec<String>>,
    /// Next subscription index per external event name
    external_next_index: std::collections::HashMap<String, usize>,
    /// Cancelled external subscription schedule_ids (from ExternalSubscribedCancelled breadcrumbs)
    external_cancelled_subscriptions: std::collections::HashSet<u64>,

    // === Persistent External Event State (mailbox semantics) ===
    /// Persistent subscriptions: schedule_id -> name
    /// Each subscription is matched FIFO with persistent arrivals.
    queue_subscriptions: Vec<(u64, String)>,
    /// Persistent arrivals: name -> list of payloads in arrival order
    queue_arrivals: std::collections::HashMap<String, Vec<String>>,
    /// Persistent subscriptions that have been cancelled (dropped without completing)
    queue_cancelled_subscriptions: std::collections::HashSet<u64>,
    /// Persistent subscriptions that have already been resolved (consumed an arrival)
    queue_resolved_subscriptions: std::collections::HashSet<u64>,

    // === V2 External Event State (feature-gated) ===
    /// V2 external subscriptions: schedule_id -> (name, topic, subscription_index)
    #[cfg(feature = "replay-version-test")]
    external2_subscriptions: std::collections::HashMap<u64, (String, String, usize)>,
    /// V2 external arrivals: (name, topic) -> list of payloads in arrival order
    #[cfg(feature = "replay-version-test")]
    external2_arrivals: std::collections::HashMap<(String, String), Vec<String>>,
    /// Next subscription index per (name, topic) pair
    #[cfg(feature = "replay-version-test")]
    external2_next_index: std::collections::HashMap<(String, String), usize>,

    /// Sub-orchestration token -> resolved instance ID mapping
    sub_orchestration_instances: std::collections::HashMap<u64, String>,

    // === Cancellation Tracking ===
    /// Tokens that have been cancelled (dropped without completing)
    cancelled_tokens: std::collections::HashSet<u64>,
    /// Cancelled token -> ScheduleKind mapping (for determining cancellation action)
    cancelled_token_kinds: std::collections::HashMap<u64, ScheduleKind>,

    // Execution metadata
    execution_id: u64,
    instance_id: String,
    orchestration_name: String,
    orchestration_version: String,
    logging_enabled_this_poll: bool,

    /// Accumulated custom status from history events.
    /// Updated by `CustomStatusUpdated` events during replay and by `set_custom_status()` / `reset_custom_status()` calls.
    /// `None` means no custom status has been set (either never set, or cleared via `reset_custom_status()`).
    accumulated_custom_status: Option<String>,

    /// Instance-scoped key-value store (values only).
    /// Seeded from the provider's materialized `kv_store` table at fetch time,
    /// then kept current by `set_kv_value`/`clear_kv_value`/`clear_all_kv_values` calls.
    kv_state: std::collections::HashMap<String, String>,

    /// Per-key `last_updated_at_ms` timestamps, seeded from the provider snapshot.
    /// Used by `prune_kv_values_updated_before()` for deterministic pruning decisions.
    /// Keys written during the current turn are marked with `u64::MAX` (never prunable
    /// until the next turn, when their real timestamp will appear in the snapshot).
    kv_metadata: std::collections::HashMap<String, u64>,
}

impl CtxInner {
    fn new(
        _history: Vec<Event>, // Kept for API compatibility, no longer used
        execution_id: u64,
        instance_id: String,
        orchestration_name: String,
        orchestration_version: String,
        _worker_id: Option<String>, // Kept for API compatibility, no longer used
    ) -> Self {
        Self {
            // Start in replaying state - will be set to false when we move past baseline history
            is_replaying: true,

            // Replay engine state
            next_token: 0,
            emitted_actions: Vec::new(),
            completion_results: Default::default(),
            token_bindings: Default::default(),
            external_subscriptions: Default::default(),
            external_arrivals: Default::default(),
            external_next_index: Default::default(),
            external_cancelled_subscriptions: Default::default(),
            queue_subscriptions: Default::default(),
            queue_arrivals: Default::default(),
            queue_cancelled_subscriptions: Default::default(),
            queue_resolved_subscriptions: Default::default(),
            #[cfg(feature = "replay-version-test")]
            external2_subscriptions: Default::default(),
            #[cfg(feature = "replay-version-test")]
            external2_arrivals: Default::default(),
            #[cfg(feature = "replay-version-test")]
            external2_next_index: Default::default(),
            sub_orchestration_instances: Default::default(),

            // Cancellation tracking
            cancelled_tokens: Default::default(),
            cancelled_token_kinds: Default::default(),

            // Execution metadata
            execution_id,
            instance_id,
            orchestration_name,
            orchestration_version,
            logging_enabled_this_poll: false,

            accumulated_custom_status: None,

            kv_state: std::collections::HashMap::new(),
            kv_metadata: std::collections::HashMap::new(),
        }
    }

    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    // === Replay Engine Helpers ===

    /// Emit an action and return a token for correlation.
    fn emit_action(&mut self, action: Action) -> u64 {
        self.next_token += 1;
        let token = self.next_token;
        self.emitted_actions.push((token, action));
        token
    }

    /// Drain all emitted actions (called by replay engine after polling).
    fn drain_emitted_actions(&mut self) -> Vec<(u64, Action)> {
        std::mem::take(&mut self.emitted_actions)
    }

    /// Bind a token to a schedule_id (called by replay engine when matching action to history).
    fn bind_token(&mut self, token: u64, schedule_id: u64) {
        self.token_bindings.insert(token, schedule_id);
    }

    /// Get the schedule_id bound to a token (returns None if not yet bound).
    fn get_bound_schedule_id(&self, token: u64) -> Option<u64> {
        self.token_bindings.get(&token).copied()
    }

    /// Deliver a completion result for a schedule_id.
    fn deliver_result(&mut self, schedule_id: u64, result: CompletionResult) {
        // Find the token that was bound to this schedule_id
        for (&token, &sid) in &self.token_bindings {
            if sid == schedule_id {
                self.completion_results.insert(token, result);
                return;
            }
        }
        tracing::warn!(
            schedule_id,
            "dropping completion result with no binding (unsupported for now)"
        );
    }

    /// Check if a result is available for a token.
    fn get_result(&self, token: u64) -> Option<&CompletionResult> {
        self.completion_results.get(&token)
    }

    /// Bind an external subscription to a deterministic index.
    fn bind_external_subscription(&mut self, schedule_id: u64, name: &str) {
        let idx = self.external_next_index.entry(name.to_string()).or_insert(0);
        let subscription_index = *idx;
        *idx += 1;
        self.external_subscriptions
            .insert(schedule_id, (name.to_string(), subscription_index));
    }

    /// Check if there is an active (non-cancelled) subscription slot available
    /// for a positional external event.
    ///
    /// Returns `true` if there is at least one active subscription for this name
    /// that hasn't been matched to an arrival yet. Cancelled subscriptions are
    /// excluded — they don't occupy arrival slots (compression skips them).
    fn has_pending_subscription_slot(&self, name: &str) -> bool {
        let active_subs = self
            .external_subscriptions
            .iter()
            .filter(|(sid, (n, _))| n == name && !self.external_cancelled_subscriptions.contains(sid))
            .count();
        let delivered = self.external_arrivals.get(name).map_or(0, |v| v.len());
        active_subs > delivered
    }

    /// Deliver an external event (appends to arrival list for the name).
    fn deliver_external_event(&mut self, name: String, data: String) {
        self.external_arrivals.entry(name).or_default().push(data);
    }

    /// Get external event data for a subscription (by schedule_id).
    ///
    /// Uses compressed (effective) indices that skip cancelled subscriptions:
    /// effective_index = raw_index - count_of_cancelled_subscriptions_with_lower_raw_index
    ///
    /// Stale events are prevented from entering the arrival list by the causal
    /// check in the replay loop (`has_pending_subscription_slot`), so the
    /// arrival array only contains events that have a matching active slot.
    fn get_external_event(&self, schedule_id: u64) -> Option<&String> {
        let (name, subscription_index) = self.external_subscriptions.get(&schedule_id)?;

        // If this subscription is cancelled, it never resolves
        if self.external_cancelled_subscriptions.contains(&schedule_id) {
            return None;
        }

        // Count cancelled subscriptions for this name with a lower raw index
        let cancelled_below = self
            .external_subscriptions
            .values()
            .filter(|(n, idx)| n == name && *idx < *subscription_index)
            .filter(|(_, idx)| {
                // Check if this subscription's schedule_id is in the cancelled set
                self.external_subscriptions
                    .iter()
                    .any(|(sid, (n, i))| n == name && i == idx && self.external_cancelled_subscriptions.contains(sid))
            })
            .count();

        let effective_index = subscription_index - cancelled_below;
        let arrivals = self.external_arrivals.get(name)?;
        arrivals.get(effective_index)
    }

    /// Mark an external subscription as cancelled.
    fn mark_external_subscription_cancelled(&mut self, schedule_id: u64) {
        self.external_cancelled_subscriptions.insert(schedule_id);
    }

    // === Persistent External Event Methods (mailbox semantics) ===

    /// Bind a persistent subscription.
    fn bind_queue_subscription(&mut self, schedule_id: u64, name: &str) {
        self.queue_subscriptions.push((schedule_id, name.to_string()));
    }

    /// Deliver a persistent external event (appends to arrival list for the name).
    fn deliver_queue_message(&mut self, name: String, data: String) {
        self.queue_arrivals.entry(name).or_default().push(data);
    }

    /// Mark a persistent subscription as cancelled (dropped without completing).
    fn mark_queue_subscription_cancelled(&mut self, schedule_id: u64) {
        self.queue_cancelled_subscriptions.insert(schedule_id);
    }

    /// Get persistent event data for a subscription (by schedule_id).
    ///
    /// Uses FIFO matching: the first active (non-cancelled, non-resolved) persistent
    /// subscription gets the first unmatched persistent arrival for that name.
    fn get_queue_message(&mut self, schedule_id: u64) -> Option<String> {
        // Find which name this subscription is for
        let name = self
            .queue_subscriptions
            .iter()
            .find(|(sid, _)| *sid == schedule_id)
            .map(|(_, n)| n.clone())?;

        // Already resolved?
        if self.queue_resolved_subscriptions.contains(&schedule_id) {
            return None;
        }

        // Already cancelled?
        if self.queue_cancelled_subscriptions.contains(&schedule_id) {
            return None;
        }

        // Our arrival index is exactly the number of active (non-cancelled)
        // subscriptions for this name that were created before us.
        // This guarantees strict FIFO matching regardless of poll order.
        let arrival_index: usize = self
            .queue_subscriptions
            .iter()
            .take_while(|(sid, _)| *sid != schedule_id)
            .filter(|(sid, n)| n == &name && !self.queue_cancelled_subscriptions.contains(sid))
            .count();

        // Get arrivals for this name
        let arrivals = self.queue_arrivals.get(&name)?;

        if arrival_index < arrivals.len() {
            // Mark as resolved
            self.queue_resolved_subscriptions.insert(schedule_id);
            Some(arrivals[arrival_index].clone())
        } else {
            None
        }
    }

    /// Get cancelled persistent wait schedule_ids (tokens that were bound and then dropped).
    fn get_cancelled_queue_ids(&self) -> Vec<u64> {
        let mut ids = Vec::new();
        for &token in &self.cancelled_tokens {
            if let Some(kind) = self.cancelled_token_kinds.get(&token)
                && matches!(kind, ScheduleKind::QueueDequeue { .. })
                && let Some(&schedule_id) = self.token_bindings.get(&token)
            {
                ids.push(schedule_id);
            }
        }
        ids
    }

    /// V2: Bind an external subscription with topic to a deterministic index.
    #[cfg(feature = "replay-version-test")]
    fn bind_external_subscription2(&mut self, schedule_id: u64, name: &str, topic: &str) {
        let key = (name.to_string(), topic.to_string());
        let idx = self.external2_next_index.entry(key.clone()).or_insert(0);
        let subscription_index = *idx;
        *idx += 1;
        self.external2_subscriptions
            .insert(schedule_id, (name.to_string(), topic.to_string(), subscription_index));
    }

    /// V2: Deliver an external event with topic (appends to arrival list for (name, topic)).
    #[cfg(feature = "replay-version-test")]
    fn deliver_external_event2(&mut self, name: String, topic: String, data: String) {
        self.external2_arrivals.entry((name, topic)).or_default().push(data);
    }

    /// V2: Get external event data for a topic-based subscription (by schedule_id).
    #[cfg(feature = "replay-version-test")]
    fn get_external_event2(&self, schedule_id: u64) -> Option<&String> {
        let (name, topic, subscription_index) = self.external2_subscriptions.get(&schedule_id)?;
        let arrivals = self.external2_arrivals.get(&(name.clone(), topic.clone()))?;
        arrivals.get(*subscription_index)
    }

    // === Cancellation Helpers ===

    /// Mark a token as cancelled (called by DurableFuture::drop).
    fn mark_token_cancelled(&mut self, token: u64, kind: ScheduleKind) {
        self.cancelled_tokens.insert(token);
        self.cancelled_token_kinds.insert(token, kind);
    }

    /// Get cancelled activity schedule_ids (tokens that were bound and then dropped).
    fn get_cancelled_activity_ids(&self) -> Vec<u64> {
        let mut ids = Vec::new();
        for &token in &self.cancelled_tokens {
            if let Some(kind) = self.cancelled_token_kinds.get(&token)
                && matches!(kind, ScheduleKind::Activity { .. })
                && let Some(&schedule_id) = self.token_bindings.get(&token)
            {
                ids.push(schedule_id);
            }
        }
        ids
    }

    /// Get cancelled external wait schedule_ids (tokens that were bound and then dropped).
    fn get_cancelled_external_wait_ids(&self) -> Vec<u64> {
        let mut ids = Vec::new();
        for &token in &self.cancelled_tokens {
            if let Some(kind) = self.cancelled_token_kinds.get(&token)
                && matches!(kind, ScheduleKind::ExternalWait { .. })
                && let Some(&schedule_id) = self.token_bindings.get(&token)
            {
                ids.push(schedule_id);
            }
        }
        ids
    }

    /// Get cancelled sub-orchestration cancellations.
    ///
    /// Returns `(scheduling_event_id, child_instance_id)` for sub-orchestration futures that were
    /// bound (schedule_id assigned) and then dropped.
    fn get_cancelled_sub_orchestration_cancellations(&self) -> Vec<(u64, String)> {
        let mut cancels = Vec::new();
        for &token in &self.cancelled_tokens {
            if let Some(ScheduleKind::SubOrchestration { token: sub_token }) = self.cancelled_token_kinds.get(&token)
                && let Some(&schedule_id) = self.token_bindings.get(&token)
            {
                // Look up the resolved instance ID from our mapping
                if let Some(instance_id) = self.sub_orchestration_instances.get(sub_token) {
                    cancels.push((schedule_id, instance_id.clone()));
                }
                // If not in mapping, the action wasn't bound yet - nothing to cancel
            }
        }
        cancels
    }

    /// Bind a sub-orchestration token to its resolved instance ID.
    fn bind_sub_orchestration_instance(&mut self, token: u64, instance_id: String) {
        self.sub_orchestration_instances.insert(token, instance_id);
    }

    /// Clear cancelled tokens (called after turn completion to avoid re-processing).
    fn clear_cancelled_tokens(&mut self) {
        self.cancelled_tokens.clear();
        self.cancelled_token_kinds.clear();
    }

    // Note: deterministic GUID generation was removed from public API.
}

/// User-facing orchestration context for scheduling and replay-safe helpers.
/// Context provided to activities for logging and metadata access.
///
/// Unlike [`OrchestrationContext`], activities are leaf nodes that cannot schedule new work,
/// but they often need to emit structured logs and inspect orchestration metadata. The
/// `ActivityContext` exposes the parent orchestration information and trace helpers that log
/// with full correlation fields.
///
/// # Examples
///
/// ```rust,no_run
/// # use duroxide::ActivityContext;
/// # use duroxide::runtime::registry::ActivityRegistry;
/// let activities = ActivityRegistry::builder()
///     .register("ProvisionVM", |ctx: ActivityContext, config: String| async move {
///         ctx.trace_info(format!("Provisioning VM with config: {}", config));
///         
///         // Do actual work (can use sleep, HTTP, etc.)
///         let vm_id = provision_vm_internal(config).await?;
///         
///         ctx.trace_info(format!("VM provisioned: {}", vm_id));
///         Ok(vm_id)
///     })
///     .build();
/// # async fn provision_vm_internal(config: String) -> Result<String, String> { Ok("vm-123".to_string()) }
/// ```
///
/// # Metadata Access
///
/// Activity context provides access to orchestration correlation metadata:
/// - `instance_id()` - Orchestration instance identifier
/// - `execution_id()` - Execution number (for ContinueAsNew scenarios)
/// - `orchestration_name()` - Parent orchestration name
/// - `orchestration_version()` - Parent orchestration version
/// - `activity_name()` - Current activity name
///
/// # Cancellation Support
///
/// Activities can respond to cancellation when their parent orchestration reaches a terminal state:
/// - `is_cancelled()` - Check if cancellation has been requested
/// - `cancelled()` - Future that completes when cancellation is requested (for use with `tokio::select!`)
/// - `cancellation_token()` - Get a clone of the token for spawned tasks
///
/// # Determinism
///
/// Activity trace helpers (`trace_info`, `trace_warn`, etc.) do **not** participate in
/// deterministic replay. They emit logs directly using [`tracing`] and should only be used for
/// diagnostic purposes.
#[derive(Clone)]
pub struct ActivityContext {
    instance_id: String,
    execution_id: u64,
    orchestration_name: String,
    orchestration_version: String,
    activity_name: String,
    activity_id: u64,
    worker_id: String,
    /// Optional session ID when scheduled via `schedule_activity_on_session`.
    session_id: Option<String>,
    /// Optional routing tag when scheduled via `.with_tag()`.
    tag: Option<String>,
    /// Cancellation token for cooperative cancellation.
    /// Triggered when the parent orchestration reaches a terminal state.
    cancellation_token: tokio_util::sync::CancellationToken,
    /// Provider store for accessing the Client API.
    store: std::sync::Arc<dyn crate::providers::Provider>,
}

impl ActivityContext {
    /// Create a new activity context with a specific cancellation token.
    ///
    /// This constructor is intended for internal runtime use when the worker
    /// dispatcher needs to provide a cancellation token that can be triggered
    /// during activity execution.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_cancellation(
        instance_id: String,
        execution_id: u64,
        orchestration_name: String,
        orchestration_version: String,
        activity_name: String,
        activity_id: u64,
        worker_id: String,
        session_id: Option<String>,
        tag: Option<String>,
        cancellation_token: tokio_util::sync::CancellationToken,
        store: std::sync::Arc<dyn crate::providers::Provider>,
    ) -> Self {
        Self {
            instance_id,
            execution_id,
            orchestration_name,
            orchestration_version,
            activity_name,
            activity_id,
            worker_id,
            session_id,
            tag,
            cancellation_token,
            store,
        }
    }

    /// Returns the orchestration instance identifier.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Returns the execution id within the orchestration instance.
    pub fn execution_id(&self) -> u64 {
        self.execution_id
    }

    /// Returns the parent orchestration name.
    pub fn orchestration_name(&self) -> &str {
        &self.orchestration_name
    }

    /// Returns the parent orchestration version.
    pub fn orchestration_version(&self) -> &str {
        &self.orchestration_version
    }

    /// Returns the activity name being executed.
    pub fn activity_name(&self) -> &str {
        &self.activity_name
    }

    /// Returns the worker dispatcher ID processing this activity.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Returns the session ID if this activity was scheduled via `schedule_activity_on_session`.
    ///
    /// Returns `None` for regular activities scheduled via `schedule_activity`.
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Returns the routing tag if this activity was scheduled via `.with_tag()`.
    ///
    /// Returns `None` for activities scheduled without a tag.
    pub fn tag(&self) -> Option<&str> {
        self.tag.as_deref()
    }

    /// Emit an INFO level trace entry associated with this activity.
    pub fn trace_info(&self, message: impl Into<String>) {
        tracing::info!(
            target: "duroxide::activity",
            instance_id = %self.instance_id,
            execution_id = %self.execution_id,
            orchestration_name = %self.orchestration_name,
            orchestration_version = %self.orchestration_version,
            activity_name = %self.activity_name,
            activity_id = %self.activity_id,
            worker_id = %self.worker_id,
            "{}",
            message.into()
        );
    }

    /// Emit a WARN level trace entry associated with this activity.
    pub fn trace_warn(&self, message: impl Into<String>) {
        tracing::warn!(
            target: "duroxide::activity",
            instance_id = %self.instance_id,
            execution_id = %self.execution_id,
            orchestration_name = %self.orchestration_name,
            orchestration_version = %self.orchestration_version,
            activity_name = %self.activity_name,
            activity_id = %self.activity_id,
            worker_id = %self.worker_id,
            "{}",
            message.into()
        );
    }

    /// Emit an ERROR level trace entry associated with this activity.
    pub fn trace_error(&self, message: impl Into<String>) {
        tracing::error!(
            target: "duroxide::activity",
            instance_id = %self.instance_id,
            execution_id = %self.execution_id,
            orchestration_name = %self.orchestration_name,
            orchestration_version = %self.orchestration_version,
            activity_name = %self.activity_name,
            activity_id = %self.activity_id,
            worker_id = %self.worker_id,
            "{}",
            message.into()
        );
    }

    /// Emit a DEBUG level trace entry associated with this activity.
    pub fn trace_debug(&self, message: impl Into<String>) {
        tracing::debug!(
            target: "duroxide::activity",
            instance_id = %self.instance_id,
            execution_id = %self.execution_id,
            orchestration_name = %self.orchestration_name,
            orchestration_version = %self.orchestration_version,
            activity_name = %self.activity_name,
            activity_id = %self.activity_id,
            worker_id = %self.worker_id,
            "{}",
            message.into()
        );
    }

    // ===== Cancellation Support =====

    /// Check if cancellation has been requested.
    ///
    /// Returns `true` if the parent orchestration has completed, failed,
    /// or been cancelled. Activities can use this for cooperative cancellation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// for item in items {
    ///     if ctx.is_cancelled() {
    ///         return Err("Activity cancelled".into());
    ///     }
    ///     process(item).await;
    /// }
    /// ```
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Returns a future that completes when cancellation is requested.
    ///
    /// Use with `tokio::select!` for interruptible activities. This allows
    /// activities to respond promptly to cancellation without polling.
    ///
    /// # Example
    ///
    /// ```ignore
    /// tokio::select! {
    ///     result = do_work() => return result,
    ///     _ = ctx.cancelled() => return Err("Cancelled".into()),
    /// }
    /// ```
    pub async fn cancelled(&self) {
        self.cancellation_token.cancelled().await
    }

    /// Get a clone of the cancellation token for use in spawned tasks.
    ///
    /// If your activity spawns child tasks with `tokio::spawn()`, you should
    /// pass them this token so they can also respond to cancellation.
    ///
    /// **Important:** If you spawn additional tasks/threads and do not pass them
    /// the cancellation token, they may outlive the activity's cancellation/abort.
    /// This is user error - the runtime provides the signal but cannot guarantee
    /// termination of arbitrary spawned work.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let token = ctx.cancellation_token();
    /// let handle = tokio::spawn(async move {
    ///     loop {
    ///         tokio::select! {
    ///             _ = do_work() => {}
    ///             _ = token.cancelled() => break,
    ///         }
    ///     }
    /// });
    /// ```
    pub fn cancellation_token(&self) -> tokio_util::sync::CancellationToken {
        self.cancellation_token.clone()
    }

    /// Get a Client for management operations.
    ///
    /// This allows activities to perform management operations such as
    /// pruning old executions, deleting instances, or querying instance status.
    ///
    /// # Example: Self-Pruning Eternal Orchestration
    ///
    /// ```ignore
    /// // Activity that prunes old executions
    /// async fn prune_self(ctx: ActivityContext, _input: String) -> Result<String, String> {
    ///     let client = ctx.get_client();
    ///     let instance_id = ctx.instance_id();
    ///
    ///     let result = client.prune_executions(instance_id, PruneOptions {
    ///         keep_last: Some(1), // Keep only current execution
    ///         ..Default::default()
    ///     }).await.map_err(|e| e.to_string())?;
    ///
    ///     Ok(format!("Pruned {} executions", result.executions_deleted))
    /// }
    /// ```
    pub fn get_client(&self) -> crate::Client {
        crate::Client::new(self.store.clone())
    }
}

impl std::fmt::Debug for ActivityContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActivityContext")
            .field("instance_id", &self.instance_id)
            .field("execution_id", &self.execution_id)
            .field("orchestration_name", &self.orchestration_name)
            .field("orchestration_version", &self.orchestration_version)
            .field("activity_name", &self.activity_name)
            .field("activity_id", &self.activity_id)
            .field("worker_id", &self.worker_id)
            .field("cancellation_token", &self.cancellation_token)
            .field("store", &"<Provider>")
            .finish()
    }
}

#[derive(Clone)]
pub struct OrchestrationContext {
    inner: Arc<Mutex<CtxInner>>,
}

/// A future that never resolves, used by `continue_as_new()` to prevent further execution.
///
/// This future always returns `Poll::Pending`, ensuring that code after `await ctx.continue_as_new()`
/// is unreachable. The runtime extracts actions before checking the future's state, so the
/// `ContinueAsNew` action is properly recorded and processed.
struct ContinueAsNewFuture;

impl Future for ContinueAsNewFuture {
    type Output = Result<String, String>; // Matches orchestration return type, but never resolves

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Always pending - never resolves, making code after await unreachable
        // The runtime checks pending_actions before using the output, so this value is never used
        Poll::Pending
    }
}

impl OrchestrationContext {
    /// Construct a new context from an existing history vector.
    ///
    /// # Parameters
    ///
    /// * `orchestration_name` - The name of the orchestration being executed.
    /// * `orchestration_version` - The semantic version string of the orchestration.
    /// * `worker_id` - Optional dispatcher worker ID for logging correlation.
    ///   - `Some(id)`: Used by runtime dispatchers to include worker_id in traces
    ///   - `None`: Used by standalone/test execution without runtime context
    pub fn new(
        history: Vec<Event>,
        execution_id: u64,
        instance_id: String,
        orchestration_name: String,
        orchestration_version: String,
        worker_id: Option<String>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CtxInner::new(
                history,
                execution_id,
                instance_id,
                orchestration_name,
                orchestration_version,
                worker_id,
            ))),
        }
    }

    /// Check if the orchestration is currently replaying history.
    ///
    /// Returns `true` when processing events from persisted history (replay),
    /// and `false` when executing new logic beyond the stored history.
    ///
    /// This is useful for skipping side effects during replay, such as:
    /// - Logging/tracing that should only happen on first execution
    /// - Metrics that shouldn't be double-counted
    /// - External notifications that shouldn't be re-sent
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// if !ctx.is_replaying() {
    ///     // Only log on first execution, not during replay
    ///     println!("Starting workflow for the first time");
    /// }
    /// # }
    /// ```
    pub fn is_replaying(&self) -> bool {
        self.inner.lock().unwrap().is_replaying
    }

    /// Set the replaying state (used by replay engine and test harness).
    #[doc(hidden)]
    pub fn set_is_replaying(&self, is_replaying: bool) {
        self.inner.lock().unwrap().is_replaying = is_replaying;
    }

    /// Bind an external subscription to a schedule_id (used by replay engine and test harness).
    #[doc(hidden)]
    pub fn bind_external_subscription(&self, schedule_id: u64, name: &str) {
        self.inner.lock().unwrap().bind_external_subscription(schedule_id, name);
    }

    /// Deliver an external event (used by replay engine and test harness).
    #[doc(hidden)]
    pub fn deliver_external_event(&self, name: String, data: String) {
        self.inner.lock().unwrap().deliver_external_event(name, data);
    }

    /// Bind a persistent subscription to a schedule_id (used by replay engine).
    #[doc(hidden)]
    pub fn bind_queue_subscription(&self, schedule_id: u64, name: &str) {
        self.inner.lock().unwrap().bind_queue_subscription(schedule_id, name);
    }

    /// Deliver a persistent external event (used by replay engine).
    #[doc(hidden)]
    pub fn deliver_queue_message(&self, name: String, data: String) {
        self.inner.lock().unwrap().deliver_queue_message(name, data);
    }

    /// Mark a persistent subscription as cancelled (used by replay engine).
    pub(crate) fn mark_queue_subscription_cancelled(&self, schedule_id: u64) {
        self.inner
            .lock()
            .unwrap()
            .mark_queue_subscription_cancelled(schedule_id);
    }

    // =========================================================================
    // Cancellation Support (DurableFuture integration)
    // =========================================================================

    /// Mark a token as cancelled (called by DurableFuture::drop).
    pub(crate) fn mark_token_cancelled(&self, token: u64, kind: &ScheduleKind) {
        self.inner.lock().unwrap().mark_token_cancelled(token, kind.clone());
    }

    /// Get cancelled activity schedule_ids for this turn.
    pub(crate) fn get_cancelled_activity_ids(&self) -> Vec<u64> {
        self.inner.lock().unwrap().get_cancelled_activity_ids()
    }

    /// Get cancelled external wait schedule_ids for this turn.
    pub(crate) fn get_cancelled_external_wait_ids(&self) -> Vec<u64> {
        self.inner.lock().unwrap().get_cancelled_external_wait_ids()
    }

    /// Mark an external subscription as cancelled (called by replay engine).
    pub(crate) fn mark_external_subscription_cancelled(&self, schedule_id: u64) {
        self.inner
            .lock()
            .unwrap()
            .mark_external_subscription_cancelled(schedule_id);
    }

    /// Get cancelled persistent wait schedule_ids for this turn.
    pub(crate) fn get_cancelled_queue_ids(&self) -> Vec<u64> {
        self.inner.lock().unwrap().get_cancelled_queue_ids()
    }

    /// Get cancelled sub-orchestration cancellations for this turn.
    pub(crate) fn get_cancelled_sub_orchestration_cancellations(&self) -> Vec<(u64, String)> {
        self.inner
            .lock()
            .unwrap()
            .get_cancelled_sub_orchestration_cancellations()
    }

    /// Clear cancelled tokens after processing (called by replay engine).
    pub(crate) fn clear_cancelled_tokens(&self) {
        self.inner.lock().unwrap().clear_cancelled_tokens();
    }

    /// Bind a sub-orchestration token to its resolved instance ID.
    pub(crate) fn bind_sub_orchestration_instance(&self, token: u64, instance_id: String) {
        self.inner
            .lock()
            .unwrap()
            .bind_sub_orchestration_instance(token, instance_id);
    }

    // =========================================================================
    // Simplified Mode Tracing (Replay-Guarded)
    // =========================================================================
    //
    // These trace methods use `is_replaying()` as a guard, which means:
    // - No history events are created for traces
    // - Traces only emit on first execution, not during replay
    // - Much simpler and more efficient than system-call-based tracing

    /// Convenience wrapper for INFO level tracing.
    ///
    /// Logs with INFO level and includes instance context automatically.
    /// Only emits on first execution, not during replay.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// ctx.trace_info("Processing order");
    /// ctx.trace_info(format!("Processing {} items", 42));
    /// # }
    /// ```
    pub fn trace_info(&self, message: impl Into<String>) {
        self.trace("INFO", message);
    }

    /// Convenience wrapper for WARN level tracing.
    ///
    /// Logs with WARN level and includes instance context automatically.
    /// Only emits on first execution, not during replay.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// ctx.trace_warn("Retrying failed operation");
    /// # }
    /// ```
    pub fn trace_warn(&self, message: impl Into<String>) {
        self.trace("WARN", message);
    }

    /// Convenience wrapper for ERROR level tracing.
    ///
    /// Logs with ERROR level and includes instance context automatically.
    /// Only emits on first execution, not during replay.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// ctx.trace_error("Payment processing failed");
    /// # }
    /// ```
    pub fn trace_error(&self, message: impl Into<String>) {
        self.trace("ERROR", message);
    }

    /// Convenience wrapper for DEBUG level tracing.
    ///
    /// Logs with DEBUG level and includes instance context automatically.
    /// Only emits on first execution, not during replay.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// ctx.trace_debug("Detailed state information");
    /// # }
    /// ```
    pub fn trace_debug(&self, message: impl Into<String>) {
        self.trace("DEBUG", message);
    }

    /// Drain emitted actions.
    /// Returns a list of (token, Action) pairs.
    #[doc(hidden)]
    pub fn drain_emitted_actions(&self) -> Vec<(u64, Action)> {
        self.inner.lock().unwrap().drain_emitted_actions()
    }

    /// Get a snapshot of emitted actions without draining.
    /// Returns a list of (token, Action) pairs.
    #[doc(hidden)]
    pub fn get_emitted_actions(&self) -> Vec<(u64, Action)> {
        self.inner.lock().unwrap().emitted_actions.clone()
    }

    /// Bind a token to a schedule_id.
    #[doc(hidden)]
    pub fn bind_token(&self, token: u64, schedule_id: u64) {
        self.inner.lock().unwrap().bind_token(token, schedule_id);
    }

    /// Deliver a result for a token.
    #[doc(hidden)]
    pub fn deliver_result(&self, schedule_id: u64, result: CompletionResult) {
        self.inner.lock().unwrap().deliver_result(schedule_id, result);
    }

    /// Returns the orchestration instance identifier.
    ///
    /// This is the unique identifier for this orchestration instance, typically
    /// provided when starting the orchestration.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// let id = ctx.instance_id();
    /// ctx.trace_info(format!("Processing instance: {}", id));
    /// # }
    /// ```
    pub fn instance_id(&self) -> String {
        self.inner.lock().unwrap().instance_id.clone()
    }

    /// Returns the current execution ID within this orchestration instance.
    ///
    /// The execution ID increments each time `continue_as_new()` is called.
    /// Execution 1 is the initial execution.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// let exec_id = ctx.execution_id();
    /// ctx.trace_info(format!("Execution #{}", exec_id));
    /// # }
    /// ```
    pub fn execution_id(&self) -> u64 {
        self.inner.lock().unwrap().execution_id
    }

    /// Returns the orchestration name.
    ///
    /// This is the name registered with the orchestration registry.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// let name = ctx.orchestration_name();
    /// ctx.trace_info(format!("Running orchestration: {}", name));
    /// # }
    /// ```
    pub fn orchestration_name(&self) -> String {
        self.inner.lock().unwrap().orchestration_name.clone()
    }

    /// Returns the orchestration version.
    ///
    /// This is the semantic version string associated with the orchestration.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// let version = ctx.orchestration_version();
    /// ctx.trace_info(format!("Version: {}", version));
    /// # }
    /// ```
    pub fn orchestration_version(&self) -> String {
        self.inner.lock().unwrap().orchestration_version.clone()
    }

    // Replay-safe logging control
    /// Indicates whether logging is enabled for the current poll. This is
    /// flipped on when a decision is recorded to minimize log noise.
    pub fn is_logging_enabled(&self) -> bool {
        self.inner.lock().unwrap().logging_enabled_this_poll
    }
    // log_buffer removed - not used

    /// Emit a structured trace entry with automatic context correlation.
    ///
    /// Creates a system call event for deterministic replay and logs to tracing.
    /// The log entry automatically includes correlation fields:
    /// - `instance_id` - The orchestration instance identifier
    /// - `execution_id` - The current execution number
    /// - `orchestration_name` - Name of the orchestration
    /// - `orchestration_version` - Semantic version
    ///
    /// # Determinism
    ///
    /// This method is replay-safe: logs are only emitted on first execution,
    /// not during replay.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// ctx.trace("INFO", "Processing started");
    /// ctx.trace("WARN", format!("Retry attempt: {}", 3));
    /// ctx.trace("ERROR", "Payment validation failed");
    /// # }
    /// ```
    ///
    /// # Output
    ///
    /// ```text
    /// 2024-10-30T10:15:23.456Z INFO duroxide::orchestration [order-123] Processing started
    /// ```
    ///
    /// All logs include instance_id, execution_id, orchestration_name for correlation.
    pub fn trace(&self, level: impl Into<String>, message: impl Into<String>) {
        self.trace_internal(&level.into(), &message.into());
    }

    /// Internal implementation of trace (guarded by is_replaying)
    fn trace_internal(&self, level: &str, message: &str) {
        let inner = self.inner.lock().unwrap();

        // Only trace if not replaying
        if !inner.is_replaying {
            match level.to_uppercase().as_str() {
                "INFO" => tracing::info!(
                    target: "duroxide::orchestration",
                    instance_id = %inner.instance_id,
                    execution_id = %inner.execution_id,
                    orchestration_name = %inner.orchestration_name,
                    orchestration_version = %inner.orchestration_version,
                    "{}",
                    message
                ),
                "WARN" => tracing::warn!(
                    target: "duroxide::orchestration",
                    instance_id = %inner.instance_id,
                    execution_id = %inner.execution_id,
                    orchestration_name = %inner.orchestration_name,
                    orchestration_version = %inner.orchestration_version,
                    "{}",
                    message
                ),
                "ERROR" => tracing::error!(
                    target: "duroxide::orchestration",
                    instance_id = %inner.instance_id,
                    execution_id = %inner.execution_id,
                    orchestration_name = %inner.orchestration_name,
                    orchestration_version = %inner.orchestration_version,
                    "{}",
                    message
                ),
                "DEBUG" => tracing::debug!(
                    target: "duroxide::orchestration",
                    instance_id = %inner.instance_id,
                    execution_id = %inner.execution_id,
                    orchestration_name = %inner.orchestration_name,
                    orchestration_version = %inner.orchestration_version,
                    "{}",
                    message
                ),
                _ => tracing::trace!(
                    target: "duroxide::orchestration",
                    instance_id = %inner.instance_id,
                    execution_id = %inner.execution_id,
                    orchestration_name = %inner.orchestration_name,
                    orchestration_version = %inner.orchestration_version,
                    level = %level,
                    "{}",
                    message
                ),
            }
        }
    }

    /// Generate a new deterministic GUID.
    ///
    /// This schedules a built-in activity that generates a unique identifier.
    /// The GUID is deterministic across replays (the same value is returned
    /// when the orchestration replays).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) -> Result<(), String> {
    /// let guid = ctx.new_guid().await?;
    /// println!("Generated GUID: {}", guid);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_guid(&self) -> impl Future<Output = Result<String, String>> {
        self.schedule_activity(SYSCALL_ACTIVITY_NEW_GUID, "")
    }

    /// Get the current UTC time.
    ///
    /// This schedules a built-in activity that returns the current time.
    /// The time is deterministic across replays (the same value is returned
    /// when the orchestration replays).
    ///
    /// # Errors
    ///
    /// Returns an error if the activity fails or if the time value cannot be parsed.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # use std::time::{SystemTime, Duration};
    /// # async fn example(ctx: OrchestrationContext) -> Result<(), String> {
    /// let now = ctx.utc_now().await?;
    /// let deadline = now + Duration::from_secs(3600); // 1 hour from now
    /// # Ok(())
    /// # }
    /// ```
    pub fn utc_now(&self) -> impl Future<Output = Result<SystemTime, String>> {
        let fut = self.schedule_activity(SYSCALL_ACTIVITY_UTC_NOW_MS, "");
        async move {
            let s = fut.await?;
            let ms = s.parse::<u64>().map_err(|e| e.to_string())?;
            Ok(UNIX_EPOCH + StdDuration::from_millis(ms))
        }
    }

    /// Continue the current execution as a new execution with fresh input.
    ///
    /// This terminates the current execution and starts a new execution with the provided input.
    /// Returns a future that never resolves, ensuring code after `await` is unreachable.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) -> Result<String, String> {
    /// let n: u32 = 0;
    /// if n < 2 {
    ///     return ctx.continue_as_new("next_input").await; // Execution terminates here
    ///     // This code is unreachable - compiler will warn
    /// }
    /// Ok("completed".to_string())
    /// # }
    /// ```
    pub fn continue_as_new(&self, input: impl Into<String>) -> impl Future<Output = Result<String, String>> {
        let mut inner = self.inner.lock().unwrap();
        let input: String = input.into();
        let action = Action::ContinueAsNew { input, version: None };

        inner.emit_action(action);
        ContinueAsNewFuture
    }

    pub fn continue_as_new_typed<In: serde::Serialize>(
        &self,
        input: &In,
    ) -> impl Future<Output = Result<String, String>> {
        // Serialization should never fail for valid input types - if it does, it's a programming error
        let payload =
            crate::_typed_codec::Json::encode(input).expect("Serialization should never fail for valid input");
        self.continue_as_new(payload)
    }

    /// ContinueAsNew to a specific target version (string is parsed as semver later).
    pub fn continue_as_new_versioned(
        &self,
        version: impl Into<String>,
        input: impl Into<String>,
    ) -> impl Future<Output = Result<String, String>> {
        let mut inner = self.inner.lock().unwrap();
        let action = Action::ContinueAsNew {
            input: input.into(),
            version: Some(version.into()),
        };
        inner.emit_action(action);
        ContinueAsNewFuture
    }
}

/// Generate a deterministic GUID for use in orchestrations.
///
/// Uses timestamp + thread-local counter for uniqueness.
pub(crate) fn generate_guid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    // Thread-local counter for uniqueness within the same timestamp
    thread_local! {
        static COUNTER: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }
    let counter = COUNTER.with(|c| {
        let val = c.get();
        c.set(val.wrapping_add(1));
        val
    });

    // Format as UUID-like string
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (timestamp >> 96) as u32,
        ((timestamp >> 80) & 0xFFFF) as u16,
        (counter & 0xFFFF) as u16,
        ((timestamp >> 64) & 0xFFFF) as u16,
        (timestamp & 0xFFFFFFFFFFFF) as u64
    )
}

impl OrchestrationContext {
    /// Schedule activity with automatic retry on failure.
    ///
    /// **Retry behavior:**
    /// - Retries on activity **errors** up to `policy.max_attempts`
    /// - **Timeouts are NOT retried** - if any attempt times out, returns error immediately
    /// - Only application errors trigger retry logic
    ///
    /// **Timeout behavior (if `policy.total_timeout` is set):**
    /// - Each activity attempt is raced against the timeout
    /// - If the timeout fires before the activity completes → returns timeout error (no retry)
    /// - If the activity fails with an error before timeout → retry according to policy
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::{OrchestrationContext, RetryPolicy, BackoffStrategy};
    /// # use std::time::Duration;
    /// # async fn example(ctx: OrchestrationContext) -> Result<(), String> {
    /// // Simple retry with defaults (no timeout)
    /// let result = ctx.schedule_activity_with_retry(
    ///     "CallAPI",
    ///     "request",
    ///     RetryPolicy::new(3),
    /// ).await?;
    ///
    /// // Retry with per-attempt timeout and custom backoff
    /// let result = ctx.schedule_activity_with_retry(
    ///     "CallAPI",
    ///     "request",
    ///     RetryPolicy::new(5)
    ///         .with_timeout(Duration::from_secs(30)) // 30s per attempt
    ///         .with_backoff(BackoffStrategy::Fixed { delay: Duration::from_secs(1) }),
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if all retry attempts fail or if a timeout occurs (timeouts are not retried).
    pub async fn schedule_activity_with_retry(
        &self,
        name: impl Into<String>,
        input: impl Into<String>,
        policy: RetryPolicy,
    ) -> Result<String, String> {
        let name = name.into();
        let input = input.into();
        let mut last_error = String::new();

        for attempt in 1..=policy.max_attempts {
            // Each attempt: optionally race against per-attempt timeout
            let activity_result = if let Some(timeout) = policy.timeout {
                // Race activity vs per-attempt timeout
                let deadline = async {
                    self.schedule_timer(timeout).await;
                    Err::<String, String>("timeout: activity timed out".to_string())
                };
                let activity = self.schedule_activity(&name, &input);

                match self.select2(activity, deadline).await {
                    Either2::First(result) => result,
                    Either2::Second(Err(e)) => {
                        // Timeout fired - exit immediately, no retry for timeouts
                        return Err(e);
                    }
                    Either2::Second(Ok(_)) => unreachable!(),
                }
            } else {
                // No timeout - just await the activity
                self.schedule_activity(&name, &input).await
            };

            match activity_result {
                Ok(result) => return Ok(result),
                Err(e) => {
                    // Activity failed with error - apply retry policy
                    last_error = e.clone();
                    if attempt < policy.max_attempts {
                        self.trace(
                            "warn",
                            format!(
                                "Activity '{}' attempt {}/{} failed: {}. Retrying...",
                                name, attempt, policy.max_attempts, e
                            ),
                        );
                        let delay = policy.delay_for_attempt(attempt);
                        if !delay.is_zero() {
                            self.schedule_timer(delay).await;
                        }
                    }
                }
            }
        }
        Err(last_error)
    }

    /// Typed variant of `schedule_activity_with_retry`.
    ///
    /// Serializes input once and deserializes the successful result.
    ///
    /// # Errors
    ///
    /// Returns an error if all retry attempts fail, if a timeout occurs, if input serialization fails, or if result deserialization fails.
    pub async fn schedule_activity_with_retry_typed<In: serde::Serialize, Out: serde::de::DeserializeOwned>(
        &self,
        name: impl Into<String>,
        input: &In,
        policy: RetryPolicy,
    ) -> Result<Out, String> {
        let payload = crate::_typed_codec::Json::encode(input).expect("encode");
        let result = self.schedule_activity_with_retry(name, payload, policy).await?;
        crate::_typed_codec::Json::decode::<Out>(&result)
    }

    /// Schedule an activity with automatic retry on a specific session.
    ///
    /// Combines retry semantics from [`Self::schedule_activity_with_retry`] with
    /// session affinity from [`Self::schedule_activity_on_session`]. All retry
    /// attempts are pinned to the same `session_id`, ensuring they execute on
    /// the same worker.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::{OrchestrationContext, RetryPolicy, BackoffStrategy};
    /// # use std::time::Duration;
    /// # async fn example(ctx: OrchestrationContext) -> Result<(), String> {
    /// let session = ctx.new_guid().await?;
    /// let result = ctx.schedule_activity_with_retry_on_session(
    ///     "RunQuery",
    ///     "SELECT 1",
    ///     RetryPolicy::new(3)
    ///         .with_backoff(BackoffStrategy::Fixed { delay: Duration::from_secs(1) }),
    ///     &session,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if all retry attempts fail or if a timeout occurs (timeouts are not retried).
    pub async fn schedule_activity_with_retry_on_session(
        &self,
        name: impl Into<String>,
        input: impl Into<String>,
        policy: RetryPolicy,
        session_id: impl Into<String>,
    ) -> Result<String, String> {
        let name = name.into();
        let input = input.into();
        let session_id = session_id.into();
        let mut last_error = String::new();

        for attempt in 1..=policy.max_attempts {
            let activity_result = if let Some(timeout) = policy.timeout {
                let deadline = async {
                    self.schedule_timer(timeout).await;
                    Err::<String, String>("timeout: activity timed out".to_string())
                };
                let activity = self.schedule_activity_on_session(&name, &input, &session_id);

                match self.select2(activity, deadline).await {
                    Either2::First(result) => result,
                    Either2::Second(Err(e)) => return Err(e),
                    Either2::Second(Ok(_)) => unreachable!(),
                }
            } else {
                self.schedule_activity_on_session(&name, &input, &session_id).await
            };

            match activity_result {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = e.clone();
                    if attempt < policy.max_attempts {
                        self.trace(
                            "warn",
                            format!(
                                "Activity '{}' (session={}) attempt {}/{} failed: {}. Retrying...",
                                name, session_id, attempt, policy.max_attempts, e
                            ),
                        );
                        let delay = policy.delay_for_attempt(attempt);
                        if !delay.is_zero() {
                            self.schedule_timer(delay).await;
                        }
                    }
                }
            }
        }
        Err(last_error)
    }

    /// Typed variant of [`Self::schedule_activity_with_retry_on_session`].
    ///
    /// Serializes input once and deserializes the successful result. All retry
    /// attempts are pinned to the same session.
    ///
    /// # Errors
    ///
    /// Returns an error if all retry attempts fail, if a timeout occurs, if input
    /// serialization fails, or if result deserialization fails.
    pub async fn schedule_activity_with_retry_on_session_typed<
        In: serde::Serialize,
        Out: serde::de::DeserializeOwned,
    >(
        &self,
        name: impl Into<String>,
        input: &In,
        policy: RetryPolicy,
        session_id: impl Into<String>,
    ) -> Result<Out, String> {
        let payload = crate::_typed_codec::Json::encode(input).expect("encode");
        let result = self
            .schedule_activity_with_retry_on_session(name, payload, policy, session_id)
            .await?;
        crate::_typed_codec::Json::decode::<Out>(&result)
    }

    /// Schedule a detached orchestration with an explicit instance id.
    /// The runtime will prefix this with the parent instance to ensure global uniqueness.
    pub fn schedule_orchestration(
        &self,
        name: impl Into<String>,
        instance: impl Into<String>,
        input: impl Into<String>,
    ) {
        let name: String = name.into();
        let instance: String = instance.into();
        let input: String = input.into();
        let mut inner = self.inner.lock().unwrap();

        let _ = inner.emit_action(Action::StartOrchestrationDetached {
            scheduling_event_id: 0, // Will be assigned by replay engine
            name,
            version: None,
            instance,
            input,
        });
    }

    pub fn schedule_orchestration_typed<In: serde::Serialize>(
        &self,
        name: impl Into<String>,
        instance: impl Into<String>,
        input: &In,
    ) {
        let payload = crate::_typed_codec::Json::encode(input).expect("encode");
        self.schedule_orchestration(name, instance, payload)
    }

    /// Versioned detached orchestration start (string I/O). If `version` is None, registry policy is used for the child.
    pub fn schedule_orchestration_versioned(
        &self,
        name: impl Into<String>,
        version: Option<String>,
        instance: impl Into<String>,
        input: impl Into<String>,
    ) {
        let name: String = name.into();
        let instance: String = instance.into();
        let input: String = input.into();
        let mut inner = self.inner.lock().unwrap();

        let _ = inner.emit_action(Action::StartOrchestrationDetached {
            scheduling_event_id: 0, // Will be assigned by replay engine
            name,
            version,
            instance,
            input,
        });
    }

    pub fn schedule_orchestration_versioned_typed<In: serde::Serialize>(
        &self,
        name: impl Into<String>,
        version: Option<String>,
        instance: impl Into<String>,
        input: &In,
    ) {
        let payload = crate::_typed_codec::Json::encode(input).expect("encode");
        self.schedule_orchestration_versioned(name, version, instance, payload)
    }

    /// Set a user-defined custom status for progress reporting.
    ///
    /// This is **not** a history event or an action — it's pure metadata that gets
    /// plumbed into `ExecutionMetadata` at ack time. No impact on determinism,
    /// no replay implications.
    ///
    /// - Call it whenever, as many times as you want within a turn
    /// - Last write wins: if called twice in the same turn, only the last value is sent
    /// - Persistent across turns: if you don't call it on a later turn, the provider
    ///   keeps the previous value
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// ctx.set_custom_status("Processing item 3 of 10");
    /// let result = ctx.schedule_activity("ProcessItem", "item-3").await;
    /// ctx.set_custom_status("Processing item 4 of 10");
    /// # }
    /// ```
    pub fn set_custom_status(&self, status: impl Into<String>) {
        let status: String = status.into();
        let mut inner = self.inner.lock().unwrap();
        inner.accumulated_custom_status = Some(status.clone());
        inner.emit_action(Action::UpdateCustomStatus { status: Some(status) });
    }

    /// Clear the custom status back to `None`. The provider will set the column to NULL
    /// and increment `custom_status_version`.
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) {
    /// ctx.set_custom_status("Processing batch");
    /// // ... work ...
    /// ctx.reset_custom_status(); // done, clear the progress
    /// # }
    /// ```
    pub fn reset_custom_status(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.accumulated_custom_status = None;
        inner.emit_action(Action::UpdateCustomStatus { status: None });
    }

    /// Returns the current custom status value, if any.
    ///
    /// This reflects all `set_custom_status` / `reset_custom_status` calls made so far
    /// in this and previous turns. In a CAN'd execution, it includes the value carried
    /// from the previous execution.
    pub fn get_custom_status(&self) -> Option<String> {
        self.inner.lock().unwrap().accumulated_custom_status.clone()
    }

    // =========================================================================
    // Key-Value Store
    // =========================================================================

    /// Set a key-value pair scoped to this orchestration instance.
    ///
    /// Emits a `KeyValueSet` history event. The provider materializes this
    /// into the `kv_store` table during ack. The `last_updated_at_ms` timestamp
    /// is stamped from the current wall clock and persisted in the event.
    pub fn set_kv_value(&self, key: impl Into<String>, value: impl Into<String>) {
        let key: String = key.into();
        let value: String = value.into();
        let mut inner = self.inner.lock().unwrap();
        let last_updated_at_ms = inner.now_ms();
        inner.kv_state.insert(key.clone(), value.clone());
        inner.kv_metadata.insert(key.clone(), last_updated_at_ms);
        inner.emit_action(Action::SetKeyValue {
            key,
            value,
            last_updated_at_ms,
        });
    }

    /// Set a typed value (serialized as JSON) scoped to this orchestration instance.
    pub fn set_kv_value_typed<T: serde::Serialize>(&self, key: impl Into<String>, value: &T) {
        let serialized = serde_json::to_string(value).expect("KV value serialization should not fail");
        self.set_kv_value(key, serialized);
    }

    /// Read a KV entry from in-memory state.
    ///
    /// Pure read — no provider call, no event emitted, fully deterministic.
    /// Returns `None` if the key has never been set or was cleared.
    pub fn get_kv_value(&self, key: &str) -> Option<String> {
        self.inner.lock().unwrap().kv_state.get(key).cloned()
    }

    /// Read a typed KV entry. Returns `None` if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the key exists but deserialization fails.
    pub fn get_kv_value_typed<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>, String> {
        match self.get_kv_value(key) {
            None => Ok(None),
            Some(s) => serde_json::from_str(&s)
                .map(Some)
                .map_err(|e| format!("KV deserialization error for key '{}': {}", key, e)),
        }
    }

    /// Return a snapshot of all KV entries as a `HashMap`.
    ///
    /// Pure read from in-memory state — no provider call, no event emitted.
    pub fn get_kv_all_values(&self) -> std::collections::HashMap<String, String> {
        self.inner.lock().unwrap().kv_state.clone()
    }

    /// Return a list of all KV keys.
    ///
    /// Pure read from in-memory state — no provider call, no event emitted.
    pub fn get_kv_all_keys(&self) -> Vec<String> {
        self.inner.lock().unwrap().kv_state.keys().cloned().collect()
    }

    /// Return the number of KV entries.
    ///
    /// Pure read from in-memory state — no provider call, no event emitted.
    pub fn get_kv_length(&self) -> usize {
        self.inner.lock().unwrap().kv_state.len()
    }

    /// Clear a single key from the KV store.
    ///
    /// Emits a `KeyValueCleared` history event. After this call,
    /// `get_kv_value(key)` returns `None`.
    pub fn clear_kv_value(&self, key: impl Into<String>) {
        let key: String = key.into();
        let mut inner = self.inner.lock().unwrap();
        inner.kv_state.remove(&key);
        inner.kv_metadata.remove(&key);
        inner.emit_action(Action::ClearKeyValue { key });
    }

    /// Clear all keys from the KV store.
    ///
    /// Emits a `KeyValuesCleared` history event. After this call,
    /// `get_kv_value(key)` returns `None` for all keys.
    pub fn clear_all_kv_values(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.kv_state.clear();
        inner.kv_metadata.clear();
        inner.emit_action(Action::ClearKeyValues);
    }

    /// Remove KV entries whose persisted `last_updated_at_ms` is older than `updated_before_ms`.
    ///
    /// This helper scans the snapshot metadata loaded from the provider and emits
    /// `clear_kv_value()` for each qualifying key. Keys written during the current
    /// turn are never prunable (they haven't been acked yet).
    ///
    /// Returns the number of keys cleared.
    pub fn prune_kv_values_updated_before(&self, updated_before_ms: u64) -> usize {
        let keys_to_clear: Vec<String> = {
            let inner = self.inner.lock().unwrap();
            inner
                .kv_metadata
                .iter()
                .filter(|(_, ts)| **ts < updated_before_ms)
                .filter(|(key, _)| inner.kv_state.contains_key(*key))
                .map(|(key, _)| key.clone())
                .collect()
        };
        let count = keys_to_clear.len();
        for key in keys_to_clear {
            self.clear_kv_value(key);
        }
        count
    }

    /// Read a KV entry from another orchestration instance.
    ///
    /// This is modeled as a system activity — under the covers it schedules
    /// `__duroxide_syscall:get_kv_value` which calls `client.get_kv_value()`.
    /// Returns the value at the time the activity executes (not replay-cached on first run).
    /// On replay, the recorded result is returned — no provider call.
    pub fn get_kv_value_from_instance(
        &self,
        instance_id: impl Into<String>,
        key: impl Into<String>,
    ) -> DurableFuture<Result<Option<String>, String>> {
        let input = serde_json::json!({
            "instance_id": instance_id.into(),
            "key": key.into(),
        })
        .to_string();
        self.schedule_activity(SYSCALL_ACTIVITY_GET_KV_VALUE, input)
            .map(|result| match result {
                Ok(json_str) => serde_json::from_str::<Option<String>>(&json_str)
                    .map_err(|e| format!("get_kv_value_from_instance deserialization error: {e}")),
                Err(e) => Err(e),
            })
    }

    /// Typed variant of `get_kv_value_from_instance`.
    ///
    /// Deserializes the returned JSON string into `T`.
    /// Returns `Ok(None)` if the key doesn't exist in the remote instance.
    pub fn get_kv_value_from_instance_typed<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        instance_id: impl Into<String>,
        key: impl Into<String>,
    ) -> DurableFuture<Result<Option<T>, String>> {
        self.get_kv_value_from_instance(instance_id, key)
            .map(|result| match result {
                Ok(None) => Ok(None),
                Ok(Some(s)) => serde_json::from_str::<T>(&s)
                    .map(Some)
                    .map_err(|e| format!("get_kv_value_from_instance_typed deserialization error: {e}")),
                Err(e) => Err(e),
            })
    }
}

// Aggregate future machinery lives below (OrchestrationContext helpers)

impl OrchestrationContext {
    // =========================================================================
    // Core scheduling methods - return DurableFuture with cancellation support
    // =========================================================================

    /// Schedule an activity and return a cancellation-aware future.
    ///
    /// Returns a [`DurableFuture`] that supports cancellation on drop. If the future
    /// is dropped without completing (e.g., as a select loser), the activity will be
    /// cancelled via lock stealing.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) -> Result<String, String> {
    /// // Fan-out to multiple activities
    /// let f1 = ctx.schedule_activity("Process", "A");
    /// let f2 = ctx.schedule_activity("Process", "B");
    /// let results = ctx.join(vec![f1, f2]).await;
    /// # Ok("done".to_string())
    /// # }
    /// ```
    pub fn schedule_activity(
        &self,
        name: impl Into<String>,
        input: impl Into<String>,
    ) -> DurableFuture<Result<String, String>> {
        self.schedule_activity_internal(name, input, None)
    }

    /// Typed version of schedule_activity that serializes input and deserializes output.
    ///
    /// # Errors
    ///
    /// Returns an error if the activity fails or if the output cannot be deserialized.
    pub fn schedule_activity_typed<In: serde::Serialize, Out: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        name: impl Into<String>,
        input: &In,
    ) -> DurableFuture<Result<Out, String>> {
        let payload = crate::_typed_codec::Json::encode(input).expect("encode");
        self.schedule_activity(name, payload)
            .map(|r| r.and_then(|s| crate::_typed_codec::Json::decode::<Out>(&s)))
    }

    /// Schedule an activity routed to the worker owning the given session.
    ///
    /// If no worker owns the session, any worker can claim it on first fetch.
    /// Once claimed, all subsequent activities with the same `session_id` route
    /// to the claiming worker until the session unpins (idle timeout or worker death).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::OrchestrationContext;
    /// # async fn example(ctx: OrchestrationContext) -> Result<String, String> {
    /// let session_id = ctx.new_guid().await?;
    /// let result = ctx.schedule_activity_on_session("run_turn", "input", &session_id).await?;
    /// # Ok(result)
    /// # }
    /// ```
    pub fn schedule_activity_on_session(
        &self,
        name: impl Into<String>,
        input: impl Into<String>,
        session_id: impl Into<String>,
    ) -> DurableFuture<Result<String, String>> {
        self.schedule_activity_internal(name, input, Some(session_id.into()))
    }

    /// Typed version of schedule_activity_on_session that serializes input and deserializes output.
    ///
    /// # Errors
    ///
    /// Returns an error if the activity fails or if the output cannot be deserialized.
    pub fn schedule_activity_on_session_typed<
        In: serde::Serialize,
        Out: serde::de::DeserializeOwned + Send + 'static,
    >(
        &self,
        name: impl Into<String>,
        input: &In,
        session_id: impl Into<String>,
    ) -> DurableFuture<Result<Out, String>> {
        let payload = crate::_typed_codec::Json::encode(input).expect("encode");
        self.schedule_activity_on_session(name, payload, session_id)
            .map(|r| r.and_then(|s| crate::_typed_codec::Json::decode::<Out>(&s)))
    }

    /// Internal implementation for activity scheduling.
    fn schedule_activity_internal(
        &self,
        name: impl Into<String>,
        input: impl Into<String>,
        session_id: Option<String>,
    ) -> DurableFuture<Result<String, String>> {
        let name: String = name.into();
        let input: String = input.into();

        let mut inner = self.inner.lock().expect("Mutex should not be poisoned");

        let token = inner.emit_action(Action::CallActivity {
            scheduling_event_id: 0, // Will be assigned by replay engine
            name: name.clone(),
            input: input.clone(),
            session_id,
            tag: None,
        });
        drop(inner);

        let ctx = self.clone();
        let inner_future = std::future::poll_fn(move |_cx| {
            let inner = ctx.inner.lock().expect("Mutex should not be poisoned");
            if let Some(result) = inner.get_result(token) {
                match result {
                    CompletionResult::ActivityOk(s) => Poll::Ready(Ok(s.clone())),
                    CompletionResult::ActivityErr(e) => Poll::Ready(Err(e.clone())),
                    _ => Poll::Pending, // Wrong result type, keep waiting
                }
            } else {
                Poll::Pending
            }
        });

        DurableFuture::new(token, ScheduleKind::Activity { name }, self.clone(), inner_future)
    }

    /// Schedule a timer and return a cancellation-aware future.
    ///
    /// Timers are virtual constructs - dropping the future is a no-op since there's
    /// no external state to cancel. However, wrapping in `DurableFuture` maintains
    /// API consistency.
    pub fn schedule_timer(&self, delay: std::time::Duration) -> DurableFuture<()> {
        let delay_ms = delay.as_millis() as u64;

        let mut inner = self.inner.lock().expect("Mutex should not be poisoned");

        let now = inner.now_ms();
        let fire_at_ms = now.saturating_add(delay_ms);
        let token = inner.emit_action(Action::CreateTimer {
            scheduling_event_id: 0,
            fire_at_ms,
        });
        drop(inner);

        let ctx = self.clone();
        let inner_future = std::future::poll_fn(move |_cx| {
            let inner = ctx.inner.lock().expect("Mutex should not be poisoned");
            if let Some(result) = inner.get_result(token) {
                match result {
                    CompletionResult::TimerFired => Poll::Ready(()),
                    _ => Poll::Pending,
                }
            } else {
                Poll::Pending
            }
        });

        DurableFuture::new(token, ScheduleKind::Timer, self.clone(), inner_future)
    }

    /// Subscribe to an external event and return a cancellation-aware future.
    ///
    /// External waits are virtual constructs - dropping the future is a no-op since
    /// there's no external state to cancel. However, wrapping in `DurableFuture`
    /// maintains API consistency.
    pub fn schedule_wait(&self, name: impl Into<String>) -> DurableFuture<String> {
        let name: String = name.into();

        let mut inner = self.inner.lock().expect("Mutex should not be poisoned");

        let token = inner.emit_action(Action::WaitExternal {
            scheduling_event_id: 0,
            name: name.clone(),
        });
        drop(inner);

        let ctx = self.clone();
        let inner_future = std::future::poll_fn(move |_cx| {
            let inner = ctx.inner.lock().expect("Mutex should not be poisoned");
            // Only resolve once the token has been bound to a persisted schedule_id.
            // External events arriving before subscription binding are currently unsupported.
            if let Some(bound_id) = inner.get_bound_schedule_id(token)
                && let Some(data) = inner.get_external_event(bound_id)
            {
                return Poll::Ready(data.clone());
            }
            Poll::Pending
        });

        DurableFuture::new(
            token,
            ScheduleKind::ExternalWait { event_name: name },
            self.clone(),
            inner_future,
        )
    }

    /// Typed version of schedule_wait.
    pub fn schedule_wait_typed<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        name: impl Into<String>,
    ) -> DurableFuture<T> {
        self.schedule_wait(name)
            .map(|s| crate::_typed_codec::Json::decode::<T>(&s).expect("decode"))
    }

    /// Dequeue the next message from a named queue (FIFO mailbox semantics).
    ///
    /// Unlike `schedule_wait`, queued events use FIFO matching:
    /// - No positional pairing — any unresolved subscription gets the first unmatched arrival
    /// - Cancelled subscriptions are skipped (don't consume arrivals)
    /// - Events that arrive before a subscription are buffered until consumed
    /// - Events survive `continue_as_new` boundaries (carried forward)
    ///
    /// The caller enqueues messages with [`Client::enqueue_event`].
    pub fn dequeue_event(&self, queue: impl Into<String>) -> DurableFuture<String> {
        let name: String = queue.into();

        let mut inner = self.inner.lock().expect("Mutex should not be poisoned");

        let token = inner.emit_action(Action::DequeueEvent {
            scheduling_event_id: 0,
            name: name.clone(),
        });
        drop(inner);

        let ctx = self.clone();
        let inner_future = std::future::poll_fn(move |_cx| {
            let mut inner = ctx.inner.lock().expect("Mutex should not be poisoned");
            if let Some(bound_id) = inner.get_bound_schedule_id(token)
                && let Some(data) = inner.get_queue_message(bound_id)
            {
                return Poll::Ready(data);
            }
            Poll::Pending
        });

        DurableFuture::new(
            token,
            ScheduleKind::QueueDequeue { event_name: name },
            self.clone(),
            inner_future,
        )
    }

    /// Typed version of [`Self::dequeue_event`]. Deserializes the message payload as `T`.
    pub fn dequeue_event_typed<T: serde::de::DeserializeOwned>(
        &self,
        queue: impl Into<String>,
    ) -> impl Future<Output = T> {
        let fut = self.dequeue_event(queue);
        async move {
            let s = fut.await;
            crate::_typed_codec::Json::decode::<T>(&s).expect("decode")
        }
    }

    /// Subscribe to a persistent external event (mailbox semantics).
    ///
    /// Prefer [`Self::dequeue_event`] — this is a deprecated alias.
    #[deprecated(note = "Use dequeue_event() instead")]
    pub fn schedule_wait_persistent(&self, name: impl Into<String>) -> DurableFuture<String> {
        self.dequeue_event(name)
    }

    /// Typed version of schedule_wait_persistent.
    ///
    /// Prefer [`Self::dequeue_event_typed`] — this is a deprecated alias.
    #[deprecated(note = "Use dequeue_event_typed() instead")]
    pub fn schedule_wait_persistent_typed<T: serde::de::DeserializeOwned>(
        &self,
        name: impl Into<String>,
    ) -> impl Future<Output = T> {
        self.dequeue_event_typed(name)
    }

    /// V2: Subscribe to an external event with topic-based pub/sub matching.
    ///
    /// Same semantics as `schedule_wait`, but matches on both `name` AND `topic`.
    /// Feature-gated for replay engine extensibility verification.
    #[cfg(feature = "replay-version-test")]
    pub fn schedule_wait2(&self, name: impl Into<String>, topic: impl Into<String>) -> DurableFuture<String> {
        let name: String = name.into();
        let topic: String = topic.into();

        let mut inner = self.inner.lock().expect("Mutex should not be poisoned");

        let token = inner.emit_action(Action::WaitExternal2 {
            scheduling_event_id: 0,
            name: name.clone(),
            topic: topic.clone(),
        });
        drop(inner);

        let ctx = self.clone();
        let inner_future = std::future::poll_fn(move |_cx| {
            let inner = ctx.inner.lock().expect("Mutex should not be poisoned");
            if let Some(bound_id) = inner.get_bound_schedule_id(token)
                && let Some(data) = inner.get_external_event2(bound_id)
            {
                return Poll::Ready(data.clone());
            }
            Poll::Pending
        });

        DurableFuture::new(
            token,
            ScheduleKind::ExternalWait { event_name: name },
            self.clone(),
            inner_future,
        )
    }

    /// Schedule a sub-orchestration and return a cancellation-aware future.
    ///
    /// The child instance ID is auto-generated from the event ID with a parent prefix.
    ///
    /// Returns a [`DurableFuture`] that supports cancellation on drop. If the future
    /// is dropped without completing, a `CancelInstance` work item will be enqueued
    /// for the child orchestration.
    pub fn schedule_sub_orchestration(
        &self,
        name: impl Into<String>,
        input: impl Into<String>,
    ) -> DurableFuture<Result<String, String>> {
        self.schedule_sub_orchestration_versioned_with_id_internal(name, None, None, input)
    }

    /// Schedule a sub-orchestration with an explicit instance ID.
    ///
    /// The provided `instance` value is used exactly as the child instance ID,
    /// without any parent prefix. Use this when you need to control the exact
    /// instance ID for the sub-orchestration.
    ///
    /// For auto-generated instance IDs, use [`schedule_sub_orchestration`] instead.
    pub fn schedule_sub_orchestration_with_id(
        &self,
        name: impl Into<String>,
        instance: impl Into<String>,
        input: impl Into<String>,
    ) -> DurableFuture<Result<String, String>> {
        self.schedule_sub_orchestration_versioned_with_id_internal(name, None, Some(instance.into()), input)
    }

    /// Schedule a versioned sub-orchestration.
    ///
    /// If `version` is `Some`, that specific version is used.
    /// If `version` is `None`, the registry's policy (e.g., Latest) is used.
    pub fn schedule_sub_orchestration_versioned(
        &self,
        name: impl Into<String>,
        version: Option<String>,
        input: impl Into<String>,
    ) -> DurableFuture<Result<String, String>> {
        self.schedule_sub_orchestration_versioned_with_id_internal(name, version, None, input)
    }

    /// Schedule a versioned sub-orchestration with an explicit instance ID.
    ///
    /// The provided `instance` value is used exactly as the child instance ID,
    /// without any parent prefix.
    ///
    /// Returns a [`DurableFuture`] that supports cancellation on drop. If the future
    /// is dropped without completing, a `CancelInstance` work item will be enqueued
    /// for the child orchestration.
    pub fn schedule_sub_orchestration_versioned_with_id(
        &self,
        name: impl Into<String>,
        version: Option<String>,
        instance: impl Into<String>,
        input: impl Into<String>,
    ) -> DurableFuture<Result<String, String>> {
        self.schedule_sub_orchestration_versioned_with_id_internal(name, version, Some(instance.into()), input)
    }

    /// Internal implementation for sub-orchestration scheduling.
    ///
    /// If `instance` is `Some`, it's an explicit ID (no parent prefix).
    /// If `instance` is `None`, auto-generate from event ID (with parent prefix).
    fn schedule_sub_orchestration_versioned_with_id_internal(
        &self,
        name: impl Into<String>,
        version: Option<String>,
        instance: Option<String>,
        input: impl Into<String>,
    ) -> DurableFuture<Result<String, String>> {
        let name: String = name.into();
        let input: String = input.into();

        let mut inner = self.inner.lock().expect("Mutex should not be poisoned");

        // For explicit instance IDs, use them as-is (no parent prefix will be added).
        // For auto-generated, use placeholder that will be replaced with SUB_ORCH_AUTO_PREFIX + event_id
        // and parent prefix will be added.
        let action_instance = match &instance {
            Some(explicit_id) => explicit_id.clone(),
            None => format!("{}{}", SUB_ORCH_PENDING_PREFIX, inner.next_token + 1),
        };
        let token = inner.emit_action(Action::StartSubOrchestration {
            scheduling_event_id: 0,
            name: name.clone(),
            version,
            instance: action_instance.clone(),
            input: input.clone(),
        });
        drop(inner);

        let ctx = self.clone();
        let inner_future = std::future::poll_fn(move |_cx| {
            let inner = ctx.inner.lock().expect("Mutex should not be poisoned");
            if let Some(result) = inner.get_result(token) {
                match result {
                    CompletionResult::SubOrchOk(s) => Poll::Ready(Ok(s.clone())),
                    CompletionResult::SubOrchErr(e) => Poll::Ready(Err(e.clone())),
                    _ => Poll::Pending,
                }
            } else {
                Poll::Pending
            }
        });

        // For cancellation, we store the token. The consumption path will look up
        // the resolved instance ID from the sub_orchestration_instances mapping.
        DurableFuture::new(
            token,
            ScheduleKind::SubOrchestration { token },
            self.clone(),
            inner_future,
        )
    }

    /// Typed version of schedule_sub_orchestration.
    ///
    /// # Errors
    ///
    /// Returns an error if the sub-orchestration fails or if the output cannot be deserialized.
    pub fn schedule_sub_orchestration_typed<In: serde::Serialize, Out: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        name: impl Into<String>,
        input: &In,
    ) -> DurableFuture<Result<Out, String>> {
        let payload = crate::_typed_codec::Json::encode(input).expect("encode");
        self.schedule_sub_orchestration(name, payload)
            .map(|r| r.and_then(|s| crate::_typed_codec::Json::decode::<Out>(&s)))
    }

    /// Typed version of schedule_sub_orchestration_with_id.
    ///
    /// # Errors
    ///
    /// Returns an error if the sub-orchestration fails or if the output cannot be deserialized.
    pub fn schedule_sub_orchestration_with_id_typed<
        In: serde::Serialize,
        Out: serde::de::DeserializeOwned + Send + 'static,
    >(
        &self,
        name: impl Into<String>,
        instance: impl Into<String>,
        input: &In,
    ) -> DurableFuture<Result<Out, String>> {
        let payload = crate::_typed_codec::Json::encode(input).expect("encode");
        self.schedule_sub_orchestration_with_id(name, instance, payload)
            .map(|r| r.and_then(|s| crate::_typed_codec::Json::decode::<Out>(&s)))
    }

    /// Await all futures concurrently using `futures::future::join_all`.
    /// Works with any `Future` type.
    pub async fn join<T, F>(&self, futures: Vec<F>) -> Vec<T>
    where
        F: Future<Output = T>,
    {
        ::futures::future::join_all(futures).await
    }

    /// Simplified join for exactly 2 futures (convenience method).
    pub async fn join2<T1, T2, F1, F2>(&self, f1: F1, f2: F2) -> (T1, T2)
    where
        F1: Future<Output = T1>,
        F2: Future<Output = T2>,
    {
        ::futures::future::join(f1, f2).await
    }

    /// Simplified join for exactly 3 futures (convenience method).
    pub async fn join3<T1, T2, T3, F1, F2, F3>(&self, f1: F1, f2: F2, f3: F3) -> (T1, T2, T3)
    where
        F1: Future<Output = T1>,
        F2: Future<Output = T2>,
        F3: Future<Output = T3>,
    {
        ::futures::future::join3(f1, f2, f3).await
    }

    /// Simplified select over 2 futures: returns the result of whichever completes first.
    /// Select over 2 futures with potentially different output types.
    ///
    /// Returns `Either2::First(result)` if first future wins, `Either2::Second(result)` if second wins.
    /// Uses futures::select_biased! for determinism (first branch polled first).
    ///
    /// # Example: Activity with timeout
    /// ```rust,no_run
    /// # use duroxide::{OrchestrationContext, Either2};
    /// # use std::time::Duration;
    /// # async fn example(ctx: OrchestrationContext) -> Result<String, String> {
    /// let work = ctx.schedule_activity("SlowWork", "input");
    /// let timeout = ctx.schedule_timer(Duration::from_secs(30));
    ///
    /// match ctx.select2(work, timeout).await {
    ///     Either2::First(result) => result,
    ///     Either2::Second(()) => Err("Operation timed out".to_string()),
    /// }
    /// # }
    /// ```
    pub async fn select2<T1, T2, F1, F2>(&self, f1: F1, f2: F2) -> Either2<T1, T2>
    where
        F1: Future<Output = T1>,
        F2: Future<Output = T2>,
    {
        use ::futures::FutureExt;
        let mut f1 = std::pin::pin!(f1.fuse());
        let mut f2 = std::pin::pin!(f2.fuse());
        ::futures::select_biased! {
            result = f1 => Either2::First(result),
            result = f2 => Either2::Second(result),
        }
    }

    /// Select over 3 futures with potentially different output types.
    ///
    /// Returns `Either3::First/Second/Third(result)` depending on which future completes first.
    /// Uses futures::select_biased! for determinism (earlier branches polled first).
    pub async fn select3<T1, T2, T3, F1, F2, F3>(&self, f1: F1, f2: F2, f3: F3) -> Either3<T1, T2, T3>
    where
        F1: Future<Output = T1>,
        F2: Future<Output = T2>,
        F3: Future<Output = T3>,
    {
        use ::futures::FutureExt;
        let mut f1 = std::pin::pin!(f1.fuse());
        let mut f2 = std::pin::pin!(f2.fuse());
        let mut f3 = std::pin::pin!(f3.fuse());
        ::futures::select_biased! {
            result = f1 => Either3::First(result),
            result = f2 => Either3::Second(result),
            result = f3 => Either3::Third(result),
        }
    }
}

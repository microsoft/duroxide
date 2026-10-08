// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Worker (activity) dispatcher implementation for Runtime
//!
//! This module contains the worker dispatcher logic that:
//! - Spawns concurrent activity workers
//! - Fetches and executes activity work items
//! - Handles activity completion and failure atomically
//! - Supports cooperative activity cancellation
//!
//! # Architecture
//!
//! The worker dispatcher spawns N concurrent worker tasks, each of which:
//! 1. Fetches activity work items from the provider
//! 2. Checks if the orchestration is still running (cancellation support)
//! 3. Spawns an activity manager task for lock renewal and cancellation detection
//! 4. Executes the activity handler

// Worker dispatcher uses Mutex locks - poison indicates a panic and should propagate
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
//! 5. Handles completion, failure, or cancellation outcomes
//!
//! # Cancellation Flow
//!
//! When an orchestration reaches a terminal state while an activity is running:
//! 1. The activity manager detects the terminal state during lock renewal
//! 2. It signals the activity's cancellation token
//! 3. The worker dispatcher waits up to the grace period for the activity to complete
//! 4. If the activity doesn't complete, it's aborted and the work item is dropped

use crate::providers::WorkItem;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use super::super::task_group::{
    FailureKind, TaskFailure, TaskGroup, TaskResult, TaskRole, protect_future_observing_panic,
};
use super::super::{Runtime, registry};

// ============================================================================
// Types
// ============================================================================

/// Tracks distinct active sessions across all worker slots in a single runtime.
///
/// A single `SessionTracker` instance is shared by every `worker_concurrency`
/// slot spawned from the same `Runtime`. It maps session_id → in-flight
/// activity count. The number of keys gives the distinct session count,
/// which is compared against `max_sessions_per_runtime`. When a session's
/// activity count reaches 0 the entry is removed, freeing a session slot.
struct SessionTracker {
    inner: std::sync::Mutex<std::collections::HashMap<String, usize>>,
}

impl SessionTracker {
    fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Number of distinct sessions currently in flight.
    fn distinct_count(&self) -> usize {
        self.inner.lock().expect("SessionTracker lock poisoned").len()
    }

    /// Increment the in-flight activity count for `session_id`.
    /// Inserts a new entry if this is the first activity for the session.
    fn increment(&self, session_id: &str) {
        let mut map = self.inner.lock().expect("SessionTracker lock poisoned");
        *map.entry(session_id.to_string()).or_insert(0) += 1;
    }

    /// Decrement the count for `session_id`, removing the entry when it reaches 0.
    fn decrement(&self, session_id: &str) {
        let mut map = self.inner.lock().expect("SessionTracker lock poisoned");
        if let std::collections::hash_map::Entry::Occupied(mut entry) = map.entry(session_id.to_string()) {
            let count = entry.get_mut();
            *count -= 1;
            if *count == 0 {
                entry.remove();
            }
        }
    }
}

/// RAII guard that releases a session slot on drop.
///
/// Created via `SessionGuard::new()`. Increments the tracker's count for
/// the session on creation; decrements (and removes at zero) on drop.
struct SessionGuard {
    tracker: Arc<SessionTracker>,
    session_id: String,
}

impl SessionGuard {
    /// Create a guard that increments the tracker for `session_id`.
    fn new(tracker: &Arc<SessionTracker>, session_id: &str) -> Self {
        tracker.increment(session_id);
        Self {
            tracker: Arc::clone(tracker),
            session_id: session_id.to_string(),
        }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.tracker.decrement(&self.session_id);
    }
}

/// Outcome of activity execution, used for metrics and ack handling.
#[derive(Debug, Clone, Copy)]
enum ActivityOutcome {
    /// Activity completed successfully
    Success,
    /// Activity failed with an application error
    AppError,
    /// Orchestration was cancelled, activity result dropped
    Cancelled,
}

/// Context for processing a single activity work item.
///
/// Groups together all the data needed to execute an activity,
/// reducing parameter count in helper functions.
struct ActivityWorkContext {
    /// The orchestration instance ID
    instance: String,
    /// The execution ID within the instance
    execution_id: u64,
    /// The activity's unique ID within the execution
    activity_id: u64,
    /// The activity handler name
    activity_name: String,
    /// Serialized input for the activity
    input: String,
    /// Lock token for the work item
    lock_token: String,
    /// Number of times this message has been fetched
    attempt_count: u32,
    /// Serialized work item (for poison message reporting)
    item_serialized: String,
    /// Worker ID for logging
    worker_id: String,
    /// Optional session ID for worker affinity routing
    session_id: Option<String>,
    /// Optional activity tag for worker specialization
    tag: Option<String>,
}

// ============================================================================
// Runtime Implementation
// ============================================================================

impl Runtime {
    /// Start the worker dispatcher with N concurrent workers for executing activities.
    ///
    /// Each worker runs in a loop, fetching and processing activity work items.
    /// Workers share the same activity registry and admission signal.
    pub(in crate::runtime) async fn run_work_dispatcher(self: Arc<Self>) -> TaskResult {
        Arc::clone(&self).own_task(TaskRole::WorkDispatcher, move |group| Box::pin(async move {
        let concurrency = self.options.worker_concurrency;
            let mut session_owner_ids: Vec<String> = Vec::new();

            // Tracks distinct active sessions across all worker slots in this
            // runtime. When distinct_count() reaches max_sessions_per_runtime,
            // ALL slots stop claiming new sessions by switching to non-session mode.
            let session_tracker = Arc::new(SessionTracker::new());

            // Derive per-slot identities:
            //   worker_id      – unique per slot, used for logging/tracing
            //   session_owner  – identity used for session lock claims
            //
            // With a stable worker_node_id all slots share the same session
            // owner so any idle slot can serve any owned session.
            // Without one, each slot gets an ephemeral identity.
            let stable_node_id = self.options.worker_node_id.clone();

            for worker_idx in 0..concurrency {
                let rt = Arc::clone(&self);
                let activities = Arc::clone(&self.activity_registry);
                let session_tracker_clone = Arc::clone(&session_tracker);

                let suffix = stable_node_id.as_deref().unwrap_or(&self.runtime_id);
                let worker_id = format!("work-{worker_idx}-{suffix}");
                let session_owner = stable_node_id.clone().unwrap_or_else(|| worker_id.clone());

                // Collect unique session owner IDs for the session manager
                if !session_owner_ids.contains(&session_owner) {
                    session_owner_ids.push(session_owner.clone());
                }

                group.spawn(TaskRole::WorkerSlot.name(), Arc::clone(&rt).own_task(TaskRole::WorkerSlot, move |_| Box::pin(async move {
                    let mut consecutive_retryable_errors: u32 = 0;

                    loop {
                        if rt.is_stopping() {
                            break;
                        }

                        let min_interval = rt.options.dispatcher_min_poll_interval;
                        let start_time = std::time::Instant::now();

                        let work_found = match process_next_work_item(
                            &rt,
                            &activities,
                            &worker_id,
                            &session_owner,
                            &session_tracker_clone,
                        )
                        .await
                        {
                            Ok(found) => {
                                consecutive_retryable_errors = 0;
                                found
                            }
                            Err(e) if e.is_retryable() => {
                                // Exponential backoff for retryable errors (database locks, etc.)
                                consecutive_retryable_errors = consecutive_retryable_errors.saturating_add(1);
                                let backoff_ms = (100 * 2_u64.pow(consecutive_retryable_errors.min(5))).min(3000);
                                warn!(
                                    "Error fetching work item (retryable, attempt {}): {:?}, backing off {}ms",
                                    consecutive_retryable_errors, e, backoff_ms
                                );
                                rt.sleep_while_running(Duration::from_millis(backoff_ms)).await;
                                continue;
                            }
                            Err(e) => {
                                // Permanent errors - log and continue with normal polling
                                warn!("Error fetching work item (permanent): {:?}", e);
                                consecutive_retryable_errors = 0;
                                rt.sleep_while_running(Duration::from_millis(100)).await;
                                continue;
                            }
                        };

                        // Enforce minimum polling interval to prevent hot loops
                        if !work_found {
                            enforce_min_poll_interval(start_time, min_interval, &rt).await;
                        }
                    }
                    Ok(())
                })));
            }

            // Spawn a single session manager background task for heartbeat + cleanup
            let session_rt = Arc::clone(&self);
            let workers_retired = CancellationToken::new();
            group.spawn_until_join(TaskRole::SessionManager.name(),
                run_session_manager(session_rt, workers_retired.clone(), session_owner_ids),
                workers_retired);
            #[cfg(feature = "test-hooks")]
            self.hooks().checkpoint(crate::runtime::test_hooks::LifecyclePoint::ParentWork(TaskRole::WorkDispatcher)).await;
            Ok(())
        })).await
    }
}

/// Process the next available work item from the queue.
///
/// Returns:
/// - `Ok(true)` if work was found and processed
/// - `Ok(false)` if no work was available
/// - `Err(e)` if fetch failed (caller handles backoff)
async fn process_next_work_item(
    rt: &Arc<Runtime>,
    activities: &Arc<registry::ActivityRegistry>,
    worker_id: &str,
    session_worker_id: &str,
    session_tracker: &Arc<SessionTracker>,
) -> Result<bool, crate::providers::ProviderError> {
    // Check session capacity: if at limit, only fetch non-session items
    let at_session_capacity = session_tracker.distinct_count() >= rt.options.max_sessions_per_runtime;

    let session_config = if at_session_capacity {
        None
    } else {
        Some(crate::providers::SessionFetchConfig {
            owner_id: session_worker_id.to_string(),
            lock_timeout: rt.options.session_lock_timeout,
        })
    };

    let (item, token, attempt_count) = match rt
        .history_store
        .fetch_work_item(
            rt.options.worker_lock_timeout,
            rt.options.dispatcher_long_poll_timeout,
            session_config.as_ref(),
            &rt.options.worker_tag_filter,
        )
        .await?
    {
        Some(result) => result,
        None => return Ok(false),
    };

    if rt.is_stopping() {
        abandon_for_runtime_stop(rt, &token).await;
        return Ok(true);
    }

    let item_serialized = serde_json::to_string(&item).unwrap_or_default();

    match item {
        WorkItem::ActivityExecute {
            instance,
            execution_id,
            id,
            name,
            input,
            session_id,
            tag,
        } => {
            // If this is a session-bound item, acquire a session slot via a guard.
            // The guard releases the slot on drop (when activity processing completes).
            // Multiple activities on the same session share one slot.
            let _session_guard = if let Some(ref sid) = session_id {
                let guard = SessionGuard::new(session_tracker, sid);
                // Re-check capacity after acquiring. The pre-fetch check is a hint
                // to avoid unnecessary fetches, but two workers can race past it.
                // If we're now over capacity (another worker won the race for a
                // different session), abandon this item so it retries later.
                if session_tracker.distinct_count() > rt.options.max_sessions_per_runtime {
                    drop(guard);
                    tracing::debug!(
                        target: "duroxide::runtime",
                        session_id = %sid,
                        worker_id = %worker_id,
                        "Session capacity exceeded after fetch (race), abandoning work item"
                    );
                    let _ = rt
                        .history_store
                        .abandon_work_item(&token, Some(Duration::from_millis(100)), true)
                        .await;
                    return Ok(true);
                }
                Some(guard)
            } else {
                None
            };

            let ctx = ActivityWorkContext {
                instance,
                execution_id,
                activity_id: id,
                activity_name: name,
                input,
                lock_token: token,
                attempt_count,
                item_serialized,
                worker_id: worker_id.to_string(),
                session_id,
                tag,
            };

            // Cancellation is detected during lock renewal (lock stealing).
            if ctx.attempt_count > rt.options.max_attempts {
                // Handle poison messages
                handle_poison_message(rt, &ctx).await;
            } else {
                // Execute activity with cancellation support
                if let Err(failures) = execute_activity(rt, activities, ctx).await {
                    rt.record_faults(&failures);
                }
            }
        }
        other => {
            error!(?other, "unexpected WorkItem in Worker dispatcher; state corruption");
            panic!("unexpected WorkItem in Worker dispatcher");
        }
    }

    Ok(true)
}

/// Enforce minimum polling interval to prevent hot loops.
async fn enforce_min_poll_interval(start_time: std::time::Instant, min_interval: Duration, rt: &Runtime) {
    let elapsed = start_time.elapsed();
    if elapsed < min_interval {
        let sleep_duration = min_interval - elapsed;
        rt.sleep_while_running(sleep_duration).await;
    } else {
        tokio::task::yield_now().await;
    }
}

// ============================================================================
// Activity Processing
// ============================================================================

/// Handle a poison message (activity fetched too many times).
async fn handle_poison_message(rt: &Arc<Runtime>, ctx: &ActivityWorkContext) {
    warn!(
        instance = %ctx.instance,
        activity_name = %ctx.activity_name,
        activity_id = ctx.activity_id,
        attempt_count = ctx.attempt_count,
        max_attempts = rt.options.max_attempts,
        "Activity message exceeded max attempts, marking as poison"
    );

    let error = crate::ErrorDetails::Poison {
        attempt_count: ctx.attempt_count,
        max_attempts: rt.options.max_attempts,
        message_type: crate::PoisonMessageType::Activity {
            instance: ctx.instance.clone(),
            execution_id: ctx.execution_id,
            activity_name: ctx.activity_name.clone(),
            activity_id: ctx.activity_id,
        },
        message: ctx.item_serialized.clone(),
    };

    let _ = rt
        .history_store
        .ack_work_item(
            &ctx.lock_token,
            Some(WorkItem::ActivityFailed {
                instance: ctx.instance.clone(),
                execution_id: ctx.execution_id,
                id: ctx.activity_id,
                details: error,
            }),
        )
        .await;

    rt.record_activity_poison();
}

// ============================================================================
// Activity Execution
// ============================================================================

/// Execute an activity with full cancellation support.
async fn execute_activity(
    rt: &Arc<Runtime>,
    activities: &Arc<registry::ActivityRegistry>,
    ctx: ActivityWorkContext,
) -> TaskResult {
    let rt = Arc::clone(rt);
    let activities = Arc::clone(activities);
    Arc::clone(&rt)
        .own_work("activity-item", move |group| {
            Box::pin(async move {
                let cancellation_token = CancellationToken::new();
                let retired = CancellationToken::new();
                group.spawn_until_join(
                    TaskRole::ActivityManager.name(),
                    run_activity_manager(
                        Arc::clone(&rt),
                        ctx.lock_token.clone(),
                        retired.clone(),
                        cancellation_token.clone(),
                    ),
                    retired,
                );
                #[cfg(feature = "test-hooks")]
                rt.hooks()
                    .checkpoint(crate::runtime::test_hooks::LifecyclePoint::ParentWork(
                        TaskRole::WorkerSlot,
                    ))
                    .await;

                let activity_ctx = build_activity_context(&rt, &ctx, cancellation_token.clone()).await;
                if rt.shutdown_state.force_signal().is_cancelled() {
                    abandon_for_runtime_stop(&rt, &ctx.lock_token).await;
                    return Ok(());
                }

                tracing::debug!(
                    target: "duroxide::runtime",
                    instance_id = %ctx.instance,
                    execution_id = %ctx.execution_id,
                    activity_name = %ctx.activity_name,
                    activity_id = %ctx.activity_id,
                    worker_id = %ctx.worker_id,
                    activity_tag = ?ctx.tag,
                    "Activity started"
                );

                let start_time = std::time::Instant::now();

                match activities.resolve_handler(&ctx.activity_name) {
                    Some((_version, handler)) => {
                        run_activity_with_cancellation(
                            &rt,
                            &ctx,
                            handler,
                            activity_ctx,
                            cancellation_token,
                            group,
                            start_time,
                        )
                        .await?;
                    }
                    None => {
                        abandon_unregistered_activity(&rt, &ctx).await;
                    }
                }
                Ok(())
            })
        })
        .await
}

/// Build the ActivityContext with orchestration metadata.
async fn build_activity_context(
    rt: &Arc<Runtime>,
    ctx: &ActivityWorkContext,
    cancellation_token: CancellationToken,
) -> crate::ActivityContext {
    let descriptor = rt.get_orchestration_descriptor(&ctx.instance).await;
    let (orch_name, orch_version) = descriptor
        .map(|d| (d.name, d.version))
        .unwrap_or_else(|| ("unknown".to_string(), "unknown".to_string()));

    crate::ActivityContext::new_with_cancellation(
        ctx.instance.clone(),
        ctx.execution_id,
        orch_name,
        orch_version,
        ctx.activity_name.clone(),
        ctx.activity_id,
        ctx.worker_id.clone(),
        ctx.session_id.clone(),
        ctx.tag.clone(),
        cancellation_token,
        Arc::clone(&rt.history_store),
    )
}

/// Run an activity with cancellation support using `tokio::select!`.
async fn run_activity_with_cancellation(
    rt: &Arc<Runtime>,
    ctx: &ActivityWorkContext,
    handler: Arc<dyn crate::runtime::ActivityHandler>,
    activity_ctx: crate::ActivityContext,
    cancellation_token: CancellationToken,
    group: &mut TaskGroup,
    start_time: std::time::Instant,
) -> TaskResult {
    let input = ctx.input.clone();
    let (send, mut completion) = oneshot::channel();
    let activity_rt = Arc::clone(rt);
    let activity = group.spawn_leaf(
        TaskRole::ActivityInvocation.name(),
        Arc::clone(&activity_rt).own_task(TaskRole::ActivityInvocation, move |_| {
            Box::pin(async move {
                #[cfg(feature = "test-hooks")]
                activity_rt
                    .hooks()
                    .checkpoint(crate::runtime::test_hooks::LifecyclePoint::ParentWork(
                        TaskRole::ActivityInvocation,
                    ))
                    .await;
                let mut output = None;
                let mut panic_message = None;
                let report = protect_future_observing_panic(
                    "activity-callback",
                    async {
                        output = Some(handler.invoke(activity_ctx, input).await);
                        Ok(())
                    },
                    &CancellationToken::new(),
                    |payload| {
                        panic_message = Some(
                            payload
                                .downcast_ref::<String>()
                                .cloned()
                                .or_else(|| payload.downcast_ref::<&str>().map(|message| (*message).to_string()))
                                .unwrap_or_else(|| "non-string panic payload".to_string()),
                        );
                    },
                )
                .await;
                if !report.failures.is_empty() {
                    if report
                        .failures
                        .iter()
                        .all(|failure| failure.kind == FailureKind::PollPanicked)
                    {
                        output = Some(Err(format!(
                            "activity callback panicked: {}",
                            panic_message.expect("poll panic captured its application message")
                        )));
                    } else {
                        return Err(report.failures);
                    }
                }
                // The parent still joins the leaf before acknowledging any result.
                let _ = send.send(output.expect("completed callback has an outcome"));
                Ok(())
            })
        }),
    );

    let (ack_result, outcome) = tokio::select! {
        biased;
        result = &mut completion => {
            if let Err(failures) = group.join(activity).await {
                abandon_for_runtime_stop(rt, &ctx.lock_token).await;
                return Err(failures);
            }
            match result {
                Ok(Ok(result)) => handle_activity_success(rt, ctx, result, start_time).await,
                Ok(Err(error)) => handle_activity_error(rt, ctx, error, start_time).await,
                Err(_) => {
                    abandon_for_runtime_stop(rt, &ctx.lock_token).await;
                    return Err(vec![TaskFailure { task: "activity-invocation-result", kind: FailureKind::Operation }]);
                }
            }
        },
        () = rt.shutdown_state.force_signal().cancelled() => {
            group.abort_leaves();
            let joined = group.join(activity).await;
            abandon_for_runtime_stop(rt, &ctx.lock_token).await;
            return joined;
        },
        () = cancellation_token.cancelled() => {
            let grace = rt.options.activity_cancellation_grace_period;

            tracing::info!(
                target: "duroxide::runtime",
                instance = %ctx.instance,
                execution_id = %ctx.execution_id,
                activity_name = %ctx.activity_name,
                activity_id = %ctx.activity_id,
                worker_id = %ctx.worker_id,
                activity_tag = ?ctx.tag,
                grace_ms = %grace.as_millis(),
                "Orchestration terminated, waiting for activity grace period"
            );

            let runtime_forced = tokio::select! {
                biased;
                () = rt.shutdown_state.force_signal().cancelled() => true,
                _ = &mut completion => false,
                () = tokio::time::sleep(grace) => {
                    group.abort_leaves();
                    false
                },
            };
            if runtime_forced {
                group.abort_leaves();
            }
            let joined = group.join(activity).await;
            if runtime_forced || joined.is_err() {
                abandon_for_runtime_stop(rt, &ctx.lock_token).await;
                return joined;
            }

            // Record metrics and ack (drop result since orchestration is terminal)
            let duration_seconds = start_time.elapsed().as_secs_f64();
            rt.record_activity_execution(&ctx.activity_name, "cancelled", duration_seconds, 0, ctx.tag.as_deref());

            let result = rt.history_store.ack_work_item(&ctx.lock_token, None).await;
            if let Err(e) = &result {
                tracing::warn!(
                    target: "duroxide::runtime",
                    instance = %ctx.instance,
                    activity_id = %ctx.activity_id,
                    error = %e,
                    "Failed to ack cancelled activity work item"
                );
            }
            (result, ActivityOutcome::Cancelled)
        }
    };
    handle_activity_outcome(rt, ctx, ack_result, outcome).await;
    Ok(())
}

async fn abandon_for_runtime_stop(rt: &Runtime, token: &str) {
    if rt.history_store.abandon_work_item(token, None, true).await.is_err() {
        warn!(target: "duroxide::runtime::lifecycle", category = "stop_abandon_activity_failed",
            "Unfinished activity could not be abandoned; lock-expiry recovery remains available");
    }
}

// ============================================================================
// Activity Completion Handlers
// ============================================================================

/// Handle successful activity completion.
async fn handle_activity_success(
    rt: &Arc<Runtime>,
    ctx: &ActivityWorkContext,
    result: String,
    start_time: std::time::Instant,
) -> (Result<(), crate::providers::ProviderError>, ActivityOutcome) {
    let duration_ms = start_time.elapsed().as_millis() as u64;
    let duration_seconds = duration_ms as f64 / 1000.0;

    tracing::debug!(
        target: "duroxide::runtime",
        instance_id = %ctx.instance,
        execution_id = %ctx.execution_id,
        activity_name = %ctx.activity_name,
        activity_id = %ctx.activity_id,
        worker_id = %ctx.worker_id,
        activity_tag = ?ctx.tag,
        outcome = "success",
        duration_ms = %duration_ms,
        result_size = %result.len(),
        "Activity completed"
    );

    rt.record_activity_execution(&ctx.activity_name, "success", duration_seconds, 0, ctx.tag.as_deref());

    let ack_result = rt
        .history_store
        .ack_work_item(
            &ctx.lock_token,
            Some(WorkItem::ActivityCompleted {
                instance: ctx.instance.clone(),
                execution_id: ctx.execution_id,
                id: ctx.activity_id,
                result,
            }),
        )
        .await;

    (ack_result, ActivityOutcome::Success)
}

/// Handle activity application error.
async fn handle_activity_error(
    rt: &Arc<Runtime>,
    ctx: &ActivityWorkContext,
    error: String,
    start_time: std::time::Instant,
) -> (Result<(), crate::providers::ProviderError>, ActivityOutcome) {
    let duration_ms = start_time.elapsed().as_millis() as u64;
    let duration_seconds = duration_ms as f64 / 1000.0;

    tracing::warn!(
        target: "duroxide::runtime",
        instance_id = %ctx.instance,
        execution_id = %ctx.execution_id,
        activity_name = %ctx.activity_name,
        activity_id = %ctx.activity_id,
        worker_id = %ctx.worker_id,
        activity_tag = ?ctx.tag,
        outcome = "app_error",
        duration_ms = %duration_ms,
        error = %error,
        "Activity failed (application error)"
    );

    rt.record_activity_execution(&ctx.activity_name, "app_error", duration_seconds, 0, ctx.tag.as_deref());

    let ack_result = rt
        .history_store
        .ack_work_item(
            &ctx.lock_token,
            Some(WorkItem::ActivityFailed {
                instance: ctx.instance.clone(),
                execution_id: ctx.execution_id,
                id: ctx.activity_id,
                details: crate::ErrorDetails::Application {
                    kind: crate::AppErrorKind::ActivityFailed,
                    message: error,
                    retryable: false,
                },
            }),
        )
        .await;

    (ack_result, ActivityOutcome::AppError)
}

/// Abandon unregistered activity with exponential backoff for rolling deployment support.
///
/// The poison message handling will eventually fail the activity if genuinely missing.
async fn abandon_unregistered_activity(rt: &Arc<Runtime>, ctx: &ActivityWorkContext) {
    let backoff = rt.options.unregistered_backoff.delay(ctx.attempt_count);
    let remaining_attempts = rt.options.max_attempts.saturating_sub(ctx.attempt_count);

    tracing::warn!(
        target: "duroxide::runtime",
        instance = %ctx.instance,
        execution_id = %ctx.execution_id,
        activity_name = %ctx.activity_name,
        activity_id = %ctx.activity_id,
        worker_id = %ctx.worker_id,
        activity_tag = ?ctx.tag,
        attempt_count = %ctx.attempt_count,
        max_attempts = %rt.options.max_attempts,
        remaining_attempts = %remaining_attempts,
        backoff_secs = %backoff.as_secs_f32(),
        "Activity not registered, abandoning with {:.1}s backoff (will poison in {} more attempts)",
        backoff.as_secs_f32(),
        remaining_attempts
    );

    // Abandon with delay - poison handling will eventually terminate if genuinely missing
    let _ = rt
        .history_store
        .abandon_work_item(&ctx.lock_token, Some(backoff), false)
        .await;
}

// ============================================================================
// Outcome Handling
// ============================================================================

/// Handle the final outcome of activity execution.
async fn handle_activity_outcome(
    rt: &Arc<Runtime>,
    ctx: &ActivityWorkContext,
    ack_result: Result<(), crate::providers::ProviderError>,
    outcome: ActivityOutcome,
) {
    match ack_result {
        Ok(()) => match outcome {
            ActivityOutcome::Success => rt.record_activity_success(),
            ActivityOutcome::AppError => rt.record_activity_app_error(),
            ActivityOutcome::Cancelled => {}
        },
        Err(e) => {
            warn!(
                instance = %ctx.instance,
                execution_id = ctx.execution_id,
                activity_id = ctx.activity_id,
                worker_id = %ctx.worker_id,
                error = %e,
                "worker: atomic ack failed, abandoning work item"
            );
            let _ = rt
                .history_store
                .abandon_work_item(&ctx.lock_token, Some(Duration::from_millis(100)), false)
                .await;
            rt.record_activity_infra_error();
        }
    }
}

// ============================================================================
// Activity Manager (Lock Renewal + Cancellation Detection)
// ============================================================================

/// Calculate the renewal interval based on lock timeout and buffer settings.
///
/// - If timeout >= 15s: renew at `(timeout - buffer)`, minimum 1s
/// - If timeout < 15s: renew at `0.5 * timeout`, minimum 1s
fn calculate_renewal_interval(lock_timeout: Duration, buffer: Duration) -> Duration {
    if lock_timeout >= Duration::from_secs(15) {
        let buffer = buffer.min(lock_timeout);
        lock_timeout
            .checked_sub(buffer)
            .unwrap_or(Duration::from_secs(1))
            .max(Duration::from_secs(1))
    } else {
        let half = (lock_timeout.as_secs_f64() * 0.5).ceil().max(1.0);
        Duration::from_secs_f64(half)
    }
}

/// Spawn a background task to manage an in-flight activity.
///
/// Handles: lock renewal, cancellation detection, and cancellation signaling.
async fn run_activity_manager(
    rt: Arc<Runtime>,
    token: String,
    retired: CancellationToken,
    cancellation_token: CancellationToken,
) -> TaskResult {
    Arc::clone(&rt)
        .own_task(TaskRole::ActivityManager, move |_| {
            Box::pin(async move {
                let lock_timeout = rt.options.worker_lock_timeout;
                let buffer = rt.options.worker_lock_renewal_buffer;
                let renewal_interval = calculate_renewal_interval(lock_timeout, buffer);

                tracing::debug!(
                    target: "duroxide::runtime::worker",
                    lock_token = %token,
                    lock_timeout_secs = %lock_timeout.as_secs(),
                    renewal_interval_secs = %renewal_interval.as_secs(),
                    "Spawning activity manager"
                );

                #[cfg(feature = "test-hooks")]
                rt.hooks()
                    .checkpoint(crate::runtime::test_hooks::LifecyclePoint::ParentWork(
                        TaskRole::ActivityManager,
                    ))
                    .await;
                let mut interval = tokio::time::interval(renewal_interval);
                interval.tick().await; // Skip first immediate tick

                loop {
                    tokio::select! {
                        biased;
                        () = retired.cancelled() => break,
                        () = rt.shutdown_state.force_signal().cancelled() => break,
                        _ = interval.tick() => {}
                    }

                    match rt.history_store.renew_work_item_lock(&token, lock_timeout).await {
                        Ok(()) => {
                            // Lock renewed successfully - orchestration is still running
                            tracing::trace!(
                                target: "duroxide::runtime::worker",
                                lock_token = %token,
                                extend_secs = %lock_timeout.as_secs(),
                                "Work item lock renewed"
                            );
                        }
                        Err(e) => {
                            // Lock renewal failed - activity was cancelled (lock stolen) or lock expired
                            tracing::info!(
                                target: "duroxide::runtime::worker",
                                lock_token = %token,
                                error = %e,
                                "Lock renewal failed, signaling activity cancellation (lock was stolen or expired)"
                            );
                            cancellation_token.cancel();
                            break;
                        }
                    }
                }

                tracing::debug!(
                    target: "duroxide::runtime::worker",
                    lock_token = %token,
                    "Activity manager stopped"
                );
                Ok(())
            })
        })
        .await
}

// ============================================================================
// Session Manager (Lock Renewal + Cleanup)
// ============================================================================

/// Background task that periodically:
/// 1. Renews session locks for all non-idle sessions owned by this runtime's workers
/// 2. Cleans up orphaned session rows (expired locks, no pending work items)
async fn run_session_manager(rt: Arc<Runtime>, retired: CancellationToken, worker_ids: Vec<String>) -> TaskResult {
    Arc::clone(&rt)
        .own_task(TaskRole::SessionManager, move |_| {
            Box::pin(async move {
                #[cfg(feature = "test-hooks")]
                rt.hooks()
                    .checkpoint(crate::runtime::test_hooks::LifecyclePoint::ParentWork(
                        TaskRole::SessionManager,
                    ))
                    .await;
                let renewal_interval =
                    calculate_renewal_interval(rt.options.session_lock_timeout, rt.options.session_lock_renewal_buffer);
                let cleanup_interval = rt.options.session_cleanup_interval;

                let mut renewal_ticker = tokio::time::interval(renewal_interval);
                renewal_ticker.tick().await; // Skip immediate first tick

                let mut cleanup_ticker = tokio::time::interval(cleanup_interval);
                cleanup_ticker.tick().await; // Skip immediate first tick

                // Pre-compute the &str slice for the batched provider call
                let owner_refs: Vec<&str> = worker_ids.iter().map(|s| s.as_str()).collect();

                tracing::debug!(
                    target: "duroxide::runtime::worker",
                    renewal_interval_secs = %renewal_interval.as_secs(),
                    cleanup_interval_secs = %cleanup_interval.as_secs(),
                    worker_count = %worker_ids.len(),
                    "Session manager started"
                );

                loop {
                    tokio::select! {
                        biased;
                        () = retired.cancelled() => break,
                        () = rt.shutdown_state.force_signal().cancelled() => break,
                        _ = renewal_ticker.tick() => {
                            // Single batched call for all worker IDs
                            match rt.history_store.renew_session_lock(
                                &owner_refs,
                                rt.options.session_lock_timeout,
                                rt.options.session_idle_timeout,
                            ).await {
                                Ok(count) => {
                                    if count > 0 {
                                        tracing::trace!(
                                            target: "duroxide::runtime::worker",
                                            sessions_renewed = %count,
                                            "Session locks renewed"
                                        );
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        target: "duroxide::runtime::worker",
                                        error = %e,
                                        "Session lock renewal failed"
                                    );
                                }
                            }
                        }
                        _ = cleanup_ticker.tick() => {
                            if rt.is_stopping() {
                                continue;
                            }
                            match rt.history_store.cleanup_orphaned_sessions(
                                rt.options.session_idle_timeout,
                            ).await {
                                Ok(count) => {
                                    if count > 0 {
                                        tracing::debug!(
                                            target: "duroxide::runtime::worker",
                                            sessions_cleaned = %count,
                                            "Orphaned sessions cleaned up"
                                        );
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        target: "duroxide::runtime::worker",
                                        error = %e,
                                        "Session cleanup failed"
                                    );
                                }
                            }
                        }
                    }
                }

                tracing::debug!(
                    target: "duroxide::runtime::worker",
                    "Session manager stopped"
                );
                Ok(())
            })
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_starts_empty() {
        let tracker = SessionTracker::new();
        assert_eq!(tracker.distinct_count(), 0);
    }

    #[test]
    fn guard_increments_and_decrements() {
        let tracker = Arc::new(SessionTracker::new());
        {
            let _g = SessionGuard::new(&tracker, "s1");
            assert_eq!(tracker.distinct_count(), 1);
        }
        // Guard dropped — session removed
        assert_eq!(tracker.distinct_count(), 0);
    }

    #[test]
    fn same_session_counts_as_one() {
        let tracker = Arc::new(SessionTracker::new());
        let _g1 = SessionGuard::new(&tracker, "s1");
        let _g2 = SessionGuard::new(&tracker, "s1");
        // Two activities, but same session → 1 distinct
        assert_eq!(tracker.distinct_count(), 1);
    }

    #[test]
    fn different_sessions_counted_separately() {
        let tracker = Arc::new(SessionTracker::new());
        let _g1 = SessionGuard::new(&tracker, "s1");
        let _g2 = SessionGuard::new(&tracker, "s2");
        assert_eq!(tracker.distinct_count(), 2);
    }

    #[test]
    fn drop_one_of_two_same_session_keeps_session() {
        let tracker = Arc::new(SessionTracker::new());
        let _g1 = SessionGuard::new(&tracker, "s1");
        {
            let _g2 = SessionGuard::new(&tracker, "s1");
            assert_eq!(tracker.distinct_count(), 1);
        }
        // g2 dropped, but g1 still alive → session still present
        assert_eq!(tracker.distinct_count(), 1);
    }

    #[test]
    fn drop_all_removes_session() {
        let tracker = Arc::new(SessionTracker::new());
        {
            let _g1 = SessionGuard::new(&tracker, "s1");
            let _g2 = SessionGuard::new(&tracker, "s1");
            let _g3 = SessionGuard::new(&tracker, "s2");
            assert_eq!(tracker.distinct_count(), 2);
        }
        // All dropped
        assert_eq!(tracker.distinct_count(), 0);
    }

    #[test]
    fn mixed_acquire_release_sequence() {
        let tracker = Arc::new(SessionTracker::new());
        let g1 = SessionGuard::new(&tracker, "a");
        let g2 = SessionGuard::new(&tracker, "b");
        let g3 = SessionGuard::new(&tracker, "a");
        assert_eq!(tracker.distinct_count(), 2); // a, b

        drop(g1);
        assert_eq!(tracker.distinct_count(), 2); // a(1), b(1) — a still has g3

        drop(g3);
        assert_eq!(tracker.distinct_count(), 1); // b only

        drop(g2);
        assert_eq!(tracker.distinct_count(), 0);
    }
}

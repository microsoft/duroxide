//! Orchestration dispatcher implementation for Runtime
//!
//! This module contains the orchestration dispatcher logic that:
//! - Spawns concurrent orchestration workers
//! - Fetches and processes orchestration items from the queue
//! - Handles orchestration execution and atomic commits
//! - Renews locks during long-running orchestration turns

// Dispatcher uses Mutex locks - poison indicates a panic and should propagate
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]

use crate::providers::{ExecutionMetadata, ProviderError, ScheduledActivityIdentifier, WorkItem};
use crate::{Event, EventKind};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::warn;

use super::super::{HistoryManager, Runtime, WorkItemReader};

/// Validate runtime limits on the history delta and fail the orchestration if any are exceeded.
///
/// Currently enforces:
/// - Custom status size limit ([`crate::runtime::limits::MAX_CUSTOM_STATUS_BYTES`])
/// - Tag name size limit ([`crate::runtime::limits::MAX_TAG_NAME_BYTES`])
///
/// Returns `true` if a limit was violated (orchestration marked as failed).
fn validate_limits(
    history_delta: &[Event],
    history_mgr: &mut HistoryManager,
    metadata: &mut ExecutionMetadata,
    worker_items: &mut Vec<WorkItem>,
    orchestrator_items: &mut Vec<WorkItem>,
    instance: &str,
    execution_id: u64,
) -> bool {
    // --- Custom status size ---
    let last_custom_status = history_delta.iter().rev().find_map(|e| {
        if let EventKind::CustomStatusUpdated { status: Some(ref s) } = e.kind {
            Some(s.clone())
        } else {
            None
        }
    });

    if let Some(ref s) = last_custom_status
        && s.len() > crate::runtime::limits::MAX_CUSTOM_STATUS_BYTES
    {
        tracing::error!(
            target: "duroxide::runtime",
            instance_id = %instance,
            execution_id = %execution_id,
            custom_status_bytes = s.len(),
            max_bytes = crate::runtime::limits::MAX_CUSTOM_STATUS_BYTES,
            "Custom status exceeds size limit, failing orchestration"
        );
        fail_orchestration_for_limit(
            format!(
                "Custom status size ({} bytes) exceeds limit ({} bytes)",
                s.len(),
                crate::runtime::limits::MAX_CUSTOM_STATUS_BYTES,
            ),
            history_mgr,
            metadata,
            worker_items,
            orchestrator_items,
            instance,
            execution_id,
        );
        return true;
    }

    // --- Tag name size ---
    let oversized_tag = history_delta.iter().find_map(|e| {
        if let EventKind::ActivityScheduled {
            tag: Some(ref t),
            ref name,
            ..
        } = e.kind
        {
            if t.len() > crate::runtime::limits::MAX_TAG_NAME_BYTES {
                Some((name.clone(), t.len()))
            } else {
                None
            }
        } else {
            None
        }
    });

    if let Some((ref activity_name, tag_len)) = oversized_tag {
        tracing::error!(
            target: "duroxide::runtime",
            instance_id = %instance,
            execution_id = %execution_id,
            activity_name = %activity_name,
            tag_bytes = tag_len,
            max_bytes = crate::runtime::limits::MAX_TAG_NAME_BYTES,
            "Activity tag exceeds size limit, failing orchestration"
        );
        fail_orchestration_for_limit(
            format!(
                "Activity '{}' tag size ({} bytes) exceeds limit ({} bytes)",
                activity_name,
                tag_len,
                crate::runtime::limits::MAX_TAG_NAME_BYTES,
            ),
            history_mgr,
            metadata,
            worker_items,
            orchestrator_items,
            instance,
            execution_id,
        );
        return true;
    }

    false
}

/// Shared helper to fail an orchestration due to a limit violation.
fn fail_orchestration_for_limit(
    message: String,
    history_mgr: &mut HistoryManager,
    metadata: &mut ExecutionMetadata,
    worker_items: &mut Vec<WorkItem>,
    orchestrator_items: &mut Vec<WorkItem>,
    instance: &str,
    execution_id: u64,
) {
    let error = crate::ErrorDetails::Application {
        kind: crate::AppErrorKind::OrchestrationFailed,
        message,
        retryable: false,
    };

    let failed_event = Event::with_event_id(
        history_mgr.next_event_id(),
        instance,
        execution_id,
        None,
        EventKind::OrchestrationFailed { details: error.clone() },
    );
    history_mgr.append(failed_event);

    metadata.status = Some("Failed".to_string());
    metadata.output = Some(error.display_message());

    worker_items.clear();
    orchestrator_items.clear();
}

/// Error returned when orchestration processing fails before execution
#[derive(Debug)]
pub(crate) enum OrchestrationProcessingError {
    /// The orchestration handler is not registered in the registry.
    /// This typically occurs during rolling deployments when old instances
    /// are processing work before the new handler is registered.
    UnregisteredOrchestration,
}

/// Calculate the renewal interval based on lock timeout and buffer settings.
///
/// # Logic
/// - If timeout ≥ 15s: renew at (timeout - buffer)
/// - If timeout < 15s: renew at 0.5 × timeout (buffer ignored)
fn calculate_renewal_interval(lock_timeout: Duration, buffer: Duration) -> Duration {
    if lock_timeout >= Duration::from_secs(15) {
        let buffer = buffer.min(lock_timeout);
        let interval = lock_timeout
            .checked_sub(buffer)
            .unwrap_or_else(|| Duration::from_secs(1));
        interval.max(Duration::from_secs(1))
    } else {
        let half = (lock_timeout.as_secs_f64() * 0.5).ceil().max(1.0);
        Duration::from_secs_f64(half)
    }
}

/// Spawn a background task to renew the lock for an in-flight orchestration.
fn spawn_orchestration_lock_renewal_task(
    store: Arc<dyn crate::providers::Provider>,
    token: String,
    lock_timeout: Duration,
    buffer: Duration,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> JoinHandle<()> {
    let renewal_interval = calculate_renewal_interval(lock_timeout, buffer);

    tracing::debug!(
        target: "duroxide::runtime::dispatchers::orchestration",
        lock_token = %token,
        lock_timeout_secs = %lock_timeout.as_secs(),
        buffer_secs = %buffer.as_secs(),
        renewal_interval_secs = %renewal_interval.as_secs(),
        "Spawning orchestration lock renewal task"
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(renewal_interval);
        interval.tick().await; // Skip first immediate tick

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if shutdown.load(Ordering::Relaxed) {
                        tracing::debug!(
                            target: "duroxide::runtime::dispatchers::orchestration",
                            lock_token = %token,
                            "Lock renewal task stopping due to shutdown"
                        );
                        break;
                    }

                    match store.renew_orchestration_item_lock(&token, lock_timeout).await {
                        Ok(()) => {
                            tracing::trace!(
                                target: "duroxide::runtime::dispatchers::orchestration",
                                lock_token = %token,
                                extend_secs = %lock_timeout.as_secs(),
                                "Orchestration lock renewed"
                            );
                        }
                        Err(e) => {
                            tracing::debug!(
                                target: "duroxide::runtime::dispatchers::orchestration",
                                lock_token = %token,
                                error = %e,
                                "Failed to renew orchestration lock (may have been acked/abandoned)"
                            );
                            // Stop renewal - lock is gone or expired
                            break;
                        }
                    }
                }
            }
        }

        tracing::debug!(
            target: "duroxide::runtime::dispatchers::orchestration",
            lock_token = %token,
            "Orchestration lock renewal task stopped"
        );
    })
}

impl Runtime {
    /// Start the orchestration dispatcher with N concurrent workers
    pub(in crate::runtime) fn start_orchestration_dispatcher(self: Arc<Self>) -> JoinHandle<()> {
        // EXECUTION: spawns N concurrent orchestration workers
        // Instance-level locking in provider prevents concurrent processing of same instance
        let concurrency = self.options.orchestration_concurrency;
        let shutdown = self.shutdown_flag.clone();

        // Build the capability filter once for all workers (immutable for this runtime's lifetime).
        let capability_filter = if let Some(ref custom_range) = self.options.supported_replay_versions {
            crate::providers::DispatcherCapabilityFilter {
                supported_duroxide_versions: vec![custom_range.clone()],
            }
        } else {
            crate::providers::DispatcherCapabilityFilter::default_for_current_build()
        };
        tracing::info!(
            target: "duroxide::runtime",
            supported_range = %format!("{}", capability_filter.supported_duroxide_versions.iter()
                .map(|r| format!(">={}, <={}", r.min, r.max))
                .collect::<Vec<_>>()
                .join(" | ")),
            "Orchestration dispatcher capability filter configured"
        );

        // The runtime-side supported range is the same as the filter range.
        // Used for defense-in-depth checks after fetch (validates the provider's filtering).
        let runtime_supported_range = if let Some(ref custom_range) = self.options.supported_replay_versions {
            custom_range.clone()
        } else {
            crate::providers::SemverRange::default_for_current_build()
        };

        tokio::spawn(async move {
            let mut worker_handles = Vec::new();

            for worker_idx in 0..concurrency {
                let rt = Arc::clone(&self);
                let shutdown = Arc::clone(&shutdown);
                let cap_filter = Some(capability_filter.clone());
                let supported_range = runtime_supported_range.clone();
                // Generate unique worker ID: orch-{index}-{runtime_id}
                let worker_id = format!("orch-{worker_idx}-{}", rt.runtime_id);
                let handle = tokio::spawn(async move {
                    // debug!("Orchestration worker {} started", worker_id);
                    let mut consecutive_retryable_errors = 0u32;
                    loop {
                        // Check shutdown flag before fetching
                        if shutdown.load(Ordering::Relaxed) {
                            // debug!("Orchestration worker {} exiting", worker_id);
                            break;
                        }

                        let min_interval = rt.options.dispatcher_min_poll_interval;
                        let poll_timeout = rt.options.dispatcher_long_poll_timeout;
                        let start_time = std::time::Instant::now();
                        let mut work_found = false;

                        match rt
                            .history_store
                            .fetch_orchestration_item(
                                rt.options.orchestrator_lock_timeout,
                                poll_timeout,
                                cap_filter.as_ref(),
                            )
                            .await
                        {
                            Ok(Some((item, lock_token, attempt_count))) => {
                                // Reset error counter on success
                                consecutive_retryable_errors = 0;

                                // Runtime-side compatibility check (defense-in-depth).
                                // Even if the provider filter missed this item (bug, provider ignoring filter),
                                // we validate the pinned version before attempting replay.
                                // The pinned version is extracted from the OrchestrationStarted event
                                // (always event_id=1, always a known event type on any version).
                                let pinned_version = item.history.iter().find_map(|e| {
                                    if matches!(&e.kind, crate::EventKind::OrchestrationStarted { .. }) {
                                        semver::Version::parse(&e.duroxide_version).ok()
                                    } else {
                                        None
                                    }
                                });

                                if let Some(pinned) = pinned_version
                                    && !supported_range.contains(&pinned)
                                {
                                    tracing::warn!(
                                        target: "duroxide::runtime",
                                        instance = %item.instance,
                                        pinned_version = %pinned,
                                        supported_range = %format!(">={}, <={}", supported_range.min, supported_range.max),
                                        attempt_count = attempt_count,
                                        "Execution pinned at incompatible version, abandoning (runtime-side check)"
                                    );
                                    // Abandon with a short delay to prevent tight spin loops when
                                    // a single runtime encounters incompatible items. The item still
                                    // reaches max_attempts quickly (e.g., 10 × 1s = 10s) while
                                    // avoiding CPU burn and starvation of compatible work items.
                                    let _ = rt
                                        .history_store
                                        .abandon_orchestration_item(&lock_token, Some(Duration::from_secs(1)), false)
                                        .await;
                                    continue;
                                }
                                // pinned_version == None means no history yet (brand new instance)
                                // — always compatible, proceed normally.

                                // Spawn lock renewal task for this orchestration
                                let renewal_handle = spawn_orchestration_lock_renewal_task(
                                    Arc::clone(&rt.history_store),
                                    lock_token.clone(),
                                    rt.options.orchestrator_lock_timeout,
                                    rt.options.orchestrator_lock_renewal_buffer,
                                    Arc::clone(&shutdown),
                                );

                                // TEST HOOK: Inject delay after spawning renewal task
                                // This simulates slow processing to test lock renewal
                                #[cfg(feature = "test-hooks")]
                                if let Some(delay) =
                                    crate::runtime::test_hooks::get_orch_processing_delay(&item.instance)
                                {
                                    tracing::debug!(
                                        instance = %item.instance,
                                        delay_ms = delay.as_millis(),
                                        "Test hook: injecting orchestration processing delay"
                                    );
                                    tokio::time::sleep(delay).await;
                                }

                                // Process orchestration item atomically
                                // Provider ensures no other worker has this instance locked
                                rt.process_orchestration_item(item, &lock_token, attempt_count, &worker_id)
                                    .await;

                                // Stop lock renewal task now that orchestration turn is complete
                                renewal_handle.abort();

                                work_found = true;
                            }
                            Ok(None) => {
                                // No work available - reset error counter
                                consecutive_retryable_errors = 0;
                            }
                            Err(e) => {
                                if e.is_retryable() {
                                    // Exponential backoff for retryable errors (database locks, etc.)
                                    consecutive_retryable_errors += 1;
                                    let backoff_ms = (100 * 2_u64.pow(consecutive_retryable_errors)).min(3000);
                                    warn!(
                                        "Error fetching orchestration item (retryable, attempt {}): {:?}, backing off {}ms",
                                        consecutive_retryable_errors, e, backoff_ms
                                    );
                                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                } else {
                                    // Permanent errors - log and continue with normal polling
                                    warn!("Error fetching orchestration item (permanent): {:?}", e);
                                    consecutive_retryable_errors = 0;
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                                continue;
                            }
                        }

                        // Enforce minimum polling interval to prevent hot loops
                        if !work_found {
                            let elapsed = start_time.elapsed();
                            if elapsed < min_interval {
                                let sleep_duration = min_interval - elapsed;
                                if !shutdown.load(Ordering::Relaxed) {
                                    tokio::time::sleep(sleep_duration).await;
                                }
                            } else {
                                // Waited long enough (e.g. long poll timeout expired), yield to prevent starvation
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                });
                worker_handles.push(handle);
            }

            // Wait for all workers to complete
            for handle in worker_handles {
                let _ = handle.await;
            }
            // debug!("Orchestration dispatcher exited");
        })
    }

    /// Process a single orchestration item atomically
    pub(in crate::runtime) async fn process_orchestration_item(
        self: &Arc<Self>,
        item: crate::providers::OrchestrationItem,
        lock_token: &str,
        attempt_count: u32,
        worker_id: &str,
    ) {
        // EXECUTION: builds deltas and commits via ack_orchestration_item
        let instance = &item.instance;

        // Check for poison message - message has been fetched too many times.
        // This handles both normal poison (repeated processing failures) and corrupted
        // history poison (history_error items that exhausted their attempts via the
        // abandon-with-backoff cycle below). The fail_orchestration_as_poison method
        // inspects item.history_error to choose the appropriate termination strategy.
        if attempt_count > self.options.max_attempts {
            warn!(
                instance = %instance,
                attempt_count = attempt_count,
                max_attempts = self.options.max_attempts,
                "Orchestration message exceeded max attempts, marking as poison"
            );

            self.fail_orchestration_as_poison(&item, lock_token, attempt_count)
                .await;
            return;
        }

        // Check for corrupted history — provider successfully fetched and locked the item
        // but could not deserialize its history events (e.g., unknown EventKind from a
        // newer duroxide version). Abandon without resetting attempt_count (ignore_attempt=false)
        // so the count keeps growing. Once attempt_count exceeds max_attempts (checked above),
        // the poison path terminates it.
        if let Some(ref error) = item.history_error {
            // Fixed 1s backoff gives another node (which may run a newer duroxide
            // version capable of deserialising these events) a window to pick it up,
            // without the exponential growth that would slow down the poison path.
            let backoff = Duration::from_secs(1);
            let remaining = self.options.max_attempts.saturating_sub(attempt_count);
            warn!(
                instance = %instance,
                error = %error,
                attempt = attempt_count,
                max_attempts = self.options.max_attempts,
                "History deserialization failed, abandoning with 1s backoff ({}/{}, {} remaining)",
                attempt_count, self.options.max_attempts, remaining,
            );
            // ignore_attempt = false: keep the incremented attempt_count so we eventually reach poison
            let _ = self
                .history_store
                .abandon_orchestration_item(lock_token, Some(backoff), false)
                .await;
            return;
        }

        // Extract metadata from history and work items
        let temp_history_mgr = HistoryManager::from_history(&item.history);
        let workitem_reader = WorkItemReader::from_messages(&item.messages, &temp_history_mgr, instance);

        // Bail on truly terminal histories (Completed/Failed), or ContinuedAsNew without a CAN start message
        if temp_history_mgr.is_completed
            || temp_history_mgr.is_failed
            || (temp_history_mgr.is_continued_as_new && !workitem_reader.is_continue_as_new)
        {
            warn!(instance = %instance, "Instance is terminal (completed/failed or CAN without start), acking batch without processing");
            let _ = self
                .ack_orchestration_with_changes(
                    lock_token,
                    item.execution_id,
                    vec![],
                    vec![],
                    vec![],
                    ExecutionMetadata::default(),
                    vec![], // cancelled_activities - none for terminal instances
                )
                .await;
            return;
        }

        // Extract version before moving temp_history_mgr
        let version = temp_history_mgr.version().unwrap_or_else(|| "unknown".to_string());

        // Decide execution id and history to use for this execution
        let (execution_id_to_use, mut history_mgr) = if workitem_reader.is_continue_as_new {
            // ContinueAsNew - start with empty history for new execution
            (item.execution_id + 1, HistoryManager::from_history(&[]))
        } else {
            // Normal execution - use existing history
            (item.execution_id, temp_history_mgr)
        };

        // Track start time for duration metrics
        let start_time = std::time::Instant::now();
        let mut turn_count = 0u64; // Track turns for this execution

        // Log start for non-empty batches (we'll record metrics after handler resolution)
        if !workitem_reader.has_orchestration_name() && !workitem_reader.completion_messages.is_empty() {
            // Empty orchestration name with completion messages - just warn and skip
            tracing::warn!(instance = %item.instance, "empty effective batch - this should not happen");
        }

        // Process the execution (unified path)
        let (mut worker_items, mut orchestrator_items, select_cancelled_activities, execution_id_for_ack) =
            if workitem_reader.has_orchestration_name() {
                // Resolve handler and execute orchestration
                let result = self
                    .resolve_and_execute_orchestration_handler(
                        instance,
                        &mut history_mgr,
                        &workitem_reader,
                        execution_id_to_use,
                        worker_id,
                    )
                    .await;

                match result {
                    Ok((wi, oi, cancels)) => {
                        // Handler resolved successfully - now record metrics
                        tracing::debug!(
                            target: "duroxide::runtime",
                            instance_id = %instance,
                            execution_id = %execution_id_to_use,
                            orchestration_name = %workitem_reader.orchestration_name,
                            orchestration_version = %version,
                            worker_id = %worker_id,
                            is_continue_as_new = %workitem_reader.is_continue_as_new,
                            "Orchestration started"
                        );

                        // Record orchestration start metric
                        let initiated_by = if workitem_reader.is_continue_as_new {
                            "continueAsNew"
                        } else {
                            "client"
                        };
                        self.record_orchestration_start(&workitem_reader.orchestration_name, &version, initiated_by);

                        // Increment active orchestrations gauge ONLY for brand new orchestrations
                        if history_mgr.is_full_history_empty() && !workitem_reader.is_continue_as_new {
                            self.increment_active_orchestrations();
                        }

                        (wi, oi, cancels, execution_id_to_use)
                    }
                    Err(OrchestrationProcessingError::UnregisteredOrchestration) => {
                        // Orchestration not registered - abandon with exponential backoff
                        let backoff = self.options.unregistered_backoff.delay(attempt_count);
                        let remaining_attempts = self.options.max_attempts.saturating_sub(attempt_count);

                        tracing::warn!(
                            target: "duroxide::runtime",
                            instance = %instance,
                            orchestration_name = %workitem_reader.orchestration_name,
                            version = %workitem_reader.version.as_deref().unwrap_or("latest"),
                            attempt_count = %attempt_count,
                            max_attempts = %self.options.max_attempts,
                            remaining_attempts = %remaining_attempts,
                            backoff_secs = %backoff.as_secs_f32(),
                            "Orchestration not registered, abandoning with {:.1}s backoff (will poison in {} more attempts)",
                            backoff.as_secs_f32(),
                            remaining_attempts
                        );

                        // Abandon with delay - poison handling will eventually terminate if genuinely missing
                        let _ = self
                            .history_store
                            .abandon_orchestration_item(lock_token, Some(backoff), false)
                            .await;
                        return;
                    }
                }
            } else {
                // Empty effective batch
                (vec![], vec![], vec![], execution_id_to_use)
            };

        // Snapshot current history delta (used for metadata + logging below).
        // NOTE: We may append additional events (e.g., cancellation requests) later in this method.
        let history_delta_snapshot = history_mgr.delta().to_vec();

        // Compute execution metadata from history_delta (runtime responsibility)
        let mut metadata =
            Runtime::compute_execution_metadata(&history_delta_snapshot, &orchestrator_items, item.execution_id);

        // Enforce runtime limits (custom status size, tag name size, etc.)
        validate_limits(
            &history_delta_snapshot,
            &mut history_mgr,
            &mut metadata,
            &mut worker_items,
            &mut orchestrator_items,
            instance,
            execution_id_for_ack,
        );

        // Calculate metrics
        let duration_seconds = start_time.elapsed().as_secs_f64();
        turn_count += 1; // At least one turn was processed

        // Log orchestration completion/failure and record metrics
        if let Some(ref status) = metadata.status {
            // Use actual orchestration name from work item, not metadata (which may be "unknown")
            let orch_name = if workitem_reader.has_orchestration_name() {
                workitem_reader.orchestration_name.as_str()
            } else {
                "unknown"
            };
            let version = metadata.orchestration_version.as_deref().unwrap_or(&version);
            let event_count = history_mgr.full_history_len();

            match status.as_str() {
                "Completed" => {
                    tracing::debug!(
                        target: "duroxide::runtime",
                        instance_id = %instance,
                        execution_id = %execution_id_for_ack,
                        orchestration_name = %orch_name,
                        orchestration_version = %version,
                        worker_id = %worker_id,
                        history_events = %event_count,
                        "Orchestration completed"
                    );

                    // Record completion metrics with labels
                    self.record_orchestration_completion_with_labels(
                        orch_name,
                        version,
                        "success",
                        duration_seconds,
                        turn_count,
                        event_count as u64,
                    );

                    // Decrement active orchestrations (truly completed)
                    self.decrement_active_orchestrations();
                }
                "Failed" => {
                    // Extract error type from history_delta to determine log level
                    let (error_type, error_category) = history_delta_snapshot
                        .iter()
                        .find_map(|event| {
                            if let EventKind::OrchestrationFailed { details } = &event.kind {
                                let category = details.category();
                                let error_type = match category {
                                    "infrastructure" => "infrastructure_error",
                                    "configuration" => "config_error",
                                    "application" => "app_error",
                                    _ => "unknown",
                                };
                                Some((error_type, category))
                            } else {
                                None
                            }
                        })
                        .unwrap_or(("unknown", "unknown"));

                    // Only log as ERROR for infrastructure/configuration errors
                    // Application errors are expected business logic failures
                    match error_category {
                        "infrastructure" | "configuration" => {
                            tracing::error!(
                                target: "duroxide::runtime",
                                instance_id = %instance,
                                execution_id = %execution_id_for_ack,
                                orchestration_name = %orch_name,
                                orchestration_version = %version,
                                worker_id = %worker_id,
                                history_events = %event_count,
                                error_type = %error_category,
                                error = metadata.output.as_deref().unwrap_or("unknown"),
                                "Orchestration failed"
                            );
                        }
                        "application" => {
                            tracing::warn!(
                                target: "duroxide::runtime",
                                instance_id = %instance,
                                execution_id = %execution_id_for_ack,
                                orchestration_name = %orch_name,
                                orchestration_version = %version,
                                worker_id = %worker_id,
                                history_events = %event_count,
                                error_type = %error_category,
                                error = metadata.output.as_deref().unwrap_or("unknown"),
                                "Orchestration failed (application error)"
                            );
                        }
                        _ => {
                            tracing::error!(
                                target: "duroxide::runtime",
                                instance_id = %instance,
                                execution_id = %execution_id_for_ack,
                                orchestration_name = %orch_name,
                                orchestration_version = %version,
                                worker_id = %worker_id,
                                history_events = %event_count,
                                error_type = %error_category,
                                error = metadata.output.as_deref().unwrap_or("unknown"),
                                "Orchestration failed (unknown error type)"
                            );
                        }
                    }

                    // Record failure metrics with labels
                    self.record_orchestration_failure_with_labels(orch_name, version, error_type, error_category);

                    // Also record completion with failed status for duration tracking
                    self.record_orchestration_completion_with_labels(
                        orch_name,
                        version,
                        "failed",
                        duration_seconds,
                        turn_count,
                        event_count as u64,
                    );

                    // Decrement active orchestrations (terminal failure)
                    self.decrement_active_orchestrations();
                }
                "ContinuedAsNew" => {
                    tracing::debug!(
                        target: "duroxide::runtime",
                        instance_id = %instance,
                        execution_id = %execution_id_for_ack,
                        orchestration_name = %orch_name,
                        orchestration_version = %version,
                        worker_id = %worker_id,
                        history_events = %event_count,
                        "Orchestration continued as new"
                    );

                    // Record continue-as-new metric
                    self.record_continue_as_new(orch_name, execution_id_for_ack);

                    // Record completion with continue-as-new status
                    self.record_orchestration_completion_with_labels(
                        orch_name,
                        version,
                        "continuedAsNew",
                        duration_seconds,
                        turn_count,
                        event_count as u64,
                    );

                    // NOTE: Do NOT decrement active_orchestrations on continue-as-new!
                    // The orchestration is still active, just starting a new execution.
                    // The increment for the new execution will happen when the ContinueAsNew
                    // message is processed (above in the is_continue_as_new branch).
                }
                _ => {}
            }
        }

        // Compute cancelled activities for terminal orchestrations
        // When an orchestration terminates (Failed, Completed, or ContinuedAsNew), any in-flight
        // activities should be cancelled via lock stealing. This handles:
        // - Failed: Orchestration errored, pending work is no longer needed
        // - Completed: "Select losers" - activities that lost a select race
        // - ContinuedAsNew: Old execution's activities won't be awaited by new execution
        let mut cancelled_activities = select_cancelled_activities;
        match metadata.status.as_deref() {
            Some("Failed") | Some("Completed") | Some("ContinuedAsNew") => {
                let inflight = history_mgr.compute_inflight_activities(instance, execution_id_for_ack);
                if !inflight.is_empty() {
                    tracing::debug!(
                        target: "duroxide::runtime",
                        instance_id = %instance,
                        status = ?metadata.status,
                        count = %inflight.len(),
                        "Cancelling in-flight activities"
                    );
                }

                // Record cancellation decisions in history for terminal lock-stealing cancellations.
                // Avoid duplicates: compute_inflight_activities already excludes activities with an
                // existing ActivityCancelRequested.
                let reason = match metadata.status.as_deref() {
                    Some("Completed") => "orchestration_terminal_completed",
                    Some("Failed") => "orchestration_terminal_failed",
                    Some("ContinuedAsNew") => "orchestration_terminal_continued_as_new",
                    _ => "orchestration_terminal_failed",
                };
                for a in &inflight {
                    let event_id = history_mgr.next_event_id();
                    history_mgr.append(Event::with_event_id(
                        event_id,
                        instance,
                        execution_id_for_ack,
                        Some(a.activity_id),
                        EventKind::ActivityCancelRequested {
                            reason: reason.to_string(),
                        },
                    ));
                }

                cancelled_activities.extend(inflight);
            }
            _ => {}
        }

        // De-dupe in case a select-loser is also in-flight at termination.
        if cancelled_activities.len() > 1 {
            use std::collections::HashSet;
            let mut seen: HashSet<(String, u64, u64)> = HashSet::with_capacity(cancelled_activities.len());
            cancelled_activities.retain(|a| seen.insert((a.instance.clone(), a.execution_id, a.activity_id)));
        }

        // Robust ack with basic retry on any provider error
        match self
            .ack_orchestration_with_changes(
                lock_token,
                execution_id_for_ack,
                history_mgr.delta().to_vec(),
                worker_items,
                orchestrator_items,
                metadata,
                cancelled_activities,
            )
            .await
        {
            Ok(()) => {
                // Success - orchestration committed
            }
            Err(e) => {
                // Failed to ack - need to fail the orchestration with infrastructure error
                //
                // This error handler is reached in two scenarios:
                // 1. Non-retryable error: Permanent infrastructure issue (data corruption, invalid format, etc.)
                // 2. Retryable error that failed after all retries: Persistent infrastructure issue
                //    (database locked, network failure, etc. that persisted despite retries)
                //
                // In both cases, the infrastructure problem prevents the orchestration from progressing,
                // so we treat it as an infrastructure failure and mark the orchestration as failed.
                warn!(instance = %instance, error = %e, "Failed to ack orchestration item, failing orchestration");

                // Create infrastructure error and commit it
                let infra_error = e.to_infrastructure_error();
                self.record_orchestration_infrastructure_error();

                // Create a new history manager to append the failure event
                let mut failure_history_mgr = HistoryManager::from_history(&item.history);
                failure_history_mgr.append_failed(infra_error.clone());

                // Try to commit the failure event
                let failure_delta = failure_history_mgr.delta().to_vec();
                let failure_metadata = Runtime::compute_execution_metadata(&failure_delta, &[], item.execution_id);

                // Attempt to ack the failure (lock is still valid since we didn't abandon it yet)
                match self
                    .ack_orchestration_with_changes(
                        lock_token,
                        execution_id_for_ack,
                        failure_delta.clone(),
                        vec![],
                        vec![],
                        failure_metadata,
                        vec![], // cancelled_activities - none for failure commits
                    )
                    .await
                {
                    Ok(()) => {
                        // Successfully committed failure event (ack also releases the lock)
                        warn!(instance = %instance, "Successfully committed orchestration failure event");
                    }
                    Err(e2) => {
                        // Failed to commit failure event - abandon lock
                        warn!(instance = %instance, error = %e2, "Failed to commit failure event, abandoning lock");
                        drop(self.history_store.abandon_orchestration_item(
                            lock_token,
                            Some(std::time::Duration::from_millis(50)),
                            false,
                        ));
                    }
                }
            }
        }
    }

    /// Resolves the orchestration handler from the registry and executes the orchestration.
    ///
    /// This function:
    /// 1. Resolves the handler (exact version if specified, or latest via policy)
    /// 2. Creates the OrchestrationStarted event if this is a new instance
    /// 3. Runs the orchestration execution
    ///
    /// Returns `Err(UnregisteredOrchestration)` if the handler is not found in the registry,
    /// which the caller should handle by abandoning with backoff for rolling deployment support.
    pub(in crate::runtime) async fn resolve_and_execute_orchestration_handler(
        self: &Arc<Self>,
        instance: &str,
        history_mgr: &mut HistoryManager,
        workitem_reader: &WorkItemReader,
        execution_id: u64,
        worker_id: &str,
    ) -> Result<(Vec<WorkItem>, Vec<WorkItem>, Vec<ScheduledActivityIdentifier>), OrchestrationProcessingError> {
        let mut worker_items = Vec::new();
        let mut orchestrator_items = Vec::new();
        let mut cancelled_activities = Vec::new();

        // Resolve handler once - use provided version or resolve from registry policy
        let resolved_handler = if let Some(v_str) = &workitem_reader.version {
            // Use exact version if provided
            if let Ok(v) = semver::Version::parse(v_str) {
                self.orchestration_registry
                    .resolve_handler_exact(&workitem_reader.orchestration_name, &v)
                    .map(|h| (v, h))
            } else {
                None
            }
        } else {
            // Resolve using policy (returns both version and handler)
            self.orchestration_registry
                .resolve_handler(&workitem_reader.orchestration_name)
        };

        let (resolved_version, handler) = match resolved_handler {
            Some((v, h)) => (v, h),
            None => {
                // Handler not registered - return error for caller to handle
                // This is expected during rolling deployments when new orchestrations
                // are started before all nodes have the updated handler
                return Err(OrchestrationProcessingError::UnregisteredOrchestration);
            }
        };

        // Create started event if this is a new instance
        if history_mgr.is_empty() {
            // Extract carry-forward events from ContinueAsNew work item for audit trail
            let (carry_forward_events, initial_custom_status) = match &workitem_reader.start_item {
                Some(WorkItem::ContinueAsNew {
                    carry_forward_events,
                    initial_custom_status,
                    ..
                }) => (
                    if carry_forward_events.is_empty() {
                        None
                    } else {
                        Some(carry_forward_events.clone())
                    },
                    initial_custom_status.clone(),
                ),
                _ => (None, None),
            };

            history_mgr.append(Event::with_event_id(
                1, // First event always has event_id=1
                instance,
                execution_id,
                None,
                EventKind::OrchestrationStarted {
                    name: workitem_reader.orchestration_name.clone(),
                    version: resolved_version.to_string(),
                    input: workitem_reader.input.clone(),
                    parent_instance: workitem_reader.parent_instance.clone(),
                    parent_id: workitem_reader.parent_id,
                    carry_forward_events,
                    initial_custom_status,
                },
            ));
        }

        // Run the atomic execution to get all changes, passing the resolved handler and version
        let (_exec_history_delta, exec_worker_items, exec_orchestrator_items, exec_cancelled_activities, _result) =
            Arc::clone(self)
                .run_single_execution_atomic(
                    instance,
                    history_mgr,
                    workitem_reader,
                    execution_id,
                    worker_id,
                    handler,
                    resolved_version.to_string(),
                )
                .await;

        // Combine all changes (history already in history_mgr via mutation)
        worker_items.extend(exec_worker_items);
        orchestrator_items.extend(exec_orchestrator_items);
        cancelled_activities.extend(exec_cancelled_activities);

        Ok((worker_items, orchestrator_items, cancelled_activities))
    }

    /// Acknowledge an orchestration item with changes, using smart retry logic based on ProviderError
    /// Returns Ok(()) on success, or the ProviderError if all retries failed
    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) async fn ack_orchestration_with_changes(
        &self,
        lock_token: &str,
        execution_id: u64,
        history_delta: Vec<Event>,
        worker_items: Vec<WorkItem>,
        orchestrator_items: Vec<WorkItem>,
        metadata: ExecutionMetadata,
        cancelled_activities: Vec<ScheduledActivityIdentifier>,
    ) -> Result<(), ProviderError> {
        // Invariant: pinned_duroxide_version may only be set on the first turn of an
        // execution, when OrchestrationStarted is in the history delta. The provider
        // stores it unconditionally — the runtime is responsible for never sending it
        // on subsequent turns. This assertion catches bugs in compute_execution_metadata
        // or manual metadata construction.
        debug_assert!(
            metadata.pinned_duroxide_version.is_none()
                || history_delta
                    .iter()
                    .any(|e| matches!(&e.kind, crate::EventKind::OrchestrationStarted { .. })),
            "pinned_duroxide_version must only be set when OrchestrationStarted is in the history delta \
             (first turn of an execution). Setting it on subsequent turns would corrupt the execution's \
             version routing. pinned={:?}, delta_events={:?}",
            metadata.pinned_duroxide_version,
            history_delta
                .iter()
                .map(|e| std::mem::discriminant(&e.kind))
                .collect::<Vec<_>>()
        );

        let mut attempts: u32 = 0;
        let max_attempts: u32 = 5;

        loop {
            match self
                .history_store
                .ack_orchestration_item(
                    lock_token,
                    execution_id,
                    history_delta.clone(),
                    worker_items.clone(),
                    orchestrator_items.clone(),
                    metadata.clone(),
                    cancelled_activities.clone(),
                )
                .await
            {
                Ok(()) => {
                    return Ok(());
                }
                Err(e) => {
                    // Check if error is retryable
                    if !e.is_retryable() {
                        // Non-retryable error - don't abandon lock yet, return error
                        // Caller will try to commit failure event, then abandon lock
                        warn!(error = %e, "ack_orchestration_item failed with non-retryable error");
                        return Err(e);
                    }

                    // Retryable error - retry with backoff
                    if attempts < max_attempts {
                        let backoff_ms = 10u64.saturating_mul(1 << attempts);
                        warn!(attempts, backoff_ms, error = %e, "ack_orchestration_item failed; retrying");
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        attempts += 1;
                        continue;
                    } else {
                        warn!(attempts, error = %e, "Failed to ack_orchestration_item after max retries");
                        // Abandon the item to release lock
                        drop(self.history_store.abandon_orchestration_item(
                            lock_token,
                            Some(std::time::Duration::from_millis(50)),
                            false,
                        ));
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Fail an orchestration as a poison message.
    ///
    /// This is called when the attempt_count exceeds max_attempts, indicating the message
    /// repeatedly fails to process (crash/abandon cycle).
    async fn fail_orchestration_as_poison(
        self: &Arc<Self>,
        item: &crate::providers::OrchestrationItem,
        lock_token: &str,
        attempt_count: u32,
    ) {
        let error = crate::ErrorDetails::Poison {
            attempt_count,
            max_attempts: self.options.max_attempts,
            message_type: crate::PoisonMessageType::Orchestration {
                instance: item.instance.clone(),
                execution_id: item.execution_id,
            },
            message: serde_json::to_string(&item.messages).unwrap_or_default(),
        };

        // --- Corrupted history poison path ---
        // If history_error is set, history in the DB is undeserializable (e.g., unknown
        // EventKind from a newer duroxide version). We cannot read existing events to
        // determine the next event_id, so we use a well-known sentinel (99999) for the
        // OrchestrationFailed event.
        //
        // IMPORTANT: The ack below MUST succeed despite corrupted history rows in the DB.
        // This relies on the provider's ack path being append-only — it only INSERTs new
        // event rows and UPDATEs execution metadata, never re-reads or re-serializes
        // existing history. A provider that does read-all → deserialize → re-serialize
        // during ack would fail here. This contract is validated by
        // test_ack_appends_event_to_corrupted_history (provider validation #45).
        if let Some(ref deser_error) = item.history_error {
            let error = crate::ErrorDetails::Poison {
                attempt_count,
                max_attempts: self.options.max_attempts,
                message_type: crate::PoisonMessageType::FailedDeserialization {
                    instance: item.instance.clone(),
                    execution_id: item.execution_id,
                    error: deser_error.clone(),
                },
                message: deser_error.clone(),
            };

            let failed_event = Event::with_event_id(
                99999,
                &item.instance,
                item.execution_id,
                None,
                EventKind::OrchestrationFailed { details: error.clone() },
            );

            let metadata = ExecutionMetadata {
                status: Some("Failed".to_string()),
                output: Some(error.display_message()),
                orchestration_name: Some(item.orchestration_name.clone()),
                orchestration_version: Some(item.version.clone()),
                parent_instance_id: None,
                pinned_duroxide_version: None,
            };

            let _ = self
                .ack_orchestration_with_changes(
                    lock_token,
                    item.execution_id,
                    vec![failed_event],
                    vec![],
                    vec![],
                    metadata,
                    vec![],
                )
                .await;

            self.record_orchestration_poison();
            return;
        }

        // Create failure event and commit
        let mut history_mgr = HistoryManager::from_history(&item.history);

        // Track parent info for sub-orchestration failure notification
        let mut parent_link: Option<(String, u64)> = None;

        // If history is empty, we need to create an OrchestrationStarted event first
        if history_mgr.is_empty() {
            // Try to extract orchestration name from work items
            let (orchestration_name, input, parent_instance, parent_id, carry_forward_events) = item
                .messages
                .iter()
                .find_map(|msg| match msg {
                    WorkItem::StartOrchestration {
                        orchestration,
                        input,
                        parent_instance,
                        parent_id,
                        ..
                    } => Some((
                        orchestration.clone(),
                        input.clone(),
                        parent_instance.clone(),
                        *parent_id,
                        None,
                    )),
                    WorkItem::ContinueAsNew {
                        orchestration,
                        input,
                        carry_forward_events,
                        ..
                    } => Some((
                        orchestration.clone(),
                        input.clone(),
                        None,
                        None,
                        Some(carry_forward_events.clone()),
                    )),
                    _ => None,
                })
                .unwrap_or_else(|| (item.orchestration_name.clone(), String::new(), None, None, None));

            // Save parent link for notification
            if let (Some(pi), Some(pid)) = (&parent_instance, parent_id) {
                parent_link = Some((pi.clone(), pid));
            }

            history_mgr.append(Event::with_event_id(
                crate::INITIAL_EVENT_ID,
                &item.instance,
                item.execution_id,
                None,
                EventKind::OrchestrationStarted {
                    name: orchestration_name,
                    version: item.version.clone(),
                    input,
                    parent_instance,
                    parent_id,
                    carry_forward_events,
                    initial_custom_status: None,
                },
            ));
        } else {
            // Check existing history for parent link
            for event in &item.history {
                if let EventKind::OrchestrationStarted {
                    parent_instance: Some(pi),
                    parent_id: Some(pid),
                    ..
                } = &event.kind
                {
                    parent_link = Some((pi.clone(), *pid));
                    break;
                }
            }
        }

        history_mgr.append_failed(error.clone());

        let metadata = ExecutionMetadata {
            status: Some("Failed".to_string()),
            output: Some(error.display_message()),
            orchestration_name: Some(item.orchestration_name.clone()),
            orchestration_version: Some(item.version.clone()),
            parent_instance_id: parent_link.as_ref().map(|(pi, _)| pi.clone()),
            pinned_duroxide_version: None, // Poison path — version already set at creation
        };

        // If this is a sub-orchestration, notify parent of failure
        let orchestrator_items = if let Some((parent_instance, parent_id)) = parent_link {
            tracing::debug!(
                target = "duroxide::runtime::execution",
                instance = %item.instance,
                parent_instance = %parent_instance,
                parent_id = %parent_id,
                "Enqueue SubOrchFailed to parent (poison)"
            );
            let parent_execution_id = self.get_execution_id_for_instance(&parent_instance, None).await;
            vec![WorkItem::SubOrchFailed {
                parent_instance,
                parent_execution_id,
                parent_id,
                details: error.clone(),
            }]
        } else {
            vec![]
        };

        let _ = self
            .ack_orchestration_with_changes(
                lock_token,
                item.execution_id,
                history_mgr.delta().to_vec(),
                vec![],
                orchestrator_items,
                metadata,
                vec![], // cancelled_activities - none for poison message handling
            )
            .await;

        // Record metrics for poison detection
        self.record_orchestration_poison();
    }
}

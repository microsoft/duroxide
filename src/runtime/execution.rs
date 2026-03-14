// Execution module uses Mutex locks - poison indicates a panic and should propagate
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]

use std::sync::Arc;
use tracing::debug;

use super::replay_engine::{ReplayEngine, TurnResult};
use crate::{
    Event, EventKind,
    providers::{ScheduledActivityIdentifier, WorkItem},
    runtime::{OrchestrationHandler, Runtime},
};

impl Runtime {
    /// Execute a single orchestration turn atomically
    ///
    /// This method processes completion messages and executes one orchestration turn,
    /// collecting all resulting work items and history changes atomically.
    /// The handler must already be resolved and provided as a parameter.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_single_execution_atomic(
        self: Arc<Self>,
        instance: &str,
        history_mgr: &mut crate::runtime::state_helpers::HistoryManager,
        workitem_reader: &crate::runtime::state_helpers::WorkItemReader,
        execution_id: u64,
        worker_id: &str,
        handler: Arc<dyn OrchestrationHandler>,
        orchestration_version: String,
        kv_snapshot: std::collections::HashMap<String, crate::providers::KvEntry>,
    ) -> (
        Vec<Event>,
        Vec<WorkItem>,
        Vec<WorkItem>,
        Vec<ScheduledActivityIdentifier>,
        Result<String, String>,
    ) {
        let orchestration_name = &workitem_reader.orchestration_name;
        debug!(instance, orchestration_name, "🚀 Starting atomic single execution");

        // Track all changes
        let mut worker_items = Vec::new();
        let mut orchestrator_items = Vec::new();
        let mut cancelled_activities: Vec<ScheduledActivityIdentifier> = Vec::new();

        // Helper: only honor detached starts at terminal; ignore all other pending actions
        let mut enqueue_detached_from_pending = |pending: &[_]| {
            for action in pending {
                if let crate::Action::StartOrchestrationDetached {
                    name,
                    version,
                    instance: sub_instance,
                    input,
                    ..
                } = action
                {
                    orchestrator_items.push(WorkItem::StartOrchestration {
                        instance: sub_instance.clone(),
                        orchestration: name.clone(),
                        input: input.clone(),
                        version: version.clone(),
                        parent_instance: None,
                        parent_id: None,
                        execution_id: crate::INITIAL_EXECUTION_ID,
                    });
                }
            }
        };

        // History must have OrchestrationStarted at this point (either from existing history or newly created in delta)
        debug_assert!(!history_mgr.is_empty(), "history_mgr should never be empty here");

        // Extract input and parent linkage from history manager
        // (works for both existing history and newly appended OrchestrationStarted in delta)
        let (input, parent_link) = history_mgr.extract_context();

        if let Some((ref pinst, pid)) = parent_link {
            tracing::debug!(target = "duroxide::runtime::execution", instance=%instance, parent_instance=%pinst, parent_id=%pid, "Detected parent link for orchestration");
        } else {
            tracing::debug!(target = "duroxide::runtime::execution", instance=%instance, "No parent link for orchestration");
        }

        // Execute orchestration turn
        let messages = &workitem_reader.completion_messages;

        debug!(
            instance = %instance,
            message_count = messages.len(),
            "starting orchestration turn atomically"
        );
        // Use full history (always contains only current execution)
        let current_execution_history = history_mgr.full_history();
        // Get the persisted history length for is_replaying tracking
        let persisted_len = history_mgr.original_len();
        let mut turn = ReplayEngine::new(instance.to_string(), execution_id, current_execution_history)
            .with_persisted_history_len(persisted_len)
            .with_kv_snapshot(kv_snapshot);

        // Prep completions from incoming messages
        if !messages.is_empty() {
            turn.prep_completions(messages.to_vec());
        }

        // Execute the orchestration logic
        let turn_result = turn.execute_orchestration(
            handler.clone(),
            input.clone(),
            orchestration_name.to_string(),
            orchestration_version.clone(),
            worker_id,
        );

        // Select/select2 losers: request cancellation for those activities now.
        for activity_id in turn.cancelled_activity_ids() {
            cancelled_activities.push(ScheduledActivityIdentifier {
                instance: instance.to_string(),
                execution_id,
                activity_id: *activity_id,
            });
        }

        // Select/select2 losers: collect sub-orchestration cancellations.
        // These will be added to orchestrator_items after the match block
        // (to avoid borrow conflict with enqueue_detached_from_pending closure).
        let cancelled_sub_orch_items: Vec<WorkItem> = turn
            .cancelled_sub_orchestration_ids()
            .iter()
            .map(|child_instance_id| WorkItem::CancelInstance {
                instance: crate::build_child_instance_id(instance, child_instance_id),
                reason: "parent dropped sub-orchestration future".to_string(),
            })
            .collect();

        // Collect history delta from turn
        history_mgr.extend(turn.history_delta().to_vec());

        // Nondeterminism detection is handled by ReplayEngine::execute_orchestration

        // Handle turn result and collect work items
        let result = match turn_result {
            TurnResult::Continue => {
                // Collect work items from pending actions
                for action in turn.pending_actions() {
                    match action {
                        crate::Action::CallActivity {
                            scheduling_event_id,
                            name,
                            input,
                            session_id,
                            tag,
                        } => {
                            let execution_id = self.get_execution_id_for_instance(instance, Some(execution_id)).await;
                            worker_items.push(WorkItem::ActivityExecute {
                                instance: instance.to_string(),
                                execution_id,
                                id: *scheduling_event_id,
                                name: name.clone(),
                                input: input.clone(),
                                session_id: session_id.clone(),
                                tag: tag.clone(),
                            });
                        }
                        crate::Action::CreateTimer {
                            scheduling_event_id,
                            fire_at_ms,
                        } => {
                            let execution_id = self.get_execution_id_for_instance(instance, Some(execution_id)).await;

                            // Enqueue TimerFired to orchestrator queue with delayed visibility
                            // Provider will use fire_at_ms for the visible_at timestamp
                            // Note: fire_at_ms is computed at scheduling time (wall-clock),
                            // ensuring timers fire at the correct absolute time regardless of history state.
                            orchestrator_items.push(WorkItem::TimerFired {
                                instance: instance.to_string(),
                                execution_id,
                                id: *scheduling_event_id,
                                fire_at_ms: *fire_at_ms,
                            });
                        }
                        crate::Action::StartSubOrchestration {
                            scheduling_event_id,
                            name,
                            version,
                            instance: sub_instance,
                            input,
                        } => {
                            let child_full = crate::build_child_instance_id(instance, sub_instance);
                            orchestrator_items.push(WorkItem::StartOrchestration {
                                instance: child_full,
                                orchestration: name.clone(),
                                input: input.clone(),
                                version: version.clone(),
                                parent_instance: Some(instance.to_string()),
                                parent_id: Some(*scheduling_event_id),
                                execution_id: crate::INITIAL_EXECUTION_ID,
                            });
                        }
                        crate::Action::StartOrchestrationDetached {
                            scheduling_event_id: _,
                            name,
                            version,
                            instance: sub_instance,
                            input,
                        } => {
                            orchestrator_items.push(WorkItem::StartOrchestration {
                                instance: sub_instance.clone(),
                                orchestration: name.clone(),
                                input: input.clone(),
                                version: version.clone(),
                                parent_instance: None,
                                parent_id: None,
                                execution_id: crate::INITIAL_EXECUTION_ID,
                            });
                        }
                        _ => {} // Other actions don't generate work items
                    }
                }

                Ok(String::new())
            }
            TurnResult::Completed(output) => {
                // Honor detached orchestration starts at terminal state
                enqueue_detached_from_pending(turn.pending_actions());

                // Emit cancellation breadcrumbs BEFORE the terminal event.
                // Terminal events (Completed/Failed/CAN) must be the last event in history.
                let outstanding = Self::get_outstanding_schedule_ids(history_mgr.full_history_iter());
                Self::emit_terminal_cancellation_breadcrumbs(history_mgr, instance, execution_id, &outstanding);

                // Add completion event last
                let next_id = history_mgr.next_event_id();
                history_mgr.append(Event::with_event_id(
                    next_id,
                    instance,
                    execution_id,
                    None,
                    EventKind::OrchestrationCompleted { output: output.clone() },
                ));

                // Notify parent if this is a sub-orchestration
                if let Some((parent_instance, parent_id)) = parent_link {
                    tracing::debug!(target = "duroxide::runtime::execution", instance=%instance, parent_instance=%parent_instance, parent_id=%parent_id, "Enqueue SubOrchCompleted to parent");
                    orchestrator_items.push(WorkItem::SubOrchCompleted {
                        parent_instance: parent_instance.clone(),
                        parent_execution_id: self.get_execution_id_for_instance(&parent_instance, None).await,
                        parent_id,
                        result: output.clone(),
                    });
                }

                Ok(output)
            }
            TurnResult::Failed(details) => {
                // Honor detached orchestration starts at terminal state
                enqueue_detached_from_pending(turn.pending_actions());

                // Record error metric
                match &details {
                    crate::ErrorDetails::Application { .. } => self.record_orchestration_application_error(),
                    crate::ErrorDetails::Infrastructure { .. } => self.record_orchestration_infrastructure_error(),
                    crate::ErrorDetails::Configuration { .. } => self.record_orchestration_configuration_error(),
                    crate::ErrorDetails::Poison { .. } => self.record_orchestration_poison(),
                }

                // Emit cancellation breadcrumbs BEFORE the terminal event.
                // Terminal events (Completed/Failed/CAN) must be the last event in history.
                let outstanding = Self::get_outstanding_schedule_ids(history_mgr.full_history_iter());
                Self::emit_terminal_cancellation_breadcrumbs(history_mgr, instance, execution_id, &outstanding);

                // Add failure event last
                history_mgr.append_failed(details.clone());

                // Notify parent if this is a sub-orchestration
                if let Some((parent_instance, parent_id)) = parent_link {
                    tracing::debug!(target = "duroxide::runtime::execution", instance=%instance, parent_instance=%parent_instance, parent_id=%parent_id, "Enqueue SubOrchFailed to parent");
                    orchestrator_items.push(WorkItem::SubOrchFailed {
                        parent_instance: parent_instance.clone(),
                        parent_execution_id: self.get_execution_id_for_instance(&parent_instance, None).await,
                        parent_id,
                        details: details.clone(),
                    });
                }

                Err(details.display_message())
            }
            TurnResult::ContinueAsNew { input, version } => {
                // Carry forward unmatched persistent events to the new execution.
                // MUST be called BEFORE emit_terminal_cancellation_breadcrumbs, which appends
                // QueueSubscriptionCancelled events that would confuse the matching.
                let mut unmatched = Self::collect_unmatched_queue_messages(history_mgr.full_history_iter(), instance);

                // Emit cancellation breadcrumbs BEFORE the terminal event.
                // Terminal events (Completed/Failed/CAN) must be the last event in history.
                let outstanding = Self::get_outstanding_schedule_ids(history_mgr.full_history_iter());
                Self::emit_terminal_cancellation_breadcrumbs(history_mgr, instance, execution_id, &outstanding);

                // Add ContinuedAsNew terminal event last
                let next_id = history_mgr.next_event_id();
                history_mgr.append(Event::with_event_id(
                    next_id,
                    instance,
                    execution_id,
                    None,
                    EventKind::OrchestrationContinuedAsNew { input: input.clone() },
                ));

                // Enforce carry-forward limit. Applied generically so future event types
                // (not just persistent externals) are also capped.
                if unmatched.len() > Self::MAX_CARRY_FORWARD {
                    for (name, data) in &unmatched[Self::MAX_CARRY_FORWARD..] {
                        tracing::warn!(
                            instance,
                            name,
                            data,
                            "Dropping carry-forward event beyond limit of {} during continue-as-new",
                            Self::MAX_CARRY_FORWARD,
                        );
                    }
                    unmatched.truncate(Self::MAX_CARRY_FORWARD);
                }

                // Compute accumulated custom status from full history for carry-forward.
                // Scan for the last CustomStatusUpdated event in the complete history.
                let initial_custom_status = Self::compute_accumulated_custom_status(history_mgr.full_history_iter());

                // Enqueue continue as new work item with carry-forward events embedded.
                // The orchestration dispatcher will seed these into the new execution's
                // history before any new externally-raised events, preserving FIFO.
                orchestrator_items.push(WorkItem::ContinueAsNew {
                    instance: instance.to_string(),
                    orchestration: orchestration_name.to_string(),
                    input: input.clone(),
                    version: version.clone(),
                    carry_forward_events: unmatched,
                    initial_custom_status,
                });

                Ok("continued as new".to_string())
            }
            TurnResult::Cancelled(reason) => {
                let details = crate::ErrorDetails::Application {
                    kind: crate::AppErrorKind::Cancelled { reason: reason.clone() },
                    message: String::new(),
                    retryable: false,
                };
                // Emit cancellation breadcrumbs BEFORE the terminal event.
                // Terminal events (Completed/Failed/CAN) must be the last event in history.
                let outstanding = Self::get_outstanding_schedule_ids(history_mgr.full_history_iter());
                Self::emit_terminal_cancellation_breadcrumbs(history_mgr, instance, execution_id, &outstanding);

                // Add failure event last, and propagate cancellation to outstanding sub-orchestrations.
                history_mgr.append_failed(details.clone());

                for (_schedule_id, child_suffix) in &outstanding.sub_orchestrations {
                    orchestrator_items.push(WorkItem::CancelInstance {
                        instance: crate::build_child_instance_id(instance, child_suffix),
                        reason: "parent canceled".to_string(),
                    });
                }

                // Notify parent if this is a sub-orchestration
                if let Some((parent_instance, parent_id)) = parent_link {
                    orchestrator_items.push(WorkItem::SubOrchFailed {
                        parent_instance: parent_instance.clone(),
                        parent_execution_id: self.get_execution_id_for_instance(&parent_instance, None).await,
                        parent_id,
                        details: details.clone(),
                    });
                }

                Err(details.display_message())
            }
        };

        // Now add cancelled sub-orchestration items (deferred to avoid borrow conflict)
        orchestrator_items.extend(cancelled_sub_orch_items);

        debug!(
            instance,
            "run_single_execution_atomic complete: history_delta={}, worker={}, orch={}",
            history_mgr.delta().len(),
            worker_items.len(),
            orchestrator_items.len()
        );
        (
            history_mgr.delta().to_vec(),
            worker_items,
            orchestrator_items,
            cancelled_activities,
            result,
        )
    }

    /// Compute the accumulated custom status from the full history for CAN carry-forward.
    ///
    /// Scans for `initial_custom_status` in `OrchestrationStarted` and all
    /// `CustomStatusUpdated` events, returning the last-write value.
    fn compute_accumulated_custom_status<'a>(history: impl Iterator<Item = &'a crate::Event>) -> Option<String> {
        let mut status: Option<String> = None;
        for event in history {
            match &event.kind {
                crate::EventKind::OrchestrationStarted {
                    initial_custom_status: Some(s),
                    ..
                } => {
                    status = Some(s.clone());
                }
                crate::EventKind::CustomStatusUpdated { status: s } => {
                    status = s.clone();
                }
                _ => {}
            }
        }
        status
    }

    /// Maximum number of events that can be carried forward across a continue-as-new boundary.
    /// Applied generically to the final carry-forward vec regardless of event source.
    const MAX_CARRY_FORWARD: usize = super::limits::MAX_CARRY_FORWARD_EVENTS;

    /// Collect unmatched persistent events from history for CAN carry-forward.
    ///
    /// Per-name FIFO matching: each active (non-cancelled) subscription consumes one
    /// arrival for its name. Remaining arrivals are returned in history (event_id) order
    /// to preserve cross-name FIFO.
    fn collect_unmatched_queue_messages<'a>(
        history: impl Iterator<Item = &'a Event>,
        _instance: &str,
    ) -> Vec<(String, String)> {
        use std::collections::HashMap;

        // Single pass: collect cancelled IDs, active sub counts, and arrivals in order.
        let mut cancelled_ids = std::collections::HashSet::new();
        let mut sub_event_ids: Vec<(u64, String)> = Vec::new(); // (event_id, name)
        let mut arrivals: Vec<(String, String)> = Vec::new(); // (name, data) in history order
        for e in history {
            match &e.kind {
                EventKind::QueueSubscriptionCancelled { .. } => {
                    if let Some(src) = e.source_event_id {
                        cancelled_ids.insert(src);
                    }
                }
                EventKind::QueueSubscribed { name } => {
                    sub_event_ids.push((e.event_id(), name.clone()));
                }
                EventKind::QueueEventDelivered { name, data } => {
                    arrivals.push((name.clone(), data.clone()));
                }
                _ => {}
            }
        }

        // Count active (non-cancelled) subscriptions per name
        let mut active_subs_by_name: HashMap<String, usize> = HashMap::new();
        for (eid, name) in &sub_event_ids {
            if !cancelled_ids.contains(eid) {
                *active_subs_by_name.entry(name.clone()).or_default() += 1;
            }
        }

        // Walk arrivals in FIFO order, consuming per-name
        let mut consumed_by_name: HashMap<String, usize> = HashMap::new();
        let mut unmatched: Vec<(String, String)> = Vec::new();
        for (name, data) in arrivals {
            let consumed = consumed_by_name.entry(name.clone()).or_default();
            let active = active_subs_by_name.get(&name).copied().unwrap_or(0);
            if *consumed < active {
                *consumed += 1;
            } else {
                unmatched.push((name, data));
            }
        }

        unmatched
    }

    /// Find outstanding (unresolved, uncancelled) schedule IDs at orchestration terminal.
    ///
    /// Returns four vectors of schedule_event_ids for:
    /// - Activities: scheduled but not completed/failed/cancel-requested
    /// - Sub-orchestrations: scheduled but not completed/failed/cancel-requested (with child instance suffix)
    /// - Positional subscriptions: subscribed but not delivered/cancelled
    /// - Persistent subscriptions: subscribed but not cancelled (delivery is per-name FIFO, not per-sub)
    ///
    /// This is used to emit `*CancelRequested` / `*Cancelled` breadcrumbs when an
    /// orchestration reaches a terminal state (Completed, Failed, ContinueAsNew, Cancelled).
    fn get_outstanding_schedule_ids<'a>(history: impl Iterator<Item = &'a Event>) -> OutstandingSchedules {
        use std::collections::{HashMap, HashSet};

        let mut scheduled_activities: Vec<u64> = Vec::new();
        let mut scheduled_sub_orchs: Vec<(u64, String)> = Vec::new(); // (event_id, child_suffix)
        let mut subscribed_positional: Vec<u64> = Vec::new();
        let mut subscribed_queue: Vec<u64> = Vec::new();

        let mut resolved_activities: HashSet<u64> = HashSet::new(); // source_event_id of completions/failures
        let mut cancelled_activities: HashSet<u64> = HashSet::new(); // source_event_id of cancel-requests
        let mut resolved_sub_orchs: HashSet<u64> = HashSet::new();
        let mut cancelled_sub_orchs: HashSet<u64> = HashSet::new();

        let mut cancelled_positional: HashSet<u64> = HashSet::new();
        let mut cancelled_persistent: HashSet<u64> = HashSet::new();

        // For positional causal resolution: track pending subs per name
        let mut pending_positional_subs: HashMap<String, std::collections::VecDeque<u64>> = HashMap::new();
        let mut resolved_positional: HashSet<u64> = HashSet::new();

        // For persistent FIFO resolution: count subs and arrivals per name
        let mut queue_sub_ids: Vec<(u64, String)> = Vec::new(); // (event_id, name)
        let mut persistent_arrival_count: HashMap<String, usize> = HashMap::new();

        for e in history {
            match &e.kind {
                // Activities
                EventKind::ActivityScheduled { .. } => {
                    scheduled_activities.push(e.event_id());
                }
                EventKind::ActivityCompleted { .. } | EventKind::ActivityFailed { .. } => {
                    if let Some(src) = e.source_event_id {
                        resolved_activities.insert(src);
                    }
                }
                EventKind::ActivityCancelRequested { .. } => {
                    if let Some(src) = e.source_event_id {
                        cancelled_activities.insert(src);
                    }
                }

                // Sub-orchestrations
                EventKind::SubOrchestrationScheduled { instance: child, .. } => {
                    scheduled_sub_orchs.push((e.event_id(), child.clone()));
                }
                EventKind::SubOrchestrationCompleted { .. } | EventKind::SubOrchestrationFailed { .. } => {
                    if let Some(src) = e.source_event_id {
                        resolved_sub_orchs.insert(src);
                    }
                }
                EventKind::SubOrchestrationCancelRequested { .. } => {
                    if let Some(src) = e.source_event_id {
                        cancelled_sub_orchs.insert(src);
                    }
                }

                // Positional subscriptions (causal matching: arrival must happen after sub)
                EventKind::ExternalSubscribed { name } => {
                    subscribed_positional.push(e.event_id());
                    pending_positional_subs
                        .entry(name.clone())
                        .or_default()
                        .push_back(e.event_id());
                }
                EventKind::ExternalEvent { name, .. } => {
                    // Causal check: only consumes if there is a pending sub
                    if let Some(sub_id) = pending_positional_subs
                        .get_mut(name)
                        .and_then(|queue| queue.pop_front())
                    {
                        resolved_positional.insert(sub_id);
                    }
                }
                EventKind::ExternalSubscribedCancelled { .. } => {
                    if let Some(src) = e.source_event_id {
                        cancelled_positional.insert(src);
                        // Remove from pending queue
                        for queue in pending_positional_subs.values_mut() {
                            queue.retain(|&id| id != src);
                        }
                    }
                }

                // Persistent subscriptions
                EventKind::QueueSubscribed { name } => {
                    subscribed_queue.push(e.event_id());
                    queue_sub_ids.push((e.event_id(), name.clone()));
                }
                EventKind::QueueEventDelivered { name, .. } => {
                    *persistent_arrival_count.entry(name.clone()).or_default() += 1;
                }
                EventKind::QueueSubscriptionCancelled { .. } => {
                    if let Some(src) = e.source_event_id {
                        cancelled_persistent.insert(src);
                    }
                }

                _ => {}
            }
        }

        // For persistent subs: a subscription is "resolved" if it was consumed by a FIFO arrival.
        // Walk subs by name in order, consuming arrivals per-name.
        let mut resolved_persistent: HashSet<u64> = HashSet::new();
        let mut consumed_per_name: HashMap<String, usize> = HashMap::new();
        for (eid, name) in &queue_sub_ids {
            if cancelled_persistent.contains(eid) {
                continue; // Cancelled subs don't consume
            }
            let consumed = consumed_per_name.entry(name.clone()).or_default();
            let available = persistent_arrival_count.get(name).copied().unwrap_or(0);
            if *consumed < available {
                *consumed += 1;
                resolved_persistent.insert(*eid);
            }
        }

        OutstandingSchedules {
            activities: scheduled_activities
                .into_iter()
                .filter(|id| !resolved_activities.contains(id) && !cancelled_activities.contains(id))
                .collect(),
            sub_orchestrations: scheduled_sub_orchs
                .into_iter()
                .filter(|(id, _)| !resolved_sub_orchs.contains(id) && !cancelled_sub_orchs.contains(id))
                .collect(),
            positional_subscriptions: subscribed_positional
                .into_iter()
                .filter(|id| !resolved_positional.contains(id) && !cancelled_positional.contains(id))
                .collect(),
            queue_subscriptions: subscribed_queue
                .into_iter()
                .filter(|id| !resolved_persistent.contains(id) && !cancelled_persistent.contains(id))
                .collect(),
        }
    }

    /// Emit terminal-state cancellation breadcrumbs for outstanding subscriptions.
    ///
    /// Called at Completed, Failed, ContinueAsNew, and Cancelled terminal states.
    /// Emits `*Cancelled` events with `reason: "orchestration_terminal"` for any
    /// subscription that hasn't been resolved or already cancelled (e.g. by `dropped_future`).
    ///
    /// NOTE: Activities and sub-orchestrations are NOT handled here because:
    /// - Activities are cancelled via lock-stealing in the orchestration dispatcher
    ///   (`compute_inflight_activities` + `cancel_scheduled_activity`)
    /// - Sub-orchestrations are cancelled via `cancelled_sub_orch_items` (dropped futures)
    ///   and explicit `CancelInstance` work items in the Cancelled branch
    fn emit_terminal_cancellation_breadcrumbs(
        history_mgr: &mut crate::runtime::state_helpers::HistoryManager,
        instance: &str,
        execution_id: u64,
        outstanding: &OutstandingSchedules,
    ) {
        for &schedule_id in &outstanding.positional_subscriptions {
            let next_id = history_mgr.next_event_id();
            history_mgr.append(Event::with_event_id(
                next_id,
                instance,
                execution_id,
                Some(schedule_id),
                EventKind::ExternalSubscribedCancelled {
                    reason: "orchestration_terminal".to_string(),
                },
            ));
        }

        for &schedule_id in &outstanding.queue_subscriptions {
            let next_id = history_mgr.next_event_id();
            history_mgr.append(Event::with_event_id(
                next_id,
                instance,
                execution_id,
                Some(schedule_id),
                EventKind::QueueSubscriptionCancelled {
                    reason: "orchestration_terminal".to_string(),
                },
            ));
        }
    }
}

/// Outstanding schedule IDs at an orchestration terminal state.
struct OutstandingSchedules {
    /// Activity schedule IDs that are unresolved and uncancelled.
    /// Not used for breadcrumb emission (dispatcher handles via lock-stealing),
    /// but computed for completeness and potential future use.
    #[allow(dead_code)]
    activities: Vec<u64>,
    /// Sub-orchestration schedule IDs (with child suffix) that are unresolved and uncancelled.
    sub_orchestrations: Vec<(u64, String)>,
    /// Positional subscription schedule IDs that are unresolved and uncancelled.
    positional_subscriptions: Vec<u64>,
    /// Persistent subscription schedule IDs that are unresolved and uncancelled.
    queue_subscriptions: Vec<u64>,
}

#[cfg(test)]
#[allow(clippy::useless_vec)]
mod tests {
    use super::*;
    use crate::runtime::Runtime;

    /// Helper: create a persistent subscription event.
    fn sub(event_id: u64, name: &str) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            None,
            EventKind::QueueSubscribed { name: name.to_string() },
        )
    }

    /// Helper: create a persistent event arrival.
    fn arrival(event_id: u64, name: &str, data: &str) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            None,
            EventKind::QueueEventDelivered {
                name: name.to_string(),
                data: data.to_string(),
            },
        )
    }

    /// Helper: create a persistent subscription cancellation breadcrumb.
    fn cancel(event_id: u64, source_event_id: u64) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            Some(source_event_id),
            EventKind::QueueSubscriptionCancelled {
                reason: "dropped_future".to_string(),
            },
        )
    }

    /// Helper: some unrelated event to ensure it's ignored.
    fn activity_scheduled(event_id: u64) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            None,
            EventKind::ActivityScheduled {
                name: "task".to_string(),
                input: "{}".to_string(),
                session_id: None,
                tag: None,
            },
        )
    }

    #[test]
    fn no_events_returns_empty() {
        let history: Vec<Event> = vec![];
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert!(result.is_empty());
    }

    #[test]
    fn arrivals_only_all_unmatched() {
        let history = vec![arrival(1, "X", "a"), arrival(2, "X", "b"), arrival(3, "Y", "c")];
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert_eq!(
            result,
            vec![
                ("X".to_string(), "a".to_string()),
                ("X".to_string(), "b".to_string()),
                ("Y".to_string(), "c".to_string()),
            ]
        );
    }

    #[test]
    fn subs_consume_arrivals_per_name() {
        let history = vec![
            sub(1, "X"),
            sub(2, "Y"),
            arrival(3, "X", "x1"),
            arrival(4, "X", "x2"),
            arrival(5, "Y", "y1"),
        ];
        // X: 1 sub consumes x1, x2 is unmatched
        // Y: 1 sub consumes y1, nothing unmatched
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert_eq!(result, vec![("X".to_string(), "x2".to_string())]);
    }

    #[test]
    fn cancelled_sub_does_not_consume() {
        let history = vec![
            sub(1, "X"),  // active
            sub(2, "X"),  // will be cancelled
            cancel(3, 2), // cancels sub at event_id=2
            arrival(4, "X", "a"),
            arrival(5, "X", "b"),
        ];
        // 1 active sub (event 1) consumes "a"; "b" is unmatched
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert_eq!(result, vec![("X".to_string(), "b".to_string())]);
    }

    #[test]
    fn per_name_isolation() {
        // Sub for Y should NOT consume arrival for X
        let history = vec![sub(1, "Y"), arrival(2, "X", "x-data"), arrival(3, "Y", "y-data")];
        // Y: 1 sub consumes y-data
        // X: 0 subs, x-data unmatched
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert_eq!(result, vec![("X".to_string(), "x-data".to_string())]);
    }

    #[test]
    fn fifo_order_preserved_across_names() {
        let history = vec![
            arrival(1, "B", "b1"),
            arrival(2, "A", "a1"),
            arrival(3, "B", "b2"),
            arrival(4, "A", "a2"),
        ];
        // All unmatched, order must be: B-b1, A-a1, B-b2, A-a2
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert_eq!(
            result,
            vec![
                ("B".to_string(), "b1".to_string()),
                ("A".to_string(), "a1".to_string()),
                ("B".to_string(), "b2".to_string()),
                ("A".to_string(), "a2".to_string()),
            ]
        );
    }

    #[test]
    fn fifo_order_with_partial_consumption() {
        // Sub for A consumes a1 (first A arrival); remaining order is B-b1, A-a2, B-b2
        let history = vec![
            sub(1, "A"),
            arrival(2, "B", "b1"),
            arrival(3, "A", "a1"), // consumed by sub
            arrival(4, "A", "a2"),
            arrival(5, "B", "b2"),
        ];
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert_eq!(
            result,
            vec![
                ("B".to_string(), "b1".to_string()),
                ("A".to_string(), "a2".to_string()),
                ("B".to_string(), "b2".to_string()),
            ]
        );
    }

    #[test]
    fn unrelated_events_are_ignored() {
        let history = vec![
            activity_scheduled(1),
            sub(2, "X"),
            activity_scheduled(3),
            arrival(4, "X", "x1"),
            arrival(5, "X", "x2"),
            activity_scheduled(6),
        ];
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert_eq!(result, vec![("X".to_string(), "x2".to_string())]);
    }

    #[test]
    fn more_subs_than_arrivals_returns_empty() {
        let history = vec![sub(1, "X"), sub(2, "X"), sub(3, "X"), arrival(4, "X", "only-one")];
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert!(result.is_empty());
    }

    #[test]
    fn cancelled_sub_with_multiple_names() {
        let history = vec![
            sub(1, "A"), // active
            sub(2, "B"), // cancelled
            cancel(3, 2),
            arrival(4, "A", "a-data"), // consumed by A sub
            arrival(5, "B", "b-data"), // B sub cancelled → unmatched
        ];
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert_eq!(result, vec![("B".to_string(), "b-data".to_string())]);
    }

    #[test]
    fn collect_returns_all_unmatched_without_cap() {
        let mut history = Vec::new();
        // 25 arrivals, no subs → all 25 returned (cap is applied at CAN site, not here)
        for i in 0..25u64 {
            history.push(arrival(i + 1, "X", &format!("data-{i}")));
        }
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert_eq!(result.len(), 25);
        for (i, (name, data)) in result.iter().enumerate() {
            assert_eq!(name, "X");
            assert_eq!(data, &format!("data-{i}"));
        }
    }

    #[test]
    fn max_carry_forward_constant_is_20() {
        assert_eq!(Runtime::MAX_CARRY_FORWARD, 20);
    }

    #[test]
    fn empty_history_no_subs_no_arrivals() {
        let history = vec![activity_scheduled(1), activity_scheduled(2)];
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert!(result.is_empty());
    }

    #[test]
    fn cancel_without_matching_sub_is_harmless() {
        // A cancellation for a non-existent sub should be silently ignored
        let history = vec![
            cancel(1, 999), // no sub with event_id=999
            arrival(2, "X", "data"),
        ];
        let result = Runtime::collect_unmatched_queue_messages(history.iter(), "inst");
        assert_eq!(result, vec![("X".to_string(), "data".to_string())]);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Helpers for get_outstanding_schedule_ids tests
    // ──────────────────────────────────────────────────────────────────────────

    fn positional_sub(event_id: u64, name: &str) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            None,
            EventKind::ExternalSubscribed { name: name.to_string() },
        )
    }

    fn positional_event(event_id: u64, source_event_id: u64, name: &str, data: &str) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            Some(source_event_id),
            EventKind::ExternalEvent {
                name: name.to_string(),
                data: data.to_string(),
            },
        )
    }

    fn positional_cancelled(event_id: u64, source_event_id: u64) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            Some(source_event_id),
            EventKind::ExternalSubscribedCancelled {
                reason: "dropped_future".to_string(),
            },
        )
    }

    fn activity_completed(event_id: u64, source_event_id: u64) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            Some(source_event_id),
            EventKind::ActivityCompleted {
                result: "ok".to_string(),
            },
        )
    }

    fn activity_cancel_requested(event_id: u64, source_event_id: u64) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            Some(source_event_id),
            EventKind::ActivityCancelRequested {
                reason: "dropped_future".to_string(),
            },
        )
    }

    fn sub_orch_scheduled(event_id: u64, child: &str) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            None,
            EventKind::SubOrchestrationScheduled {
                name: "child".to_string(),
                instance: child.to_string(),
                input: "{}".to_string(),
            },
        )
    }

    fn sub_orch_completed(event_id: u64, source_event_id: u64) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            Some(source_event_id),
            EventKind::SubOrchestrationCompleted {
                result: "done".to_string(),
            },
        )
    }

    fn sub_orch_cancel_requested(event_id: u64, source_event_id: u64) -> Event {
        Event::with_event_id(
            event_id,
            "inst",
            1,
            Some(source_event_id),
            EventKind::SubOrchestrationCancelRequested {
                reason: "dropped_future".to_string(),
            },
        )
    }

    // ──────────────────────────────────────────────────────────────────────────
    // get_outstanding_schedule_ids tests
    // ──────────────────────────────────────────────────────────────────────────

    #[test]
    fn outstanding_empty_history() {
        let history: Vec<Event> = vec![];
        let result = Runtime::get_outstanding_schedule_ids(history.iter());
        assert!(result.activities.is_empty());
        assert!(result.sub_orchestrations.is_empty());
        assert!(result.positional_subscriptions.is_empty());
        assert!(result.queue_subscriptions.is_empty());
    }

    #[test]
    fn outstanding_all_resolved() {
        let history = vec![
            activity_scheduled(1),
            activity_completed(2, 1),
            positional_sub(3, "ev"),
            positional_event(4, 3, "ev", "data"),
            sub(5, "X"),
            arrival(6, "X", "x-data"),
            sub_orch_scheduled(7, "child-1"),
            sub_orch_completed(8, 7),
        ];
        let result = Runtime::get_outstanding_schedule_ids(history.iter());
        assert!(result.activities.is_empty());
        assert!(result.sub_orchestrations.is_empty());
        assert!(result.positional_subscriptions.is_empty());
        assert!(result.queue_subscriptions.is_empty());
    }

    #[test]
    fn outstanding_all_cancelled() {
        let history = vec![
            activity_scheduled(1),
            activity_cancel_requested(2, 1),
            positional_sub(3, "ev"),
            positional_cancelled(4, 3),
            sub(5, "X"),
            cancel(6, 5),
            sub_orch_scheduled(7, "child-1"),
            sub_orch_cancel_requested(8, 7),
        ];
        let result = Runtime::get_outstanding_schedule_ids(history.iter());
        assert!(result.activities.is_empty());
        assert!(result.sub_orchestrations.is_empty());
        assert!(result.positional_subscriptions.is_empty());
        assert!(result.queue_subscriptions.is_empty());
    }

    #[test]
    fn outstanding_mixed_resolved_and_outstanding() {
        let history = vec![
            activity_scheduled(1), // resolved
            activity_completed(2, 1),
            activity_scheduled(3),   // outstanding
            positional_sub(4, "ev"), // resolved
            positional_event(5, 4, "ev", "d"),
            positional_sub(6, "ev2"), // outstanding
            sub(7, "X"),              // resolved (consumed by arrival)
            arrival(8, "X", "x-data"),
            sub(9, "Y"),                  // outstanding (no arrival for Y)
            sub_orch_scheduled(10, "c1"), // resolved
            sub_orch_completed(11, 10),
            sub_orch_scheduled(12, "c2"), // outstanding
        ];
        let result = Runtime::get_outstanding_schedule_ids(history.iter());
        assert_eq!(result.activities, vec![3]);
        assert_eq!(result.sub_orchestrations, vec![(12, "c2".to_string())]);
        assert_eq!(result.positional_subscriptions, vec![6]);
        assert_eq!(result.queue_subscriptions, vec![9]);
    }

    #[test]
    fn outstanding_persistent_fifo_partial() {
        // 2 subs for X, 1 arrival — first sub consumed, second outstanding
        let history = vec![sub(1, "X"), sub(2, "X"), arrival(3, "X", "data")];
        let result = Runtime::get_outstanding_schedule_ids(history.iter());
        assert_eq!(result.queue_subscriptions, vec![2]);
    }

    #[test]
    fn outstanding_persistent_cancelled_sub_not_counted_as_consumer() {
        // Sub at 1 is cancelled, sub at 2 is active → 1 arrival consumed by sub 2
        let history = vec![sub(1, "X"), cancel(3, 1), sub(2, "X"), arrival(4, "X", "data")];
        let result = Runtime::get_outstanding_schedule_ids(history.iter());
        // sub 1 is cancelled (not outstanding), sub 2 consumed by arrival (not outstanding)
        assert!(result.queue_subscriptions.is_empty());
    }

    #[test]
    fn outstanding_persistent_more_arrivals_than_subs() {
        // 1 sub, 3 arrivals — sub consumed, no outstanding subs
        let history = vec![
            sub(1, "X"),
            arrival(2, "X", "a"),
            arrival(3, "X", "b"),
            arrival(4, "X", "c"),
        ];
        let result = Runtime::get_outstanding_schedule_ids(history.iter());
        assert!(result.queue_subscriptions.is_empty());
    }
}

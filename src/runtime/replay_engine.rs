// Replay engine uses Mutex locks - poison indicates a panic and should propagate
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]

use crate::{Action, Event, EventKind, providers::WorkItem, runtime::OrchestrationHandler};
use crate::{CompletionResult, OrchestrationContext};
use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use tracing::{debug, warn};

/// Result of executing an orchestration turn
#[derive(Debug)]
pub enum TurnResult {
    /// Turn completed successfully, orchestration continues
    Continue,
    /// Orchestration completed with output
    Completed(String),
    /// Orchestration failed with error details
    Failed(crate::ErrorDetails),
    /// Orchestration requested continue-as-new
    ContinueAsNew { input: String, version: Option<String> },
    /// Orchestration was cancelled
    Cancelled(String),
}

/// Replays history and executes one deterministic orchestration evaluation
pub struct ReplayEngine {
    /// Instance identifier
    pub(crate) instance: String,

    /// Current execution ID
    pub(crate) execution_id: u64,
    /// History events generated during this run
    pub(crate) history_delta: Vec<Event>,
    /// Actions to dispatch after persistence
    pub(crate) pending_actions: Vec<crate::Action>,

    /// ActivityScheduled event_ids for activity losers of select/select2.
    /// These should be cancelled via provider lock stealing.
    pub(crate) cancelled_activity_ids: Vec<u64>,

    /// Sub-orchestration instance IDs for sub-orch losers of select/select2.
    /// These should be cancelled via CancelInstance work items.
    pub(crate) cancelled_sub_orchestration_ids: Vec<String>,

    /// Current history at start of run
    pub(crate) baseline_history: Vec<Event>,
    /// Next event_id for new events added this run
    pub(crate) next_event_id: u64,
    /// Unified error collector for system-level errors that abort the turn
    pub(crate) abort_error: Option<crate::ErrorDetails>,

    /// Number of events in baseline_history that were actually persisted (from DB).
    /// Events beyond this index in baseline_history are NEW this turn (not replay).
    /// Used to correctly track is_replaying state.
    persisted_history_len: usize,
}

impl ReplayEngine {
    /// Create a new replay engine for an instance/execution
    pub fn new(instance: String, execution_id: u64, baseline_history: Vec<Event>) -> Self {
        let next_event_id = baseline_history.last().map(|e| e.event_id() + 1).unwrap_or(1);
        let persisted_len = baseline_history.len(); // Default: assume all are persisted

        Self {
            instance,
            execution_id,
            history_delta: Vec::new(),
            pending_actions: Vec::new(),
            cancelled_activity_ids: Vec::new(),
            cancelled_sub_orchestration_ids: Vec::new(),
            baseline_history,
            next_event_id,
            abort_error: None,
            persisted_history_len: persisted_len,
        }
    }

    /// Set the number of events in baseline_history that were actually persisted.
    ///
    /// This is used to correctly track the `is_replaying` state.
    /// Events at indices `0..persisted_len` in the working history are replays;
    /// events at indices `persisted_len..` are new this turn.
    ///
    /// This should be set to `history_mgr.original_len()` - the count of events
    /// that came from the database, NOT including newly created events like
    /// OrchestrationStarted on the first turn.
    pub fn with_persisted_history_len(mut self, len: usize) -> Self {
        self.persisted_history_len = len;
        self
    }

    /// Stage 1: Convert completion messages directly to events
    ///
    /// The conversion from WorkItem to Event generates history_delta
    /// which is then processed by the execution path.
    pub fn prep_completions(&mut self, messages: Vec<WorkItem>) {
        debug!(
            instance = %self.instance,
            message_count = messages.len(),
            "converting messages to events"
        );

        for msg in messages {
            // Check filtering conditions
            if !self.is_completion_for_current_execution(&msg) {
                if self.has_continue_as_new_in_history() {
                    warn!(instance = %self.instance, "ignoring completion from previous execution");
                } else {
                    warn!(instance = %self.instance, "completion from different execution");
                }
                continue;
            }

            if self.is_completion_already_in_history(&msg) {
                warn!(instance = %self.instance, "ignoring duplicate completion");
                continue;
            }

            // Drop duplicates already staged in this run's history_delta
            let already_in_delta = match &msg {
                WorkItem::ActivityCompleted { id, .. } | WorkItem::ActivityFailed { id, .. } => {
                    self.history_delta.iter().any(|e| {
                        e.source_event_id == Some(*id)
                            && matches!(
                                &e.kind,
                                EventKind::ActivityCompleted { .. } | EventKind::ActivityFailed { .. }
                            )
                    })
                }
                WorkItem::TimerFired { id, .. } => self
                    .history_delta
                    .iter()
                    .any(|e| e.source_event_id == Some(*id) && matches!(&e.kind, EventKind::TimerFired { .. })),
                WorkItem::SubOrchCompleted { parent_id, .. } | WorkItem::SubOrchFailed { parent_id, .. } => {
                    self.history_delta.iter().any(|e| {
                        e.source_event_id == Some(*parent_id)
                            && matches!(
                                &e.kind,
                                EventKind::SubOrchestrationCompleted { .. } | EventKind::SubOrchestrationFailed { .. }
                            )
                    })
                }
                // External events (positional, persistent, v2) are never deduplicated.
                // Multiple events with the same name+data are separate arrivals by design.
                WorkItem::ExternalRaised { .. } => false,
                WorkItem::QueueMessage { .. } => false,
                #[cfg(feature = "replay-version-test")]
                WorkItem::ExternalRaised2 { .. } => false,
                WorkItem::CancelInstance { .. } => false,
                _ => false, // Non-completion work items
            };
            if already_in_delta {
                warn!(instance = %self.instance, "dropping duplicate completion in current run");
                continue;
            }

            // Nondeterminism detection: ensure completion has a matching schedule and kind
            let schedule_kind = |id: &u64| -> Option<&'static str> {
                for e in self.baseline_history.iter().chain(self.history_delta.iter()) {
                    if e.event_id != *id {
                        continue;
                    }
                    match &e.kind {
                        EventKind::ActivityScheduled { .. } => return Some("activity"),
                        EventKind::TimerCreated { .. } => return Some("timer"),
                        EventKind::SubOrchestrationScheduled { .. } => return Some("suborchestration"),
                        _ => {}
                    }
                }
                None
            };
            let mut nd_err: Option<crate::ErrorDetails> = None;
            match &msg {
                WorkItem::ActivityCompleted { id, .. } | WorkItem::ActivityFailed { id, .. } => {
                    match schedule_kind(id) {
                        Some("activity") => {}
                        Some(other) => {
                            nd_err = Some(crate::ErrorDetails::Configuration {
                                kind: crate::ConfigErrorKind::Nondeterminism,
                                resource: String::new(),
                                message: Some(format!(
                                    "completion kind mismatch for id={id}, expected '{other}', got 'activity'"
                                )),
                            })
                        }
                        None => {
                            nd_err = Some(crate::ErrorDetails::Configuration {
                                kind: crate::ConfigErrorKind::Nondeterminism,
                                resource: String::new(),
                                message: Some(format!("no matching schedule for completion id={id}")),
                            })
                        }
                    }
                }
                WorkItem::TimerFired { id, .. } => match schedule_kind(id) {
                    Some("timer") => {}
                    Some(other) => {
                        nd_err = Some(crate::ErrorDetails::Configuration {
                            kind: crate::ConfigErrorKind::Nondeterminism,
                            resource: String::new(),
                            message: Some(format!(
                                "completion kind mismatch for id={id}, expected '{other}', got 'timer'"
                            )),
                        })
                    }
                    None => {
                        nd_err = Some(crate::ErrorDetails::Configuration {
                            kind: crate::ConfigErrorKind::Nondeterminism,
                            resource: String::new(),
                            message: Some(format!("no matching schedule for timer id={id}")),
                        })
                    }
                },
                WorkItem::SubOrchCompleted { parent_id, .. } | WorkItem::SubOrchFailed { parent_id, .. } => {
                    match schedule_kind(parent_id) {
                        Some("suborchestration") => {}
                        Some(other) => {
                            nd_err = Some(crate::ErrorDetails::Configuration {
                                kind: crate::ConfigErrorKind::Nondeterminism,
                                resource: String::new(),
                                message: Some(format!(
                                    "completion kind mismatch for id={parent_id}, expected '{other}', got 'suborchestration'"
                                )),
                            })
                        }
                        None => {
                            nd_err = Some(crate::ErrorDetails::Configuration {
                                kind: crate::ConfigErrorKind::Nondeterminism,
                                resource: String::new(),
                                message: Some(format!("no matching schedule for sub-orchestration id={parent_id}")),
                            })
                        }
                    }
                }
                WorkItem::ExternalRaised { .. } | WorkItem::QueueMessage { .. } | WorkItem::CancelInstance { .. } => {}
                #[cfg(feature = "replay-version-test")]
                WorkItem::ExternalRaised2 { .. } => {}
                _ => {} // Non-completion work items
            }
            if let Some(err) = nd_err {
                warn!(instance = %self.instance, error = %err.display_message(), "detected nondeterminism in completion batch");
                self.abort_error = Some(err);
                continue;
            }

            // Convert message to event
            let event_opt = match msg {
                WorkItem::ActivityCompleted { id, result, .. } => Some(Event::new(
                    &self.instance,
                    self.execution_id,
                    Some(id),
                    EventKind::ActivityCompleted { result },
                )),
                WorkItem::ActivityFailed { id, details, .. } => {
                    // Always create event in history for audit trail
                    let event = Event::new(
                        &self.instance,
                        self.execution_id,
                        Some(id),
                        EventKind::ActivityFailed {
                            details: details.clone(),
                        },
                    );

                    // Check if system error (abort turn)
                    match &details {
                        crate::ErrorDetails::Configuration { .. }
                        | crate::ErrorDetails::Infrastructure { .. }
                        | crate::ErrorDetails::Poison { .. } => {
                            warn!(
                                instance = %self.instance,
                                activity_id = id,
                                error = %details.display_message(),
                                "System error aborts turn"
                            );
                            if self.abort_error.is_none() {
                                self.abort_error = Some(details.clone());
                            }
                        }
                        crate::ErrorDetails::Application { .. } => {
                            // Normal flow
                        }
                    }

                    Some(event)
                }
                WorkItem::TimerFired { id, fire_at_ms, .. } => Some(Event::new(
                    &self.instance,
                    self.execution_id,
                    Some(id),
                    EventKind::TimerFired { fire_at_ms },
                )),
                WorkItem::ExternalRaised { name, data, .. } => {
                    // Materialize unconditionally. The replay engine's matching algorithm
                    // handles cancelled subscriptions by consuming (and discarding) arrivals,
                    // so stale events don't corrupt delivery to later subscriptions.
                    Some(Event::new(
                        &self.instance,
                        self.execution_id,
                        None, // ExternalEvent doesn't have source_event_id
                        EventKind::ExternalEvent { name, data },
                    ))
                }
                WorkItem::QueueMessage { name, data, .. } => {
                    // Materialize unconditionally. The cap on persistent events per execution
                    // is enforced at CAN carry-forward time, not at materialization time.
                    Some(Event::new(
                        &self.instance,
                        self.execution_id,
                        None,
                        EventKind::QueueEventDelivered { name, data },
                    ))
                }
                #[cfg(feature = "replay-version-test")]
                WorkItem::ExternalRaised2 { name, topic, data, .. } => {
                    // Only materialize ExternalEvent2 if a subscription exists with matching name AND topic
                    let subscribed = self.baseline_history.iter().any(
                        |e| matches!(&e.kind, EventKind::ExternalSubscribed2 { name: n, topic: t } if n == &name && t == &topic),
                    );
                    if subscribed {
                        Some(Event::new(
                            &self.instance,
                            self.execution_id,
                            None,
                            EventKind::ExternalEvent2 { name, topic, data },
                        ))
                    } else {
                        warn!(instance = %self.instance, event_name=%name, topic=%topic, "dropping ExternalRaised2 with no matching subscription in history");
                        None
                    }
                }
                WorkItem::SubOrchCompleted { parent_id, result, .. } => Some(Event::new(
                    &self.instance,
                    self.execution_id,
                    Some(parent_id),
                    EventKind::SubOrchestrationCompleted { result },
                )),
                WorkItem::SubOrchFailed { parent_id, details, .. } => {
                    // Always create event in history for audit trail
                    let event = Event::new(
                        &self.instance,
                        self.execution_id,
                        Some(parent_id),
                        EventKind::SubOrchestrationFailed {
                            details: details.clone(),
                        },
                    );

                    // Check if system error (abort parent turn)
                    match &details {
                        crate::ErrorDetails::Configuration { .. }
                        | crate::ErrorDetails::Infrastructure { .. }
                        | crate::ErrorDetails::Poison { .. } => {
                            warn!(instance = %self.instance, parent_id, ?details, "Child system error aborts parent");
                            if self.abort_error.is_none() {
                                self.abort_error = Some(details);
                            }
                        }
                        crate::ErrorDetails::Application { .. } => {
                            // Normal flow
                        }
                    }

                    Some(event)
                }
                WorkItem::CancelInstance { reason, .. } => {
                    let already_terminated = self.baseline_history.iter().any(|e| {
                        matches!(
                            &e.kind,
                            EventKind::OrchestrationCompleted { .. } | EventKind::OrchestrationFailed { .. }
                        )
                    });
                    let already_cancelled = self
                        .baseline_history
                        .iter()
                        .chain(self.history_delta.iter())
                        .any(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. }));

                    if !already_terminated && !already_cancelled {
                        Some(Event::new(
                            &self.instance,
                            self.execution_id,
                            None,
                            EventKind::OrchestrationCancelRequested { reason },
                        ))
                    } else {
                        None
                    }
                }
                _ => None, // Non-completion work items
            };

            if let Some(mut event) = event_opt {
                // Assign event_id and add to history_delta
                event.set_event_id(self.next_event_id);
                self.next_event_id += 1;
                self.history_delta.push(event);
            }
        }

        debug!(
            instance = %self.instance,
            event_count = self.history_delta.len(),
            "completion events created"
        );
    }

    /// Get the current execution ID from the baseline history
    fn get_current_execution_id(&self) -> u64 {
        self.execution_id
    }

    /// Check if a completion message belongs to the current execution
    fn is_completion_for_current_execution(&self, msg: &WorkItem) -> bool {
        let current_execution_id = self.get_current_execution_id();
        match msg {
            WorkItem::ActivityCompleted { execution_id, .. } => *execution_id == current_execution_id,
            WorkItem::ActivityFailed { execution_id, .. } => *execution_id == current_execution_id,
            WorkItem::TimerFired { execution_id, .. } => *execution_id == current_execution_id,
            WorkItem::SubOrchCompleted {
                parent_execution_id, ..
            } => *parent_execution_id == current_execution_id,
            WorkItem::SubOrchFailed {
                parent_execution_id, ..
            } => *parent_execution_id == current_execution_id,
            WorkItem::ExternalRaised { .. } => true, // External events don't have execution IDs
            WorkItem::QueueMessage { .. } => true,   // Persistent external events don't have execution IDs
            #[cfg(feature = "replay-version-test")]
            WorkItem::ExternalRaised2 { .. } => true, // V2 external events don't have execution IDs either
            WorkItem::CancelInstance { .. } => true, // Cancellation applies to current execution
            _ => false,                              // Non-completion work items (shouldn't reach here)
        }
    }

    /// Check if a completion is already in the baseline history (duplicate)
    fn is_completion_already_in_history(&self, msg: &WorkItem) -> bool {
        match msg {
            WorkItem::ActivityCompleted { id, .. } => self
                .baseline_history
                .iter()
                .any(|e| e.source_event_id == Some(*id) && matches!(&e.kind, EventKind::ActivityCompleted { .. })),
            WorkItem::TimerFired { id, .. } => self
                .baseline_history
                .iter()
                .any(|e| e.source_event_id == Some(*id) && matches!(&e.kind, EventKind::TimerFired { .. })),
            WorkItem::SubOrchCompleted { parent_id, .. } => self.baseline_history.iter().any(|e| {
                e.source_event_id == Some(*parent_id) && matches!(&e.kind, EventKind::SubOrchestrationCompleted { .. })
            }),
            WorkItem::SubOrchFailed { parent_id, .. } => self.baseline_history.iter().any(|e| {
                e.source_event_id == Some(*parent_id) && matches!(&e.kind, EventKind::SubOrchestrationFailed { .. })
            }),
            // External events (positional, persistent, v2) are never deduplicated.
            // Multiple events with the same name+data are separate arrivals by design.
            WorkItem::ExternalRaised { .. } => false,
            WorkItem::QueueMessage { .. } => false,
            #[cfg(feature = "replay-version-test")]
            WorkItem::ExternalRaised2 { .. } => false,
            _ => false,
        }
    }

    /// Check if this orchestration has used continue_as_new
    fn has_continue_as_new_in_history(&self) -> bool {
        self.baseline_history
            .iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationContinuedAsNew { .. }))
    }

    /// Stage 2: Execute one turn of the orchestration using the replay engine
    /// This stage runs the orchestration logic and generates history deltas and actions
    ///
    /// This implementation uses the commands-vs-history model:
    /// 1. Processes history events in order, matching emitted actions
    /// 2. Delivers completions to the results map
    /// 3. Returns new actions beyond history as pending_actions
    pub fn execute_orchestration(
        &mut self,
        handler: Arc<dyn OrchestrationHandler>,
        input: String,
        orchestration_name: String,
        orchestration_version: String,
        worker_id: &str,
    ) -> TurnResult {
        debug!(instance = %self.instance, "executing orchestration turn");

        if let Some(err) = self.abort_error.clone() {
            return TurnResult::Failed(err);
        }

        let mut working_history = self.baseline_history.clone();
        working_history.extend_from_slice(&self.history_delta);

        if working_history.iter().any(|e| {
            matches!(
                e.kind,
                EventKind::OrchestrationCompleted { .. }
                    | EventKind::OrchestrationFailed { .. }
                    | EventKind::OrchestrationContinuedAsNew { .. }
            )
        }) {
            return TurnResult::Continue;
        }

        if working_history.is_empty() {
            return TurnResult::Failed(crate::ErrorDetails::Configuration {
                kind: crate::ConfigErrorKind::Nondeterminism,
                resource: String::new(),
                message: Some("corrupted history: empty".to_string()),
            });
        }

        if !matches!(working_history[0].kind, EventKind::OrchestrationStarted { .. }) {
            return TurnResult::Failed(crate::ErrorDetails::Configuration {
                kind: crate::ConfigErrorKind::Nondeterminism,
                resource: String::new(),
                message: Some("corrupted history: first event must be OrchestrationStarted".to_string()),
            });
        }

        let ctx = OrchestrationContext::new(
            Vec::new(),
            self.execution_id,
            self.instance.clone(),
            orchestration_name.clone(),
            orchestration_version.clone(),
            Some(worker_id.to_string()),
        );

        let ctx_for_future = ctx.clone();
        let h = handler.clone();
        let inp = input.clone();
        let fut_result = catch_unwind(AssertUnwindSafe(|| {
            let f: Pin<Box<dyn Future<Output = Result<String, String>> + Send>> =
                Box::pin(async move { h.invoke(ctx_for_future, inp).await });
            f
        }));

        let mut fut = match fut_result {
            Ok(f) => f,
            Err(panic_payload) => {
                let msg = extract_panic_message(panic_payload);
                return TurnResult::Failed(crate::ErrorDetails::Application {
                    kind: crate::AppErrorKind::Panicked,
                    message: msg,
                    retryable: false,
                });
            }
        };

        let mut open_schedules: HashSet<u64> = HashSet::new();
        let mut schedule_kinds: std::collections::HashMap<u64, ActionKind> = std::collections::HashMap::new();
        let mut emitted_actions: VecDeque<(u64, Action)> = VecDeque::new();

        let mut must_poll = true;
        let mut output_opt: Option<Result<String, String>> = None;
        let replay_boundary = self.persisted_history_len;

        if replay_boundary == 0 {
            ctx.set_is_replaying(false);
        }

        // Pre-initialize accumulated_custom_status from OrchestrationStarted before any polling.
        // This is needed because the first poll may happen before apply_history_event processes
        // the OrchestrationStarted event (the loop polls before applying at each iteration).
        if let Some(EventKind::OrchestrationStarted {
            initial_custom_status: Some(status),
            ..
        }) = working_history.first().map(|e| &e.kind)
        {
            ctx.inner
                .lock()
                .expect("Mutex should not be poisoned")
                .accumulated_custom_status = Some(status.clone());
        }

        for (event_index, event) in working_history.iter().enumerate() {
            if event_index >= replay_boundary {
                ctx.set_is_replaying(false);
            }

            if must_poll {
                match Self::poll_orchestration_future(&mut fut, &ctx, &mut emitted_actions) {
                    Ok(Some(result)) => {
                        output_opt = Some(result);
                        break;
                    }
                    Ok(None) => {}
                    Err(err) => return err,
                }
                must_poll = false;
            }

            if let Err(err) = self.apply_history_event(
                &ctx,
                event,
                &mut emitted_actions,
                &mut open_schedules,
                &mut schedule_kinds,
                &mut must_poll,
            ) {
                return err;
            }
        }

        ctx.set_is_replaying(false);

        if must_poll && output_opt.is_none() {
            match Self::poll_orchestration_future(&mut fut, &ctx, &mut emitted_actions) {
                Ok(Some(result)) => output_opt = Some(result),
                Ok(None) => {}
                Err(err) => return err,
            }
        }

        {
            let cancelled_queue_waits = ctx.get_cancelled_queue_ids();
            for schedule_id in cancelled_queue_waits {
                ctx.mark_queue_subscription_cancelled(schedule_id);
            }
        }

        self.convert_emitted_actions(&ctx, emitted_actions);

        if let Err(err) = self.run_quiescence_loop(&ctx, &mut fut, &mut output_opt) {
            return err;
        }

        self.finalize_turn(&ctx, output_opt)
    }

    fn poll_orchestration_future(
        fut: &mut Pin<Box<dyn Future<Output = Result<String, String>> + Send + '_>>,
        ctx: &OrchestrationContext,
        emitted_actions: &mut VecDeque<(u64, Action)>,
    ) -> Result<Option<Result<String, String>>, TurnResult> {
        let poll_result = catch_unwind(AssertUnwindSafe(|| poll_once(fut.as_mut())));
        match poll_result {
            Ok(Poll::Ready(result)) => {
                emitted_actions.extend(ctx.drain_emitted_actions());
                Ok(Some(result))
            }
            Ok(Poll::Pending) => {
                emitted_actions.extend(ctx.drain_emitted_actions());
                Ok(None)
            }
            Err(panic_payload) => {
                let msg = extract_panic_message(panic_payload);
                Err(TurnResult::Failed(crate::ErrorDetails::Application {
                    kind: crate::AppErrorKind::Panicked,
                    message: msg,
                    retryable: false,
                }))
            }
        }
    }

    fn apply_history_event(
        &mut self,
        ctx: &OrchestrationContext,
        event: &Event,
        emitted_actions: &mut VecDeque<(u64, Action)>,
        open_schedules: &mut HashSet<u64>,
        schedule_kinds: &mut std::collections::HashMap<u64, ActionKind>,
        must_poll: &mut bool,
    ) -> Result<(), TurnResult> {
        match &event.kind {
            EventKind::OrchestrationStarted {
                initial_custom_status: Some(status),
                ..
            } => {
                ctx.inner
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .accumulated_custom_status = Some(status.clone());
            }
            EventKind::OrchestrationStarted { .. } => {}
            EventKind::CustomStatusUpdated { status } => {
                ctx.inner
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .accumulated_custom_status = status.clone();

                // Consume the corresponding UpdateCustomStatus action from emitted_actions.
                // During replay the orchestration re-executes set_custom_status/reset_custom_status
                // which emits an UpdateCustomStatus action. We must pop it so subsequent schedule
                // events match correctly.
                if let Some((_, action)) = emitted_actions.pop_front()
                    && !action_matches_event_kind(&action, &event.kind)
                {
                    return Err(TurnResult::Failed(nondeterminism_error(&format!(
                        "custom status mismatch: action={:?} vs event={:?}",
                        action, event.kind
                    ))));
                }
                // If no action was emitted (e.g., first execution seeded initial_custom_status),
                // that is fine — the event came from OrchestrationStarted carry-forward.
            }
            EventKind::ActivityScheduled { name, input: inp, .. } => {
                self.match_and_bind_schedule(
                    ctx,
                    emitted_actions,
                    open_schedules,
                    schedule_kinds,
                    event,
                    ActionKind::Activity {
                        name: name.clone(),
                        input: inp.clone(),
                    },
                )?;
            }
            EventKind::TimerCreated { fire_at_ms } => {
                self.match_and_bind_schedule(
                    ctx,
                    emitted_actions,
                    open_schedules,
                    schedule_kinds,
                    event,
                    ActionKind::Timer {
                        fire_at_ms: *fire_at_ms,
                    },
                )?;
            }
            EventKind::ExternalSubscribed { name } => {
                self.match_and_bind_schedule(
                    ctx,
                    emitted_actions,
                    open_schedules,
                    schedule_kinds,
                    event,
                    ActionKind::External { name: name.clone() },
                )?;
                ctx.inner
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .bind_external_subscription(event.event_id(), name);
            }
            EventKind::QueueSubscribed { name } => {
                self.match_and_bind_schedule(
                    ctx,
                    emitted_actions,
                    open_schedules,
                    schedule_kinds,
                    event,
                    ActionKind::Persistent { name: name.clone() },
                )?;
                ctx.bind_queue_subscription(event.event_id(), name);
                *must_poll = true;
            }
            #[cfg(feature = "replay-version-test")]
            EventKind::ExternalSubscribed2 { name, topic } => {
                self.match_and_bind_schedule(
                    ctx,
                    emitted_actions,
                    open_schedules,
                    schedule_kinds,
                    event,
                    ActionKind::External2 {
                        name: name.clone(),
                        topic: topic.clone(),
                    },
                )?;
                ctx.inner
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .bind_external_subscription2(event.event_id(), name, topic);
            }
            EventKind::SubOrchestrationScheduled {
                name,
                instance,
                input: inp,
                ..
            } => {
                let token = self.match_and_bind_schedule(
                    ctx,
                    emitted_actions,
                    open_schedules,
                    schedule_kinds,
                    event,
                    ActionKind::SubOrch {
                        name: name.clone(),
                        instance: instance.clone(),
                        input: inp.clone(),
                    },
                )?;
                ctx.bind_sub_orchestration_instance(token, instance.clone());
            }
            EventKind::OrchestrationChained {
                name,
                instance,
                input: inp,
            } => {
                let (token, action) = match emitted_actions.pop_front() {
                    Some(a) => a,
                    None => {
                        return Err(TurnResult::Failed(nondeterminism_error(
                            "history OrchestrationChained but no emitted action",
                        )));
                    }
                };
                if !action_matches_event_kind(&action, &event.kind) {
                    return Err(TurnResult::Failed(nondeterminism_error(&format!(
                        "schedule mismatch: action={:?} vs event={:?}",
                        action, event.kind
                    ))));
                }
                ctx.bind_token(token, event.event_id());
                let _ = (name, instance, inp);
            }
            EventKind::ActivityCompleted { result } => {
                if let Some(source_id) = event.source_event_id {
                    if !open_schedules.contains(&source_id) {
                        return Err(TurnResult::Failed(nondeterminism_error(
                            "completion without open schedule",
                        )));
                    }
                    if !matches!(schedule_kinds.get(&source_id), Some(ActionKind::Activity { .. })) {
                        return Err(TurnResult::Failed(nondeterminism_error(
                            "completion kind mismatch: expected activity",
                        )));
                    }
                    ctx.deliver_result(source_id, CompletionResult::ActivityOk(result.clone()));
                    open_schedules.remove(&source_id);
                    *must_poll = true;
                }
            }
            EventKind::ActivityFailed { details } => {
                if let Some(source_id) = event.source_event_id {
                    if !open_schedules.contains(&source_id) {
                        return Err(TurnResult::Failed(nondeterminism_error(
                            "completion without open schedule",
                        )));
                    }
                    ctx.deliver_result(source_id, CompletionResult::ActivityErr(details.display_message()));
                    open_schedules.remove(&source_id);
                    *must_poll = true;
                }
            }
            EventKind::ActivityCancelRequested { .. } => {}
            EventKind::TimerFired { .. } => {
                if let Some(source_id) = event.source_event_id {
                    if !open_schedules.contains(&source_id) {
                        return Err(TurnResult::Failed(nondeterminism_error(
                            "completion without open schedule",
                        )));
                    }
                    ctx.deliver_result(source_id, CompletionResult::TimerFired);
                    open_schedules.remove(&source_id);
                    *must_poll = true;
                }
            }
            EventKind::SubOrchestrationCompleted { result } => {
                if let Some(source_id) = event.source_event_id {
                    if !open_schedules.contains(&source_id) {
                        return Err(TurnResult::Failed(nondeterminism_error(
                            "completion without open schedule",
                        )));
                    }
                    ctx.deliver_result(source_id, CompletionResult::SubOrchOk(result.clone()));
                    open_schedules.remove(&source_id);
                    *must_poll = true;
                }
            }
            EventKind::SubOrchestrationFailed { details } => {
                if let Some(source_id) = event.source_event_id {
                    if !open_schedules.contains(&source_id) {
                        return Err(TurnResult::Failed(nondeterminism_error(
                            "completion without open schedule",
                        )));
                    }
                    ctx.deliver_result(source_id, CompletionResult::SubOrchErr(details.display_message()));
                    open_schedules.remove(&source_id);
                    *must_poll = true;
                }
            }
            EventKind::SubOrchestrationCancelRequested { .. } => {}
            EventKind::ExternalSubscribedCancelled { .. } => {
                if let Some(source_id) = event.source_event_id {
                    ctx.mark_external_subscription_cancelled(source_id);
                }
            }
            EventKind::QueueSubscriptionCancelled { .. } => {
                if let Some(source_id) = event.source_event_id {
                    ctx.mark_queue_subscription_cancelled(source_id);
                }
            }
            EventKind::ExternalEvent { name, data } => {
                let inner = ctx.inner.lock().expect("Mutex should not be poisoned");
                if inner.has_pending_subscription_slot(name) {
                    drop(inner);
                    ctx.inner
                        .lock()
                        .expect("Mutex should not be poisoned")
                        .deliver_external_event(name.clone(), data.clone());
                    *must_poll = true;
                } else {
                    drop(inner);
                    warn!(
                        instance = %self.instance,
                        event_name = %name,
                        "skipping ExternalEvent delivery: no pending subscription slot (event remains in history for audit)"
                    );
                }
            }
            EventKind::QueueEventDelivered { name, data } => {
                ctx.inner
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .deliver_queue_message(name.clone(), data.clone());
                *must_poll = true;
            }
            #[cfg(feature = "replay-version-test")]
            EventKind::ExternalEvent2 { name, topic, data } => {
                ctx.inner
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .deliver_external_event2(name.clone(), topic.clone(), data.clone());
                *must_poll = true;
            }
            EventKind::OrchestrationCancelRequested { .. } => {}
            EventKind::OrchestrationCompleted { .. }
            | EventKind::OrchestrationFailed { .. }
            | EventKind::OrchestrationContinuedAsNew { .. } => {}
        }
        Ok(())
    }

    fn run_quiescence_loop(
        &mut self,
        ctx: &OrchestrationContext,
        fut: &mut Pin<Box<dyn Future<Output = Result<String, String>> + Send + '_>>,
        output_opt: &mut Option<Result<String, String>>,
    ) -> Result<(), TurnResult> {
        const MAX_QUIESCENCE_ITERATIONS: usize = 100;
        let mut quiescence_iter = 0;
        let mut emitted_actions = VecDeque::new();

        while output_opt.is_none() && !self.pending_actions.is_empty() && quiescence_iter < MAX_QUIESCENCE_ITERATIONS {
            quiescence_iter += 1;
            let actions_before = self.pending_actions.len();

            if let Some(res) = Self::poll_orchestration_future(fut, ctx, &mut emitted_actions)? {
                *output_opt = Some(res);
            }
            self.convert_emitted_actions(ctx, emitted_actions.drain(..));

            if self.pending_actions.len() == actions_before {
                break;
            }
        }

        if quiescence_iter >= MAX_QUIESCENCE_ITERATIONS {
            return Err(TurnResult::Failed(crate::ErrorDetails::Infrastructure {
                operation: "quiescence_loop".to_string(),
                message: format!(
                    "quiescence loop exceeded {MAX_QUIESCENCE_ITERATIONS} iterations —                      orchestration is emitting unbounded persistent event subscriptions"
                ),
                retryable: false,
            }));
        }

        Ok(())
    }

    fn finalize_turn(&mut self, ctx: &OrchestrationContext, output_opt: Option<Result<String, String>>) -> TurnResult {
        let cancel_event = self
            .baseline_history
            .iter()
            .chain(self.history_delta.iter())
            .find(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. }));

        if let Some(EventKind::OrchestrationCancelRequested { reason }) = cancel_event.map(|e| &e.kind) {
            return TurnResult::Cancelled(reason.clone());
        }

        for decision in &self.pending_actions {
            if let crate::Action::ContinueAsNew { input, version } = decision {
                return TurnResult::ContinueAsNew {
                    input: input.clone(),
                    version: version.clone(),
                };
            }
        }

        if let Some(output) = output_opt {
            if let Err(r) = self.collect_cancelled_from_context(ctx) {
                return r;
            }

            return match output {
                Ok(result) => TurnResult::Completed(result),
                Err(error) => TurnResult::Failed(crate::ErrorDetails::Application {
                    kind: crate::AppErrorKind::OrchestrationFailed,
                    message: error,
                    retryable: false,
                }),
            };
        }

        if let Err(r) = self.collect_cancelled_from_context(ctx) {
            return r;
        }

        TurnResult::Continue
    }
    /// Convert emitted actions into `pending_actions` and `history_delta` entries.
    ///
    /// For each `(token, action)` pair:
    /// 1. Assigns a deterministic `event_id`
    /// 2. Binds the token → event_id on the context
    /// 3. Binds sub-orchestration instance mappings (for cancellation routing)
    /// 4. Binds external/persistent subscription indexes (for same-turn FIFO matching)
    /// 5. Converts the action to a history event and appends to `history_delta`
    /// 6. Pushes the action to `pending_actions` for the dispatcher
    fn convert_emitted_actions(
        &mut self,
        ctx: &OrchestrationContext,
        actions: impl IntoIterator<Item = (u64, Action)>,
    ) -> Vec<Event> {
        let mut synthetic_events = Vec::new();
        for (token, action) in actions {
            let event_id = self.next_event_id;
            self.next_event_id += 1;

            ctx.bind_token(token, event_id);

            let updated_action = update_action_event_id(action, event_id);

            if let crate::Action::StartSubOrchestration { instance, .. } = &updated_action {
                ctx.bind_sub_orchestration_instance(token, instance.clone());
            }

            // Bind subscriptions so polling can resolve in the same turn
            // (e.g., persistent event arrived before subscription was created)
            match &updated_action {
                crate::Action::WaitExternal { name, .. } => {
                    ctx.bind_external_subscription(event_id, name);
                }
                crate::Action::DequeueEvent { name, .. } => {
                    ctx.bind_queue_subscription(event_id, name);
                }
                _ => {}
            }

            if let Some(event) = action_to_event(&updated_action, &self.instance, self.execution_id, event_id) {
                synthetic_events.push(event.clone());
                self.history_delta.push(event);
            }

            self.pending_actions.push(updated_action);
        }
        synthetic_events
    }

    /// Collect dropped-future cancellation decisions from the `OrchestrationContext` and reconcile
    /// them against persisted history.
    ///
    /// This function exists because *dropped* durable futures are a real, durable side-effect in
    /// Duroxide:
    ///
    /// - `ctx.schedule_activity(...)` / `ctx.schedule_sub_orchestration(...)` immediately emit a
    ///   schedule action (durable).
    /// - If the returned future is later **dropped** (e.g. it loses a `select2`), we must request
    ///   cancellation of that already-scheduled work.
    ///
    /// There are two outputs:
    ///
    /// 1) **Provider side-channel** cancellation signals (used to actually cancel work):
    ///    - `self.cancelled_activity_ids` (lock-stealing activity cancellation)
    ///    - `self.cancelled_sub_orchestration_ids` (enqueue `CancelInstance`)
    ///
    /// 2) **History breadcrumbs** for replay determinism + observability:
    ///    - `EventKind::ActivityCancelRequested { reason: "dropped_future" }`
    ///    - `EventKind::SubOrchestrationCancelRequested { reason: "dropped_future" }`
    ///
    /// The core problem: when replaying, the runtime must ensure the orchestration makes the
    /// *same* cancellation decisions for schedules that are already in persisted history.
    ///
    /// ---------------------------------------------------------------------------
    /// Algorithm (high level)
    /// ---------------------------------------------------------------------------
    ///
    /// We compute three pieces of information:
    ///
    /// - **Persisted schedules**: schedule IDs that exist in the replayed (persisted) segment.
    /// - **Persisted dropped-future cancel-requests**: the authoritative record of previously
    ///   decided dropped-future cancellations.
    /// - **Context cancellations**: what the current *code* dropped this turn.
    ///
    /// Then we compare the persisted cancel-requests against the code’s cancellations, but only
    /// for schedule IDs that are in the replayed segment.
    ///
    /// Pseudocode of the reconciliation:
    ///
    /// ```text
    /// // 1) Authoritative record from persisted history
    /// let baseline_cancelled = { source_event_id of *CancelRequested(reason="dropped_future") };
    ///
    /// // 2) Which schedule IDs are actually part of the replayed segment
    /// let baseline_schedules = { event_id of *Scheduled events in persisted history };
    ///
    /// // 3) What this run’s code dropped (can include new schedules created this turn)
    /// let ctx_cancelled_all = ctx.get_cancelled_*();
    ///
    /// // 4) Restrict to the replayed segment to avoid false positives
    /// let ctx_cancelled_in_replayed_segment = ctx_cancelled_all ∩ baseline_schedules;
    ///
    /// // 5) If we already have a persisted record of dropped-future cancellations, enforce it
    /// if baseline_cancelled is non-empty {
    ///     assert_eq!(baseline_cancelled, ctx_cancelled_in_replayed_segment);
    /// }
    /// ```
    ///
    /// ---------------------------------------------------------------------------
    /// Why do we intersect with persisted schedules?
    /// ---------------------------------------------------------------------------
    ///
    /// `ctx.get_cancelled_activity_ids()` / `ctx.get_cancelled_sub_orchestration_cancellations()`
    /// report **everything dropped by code in this turn**, including:
    ///
    /// - Drops of futures for schedules created in the replayed (persisted) segment
    /// - Drops of futures for schedules created **this turn** (not yet persisted)
    ///
    /// Only the first category is meaningful for replay determinism checks.
    ///
    /// Without the intersection, we would get false nondeterminism failures whenever replay is
    /// happening *and* the current turn also schedules & drops a new future.
    ///
    /// ---------------------------------------------------------------------------
    /// Why is the enforcement gated on “baseline cancel-requests is non-empty”?
    /// ---------------------------------------------------------------------------
    ///
    /// The first turn that *introduces* a `dropped_future` cancellation request cannot possibly
    /// have that event in persisted history yet.
    ///
    /// Example:
    ///
    /// ```text
    /// // Turn 1 persisted:
    /// //   ActivityScheduled(id=2)
    /// //   TimerCreated(id=3)
    /// // Turn 2 (current): TimerFired arrives, code does select2(timer wins) and drops activity
    /// //   -> we need to emit ActivityCancelRequested(id=2, reason="dropped_future") now
    /// ```
    ///
    /// On that “introduction” turn:
    /// - `baseline_dropped_future_cancel_requests_*` is empty (not persisted yet)
    /// - `ctx_cancelled_*_in_replayed_segment` contains the dropped schedule
    ///
    /// If we enforced equality unconditionally, we’d incorrectly treat every normal first-time
    /// dropped-future cancellation as nondeterminism.
    ///
    /// Once the cancel-request breadcrumb is persisted, subsequent replays can enforce it.
    ///
    /// ---------------------------------------------------------------------------
    /// What kinds of issues does this catch?
    /// ---------------------------------------------------------------------------
    ///
    /// (A) **Removed drop** (code no longer drops a future that history says was dropped)
    ///
    /// - Persisted history has `*CancelRequested(reason="dropped_future", source_event_id=X)`
    /// - Current code no longer drops that future during replay
    /// - Result: set mismatch → nondeterminism
    ///
    /// This is the most common “why did this suddenly start failing?” scenario during refactors.
    ///
    /// (B) **Added drop** for a schedule in the replayed segment
    ///
    /// - If the baseline already contains at least one persisted dropped-future cancel-request
    ///   (anywhere), the enforcement gate is “on” and extra cancellations in the replayed segment
    ///   will be detected as nondeterminism.
    /// - If the baseline contains *zero* dropped-future cancel-requests, adding a drop is treated
    ///   as an “introduction” turn and will not be rejected until the breadcrumb is persisted.
    ///
    /// ---------------------------------------------------------------------------
    /// What kinds of issues does this NOT catch?
    /// ---------------------------------------------------------------------------
    ///
    /// (1) **Timing/order changes** for a drop within the same turn
    ///
    /// The check is set-based (membership), not order-based. If you move a `drop(fut)` to happen
    /// later (e.g., after awaiting something else) but it still happens by the end of the turn,
    /// the same schedule ID will appear in `ctx_cancelled_*` and the sets will still match.
    ///
    /// (2) **Non-"dropped_future" cancellation reasons**
    ///
    /// We only reconcile the `reason == "dropped_future"` stream here. Terminal cleanup
    /// cancellations (e.g. orchestration failed/completed/continued-as-new) are produced outside
    /// the replay engine and may legitimately vary in when/how they’re emitted.
    ///
    /// ---------------------------------------------------------------------------
    /// Side-channel emission policy (important for debugging “why didn’t it cancel again?”)
    /// ---------------------------------------------------------------------------
    ///
    /// We only emit provider side-channel cancellations when we also record a NEW cancellation
    /// request event in history_delta. If the cancellation-request breadcrumb already exists in
    /// persisted history, replay drops do NOT re-send the side-channel every turn.
    ///
    /// This prevents redundant provider operations and makes replay idempotent.
    fn collect_cancelled_from_context(&mut self, ctx: &OrchestrationContext) -> Result<(), TurnResult> {
        // Collect cancellation decisions from the context.
        //
        // NOTE: We only emit provider side-channel cancellations when we also record a NEW
        // cancellation-request event this turn. If the cancellation-request is already present
        // in persisted history, re-sending the side-channel on every replay turn is redundant.
        let cancelled_activities = ctx.get_cancelled_activity_ids();
        let cancelled_sub_orchs = ctx.get_cancelled_sub_orchestration_cancellations();
        let cancelled_external_waits = ctx.get_cancelled_external_wait_ids();
        let cancelled_queue_waits = ctx.get_cancelled_queue_ids();

        // If we're replaying, enforce that dropped-future cancellation decisions match persisted history.
        //
        // We only validate the "dropped_future" reason stream, since other cancellation-request events
        // (e.g., terminal cleanup) may be emitted outside the replay engine.
        if self.persisted_history_len > 0 {
            let baseline_dropped_future_cancel_requests_activity: HashSet<u64> = self.baseline_history
                [..self.persisted_history_len]
                .iter()
                .filter_map(|e| {
                    if let EventKind::ActivityCancelRequested { reason } = &e.kind
                        && reason == "dropped_future"
                    {
                        e.source_event_id
                    } else {
                        None
                    }
                })
                .collect();

            let baseline_dropped_future_cancel_requests_sub_orch: HashSet<u64> = self.baseline_history
                [..self.persisted_history_len]
                .iter()
                .filter_map(|e| {
                    if let EventKind::SubOrchestrationCancelRequested { reason } = &e.kind
                        && reason == "dropped_future"
                    {
                        e.source_event_id
                    } else {
                        None
                    }
                })
                .collect();

            let baseline_dropped_future_cancel_requests_external: HashSet<u64> = self.baseline_history
                [..self.persisted_history_len]
                .iter()
                .filter_map(|e| {
                    if let EventKind::ExternalSubscribedCancelled { reason } = &e.kind
                        && reason == "dropped_future"
                    {
                        e.source_event_id
                    } else {
                        None
                    }
                })
                .collect();

            let baseline_dropped_future_cancel_requests_queue: HashSet<u64> = self.baseline_history
                [..self.persisted_history_len]
                .iter()
                .filter_map(|e| {
                    if let EventKind::QueueSubscriptionCancelled { reason } = &e.kind
                        && reason == "dropped_future"
                    {
                        e.source_event_id
                    } else {
                        None
                    }
                })
                .collect();

            // Only compare cancellation decisions for schedules that are part of the persisted
            // history segment. This avoids false positives when the current turn produces NEW
            // cancellation decisions for schedules created this turn (not yet persisted).
            let baseline_persisted_activity_schedules: HashSet<u64> = self.baseline_history
                [..self.persisted_history_len]
                .iter()
                .filter_map(|e| match &e.kind {
                    EventKind::ActivityScheduled { .. } => Some(e.event_id()),
                    _ => None,
                })
                .collect();

            let baseline_persisted_sub_orch_schedules: HashSet<u64> = self.baseline_history
                [..self.persisted_history_len]
                .iter()
                .filter_map(|e| match &e.kind {
                    EventKind::SubOrchestrationScheduled { .. } => Some(e.event_id()),
                    _ => None,
                })
                .collect();

            let baseline_persisted_external_schedules: HashSet<u64> = self.baseline_history
                [..self.persisted_history_len]
                .iter()
                .filter_map(|e| match &e.kind {
                    EventKind::ExternalSubscribed { .. } => Some(e.event_id()),
                    _ => None,
                })
                .collect();

            let baseline_persisted_queue_schedules: HashSet<u64> = self.baseline_history[..self.persisted_history_len]
                .iter()
                .filter_map(|e| match &e.kind {
                    EventKind::QueueSubscribed { .. } => Some(e.event_id()),
                    _ => None,
                })
                .collect();

            let ctx_cancelled_activity_schedules_all: HashSet<u64> = cancelled_activities.iter().copied().collect();
            let ctx_cancelled_sub_orch_schedules_all: HashSet<u64> =
                cancelled_sub_orchs.iter().map(|(id, _)| *id).collect();
            let ctx_cancelled_external_schedules_all: HashSet<u64> = cancelled_external_waits.iter().copied().collect();
            let ctx_cancelled_queue_schedules_all: HashSet<u64> = cancelled_queue_waits.iter().copied().collect();

            // Only compare cancellation decisions for schedules that are part of the persisted
            // history segment. This avoids false positives when the current turn produces NEW
            // cancellation decisions for schedules created this turn (not yet persisted).
            let ctx_cancelled_activity_schedules_in_replayed_segment: HashSet<u64> =
                ctx_cancelled_activity_schedules_all
                    .intersection(&baseline_persisted_activity_schedules)
                    .copied()
                    .collect();
            let ctx_cancelled_sub_orch_schedules_in_replayed_segment: HashSet<u64> =
                ctx_cancelled_sub_orch_schedules_all
                    .intersection(&baseline_persisted_sub_orch_schedules)
                    .copied()
                    .collect();
            let ctx_cancelled_external_schedules_in_replayed_segment: HashSet<u64> =
                ctx_cancelled_external_schedules_all
                    .intersection(&baseline_persisted_external_schedules)
                    .copied()
                    .collect();
            let ctx_cancelled_queue_schedules_in_replayed_segment: HashSet<u64> = ctx_cancelled_queue_schedules_all
                .intersection(&baseline_persisted_queue_schedules)
                .copied()
                .collect();

            // Per-type subset enforcement: for each schedule type independently, verify that
            // every persisted cancellation breadcrumb is still reproduced on replay.
            //
            // Uses subset (⊆) instead of equality (==) to allow new cancellation decisions
            // to accumulate across turns. A schedule created in turn N may be dropped for the
            // first time in turn N+2 — the new cancellation is legitimate, not nondeterminism.
            //
            // Each type is gated independently to avoid cross-type pollution: a persisted
            // external-wait cancellation does not activate enforcement for activities.
            if !baseline_dropped_future_cancel_requests_activity.is_empty()
                && !baseline_dropped_future_cancel_requests_activity
                    .is_subset(&ctx_cancelled_activity_schedules_in_replayed_segment)
            {
                return Err(TurnResult::Failed(nondeterminism_error(&format!(
                    "cancellation mismatch (activities): baseline_dropped_future_cancel_requests={baseline_dropped_future_cancel_requests_activity:?} ctx_cancelled_in_replayed_segment={ctx_cancelled_activity_schedules_in_replayed_segment:?}"
                ))));
            }

            if !baseline_dropped_future_cancel_requests_sub_orch.is_empty()
                && !baseline_dropped_future_cancel_requests_sub_orch
                    .is_subset(&ctx_cancelled_sub_orch_schedules_in_replayed_segment)
            {
                return Err(TurnResult::Failed(nondeterminism_error(&format!(
                    "cancellation mismatch (sub-orchestrations): baseline_dropped_future_cancel_requests={baseline_dropped_future_cancel_requests_sub_orch:?} ctx_cancelled_in_replayed_segment={ctx_cancelled_sub_orch_schedules_in_replayed_segment:?}"
                ))));
            }

            if !baseline_dropped_future_cancel_requests_external.is_empty()
                && !baseline_dropped_future_cancel_requests_external
                    .is_subset(&ctx_cancelled_external_schedules_in_replayed_segment)
            {
                return Err(TurnResult::Failed(nondeterminism_error(&format!(
                    "cancellation mismatch (external waits): baseline_dropped_future_cancel_requests={baseline_dropped_future_cancel_requests_external:?} ctx_cancelled_in_replayed_segment={ctx_cancelled_external_schedules_in_replayed_segment:?}"
                ))));
            }

            if !baseline_dropped_future_cancel_requests_queue.is_empty()
                && !baseline_dropped_future_cancel_requests_queue
                    .is_subset(&ctx_cancelled_queue_schedules_in_replayed_segment)
            {
                return Err(TurnResult::Failed(nondeterminism_error(&format!(
                    "cancellation mismatch (persistent waits): baseline_dropped_future_cancel_requests={baseline_dropped_future_cancel_requests_queue:?} ctx_cancelled_in_replayed_segment={ctx_cancelled_queue_schedules_in_replayed_segment:?}"
                ))));
            }
        }

        // Emit history events recording cancellation decisions (requested-only), and emit the
        // provider side-channel ONLY when the cancellation-request is new.
        //
        // These are best-effort: a completion may still arrive after a cancellation request.
        // We avoid emitting duplicates if a cancellation request already exists.
        let mut already_cancelled_activity: HashSet<u64> = HashSet::new();
        let mut already_cancelled_sub_orch: HashSet<u64> = HashSet::new();
        let mut already_cancelled_external: HashSet<u64> = HashSet::new();
        for e in self.baseline_history.iter().chain(self.history_delta.iter()) {
            match &e.kind {
                EventKind::ActivityCancelRequested { .. } => {
                    if let Some(src) = e.source_event_id {
                        already_cancelled_activity.insert(src);
                    }
                }
                EventKind::SubOrchestrationCancelRequested { .. } => {
                    if let Some(src) = e.source_event_id {
                        already_cancelled_sub_orch.insert(src);
                    }
                }
                EventKind::ExternalSubscribedCancelled { .. } => {
                    if let Some(src) = e.source_event_id {
                        already_cancelled_external.insert(src);
                    }
                }
                _ => {}
            }
        }

        for schedule_id in cancelled_activities {
            if already_cancelled_activity.insert(schedule_id) {
                // Side-channel: provider lock-stealing (only once per cancellation-request).
                self.cancelled_activity_ids.push(schedule_id);

                // History breadcrumb: replay determinism + audit trail.
                let event_id = self.next_event_id;
                self.next_event_id += 1;
                self.history_delta.push(Event::with_event_id(
                    event_id,
                    self.instance.clone(),
                    self.execution_id,
                    Some(schedule_id),
                    EventKind::ActivityCancelRequested {
                        reason: "dropped_future".to_string(),
                    },
                ));
            }
        }

        for (schedule_id, child_instance_id) in cancelled_sub_orchs {
            if already_cancelled_sub_orch.insert(schedule_id) {
                // Side-channel: enqueue CancelInstance (only once per cancellation-request).
                self.cancelled_sub_orchestration_ids.push(child_instance_id);

                // History breadcrumb: replay determinism + audit trail.
                let event_id = self.next_event_id;
                self.next_event_id += 1;
                self.history_delta.push(Event::with_event_id(
                    event_id,
                    self.instance.clone(),
                    self.execution_id,
                    Some(schedule_id),
                    EventKind::SubOrchestrationCancelRequested {
                        reason: "dropped_future".to_string(),
                    },
                ));
            }
        }

        for schedule_id in cancelled_external_waits {
            if already_cancelled_external.insert(schedule_id) {
                // No provider side-channel needed (external waits are virtual constructs).

                // History breadcrumb: replay determinism + audit trail.
                let event_id = self.next_event_id;
                self.next_event_id += 1;
                self.history_delta.push(Event::with_event_id(
                    event_id,
                    self.instance.clone(),
                    self.execution_id,
                    Some(schedule_id),
                    EventKind::ExternalSubscribedCancelled {
                        reason: "dropped_future".to_string(),
                    },
                ));
            }
        }

        // Handle persistent wait cancellations (also virtual, no provider side-channel).
        // Persistent subscriptions that are cancelled are simply skipped during FIFO matching,
        // and the history breadcrumb ensures CAN carry-forward correctly excludes them.
        let mut already_cancelled_persistent: HashSet<u64> = HashSet::new();
        for e in self.baseline_history.iter().chain(self.history_delta.iter()) {
            if let (EventKind::QueueSubscriptionCancelled { .. }, Some(src)) = (&e.kind, e.source_event_id) {
                already_cancelled_persistent.insert(src);
            }
        }
        let cancelled_queue_waits = ctx.get_cancelled_queue_ids();
        for schedule_id in cancelled_queue_waits {
            if already_cancelled_persistent.insert(schedule_id) {
                // History breadcrumb: ensures CAN carry-forward can reconstruct
                // which persistent subscriptions were cancelled from history alone.
                let event_id = self.next_event_id;
                self.next_event_id += 1;
                self.history_delta.push(Event::with_event_id(
                    event_id,
                    self.instance.clone(),
                    self.execution_id,
                    Some(schedule_id),
                    EventKind::QueueSubscriptionCancelled {
                        reason: "dropped_future".to_string(),
                    },
                ));
            }
        }

        // Clear the context's cancelled tokens (avoid re-processing if called again)
        ctx.clear_cancelled_tokens();

        Ok(())
    }

    /// Helper to match and bind schedule events.
    /// Returns `Ok(token)` on success, `Err(TurnResult)` on failure.
    fn match_and_bind_schedule(
        &self,
        ctx: &OrchestrationContext,
        emitted_actions: &mut VecDeque<(u64, Action)>,
        open_schedules: &mut HashSet<u64>,
        schedule_kinds: &mut std::collections::HashMap<u64, ActionKind>,
        event: &Event,
        expected_kind: ActionKind,
    ) -> Result<u64, TurnResult> {
        let (token, action) = match emitted_actions.pop_front() {
            Some(a) => a,
            None => {
                return Err(TurnResult::Failed(nondeterminism_error(
                    "history schedule but no emitted action",
                )));
            }
        };

        // Validate action matches event
        if !action_matches_event_kind(&action, &event.kind) {
            return Err(TurnResult::Failed(nondeterminism_error(&format!(
                "schedule mismatch: action={:?} vs event={:?}",
                action, event.kind
            ))));
        }

        // Bind token to schedule_id
        ctx.bind_token(token, event.event_id());
        open_schedules.insert(event.event_id());
        schedule_kinds.insert(event.event_id(), expected_kind);

        Ok(token)
    }
}

impl ReplayEngine {
    // Getter methods for atomic execution
    pub fn history_delta(&self) -> &[Event] {
        &self.history_delta
    }

    pub fn pending_actions(&self) -> &[crate::Action] {
        &self.pending_actions
    }

    pub fn cancelled_activity_ids(&self) -> &[u64] {
        &self.cancelled_activity_ids
    }

    pub fn cancelled_sub_orchestration_ids(&self) -> &[String] {
        &self.cancelled_sub_orchestration_ids
    }

    /// Check if this run made any progress (added history)
    pub fn made_progress(&self) -> bool {
        !self.history_delta.is_empty()
    }

    /// Get the final history after this run
    pub fn final_history(&self) -> Vec<Event> {
        let mut final_hist = self.baseline_history.clone();
        final_hist.extend_from_slice(&self.history_delta);
        final_hist
    }
}

// === Helper types and functions ===

/// Action kind for tracking schedule types
/// NOTE: Fields are intentionally unused - we only match on variant type, not field values
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ActionKind {
    Activity {
        name: String,
        input: String,
    },
    Timer {
        fire_at_ms: u64,
    },
    External {
        name: String,
    },
    Persistent {
        name: String,
    },
    #[cfg(feature = "replay-version-test")]
    External2 {
        name: String,
        topic: String,
    },
    SubOrch {
        name: String,
        instance: String,
        input: String,
    },
}

/// Create a nondeterminism error
fn nondeterminism_error(msg: &str) -> crate::ErrorDetails {
    crate::ErrorDetails::Configuration {
        kind: crate::ConfigErrorKind::Nondeterminism,
        resource: String::new(),
        message: Some(msg.to_string()),
    }
}

/// Extract panic message from payload
fn extract_panic_message(panic_payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic_payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "orchestration panicked".to_string()
    }
}

/// Convert an Action to an Event for history persistence
fn action_to_event(action: &Action, instance: &str, execution_id: u64, event_id: u64) -> Option<Event> {
    let kind = match action {
        Action::CallActivity {
            name,
            input,
            session_id,
            tag,
            ..
        } => EventKind::ActivityScheduled {
            name: name.clone(),
            input: input.clone(),
            session_id: session_id.clone(),
            tag: tag.clone(),
        },
        Action::CreateTimer { fire_at_ms, .. } => EventKind::TimerCreated {
            fire_at_ms: *fire_at_ms,
        },
        Action::WaitExternal { name, .. } => EventKind::ExternalSubscribed { name: name.clone() },
        Action::DequeueEvent { name, .. } => EventKind::QueueSubscribed { name: name.clone() },
        #[cfg(feature = "replay-version-test")]
        Action::WaitExternal2 { name, topic, .. } => EventKind::ExternalSubscribed2 {
            name: name.clone(),
            topic: topic.clone(),
        },
        Action::StartSubOrchestration {
            name,
            instance: sub_instance,
            input,
            ..
        } => EventKind::SubOrchestrationScheduled {
            name: name.clone(),
            instance: sub_instance.clone(),
            input: input.clone(),
        },
        Action::StartOrchestrationDetached {
            name, instance, input, ..
        } => EventKind::OrchestrationChained {
            name: name.clone(),
            instance: instance.clone(),
            input: input.clone(),
        },
        // ContinueAsNew doesn't become a schedule event - it has its own terminal event
        Action::ContinueAsNew { .. } => {
            return None;
        }
        // UpdateCustomStatus becomes a CustomStatusUpdated history event
        Action::UpdateCustomStatus { status } => EventKind::CustomStatusUpdated { status: status.clone() },
    };

    Some(Event::with_event_id(event_id, instance, execution_id, None, kind))
}

/// Update an action's scheduling_event_id to the correct event_id.
/// Also generates the actual sub-orchestration instance ID from the event_id
/// (unless an explicit instance ID was provided, indicated by not starting with SUB_ORCH_PENDING_PREFIX).
fn update_action_event_id(action: Action, event_id: u64) -> Action {
    match action {
        Action::CallActivity {
            name,
            input,
            session_id,
            tag,
            ..
        } => Action::CallActivity {
            scheduling_event_id: event_id,
            name,
            input,
            session_id,
            tag,
        },
        Action::CreateTimer { fire_at_ms, .. } => Action::CreateTimer {
            scheduling_event_id: event_id,
            fire_at_ms,
        },
        Action::WaitExternal { name, .. } => Action::WaitExternal {
            scheduling_event_id: event_id,
            name,
        },
        Action::DequeueEvent { name, .. } => Action::DequeueEvent {
            scheduling_event_id: event_id,
            name,
        },
        #[cfg(feature = "replay-version-test")]
        Action::WaitExternal2 { name, topic, .. } => Action::WaitExternal2 {
            scheduling_event_id: event_id,
            name,
            topic,
        },
        Action::StartSubOrchestration {
            name,
            instance,
            input,
            version,
            ..
        } => {
            // If instance starts with the pending prefix, it's a placeholder that needs to be replaced.
            // Otherwise, it's an explicit instance ID provided by the user.
            let final_instance = if instance.starts_with(crate::SUB_ORCH_PENDING_PREFIX) {
                format!("{}{event_id}", crate::SUB_ORCH_AUTO_PREFIX)
            } else {
                instance
            };
            Action::StartSubOrchestration {
                scheduling_event_id: event_id,
                name,
                instance: final_instance,
                input,
                version,
            }
        }
        Action::StartOrchestrationDetached {
            name,
            version,
            instance,
            input,
            ..
        } => Action::StartOrchestrationDetached {
            scheduling_event_id: event_id,
            name,
            version,
            instance,
            input,
        },
        // ContinueAsNew doesn't have scheduling_event_id
        Action::ContinueAsNew { .. } => action,
        // UpdateCustomStatus doesn't have scheduling_event_id
        Action::UpdateCustomStatus { .. } => action,
    }
}

/// Poll a future once
fn poll_once<F: Future + ?Sized>(fut: Pin<&mut F>) -> Poll<F::Output> {
    // Create a no-op waker
    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(|_| RawWaker::new(std::ptr::null(), &VTABLE), |_| {}, |_| {}, |_| {});
    let raw_waker = RawWaker::new(std::ptr::null(), &VTABLE);
    let waker = unsafe { Waker::from_raw(raw_waker) };
    let mut cx = Context::from_waker(&waker);
    fut.poll(&mut cx)
}

/// Check if an action matches an event kind
fn action_matches_event_kind(action: &Action, event_kind: &EventKind) -> bool {
    match (action, event_kind) {
        (
            Action::CallActivity {
                name,
                input,
                session_id,
                tag,
                ..
            },
            EventKind::ActivityScheduled {
                name: en,
                input: ei,
                session_id: es,
                tag: et,
            },
        ) => name == en && input == ei && session_id == es && tag == et,

        (Action::CreateTimer { fire_at_ms, .. }, EventKind::TimerCreated { fire_at_ms: ef }) => {
            // Allow some tolerance for timer fire_at_ms since it's computed at different times
            // In practice, the replay should use the exact value from history
            let _ = fire_at_ms;
            let _ = ef;
            true // Timers match by position, not by exact fire_at_ms
        }

        (Action::WaitExternal { name, .. }, EventKind::ExternalSubscribed { name: en }) => name == en,

        (Action::DequeueEvent { name, .. }, EventKind::QueueSubscribed { name: en }) => name == en,

        #[cfg(feature = "replay-version-test")]
        (Action::WaitExternal2 { name, topic, .. }, EventKind::ExternalSubscribed2 { name: en, topic: et }) => {
            name == en && topic == et
        }

        (
            Action::StartSubOrchestration { name, input, .. },
            EventKind::SubOrchestrationScheduled {
                name: en, input: ei, ..
            },
        ) => name == en && input == ei,

        (
            Action::StartOrchestrationDetached {
                name, instance, input, ..
            },
            EventKind::OrchestrationChained {
                name: en,
                instance: ei,
                input: inp,
            },
        ) => name == en && instance == ei && input == inp,

        // UpdateCustomStatus matches CustomStatusUpdated by kind only (value may differ across code versions)
        (Action::UpdateCustomStatus { .. }, EventKind::CustomStatusUpdated { .. }) => true,

        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Event, EventKind};

    #[test]
    fn test_engine_creation() {
        let engine = ReplayEngine::new(
            "test-instance".to_string(),
            1, // execution_id
            vec![Event::with_event_id(
                0,
                "test-instance",
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "test-orch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "test-input".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
        );

        assert_eq!(engine.instance, "test-instance");
        assert!(engine.history_delta.is_empty());
        assert!(!engine.made_progress());
    }
}

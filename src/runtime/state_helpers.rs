// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::{
    Event, EventKind,
    providers::{ScheduledActivityIdentifier, WorkItem},
};
use tracing::warn;

/// Reader for extracting metadata from orchestration history
///
/// This struct provides convenient access to key information derived from
/// the event history without needing to repeatedly scan through events.
#[derive(Debug, Clone)]
pub struct HistoryManager {
    /// Orchestration name (from OrchestrationStarted)
    pub orchestration_name: Option<String>,

    /// Orchestration version (from OrchestrationStarted)
    pub orchestration_version: Option<String>,

    /// Original input (from OrchestrationStarted)
    pub orchestration_input: Option<String>,

    /// Parent instance if this is a sub-orchestration
    pub parent_instance: Option<String>,

    /// Parent event ID if this is a sub-orchestration
    pub parent_id: Option<u64>,

    /// Whether the orchestration has completed successfully
    pub is_completed: bool,

    /// Whether the orchestration has failed
    pub is_failed: bool,

    /// Whether the orchestration has continued as new
    pub is_continued_as_new: bool,

    /// The execution ID from the most recent OrchestrationStarted
    pub current_execution_id: Option<u64>,

    /// The complete history being managed
    history: Vec<Event>,

    /// New events to be appended (history delta)
    delta: Vec<Event>,
}

impl HistoryManager {
    /// Extract metadata from orchestration history
    ///
    /// Scans through the history (in reverse for terminal states) to extract
    /// commonly needed information.
    pub fn from_history(history: &[Event]) -> Self {
        let mut metadata = Self {
            orchestration_name: None,
            orchestration_version: None,
            orchestration_input: None,
            parent_instance: None,
            parent_id: None,
            is_completed: false,
            is_failed: false,
            is_continued_as_new: false,
            current_execution_id: None,
            history: history.to_vec(),
            delta: Vec::new(),
        };

        // Scan forward for OrchestrationStarted (could be multiple due to CAN)
        // We want the most recent one for current execution
        // Note: execution_id is derived from counting OrchestrationStarted events, not stored in the event
        let mut execution_id_counter = 0u64;
        let mut last_started_index = None;
        for (idx, event) in history.iter().enumerate() {
            if let EventKind::OrchestrationStarted {
                name,
                version,
                input,
                parent_instance,
                parent_id,
                ..
            } = &event.kind
            {
                execution_id_counter += 1;
                metadata.orchestration_name = Some(name.clone());
                metadata.orchestration_version = Some(version.clone());
                metadata.orchestration_input = Some(input.clone());
                metadata.parent_instance = parent_instance.clone();
                metadata.parent_id = *parent_id;
                metadata.current_execution_id = Some(execution_id_counter);
                last_started_index = Some(idx);
                // Don't break - we want the LAST (most recent) OrchestrationStarted
            }
        }

        // Check for terminal states AFTER the most recent OrchestrationStarted
        if let Some(start_idx) = last_started_index {
            for event in history[(start_idx + 1)..].iter() {
                match &event.kind {
                    EventKind::OrchestrationCompleted { .. } => {
                        metadata.is_completed = true;
                        break;
                    }
                    EventKind::OrchestrationFailed { .. } => {
                        metadata.is_failed = true;
                        break;
                    }
                    EventKind::OrchestrationContinuedAsNew { .. } => {
                        metadata.is_continued_as_new = true;
                        break;
                    }
                    _ => {}
                }
            }
        }

        metadata
    }

    /// Check if the orchestration is in a terminal state
    pub fn is_terminal(&self) -> bool {
        self.is_completed || self.is_failed || self.is_continued_as_new
    }

    /// Check if the history is empty (new instance with no events yet)
    pub fn is_empty(&self) -> bool {
        self.history.is_empty() && self.delta.is_empty()
    }

    /// Get the length of the original (persisted) history from the database.
    /// This does NOT include events added to delta this turn.
    /// Used by simplified replay mode to correctly track is_replaying state.
    pub fn original_len(&self) -> usize {
        self.history.len()
    }

    /// Get a human-readable status string
    pub fn status(&self) -> &'static str {
        if self.is_completed {
            "Completed"
        } else if self.is_failed {
            "Failed"
        } else if self.is_continued_as_new {
            "ContinuedAsNew"
        } else {
            "Running"
        }
    }

    // === Mutation methods for building history delta ===

    /// Calculate the next event ID based on existing history and delta
    pub fn next_event_id(&self) -> u64 {
        self.history
            .iter()
            .chain(self.delta.iter())
            .map(|e| e.event_id())
            .max()
            .unwrap_or(0)
            + 1
    }

    /// Append a single event to the delta
    pub fn append(&mut self, event: Event) {
        self.delta.push(event);
    }

    /// Append an OrchestrationFailed event with the next event_id
    pub fn append_failed(&mut self, instance_id: &str, execution_id: u64, details: crate::ErrorDetails) {
        let next_id = self.next_event_id();
        self.append(Event::with_event_id(
            next_id,
            instance_id,
            execution_id,
            None,
            EventKind::OrchestrationFailed { details },
        ));
    }

    /// Extend delta with multiple events
    pub fn extend(&mut self, events: Vec<Event>) {
        self.delta.extend(events);
    }

    /// Get a reference to the history delta
    pub fn delta(&self) -> &[Event] {
        &self.delta
    }

    /// Consume the manager and return the history delta
    pub fn into_delta(self) -> Vec<Event> {
        self.delta
    }

    /// Get the total number of events in the complete history (original + delta)
    /// This is more efficient than calling `full_history().len()` as it doesn't allocate.
    pub fn full_history_len(&self) -> usize {
        self.history.len() + self.delta.len()
    }

    /// Check if the complete history (original + delta) is empty
    /// This is more efficient than calling `full_history().is_empty()` as it doesn't allocate.
    pub fn is_full_history_empty(&self) -> bool {
        self.history.is_empty() && self.delta.is_empty()
    }

    /// Get an iterator over the complete history (original + delta)
    /// This is more efficient than calling `full_history()` when you only need to iterate,
    /// as it doesn't allocate a new Vec.
    pub fn full_history_iter(&self) -> impl Iterator<Item = &Event> {
        self.history.iter().chain(self.delta.iter())
    }

    /// Get the complete history (original + delta) as an owned Vec.
    ///
    /// **Note:** This allocates a new Vec. Prefer `full_history_iter()`, `full_history_len()`,
    /// or `is_full_history_empty()` when possible to avoid allocation.
    pub fn full_history(&self) -> Vec<Event> {
        [&self.history[..], &self.delta[..]].concat()
    }

    /// Get the version from the most recent OrchestrationStarted event
    /// Checks both existing history (cached) and delta (for newly created instances)
    /// Returns None for "0.0.0" (placeholder/unregistered version)
    pub fn version(&self) -> Option<String> {
        // First check cached metadata from initial history
        if let Some(ref v) = self.orchestration_version {
            if v == "0.0.0" {
                return None;
            }
            return Some(v.clone());
        }

        // If no cached version, check delta for newly appended OrchestrationStarted
        for e in self.delta.iter().rev() {
            if let EventKind::OrchestrationStarted { version, .. } = &e.kind {
                if version == "0.0.0" {
                    return None;
                }
                return Some(version.clone());
            }
        }

        None
    }

    /// Get the input from the most recent OrchestrationStarted event
    pub fn input(&self) -> Option<&str> {
        self.orchestration_input.as_deref()
    }

    /// Extract input and parent linkage from history for orchestration context
    /// This looks at the full history including any newly appended events in the delta
    pub fn extract_context(&self) -> (String, Option<(String, u64)>) {
        // First check if we have metadata from the initial scan
        if let Some(ref input) = self.orchestration_input {
            let parent_link = if let (Some(parent_inst), Some(parent_id)) = (&self.parent_instance, self.parent_id) {
                Some((parent_inst.clone(), parent_id))
            } else {
                None
            };
            return (input.clone(), parent_link);
        }

        // If no metadata yet (empty initial history), check the delta for OrchestrationStarted
        for e in self.delta.iter().rev() {
            if let EventKind::OrchestrationStarted {
                input,
                parent_instance,
                parent_id,
                ..
            } = &e.kind
            {
                let parent_link = if let (Some(pinst), Some(pid)) = (parent_instance.clone(), *parent_id) {
                    Some((pinst, pid))
                } else {
                    None
                };
                return (input.clone(), parent_link);
            }
        }

        // Fallback - no OrchestrationStarted found
        (String::new(), None)
    }

    /// Compute the set of in-flight activities for this orchestration
    ///
    /// In-flight activities are those that have been scheduled (have an ActivityScheduled event)
    /// but have not yet completed (no corresponding ActivityCompleted or ActivityFailed event
    /// with matching source_event_id).
    ///
    /// This is used for activity cancellation via lock stealing - when an orchestration is
    /// terminated, we delete the worker queue entries for any in-flight activities to signal
    /// to the worker that the activity has been cancelled.
    pub fn compute_inflight_activities(&self, instance: &str, execution_id: u64) -> Vec<ScheduledActivityIdentifier> {
        // Collect all scheduled activity event IDs
        let scheduled: std::collections::HashSet<u64> = self
            .full_history_iter()
            .filter_map(|e| {
                if matches!(&e.kind, EventKind::ActivityScheduled { .. }) {
                    Some(e.event_id)
                } else {
                    None
                }
            })
            .collect();

        // Collect all completed/failed activity source_event_ids (the ActivityScheduled event_id they reference)
        let completed: std::collections::HashSet<u64> = self
            .full_history_iter()
            .filter_map(|e| {
                if matches!(
                    &e.kind,
                    EventKind::ActivityCompleted { .. } | EventKind::ActivityFailed { .. }
                ) {
                    e.source_event_id
                } else {
                    None
                }
            })
            .collect();

        // Collect all activity cancellation request source_event_ids.
        // These are best-effort, but once recorded they reflect a deterministic cancellation decision.
        //
        // IMPORTANT: This scans the *full history* (persisted history + this turn's delta).
        // That means an ActivityCancelRequested appended in the current turn (not yet persisted)
        // will exclude the activity from the "in-flight" set.
        //
        // This is intentional and safe:
        // - Terminal cancellation and select-loser cancellation are applied in the same provider
        //   ack that persists this turn's delta, so the cancel-request event and the cancellation
        //   side-effect are committed (or retried) together.
        // - Excluding them here prevents double-canceling the same activity within a single turn.
        let cancel_requested: std::collections::HashSet<u64> = self
            .full_history_iter()
            .filter_map(|e| {
                if matches!(&e.kind, EventKind::ActivityCancelRequested { .. }) {
                    e.source_event_id
                } else {
                    None
                }
            })
            .collect();

        // In-flight = scheduled - completed - cancel_requested
        scheduled
            .difference(&completed)
            .filter(|id| !cancel_requested.contains(id))
            .map(|&activity_id| ScheduledActivityIdentifier {
                instance: instance.to_string(),
                execution_id,
                activity_id,
            })
            .collect()
    }
}

/// Reader for extracting information from a batch of work items
///
/// Separates start/CAN items from completion messages and extracts
/// execution parameters in a single pass.
#[derive(Debug)]
pub struct WorkItemReader {
    /// The start or continue-as-new item, if present
    pub start_item: Option<WorkItem>,

    /// All completion messages (ActivityCompleted, TimerFired, etc.)
    pub completion_messages: Vec<WorkItem>,

    /// Orchestration name (from start item or fallback)
    pub orchestration_name: String,

    /// Input string (from start item or empty)
    pub input: String,

    /// Version (from start item or None)
    pub version: Option<String>,

    /// Parent instance (from start item or None)
    pub parent_instance: Option<String>,

    /// Parent event ID (from start item or None)
    pub parent_id: Option<u64>,

    /// Whether this is a ContinueAsNew
    pub is_continue_as_new: bool,
}

impl WorkItemReader {
    /// Parse a batch of work items
    ///
    /// Separates start/CAN from completions and extracts parameters.
    /// Falls back to history_reader if no start item is present.
    pub fn from_messages(messages: &[WorkItem], history_mgr: &HistoryManager, instance: &str) -> Self {
        let mut start_item: Option<WorkItem> = None;
        let mut completion_messages: Vec<WorkItem> = Vec::new();

        // Separate start/CAN from completions
        for work_item in messages {
            match work_item {
                WorkItem::StartOrchestration { .. } | WorkItem::ContinueAsNew { .. } => {
                    if start_item.is_some() {
                        warn!(instance, "Duplicate Start/ContinueAsNew in batch - ignoring duplicate");
                        continue;
                    }
                    start_item = Some(work_item.clone());
                }
                // Non-start/CAN work items are completion messages
                WorkItem::ActivityCompleted { .. }
                | WorkItem::ActivityFailed { .. }
                | WorkItem::TimerFired { .. }
                | WorkItem::ExternalRaised { .. }
                | WorkItem::QueueMessage { .. }
                | WorkItem::SubOrchCompleted { .. }
                | WorkItem::SubOrchFailed { .. }
                | WorkItem::CancelInstance { .. } => {
                    completion_messages.push(work_item.clone());
                }
                #[cfg(feature = "replay-version-test")]
                WorkItem::ExternalRaised2 { .. } => {
                    completion_messages.push(work_item.clone());
                }
                // ActivityExecute shouldn't appear in orchestrator queue
                WorkItem::ActivityExecute { .. } => {}
            }
        }

        // Extract parameters from start item or use defaults
        let (orchestration_name, input, version, parent_instance, parent_id, is_continue_as_new) =
            if let Some(ref item) = start_item {
                match item {
                    WorkItem::StartOrchestration {
                        orchestration,
                        input,
                        version,
                        parent_instance,
                        parent_id,
                        ..
                    } => (
                        orchestration.clone(),
                        input.clone(),
                        version.clone(),
                        parent_instance.clone(),
                        *parent_id,
                        false,
                    ),
                    WorkItem::ContinueAsNew {
                        orchestration,
                        input,
                        version,
                        carry_forward_events,
                        ..
                    } => {
                        // Prepend carry-forward events as synthetic completions at the front.
                        // This guarantees they are materialized before any new externally-raised
                        // persistent events, preserving FIFO order across CAN boundaries.
                        let mut carried: Vec<WorkItem> = carry_forward_events
                            .iter()
                            .map(|(name, data)| WorkItem::QueueMessage {
                                instance: instance.to_string(),
                                name: name.clone(),
                                data: data.clone(),
                            })
                            .collect();
                        carried.append(&mut completion_messages);
                        completion_messages = carried;

                        (orchestration.clone(), input.clone(), version.clone(), None, None, true)
                    }
                    _ => unreachable!(),
                }
            } else {
                // No start item - extract ALL fields from history manager
                // This is the completion-only replay path where OrchestrationStarted already exists in history.
                // All fields must come from history to ensure correct replay behavior.
                let orchestration_name = history_mgr.orchestration_name.clone().unwrap_or_else(|| {
                    if !completion_messages.is_empty() {
                        warn!(instance, "completion messages for unstarted instance");
                    }
                    String::new()
                });
                let input = history_mgr.orchestration_input.clone().unwrap_or_default();
                let version = history_mgr.version();
                let parent_instance = history_mgr.parent_instance.clone();
                let parent_id = history_mgr.parent_id;
                (orchestration_name, input, version, parent_instance, parent_id, false)
            };

        Self {
            start_item,
            completion_messages,
            orchestration_name,
            input,
            version,
            parent_instance,
            parent_id,
            is_continue_as_new,
        }
    }

    /// Check if this batch has a start or continue-as-new item
    pub fn has_start_item(&self) -> bool {
        self.start_item.is_some()
    }

    /// Check if the orchestration name is empty (error condition)
    pub fn has_orchestration_name(&self) -> bool {
        !self.orchestration_name.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_history_reader_from_empty_history() {
        let metadata = HistoryManager::from_history(&[]);
        assert!(metadata.orchestration_name.is_none());
        assert!(!metadata.is_terminal());
        assert_eq!(metadata.status(), "Running");
    }

    #[test]
    fn test_history_reader_from_started_only() {
        let history = vec![Event::with_event_id(
            1,
            "test-inst",
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
        )];

        let metadata = HistoryManager::from_history(&history);
        assert_eq!(metadata.orchestration_name, Some("test-orch".to_string()));
        assert_eq!(metadata.orchestration_version, Some("1.0.0".to_string()));
        assert_eq!(metadata.orchestration_input, Some("test-input".to_string()));
        assert!(!metadata.is_terminal());
        assert_eq!(metadata.status(), "Running");
        assert_eq!(metadata.current_execution_id, Some(1));
    }

    #[test]
    fn test_history_reader_completed() {
        let history = vec![
            Event::with_event_id(
                1,
                "test-inst",
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
            ),
            Event::with_event_id(
                2,
                "test-inst",
                1,
                None,
                EventKind::OrchestrationCompleted {
                    output: "success".to_string(),
                },
            ),
        ];

        let metadata = HistoryManager::from_history(&history);
        assert!(metadata.is_completed);
        assert!(metadata.is_terminal());
        assert_eq!(metadata.status(), "Completed");
    }

    #[test]
    fn test_history_reader_failed() {
        let history = vec![
            Event::with_event_id(
                1,
                "test-inst",
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
            ),
            Event::with_event_id(
                2,
                "test-inst",
                1,
                None,
                EventKind::OrchestrationFailed {
                    details: crate::ErrorDetails::Application {
                        kind: crate::AppErrorKind::OrchestrationFailed,
                        message: "boom".to_string(),
                        retryable: false,
                    },
                },
            ),
        ];

        let metadata = HistoryManager::from_history(&history);
        assert!(metadata.is_failed);
        assert!(metadata.is_terminal());
        assert_eq!(metadata.status(), "Failed");
    }

    #[test]
    fn test_history_reader_continued_as_new() {
        let history = vec![
            Event::with_event_id(
                1,
                "test-inst",
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "test-orch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "input1".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            ),
            Event::with_event_id(
                2,
                "test-inst",
                1,
                None,
                EventKind::OrchestrationContinuedAsNew {
                    input: "input2".to_string(),
                },
            ),
            Event::with_event_id(
                3,
                "test-inst",
                2,
                None,
                EventKind::OrchestrationStarted {
                    name: "test-orch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "input2".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            ),
        ];

        let metadata = HistoryManager::from_history(&history);
        // Most recent execution
        assert_eq!(metadata.orchestration_input, Some("input2".to_string()));
        assert_eq!(metadata.current_execution_id, Some(2));
        assert!(!metadata.is_terminal()); // Current execution is running
    }

    #[test]
    fn test_history_reader_with_parent() {
        let history = vec![Event::with_event_id(
            1,
            "child-inst",
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "child-orch".to_string(),
                version: "1.0.0".to_string(),
                input: "test".to_string(),
                parent_instance: Some("parent-instance".to_string()),
                parent_id: Some(42),
                carry_forward_events: None,
                initial_custom_status: None,
            },
        )];

        let metadata = HistoryManager::from_history(&history);
        assert_eq!(metadata.parent_instance, Some("parent-instance".to_string()));
        assert_eq!(metadata.parent_id, Some(42));
    }

    #[test]
    fn test_workitem_reader_with_start() {
        let messages = vec![
            WorkItem::StartOrchestration {
                instance: "test-inst".to_string(),
                orchestration: "test-orch".to_string(),
                input: "test-input".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: Some("parent".to_string()),
                parent_id: Some(42),
                execution_id: crate::INITIAL_EXECUTION_ID,
            },
            WorkItem::ActivityCompleted {
                instance: "test-inst".to_string(),
                execution_id: 1,
                id: 1,
                result: "result".to_string(),
            },
        ];

        let history_mgr = HistoryManager::from_history(&[]);
        let reader = WorkItemReader::from_messages(&messages, &history_mgr, "test-inst");

        assert!(reader.has_start_item());
        assert_eq!(reader.orchestration_name, "test-orch");
        assert_eq!(reader.input, "test-input");
        assert_eq!(reader.version, Some("1.0.0".to_string()));
        assert_eq!(reader.parent_instance, Some("parent".to_string()));
        assert_eq!(reader.parent_id, Some(42));
        assert!(!reader.is_continue_as_new);
        assert_eq!(reader.completion_messages.len(), 1);
    }

    #[test]
    fn test_workitem_reader_with_can() {
        let messages = vec![WorkItem::ContinueAsNew {
            instance: "test-inst".to_string(),
            orchestration: "test-orch".to_string(),
            input: "new-input".to_string(),
            version: Some("2.0.0".to_string()),
            carry_forward_events: vec![],
            initial_custom_status: None,
        }];

        let history_mgr = HistoryManager::from_history(&[]);
        let reader = WorkItemReader::from_messages(&messages, &history_mgr, "test-inst");

        assert!(reader.has_start_item());
        assert_eq!(reader.orchestration_name, "test-orch");
        assert_eq!(reader.input, "new-input");
        assert!(reader.is_continue_as_new);
        assert_eq!(reader.parent_instance, None);
        assert_eq!(reader.parent_id, None);
    }

    #[test]
    fn test_workitem_reader_completion_only() {
        let messages = vec![
            WorkItem::ActivityCompleted {
                instance: "test-inst".to_string(),
                execution_id: 1,
                id: 1,
                result: "result".to_string(),
            },
            WorkItem::TimerFired {
                instance: "test-inst".to_string(),
                execution_id: 1,
                id: 2,
                fire_at_ms: 1000,
            },
        ];

        let history = vec![Event::with_event_id(
            1,
            "test-inst".to_string(),
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "test-orch".to_string(),
                version: "1.0.0".to_string(),
                input: "test-input".to_string(),
                parent_instance: Some("parent-inst".to_string()),
                parent_id: Some(42),
                carry_forward_events: None,
                initial_custom_status: None,
            },
        )];

        let history_mgr = HistoryManager::from_history(&history);
        let reader = WorkItemReader::from_messages(&messages, &history_mgr, "test-inst");

        // Completion-only: all fields extracted from history
        assert!(!reader.has_start_item());
        assert_eq!(reader.orchestration_name, "test-orch");
        assert_eq!(reader.input, "test-input"); // From history
        assert_eq!(reader.version, Some("1.0.0".to_string())); // From history (issue #49 fix)
        assert_eq!(reader.parent_instance, Some("parent-inst".to_string())); // From history
        assert_eq!(reader.parent_id, Some(42)); // From history
        assert!(!reader.is_continue_as_new);
        assert_eq!(reader.completion_messages.len(), 2);
    }

    #[test]
    fn test_workitem_reader_empty_messages() {
        let messages = vec![];
        let history_mgr = HistoryManager::from_history(&[]);
        let reader = WorkItemReader::from_messages(&messages, &history_mgr, "test-inst");

        assert!(!reader.has_start_item());
        assert_eq!(reader.orchestration_name, ""); // Empty
        assert!(!reader.has_orchestration_name());
        assert_eq!(reader.completion_messages.len(), 0);
    }

    #[test]
    fn test_workitem_reader_duplicate_start() {
        let messages = vec![
            WorkItem::StartOrchestration {
                instance: "test-inst".to_string(),
                orchestration: "first-orch".to_string(),
                input: "input1".to_string(),
                version: None,
                parent_instance: None,
                parent_id: None,
                execution_id: crate::INITIAL_EXECUTION_ID,
            },
            WorkItem::StartOrchestration {
                instance: "test-inst".to_string(),
                orchestration: "second-orch".to_string(),
                input: "input2".to_string(),
                version: None,
                parent_instance: None,
                parent_id: None,
                execution_id: crate::INITIAL_EXECUTION_ID,
            },
        ];

        let history_mgr = HistoryManager::from_history(&[]);
        let reader = WorkItemReader::from_messages(&messages, &history_mgr, "test-inst");

        // Should only use the first one
        assert_eq!(reader.orchestration_name, "first-orch");
        assert_eq!(reader.input, "input1");
    }

    #[test]
    fn test_full_history_len() {
        // Empty history manager
        let mgr = HistoryManager::from_history(&[]);
        assert_eq!(mgr.full_history_len(), 0);

        // History only
        let history = vec![Event::with_event_id(
            1,
            "test-inst",
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
        )];
        let mgr = HistoryManager::from_history(&history);
        assert_eq!(mgr.full_history_len(), 1);

        // History + delta
        let mut mgr = HistoryManager::from_history(&history);
        mgr.append(Event::with_event_id(
            2,
            "test-inst",
            1,
            None,
            EventKind::OrchestrationCompleted {
                output: "done".to_string(),
            },
        ));
        assert_eq!(mgr.full_history_len(), 2);
        assert_eq!(mgr.full_history_len(), mgr.full_history().len());
    }

    #[test]
    fn test_is_full_history_empty() {
        // Empty history manager
        let mgr = HistoryManager::from_history(&[]);
        assert!(mgr.is_full_history_empty());

        // Non-empty history
        let history = vec![Event::with_event_id(
            1,
            "test-inst",
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
        )];
        let mgr = HistoryManager::from_history(&history);
        assert!(!mgr.is_full_history_empty());

        // Empty history but non-empty delta
        let mut mgr = HistoryManager::from_history(&[]);
        mgr.append(Event::with_event_id(
            1,
            "test-inst",
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
        ));
        assert!(!mgr.is_full_history_empty());
    }

    #[test]
    fn test_full_history_iter() {
        let history = vec![Event::with_event_id(
            1,
            "test-inst",
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
        )];
        let mut mgr = HistoryManager::from_history(&history);
        mgr.append(Event::with_event_id(
            2,
            "test-inst",
            1,
            None,
            EventKind::OrchestrationCompleted {
                output: "done".to_string(),
            },
        ));

        // Iterator should yield same events as full_history()
        let iter_events: Vec<_> = mgr.full_history_iter().cloned().collect();
        let full_history = mgr.full_history();
        assert_eq!(iter_events, full_history);

        // Check order: history first, then delta
        assert_eq!(iter_events.len(), 2);
        assert!(matches!(iter_events[0].kind, EventKind::OrchestrationStarted { .. }));
        assert!(matches!(iter_events[1].kind, EventKind::OrchestrationCompleted { .. }));
    }

    #[test]
    fn test_workitem_reader_can_carry_forward_prepended() {
        // CAN with carry-forward events + an external completion that arrived later.
        // The carry-forward events must come BEFORE the external event in completions.
        let messages = vec![
            WorkItem::ContinueAsNew {
                instance: "inst".to_string(),
                orchestration: "orch".to_string(),
                input: "new".to_string(),
                version: None,
                carry_forward_events: vec![
                    ("X".to_string(), "old-x".to_string()),
                    ("Y".to_string(), "old-y".to_string()),
                ],
                initial_custom_status: None,
            },
            WorkItem::QueueMessage {
                instance: "inst".to_string(),
                name: "X".to_string(),
                data: "new-x".to_string(),
            },
        ];

        let history_mgr = HistoryManager::from_history(&[]);
        let reader = WorkItemReader::from_messages(&messages, &history_mgr, "inst");

        assert!(reader.is_continue_as_new);
        assert_eq!(reader.completion_messages.len(), 3);
        // Carry-forward events first (FIFO order preserved)
        assert!(matches!(
            &reader.completion_messages[0],
            WorkItem::QueueMessage { name, data, .. }
            if name == "X" && data == "old-x"
        ));
        assert!(matches!(
            &reader.completion_messages[1],
            WorkItem::QueueMessage { name, data, .. }
            if name == "Y" && data == "old-y"
        ));
        // New event comes last
        assert!(matches!(
            &reader.completion_messages[2],
            WorkItem::QueueMessage { name, data, .. }
            if name == "X" && data == "new-x"
        ));
    }
}

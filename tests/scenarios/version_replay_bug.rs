//! Bug reproduction test for issue #49: WorkItemReader doesn't extract version from history during replay
//!
//! This test reproduces the exact scenario where:
//! 1. v1.0.2 orchestration schedules activity "system-prune"
//! 2. v1.0.3 orchestration schedules different activity "system-prune-2"
//! 3. Orchestration starts with v1.0.2 (explicit version)
//! 4. Activity is scheduled, server "restarts" (lock expires)
//! 5. Activity completes, completion message arrives (no start item in batch)
//! 6. BUG: WorkItemReader sets version=None, runtime uses Latest (v1.0.3)
//! 7. v1.0.3 tries to schedule "system-prune-2" but history shows "system-prune" → NONDETERMINISM ERROR

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{Client, OrchestrationRegistry};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[path = "../common/mod.rs"]
mod common;

/// Reproduction test for issue #49: version not extracted from history during completion-only replay
///
/// The bug occurs when:
/// 1. Orchestration v1.0.2 is started and schedules activity A
/// 2. Runtime restarts (or lock expires and is re-acquired by different node)
/// 3. Activity A completes, completion message arrives WITHOUT start item
/// 4. WorkItemReader.version should be "1.0.2" (from history) but is None
/// 5. None version triggers Latest policy resolution → picks v1.0.3
/// 6. v1.0.3 handler schedules activity B → history shows activity A → NONDETERMINISM
#[tokio::test]
async fn e2e_replay_completion_only_must_use_version_from_history() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Track which versions are called
    let v102_called = Arc::new(AtomicUsize::new(0));
    let v103_called = Arc::new(AtomicUsize::new(0));
    let prune_activity_called = Arc::new(AtomicUsize::new(0));

    // v1.0.2: schedules activity "system-prune"
    let v102_called_clone = v102_called.clone();
    let handler_v102 = move |ctx: duroxide::OrchestrationContext, _input: String| {
        let v102_called = v102_called_clone.clone();
        async move {
            v102_called.fetch_add(1, Ordering::SeqCst);
            let result = ctx.schedule_activity("system-prune", "prune-input").await?;
            Ok(result)
        }
    };

    // v1.0.3: schedules DIFFERENT activity "system-prune-2" (incompatible with v1.0.2 history!)
    let v103_called_clone = v103_called.clone();
    let handler_v103 = move |ctx: duroxide::OrchestrationContext, _input: String| {
        let v103_called = v103_called_clone.clone();
        async move {
            v103_called.fetch_add(1, Ordering::SeqCst);
            // This schedules a DIFFERENT activity - incompatible during replay!
            let result = ctx.schedule_activity("system-prune-2", "prune-input").await?;
            Ok(result)
        }
    };

    // Registry has BOTH versions - Latest policy will pick v1.0.3
    let orchestrations = OrchestrationRegistry::builder()
        .register_versioned("SystemPruner", "1.0.2", handler_v102)
        .register_versioned("SystemPruner", "1.0.3", handler_v103)
        .build();

    // Activities - both "system-prune" and "system-prune-2" registered
    let prune_activity_called_clone = prune_activity_called.clone();
    let activities = ActivityRegistry::builder()
        .register("system-prune", move |_ctx: duroxide::ActivityContext, input: String| {
            let called = prune_activity_called_clone.clone();
            async move {
                called.fetch_add(1, Ordering::SeqCst);
                Ok(format!("pruned:{input}"))
            }
        })
        .register(
            "system-prune-2",
            |_ctx: duroxide::ActivityContext, input: String| async move { Ok(format!("pruned-v2:{input}")) },
        )
        .build();

    let options = RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    // Start runtime with BOTH versions registered
    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities.clone(),
        orchestrations.clone(),
        options.clone(),
    )
    .await;

    let client = Client::new(store.clone());

    // Key: Start orchestration with EXPLICIT version 1.0.2
    // This ensures OrchestrationStarted event has version: "1.0.2"
    client
        .start_orchestration_versioned("issue49-repro", "SystemPruner", "1.0.2", "test-input")
        .await
        .expect("start should succeed");

    // Wait for orchestration to complete
    let status = client
        .wait_for_orchestration("issue49-repro", Duration::from_secs(10))
        .await
        .expect("wait should succeed");

    // If the bug exists, the orchestration will fail with nondeterminism error
    // because replay (after activity completes) uses Latest (v1.0.3) instead of v1.0.2
    match &status {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            // Should complete with "pruned:" (v1.0.2's activity)
            assert!(
                output.contains("pruned:"),
                "Expected v1.0.2 activity output, got: {output}"
            );
            // v1.0.2 should be called (possibly multiple times for replay), v1.0.3 should NOT be called
            assert!(v102_called.load(Ordering::SeqCst) > 0, "v1.0.2 should have been called");
            // This is the key assertion - if bug exists, v1.0.3 would also be called during replay
            assert_eq!(
                v103_called.load(Ordering::SeqCst),
                0,
                "v1.0.3 should NOT be called during replay of v1.0.2 instance"
            );
        }
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            // If we see nondeterminism error, that confirms the bug
            let details_str = format!("{details:?}");
            if details_str.contains("nondeterminism") || details_str.contains("schedule order mismatch") {
                panic!(
                    "BUG CONFIRMED: Nondeterminism error due to version mismatch during replay.\n\
                     v1.0.2 calls: {}\n\
                     v1.0.3 calls: {}\n\
                     Error: {details_str}",
                    v102_called.load(Ordering::SeqCst),
                    v103_called.load(Ordering::SeqCst)
                );
            } else {
                panic!("Orchestration failed unexpectedly: {details:?}");
            }
        }
        other => panic!("Unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Reproduction test for issue #49 with ContinueAsNew: version not extracted from history during completion-only replay
///
/// This is the CAN variant of the test. The bug occurs when:
/// 1. Orchestration v1.0.2 is started and does CAN to v1.0.2 (explicit version)
/// 2. Execution 2 (v1.0.2) schedules activity A
/// 3. Runtime restarts (or lock expires and is re-acquired by different node)
/// 4. Activity A completes, completion message arrives for execution 2 WITHOUT start item
/// 5. WorkItemReader.version should be "1.0.2" (from execution 2's OrchestrationStarted) but was None
/// 6. None version triggers Latest policy resolution → picks v1.0.3
/// 7. v1.0.3 handler schedules activity B → history shows activity A → NONDETERMINISM
#[tokio::test]
async fn e2e_replay_completion_only_after_can_must_use_version_from_history() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Track which versions are called and for which execution
    let v102_exec1_called = Arc::new(AtomicUsize::new(0));
    let v102_exec2_called = Arc::new(AtomicUsize::new(0));
    let v103_called = Arc::new(AtomicUsize::new(0));

    // v1.0.2: First execution does CAN to v1.0.2, second execution schedules activity
    let v102_exec1_called_clone = v102_exec1_called.clone();
    let v102_exec2_called_clone = v102_exec2_called.clone();
    let handler_v102 = move |ctx: duroxide::OrchestrationContext, input: String| {
        let exec1_called = v102_exec1_called_clone.clone();
        let exec2_called = v102_exec2_called_clone.clone();
        async move {
            if input == "first-execution" {
                // Execution 1: do CAN to same version with different input
                exec1_called.fetch_add(1, Ordering::SeqCst);
                ctx.continue_as_new_versioned("1.0.2", "second-execution").await
            } else {
                // Execution 2: schedule activity (this is where replay happens after restart)
                exec2_called.fetch_add(1, Ordering::SeqCst);
                let result = ctx.schedule_activity("system-prune", "prune-input").await?;
                Ok(result)
            }
        }
    };

    // v1.0.3: schedules DIFFERENT activity (incompatible with v1.0.2 history!)
    let v103_called_clone = v103_called.clone();
    let handler_v103 = move |ctx: duroxide::OrchestrationContext, _input: String| {
        let v103_called = v103_called_clone.clone();
        async move {
            v103_called.fetch_add(1, Ordering::SeqCst);
            // This schedules a DIFFERENT activity - incompatible during replay!
            let result = ctx.schedule_activity("system-prune-2", "prune-input").await?;
            Ok(result)
        }
    };

    // Registry has BOTH versions - Latest policy will pick v1.0.3
    let orchestrations = OrchestrationRegistry::builder()
        .register_versioned("SystemPruner", "1.0.2", handler_v102)
        .register_versioned("SystemPruner", "1.0.3", handler_v103)
        .build();

    // Activities
    let activities = ActivityRegistry::builder()
        .register(
            "system-prune",
            |_ctx: duroxide::ActivityContext, input: String| async move { Ok(format!("pruned:{input}")) },
        )
        .register(
            "system-prune-2",
            |_ctx: duroxide::ActivityContext, input: String| async move { Ok(format!("pruned-v2:{input}")) },
        )
        .build();

    let options = RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities.clone(),
        orchestrations.clone(),
        options.clone(),
    )
    .await;

    let client = Client::new(store.clone());

    // Start with v1.0.2, input="first-execution" to trigger CAN
    client
        .start_orchestration_versioned("issue49-can-repro", "SystemPruner", "1.0.2", "first-execution")
        .await
        .expect("start should succeed");

    // Wait for orchestration to complete
    let status = client
        .wait_for_orchestration("issue49-can-repro", Duration::from_secs(10))
        .await
        .expect("wait should succeed");

    match &status {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            // Should complete with "pruned:" (v1.0.2's activity from execution 2)
            assert!(
                output.contains("pruned:"),
                "Expected v1.0.2 activity output, got: {output}"
            );

            // v1.0.2 execution 1 should have been called (CAN)
            assert!(
                v102_exec1_called.load(Ordering::SeqCst) > 0,
                "v1.0.2 execution 1 should have been called for CAN"
            );

            // v1.0.2 execution 2 should have been called (activity scheduling + replay)
            assert!(
                v102_exec2_called.load(Ordering::SeqCst) > 0,
                "v1.0.2 execution 2 should have been called"
            );

            // v1.0.3 should NEVER be called - this is the key assertion
            // If the bug existed, completion-only replay for execution 2 would use Latest (v1.0.3)
            assert_eq!(
                v103_called.load(Ordering::SeqCst),
                0,
                "v1.0.3 should NOT be called during replay of v1.0.2 execution 2.\n\
                 This would indicate the bug: version not extracted from execution 2's history."
            );
        }
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            let details_str = format!("{details:?}");
            if details_str.contains("nondeterminism") || details_str.contains("schedule order mismatch") {
                panic!(
                    "BUG CONFIRMED: Nondeterminism error due to version mismatch during replay after CAN.\n\
                     v1.0.2 exec1 calls: {}\n\
                     v1.0.2 exec2 calls: {}\n\
                     v1.0.3 calls: {}\n\
                     Error: {details_str}",
                    v102_exec1_called.load(Ordering::SeqCst),
                    v102_exec2_called.load(Ordering::SeqCst),
                    v103_called.load(Ordering::SeqCst)
                );
            } else {
                panic!("Orchestration failed unexpectedly: {details:?}");
            }
        }
        other => panic!("Unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Unit test: WorkItemReader must extract ALL fields from history when no start item present
#[test]
fn unit_workitem_reader_completion_only_must_preserve_version() {
    use duroxide::providers::WorkItem;
    use duroxide::runtime::{HistoryManager, WorkItemReader};
    use duroxide::{Event, EventKind};

    // Simulate completion messages WITHOUT a start item (the bug scenario)
    let messages = vec![WorkItem::ActivityCompleted {
        instance: "test-inst".to_string(),
        execution_id: 1,
        id: 1,
        result: "result".to_string(),
    }];

    // History contains OrchestrationStarted with ALL fields populated
    let history = vec![Event::with_event_id(
        1,
        "test-inst".to_string(),
        1,
        None,
        EventKind::OrchestrationStarted {
            name: "test-orch".to_string(),
            version: "1.0.2".to_string(),
            input: "original-input".to_string(),
            parent_instance: Some("parent-inst".to_string()),
            parent_id: Some(42),
            carry_forward_events: None,
            initial_custom_status: None,
        },
    )];

    let history_mgr = HistoryManager::from_history(&history);
    let reader = WorkItemReader::from_messages(&messages, &history_mgr, "test-inst");

    // ALL fields MUST be extracted from history for completion-only replay
    assert!(!reader.has_start_item(), "Should not have start item");
    assert_eq!(reader.orchestration_name, "test-orch", "Name should come from history");
    assert_eq!(reader.input, "original-input", "Input should come from history");
    assert_eq!(
        reader.version,
        Some("1.0.2".to_string()),
        "Version MUST be extracted from history to ensure correct handler is used during replay.\n\
         If version is None, the runtime uses Latest policy which may resolve to a different version,\n\
         causing nondeterminism errors. See issue #49."
    );
    assert_eq!(
        reader.parent_instance,
        Some("parent-inst".to_string()),
        "parent_instance should come from history"
    );
    assert_eq!(reader.parent_id, Some(42), "parent_id should come from history");
    assert!(
        !reader.is_continue_as_new,
        "is_continue_as_new should be false for completion-only"
    );
}

/// Unit test: WorkItemReader must extract ALL fields from history for Nth execution (after CAN)
///
/// This tests the scenario where:
/// 1. v1.0.0 starts, does CAN to v2.0.0
/// 2. v2.0.0 (execution 2) schedules activity
/// 3. Restart happens
/// 4. Activity completes, completion-only for execution 2
/// 5. ALL fields must be extracted from execution 2's OrchestrationStarted
#[test]
fn unit_workitem_reader_nth_execution_must_preserve_version() {
    use duroxide::providers::WorkItem;
    use duroxide::runtime::{HistoryManager, WorkItemReader};
    use duroxide::{Event, EventKind};

    // Simulate completion messages for execution 2 (after CAN)
    let messages = vec![WorkItem::ActivityCompleted {
        instance: "test-inst".to_string(),
        execution_id: 2, // Execution 2
        id: 1,
        result: "result".to_string(),
    }];

    // History for execution 2 ONLY (provider filters by execution_id)
    // This is what the provider returns for execution 2
    let history_exec_2 = vec![
        Event::with_event_id(
            1, // First event of execution 2
            "test-inst".to_string(),
            2, // execution_id = 2
            None,
            EventKind::OrchestrationStarted {
                name: "test-orch".to_string(),
                version: "2.0.0".to_string(), // v2.0.0 for execution 2!
                input: "can-input".to_string(),
                parent_instance: Some("parent-for-v2".to_string()),
                parent_id: Some(99),
                carry_forward_events: None,
                initial_custom_status: None,
            },
        ),
        Event::with_event_id(
            2,
            "test-inst".to_string(),
            2,
            None,
            EventKind::ActivityScheduled {
                name: "SomeActivity".to_string(),
                input: "activity-input".to_string(),
                session_id: None,
                tag: None,
            },
        ),
    ];

    let history_mgr = HistoryManager::from_history(&history_exec_2);

    // Verify HistoryManager correctly extracts all fields from execution 2's history
    assert_eq!(
        history_mgr.version(),
        Some("2.0.0".to_string()),
        "HistoryManager should extract v2.0.0 from execution 2's OrchestrationStarted"
    );

    let reader = WorkItemReader::from_messages(&messages, &history_mgr, "test-inst");

    // ALL fields MUST be extracted from execution 2's history
    assert!(!reader.has_start_item(), "Should not have start item");
    assert_eq!(reader.orchestration_name, "test-orch", "Name should come from history");
    assert_eq!(
        reader.input, "can-input",
        "Input should come from execution 2's history"
    );
    assert_eq!(
        reader.version,
        Some("2.0.0".to_string()),
        "Version MUST be extracted from execution 2's history for correct replay.\n\
         After CAN, execution 2 may have different version than execution 1."
    );
    assert_eq!(
        reader.parent_instance,
        Some("parent-for-v2".to_string()),
        "parent_instance should come from history"
    );
    assert_eq!(reader.parent_id, Some(99), "parent_id should come from history");
    assert!(
        !reader.is_continue_as_new,
        "is_continue_as_new should be false for completion-only"
    );
}

/// Unit test: Verify ALL tuple fields are correctly extracted from history
///
/// The completion-only branch returns:
/// (orchestration_name, input, version, parent_instance, parent_id, is_continue_as_new)
///
/// After fix, ALL fields should be populated from history.
#[test]
fn unit_workitem_reader_completion_only_tuple_field_analysis() {
    use duroxide::providers::WorkItem;
    use duroxide::runtime::{HistoryManager, WorkItemReader};
    use duroxide::{Event, EventKind};

    // Simulate completion messages for a sub-orchestration
    let messages = vec![WorkItem::ActivityCompleted {
        instance: "child-inst".to_string(),
        execution_id: 1,
        id: 1,
        result: "result".to_string(),
    }];

    // History with all fields populated
    let history = vec![Event::with_event_id(
        1,
        "child-inst".to_string(),
        1,
        None,
        EventKind::OrchestrationStarted {
            name: "child-orch".to_string(),
            version: "1.0.0".to_string(),
            input: "original-input".to_string(),
            parent_instance: Some("parent-inst".to_string()),
            parent_id: Some(42),
            carry_forward_events: None,
            initial_custom_status: None,
        },
    )];

    let history_mgr = HistoryManager::from_history(&history);
    let reader = WorkItemReader::from_messages(&messages, &history_mgr, "child-inst");

    // ALL tuple fields should now be correctly extracted from history:

    // Field 1: orchestration_name - from history_mgr.orchestration_name
    assert_eq!(
        reader.orchestration_name, "child-orch",
        "orchestration_name should come from history"
    );

    // Field 2: input - from history_mgr.orchestration_input
    assert_eq!(reader.input, "original-input", "input should come from history");

    // Field 3: version - from history_mgr.version() (issue #49 fix)
    assert_eq!(
        reader.version,
        Some("1.0.0".to_string()),
        "version should come from history"
    );

    // Field 4: parent_instance - from history_mgr.parent_instance
    assert_eq!(
        reader.parent_instance,
        Some("parent-inst".to_string()),
        "parent_instance should come from history"
    );

    // Field 5: parent_id - from history_mgr.parent_id
    assert_eq!(reader.parent_id, Some(42), "parent_id should come from history");

    // Field 6: is_continue_as_new = false - Correct for completion-only replay
    assert!(!reader.is_continue_as_new, "Completion-only is not CAN");
}

/// Unit test: Verify input is now correctly populated in WorkItemReader AND extract_context()
///
/// After the fix, WorkItemReader.input is populated from history for consistency.
/// extract_context() also returns the same value (used in execution.rs).
#[test]
fn unit_input_comes_from_extract_context_not_workitem_reader() {
    use duroxide::providers::WorkItem;
    use duroxide::runtime::{HistoryManager, WorkItemReader};
    use duroxide::{Event, EventKind};

    // Completion-only messages (no start item)
    let messages = vec![WorkItem::ActivityCompleted {
        instance: "test-inst".to_string(),
        execution_id: 1,
        id: 1,
        result: "result".to_string(),
    }];

    // History with specific input
    let history = vec![
        Event::with_event_id(
            1,
            "test-inst".to_string(),
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "test-orch".to_string(),
                version: "1.0.0".to_string(),
                input: "ORIGINAL-INPUT-FROM-FIRST-TURN".to_string(), // The actual input
                parent_instance: None,
                parent_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        ),
        Event::with_event_id(
            2,
            "test-inst".to_string(),
            1,
            None,
            EventKind::ActivityScheduled {
                name: "SomeActivity".to_string(),
                input: "activity-input".to_string(),
                session_id: None,
                tag: None,
            },
        ),
    ];

    let history_mgr = HistoryManager::from_history(&history);
    let reader = WorkItemReader::from_messages(&messages, &history_mgr, "test-inst");

    // After fix: WorkItemReader.input is now populated from history
    assert_eq!(
        reader.input, "ORIGINAL-INPUT-FROM-FIRST-TURN",
        "WorkItemReader.input should now be populated from history"
    );

    // extract_context() ALSO returns the correct input from history
    let (extracted_input, _parent_link) = history_mgr.extract_context();
    assert_eq!(
        extracted_input, "ORIGINAL-INPUT-FROM-FIRST-TURN",
        "extract_context() correctly retrieves input from OrchestrationStarted in history"
    );

    // Both methods now return the same value for consistency
    assert_eq!(
        reader.input, extracted_input,
        "WorkItemReader.input and extract_context() should match"
    );
}

/// Unit test: Verify Nth execution history contains OrchestrationStarted as first event
///
/// The provider fetches history filtered by execution_id.
/// For execution N, the first event MUST be OrchestrationStarted (created when CAN was processed).
#[test]
fn unit_nth_execution_history_starts_with_orchestration_started() {
    use duroxide::runtime::HistoryManager;
    use duroxide::{Event, EventKind};

    // Simulate execution 2's history (what provider returns for execution_id=2)
    // This is created when WorkItem::ContinueAsNew is processed
    let execution_2_history = vec![
        // First event of execution 2: OrchestrationStarted
        Event::with_event_id(
            1, // event_id resets to 1 for new execution
            "test-inst".to_string(),
            2, // execution_id = 2
            None,
            EventKind::OrchestrationStarted {
                name: "test-orch".to_string(),
                version: "2.0.0".to_string(), // CAN can specify a new version
                input: "can-input".to_string(),
                parent_instance: None,
                parent_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        ),
        // Second event: Activity scheduled
        Event::with_event_id(
            2,
            "test-inst".to_string(),
            2,
            None,
            EventKind::ActivityScheduled {
                name: "SomeActivity".to_string(),
                input: "activity-input".to_string(),
                session_id: None,
                tag: None,
            },
        ),
    ];

    // Verify first event is OrchestrationStarted
    assert!(
        matches!(&execution_2_history[0].kind, EventKind::OrchestrationStarted { .. }),
        "First event in Nth execution MUST be OrchestrationStarted"
    );

    // HistoryManager correctly extracts metadata from this history
    let history_mgr = HistoryManager::from_history(&execution_2_history);

    assert_eq!(
        history_mgr.orchestration_name,
        Some("test-orch".to_string()),
        "Name extracted from execution 2's OrchestrationStarted"
    );
    assert_eq!(
        history_mgr.orchestration_version,
        Some("2.0.0".to_string()),
        "Version extracted from execution 2's OrchestrationStarted"
    );
    assert_eq!(
        history_mgr.orchestration_input,
        Some("can-input".to_string()),
        "Input extracted from execution 2's OrchestrationStarted"
    );

    // version() method returns the correct version
    assert_eq!(
        history_mgr.version(),
        Some("2.0.0".to_string()),
        "version() correctly returns 2.0.0 for execution 2"
    );

    // extract_context() returns the correct input
    let (input, _) = history_mgr.extract_context();
    assert_eq!(
        input, "can-input",
        "extract_context() correctly returns input for execution 2"
    );
}

/// E2E test: Verify input is correctly preserved across CAN and completion-only replay
///
/// This test verifies that when:
/// 1. v1 calls continue_as_new with "input-for-v2"
/// 2. v2 starts (execution 2), schedules activity
/// 3. Restart happens (completion-only for execution 2)
/// 4. v2's handler receives "input-for-v2" (NOT the original input!)
#[tokio::test]
async fn e2e_can_input_preserved_during_completion_only_replay() {
    use std::sync::Mutex;

    let (store, _td) = common::create_sqlite_store_disk().await;

    // Track inputs received by each version
    let v1_inputs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let v2_inputs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // v1: Records input, then does CAN to v2 with different input
    let v1_inputs_clone = v1_inputs.clone();
    let handler_v1 = move |ctx: duroxide::OrchestrationContext, input: String| {
        let inputs = v1_inputs_clone.clone();
        async move {
            inputs.lock().unwrap().push(input.clone());
            // CAN to v2 with DIFFERENT input
            ctx.continue_as_new_versioned("2.0.0", "INPUT-FOR-V2-FROM-CAN").await
        }
    };

    // v2: Records input, schedules activity, returns
    let v2_inputs_clone = v2_inputs.clone();
    let handler_v2 = move |ctx: duroxide::OrchestrationContext, input: String| {
        let inputs = v2_inputs_clone.clone();
        async move {
            inputs.lock().unwrap().push(input.clone());
            // Schedule activity to force a second turn
            let result = ctx.schedule_activity("Echo", &input).await?;
            Ok(format!("v2-got-input:{result}"))
        }
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register_versioned("InputTest", "1.0.0", handler_v1)
        .register_versioned("InputTest", "2.0.0", handler_v2)
        .build();

    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: duroxide::ActivityContext, input: String| async move {
            Ok(input)
        })
        .build();

    let options = RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities.clone(),
        orchestrations.clone(),
        options.clone(),
    )
    .await;

    let client = Client::new(store.clone());

    // Start with v1 and ORIGINAL input
    client
        .start_orchestration_versioned("input-test", "InputTest", "1.0.0", "ORIGINAL-INPUT-FOR-V1")
        .await
        .expect("start should succeed");

    // Wait for completion
    let status = client
        .wait_for_orchestration("input-test", Duration::from_secs(10))
        .await
        .expect("wait should succeed");

    match &status {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            // v2 should have received the CAN input, not the original
            assert!(
                output.contains("INPUT-FOR-V2-FROM-CAN"),
                "v2 should output the CAN input, got: {output}"
            );

            // Verify inputs recorded
            let v1_recorded = v1_inputs.lock().unwrap();
            let v2_recorded = v2_inputs.lock().unwrap();

            // v1 should have received original input
            assert!(
                v1_recorded.iter().any(|i| i == "ORIGINAL-INPUT-FOR-V1"),
                "v1 should have received ORIGINAL-INPUT-FOR-V1, got: {v1_recorded:?}"
            );

            // v2 should have received CAN input (possibly multiple times for replay)
            // The key assertion: v2 should NEVER receive the original input
            assert!(
                v2_recorded.iter().all(|i| i == "INPUT-FOR-V2-FROM-CAN"),
                "v2 should ONLY receive INPUT-FOR-V2-FROM-CAN (the CAN input), got: {v2_recorded:?}"
            );
            assert!(
                !v2_recorded.iter().any(|i| i == "ORIGINAL-INPUT-FOR-V1"),
                "v2 should NEVER receive the original input, got: {v2_recorded:?}"
            );
        }
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {details:?}");
        }
        other => panic!("Unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Unit test: Verify completion-only replay for Nth execution uses correct input from that execution
///
/// This test verifies the internal mechanics:
/// 1. Provider returns execution N's history (filtered by execution_id)
/// 2. HistoryManager extracts input from execution N's OrchestrationStarted
/// 3. WorkItemReader.input is now populated from history (after fix)
/// 4. extract_context() also returns execution N's input
#[test]
fn unit_completion_only_replay_uses_nth_execution_input() {
    use duroxide::providers::WorkItem;
    use duroxide::runtime::{HistoryManager, WorkItemReader};
    use duroxide::{Event, EventKind};

    // Simulate completion-only messages for execution 2
    let messages = vec![WorkItem::ActivityCompleted {
        instance: "test-inst".to_string(),
        execution_id: 2, // Execution 2
        id: 1,
        result: "activity-result".to_string(),
    }];

    // History for execution 2 ONLY (as returned by provider)
    // Note: Execution 1's history is NOT included because provider filters by execution_id
    let execution_2_history = vec![
        Event::with_event_id(
            1,
            "test-inst".to_string(),
            2, // execution_id = 2
            None,
            EventKind::OrchestrationStarted {
                name: "test-orch".to_string(),
                version: "2.0.0".to_string(),
                input: "CAN-INPUT-FOR-EXECUTION-2".to_string(), // Different from execution 1!
                parent_instance: None,
                parent_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        ),
        Event::with_event_id(
            2,
            "test-inst".to_string(),
            2,
            None,
            EventKind::ActivityScheduled {
                name: "SomeActivity".to_string(),
                input: "activity-input".to_string(),
                session_id: None,
                tag: None,
            },
        ),
    ];

    let history_mgr = HistoryManager::from_history(&execution_2_history);
    let reader = WorkItemReader::from_messages(&messages, &history_mgr, "test-inst");

    // After fix: WorkItemReader.input is now populated from history
    assert_eq!(
        reader.input, "CAN-INPUT-FOR-EXECUTION-2",
        "WorkItemReader.input should be populated from execution 2's history"
    );

    // extract_context() also correctly returns execution 2's input
    let (extracted_input, _) = history_mgr.extract_context();
    assert_eq!(
        extracted_input, "CAN-INPUT-FOR-EXECUTION-2",
        "extract_context() MUST return execution 2's input, NOT execution 1's original input.\n\
         The provider filters history by execution_id, so only execution 2's events are present.\n\
         This ensures the handler receives the correct input during replay."
    );

    // Both methods return the same value for consistency
    assert_eq!(
        reader.input, extracted_input,
        "WorkItemReader.input and extract_context() should match"
    );
}

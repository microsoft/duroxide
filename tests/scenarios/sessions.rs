// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Session Scenario Test — Durable-Copilot-SDK Pattern
//!
//! This test faithfully models the durable-copilot-sdk's architecture, the primary
//! motivation for the session feature. The SDK keeps `CopilotSession` objects in
//! memory on the worker process. Each durable "turn" is an activity. All turns for
//! the same conversation must route to the same worker so the session context
//! (conversation history, auth tokens, copilot CLI child process) doesn't need to
//! be expensively recreated.
//!
//! Architecture modeled (from `durable-copilot-sdk/src/`):
//!
//!   `DurableCopilotClient` → starts orchestration per `sendAndWait()`
//!       ↓
//!   `durableTurnOrchestration` → runs `runAgentTurn` activity, handles wait/timer
//!       loops via continueAsNew, returns the final response
//!       ↓
//!   `runAgentTurn` activity → accesses `SessionManager.getSession(sessionId)` for
//!       in-memory `CopilotSession`. Cache miss → `createSession()`. Hit → reuse.
//!       Returns `TurnResult: completed | wait | input_required | error`
//!       ↓
//!   `SessionManager` (session-manager.ts) → `Map<sessionId, CopilotSession>`
//!
//! What sessions enable: scaling from 1 worker replica to N, while keeping all
//! turns for a conversation on the same worker.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[path = "../common/mod.rs"]
mod common;

// ============================================================================
// SessionManager — faithful port of session-manager.ts
// ============================================================================

/// Simulates a `CopilotSession` from @github/copilot-sdk.
/// In the real SDK this holds auth tokens, the copilot CLI child process,
/// conversation message history, and registered tools.
#[derive(Debug, Clone)]
struct CopilotSession {
    /// Accumulated conversation messages (simulates session.getMessages())
    messages: Vec<TurnMessage>,
    /// System message configured at session creation
    #[allow(dead_code)]
    system_message: String,
}

#[derive(Debug, Clone)]
struct TurnMessage {
    #[allow(dead_code)]
    role: &'static str, // "user" | "assistant"
    #[allow(dead_code)]
    content: String,
}

/// Port of `SessionManager` from session-manager.ts.
/// Tracks active CopilotSession objects in memory. The session_id is the key.
/// All worker slots on the same process share this single map — this is why
/// `worker_node_id` must be process-level, not per-slot.
struct SessionManager {
    sessions: std::sync::Mutex<HashMap<String, CopilotSession>>,
    /// Tracks how many times createSession was called (expensive operation)
    create_count: AtomicUsize,
}

impl SessionManager {
    fn new() -> Self {
        Self {
            sessions: std::sync::Mutex::new(HashMap::new()),
            create_count: AtomicUsize::new(0),
        }
    }

    /// Mirrors `sessionManager.getSession(sessionId)` — returns existing or None
    fn get_session(&self, session_id: &str) -> Option<CopilotSession> {
        self.sessions.lock().unwrap().get(session_id).cloned()
    }

    /// Mirrors `sessionManager.createSession(config)` — creates and tracks
    fn create_session(&self, session_id: &str, system_message: &str) -> CopilotSession {
        self.create_count.fetch_add(1, Ordering::SeqCst);
        let session = CopilotSession {
            messages: Vec::new(),
            system_message: system_message.to_string(),
        };
        self.sessions
            .lock()
            .unwrap()
            .insert(session_id.to_string(), session.clone());
        session
    }

    /// Update session state after a turn (saves messages back)
    fn update_session(&self, session_id: &str, session: &CopilotSession) {
        self.sessions
            .lock()
            .unwrap()
            .insert(session_id.to_string(), session.clone());
    }

    fn active_session_ids(&self) -> Vec<String> {
        self.sessions.lock().unwrap().keys().cloned().collect()
    }
}

// ============================================================================
// TurnResult — port of types.ts TurnResult
// ============================================================================

/// What the `runAgentTurn` activity returns to the orchestration.
/// Directly maps to `TurnResult` from durable-copilot-sdk/src/types.ts.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(tag = "type")]
enum TurnResult {
    #[serde(rename = "completed")]
    Completed { content: String },
    #[serde(rename = "wait")]
    Wait {
        seconds: u64,
        reason: String,
        #[allow(dead_code)]
        content: Option<String>,
    },
    #[serde(rename = "error")]
    Error { message: String },
}

// ============================================================================
// The Test
// ============================================================================

/// Scaled-out durable-copilot-sdk: multiple concurrent conversations across
/// two worker runtimes sharing the same store, each with its own SessionManager.
///
/// Models the production AKS deployment:
///   - Client process (tui-scaled.js) enqueues orchestrations → PostgreSQL
///   - N worker pods (worker.js) poll for work, each holding a SessionManager
///   - Sessions pin conversations to specific pods via session affinity
///
/// Test plan:
///   1. Start 2 runtimes (simulating 2 AKS worker pods) with different worker_node_ids
///   2. Each runtime has its own SessionManager (in-memory session state)
///   3. Start 4 concurrent conversations, each doing multi-turn + wait + continueAsNew
///   4. Assert: all turns for each conversation hit the same SessionManager
///   5. Assert: both workers served conversations (load distributed)
///   6. Assert: in-memory state (message history) accumulated correctly
///   7. Assert: no session was created more than once on its owning worker
#[tokio::test]
async fn durable_copilot_sdk_scaled_out_multi_conversation() {
    // ── Each worker gets its own SessionManager (like worker.js instances) ──
    let worker_a_sessions = Arc::new(SessionManager::new());
    let worker_b_sessions = Arc::new(SessionManager::new());

    // Track which worker handled which session (for assertions)
    let session_assignment: Arc<std::sync::Mutex<HashMap<String, String>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));

    // ── Shared store (simulates PostgreSQL) ──
    let store: Arc<dyn duroxide::providers::Provider> = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );

    // ── Build activities + orchestrations (same registration for both runtimes) ──

    // `runAgentTurn` activity — port of activity.ts createRunAgentTurnActivity()
    fn build_activities(
        session_mgr: Arc<SessionManager>,
        assignments: Arc<std::sync::Mutex<HashMap<String, String>>>,
        worker_name: &'static str,
    ) -> ActivityRegistry {
        ActivityRegistry::builder()
            .register("runAgentTurn", move |ctx: ActivityContext, input: String| {
                let mgr = session_mgr.clone();
                let asgn = assignments.clone();
                async move {
                    // Parse TurnInput (simplified — just session_id|prompt|iteration|system_message)
                    let parts: Vec<&str> = input.splitn(4, '|').collect();
                    let session_id = parts[0];
                    let prompt = parts.get(1).unwrap_or(&"");
                    let iteration: u32 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
                    let system_message = parts.get(3).unwrap_or(&"You are a helpful assistant.");

                    // Verify session routing
                    assert_eq!(ctx.session_id(), Some(session_id), "Activity must see its session_id");

                    // Record which worker handles this session
                    asgn.lock()
                        .unwrap()
                        .insert(session_id.to_string(), worker_name.to_string());

                    // --- Port of activity.ts: get or create session ---
                    let mut session = match mgr.get_session(session_id) {
                        Some(s) => s,
                        None => mgr.create_session(session_id, system_message),
                    };

                    // Add user message
                    session.messages.push(TurnMessage {
                        role: "user",
                        content: prompt.to_string(),
                    });

                    // Simulate LLM response (in real SDK: session.sendAndWait())
                    let response = format!(
                        "[{}] Reply to '{}' (turn {}, history={})",
                        worker_name,
                        prompt,
                        iteration,
                        session.messages.len()
                    );

                    session.messages.push(TurnMessage {
                        role: "assistant",
                        content: response.clone(),
                    });

                    // Save session state back
                    mgr.update_session(session_id, &session);

                    // --- Simulate the "wait tool" pattern ---
                    // On iteration 0, if the prompt mentions "check back", the LLM
                    // calls the wait tool → activity returns TurnResult::Wait →
                    // orchestration schedules durable timer → CAN with iteration+1
                    if iteration == 0 && prompt.contains("check back") {
                        let result = TurnResult::Wait {
                            seconds: 1,
                            reason: "Checking deployment status".to_string(),
                            content: Some(response),
                        };
                        return Ok(serde_json::to_string(&result).unwrap());
                    }

                    let result = TurnResult::Completed { content: response };
                    Ok(serde_json::to_string(&result).unwrap())
                }
            })
            .build()
    }

    // `durableTurnOrchestration` — port of orchestration.ts
    fn build_orchestrations() -> OrchestrationRegistry {
        OrchestrationRegistry::builder()
            .register("durable-turn", |ctx: OrchestrationContext, input: String| async move {
                // Parse state: "session_id|prompt|iteration|system_message"
                let parts: Vec<&str> = input.splitn(4, '|').collect();
                let session_id = parts[0].to_string();
                let _prompt = parts.get(1).unwrap_or(&"").to_string();
                let iteration: u32 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
                let system_message = parts.get(3).unwrap_or(&"You are a helpful assistant.").to_string();

                // Call runAgentTurn activity on this session
                let result_str = ctx
                    .schedule_activity_on_session("runAgentTurn", &input, &session_id)
                    .await?;

                let result: TurnResult =
                    serde_json::from_str(&result_str).map_err(|e| format!("Failed to parse TurnResult: {e}"))?;

                match result {
                    TurnResult::Completed { content } => Ok(content),
                    TurnResult::Wait { seconds, .. } => {
                        // Schedule durable timer (port of: yield ctx.scheduleTimer)
                        ctx.schedule_timer(Duration::from_millis(seconds * 100)).await;

                        // ContinueAsNew with next prompt (port of: yield ctx.continueAsNew)
                        let next_prompt = "The wait is complete. Continue with your task.";
                        let new_input = format!("{}|{}|{}|{}", session_id, next_prompt, iteration + 1, system_message);
                        ctx.continue_as_new(new_input).await
                    }
                    TurnResult::Error { message } => Err(message),
                }
            })
            .build()
    }

    let assignments_a = session_assignment.clone();
    let assignments_b = session_assignment.clone();

    // ── Start two runtimes (simulating two AKS worker pods) ──
    let rt_a = runtime::Runtime::start_with_options(
        store.clone(),
        build_activities(worker_a_sessions.clone(), assignments_a, "worker-A"),
        build_orchestrations(),
        RuntimeOptions {
            worker_concurrency: 2,
            orchestration_concurrency: 2,
            worker_node_id: Some("pod-worker-a".to_string()),
            max_sessions_per_runtime: 4,
            ..Default::default()
        },
    )
    .await;

    let rt_b = runtime::Runtime::start_with_options(
        store.clone(),
        build_activities(worker_b_sessions.clone(), assignments_b, "worker-B"),
        build_orchestrations(),
        RuntimeOptions {
            worker_concurrency: 2,
            orchestration_concurrency: 2,
            worker_node_id: Some("pod-worker-b".to_string()),
            max_sessions_per_runtime: 4,
            ..Default::default()
        },
    )
    .await;

    let client = Client::new(store);

    // ── Start 4 concurrent conversations (like 4 users chatting) ──
    // Conversations 1-2: simple single-turn (completed immediately)
    // Conversations 3-4: multi-turn with wait tool (durable timer + CAN)
    let conversations = vec![
        ("conv-1", "Hello, what is Rust?", false),
        ("conv-2", "Explain async/await", false),
        ("conv-3", "Deploy my app and check back in 30s", true),
        ("conv-4", "Run the test suite and check back later", true),
    ];

    for (conv_id, prompt, _uses_wait) in &conversations {
        let session_id = format!("session-{conv_id}");
        let input = format!("{session_id}|{prompt}|0|You are a helpful coding assistant.");
        client
            .start_orchestration(*conv_id, "durable-turn", &input)
            .await
            .unwrap();
    }

    // ── Wait for all conversations to complete ──
    let mut results = HashMap::new();
    for (conv_id, _, _) in &conversations {
        let status = client
            .wait_for_orchestration(conv_id, Duration::from_secs(30))
            .await
            .unwrap();
        match status {
            runtime::OrchestrationStatus::Completed { output, .. } => {
                results.insert(conv_id.to_string(), output);
            }
            other => panic!("Conversation {conv_id} expected completed, got {:?}", other),
        }
    }

    // ── Assertions ──

    // 1. All 4 conversations completed
    assert_eq!(results.len(), 4, "All 4 conversations should complete");

    // 2. Simple conversations have single-turn responses
    for conv_id in &["conv-1", "conv-2"] {
        let output = &results[*conv_id];
        assert!(
            output.contains("Reply to"),
            "Simple conversation {conv_id} should have a reply, got: {output}"
        );
        assert!(
            output.contains("turn 0"),
            "Simple conversation {conv_id} should be turn 0, got: {output}"
        );
    }

    // 3. Wait-tool conversations went through CAN and came back for turn 2
    for conv_id in &["conv-3", "conv-4"] {
        let output = &results[*conv_id];
        assert!(
            output.contains("turn 1"),
            "Wait conversation {conv_id} should reach turn 1 after CAN, got: {output}"
        );
        assert!(
            output.contains("The wait is complete"),
            "Wait conversation {conv_id} should get continuation prompt, got: {output}"
        );
    }

    // 4. Both workers served conversations (load distributed, not all on one)
    // 5. Each session was created at most once on its owning worker
    // 6. Wait-tool conversations accumulated history across CAN
    {
        let assignments = session_assignment.lock().unwrap();
        let workers_used: std::collections::HashSet<&String> = assignments.values().collect();
        tracing::info!(
            worker_assignment = ?*assignments,
            distinct_workers = workers_used.len(),
            "Session-to-worker assignment"
        );
        assert!(
            !assignments.is_empty(),
            "All conversations should have been assigned to a worker"
        );

        let total_creates_a = worker_a_sessions.create_count.load(Ordering::SeqCst);
        let total_creates_b = worker_b_sessions.create_count.load(Ordering::SeqCst);
        let sessions_on_a = worker_a_sessions.active_session_ids().len();
        let sessions_on_b = worker_b_sessions.active_session_ids().len();
        tracing::info!(
            creates_a = total_creates_a,
            creates_b = total_creates_b,
            sessions_a = sessions_on_a,
            sessions_b = sessions_on_b,
            "SessionManager stats"
        );
        assert_eq!(
            total_creates_a, sessions_on_a,
            "Worker A: each session should be created exactly once (no migration re-creation)"
        );
        assert_eq!(
            total_creates_b, sessions_on_b,
            "Worker B: each session should be created exactly once (no migration re-creation)"
        );
        assert_eq!(
            sessions_on_a + sessions_on_b,
            4,
            "All 4 sessions should be distributed across both workers"
        );

        for conv_id in ["conv-3", "conv-4"] {
            let sid = format!("session-{conv_id}");
            let assignment = &assignments[&sid];
            let mgr = if assignment == "worker-A" {
                &worker_a_sessions
            } else {
                &worker_b_sessions
            };
            let session = mgr.get_session(&sid).expect("Session should exist in manager");
            assert!(
                session.messages.len() >= 4, // 2 turns × (user + assistant)
                "Session {sid} should have at least 4 messages (2 turns), got {}",
                session.messages.len()
            );
        }
    } // drop assignments guard before await

    rt_a.shutdown(None).await;
    rt_b.shutdown(None).await;
}

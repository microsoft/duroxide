// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Scenario test: "Chat with your orchestration"
//!
//! Demonstrates a conversational pattern where an external client sends typed
//! messages into an orchestration via `enqueue_event_typed`, the orchestration
//! dequeues them, processes via an activity (simulated LLM), and publishes
//! responses through `set_custom_status`. The client polls with
//! `wait_for_status_change` to receive each reply.
//!
//! Pattern:  UI → enqueue_event("inbox", ChatMessage) → Orchestration
//!           Orchestration → schedule_activity("Generate") → Activity (simulated LLM)
//!           Orchestration → set_custom_status(ChatStatus) → UI polls wait_for_status_change

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{ActivityContext, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::time::Duration;

#[path = "../common/mod.rs"]
mod common;

// === Typed message types ===

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ChatMessage {
    seq: u32,
    text: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ChatStatus {
    state: String,
    last_response: Option<String>,
    msg_seq: u32,
}

/// Poll `wait_for_status_change` until `custom_status` contains a ChatStatus
/// with the given `state` and `msg_seq`. Returns the version when found.
async fn wait_for_chat_state(
    client: &duroxide::Client,
    instance: &str,
    version: &mut u64,
    expected_state: &str,
    expected_seq: u32,
) -> ChatStatus {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status = client
            .wait_for_status_change(
                instance,
                *version,
                Duration::from_millis(50),
                deadline.saturating_duration_since(std::time::Instant::now()),
            )
            .await
            .unwrap_or_else(|e| panic!("Timeout waiting for state={expected_state} seq={expected_seq}: {e:?}"));

        match &status {
            OrchestrationStatus::Running {
                custom_status: Some(s),
                custom_status_version: v,
                ..
            } => {
                *version = *v;
                if let Some(cs) = serde_json::from_str::<ChatStatus>(s)
                    .ok()
                    .filter(|cs| cs.state == expected_state && cs.msg_seq == expected_seq)
                {
                    return cs;
                }
            }
            OrchestrationStatus::Completed { .. } => {
                panic!("Orchestration completed while waiting for state={expected_state} seq={expected_seq}");
            }
            other => {
                panic!("Unexpected status while waiting for state={expected_state}: {other:?}");
            }
        }
    }
}

/// Full multi-turn chat scenario:
///   1. Client sends "Hello!" → orchestration responds "Echo: Hello!"
///   2. Client sends "How are you?" → orchestration responds "Echo: How are you?"
///   3. Client sends "Bye!" → orchestration exits gracefully
///
/// Each turn verifies custom_status carries the response back.
#[tokio::test]
async fn copilot_chat_scenario() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // --- Activity: simulated LLM response generator ---
    let activity_registry = ActivityRegistry::builder()
        .register(
            "GenerateResponse",
            |_ctx: ActivityContext, user_msg: String| async move {
                // Simulate LLM: just echo with a prefix
                Ok(format!("Echo: {user_msg}"))
            },
        )
        .build();

    // --- Orchestration: chat via continue_as_new ---
    //
    // Each execution handles exactly one message, then continues-as-new.
    // This keeps history bounded and demonstrates that custom_status and
    // queued events survive CAN boundaries.
    //
    // We do NOT set "waiting" at the top because CAN is instant — the new
    // execution would overwrite the previous "replied" before the client
    // can observe it. Instead, custom_status is only set to "replied"
    // after processing, and that status persists across the CAN boundary.
    let chat_orch = |ctx: OrchestrationContext, _input: String| async move {
        // Dequeue next message (blocks until available)
        let msg: ChatMessage = ctx.dequeue_event_typed("inbox").await;

        // Call the "LLM" activity
        let response = ctx.schedule_activity("GenerateResponse", &msg.text).await?;

        // Publish reply via custom_status (persists across CAN)
        ctx.set_custom_status(
            serde_json::to_string(&ChatStatus {
                state: "replied".into(),
                last_response: Some(response),
                msg_seq: msg.seq,
            })
            .unwrap()
            .as_str(),
        );

        if msg.text.to_lowercase().contains("bye") {
            return Ok(format!("Chat ended after {} messages", msg.seq));
        }

        // Continue as new — custom_status and queued events carry forward
        ctx.continue_as_new("".to_string()).await
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("ChatBot", chat_orch).build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = duroxide::Client::new(store.clone());

    // Start the chat orchestration
    client.start_orchestration("chat-42", "ChatBot", "").await.unwrap();

    let mut version = 0u64;

    // --- Turn 1: "Hello!" ---
    // Send the message — orchestration is already waiting on dequeue_event
    client
        .enqueue_event_typed(
            "chat-42",
            "inbox",
            &ChatMessage {
                seq: 1,
                text: "Hello!".into(),
            },
        )
        .await
        .unwrap();

    // Wait for the reply (state=replied, seq=1)
    // After replying, the orchestration does continue_as_new → new execution
    // starts and blocks on dequeue_event again
    let cs = wait_for_chat_state(&client, "chat-42", &mut version, "replied", 1).await;
    assert_eq!(cs.last_response.as_deref(), Some("Echo: Hello!"));

    // --- Turn 2: "How are you?" ---
    // New CAN execution is now blocking on dequeue_event.
    // custom_status still shows "replied" from previous execution (persists across CAN).
    client
        .enqueue_event_typed(
            "chat-42",
            "inbox",
            &ChatMessage {
                seq: 2,
                text: "How are you?".into(),
            },
        )
        .await
        .unwrap();

    let cs = wait_for_chat_state(&client, "chat-42", &mut version, "replied", 2).await;
    assert_eq!(cs.last_response.as_deref(), Some("Echo: How are you?"));

    // --- Turn 3: "Bye!" — orchestration completes ---
    client
        .enqueue_event_typed(
            "chat-42",
            "inbox",
            &ChatMessage {
                seq: 3,
                text: "Bye!".into(),
            },
        )
        .await
        .unwrap();

    // Wait for completion
    let final_status = client
        .wait_for_orchestration("chat-42", Duration::from_secs(5))
        .await
        .unwrap();

    match final_status {
        OrchestrationStatus::Completed {
            output, custom_status, ..
        } => {
            assert_eq!(output, "Chat ended after 3 messages");
            // Last custom_status should be the "replied" for "Bye!"
            let cs: ChatStatus = serde_json::from_str(custom_status.as_deref().unwrap()).unwrap();
            assert_eq!(cs.state, "replied");
            assert_eq!(cs.last_response.as_deref(), Some("Echo: Bye!"));
            assert_eq!(cs.msg_seq, 3);
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

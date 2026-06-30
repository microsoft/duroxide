// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Sub-orchestration Instance ID Generation Tests
//!
//! Tests for deterministic child instance ID generation.

use super::helpers::*;
use async_trait::async_trait;
use duroxide::{OrchestrationContext, OrchestrationHandler};
use std::sync::Arc;

/// Auto-generated instance ID should be sub::{event_id}.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     // Instance ID is auto-generated as "sub::{scheduling_event_id}"
///     let result = ctx.schedule_sub_orchestration("Child", "child-input").await?;
///     Ok(result)
/// }
/// ```
#[test]
fn auto_instance_id() {
    let history = vec![started_event(1)]; // OrchestrationStarted
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SubOrchHandler::new("Child", "child-input"));

    assert_continue(&result);

    // Check the action has the right instance ID format
    assert_eq!(engine.pending_actions().len(), 1);
    match &engine.pending_actions()[0] {
        duroxide::Action::StartSubOrchestration {
            instance,
            scheduling_event_id,
            ..
        } => {
            // Instance should be sub::{event_id}
            assert_eq!(instance, &format!("sub::{scheduling_event_id}"));
        }
        _ => panic!("Expected StartSubOrchestration action"),
    }
}

/// Replay uses history's instance ID.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_sub_orchestration("Child", "child-input").await?;
///     Ok(result)  // Returns "child-result" from history
/// }
/// ```
#[test]
fn replay_uses_history_instance() {
    let history = vec![
        started_event(1),                                        // OrchestrationStarted
        sub_orch_scheduled(2, "Child", "sub::2", "child-input"), // Auto-generated instance
        sub_orch_completed(3, 2, "child-result"),                // Sub-orchestration completed
    ];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, SubOrchHandler::new("Child", "child-input"));

    // Should complete using the history's instance ID
    assert_completed(&result, "child-result");
}

/// Explicit instance ID (via schedule_sub_orchestration_with_id) is preserved.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     let result = ctx.schedule_sub_orchestration_with_id(
///         "Child",
///         Some("my-custom-instance"),  // Explicit ID
///         "child-input",
///     ).await?;
///     Ok(result)
/// }
/// ```
#[test]
fn explicit_instance_preserved() {
    struct ExplicitInstanceHandler;

    #[async_trait]
    impl OrchestrationHandler for ExplicitInstanceHandler {
        async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
            let result = ctx
                .schedule_sub_orchestration_with_id("Child", "my-custom-instance", "child-input")
                .await?;
            Ok(result)
        }
    }

    let history = vec![started_event(1)];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, Arc::new(ExplicitInstanceHandler));

    assert_continue(&result);

    // Check the action preserves the explicit instance ID
    match &engine.pending_actions()[0] {
        duroxide::Action::StartSubOrchestration { instance, .. } => {
            assert_eq!(instance, "my-custom-instance", "Explicit instance should be preserved");
        }
        _ => panic!("Expected StartSubOrchestration action"),
    }
}

/// Multiple sub-orchestrations get unique instance IDs.
///
/// Orchestration code:
/// ```ignore
/// async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
///     drop(ctx.schedule_sub_orchestration("Child1", "input1"));  // Gets sub::2
///     drop(ctx.schedule_sub_orchestration("Child2", "input2"));  // Gets sub::3
///     Ok("done".to_string())
/// }
/// ```
#[test]
fn multiple_sub_orchs_unique_ids() {
    struct TwoSubOrchsHandler;

    #[async_trait]
    impl OrchestrationHandler for TwoSubOrchsHandler {
        async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
            // Schedule two sub-orchestrations without awaiting (fire-and-forget)
            drop(ctx.schedule_sub_orchestration("Child1", "input1"));
            drop(ctx.schedule_sub_orchestration("Child2", "input2"));
            Ok("done".to_string())
        }
    }

    let history = vec![started_event(1)];
    let mut engine = create_engine(history);
    let result = execute(&mut engine, Arc::new(TwoSubOrchsHandler));

    assert_completed(&result, "done");

    // Check both actions have different instance IDs
    assert_eq!(engine.pending_actions().len(), 2);

    let instances: Vec<_> = engine
        .pending_actions()
        .iter()
        .filter_map(|a| match a {
            duroxide::Action::StartSubOrchestration { instance, .. } => Some(instance.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(instances.len(), 2);
    assert_ne!(instances[0], instances[1], "Instance IDs should be unique");
    assert!(
        instances[0].starts_with(duroxide::SUB_ORCH_AUTO_PREFIX),
        "Should have auto-generated prefix"
    );
    assert!(
        instances[1].starts_with(duroxide::SUB_ORCH_AUTO_PREFIX),
        "Should have auto-generated prefix"
    );
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Property-based tests using proptest to verify invariants
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::{ActivityRegistry, OrchestrationRegistry};
use duroxide::*;
use proptest::prelude::*;
use std::sync::Arc;

// ============================================================================
// Test Strategy: Generate arbitrary event sequences and verify invariants
// ============================================================================

/// Generate arbitrary orchestration names
fn arb_orch_name() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Za-z][A-Za-z0-9]{0,20}").unwrap()
}

/// Generate arbitrary inputs (simple strings for testing)
fn arb_input() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-z0-9]{0,10}").unwrap()
}

// ============================================================================
// Property 1: Event Ordering Invariants
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    /// Property: Event IDs are monotonically increasing within an execution
    ///
    /// This verifies that event_id sequencing is correct.
    #[test]
    fn prop_event_ids_monotonic(
        orch_name in arb_orch_name(),
        input in arb_input(),
    ) {
        let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
            // Simple orchestration that does one activity
            let activities = ActivityRegistry::builder()
                .register("TestActivity", |_ctx, input: String| async move {
                    Ok(format!("done-{input}"))
                })
                .build();

            let orchestrations = OrchestrationRegistry::builder()
                .register(
                    orch_name.clone(),
                    |ctx: OrchestrationContext, input: String| async move {
                        let _ = ctx.schedule_activity("TestActivity", input.clone()).await;
                        Ok(input)
                    },
                )
                .build();

            let provider = Arc::new(
                providers::sqlite::SqliteProvider::new_in_memory()
                    .await
                    .expect("provider creation")
            );

            let rt = runtime::Runtime::start_with_store(
                provider.clone(),
                activities,
                orchestrations,
            )
            .await;

            let client = Client::new(provider.clone());
            let instance = format!("test-{orch_name}");

            client
                .start_orchestration(&instance, &orch_name, input)
                .await
                .expect("start");

            let _ = client
                .wait_for_orchestration(&instance, std::time::Duration::from_secs(5))
                .await;

            // Trigger shutdown
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            // Get history and verify event_id ordering
            let history = provider
                .read_history(&instance)
                .await
                .expect("get history");

            // Property: Event IDs should be strictly increasing
            let mut prev_id = 0u64;
            for event in &history {
                let event_id = event.event_id();
                if event_id <= prev_id {
                    return Err(format!("Event IDs not monotonic: {event_id} <= {prev_id}"));
                }
                prev_id = event_id;
            }

            // Property: Event IDs should be contiguous (no gaps)
            let expected_ids: Vec<u64> = (1..=history.len() as u64).collect();
            let actual_ids: Vec<u64> = history.iter().map(|e| e.event_id()).collect();
            if actual_ids != expected_ids {
                return Err(format!("Event IDs not contiguous: {actual_ids:?} != {expected_ids:?}"));
            }

            Ok(())
        });

        prop_assert!(result.is_ok(), "{}", result.unwrap_err());
    }
}

// ============================================================================
// Property 2: State Machine Invariants
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    /// Property: An orchestration instance can only reach a terminal state once
    ///
    /// Terminal states: Completed or Failed
    #[test]
    fn prop_single_terminal_state(
        orch_name in arb_orch_name(),
        input in arb_input(),
    ) {
        let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
            // Simple successful orchestration
            let activities = ActivityRegistry::builder().build();

            let orchestrations = OrchestrationRegistry::builder()
                .register(
                    orch_name.clone(),
                    |_ctx: OrchestrationContext, input: String| async move {
                        Ok(input)
                    },
                )
                .build();

            let provider = Arc::new(
                providers::sqlite::SqliteProvider::new_in_memory()
                    .await
                    .expect("provider creation")
            );

            let rt = runtime::Runtime::start_with_store(
                provider.clone(),
                activities,
                orchestrations,
            )
            .await;

            let client = Client::new(provider.clone());
            let instance = format!("test-{orch_name}");

            client
                .start_orchestration(&instance, &orch_name, input)
                .await
                .expect("start");

            let _ = client
                .wait_for_orchestration(&instance, std::time::Duration::from_secs(5))
                .await;

            drop(rt);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            // Get history and count terminal events
            let history = provider
                .read_history(&instance)
                .await
                .expect("get history");

            let terminal_count = history.iter().filter(|e| {
                matches!(
                    &e.kind,
                    EventKind::OrchestrationCompleted { .. }
                        | EventKind::OrchestrationFailed { .. }
                )
            }).count();

            // Property: Should have exactly one terminal event
            if terminal_count != 1 {
                return Err(format!("Expected exactly 1 terminal event, got {terminal_count}"));
            }

            // Property: Terminal event should be the last event
            if let Some(last_event) = history.last() {
                let is_terminal = matches!(
                    &last_event.kind,
                    EventKind::OrchestrationCompleted { .. } | EventKind::OrchestrationFailed { .. }
                );
                if !is_terminal {
                    return Err("Last event should be terminal".to_string());
                }
            }

            Ok(())
        });

        prop_assert!(result.is_ok(), "{}", result.unwrap_err());
    }
}

// ============================================================================
// Property 3: Activity Completion Matching
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    /// Property: Every ActivityScheduled event should have a corresponding completion
    ///
    /// This verifies the activity lifecycle is complete.
    #[test]
    fn prop_activity_lifecycle(
        orch_name in arb_orch_name(),
        input in arb_input(),
        activity_count in 1..4usize,
    ) {
        let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
            // Register multiple activities using method chaining
            let mut activities_builder = ActivityRegistry::builder();
            for i in 0..activity_count {
                let name = format!("Activity{i}");
                activities_builder = activities_builder.register(name.clone(), move |_ctx, input: String| async move {
                    Ok(format!("done-{input}"))
                });
            }
            let activities = activities_builder.build();

            // Orchestration calls all activities
            let activity_count_clone = activity_count;
            let orchestrations = OrchestrationRegistry::builder()
                .register(
                    orch_name.clone(),
                    move |ctx: OrchestrationContext, input: String| async move {
                        for i in 0..activity_count_clone {
                            let name = format!("Activity{i}");
                            let _ = ctx.schedule_activity(&name, input.clone()).await;
                        }
                        Ok(input)
                    },
                )
                .build();

            let provider = Arc::new(
                providers::sqlite::SqliteProvider::new_in_memory()
                    .await
                    .expect("provider creation")
            );

            let rt = runtime::Runtime::start_with_store(
                provider.clone(),
                activities,
                orchestrations,
            )
            .await;

            let client = Client::new(provider.clone());
            let instance = format!("test-{orch_name}");

            client
                .start_orchestration(&instance, &orch_name, input)
                .await
                .expect("start");

            let _ = client
                .wait_for_orchestration(&instance, std::time::Duration::from_secs(10))
                .await;

            drop(rt);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            // Get history
            let history = provider
                .read_history(&instance)
                .await
                .expect("get history");

            // Collect scheduled activity event IDs
            let scheduled_ids: std::collections::HashSet<u64> = history
                .iter()
                .filter_map(|e| match &e.kind {
                    EventKind::ActivityScheduled { .. } => Some(e.event_id),
                    _ => None,
                })
                .collect();

            // Collect completed activity source event IDs
            let completed_source_ids: std::collections::HashSet<u64> = history
                .iter()
                .filter_map(|e| match &e.kind {
                    EventKind::ActivityCompleted { .. }
                    | EventKind::ActivityFailed { .. } => e.source_event_id,
                    _ => None,
                })
                .collect();

            // Property: Every scheduled activity should have a completion
            if scheduled_ids != completed_source_ids {
                return Err(format!("All scheduled activities should have completions: scheduled={scheduled_ids:?}, completed={completed_source_ids:?}"));
            }

            // Property: Counts should match
            if scheduled_ids.len() != activity_count {
                return Err(format!("Activity count mismatch: expected {} got {}", activity_count, scheduled_ids.len()));
            }

            Ok(())
        });

        prop_assert!(result.is_ok(), "{}", result.unwrap_err());
    }
}

// ============================================================================
// Property 4: History Event Count Bounds
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    /// Property: History event count has expected bounds
    ///
    /// For a simple orchestration with N activities, we expect:
    /// - Minimum events: 1 start + N scheduled + N completed + 1 orch completed = 2N + 2
    #[test]
    fn prop_history_event_count_bounds(
        orch_name in arb_orch_name(),
        input in arb_input(),
        activity_count in 0..4usize,
    ) {
        let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
            let mut activities_builder = ActivityRegistry::builder();
            for i in 0..activity_count {
                let name = format!("Activity{i}");
                activities_builder = activities_builder.register(name.clone(), move |_ctx, input: String| async move {
                    Ok(format!("done-{input}"))
                });
            }
            let activities = activities_builder.build();

            let activity_count_clone = activity_count;
            let orchestrations = OrchestrationRegistry::builder()
                .register(
                    orch_name.clone(),
                    move |ctx: OrchestrationContext, input: String| async move {
                        for i in 0..activity_count_clone {
                            let name = format!("Activity{i}");
                            let _ = ctx.schedule_activity(&name, input.clone()).await;
                        }
                        Ok(input)
                    },
                )
                .build();

            let provider = Arc::new(
                providers::sqlite::SqliteProvider::new_in_memory()
                    .await
                    .expect("provider creation")
            );

            let rt = runtime::Runtime::start_with_store(
                provider.clone(),
                activities,
                orchestrations,
            )
            .await;

            let client = Client::new(provider.clone());
            let instance = format!("test-{orch_name}");

            client
                .start_orchestration(&instance, &orch_name, input)
                .await
                .expect("start");

            let _ = client
                .wait_for_orchestration(&instance, std::time::Duration::from_secs(10))
                .await;

            drop(rt);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            let history = provider
                .read_history(&instance)
                .await
                .expect("get history");

            // Property: Minimum event count bound
            // 1 OrchestrationStarted + N ActivityScheduled + N ActivityCompleted + 1 OrchestrationCompleted
            let min_expected = 2 + (activity_count * 2);

            if history.len() < min_expected {
                return Err(format!("History too short: expected at least {} events, got {}", min_expected, history.len()));
            }

            // Property: Should not have excessive events (basic sanity check)
            // Allow some buffer for potential system events
            let max_expected = min_expected + 10;
            if history.len() > max_expected {
                return Err(format!("History too long: expected at most {} events, got {}", max_expected, history.len()));
            }

            Ok(())
        });

        prop_assert!(result.is_ok(), "{}", result.unwrap_err());
    }
}

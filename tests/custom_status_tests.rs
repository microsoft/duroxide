// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Custom status e2e tests
//
// Validates set_custom_status(), reset_custom_status(), and the custom_status
// field on OrchestrationStatus across various scenarios.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

mod common;

use duroxide::providers::Provider;
use duroxide::runtime::{self, OrchestrationStatus, limits, registry::ActivityRegistry};
use duroxide::{ActivityContext, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Basic set / reset
// =============================================================================

/// Orchestration sets a custom status before completing.
/// Verify the status is visible on the completed OrchestrationStatus.
#[tokio::test]
async fn custom_status_set_visible_on_completion() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("SetStatus", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_custom_status("step-1");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("cs-set", "SetStatus", "").await.unwrap();

    let status = client
        .wait_for_orchestration("cs-set", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed {
            output,
            custom_status,
            custom_status_version,
        } => {
            assert_eq!(output, "done");
            assert_eq!(custom_status, Some("step-1".to_string()));
            assert!(custom_status_version >= 1, "version should be >= 1");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Orchestration sets a status then resets it before completing.
/// The final custom_status should be None.
#[tokio::test]
async fn custom_status_reset_clears_to_none() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ResetStatus", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_custom_status("temporary");
            ctx.reset_custom_status();
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("cs-reset", "ResetStatus", "").await.unwrap();

    let status = client
        .wait_for_orchestration("cs-reset", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed {
            custom_status,
            custom_status_version,
            ..
        } => {
            assert_eq!(custom_status, None, "reset should clear to None");
            // Both set + reset increment the version, so version >= 1
            assert!(custom_status_version >= 1);
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Last-write-wins within a turn
// =============================================================================

/// Multiple set_custom_status calls in a single turn — last write wins.
#[tokio::test]
async fn custom_status_last_write_wins() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("LastWrite", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_custom_status("first");
            ctx.set_custom_status("second");
            ctx.set_custom_status("third");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("cs-lww", "LastWrite", "").await.unwrap();

    let status = client
        .wait_for_orchestration("cs-lww", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { custom_status, .. } => {
            assert_eq!(custom_status, Some("third".to_string()));
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Multi-turn persistence
// =============================================================================

/// Custom status set in turn 1 persists across turns.
/// Turn 1: set status + schedule activity. Turn 2: activity completes, orchestration completes.
/// The status should still be visible since it wasn't cleared.
#[tokio::test]
async fn custom_status_persists_across_turns() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("MultiTurn", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_custom_status("processing");
            // This causes a turn boundary — suspends here, resumes in next turn
            let result = ctx.schedule_activity("Echo", "hello").await?;
            // We do NOT call set_custom_status again, so "processing" should persist
            Ok(result)
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("cs-persist", "MultiTurn", "").await.unwrap();

    let status = client
        .wait_for_orchestration("cs-persist", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed {
            output, custom_status, ..
        } => {
            assert_eq!(output, "hello");
            // Status was set in turn 1, not updated in turn 2, so provider keeps it
            assert_eq!(custom_status, Some("processing".to_string()));
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Update across turns
// =============================================================================

/// Custom status updated in turn 2 overrides turn 1's value.
#[tokio::test]
async fn custom_status_updated_in_later_turn() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("UpdateStatus", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_custom_status("step-1");
            ctx.schedule_activity("Echo", "a").await?;
            ctx.set_custom_status("step-2");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("cs-update", "UpdateStatus", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("cs-update", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { custom_status, .. } => {
            assert_eq!(custom_status, Some("step-2".to_string()));
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// No custom status set — default is None
// =============================================================================

/// Orchestration that never calls set_custom_status should have custom_status = None.
#[tokio::test]
async fn custom_status_none_when_not_set() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("NoStatus", |_ctx: OrchestrationContext, _input: String| async move {
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("cs-none", "NoStatus", "").await.unwrap();

    let status = client
        .wait_for_orchestration("cs-none", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed {
            custom_status,
            custom_status_version,
            ..
        } => {
            assert_eq!(custom_status, None);
            assert_eq!(custom_status_version, 0);
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Custom status on failure
// =============================================================================

/// Custom status is still visible when the orchestration fails.
#[tokio::test]
async fn custom_status_visible_on_failure() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "FailWithStatus",
            |ctx: OrchestrationContext, _input: String| async move {
                ctx.set_custom_status("about-to-fail");
                Err("boom".to_string())
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("cs-fail", "FailWithStatus", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("cs-fail", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Failed { custom_status, .. } => {
            assert_eq!(custom_status, Some("about-to-fail".to_string()));
        }
        other => panic!("Expected Failed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Version monotonically increases
// =============================================================================

/// Each set/reset call should increment custom_status_version.
/// Turn 1: set → version 1. Turn 2: set → version 2.
#[tokio::test]
async fn custom_status_version_increments() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("VersionIncr", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_custom_status("v1");
            ctx.schedule_activity("Echo", "a").await?;
            ctx.set_custom_status("v2");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("cs-ver", "VersionIncr", "").await.unwrap();

    let status = client
        .wait_for_orchestration("cs-ver", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed {
            custom_status_version, ..
        } => {
            // At least 2 increments (one per turn that called set_custom_status)
            assert!(
                custom_status_version >= 2,
                "Expected version >= 2, got {custom_status_version}"
            );
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Size limit enforcement
// =============================================================================

/// Custom status exceeding MAX_CUSTOM_STATUS_BYTES fails the orchestration.
#[tokio::test]
async fn custom_status_exceeding_size_limit_fails_orchestration() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "OversizedStatus",
            |ctx: OrchestrationContext, _input: String| async move {
                // Create a string that exceeds the 256KB limit
                let oversized = "x".repeat(limits::MAX_CUSTOM_STATUS_BYTES + 1);
                ctx.set_custom_status(oversized);
                Ok("should-not-reach".to_string())
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("cs-oversize", "OversizedStatus", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("cs-oversize", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            let msg = format!("{details:?}");
            assert!(
                msg.contains("Custom status size"),
                "Expected size limit error, got: {msg}"
            );
            assert!(msg.contains("exceeds limit"), "Expected size limit error, got: {msg}");
        }
        other => panic!("Expected Failed due to size limit, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Custom status exactly at the limit should succeed.
#[tokio::test]
async fn custom_status_at_size_limit_succeeds() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ExactLimit", |ctx: OrchestrationContext, _input: String| async move {
            let exactly_at_limit = "x".repeat(limits::MAX_CUSTOM_STATUS_BYTES);
            ctx.set_custom_status(exactly_at_limit);
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("cs-exact", "ExactLimit", "").await.unwrap();

    let status = client
        .wait_for_orchestration("cs-exact", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Custom status survives continue-as-new
// =============================================================================

/// Custom status set in an earlier execution persists across continue_as_new boundaries.
/// Execution 1: set "step-A" → CAN → Execution 2: verify "step-A" via activity → CAN →
/// Execution 3: set "step-B" → complete.
#[tokio::test]
async fn custom_status_persists_across_continue_as_new() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let store_for_activity = store.clone();
    let activities = ActivityRegistry::builder()
        .register("ReadStatus", move |_ctx: ActivityContext, instance: String| {
            let s = store_for_activity.clone();
            async move {
                // Read the custom status directly from the provider
                let result = s.get_custom_status(&instance, 0).await.unwrap();
                match result {
                    Some((Some(status), version)) => Ok(format!("{status}@v{version}")),
                    Some((None, version)) => Ok(format!("null@v{version}")),
                    None => Ok("none".to_string()),
                }
            }
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("CanStatus", |ctx: OrchestrationContext, input: String| async move {
            let n: u32 = input.parse().unwrap_or(0);
            match n {
                0 => {
                    // Execution 1: set status, then CAN
                    ctx.set_custom_status("step-A");
                    ctx.continue_as_new("1".to_string()).await
                }
                1 => {
                    // Execution 2: verify "step-A" is still visible, then CAN
                    let status_snapshot = ctx
                        .schedule_activity("ReadStatus", "cs-can")
                        .await
                        .expect("ReadStatus activity failed");
                    assert_eq!(status_snapshot, "step-A@v1", "step-A should be visible in execution 2");
                    ctx.continue_as_new("2".to_string()).await
                }
                _ => {
                    // Execution 3: update status and complete
                    ctx.set_custom_status("step-B");
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("cs-can", "CanStatus", "0").await.unwrap();

    let status = client
        .wait_for_orchestration("cs-can", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed {
            output,
            custom_status,
            custom_status_version,
        } => {
            assert_eq!(output, "done");
            // Status was set twice: "step-A" in exec 1, "step-B" in exec 3
            assert_eq!(custom_status, Some("step-B".to_string()));
            assert_eq!(custom_status_version, 2, "two set_custom_status calls total");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Custom status set and then reset (cleared) across CAN.
/// Execution 1: set "foo" → CAN → Execution 2: reset → complete.
#[tokio::test]
async fn custom_status_reset_across_continue_as_new() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("CanReset", |ctx: OrchestrationContext, input: String| async move {
            let n: u32 = input.parse().unwrap_or(0);
            if n == 0 {
                ctx.set_custom_status("foo");
                ctx.continue_as_new("1".to_string()).await
            } else {
                ctx.reset_custom_status();
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("cs-can-reset", "CanReset", "0")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("cs-can-reset", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed {
            custom_status,
            custom_status_version,
            ..
        } => {
            assert_eq!(custom_status, None, "reset should clear even across CAN");
            assert_eq!(custom_status_version, 2, "set + reset = version 2");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// get_custom_status() inside orchestration code
// =============================================================================

/// Verify ctx.get_custom_status() returns the correct value within the orchestration,
/// including across turn boundaries (replay).
#[tokio::test]
async fn custom_status_get_reflects_set_across_turns() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("GetterTest", |ctx: OrchestrationContext, _input: String| async move {
            // Before any set, should be None
            assert_eq!(ctx.get_custom_status(), None, "initial should be None");

            ctx.set_custom_status("step-1");
            assert_eq!(
                ctx.get_custom_status(),
                Some("step-1".to_string()),
                "should reflect set immediately"
            );

            // Cross a turn boundary — schedule_activity suspends
            let _ = ctx.schedule_activity("Echo", "ping").await?;

            // After replay, get_custom_status should still return "step-1"
            assert_eq!(
                ctx.get_custom_status(),
                Some("step-1".to_string()),
                "should survive replay across turns"
            );

            ctx.set_custom_status("step-2");
            assert_eq!(
                ctx.get_custom_status(),
                Some("step-2".to_string()),
                "should reflect second set"
            );

            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("cs-getter", "GetterTest", "").await.unwrap();

    let status = client
        .wait_for_orchestration("cs-getter", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed {
            custom_status,
            custom_status_version,
            ..
        } => {
            assert_eq!(custom_status, Some("step-2".to_string()));
            assert_eq!(custom_status_version, 2);
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Verify ctx.get_custom_status() returns the carried-over value from a previous
/// execution after continue_as_new, including reset and re-set across CAN iterations.
///
/// Execution 1: set "from-first" → CAN
/// Execution 2: verify carry-forward = "from-first", then reset → CAN
/// Execution 3: verify carry-forward = None (was reset), then set "from-third" → CAN
/// Execution 4: verify carry-forward = "from-third", cross turn boundary, verify again → complete
#[tokio::test]
async fn custom_status_get_reflects_carry_forward_after_can() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("CANGetter", |ctx: OrchestrationContext, input: String| async move {
            let iteration: u32 = input.parse().unwrap_or(0);
            match iteration {
                0 => {
                    // Execution 1: set status, CAN
                    assert_eq!(ctx.get_custom_status(), None, "iter 0: initial should be None");
                    ctx.set_custom_status("from-first");
                    assert_eq!(
                        ctx.get_custom_status(),
                        Some("from-first".to_string()),
                        "iter 0: should reflect set"
                    );
                    ctx.continue_as_new("1").await?;
                    Ok("unreachable".to_string())
                }
                1 => {
                    // Execution 2: verify carry-forward, then reset, CAN
                    assert_eq!(
                        ctx.get_custom_status(),
                        Some("from-first".to_string()),
                        "iter 1: should carry forward from iter 0"
                    );
                    ctx.reset_custom_status();
                    assert_eq!(ctx.get_custom_status(), None, "iter 1: reset should clear");
                    ctx.continue_as_new("2").await?;
                    Ok("unreachable".to_string())
                }
                2 => {
                    // Execution 3: verify reset carried forward as None, set new value, CAN
                    assert_eq!(
                        ctx.get_custom_status(),
                        None,
                        "iter 2: reset in iter 1 should carry forward as None"
                    );
                    ctx.set_custom_status("from-third");
                    ctx.continue_as_new("3").await?;
                    Ok("unreachable".to_string())
                }
                _ => {
                    // Execution 4: verify "from-third" carried, cross turn boundary, verify again
                    assert_eq!(
                        ctx.get_custom_status(),
                        Some("from-third".to_string()),
                        "iter 3: should carry forward from iter 2"
                    );

                    let _ = ctx.schedule_activity("Echo", "ping").await?;

                    // After replay, carried value should still be there
                    assert_eq!(
                        ctx.get_custom_status(),
                        Some("from-third".to_string()),
                        "iter 3: carried value should survive replay"
                    );

                    Ok("done".to_string())
                }
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("cs-can-getter", "CANGetter", "0")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("cs-can-getter", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed {
            custom_status,
            custom_status_version,
            ..
        } => {
            // set("from-first") in iter 0, reset in iter 1, set("from-third") in iter 2
            // = 3 mutations total, final value is "from-third"
            assert_eq!(custom_status, Some("from-third".to_string()));
            assert_eq!(custom_status_version, 3);
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// 256KB size limit — precise boundary tests
// =============================================================================

/// Custom status at exactly MAX_CUSTOM_STATUS_BYTES (256KB) succeeds.
/// One byte above the limit fails the orchestration.
/// One byte below the limit succeeds.
#[tokio::test]
async fn custom_status_size_limit_precise_boundary() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();

    // --- One byte below limit: should succeed ---
    let orchestrations = OrchestrationRegistry::builder()
        .register("BelowLimit", |ctx: OrchestrationContext, _input: String| async move {
            let below = "x".repeat(limits::MAX_CUSTOM_STATUS_BYTES - 1);
            ctx.set_custom_status(below);
            Ok("ok".to_string())
        })
        .register("AtLimit", |ctx: OrchestrationContext, _input: String| async move {
            let exact = "x".repeat(limits::MAX_CUSTOM_STATUS_BYTES);
            ctx.set_custom_status(exact);
            Ok("ok".to_string())
        })
        .register("AboveLimit", |ctx: OrchestrationContext, _input: String| async move {
            let above = "x".repeat(limits::MAX_CUSTOM_STATUS_BYTES + 1);
            ctx.set_custom_status(above);
            Ok("should-not-reach".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());

    // Below limit
    client.start_orchestration("cs-below", "BelowLimit", "").await.unwrap();
    let status = client
        .wait_for_orchestration("cs-below", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "ok", "one byte below limit should succeed");
        }
        other => panic!("Expected Completed for below-limit, got: {other:?}"),
    }

    // At limit
    client.start_orchestration("cs-at", "AtLimit", "").await.unwrap();
    let status = client
        .wait_for_orchestration("cs-at", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "ok", "exactly at limit should succeed");
        }
        other => panic!("Expected Completed for at-limit, got: {other:?}"),
    }

    // Above limit
    client.start_orchestration("cs-above", "AboveLimit", "").await.unwrap();
    let status = client
        .wait_for_orchestration("cs-above", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(
                msg.contains("Custom status size") && msg.contains("exceeds limit"),
                "Expected size limit error, got: {msg}"
            );
        }
        other => panic!("Expected Failed for above-limit, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

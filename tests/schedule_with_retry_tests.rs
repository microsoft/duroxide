// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Tests for schedule_activity_with_retry functionality
//!
//! This file contains:
//! - Unit tests for RetryPolicy and BackoffStrategy
//! - Integration tests for retry behavior
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]
//! - Stale event / cross-execution tests
//! - Timeout vs error behavior tests

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, OrchestrationStatus};
use duroxide::{ActivityContext, BackoffStrategy, Client, OrchestrationContext, OrchestrationRegistry, RetryPolicy};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

mod common;

// ============================================================================
// Unit Tests: RetryPolicy Construction
// ============================================================================

#[test]
fn test_retry_policy_default() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.max_attempts, 3);
    assert!(policy.timeout.is_none());
    // Default backoff is exponential
    match policy.backoff {
        BackoffStrategy::Exponential { base, multiplier, max } => {
            assert_eq!(base, Duration::from_millis(100));
            assert!((multiplier - 2.0).abs() < f64::EPSILON);
            assert_eq!(max, Duration::from_secs(30));
        }
        _ => panic!("expected exponential backoff"),
    }
}

#[test]
fn test_retry_policy_new() {
    let policy = RetryPolicy::new(5);
    assert_eq!(policy.max_attempts, 5);
    assert!(policy.timeout.is_none());
}

#[test]
fn test_retry_policy_new_single_attempt() {
    let policy = RetryPolicy::new(1);
    assert_eq!(policy.max_attempts, 1);
}

#[test]
#[should_panic(expected = "max_attempts must be at least 1")]
fn test_retry_policy_new_zero_panics() {
    let _ = RetryPolicy::new(0);
}

#[test]
fn test_retry_policy_builder_with_timeout() {
    let policy = RetryPolicy::new(3).with_timeout(Duration::from_secs(60));
    assert_eq!(policy.max_attempts, 3);
    assert_eq!(policy.timeout, Some(Duration::from_secs(60)));
}

#[test]
fn test_retry_policy_builder_with_backoff() {
    let policy = RetryPolicy::new(3).with_backoff(BackoffStrategy::Fixed {
        delay: Duration::from_secs(1),
    });
    match policy.backoff {
        BackoffStrategy::Fixed { delay } => {
            assert_eq!(delay, Duration::from_secs(1));
        }
        _ => panic!("expected fixed backoff"),
    }
}

#[test]
fn test_retry_policy_builder_chained() {
    let policy = RetryPolicy::new(10)
        .with_timeout(Duration::from_secs(120))
        .with_backoff(BackoffStrategy::Linear {
            base: Duration::from_millis(500),
            max: Duration::from_secs(10),
        });

    assert_eq!(policy.max_attempts, 10);
    assert_eq!(policy.timeout, Some(Duration::from_secs(120)));
    match policy.backoff {
        BackoffStrategy::Linear { base, max } => {
            assert_eq!(base, Duration::from_millis(500));
            assert_eq!(max, Duration::from_secs(10));
        }
        _ => panic!("expected linear backoff"),
    }
}

// ============================================================================
// Unit Tests: BackoffStrategy::None
// ============================================================================

#[test]
fn test_backoff_none_always_zero() {
    let backoff = BackoffStrategy::None;
    assert_eq!(backoff.delay_for_attempt(1), Duration::ZERO);
    assert_eq!(backoff.delay_for_attempt(2), Duration::ZERO);
    assert_eq!(backoff.delay_for_attempt(100), Duration::ZERO);
}

// ============================================================================
// Unit Tests: BackoffStrategy::Fixed
// ============================================================================

#[test]
fn test_backoff_fixed_same_delay() {
    let backoff = BackoffStrategy::Fixed {
        delay: Duration::from_secs(5),
    };
    assert_eq!(backoff.delay_for_attempt(1), Duration::from_secs(5));
    assert_eq!(backoff.delay_for_attempt(2), Duration::from_secs(5));
    assert_eq!(backoff.delay_for_attempt(10), Duration::from_secs(5));
    assert_eq!(backoff.delay_for_attempt(100), Duration::from_secs(5));
}

#[test]
fn test_backoff_fixed_zero_delay() {
    let backoff = BackoffStrategy::Fixed { delay: Duration::ZERO };
    assert_eq!(backoff.delay_for_attempt(1), Duration::ZERO);
    assert_eq!(backoff.delay_for_attempt(5), Duration::ZERO);
}

// ============================================================================
// Unit Tests: BackoffStrategy::Linear
// ============================================================================

#[test]
fn test_backoff_linear_multiplies_by_attempt() {
    let backoff = BackoffStrategy::Linear {
        base: Duration::from_millis(100),
        max: Duration::from_secs(10),
    };
    // delay = base * attempt
    assert_eq!(backoff.delay_for_attempt(1), Duration::from_millis(100));
    assert_eq!(backoff.delay_for_attempt(2), Duration::from_millis(200));
    assert_eq!(backoff.delay_for_attempt(3), Duration::from_millis(300));
    assert_eq!(backoff.delay_for_attempt(10), Duration::from_secs(1));
}

#[test]
fn test_backoff_linear_respects_max() {
    let backoff = BackoffStrategy::Linear {
        base: Duration::from_secs(1),
        max: Duration::from_secs(5),
    };
    assert_eq!(backoff.delay_for_attempt(1), Duration::from_secs(1));
    assert_eq!(backoff.delay_for_attempt(5), Duration::from_secs(5));
    // Beyond max, should cap at max
    assert_eq!(backoff.delay_for_attempt(10), Duration::from_secs(5));
    assert_eq!(backoff.delay_for_attempt(100), Duration::from_secs(5));
}

#[test]
fn test_backoff_linear_zero_base() {
    let backoff = BackoffStrategy::Linear {
        base: Duration::ZERO,
        max: Duration::from_secs(10),
    };
    assert_eq!(backoff.delay_for_attempt(1), Duration::ZERO);
    assert_eq!(backoff.delay_for_attempt(100), Duration::ZERO);
}

// ============================================================================
// Unit Tests: BackoffStrategy::Exponential
// ============================================================================

#[test]
fn test_backoff_exponential_doubles() {
    let backoff = BackoffStrategy::Exponential {
        base: Duration::from_millis(100),
        multiplier: 2.0,
        max: Duration::from_secs(60),
    };
    // delay = base * multiplier^(attempt-1)
    assert_eq!(backoff.delay_for_attempt(1), Duration::from_millis(100)); // 100 * 2^0
    assert_eq!(backoff.delay_for_attempt(2), Duration::from_millis(200)); // 100 * 2^1
    assert_eq!(backoff.delay_for_attempt(3), Duration::from_millis(400)); // 100 * 2^2
    assert_eq!(backoff.delay_for_attempt(4), Duration::from_millis(800)); // 100 * 2^3
}

#[test]
fn test_backoff_exponential_respects_max() {
    let backoff = BackoffStrategy::Exponential {
        base: Duration::from_millis(100),
        multiplier: 2.0,
        max: Duration::from_millis(500),
    };
    assert_eq!(backoff.delay_for_attempt(1), Duration::from_millis(100));
    assert_eq!(backoff.delay_for_attempt(2), Duration::from_millis(200));
    assert_eq!(backoff.delay_for_attempt(3), Duration::from_millis(400));
    // 100 * 2^3 = 800, but capped at 500
    assert_eq!(backoff.delay_for_attempt(4), Duration::from_millis(500));
    assert_eq!(backoff.delay_for_attempt(10), Duration::from_millis(500));
}

#[test]
fn test_backoff_exponential_different_multiplier() {
    let backoff = BackoffStrategy::Exponential {
        base: Duration::from_millis(100),
        multiplier: 3.0,
        max: Duration::from_secs(60),
    };
    assert_eq!(backoff.delay_for_attempt(1), Duration::from_millis(100)); // 100 * 3^0
    assert_eq!(backoff.delay_for_attempt(2), Duration::from_millis(300)); // 100 * 3^1
    assert_eq!(backoff.delay_for_attempt(3), Duration::from_millis(900)); // 100 * 3^2
}

#[test]
fn test_backoff_exponential_multiplier_one() {
    // multiplier = 1.0 means constant delay (like Fixed)
    let backoff = BackoffStrategy::Exponential {
        base: Duration::from_millis(500),
        multiplier: 1.0,
        max: Duration::from_secs(60),
    };
    assert_eq!(backoff.delay_for_attempt(1), Duration::from_millis(500));
    assert_eq!(backoff.delay_for_attempt(5), Duration::from_millis(500));
    assert_eq!(backoff.delay_for_attempt(100), Duration::from_millis(500));
}

#[test]
fn test_backoff_exponential_large_attempt_no_overflow() {
    let backoff = BackoffStrategy::Exponential {
        base: Duration::from_millis(100),
        multiplier: 2.0,
        max: Duration::from_secs(30),
    };
    // Very large attempt number should not panic, just cap at max
    let delay = backoff.delay_for_attempt(1000);
    assert_eq!(delay, Duration::from_secs(30));
}

#[test]
fn test_backoff_exponential_zero_base() {
    let backoff = BackoffStrategy::Exponential {
        base: Duration::ZERO,
        multiplier: 2.0,
        max: Duration::from_secs(30),
    };
    assert_eq!(backoff.delay_for_attempt(1), Duration::ZERO);
    assert_eq!(backoff.delay_for_attempt(100), Duration::ZERO);
}

// ============================================================================
// Unit Tests: RetryPolicy::delay_for_attempt delegation
// ============================================================================

#[test]
fn test_policy_delay_for_attempt_delegates() {
    let policy = RetryPolicy::new(5).with_backoff(BackoffStrategy::Fixed {
        delay: Duration::from_secs(2),
    });
    assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(2));
    assert_eq!(policy.delay_for_attempt(5), Duration::from_secs(2));
}

// ============================================================================
// Unit Tests: Edge Cases
// ============================================================================

#[test]
fn test_large_max_attempts() {
    let policy = RetryPolicy::new(1000);
    assert_eq!(policy.max_attempts, 1000);
}

#[test]
fn test_very_short_timeout() {
    let policy = RetryPolicy::new(3).with_timeout(Duration::from_millis(1));
    assert_eq!(policy.timeout, Some(Duration::from_millis(1)));
}

#[test]
fn test_zero_duration_timeout() {
    let policy = RetryPolicy::new(3).with_timeout(Duration::ZERO);
    assert_eq!(policy.timeout, Some(Duration::ZERO));
}

// ============================================================================
// Integration Tests: Basic Retry Behavior
// ============================================================================

/// Activity succeeds on first attempt - no retry needed
#[tokio::test]
async fn test_activity_succeeds_first_attempt() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("SuccessActivity", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("success:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, input: String| async move {
            ctx.schedule_activity_with_retry("SuccessActivity", input, RetryPolicy::new(3))
                .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-success-1", "RetryOrch", "test")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-success-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "success:test");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Activity fails twice then succeeds on third attempt
#[tokio::test]
async fn test_activity_fails_then_succeeds() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let attempt_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = attempt_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("FlakyActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter_clone.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    Err(format!("fail on attempt {attempt}"))
                } else {
                    Ok(format!("success on attempt {attempt}"))
                }
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry(
                "FlakyActivity",
                "",
                RetryPolicy::new(5).with_backoff(BackoffStrategy::None),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-flaky-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-flaky-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "success on attempt 3");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    assert_eq!(attempt_counter.load(Ordering::SeqCst), 3);
    rt.shutdown(None).await;
}

/// Activity exhausts all attempts and returns final error
#[tokio::test]
async fn test_activity_exhausts_all_attempts() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let attempt_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = attempt_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("AlwaysFailActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter_clone.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                Err::<String, String>(format!("fail on attempt {attempt}"))
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry(
                "AlwaysFailActivity",
                "",
                RetryPolicy::new(3).with_backoff(BackoffStrategy::None),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-exhaust-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-exhaust-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            // The orchestration completes with Ok(Err(...)) which becomes the output
            assert!(
                output.contains("fail on attempt 3"),
                "expected last error, got: {output}"
            );
        }
        OrchestrationStatus::Failed { details, .. } => {
            // Activity error surfaces as orchestration failure
            let msg = details.display_message();
            assert!(msg.contains("fail on attempt 3"), "expected last error, got: {msg}");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    assert_eq!(attempt_counter.load(Ordering::SeqCst), 3);
    rt.shutdown(None).await;
}

/// Single attempt with failure returns error immediately
#[tokio::test]
async fn test_single_attempt_fails() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("FailOnce", |_ctx: ActivityContext, _input: String| async move {
            Err::<String, String>("single failure".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry("FailOnce", "", RetryPolicy::new(1))
                .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-single-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-single-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("single failure"), "expected error, got: {output}");
        }
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(msg.contains("single failure"), "expected error, got: {msg}");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

// ============================================================================
// Integration Tests: Backoff Timing
// ============================================================================

/// Verify retry with fixed backoff creates timer events
#[tokio::test]
async fn test_retry_with_fixed_backoff() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let attempt_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = attempt_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("FlakyActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter_clone.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    Err(format!("fail {attempt}"))
                } else {
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry(
                "FlakyActivity",
                "",
                RetryPolicy::new(5).with_backoff(BackoffStrategy::Fixed {
                    delay: Duration::from_millis(50),
                }),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-backoff-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-backoff-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    // Verify history contains timer events for backoff
    let history = store.read("retry-backoff-1").await.unwrap();
    let timer_created_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::TimerCreated { .. }))
        .count();
    // Should have 2 backoff timers (after attempt 1 and 2, not after attempt 3 which succeeded)
    assert_eq!(
        timer_created_count, 2,
        "expected 2 backoff timers, got {timer_created_count}"
    );

    rt.shutdown(None).await;
}

/// Verify no backoff strategy creates no timer events between attempts
#[tokio::test]
async fn test_retry_with_no_backoff() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let attempt_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = attempt_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("FlakyActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter_clone.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    Err(format!("fail {attempt}"))
                } else {
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry(
                "FlakyActivity",
                "",
                RetryPolicy::new(5).with_backoff(BackoffStrategy::None),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-no-backoff-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-no-backoff-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    // Verify history contains NO timer events (no backoff)
    let history = store.read("retry-no-backoff-1").await.unwrap();
    let timer_created_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::TimerCreated { .. }))
        .count();
    assert_eq!(
        timer_created_count, 0,
        "expected 0 backoff timers, got {timer_created_count}"
    );

    rt.shutdown(None).await;
}

// ============================================================================
// Integration Tests: Timeout Behavior
// ============================================================================

/// Timeout fires before first activity completes
#[tokio::test]
async fn test_timeout_fires_before_success() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("SlowActivity", |_ctx: ActivityContext, _input: String| async move {
            // Sleep longer than timeout
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok("should not reach".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry(
                "SlowActivity",
                "",
                RetryPolicy::new(3).with_timeout(Duration::from_millis(100)),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-timeout-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-timeout-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("timeout"), "expected timeout error, got: {output}");
        }
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(msg.contains("timeout"), "expected timeout error, got: {msg}");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Activity succeeds before timeout fires
#[tokio::test]
async fn test_success_before_timeout() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("FastActivity", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("fast:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, input: String| async move {
            ctx.schedule_activity_with_retry(
                "FastActivity",
                input,
                RetryPolicy::new(3).with_timeout(Duration::from_secs(60)),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-fast-1", "RetryOrch", "data")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-fast-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "fast:data");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Verify that timeout exits immediately WITHOUT retry, even when max_attempts > 1.
/// This test confirms: timeout → immediate exit (no retry), error → retry applies.
#[tokio::test]
async fn test_timeout_exits_immediately_without_retry() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let attempt_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = attempt_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("SlowActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter_clone.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                // Activity takes 200ms - longer than the 50ms timeout
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok("should not reach here".to_string())
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            // 3 max attempts with 150ms timeout - but timeout should prevent ANY retry
            // Using 150ms instead of 50ms to give worker time to fetch activity under load,
            // while still being less than the 200ms activity duration
            ctx.schedule_activity_with_retry(
                "SlowActivity",
                "",
                RetryPolicy::new(3)
                    .with_timeout(Duration::from_millis(150))
                    .with_backoff(BackoffStrategy::None),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("timeout-no-retry-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("timeout-no-retry-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("timeout"), "expected timeout error, got: {output}");
        }
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(msg.contains("timeout"), "expected timeout error, got: {msg}");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    // KEY ASSERTION: Only 1 activity was ever attempted (no retry on timeout)
    assert_eq!(
        attempt_counter.load(Ordering::SeqCst),
        1,
        "expected exactly 1 attempt (no retry on timeout)"
    );

    // Verify history structure
    let history = store.read("timeout-no-retry-1").await.unwrap();

    // Should have exactly 1 ActivityScheduled (no retries)
    let activity_scheduled_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::ActivityScheduled { .. }))
        .count();
    assert_eq!(
        activity_scheduled_count, 1,
        "expected 1 ActivityScheduled (no retry), got {activity_scheduled_count}"
    );

    // Should have exactly 1 TimerCreated (the deadline timer)
    let timer_created_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::TimerCreated { .. }))
        .count();
    assert_eq!(
        timer_created_count, 1,
        "expected 1 TimerCreated (deadline), got {timer_created_count}"
    );

    // Should have exactly 1 TimerFired (deadline won the race)
    let timer_fired_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::TimerFired { .. }))
        .count();
    assert_eq!(
        timer_fired_count, 1,
        "expected 1 TimerFired (deadline won), got {timer_fired_count}"
    );

    rt.shutdown(None).await;
}

/// Verify that errors DO trigger retries while timeouts do NOT.
/// First attempt: error (clearly faster than timeout) → retry
/// Second attempt: timeout (clearly slower than timeout) → exit immediately
#[tokio::test]
async fn test_error_retries_but_timeout_exits() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let attempt_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = attempt_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("MixedActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter_clone.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    // First attempt: fail FAST (no sleep) → clearly beats 500ms timeout → should retry
                    Err("fast failure".to_string())
                } else {
                    // Second attempt: SLOW activity (1s) → clearly loses to 100ms timeout → should NOT retry
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok("should not reach here".to_string())
                }
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry(
                "MixedActivity",
                "",
                RetryPolicy::new(5) // 5 attempts allowed
                    .with_timeout(Duration::from_millis(500)) // 500ms timeout
                    .with_backoff(BackoffStrategy::None),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("error-then-timeout-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("error-then-timeout-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            // Should be timeout error (not the "fast failure" error)
            assert!(output.contains("timeout"), "expected timeout error, got: {output}");
        }
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(msg.contains("timeout"), "expected timeout error, got: {msg}");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    // KEY ASSERTION: History shows 2 activities were scheduled
    // - Attempt 1: scheduled, ran, failed with error → retry triggered
    // - Attempt 2: scheduled, but timeout fired before activity completed
    let history = store.read("error-then-timeout-1").await.unwrap();
    let activity_scheduled_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::ActivityScheduled { .. }))
        .count();
    assert_eq!(
        activity_scheduled_count, 2,
        "expected 2 ActivityScheduled (error triggered retry), got {activity_scheduled_count}"
    );

    // First activity should have failed (with error)
    let activity_failed_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::ActivityFailed { .. }))
        .count();
    assert_eq!(
        activity_failed_count, 1,
        "expected 1 ActivityFailed (first attempt's error), got {activity_failed_count}"
    );

    // Should have at least 2 TimerCreated (one per-attempt timeout)
    let timer_created_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::TimerCreated { .. }))
        .count();
    assert!(
        timer_created_count >= 2,
        "expected at least 2 TimerCreated (one per attempt), got {timer_created_count}"
    );

    rt.shutdown(None).await;
}

// ============================================================================
// Integration Tests: Typed Variant
// ============================================================================

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct TypedInput {
    value: i32,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct TypedOutput {
    result: i32,
}

/// Typed retry variant deserializes result correctly
#[tokio::test]
async fn test_typed_activity_retry_success() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("TypedActivity", |_ctx: ActivityContext, input: String| async move {
            let parsed: TypedInput = serde_json::from_str(&input).map_err(|e| e.to_string())?;
            let output = TypedOutput {
                result: parsed.value * 2,
            };
            Ok(serde_json::to_string(&output).unwrap())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            let input = TypedInput { value: 21 };
            let result: TypedOutput = ctx
                .schedule_activity_with_retry_typed("TypedActivity", &input, RetryPolicy::new(3))
                .await?;
            Ok(serde_json::to_string(&result).unwrap())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-typed-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-typed-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            let result: TypedOutput = serde_json::from_str(&output).unwrap();
            assert_eq!(result, TypedOutput { result: 42 });
        }
        other => panic!("unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

// ============================================================================
// Integration Tests: History Verification
// ============================================================================

/// Verify N attempts create N ActivityScheduled events
#[tokio::test]
async fn test_history_contains_all_activity_scheduled_events() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let attempt_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = attempt_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("FlakyActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter_clone.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 4 {
                    Err(format!("fail {attempt}"))
                } else {
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry(
                "FlakyActivity",
                "",
                RetryPolicy::new(5).with_backoff(BackoffStrategy::None),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-history-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-history-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { .. } => {}
        other => panic!("unexpected status: {other:?}"),
    }

    // Check history has 4 ActivityScheduled events (3 failures + 1 success)
    let history = store.read("retry-history-1").await.unwrap();
    let scheduled_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::ActivityScheduled { .. }))
        .count();
    assert_eq!(scheduled_count, 4, "expected 4 ActivityScheduled events");

    rt.shutdown(None).await;
}

/// Large number of attempts doesn't cause issues
#[tokio::test]
async fn test_large_max_attempts_integration() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let attempt_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = attempt_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("FlakyActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter_clone.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 10 {
                    Err(format!("fail {attempt}"))
                } else {
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry(
                "FlakyActivity",
                "",
                RetryPolicy::new(20).with_backoff(BackoffStrategy::None),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-large-1", "RetryOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-large-1", Duration::from_secs(30))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    assert_eq!(attempt_counter.load(Ordering::SeqCst), 10);
    rt.shutdown(None).await;
}

// ============================================================================
// Stale Event / Replay Tests
// ============================================================================

/// Test replay with stale events in history works correctly
#[tokio::test]
async fn test_replay_with_stale_events_in_history() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let call_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = call_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("CountedActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter_clone.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok("done".to_string())
            }
        })
        .build();

    // Orchestration with retry that will replay from history
    let orchestrations = OrchestrationRegistry::builder()
        .register("ReplayOrch", |ctx: OrchestrationContext, _input: String| async move {
            // This activity will be scheduled with retry
            ctx.schedule_activity_with_retry(
                "CountedActivity",
                "",
                RetryPolicy::new(3).with_backoff(BackoffStrategy::None),
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client.start_orchestration("replay-1", "ReplayOrch", "").await.unwrap();

    match client
        .wait_for_orchestration("replay-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    // Activity should have been called exactly once (no failures, no replays of actual execution)
    assert_eq!(call_counter.load(Ordering::SeqCst), 1);

    rt.shutdown(None).await;
}

/// Test that retry timeout produces correct history and subsequent replay works
#[tokio::test]
async fn test_timeout_history_replays_correctly() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("SlowActivity", |_ctx: ActivityContext, _input: String| async move {
            // This activity is slow but will be cut off by timeout
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok("should not reach".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TimeoutOrch", |ctx: OrchestrationContext, _input: String| async move {
            let result = ctx
                .schedule_activity_with_retry(
                    "SlowActivity",
                    "",
                    RetryPolicy::new(1).with_timeout(Duration::from_millis(50)),
                )
                .await;

            // Return the error as success so we can inspect it
            match result {
                Ok(v) => Ok(v),
                Err(e) => Ok(format!("error:{e}")),
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("timeout-replay-1", "TimeoutOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("timeout-replay-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("timeout"), "expected timeout error, got: {output}");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    // Verify history structure
    let history = store.read("timeout-replay-1").await.unwrap();

    // Should have: OrchestrationStarted, TimerCreated (deadline), ActivityScheduled, TimerFired (deadline won), OrchestrationCompleted
    let timer_created = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::TimerCreated { .. }))
        .count();
    let timer_fired = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::TimerFired { .. }))
        .count();

    assert!(timer_created >= 1, "expected at least 1 TimerCreated");
    assert!(timer_fired >= 1, "expected at least 1 TimerFired (deadline)");

    rt.shutdown(None).await;
}

// ============================================================================
// Continue-As-New Tests
// ============================================================================

/// Test that activity completion from previous execution doesn't affect new execution
#[tokio::test]
async fn test_activity_completes_after_continue_as_new() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let execution_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = execution_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("TrackedActivity", move |_ctx: ActivityContext, input: String| {
            let counter = counter_clone.clone();
            async move {
                let exec = counter.fetch_add(1, Ordering::SeqCst);
                Ok(format!("exec{exec}:{input}"))
            }
        })
        .build();

    // Orchestration that continues as new after first execution
    let orchestrations = OrchestrationRegistry::builder()
        .register("CANOrch", |ctx: OrchestrationContext, input: String| async move {
            let count: i32 = input.parse().unwrap_or(0);

            let result = ctx.schedule_activity("TrackedActivity", &input).await?;

            if count < 2 {
                return ctx.continue_as_new((count + 1).to_string()).await;
            }

            Ok(result)
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client.start_orchestration("can-stale-1", "CANOrch", "0").await.unwrap();

    match client
        .wait_for_orchestration("can-stale-1", Duration::from_secs(15))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            // Final execution should have count=2
            assert!(
                output.contains("2"),
                "expected final execution with count 2, got: {output}"
            );
        }
        other => panic!("unexpected status: {other:?}"),
    }

    // Should have 3 activity executions (0, 1, 2)
    assert_eq!(execution_counter.load(Ordering::SeqCst), 3);

    rt.shutdown(None).await;
}

/// Test that retry with timeout and continue-as-new handles stale events correctly
#[tokio::test]
async fn test_retry_timeout_with_continue_as_new() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let attempt_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = attempt_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("FlakyActivity", move |_ctx: ActivityContext, input: String| {
            let counter = counter_clone.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                // Fail on execution 1, succeed on execution 2+
                let exec: i32 = input.parse().unwrap_or(0);
                if exec == 0 {
                    Err(format!("fail exec0 attempt{attempt}"))
                } else {
                    Ok(format!("success exec{exec} attempt{attempt}"))
                }
            }
        })
        .build();

    // Orchestration: retry with backoff, continue-as-new after failure
    let orchestrations = OrchestrationRegistry::builder()
        .register("RetryCANOrch", |ctx: OrchestrationContext, input: String| async move {
            let exec: i32 = input.parse().unwrap_or(0);

            let result = ctx
                .schedule_activity_with_retry(
                    "FlakyActivity",
                    &input,
                    RetryPolicy::new(2).with_backoff(BackoffStrategy::None),
                )
                .await;

            match result {
                Ok(v) => Ok(v),
                Err(_) if exec < 1 => {
                    // Continue to next execution
                    return ctx.continue_as_new((exec + 1).to_string()).await;
                }
                Err(e) => Err(e),
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-can-1", "RetryCANOrch", "0")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("retry-can-1", Duration::from_secs(15))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            // Should succeed on execution 1
            assert!(output.contains("success"), "expected success, got: {output}");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

// ============================================================================
// Multiple Concurrent Orchestrations
// ============================================================================

/// Test that multiple orchestrations with retry don't interfere with each other
#[tokio::test]
async fn test_multiple_orchestrations_retrying() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let counter1 = Arc::new(AtomicU32::new(0));
    let counter2 = Arc::new(AtomicU32::new(0));
    let c1 = counter1.clone();
    let c2 = counter2.clone();

    let activities = ActivityRegistry::builder()
        .register("Flaky1", move |_ctx: ActivityContext, _input: String| {
            let counter = c1.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 2 {
                    Err(format!("flaky1 fail {attempt}"))
                } else {
                    Ok(format!("flaky1 ok {attempt}"))
                }
            }
        })
        .register("Flaky2", move |_ctx: ActivityContext, _input: String| {
            let counter = c2.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    Err(format!("flaky2 fail {attempt}"))
                } else {
                    Ok(format!("flaky2 ok {attempt}"))
                }
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("Orch1", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry("Flaky1", "", RetryPolicy::new(5).with_backoff(BackoffStrategy::None))
                .await
        })
        .register("Orch2", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity_with_retry("Flaky2", "", RetryPolicy::new(5).with_backoff(BackoffStrategy::None))
                .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    // Start both orchestrations concurrently
    client.start_orchestration("multi-1", "Orch1", "").await.unwrap();
    client.start_orchestration("multi-2", "Orch2", "").await.unwrap();

    // Wait for both
    let result1 = client
        .wait_for_orchestration("multi-1", Duration::from_secs(15))
        .await
        .unwrap();
    let result2 = client
        .wait_for_orchestration("multi-2", Duration::from_secs(15))
        .await
        .unwrap();

    match result1 {
        OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("flaky1 ok"), "expected flaky1 success, got: {output}");
        }
        other => panic!("unexpected status for multi-1: {other:?}"),
    }

    match result2 {
        OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("flaky2 ok"), "expected flaky2 success, got: {output}");
        }
        other => panic!("unexpected status for multi-2: {other:?}"),
    }

    // Verify attempt counts
    assert_eq!(counter1.load(Ordering::SeqCst), 2, "flaky1 should have 2 attempts");
    assert_eq!(counter2.load(Ordering::SeqCst), 3, "flaky2 should have 3 attempts");

    rt.shutdown(None).await;
}

// ============================================================================
// schedule_activity_with_retry_on_session tests
// ============================================================================

/// Basic retry on session: activity fails once then succeeds, all on same session.
#[tokio::test]
async fn retry_on_session_basic_success() {
    let (store, _td) = common::create_sqlite_store_disk().await;
    let counter = Arc::new(AtomicU32::new(0));
    let c = counter.clone();

    let activities = ActivityRegistry::builder()
        .register("FlakySession", move |_ctx: ActivityContext, _input: String| {
            let counter = c.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 2 {
                    Err(format!("fail attempt {attempt}"))
                } else {
                    Ok(format!("ok attempt {attempt}"))
                }
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetrySession", |ctx: OrchestrationContext, _: String| async move {
            let session = ctx.new_guid().await?;
            ctx.schedule_activity_with_retry_on_session(
                "FlakySession",
                "input",
                RetryPolicy::new(3).with_backoff(BackoffStrategy::None),
                &session,
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-sess-1", "RetrySession", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("retry-sess-1", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "ok attempt 2");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    assert_eq!(counter.load(Ordering::SeqCst), 2, "Should have 2 attempts");
    rt.shutdown(None).await;
}

/// All retry attempts fail on session — returns last error.
#[tokio::test]
async fn retry_on_session_all_attempts_fail() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("AlwaysFail", |_ctx: ActivityContext, _input: String| async move {
            Err::<String, String>("nope".into())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("RetrySessionFail", |ctx: OrchestrationContext, _: String| async move {
            let session = ctx.new_guid().await?;
            ctx.schedule_activity_with_retry_on_session(
                "AlwaysFail",
                "",
                RetryPolicy::new(2).with_backoff(BackoffStrategy::None),
                &session,
            )
            .await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("retry-sess-fail", "RetrySessionFail", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("retry-sess-fail", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(msg.contains("nope"), "expected 'nope', got: {msg}");
        }
        other => panic!("Expected Failed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Typed retry on session: serialize input, deserialize output.
#[tokio::test]
async fn retry_on_session_typed_round_trip() {
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct Query {
        sql: String,
    }
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct QueryResult {
        rows: u32,
    }

    let (store, _td) = common::create_sqlite_store_disk().await;
    let counter = Arc::new(AtomicU32::new(0));
    let c = counter.clone();

    let activities = ActivityRegistry::builder()
        .register("TypedSessionAct", move |_ctx: ActivityContext, input: String| {
            let counter = c.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                let _q: Query = serde_json::from_str(&input).map_err(|e| e.to_string())?;
                if attempt < 2 {
                    Err("transient".into())
                } else {
                    Ok(serde_json::to_string(&QueryResult { rows: 42 }).unwrap())
                }
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TypedRetrySession", |ctx: OrchestrationContext, _: String| async move {
            let session = ctx.new_guid().await?;
            let result: QueryResult = ctx
                .schedule_activity_with_retry_on_session_typed(
                    "TypedSessionAct",
                    &Query { sql: "SELECT 1".into() },
                    RetryPolicy::new(3).with_backoff(BackoffStrategy::None),
                    &session,
                )
                .await?;
            Ok(format!("rows={}", result.rows))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("typed-retry-sess", "TypedRetrySession", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("typed-retry-sess", Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "rows=42");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    assert_eq!(counter.load(Ordering::SeqCst), 2);
    rt.shutdown(None).await;
}

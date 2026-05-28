// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Use SQLite via common helper
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, OrchestrationStatus};
use duroxide::{ActivityContext, Either2, EventKind, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

mod common;

/// Tests that select2 resolves based on which completion becomes visible first.
/// In simplified replay, the engine may poll between history events, so the first delivered
/// external completion (history order) can win even if it is not the first branch.
#[tokio::test]
async fn select2_two_externals_first_delivery_wins() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orchestrator = |ctx: OrchestrationContext, _input: String| async move {
        let a = ctx.schedule_wait("A");
        let b = ctx.schedule_wait("B");
        let (idx, out) = ctx.select2(a, b).await.into_tuple();
        match (idx, out) {
            (0, v) => Ok(format!("A:{v}")),
            (1, v) => Ok(format!("B:{v}")),
            _ => unreachable!("select2 should return External outputs here"),
        }
    };

    let acts = ActivityRegistry::builder().build();
    let reg = OrchestrationRegistry::builder()
        .register("ABSelect2", orchestrator)
        .build();
    let rt1 = runtime::Runtime::start_with_store(store.clone(), acts, reg).await;
    let client = duroxide::Client::new(store.clone());

    client.start_orchestration("inst-ab2", "ABSelect2", "").await.unwrap();

    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-ab2",
            |h| {
                let mut seen_a = false;
                let mut seen_b = false;
                for e in h.iter() {
                    if let EventKind::ExternalSubscribed { name } = &e.kind {
                        if name == "A" {
                            seen_a = true;
                        }
                        if name == "B" {
                            seen_b = true;
                        }
                    }
                }
                seen_a && seen_b
            },
            3_000
        )
        .await,
        "timeout waiting for subscriptions"
    );
    rt1.shutdown(None).await;

    let wi_b = duroxide::providers::WorkItem::ExternalRaised {
        instance: "inst-ab2".to_string(),
        name: "B".to_string(),
        data: "vb".to_string(),
    };
    let wi_a = duroxide::providers::WorkItem::ExternalRaised {
        instance: "inst-ab2".to_string(),
        name: "A".to_string(),
        data: "va".to_string(),
    };
    let _ = store.enqueue_for_orchestrator(wi_b, None).await;
    let _ = store.enqueue_for_orchestrator(wi_a, None).await;

    let acts2 = ActivityRegistry::builder().build();
    let reg2 = OrchestrationRegistry::builder()
        .register("ABSelect2", orchestrator)
        .build();
    let rt2 = runtime::Runtime::start_with_store(store.clone(), acts2, reg2).await;

    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-ab2",
            |h| {
                h.iter()
                    .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { .. }))
            },
            5_000
        )
        .await,
        "timeout waiting for completion"
    );
    let hist = store.read("inst-ab2").await.unwrap_or_default();
    let output = match hist.last().map(|e| &e.kind) {
        Some(EventKind::OrchestrationCompleted { output }) => output.clone(),
        _ => String::new(),
    };

    // With batch processing, both events may be in history
    // The key is that select picks the first one in history order
    let b_index = hist
        .iter()
        .position(|e| matches!(&e.kind, EventKind::ExternalEvent { name, .. } if name == "B"));
    let a_index = hist
        .iter()
        .position(|e| matches!(&e.kind, EventKind::ExternalEvent { name, .. } if name == "A"));

    assert!(b_index.is_some(), "expected ExternalEvent B in history: {hist:#?}");

    // Both events should be in history (batch processing enqueued both)
    if let (Some(b_idx), Some(a_idx)) = (b_index, a_index) {
        // History order is B before A because B was enqueued first
        assert!(
            b_idx < a_idx,
            "expected B (idx={b_idx}) to appear before A (idx={a_idx}) in history order: {hist:#?}"
        );
    }

    assert_eq!(
        output, "B:vb",
        "expected B to win since it is delivered first (history order), got {output}"
    );
    rt2.shutdown(None).await;
}

/// Tests that select3 resolves based on history order when multiple completions arrive in the same batch.
/// B is enqueued first, so B appears first in history and wins.
#[tokio::test]
async fn select3_mixed_branch_order_winner() {
    // A (external), T (timer), B (external): enqueue B first, then A; timer much later
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orchestrator = |ctx: OrchestrationContext, _input: String| async move {
        let a = async { Either2::First(ctx.schedule_wait("A").await) };
        let t = async {
            ctx.schedule_timer(Duration::from_millis(500)).await;
            Either2::Second(())
        };
        let b = async { Either2::First(ctx.schedule_wait("B").await) };
        let (idx, out) = ctx.select3(a, t, b).await.into_tuple();
        match (idx, out) {
            (0, Either2::First(v)) => Ok(format!("A:{v}")),
            (1, Either2::Second(_)) => Ok("T".to_string()),
            (2, Either2::First(v)) => Ok(format!("B:{v}")),
            _ => unreachable!(),
        }
    };

    let acts = ActivityRegistry::builder().build();
    let reg = OrchestrationRegistry::builder()
        .register("ATBSelect", orchestrator)
        .build();
    let rt1 = runtime::Runtime::start_with_store(store.clone(), acts, reg).await;
    let client = duroxide::Client::new(store.clone());

    client.start_orchestration("inst-atb", "ATBSelect", "").await.unwrap();
    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-atb",
            |h| {
                let mut seen_a = false;
                let mut seen_b = false;
                for e in h.iter() {
                    if let EventKind::ExternalSubscribed { name } = &e.kind {
                        if name == "A" {
                            seen_a = true;
                        }
                        if name == "B" {
                            seen_b = true;
                        }
                    }
                }
                seen_a && seen_b
            },
            10_000
        )
        .await
    );

    // TIMING-SENSITIVE: Use immediate shutdown (no graceful wait) because:
    // - Timer(500ms) is ticking and will fire during rt2 startup if we delay
    // - Graceful shutdown would add 1000ms delay, virtually guaranteeing timer fires first
    // - Test expects externals to be processed before timer expires
    // - Immediate abort stops timer dispatcher instantly, preventing premature firing
    rt1.shutdown(Some(0)).await;

    let wi_b = duroxide::providers::WorkItem::ExternalRaised {
        instance: "inst-atb".to_string(),
        name: "B".to_string(),
        data: "vb".to_string(),
    };
    let wi_a = duroxide::providers::WorkItem::ExternalRaised {
        instance: "inst-atb".to_string(),
        name: "A".to_string(),
        data: "va".to_string(),
    };
    let _ = store.enqueue_for_orchestrator(wi_b, None).await;
    let _ = store.enqueue_for_orchestrator(wi_a, None).await;

    let acts2 = ActivityRegistry::builder().build();
    let reg2 = OrchestrationRegistry::builder()
        .register("ATBSelect", orchestrator)
        .build();
    let rt2 = runtime::Runtime::start_with_store(store.clone(), acts2, reg2).await;

    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-atb",
            |h| {
                h.iter()
                    .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { .. }))
            },
            5_000
        )
        .await
    );
    let hist = store.read("inst-atb").await.unwrap_or_default();
    let output = match hist.last().map(|e| &e.kind) {
        Some(EventKind::OrchestrationCompleted { output }) => output.clone(),
        _ => String::new(),
    };

    // With batch processing, both events may be in history
    // When both externals are ready before timer, one of them wins (not the timer)
    let b_index = hist
        .iter()
        .position(|e| matches!(&e.kind, EventKind::ExternalEvent { name, .. } if name == "B"));
    let a_index = hist
        .iter()
        .position(|e| matches!(&e.kind, EventKind::ExternalEvent { name, .. } if name == "A"));

    assert!(b_index.is_some(), "expected ExternalEvent B in history: {hist:#?}");

    // History order (B before A) because B was enqueued first
    if let (Some(b_idx), Some(a_idx)) = (b_index, a_index) {
        assert!(
            b_idx < a_idx,
            "expected B (idx={b_idx}) to appear before A (idx={a_idx}) in history order: {hist:#?}"
        );
    }

    // B wins because it appears first in history order (enqueued first)
    assert_eq!(
        output, "B:vb",
        "expected B to win since it is delivered first (history order), got {output}"
    );
    rt2.shutdown(None).await;
}

// Test that join returns results in schedule order (the order futures were passed in).
// This is the intuitive behavior: join(vec![a, b]) returns [result_a, result_b].
#[tokio::test]
async fn join_returns_schedule_order() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Activity that returns its input after a variable delay
    let delay_activity = |_ctx: ActivityContext, input: String| async move {
        let (name, delay_ms): (String, u64) = serde_json::from_str(&input).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
        Ok(name)
    };

    let orchestrator = |ctx: OrchestrationContext, _input: String| async move {
        // Schedule A with longer delay, B with shorter delay
        // Even though B completes first, join should return [A, B] (schedule order)
        let a = ctx.schedule_activity("Delay", r#"["A",100]"#);
        let b = ctx.schedule_activity("Delay", r#"["B",10]"#);
        let outs = ctx.join(vec![a, b]).await;
        // Map outputs to a compact string
        let s: String = outs
            .into_iter()
            .map(|o| o.unwrap_or_else(|e| e))
            .collect::<Vec<_>>()
            .join(",");
        Ok(s)
    };

    let acts = ActivityRegistry::builder().register("Delay", delay_activity).build();
    let reg = OrchestrationRegistry::builder()
        .register("JoinAB", orchestrator)
        .build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, reg).await;
    let client = duroxide::Client::new(store.clone());

    client.start_orchestration("inst-join", "JoinAB", "").await.unwrap();

    let status = client
        .wait_for_orchestration("inst-join", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            // A was scheduled first, B second - join returns in schedule order
            assert_eq!(output, "A,B", "join should return results in schedule order");
        }
        other => panic!("Expected Completed, got {other:?}"),
    }

    rt.shutdown(None).await;
}

// ============================================================================
// select2 Scheduling Event Consumption Tests (Regression)
// ============================================================================
//
// These tests verify the fix for a nondeterminism bug where select2 wouldn't
// consume the loser's scheduling events during replay.
//
// Original Bug: During replay, select2 would return immediately when the winner
// was found, leaving the loser's scheduling event (e.g., TimerCreated) unclaimed.
// When subsequent code tried to schedule new operations, it would see the
// unclaimed event and report a nondeterminism error.
//
// Fix: Modified select/join behavior to ensure deterministic ordering
// two-phase polling: first poll ALL children to ensure they claim their
// scheduling events, then check which one is ready.

/// Regression test: select2 loser's event must be consumed during replay
///
/// Previously, select2 would return immediately when the winner was found,
/// leaving the loser's scheduling event unclaimed. This caused nondeterminism
/// when subsequent code tried to schedule new operations.
///
/// Fixed by polling ALL children before checking for a winner.
#[tokio::test]
async fn test_select2_loser_event_consumed_during_replay() {
    let (store, _td) = common::create_sqlite_store_disk().await;
    let attempt_counter = StdArc::new(AtomicU32::new(0));
    let counter_clone = attempt_counter.clone();

    let activities = ActivityRegistry::builder()
        .register("FastFailActivity", move |_ctx: ActivityContext, _input: String| {
            let counter = counter_clone.clone();
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                // Activity completes FAST with error - beats the 500ms timer
                Err(format!("fast failure on attempt {attempt}"))
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "SelectLoserOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                // ATTEMPT 1: Race activity vs timer
                // Activity will complete fast (with error), timer (500ms) loses
                let timer1 = ctx.schedule_timer(Duration::from_millis(500));
                let activity1 = ctx.schedule_activity("FastFailActivity", "");

                // Activity wins (index 0)
                let first_error = match ctx.select2(activity1, timer1).await {
                    Either2::First(Err(e)) => e,
                    Either2::First(Ok(_)) => return Ok("unexpected success".to_string()),
                    Either2::Second(_) => return Err("timer won unexpectedly".to_string()),
                };

                // ATTEMPT 2: Schedule another activity
                // Previously this would fail with nondeterminism during replay
                // because the timer's scheduling event wasn't consumed
                let activity2 = ctx.schedule_activity("FastFailActivity", "");
                let second_result = activity2.await;

                Ok(format!("first: {first_error}, second: {second_result:?}"))
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("select-loser-1", "SelectLoserOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("select-loser-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            // Should complete successfully now that the bug is fixed
            assert!(
                output.contains("first:"),
                "expected successful completion, got: {output}"
            );
        }
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            panic!("should not fail with nondeterminism anymore: {msg}");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    // Both activities should have been called
    assert_eq!(attempt_counter.load(Ordering::SeqCst), 2);

    // Wait for the loser timer to fire (it's 500ms, so wait a bit)
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Check history: the loser timer's completion event (TimerFired) should be
    // properly handled by the runtime (eaten as stale, not causing any issues)
    let history = store.read("select-loser-1").await.unwrap();

    // There should be exactly 1 TimerCreated (the loser timer from select2)
    let timer_created_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::TimerCreated { .. }))
        .count();
    assert_eq!(timer_created_count, 1, "expected 1 loser timer scheduled");

    // The loser timer's TimerFired event should be present (timer fired after orchestration completed)
    // but since the orchestration already completed, it's a stale event that gets ignored
    let timer_fired_count = history
        .iter()
        .filter(|e| matches!(&e.kind, duroxide::EventKind::TimerFired { .. }))
        .count();
    // The timer fires after orchestration completes, so TimerFired may or may not be in history
    // depending on timing. What matters is: if it's there, the runtime handled it gracefully.
    // Since the orchestration completed successfully, any stale event was properly ignored.
    assert!(
        timer_fired_count <= 1,
        "expected at most 1 timer fired event, got {timer_fired_count}"
    );

    // Verify orchestration completed (not failed due to stale event)
    let completed = history
        .iter()
        .any(|e| matches!(&e.kind, duroxide::EventKind::OrchestrationCompleted { .. }));
    assert!(completed, "orchestration should have completed successfully");

    rt.shutdown(None).await;
}

/// Regression test: simpler variant with explicit schedule after select2
#[tokio::test]
async fn test_select2_schedule_after_winner_returns() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("Instant", |_ctx: ActivityContext, _input: String| async move {
            // Returns instantly
            Ok("done".to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("MinimalOrch", |ctx: OrchestrationContext, _input: String| async move {
            // Race: instant activity vs 1 second timer
            // Activity wins immediately, timer is abandoned
            let timer = ctx.schedule_timer(Duration::from_secs(1));
            let activity = ctx.schedule_activity("Instant", "");

            if !ctx.select2(activity, timer).await.is_first() {
                return Err("timer won unexpectedly".to_string());
            }

            // Now schedule another activity
            // Previously this would fail because the timer's scheduling event
            // wasn't consumed during replay
            let result = ctx.schedule_activity("Instant", "").await?;

            Ok(result)
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("minimal-1", "MinimalOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("minimal-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done");
        }
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            panic!("should not fail with nondeterminism anymore: {msg}");
        }
        other => panic!("unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Simplified Mode Futures Tests
// =============================================================================

/// Test that awaiting B does not block on unawaited A's completion arriving first.
///
/// Scenario:
/// - Schedule activity A (don't await)
/// - Schedule activity B and await it
/// - A completes before B in history
/// - Expectation: B's await should resolve when B completes, not block on A
///
/// This tests that the simplified replay engine correctly handles out-of-order
/// completions relative to await order.
#[tokio::test]
async fn simplified_futures_unawaited_completion_does_not_block() {
    use std::sync::atomic::AtomicUsize;

    static A_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static B_COUNTER: AtomicUsize = AtomicUsize::new(0);
    A_COUNTER.store(0, Ordering::SeqCst);
    B_COUNTER.store(0, Ordering::SeqCst);

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("ActivityA", |_ctx: ActivityContext, input: String| async move {
            A_COUNTER.fetch_add(1, Ordering::SeqCst);
            // A is fast - completes quickly
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            Ok(format!("A:{input}"))
        })
        .register("ActivityB", |_ctx: ActivityContext, input: String| async move {
            B_COUNTER.fetch_add(1, Ordering::SeqCst);
            // B is slower
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Ok(format!("B:{input}"))
        })
        .build();

    // Orchestration: schedule A (don't await), then schedule and await B
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Schedule A but don't await it yet
        let a_future = ctx.schedule_activity("ActivityA", "first");

        // Schedule B and await it immediately
        let b_result = ctx.schedule_activity("ActivityB", "second").await?;

        // Now await A
        let a_result = a_future.await?;

        Ok(format!("B={b_result},A={a_result}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("UnawaitedFirst", orchestration)
        .build();

    let options = runtime::RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 2, // Allow both activities to run concurrently
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("unawaited-first-1", "UnawaitedFirst", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("unawaited-first-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            // Both should complete
            assert!(output.contains("B=B:second"), "Should have B result: {output}");
            assert!(output.contains("A=A:first"), "Should have A result: {output}");

            // Verify both activities ran exactly once
            assert_eq!(A_COUNTER.load(Ordering::SeqCst), 1, "A should run once");
            assert_eq!(B_COUNTER.load(Ordering::SeqCst), 1, "B should run once");
        }
        OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    // Verify history order: A's ActivityScheduled comes before B's, but completion order
    // depends on timing. The key is that the orchestration completed successfully,
    // meaning B's await didn't block on A's completion.
    let hist = store.read("unawaited-first-1").await.unwrap();
    let mut a_scheduled_id = None;
    let mut b_scheduled_id = None;

    for event in &hist {
        if let EventKind::ActivityScheduled { name, .. } = &event.kind {
            if name == "ActivityA" {
                a_scheduled_id = Some(event.event_id);
            } else if name == "ActivityB" {
                b_scheduled_id = Some(event.event_id);
            }
        }
    }

    // A should be scheduled before B (lower event_id)
    assert!(
        a_scheduled_id.unwrap() < b_scheduled_id.unwrap(),
        "A should be scheduled before B: A={a_scheduled_id:?}, B={b_scheduled_id:?}"
    );

    rt.shutdown(None).await;
}

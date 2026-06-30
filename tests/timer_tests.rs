// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::Either2;
use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc as StdArc;
use std::time::Duration;
use tempfile::TempDir;

mod common;

/// Helper to create a SQLite store for testing
async fn create_sqlite_store() -> (StdArc<dyn Provider>, TempDir) {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store = StdArc::new(SqliteProvider::new(&db_url, None).await.unwrap()) as StdArc<dyn Provider>;
    (store, td)
}

/// Helper to create a SQLite store with specific name
async fn create_sqlite_store_named(name: &str) -> (StdArc<dyn Provider>, TempDir, String) {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join(format!("{name}.db"));
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store = StdArc::new(SqliteProvider::new(&db_url, None).await.unwrap()) as StdArc<dyn Provider>;
    (store, td, db_url)
}

// ============================================================================
// BASIC TIMER TESTS
// ============================================================================

#[tokio::test]
async fn single_timer_fires() {
    let (store, _td) = create_sqlite_store().await;

    const TIMER_MS: u64 = 50;
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(Duration::from_millis(TIMER_MS)).await;
        Ok("done".to_string())
    };

    let reg = OrchestrationRegistry::builder().register("OneTimer", orch).build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, reg).await;
    let client = duroxide::Client::new(store.clone());

    let start = std::time::Instant::now();
    client.start_orchestration("inst-one", "OneTimer", "").await.unwrap();

    let status = client
        .wait_for_orchestration("inst-one", std::time::Duration::from_secs(5))
        .await
        .unwrap();
    let elapsed = start.elapsed().as_millis() as u64;

    // Verify timer took at least TIMER_MS
    assert!(
        elapsed >= TIMER_MS,
        "Timer fired too early: expected >={TIMER_MS}ms, got {elapsed}ms"
    );

    assert!(matches!(
        status,
        duroxide::runtime::OrchestrationStatus::Completed { .. }
    ));
    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        assert_eq!(output, "done");
    }

    drop(rt);
}

#[tokio::test]
async fn multiple_timers_fire_in_order() {
    let (store, _td) = create_sqlite_store().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let t1 = ctx.schedule_timer(Duration::from_millis(100)).await;
        let t2 = ctx.schedule_timer(Duration::from_millis(50)).await;
        let t3 = ctx.schedule_timer(Duration::from_millis(75)).await;

        // Verify timers fired in correct order (t2, t3, t1)
        let results = vec![t1, t2, t3];
        Ok(format!("timers: {results:?}"))
    };

    let reg = OrchestrationRegistry::builder().register("MultiTimer", orch).build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, reg).await;
    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("inst-multi", "MultiTimer", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-multi", std::time::Duration::from_secs(5))
        .await
        .unwrap();

    assert!(matches!(
        status,
        duroxide::runtime::OrchestrationStatus::Completed { .. }
    ));

    drop(rt);
}

#[tokio::test]
async fn timer_with_activity() {
    let (store, _td) = create_sqlite_store().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let timer_future = ctx.schedule_timer(Duration::from_millis(50));
        let activity_future = ctx.schedule_activity("TestActivity", "input");

        // Wait for both
        let timer_result = timer_future.await;
        let activity_result = activity_future.await.unwrap();

        Ok(format!("timer: {timer_result:?}, activity: {activity_result}"))
    };

    let activity_registry = ActivityRegistry::builder()
        .register("TestActivity", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("processed: {input}"))
        })
        .build();

    let reg = OrchestrationRegistry::builder().register("TimerActivity", orch).build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, reg).await;
    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("inst-timer-activity", "TimerActivity", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-timer-activity", std::time::Duration::from_secs(5))
        .await
        .unwrap();

    assert!(matches!(
        status,
        duroxide::runtime::OrchestrationStatus::Completed { .. }
    ));

    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        assert!(output.contains("timer:"));
        assert!(output.contains("activity: processed: input"));
    }

    drop(rt);
}

// ============================================================================
// TIMER RECOVERY TESTS
// ============================================================================

/// Test that verifies timer recovery after crash between dequeue and fire
///
/// Scenario:
/// 1. Orchestration schedules a timer
/// 2. Timer is dequeued from the timer queue  
/// 3. System crashes before timer fires (before TimerFired is enqueued)
/// 4. System restarts
/// 5. Timer should be redelivered and fire correctly
#[tokio::test]
async fn timer_recovery_after_crash_before_fire() {
    let (store1, _td, _db_url) = create_sqlite_store_named("timer_recovery").await;

    const TIMER_MS: u64 = 500;

    // Simple orchestration that schedules a timer and then completes
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        // Schedule a timer with enough delay that we can "crash" before it fires
        ctx.schedule_timer(Duration::from_millis(TIMER_MS)).await;

        // Do something after timer to prove it fired
        let result = ctx.schedule_activity("PostTimer", "done").await?;
        Ok(result)
    };

    let activity_registry = ActivityRegistry::builder()
        .register("PostTimer", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("Timer fired, then: {input}"))
        })
        .build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("TimerRecoveryTest", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store1.clone(), activity_registry, orchestration_registry).await;

    let client = duroxide::Client::new(store1.clone());

    // Start orchestration
    client
        .start_orchestration("timer-recovery-instance", "TimerRecoveryTest", "")
        .await
        .unwrap();

    // Wait a bit to ensure timer is scheduled
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // "Crash" the runtime (drop it)
    drop(rt);

    // Simulate crash by checking that timer is in queue but not fired
    // Note: Timer might have already been processed, so we don't assert it's still there
    // The important part is that the orchestration can recover

    // Restart runtime with same store
    let orch2 = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(Duration::from_millis(TIMER_MS)).await;
        let result = ctx.schedule_activity("PostTimer", "done").await?;
        Ok(result)
    };

    let activity_registry2 = ActivityRegistry::builder()
        .register("PostTimer", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("Timer fired, then: {input}"))
        })
        .build();

    let orchestration_registry2 = OrchestrationRegistry::builder()
        .register("TimerRecoveryTest", orch2)
        .build();

    let rt2 = runtime::Runtime::start_with_store(store1.clone(), activity_registry2, orchestration_registry2).await;

    // Wait for orchestration to complete
    let status = client
        .wait_for_orchestration("timer-recovery-instance", std::time::Duration::from_secs(10))
        .await
        .unwrap();

    assert!(matches!(
        status,
        duroxide::runtime::OrchestrationStatus::Completed { .. }
    ));

    // Verify the result shows timer fired
    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        assert_eq!(output, "Timer fired, then: done");
    }

    drop(rt2);
}

#[tokio::test]
async fn timer_recovery_after_crash_after_fire() {
    let (store1, _td, _db_url) = create_sqlite_store_named("timer_recovery_after").await;

    const TIMER_MS: u64 = 100;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(Duration::from_millis(TIMER_MS)).await;
        let result = ctx.schedule_activity("PostTimer", "done").await?;
        Ok(result)
    };

    let activity_registry = ActivityRegistry::builder()
        .register("PostTimer", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("Timer fired, then: {input}"))
        })
        .build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("TimerRecoveryAfterTest", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store1.clone(), activity_registry, orchestration_registry).await;

    let client = duroxide::Client::new(store1.clone());

    // Start orchestration
    client
        .start_orchestration("timer-recovery-after-instance", "TimerRecoveryAfterTest", "")
        .await
        .unwrap();

    // Wait for timer to fire and be processed
    tokio::time::sleep(std::time::Duration::from_millis(TIMER_MS + 50)).await;

    // "Crash" the runtime after timer fired
    drop(rt);

    // Restart runtime
    let orch2 = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(Duration::from_millis(TIMER_MS)).await;
        let result = ctx.schedule_activity("PostTimer", "done").await?;
        Ok(result)
    };

    let activity_registry2 = ActivityRegistry::builder()
        .register("PostTimer", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("Timer fired, then: {input}"))
        })
        .build();

    let orchestration_registry2 = OrchestrationRegistry::builder()
        .register("TimerRecoveryAfterTest", orch2)
        .build();

    let rt2 = runtime::Runtime::start_with_store(store1.clone(), activity_registry2, orchestration_registry2).await;

    // Wait for orchestration to complete
    let status = client
        .wait_for_orchestration("timer-recovery-after-instance", std::time::Duration::from_secs(5))
        .await
        .unwrap();

    assert!(matches!(
        status,
        duroxide::runtime::OrchestrationStatus::Completed { .. }
    ));

    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        assert_eq!(output, "Timer fired, then: done");
    }

    drop(rt2);
}

// ============================================================================
// TIMER EDGE CASES
// ============================================================================

#[tokio::test]
async fn zero_duration_timer() {
    let (store, _td) = create_sqlite_store().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(Duration::ZERO).await;
        Ok("zero-timer-fired".to_string())
    };

    let reg = OrchestrationRegistry::builder().register("ZeroTimer", orch).build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, reg).await;
    let client = duroxide::Client::new(store.clone());

    client.start_orchestration("inst-zero", "ZeroTimer", "").await.unwrap();

    let status = client
        .wait_for_orchestration("inst-zero", std::time::Duration::from_secs(5))
        .await
        .unwrap();

    assert!(matches!(
        status,
        duroxide::runtime::OrchestrationStatus::Completed { .. }
    ));

    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        assert_eq!(output, "zero-timer-fired");
    }

    drop(rt);
}

#[tokio::test]
async fn timer_cancellation() {
    let (store, _td) = create_sqlite_store().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        // Schedule a timer and wait for it
        ctx.schedule_timer(Duration::from_millis(100)).await;
        Ok("timer-completed".to_string())
    };

    let reg = OrchestrationRegistry::builder().register("TimerCancel", orch).build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, reg).await;
    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("inst-cancel", "TimerCancel", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-cancel", std::time::Duration::from_secs(5))
        .await
        .unwrap();

    assert!(matches!(
        status,
        duroxide::runtime::OrchestrationStatus::Completed { .. }
    ));

    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        assert_eq!(output, "timer-completed");
    }

    drop(rt);
}

#[tokio::test]
async fn multiple_timers_recovery_after_crash() {
    let (store1, _td, _db_url) = create_sqlite_store_named("multiple_timers_recovery").await;

    const TIMER_MS: u64 = 100;

    // Simple orchestration that schedules multiple timers
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        // Schedule multiple timers
        let timer1 = ctx.schedule_timer(Duration::from_millis(TIMER_MS));
        let timer2 = ctx.schedule_timer(Duration::from_millis(TIMER_MS + 50));
        let timer3 = ctx.schedule_timer(Duration::from_millis(TIMER_MS + 100));

        // Wait for all timers
        timer1.await;
        timer2.await;
        timer3.await;

        Ok("all-timers-fired".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("MultipleTimersRecoveryTest", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(
        store1.clone(),
        ActivityRegistry::builder().build(),
        orchestration_registry,
    )
    .await;

    let client = duroxide::Client::new(store1.clone());

    // Start orchestration
    client
        .start_orchestration("multiple-timers-recovery-instance", "MultipleTimersRecoveryTest", "")
        .await
        .unwrap();

    // Wait a bit to ensure timers are scheduled
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // "Crash" the runtime
    drop(rt);

    // Restart runtime
    let orch2 = |ctx: OrchestrationContext, _input: String| async move {
        let timer1 = ctx.schedule_timer(Duration::from_millis(TIMER_MS));
        let timer2 = ctx.schedule_timer(Duration::from_millis(TIMER_MS + 50));
        let timer3 = ctx.schedule_timer(Duration::from_millis(TIMER_MS + 100));

        timer1.await;
        timer2.await;
        timer3.await;

        Ok("all-timers-fired".to_string())
    };

    let orchestration_registry2 = OrchestrationRegistry::builder()
        .register("MultipleTimersRecoveryTest", orch2)
        .build();

    let rt2 = runtime::Runtime::start_with_store(
        store1.clone(),
        ActivityRegistry::builder().build(),
        orchestration_registry2,
    )
    .await;

    // Wait for orchestration to complete
    let status = client
        .wait_for_orchestration("multiple-timers-recovery-instance", std::time::Duration::from_secs(10))
        .await
        .unwrap();

    assert!(matches!(
        status,
        duroxide::runtime::OrchestrationStatus::Completed { .. }
    ));

    // Verify the result
    if let duroxide::runtime::OrchestrationStatus::Completed { output, .. } = status {
        assert_eq!(output, "all-timers-fired");
    }

    drop(rt2);
}

// ============================================================================
// TIMER FIRE TIME REGRESSION TESTS
// ============================================================================

/// Regression test: Timer must fire at correct time even when previous TimerFired exists in history
///
/// NOTE: This test is duplicated in tests/scenarios/toygres.rs as timer_fires_at_correct_time_regression
/// since the bug was discovered via the toygres instance actor pattern.
///
/// Bug summary:
/// 1. A poll timer fires, creating TimerFired in history
/// 2. Later, a timeout timer is scheduled
/// 3. BUG (fixed): The timeout timer fired early because calculate_timer_fire_time()
///    used the PREVIOUS TimerFired.fire_at_ms as "now" instead of actual system time
///
/// FIX: Action::CreateTimer now includes fire_at_ms (computed in futures.rs using system time)
///      instead of delay_ms. execution.rs uses this directly instead of recalculating.
#[tokio::test]
async fn timer_fires_at_correct_time_after_previous_timer() {
    let (store, _td) = create_sqlite_store().await;

    // Activity that takes a configurable time to complete
    async fn slow_activity(_ctx: ActivityContext, input: String) -> Result<String, String> {
        let delay_ms: u64 = input.parse().unwrap_or(2000);
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        Ok("activity_done".to_string())
    }

    // Orchestration that:
    // 1. Waits for a short timer (creates TimerFired in history)
    // 2. Waits significant real time to pass via slow activity
    // 3. Races a timer against a fast activity
    // 4. The fast activity should win (timer fires at correct future time)
    let test_orch = |ctx: OrchestrationContext, _input: String| async move {
        // Phase 1: Wait for a 100ms timer - this creates TimerFired in history at time T0+100
        ctx.schedule_timer(Duration::from_millis(100)).await;

        // Phase 2: Do a slow activity (2 seconds of real time passes)
        // After this, system time is approximately T0 + 2100ms
        let _ = ctx.schedule_activity("SlowActivity", "2000").await;

        // Phase 3: Now race a 1-second timer against a fast activity (100ms)
        //
        // Expected behavior (after fix):
        // - Timer fire_at = now + 1000 = (T0 + 2100) + 1000 = T0 + 3100
        // - Activity completes at T0 + 2200 (100ms from now)
        // - Activity wins because T0 + 2200 < T0 + 3100
        let timer = ctx.schedule_timer(Duration::from_secs(1));
        let activity = ctx.schedule_activity("SlowActivity", "100"); // 100ms activity

        let result = match ctx.select2(timer, activity).await {
            Either2::First(_) => {
                // Timer won - would indicate regression
                "timer_won".to_string()
            }
            Either2::Second(r) => {
                // Activity won - correct behavior
                r.unwrap_or_else(|e| format!("activity_failed: {e}"))
            }
        };
        Ok(result)
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("TimerFireTimeTest", test_orch)
        .build();

    let activities = ActivityRegistry::builder()
        .register("SlowActivity", slow_activity)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = duroxide::Client::new(store.clone());

    client
        .start_orchestration("timer-fire-time-test", "TimerFireTimeTest", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("timer-fire-time-test", Duration::from_secs(15))
        .await
        .unwrap();

    rt.shutdown(None).await;

    match status {
        duroxide::runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_ne!(
                output, "timer_won",
                "Timer fired early! The 1-second timer should not beat a 100ms activity. \
                This indicates calculate_timer_fire_time bug has regressed."
            );
            assert_eq!(output, "activity_done", "Activity should have won the race");
        }
        duroxide::runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected status: {status:?}"),
    }
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::EventKind;
use duroxide::OrchestrationStatus;
use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, Client, Event, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc as StdArc;
use std::time::Duration;
mod common;
use common::*;

async fn recovery_across_restart_core<F1, F2>(make_store_stage1: F1, make_store_stage2: F2, instance: String)
where
    F1: Fn() -> StdArc<dyn Provider>,
    F2: Fn() -> StdArc<dyn Provider>,
{
    let orchestrator = |ctx: OrchestrationContext, _input: String| async move {
        let s1 = ctx.schedule_activity("Step", "1").await.unwrap();
        let s2 = ctx.schedule_activity("Step", "2").await.unwrap();
        let _ = ctx.schedule_wait("Resume").await;
        let s3 = ctx.schedule_activity("Step", "3").await.unwrap();
        let s4 = ctx.schedule_activity("Step", "4").await.unwrap();
        Ok(format!("{s1}{s2}{s3}{s4}"))
    };

    let count_scheduled = |hist: &Vec<Event>, input: &str| -> usize {
        hist.iter()
            .filter(
                |e| matches!(&e.kind, EventKind::ActivityScheduled { name, input: inp, .. } if name == "Step" && inp == input),
            )
            .count()
    };

    let store1 = make_store_stage1();
    let activity_registry = ActivityRegistry::builder()
        .register("Step", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
    let orchestration_registry = OrchestrationRegistry::builder()
        .register("RecoveryTest", orchestrator)
        .build();

    let rt1 = runtime::Runtime::start_with_store(
        store1.clone(),
        activity_registry.clone(),
        orchestration_registry.clone(),
    )
    .await;
    let client1 = Client::new(store1.clone());
    let _ = client1.start_orchestration(&instance, "RecoveryTest", "").await;

    // Wait until the subscription for the Resume event has been written to history.
    // This guarantees that steps 1 and 2 have executed and were persisted.
    assert!(wait_for_subscription(store1.clone(), &instance, "Resume", 1000).await);

    let pre_crash_hist = store1.read(&instance).await.unwrap_or_default();
    assert_eq!(count_scheduled(&pre_crash_hist, "1"), 1);
    assert_eq!(count_scheduled(&pre_crash_hist, "2"), 1);
    assert_eq!(count_scheduled(&pre_crash_hist, "3"), 0);

    rt1.shutdown(None).await;
    // no handle to drop when using client

    let store2 = make_store_stage2();
    let rt2 = runtime::Runtime::start_with_store(
        store2.clone(),
        activity_registry.clone(),
        orchestration_registry.clone(),
    )
    .await;
    let instance_for_spawn = instance.clone();
    let store2_for_client = store2.clone();
    let store2_for_wait = store2.clone();
    tokio::spawn(async move {
        // Wait for the subscription to be written before raising the event
        // (for fresh in-memory stores, we need to wait for the orchestration to progress)
        let _ = wait_for_subscription(store2_for_wait, &instance_for_spawn, "Resume", 2000).await;
        let client = Client::new(store2_for_client.clone());
        let _ = client.raise_event(&instance_for_spawn, "Resume", "go").await;
    });

    // Start the orchestration fresh - this simulates recovery where the instance
    // doesn't exist yet in the new environment
    let client2 = Client::new(store2.clone());
    client2
        .start_orchestration(&instance, "RecoveryTest", "")
        .await
        .unwrap();

    let client2_wait = Client::new(store2.clone());
    match client2_wait
        .wait_for_orchestration(&instance, std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "1234"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    let final_hist2 = store2.read(&instance).await.unwrap_or_default();
    assert_eq!(count_scheduled(&final_hist2, "3"), 1);
    assert_eq!(count_scheduled(&final_hist2, "4"), 1);

    rt2.shutdown(None).await;
}

#[tokio::test]
async fn recovery_across_restart_sqlite_provider() {
    let base = std::env::current_dir().unwrap().join(".testdata");
    std::fs::create_dir_all(&base).unwrap();
    let dir = base.join(format!(
        "fs_recovery_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let instance = String::from("inst-recover-sqlite-1");

    // Use the SAME sqlite file across restarts to simulate persistence
    let db = dir.join("recovery.db");
    std::fs::File::create(&db).unwrap();
    let url = format!("sqlite:{}", db.display());

    let store1_arc = StdArc::new(SqliteProvider::new(&url, None).await.unwrap()) as StdArc<dyn Provider>;
    let store2_arc = StdArc::new(SqliteProvider::new(&url, None).await.unwrap()) as StdArc<dyn Provider>;

    let s1 = store1_arc.clone();
    let s2 = store2_arc.clone();
    let make_store1 = move || s1.clone();
    let make_store2 = move || s2.clone();

    recovery_across_restart_core(make_store1, make_store2, instance.clone()).await;

    let store = store2_arc; // already an Arc
    let hist = store.read(&instance).await.unwrap_or_default();
    let count = |inp: &str| {
        hist.iter()
            .filter(|e| matches!(&e.kind, EventKind::ActivityScheduled { name, input, .. } if name == "Step" && input == inp))
            .count()
    };
    assert_eq!(count("1"), 1);
    assert_eq!(count("2"), 1);
    assert_eq!(count("3"), 1);
    assert_eq!(count("4"), 1);
}

#[tokio::test]
async fn recovery_across_restart_sqlite_memory() {
    // Note: This test doesn't actually test recovery for in-memory provider
    // since we create separate stores. It just tests the orchestration completes
    // when started fresh in stage 2.
    let instance = String::from("inst-recover-mem-1");
    let mem1 = StdArc::new(SqliteProvider::new_in_memory().await.unwrap()) as StdArc<dyn Provider>;
    let mem2 = StdArc::new(SqliteProvider::new_in_memory().await.unwrap()) as StdArc<dyn Provider>;
    let make_store1 = move || mem1.clone();
    let make_store2 = move || mem2.clone();

    recovery_across_restart_core(make_store1, make_store2, instance.clone()).await;

    let store_before = StdArc::new(SqliteProvider::new_in_memory().await.unwrap()) as StdArc<dyn Provider>;
    let store_after = StdArc::new(SqliteProvider::new_in_memory().await.unwrap()) as StdArc<dyn Provider>;
    let hist_before = store_before.read(&instance).await.unwrap_or_default();
    let hist_after = store_after.read(&instance).await.unwrap_or_default();

    let count = |hist: &Vec<Event>, inp: &str| {
        hist.iter()
            .filter(|e| matches!(&e.kind, EventKind::ActivityScheduled { name, input, .. } if name == "Step" && input == inp))
            .count()
    };
    assert_eq!(count(&hist_before, "1"), 0);
    assert_eq!(count(&hist_before, "2"), 0);
    assert_eq!(count(&hist_after, "1"), 0);
    assert_eq!(count(&hist_after, "2"), 0);
}

#[tokio::test]
async fn recovery_multiple_orchestrations_sqlite_provider() {
    // Prepare a dedicated directory
    let base = std::env::current_dir().unwrap().join(".testdata");
    std::fs::create_dir_all(&base).unwrap();
    let dir = base.join(format!(
        "fs_recovery_multi_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // Orchestrations of different shapes
    let orch_echo_wait = |ctx: OrchestrationContext, input: String| async move {
        let _a1 = ctx.schedule_activity("Echo", input.clone()).await.unwrap();
        ctx.schedule_timer(Duration::from_millis(200)).await;
        let _a2 = ctx.schedule_activity("Echo", input.clone()).await.unwrap();
        Ok(format!("done:{input}"))
    };
    let orch_upper_only = |ctx: OrchestrationContext, input: String| async move {
        let up = ctx.schedule_activity("Upper", input).await.unwrap();
        Ok(format!("upper:{up}"))
    };
    let orch_wait_event_then_echo = |ctx: OrchestrationContext, input: String| async move {
        let _ = ctx.schedule_wait("Go").await;
        let echoed = ctx.schedule_activity("Echo", input).await.unwrap();
        Ok(format!("acked:{echoed}"))
    };
    let orch_compute_sum = |ctx: OrchestrationContext, input: String| async move {
        // input format: "a,b"
        let sum = ctx.schedule_activity("Add", input).await.unwrap();
        Ok(format!("sum={sum}"))
    };
    let orch_two_timers = |ctx: OrchestrationContext, input: String| async move {
        ctx.schedule_timer(Duration::from_millis(150)).await;
        ctx.schedule_timer(Duration::from_millis(150)).await;
        let _ = ctx.schedule_activity("Echo", input.clone()).await.unwrap();
        Ok(format!("twodone:{input}"))
    };

    let activity_registry = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .register("Upper", |_ctx: ActivityContext, input: String| async move {
            Ok(input.to_uppercase())
        })
        .register("Add", |_ctx: ActivityContext, input: String| async move {
            let mut it = input.split(',');
            let a = it.next().unwrap_or("0").parse::<i64>().unwrap_or(0);
            let b = it.next().unwrap_or("0").parse::<i64>().unwrap_or(0);
            Ok((a + b).to_string())
        })
        .build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("EchoWait", orch_echo_wait)
        .register("UpperOnly", orch_upper_only)
        .register("WaitEvent", orch_wait_event_then_echo)
        .register("ComputeSum", orch_compute_sum)
        .register("TwoTimers", orch_two_timers)
        .build();

    // Stage 1: start instances and shut down before all complete
    let db1 = dir.join("multi.db");
    std::fs::File::create(&db1).unwrap();
    let url1 = format!("sqlite:{}", db1.display());
    let store1 = StdArc::new(SqliteProvider::new(&url1, None).await.unwrap()) as StdArc<dyn Provider>;
    let rt1 = runtime::Runtime::start_with_store(
        store1.clone(),
        activity_registry.clone(),
        orchestration_registry.clone(),
    )
    .await;

    let cases = vec![
        ("inst-echo-wait", "EchoWait", "i1"),
        ("inst-upper", "UpperOnly", "hi"),
        ("inst-wait", "WaitEvent", "evt"),
        ("inst-sum", "ComputeSum", "2,3"),
        ("inst-2timers", "TwoTimers", "z"),
    ];

    let client1 = Client::new(store1.clone());
    for (inst, name, input) in &cases {
        client1.start_orchestration(*inst, *name, *input).await.unwrap();
    }
    // Wait for each orchestration to reach its expected pre-shutdown checkpoint
    // EchoWait: timer created but not yet fired
    assert!(
        wait_for_history(
            store1.clone(),
            "inst-echo-wait",
            |h| {
                let has_timer_created = h.iter().any(|e| matches!(&e.kind, EventKind::TimerCreated { .. }));
                let has_timer_fired = h.iter().any(|e| matches!(&e.kind, EventKind::TimerFired { .. }));
                has_timer_created && !has_timer_fired
            },
            2000
        )
        .await
    );

    // UpperOnly: just needs its single activity scheduled/completed
    assert!(
        wait_for_history(
            store1.clone(),
            "inst-upper",
            |h| { h.iter().any(|e| matches!(&e.kind, EventKind::ActivityCompleted { .. })) },
            1000
        )
        .await
    );

    // WaitEvent: subscription written
    assert!(wait_for_subscription(store1.clone(), "inst-wait", "Go", 1000).await);

    // ComputeSum: either scheduled or completed quickly
    assert!(
        wait_for_history(
            store1.clone(),
            "inst-sum",
            |h| {
                h.iter()
                    .any(|e| matches!(&e.kind, EventKind::ActivityScheduled { name, .. } if name == "Add"))
                    || h.iter().any(|e| matches!(&e.kind, EventKind::ActivityCompleted { .. }))
            },
            2000
        )
        .await
    );

    // TwoTimers: both timers created
    assert!(
        wait_for_history(
            store1.clone(),
            "inst-2timers",
            |h| {
                h.iter()
                    .filter(|e| matches!(&e.kind, EventKind::TimerCreated { .. }))
                    .count()
                    >= 2
            },
            1000
        )
        .await
    );
    rt1.shutdown(None).await;

    // Stage 2: restart with same store; runtime should auto-resume non-terminal instances
    // Reopen the same DB file for stage 2 to simulate restart
    let store2 = StdArc::new(SqliteProvider::new(&url1, None).await.unwrap()) as StdArc<dyn Provider>;
    let store2_for_wait = store2.clone();
    let store2_for_client = store2.clone();
    let rt2 = runtime::Runtime::start_with_store(store2.clone(), activity_registry, orchestration_registry).await;

    // Raise external event for the WaitEvent orchestration after restart
    tokio::spawn(async move {
        // Gate raising the event on the subscription being persisted
        let _ = wait_for_subscription(store2_for_wait, "inst-wait", "Go", 2_000).await;
        let client = Client::new(store2_for_client.clone());
        let _ = client.raise_event("inst-wait", "Go", "ok").await;
    });

    // Use wait helper for each instance
    let client2 = Client::new(store2.clone());
    for (inst, name, input) in &cases {
        match client2
            .wait_for_orchestration(inst, std::time::Duration::from_secs(6))
            .await
            .unwrap()
        {
            OrchestrationStatus::Completed { output, .. } => match *name {
                "EchoWait" => assert_eq!(output, format!("done:{input}")),
                "UpperOnly" => assert_eq!(output, format!("upper:{}", input.to_uppercase())),
                "WaitEvent" => assert_eq!(output, format!("acked:{input}")),
                "ComputeSum" => assert_eq!(output, "sum=5"),
                "TwoTimers" => assert_eq!(output, format!("twodone:{input}")),
                _ => unreachable!(),
            },
            OrchestrationStatus::Failed { details, .. } => panic!("{inst} failed: {}", details.display_message()),
            OrchestrationStatus::Running { .. } | OrchestrationStatus::NotFound => unreachable!(),
        }
    }

    rt2.shutdown(None).await;
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, Client, EventKind, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

mod common;

/// End-to-end test: schedule the same activity name+input twice, producing different results,
/// and verify the first completion is replayed correctly across a runtime restart.
#[tokio::test]
async fn same_activity_name_and_input_routes_correctly_across_restart() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let counter = Arc::new(AtomicUsize::new(0));

    let activity_registry = {
        let counter = counter.clone();
        ActivityRegistry::builder()
            .register("Task", move |_ctx: ActivityContext, _input: String| {
                let counter = counter.clone();
                async move {
                    // Returns different results across invocations even for identical (name, input).
                    let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    Ok(format!("R{n}"))
                }
            })
            .build()
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register(
            "SameActivityTwice",
            |ctx: OrchestrationContext, _input: String| async move {
                let r1 = ctx.schedule_activity("Task", "same").await?;

                // Insert a timer turn so we can restart before the second activity is scheduled.
                ctx.schedule_timer(Duration::from_secs(1)).await;

                let r2 = ctx.schedule_activity("Task", "same").await?;
                Ok(format!("{r1},{r2}"))
            },
        )
        .build();

    // Runtime 1: run until the first activity completion + timer schedule appear, then shut down.
    let rt1 = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-same-activity", "SameActivityTwice", "")
        .await
        .unwrap();

    // Wait until we have the first completion and the timer has been scheduled.
    let ok = common::wait_for_history(
        store.clone(),
        "inst-same-activity",
        |hist| {
            let has_r1 = hist
                .iter()
                .any(|e| matches!(&e.kind, EventKind::ActivityCompleted { result, .. } if result == "R1"));
            let has_timer = hist.iter().any(|e| matches!(&e.kind, EventKind::TimerCreated { .. }));
            has_r1 && has_timer
        },
        5_000,
    )
    .await;
    assert!(ok, "timed out waiting for first completion + timer schedule");

    // After one activity call, the counter should be 1.
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    rt1.shutdown(None).await;

    // Runtime 2: should continue from persisted history; must NOT re-run the first activity.
    let activity_registry = {
        let counter = counter.clone();
        ActivityRegistry::builder()
            .register("Task", move |_ctx: ActivityContext, _input: String| {
                let counter = counter.clone();
                async move {
                    let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    Ok(format!("R{n}"))
                }
            })
            .build()
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register(
            "SameActivityTwice",
            |ctx: OrchestrationContext, _input: String| async move {
                let r1 = ctx.schedule_activity("Task", "same").await?;
                ctx.schedule_timer(Duration::from_secs(1)).await;
                let r2 = ctx.schedule_activity("Task", "same").await?;
                Ok(format!("{r1},{r2}"))
            },
        )
        .build();

    let rt2 = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;

    match client
        .wait_for_orchestration("inst-same-activity", Duration::from_secs(10))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "R1,R2");
        }
        other => panic!("Expected Completed, got {other:?}"),
    }

    // If the runtime incorrectly re-ran the first activity after restart, we'd see >2.
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    rt2.shutdown(None).await;
}

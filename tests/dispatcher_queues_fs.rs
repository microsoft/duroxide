// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use std::sync::Arc as StdArc;
use std::time::Duration;

mod common;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{Event, EventKind, OrchestrationContext, OrchestrationRegistry};

async fn wait_for_history<F>(
    store: StdArc<dyn duroxide::providers::Provider>,
    instance: &str,
    pred: F,
    timeout_ms: u64,
) -> bool
where
    F: Fn(&[Event]) -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    while std::time::Instant::now() < deadline {
        let h = store.read(instance).await.unwrap_or_default();
        if pred(&h) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test]
async fn dispatcher_enqueues_timer_schedule_then_completes() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let store_dyn = store.clone() as StdArc<dyn duroxide::providers::Provider>;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(Duration::from_millis(50)).await;
        Ok("ok".to_string())
    };
    let reg = OrchestrationRegistry::builder().register("OneTimer", orch).build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store_dyn.clone(), acts, reg).await;
    let client = duroxide::Client::new(store_dyn.clone());

    let inst = "inst-disp-timer";
    client.start_orchestration(inst, "OneTimer", "").await.unwrap();

    // Orchestration should complete.
    let ok = wait_for_history(
        store_dyn.clone(),
        inst,
        |h| {
            h.iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "ok"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for completion");

    rt.shutdown(None).await;
}

#[tokio::test]
async fn dispatcher_enqueues_start_orchestration_to_orch_queue() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let store_dyn = store.clone() as StdArc<dyn duroxide::providers::Provider>;

    let acts = ActivityRegistry::builder().build();
    let child = |_: OrchestrationContext, input: String| async move { Ok(input) };
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_orchestration("Child", "W1", "A");
        Ok("scheduled".to_string())
    };
    let reg = OrchestrationRegistry::builder()
        .register("Child", child)
        .register("Parent", parent)
        .build();
    let rt = runtime::Runtime::start_with_store(store_dyn.clone(), acts, reg).await;
    let client = duroxide::Client::new(store_dyn.clone());

    client.start_orchestration("inst-parent", "Parent", "").await.unwrap();

    // Child should complete with input "A".
    let ok = wait_for_history(
        store_dyn.clone(),
        "W1",
        |h| {
            h.iter()
                .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { output, .. } if output == "A"))
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for child completion");

    rt.shutdown(None).await;
}

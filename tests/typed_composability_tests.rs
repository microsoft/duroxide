//! Tests verifying that `_typed()` schedule methods return `DurableFuture` and
//! compose correctly with `ctx.join()`, `ctx.select2()`, and `DurableFuture::map()`.
//!
//! Before this change, `_typed()` methods returned `impl Future` (an opaque type)
//! which could only be `.await`ed sequentially. Now they return concrete
//! `DurableFuture<T>` values that are first-class citizens for fan-out, racing,
//! and cancellation.
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, Client, Either2, OrchestrationContext, OrchestrationRegistry};
use serde::{Deserialize, Serialize};
use std::time::Duration;

mod common;

// ── Shared types ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AddReq {
    a: i32,
    b: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AddRes {
    sum: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MulReq {
    a: i32,
    b: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MulRes {
    product: i32,
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn math_activities() -> duroxide::runtime::registry::Registry<dyn duroxide::runtime::ActivityHandler> {
    ActivityRegistry::builder()
        .register_typed::<AddReq, AddRes, _, _>("Add", |_ctx: ActivityContext, req| async move {
            Ok(AddRes { sum: req.a + req.b })
        })
        .register_typed::<MulReq, MulRes, _, _>("Mul", |_ctx: ActivityContext, req| async move {
            Ok(MulRes {
                product: req.a * req.b,
            })
        })
        .register_typed::<AddReq, AddRes, _, _>("SlowAdd", |_ctx: ActivityContext, req| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(AddRes { sum: req.a + req.b })
        })
        .register_typed::<MulReq, MulRes, _, _>("FailDiv", |_ctx: ActivityContext, req| async move {
            if req.b == 0 {
                Err("division by zero".to_string())
            } else {
                Ok(MulRes {
                    product: req.a / req.b,
                })
            }
        })
        .build()
}

// ── Test 1: typed join (fan-out/fan-in) ────────────────────────────────────

/// `schedule_activity_typed` returns `DurableFuture` that can be collected into
/// a `Vec` and passed to `ctx.join()` for parallel fan-out.
#[tokio::test]
async fn typed_activity_join_fan_out() {
    let (store, _td) = common::create_sqlite_store_disk().await;
    let activities = math_activities();

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let futures = vec![
            ctx.schedule_activity_typed::<AddReq, AddRes>("Add", &AddReq { a: 1, b: 2 }),
            ctx.schedule_activity_typed::<AddReq, AddRes>("Add", &AddReq { a: 10, b: 20 }),
            ctx.schedule_activity_typed::<AddReq, AddRes>("Add", &AddReq { a: 100, b: 200 }),
        ];
        let results: Vec<Result<AddRes, String>> = ctx.join(futures).await;
        let total: i32 = results.into_iter().map(|r| r.unwrap().sum).sum();
        Ok(total.to_string())
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("TypedJoin", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store);
    client.start_orchestration("tj-1", "TypedJoin", "").await.unwrap();

    let status = client.wait_for_orchestration("tj-1", Duration::from_secs(5)).await.unwrap();
    match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "333"); // 3 + 30 + 300
        }
        other => panic!("Expected completed, got {other:?}"),
    }
    rt.shutdown(None).await;
}

// ── Test 2: typed join2 (heterogeneous) ────────────────────────────────────

/// `join2` with two different typed activities (Add + Mul) running in parallel.
#[tokio::test]
async fn typed_activity_join2_heterogeneous() {
    let (store, _td) = common::create_sqlite_store_disk().await;
    let activities = math_activities();

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let (add_res, mul_res) = ctx
            .join2(
                ctx.schedule_activity_typed::<AddReq, AddRes>("Add", &AddReq { a: 5, b: 3 }),
                ctx.schedule_activity_typed::<MulReq, MulRes>("Mul", &MulReq { a: 5, b: 3 }),
            )
            .await;
        let sum = add_res?.sum;
        let product = mul_res?.product;
        Ok(format!("sum={sum},product={product}"))
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("TypedJoin2", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store);
    client.start_orchestration("tj2-1", "TypedJoin2", "").await.unwrap();

    let status = client.wait_for_orchestration("tj2-1", Duration::from_secs(5)).await.unwrap();
    match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "sum=8,product=15");
        }
        other => panic!("Expected completed, got {other:?}"),
    }
    rt.shutdown(None).await;
}

// ── Test 3: typed select2 (timeout racing) ─────────────────────────────────

/// Typed activity composed with `ctx.select2()` against a timer for timeout.
#[tokio::test]
async fn typed_activity_select2_timeout() {
    let (store, _td) = common::create_sqlite_store_disk().await;
    let activities = math_activities();

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let slow = ctx.schedule_activity_typed::<AddReq, AddRes>("SlowAdd", &AddReq { a: 1, b: 2 });
        let timeout = ctx.schedule_timer(Duration::from_millis(10));

        match ctx.select2(slow, timeout).await {
            Either2::First(result) => Ok(format!("completed:{}", result?.sum)),
            Either2::Second(()) => Ok("timeout".to_string()),
        }
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("TypedSelect2", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store);
    client.start_orchestration("ts2-1", "TypedSelect2", "").await.unwrap();

    let status = client.wait_for_orchestration("ts2-1", Duration::from_secs(5)).await.unwrap();
    match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            // Either outcome valid depending on timing
            assert!(
                output == "timeout" || output.starts_with("completed:"),
                "Unexpected output: {output}"
            );
        }
        other => panic!("Expected completed, got {other:?}"),
    }
    rt.shutdown(None).await;
}

// ── Test 4: DurableFuture::map combinator ──────────────────────────────────

/// `DurableFuture::map` transforms the result while preserving composability.
#[tokio::test]
async fn durable_future_map_transforms_result() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("Double", |_ctx: ActivityContext, input: String| async move {
            let n: i32 = input.parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
            Ok((n * 2).to_string())
        })
        .build();

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // .map() to post-process inline
        let result = ctx
            .schedule_activity("Double", "21")
            .map(|r| r.map(|s| {
                let n: i32 = s.parse().unwrap();
                (n + 1).to_string()
            }))
            .await?;
        Ok(result) // "43"
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("MapCombo", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store);
    client.start_orchestration("mc-1", "MapCombo", "").await.unwrap();

    let status = client.wait_for_orchestration("mc-1", Duration::from_secs(5)).await.unwrap();
    match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "43"),
        other => panic!("Expected completed, got {other:?}"),
    }
    rt.shutdown(None).await;
}

// ── Test 5: map on typed + join ────────────────────────────────────────────

/// Chain `.map()` on typed results then collect with `join`.
#[tokio::test]
async fn typed_map_then_join() {
    let (store, _td) = common::create_sqlite_store_disk().await;
    let activities = math_activities();

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // Map typed results to extract just the value as i32
        let f1 = ctx
            .schedule_activity_typed::<AddReq, AddRes>("Add", &AddReq { a: 1, b: 2 })
            .map(|r| r.map(|res| res.sum));
        let f2 = ctx
            .schedule_activity_typed::<AddReq, AddRes>("Add", &AddReq { a: 10, b: 20 })
            .map(|r| r.map(|res| res.sum));

        let results: Vec<Result<i32, String>> = ctx.join(vec![f1, f2]).await;
        let total: i32 = results.into_iter().map(|r| r.unwrap()).sum();
        Ok(total.to_string())
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("TypedMapJoin", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store);
    client.start_orchestration("tmj-1", "TypedMapJoin", "").await.unwrap();

    let status = client.wait_for_orchestration("tmj-1", Duration::from_secs(5)).await.unwrap();
    match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "33"),
        other => panic!("Expected completed, got {other:?}"),
    }
    rt.shutdown(None).await;
}

// ── Test 6: map preserves cancellation on drop ─────────────────────────────

/// A mapped `DurableFuture` still cancels when dropped (e.g., select2 loser).
#[tokio::test]
async fn map_preserves_cancellation_on_drop() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("SlowWork", |_ctx: ActivityContext, _: String| async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok("done".to_string())
        })
        .build();

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let mapped = ctx
            .schedule_activity("SlowWork", "")
            .map(|r| r.map(|s| format!("mapped:{s}")));
        let timeout = ctx.schedule_timer(Duration::from_millis(50));

        match ctx.select2(mapped, timeout).await {
            Either2::First(result) => Ok(format!("work:{}", result?)),
            Either2::Second(()) => Ok("cancelled".to_string()),
        }
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("MapCancel", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store);
    client.start_orchestration("mcan-1", "MapCancel", "").await.unwrap();

    let status = client.wait_for_orchestration("mcan-1", Duration::from_secs(5)).await.unwrap();
    match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "cancelled"),
        other => panic!("Expected completed, got {other:?}"),
    }
    rt.shutdown(None).await;
}

// ── Test 7: typed error propagation through join ───────────────────────────

/// Errors from typed activities propagate correctly through join.
#[tokio::test]
async fn typed_error_propagation_through_join() {
    let (store, _td) = common::create_sqlite_store_disk().await;
    let activities = math_activities();

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let futures = vec![
            ctx.schedule_activity_typed::<MulReq, MulRes>("FailDiv", &MulReq { a: 10, b: 2 }),
            ctx.schedule_activity_typed::<MulReq, MulRes>("FailDiv", &MulReq { a: 10, b: 0 }), // will fail
        ];
        let results: Vec<Result<MulRes, String>> = ctx.join(futures).await;

        // First succeeds, second fails
        let first = results[0].as_ref().map(|r| r.product).unwrap();
        let second_err = results[1].as_ref().unwrap_err().clone();
        Ok(format!("ok={first},err={second_err}"))
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("TypedErrJoin", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store);
    client.start_orchestration("tej-1", "TypedErrJoin", "").await.unwrap();

    let status = client.wait_for_orchestration("tej-1", Duration::from_secs(5)).await.unwrap();
    match status {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "ok=5,err=division by zero");
        }
        other => panic!("Expected completed, got {other:?}"),
    }
    rt.shutdown(None).await;
}

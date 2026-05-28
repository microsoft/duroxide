// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Async block pattern tests for simplified replay mode.
//!
//! These tests validate complex async block patterns including:
//! - Joining multiple async blocks with control flow
//! - Racing async blocks with select2/select3
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]
//! - Mixing async blocks with durable futures
//! - Sub-orchestration patterns within async blocks

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};

mod common;

// =============================================================================
// Join Patterns
// =============================================================================

/// Join async blocks containing multiple durable futures with control flow
#[tokio::test]
async fn async_block_join_with_control_flow() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Step", |_ctx: ActivityContext, input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            Ok(format!("step:{input}"))
        })
        .register("Check", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("check:{input}"))
        })
        .build();

    // Orchestration with async blocks containing control flow and multiple activities
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Block A: sequential activities with conditional logic
        let block_a = async {
            let first = ctx.schedule_activity("Step", "A1").await?;
            if first.contains("step") {
                let second = ctx.schedule_activity("Step", "A2").await?;
                Ok::<_, String>(format!("A:[{first},{second}]"))
            } else {
                Ok("A:fallback".to_string())
            }
        };

        // Block B: different control flow pattern
        let block_b = async {
            let check = ctx.schedule_activity("Check", "B1").await?;
            let mut results = vec![check];
            for i in 2..=3 {
                let step = ctx.schedule_activity("Step", format!("B{i}")).await?;
                results.push(step);
            }
            Ok::<_, String>(format!("B:[{}]", results.join(",")))
        };

        // Block C: timer + activity
        let block_c = async {
            ctx.schedule_timer(std::time::Duration::from_millis(5)).await;
            let result = ctx.schedule_activity("Step", "C1").await?;
            Ok::<_, String>(format!("C:[timer,{result}]"))
        };

        // Join all blocks
        let (a, b, c) = ctx.join3(block_a, block_b, block_c).await;

        Ok(format!("{},{},{}", a?, b?, c?))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("JoinBlocks", orchestration)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 4,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("join-blocks-1", "JoinBlocks", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("join-blocks-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert!(
                output.contains("A:[step:A1,step:A2]"),
                "Block A result incorrect: {output}"
            );
            assert!(
                output.contains("B:[check:B1,step:B2,step:B3]"),
                "Block B result incorrect: {output}"
            );
            assert!(
                output.contains("C:[timer,step:C1]"),
                "Block C result incorrect: {output}"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Many async blocks joined with different completion times
#[tokio::test]
async fn async_block_join_many() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Work", |_ctx: ActivityContext, input: String| async move {
            // Variable delay based on input
            let delay: u64 = input.parse().unwrap_or(10);
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            Ok(format!("done:{input}"))
        })
        .build();

    // Orchestration with many parallel blocks
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Create 5 async blocks with different timing patterns
        let blocks: Vec<_> = (0..5)
            .map(|i| {
                let ctx = ctx.clone();
                async move {
                    let delay = (5 - i) * 5; // Block 0 is slowest, block 4 is fastest
                    let result = ctx.schedule_activity("Work", delay.to_string()).await?;
                    Ok::<_, String>(format!("block{i}:{result}"))
                }
            })
            .collect();

        // Join all blocks
        let results = ctx.join(blocks).await;

        // Collect results preserving order
        let mut outputs = Vec::new();
        for (i, r) in results.into_iter().enumerate() {
            match r {
                Ok(s) => outputs.push(s),
                Err(e) => outputs.push(format!("block{i}:error:{e}")),
            }
        }

        Ok(outputs.join(","))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("JoinMany", orchestration)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 5,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client.start_orchestration("join-many-1", "JoinMany", "").await.unwrap();

    match client
        .wait_for_orchestration("join-many-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // All blocks should complete, order preserved (block0, block1, ...)
            for i in 0..5 {
                let delay = (5 - i) * 5;
                let expected = format!("block{i}:done:{delay}");
                assert!(output.contains(&expected), "Missing block {i} result: {output}");
            }
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Sequential async blocks with handoff
#[tokio::test]
async fn async_block_sequential() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Process", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("processed:{input}"))
        })
        .build();

    // Orchestration: async blocks executed sequentially, each using output of previous
    let orchestration = |ctx: OrchestrationContext, input: String| async move {
        // Phase 1: initial processing
        let phase1 = async {
            let a = ctx.schedule_activity("Process", input).await?;
            let b = ctx.schedule_activity("Process", "extra").await?;
            Ok::<_, String>(format!("{a}+{b}"))
        };
        let phase1_result = phase1.await?;

        // Phase 2: uses phase1 result
        let phase2 = async {
            let result = ctx.schedule_activity("Process", phase1_result).await?;
            Ok::<_, String>(result)
        };
        let phase2_result = phase2.await?;

        // Phase 3: final wrap-up
        let phase3 = async {
            ctx.schedule_timer(std::time::Duration::from_millis(5)).await;
            let final_result = ctx.schedule_activity("Process", phase2_result).await?;
            Ok::<_, String>(format!("final:{final_result}"))
        };

        phase3.await
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("SequentialBlocks", orchestration)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("seq-blocks-1", "SequentialBlocks", "start")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("seq-blocks-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Check the nested processing happened
            assert!(output.starts_with("final:"), "Should have final prefix: {output}");
            assert!(output.contains("processed:"), "Should contain processed: {output}");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Select/Race Patterns
// =============================================================================

/// Select2 racing async blocks - first block to complete wins
#[tokio::test]
async fn async_block_select_racing() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Fast", |_ctx: ActivityContext, input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok(format!("fast:{input}"))
        })
        .register("Slow", |_ctx: ActivityContext, input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Ok(format!("slow:{input}"))
        })
        .build();

    // Orchestration racing two async blocks
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Fast block: 2 fast activities
        let fast_block = async {
            let a = ctx.schedule_activity("Fast", "1").await?;
            let b = ctx.schedule_activity("Fast", "2").await?;
            Ok::<_, String>(format!("fast_block:[{a},{b}]"))
        };

        // Slow block: 1 slow activity then another
        let slow_block = async {
            let a = ctx.schedule_activity("Slow", "1").await?;
            let b = ctx.schedule_activity("Slow", "2").await?;
            Ok::<_, String>(format!("slow_block:[{a},{b}]"))
        };

        let (winner_idx, result) = ctx.select2(fast_block, slow_block).await.into_tuple();
        Ok(format!("winner:{winner_idx},result:{}", result?))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("RaceBlocks", orchestration)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 4,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("race-blocks-1", "RaceBlocks", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("race-blocks-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Fast block should win (index 0)
            assert!(output.starts_with("winner:0,"), "Fast block should win: {output}");
            assert!(
                output.contains("fast_block:[fast:1,fast:2]"),
                "Fast block result incorrect: {output}"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Async block racing directly against a durable future
#[tokio::test]
async fn async_block_vs_durable_future() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Quick", |_ctx: ActivityContext, input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            Ok(format!("quick:{input}"))
        })
        .register("Multi", |_ctx: ActivityContext, input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            Ok(format!("multi:{input}"))
        })
        .build();

    // Orchestration: async block vs single durable future
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Single durable future (fast)
        let single_future = async { ctx.schedule_activity("Quick", "single").await };

        // Async block with multiple steps (slower overall)
        let multi_step_block = async {
            let a = ctx.schedule_activity("Multi", "1").await?;
            let b = ctx.schedule_activity("Multi", "2").await?;
            Ok::<_, String>(format!("block:[{a},{b}]"))
        };

        let (winner_idx, result) = ctx.select2(single_future, multi_step_block).await.into_tuple();
        Ok(format!("winner:{winner_idx},result:{}", result?))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("BlockVsFuture", orchestration)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 2,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("block-vs-future-1", "BlockVsFuture", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("block-vs-future-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Single future should win (index 0)
            assert!(output.starts_with("winner:0,"), "Single future should win: {output}");
            assert!(
                output.contains("quick:single"),
                "Single future result incorrect: {output}"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Select3 racing async blocks with timers
#[tokio::test]
async fn async_block_select3_with_timers() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Work", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("work:{input}"))
        })
        .build();

    // Orchestration: race 3 blocks with different timer patterns
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Block A: short timer, then activity
        let block_a = async {
            ctx.schedule_timer(std::time::Duration::from_millis(10)).await;
            let r = ctx.schedule_activity("Work", "A").await?;
            Ok::<_, String>(format!("A:{r}"))
        };

        // Block B: long timer (should lose)
        let block_b = async {
            ctx.schedule_timer(std::time::Duration::from_millis(500)).await;
            let r = ctx.schedule_activity("Work", "B").await?;
            Ok::<_, String>(format!("B:{r}"))
        };

        // Block C: medium timer
        let block_c = async {
            ctx.schedule_timer(std::time::Duration::from_millis(100)).await;
            let r = ctx.schedule_activity("Work", "C").await?;
            Ok::<_, String>(format!("C:{r}"))
        };

        let (winner_idx, result) = ctx.select3(block_a, block_b, block_c).await.into_tuple();
        Ok(format!("winner:{winner_idx},result:{}", result?))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("Select3Timers", orchestration)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("select3-timers-1", "Select3Timers", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("select3-timers-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Block A (shortest timer) should win
            assert!(output.starts_with("winner:0,"), "Block A should win: {output}");
            assert!(output.contains("A:work:A"), "Block A result incorrect: {output}");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Nested async blocks with join inside select
#[tokio::test]
async fn async_block_nested_join_in_select() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Step", |_ctx: ActivityContext, input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok(format!("step:{input}"))
        })
        .build();

    // Orchestration: race between a timeout and a block that joins multiple activities
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Timeout block
        let timeout = async {
            ctx.schedule_timer(std::time::Duration::from_secs(2)).await;
            Ok::<_, String>("timeout".to_string())
        };

        // Work block: join 3 activities (should complete before timeout)
        let work = async {
            let f1 = ctx.schedule_activity("Step", "1");
            let f2 = ctx.schedule_activity("Step", "2");
            let f3 = ctx.schedule_activity("Step", "3");

            let results = ctx.join(vec![f1, f2, f3]).await;
            let mut outputs = Vec::new();
            for r in results {
                outputs.push(r?);
            }
            Ok::<_, String>(format!("work:[{}]", outputs.join(",")))
        };

        let (winner_idx, result) = ctx.select2(work, timeout).await.into_tuple();
        Ok(format!("winner:{winner_idx},result:{}", result?))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("NestedJoinSelect", orchestration)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 3,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("nested-join-select-1", "NestedJoinSelect", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("nested-join-select-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Work block should complete before timeout
            assert!(
                output.starts_with("winner:0,"),
                "Work should complete before timeout: {output}"
            );
            assert!(output.contains("step:1"), "Result should contain step:1: {output}");
            assert!(output.contains("step:2"), "Result should contain step:2: {output}");
            assert!(output.contains("step:3"), "Result should contain step:3: {output}");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Sub-Orchestration Async Block Patterns
// =============================================================================

/// Async block with sub-orchestration + activities racing against a fast block
/// The block with sub-orchestration wins because the sub-orch is fast
#[tokio::test]
async fn async_block_suborchestration_wins_race() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CHILD_COMPLETED: AtomicUsize = AtomicUsize::new(0);
    CHILD_COMPLETED.store(0, Ordering::SeqCst);

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("FastWork", |_ctx: ActivityContext, input: String| async move {
            // Fast: minimal delay
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            Ok(format!("fast:{input}"))
        })
        .register("SlowWork", |_ctx: ActivityContext, input: String| async move {
            // Slow: much longer delay to ensure it loses
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            Ok(format!("slow:{input}"))
        })
        .build();

    // Fast child orchestration
    let child = |ctx: OrchestrationContext, input: String| async move {
        let result = ctx.schedule_activity("FastWork", input).await?;
        CHILD_COMPLETED.fetch_add(1, Ordering::SeqCst);
        Ok(format!("child:{result}"))
    };

    // Parent: race block-with-suborchestration vs slow-block
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        // Block A: sub-orchestration + activity (fast)
        let suborchestration_block = async {
            let sub_result = ctx.schedule_sub_orchestration("FastChild", "sub-input").await?;
            let activity_result = ctx.schedule_activity("FastWork", "after-sub").await?;
            Ok::<_, String>(format!("blockA:[{sub_result},{activity_result}]"))
        };

        // Block B: slow activities (should lose due to 2x500ms = 1 second)
        let slow_block = async {
            let r1 = ctx.schedule_activity("SlowWork", "1").await?;
            let r2 = ctx.schedule_activity("SlowWork", "2").await?;
            Ok::<_, String>(format!("blockB:[{r1},{r2}]"))
        };

        let (winner_idx, result) = ctx.select2(suborchestration_block, slow_block).await.into_tuple();
        Ok(format!("winner:{winner_idx},result:{}", result?))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("FastChild", child)
        .register("RaceParent", parent)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 2, // Parent + child
        worker_concurrency: 2,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("suborchestration-wins-1", "RaceParent", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("suborchestration-wins-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Block A (with sub-orchestration) should win
            assert!(output.starts_with("winner:0,"), "Sub-orch block should win: {output}");
            assert!(
                output.contains("child:fast:sub-input"),
                "Should have child result: {output}"
            );
            assert!(
                output.contains("fast:after-sub"),
                "Should have activity after sub: {output}"
            );

            // Child should have completed
            assert_eq!(
                CHILD_COMPLETED.load(Ordering::SeqCst),
                1,
                "Child should have completed once"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Async block with slow sub-orchestration loses race - child is NOT completed
/// NOTE: Sub-orchestration cancellation is NOT currently wired up (see TODO.md)
#[tokio::test]
async fn async_block_suborchestration_loses_race() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Fast", |_ctx: ActivityContext, input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            Ok(format!("fast:{input}"))
        })
        .register("VerySlow", |_ctx: ActivityContext, input: String| async move {
            // Very slow - gives race time to complete
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            Ok(format!("veryslow:{input}"))
        })
        .build();

    // Slow child orchestration - does multiple slow activities
    let slow_child = |ctx: OrchestrationContext, input: String| async move {
        // First slow activity
        let r1 = ctx.schedule_activity("VerySlow", format!("{input}-1")).await?;
        // Second slow activity
        let r2 = ctx.schedule_activity("VerySlow", format!("{input}-2")).await?;

        Ok(format!("child:[{r1},{r2}]"))
    };

    // Parent: race slow-sub-orchestration-block vs fast-block
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        // Block A: slow sub-orchestration (should lose)
        let slow_suborchestration_block = async {
            let sub_result = ctx.schedule_sub_orchestration("SlowChild", "sub-input").await?;
            Ok::<_, String>(format!("blockA:{sub_result}"))
        };

        // Block B: fast activities (should win)
        let fast_block = async {
            let r1 = ctx.schedule_activity("Fast", "1").await?;
            let r2 = ctx.schedule_activity("Fast", "2").await?;
            Ok::<_, String>(format!("blockB:[{r1},{r2}]"))
        };

        let (winner_idx, result) = ctx.select2(slow_suborchestration_block, fast_block).await.into_tuple();
        Ok(format!("winner:{winner_idx},result:{}", result?))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("SlowChild", slow_child)
        .register("RaceParentLoses", parent)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 2,
        worker_concurrency: 2,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("suborchestration-loses-1", "RaceParentLoses", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("suborchestration-loses-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Block B (fast) should win
            assert!(output.starts_with("winner:1,"), "Fast block should win: {output}");
            assert!(
                output.contains("blockB:[fast:1,fast:2]"),
                "Fast block result incorrect: {output}"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    // Find child instance ID from parent's history
    let parent_hist = store.read("suborchestration-loses-1").await.unwrap();
    let child_instance = parent_hist.iter().find_map(|e| match &e.kind {
        duroxide::EventKind::SubOrchestrationScheduled { instance, .. } => Some(instance.clone()),
        _ => None,
    });

    if let Some(child_id) = child_instance {
        // Give a moment for any async operations
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Check child orchestration status - it should NOT be Completed
        // NOTE: Currently sub-orchestration cancellation is NOT wired up (see TODO.md).
        // When a select2 loser is a sub-orchestration, we only mark its source_id as cancelled,
        // but we don't queue a provider-level cancellation like we do for activities.
        // So the child may be: NotFound (never started), Running (abandoned), or potentially
        // Failed if manually cancelled. But NOT Completed.
        let child_status = client.get_orchestration_status(&child_id).await.unwrap();
        match child_status {
            runtime::OrchestrationStatus::Completed { output, .. } => {
                panic!("Child should NOT have completed (it lost the race), but got: {output}");
            }
            runtime::OrchestrationStatus::Failed { .. }
            | runtime::OrchestrationStatus::Running { .. }
            | runtime::OrchestrationStatus::NotFound => {
                // Expected: child was never started, abandoned, or possibly cancelled
            }
        }
    }
    // If no child was scheduled, that's also fine (race completed before scheduling)

    rt.shutdown(None).await;
}

/// Multiple sub-orchestrations in async blocks joined together
#[tokio::test]
async fn async_block_multiple_suborchestrations_joined() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Transform", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("transformed:{input}"))
        })
        .build();

    // Child A: simple transformation
    let child_a = |ctx: OrchestrationContext, input: String| async move {
        let result = ctx.schedule_activity("Transform", format!("A-{input}")).await?;
        Ok(format!("childA:{result}"))
    };

    // Child B: double transformation
    let child_b = |ctx: OrchestrationContext, input: String| async move {
        let r1 = ctx.schedule_activity("Transform", format!("B1-{input}")).await?;
        let r2 = ctx.schedule_activity("Transform", format!("B2-{input}")).await?;
        Ok(format!("childB:[{r1},{r2}]"))
    };

    // Child C: timer + transformation
    let child_c = |ctx: OrchestrationContext, input: String| async move {
        ctx.schedule_timer(std::time::Duration::from_millis(5)).await;
        let result = ctx.schedule_activity("Transform", format!("C-{input}")).await?;
        Ok(format!("childC:timer+{result}"))
    };

    // Parent: join 3 async blocks, each calling a different child
    let parent = |ctx: OrchestrationContext, input: String| async move {
        let input1 = input.clone();
        let input2 = input.clone();
        let input3 = input;

        // Block 1: call ChildA + activity
        let block1 = async {
            let sub = ctx.schedule_sub_orchestration("ChildA", input1).await?;
            let act = ctx.schedule_activity("Transform", "block1-extra").await?;
            Ok::<_, String>(format!("block1:[{sub},{act}]"))
        };

        // Block 2: call ChildB
        let block2 = async {
            let sub = ctx.schedule_sub_orchestration("ChildB", input2).await?;
            Ok::<_, String>(format!("block2:{sub}"))
        };

        // Block 3: activity + call ChildC
        let block3 = async {
            let act = ctx.schedule_activity("Transform", "block3-first").await?;
            let sub = ctx.schedule_sub_orchestration("ChildC", input3).await?;
            Ok::<_, String>(format!("block3:[{act},{sub}]"))
        };

        let (r1, r2, r3) = ctx.join3(block1, block2, block3).await;
        Ok(format!("{},{},{}", r1?, r2?, r3?))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ChildA", child_a)
        .register("ChildB", child_b)
        .register("ChildC", child_c)
        .register("JoinSubOrchParent", parent)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 4, // Parent + 3 children
        worker_concurrency: 3,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("join-suborchestration-1", "JoinSubOrchParent", "data")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("join-suborchestration-1", std::time::Duration::from_secs(15))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // All three blocks should complete
            assert!(output.contains("block1:"), "Should have block1: {output}");
            assert!(output.contains("childA:"), "Should have childA result: {output}");
            assert!(output.contains("block2:"), "Should have block2: {output}");
            assert!(output.contains("childB:"), "Should have childB result: {output}");
            assert!(output.contains("block3:"), "Should have block3: {output}");
            assert!(output.contains("childC:"), "Should have childC result: {output}");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Select3: sub-orchestration blocks racing with timeout
#[tokio::test]
async fn async_block_suborchestration_racing_timeout() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Work", |_ctx: ActivityContext, input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok(format!("work:{input}"))
        })
        .register("SlowWork", |_ctx: ActivityContext, input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            Ok(format!("slowwork:{input}"))
        })
        .build();

    // Fast child
    let fast_child = |ctx: OrchestrationContext, input: String| async move {
        let r = ctx.schedule_activity("Work", input).await?;
        Ok(format!("fast-child:{r}"))
    };

    // Slow child (should get cancelled)
    let slow_child = |ctx: OrchestrationContext, input: String| async move {
        let r = ctx.schedule_activity("SlowWork", input).await?;
        Ok(format!("slow-child:{r}"))
    };

    // Parent: race fast-child-block vs slow-child-block vs timeout
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        // Block A: fast sub-orchestration (should win)
        let fast_block = async {
            let sub = ctx.schedule_sub_orchestration("FastChild", "fast-input").await?;
            Ok::<_, String>(format!("blockA:{sub}"))
        };

        // Block B: slow sub-orchestration (should lose)
        let slow_block = async {
            let sub = ctx.schedule_sub_orchestration("SlowChild", "slow-input").await?;
            Ok::<_, String>(format!("blockB:{sub}"))
        };

        // Block C: long timeout (should lose to fast block)
        let timeout_block = async {
            ctx.schedule_timer(std::time::Duration::from_secs(2)).await;
            Ok::<_, String>("blockC:timeout".to_string())
        };

        let (winner_idx, result) = ctx.select3(fast_block, slow_block, timeout_block).await.into_tuple();
        Ok(format!("winner:{winner_idx},result:{}", result?))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("FastChild", fast_child)
        .register("SlowChild", slow_child)
        .register("TimeoutRaceParent", parent)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 3,
        worker_concurrency: 2,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("timeout-race-1", "TimeoutRaceParent", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("timeout-race-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Block A (fast sub-orchestration) should win over slow block and timeout
            assert!(output.starts_with("winner:0,"), "Fast block should win: {output}");
            assert!(
                output.contains("fast-child:work:fast-input"),
                "Should have fast child result: {output}"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    // Find child instance IDs from parent's history
    let parent_hist = store.read("timeout-race-1").await.unwrap();
    let child_instances: Vec<(String, String)> = parent_hist
        .iter()
        .filter_map(|e| match &e.kind {
            duroxide::EventKind::SubOrchestrationScheduled { name, instance, .. } => {
                Some((name.clone(), instance.clone()))
            }
            _ => None,
        })
        .collect();

    // Find slow child instance if it was scheduled
    let slow_child_instance = child_instances
        .iter()
        .find(|(name, _)| name == "SlowChild")
        .map(|(_, id)| id.clone());

    if let Some(instance_id) = slow_child_instance {
        // Give a moment for any async operations
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Check child orchestration status - it should NOT be Completed
        // NOTE: Currently sub-orchestration cancellation is NOT wired up (see TODO.md).
        // When a select2 loser is a sub-orchestration, we only mark its source_id as cancelled,
        // but we don't queue a provider-level cancellation like we do for activities.
        let status = client.get_orchestration_status(&instance_id).await.unwrap();
        match status {
            runtime::OrchestrationStatus::Completed { output, .. } => {
                panic!("SlowChild should NOT have completed (it lost the race), got: {output}");
            }
            runtime::OrchestrationStatus::Failed { .. }
            | runtime::OrchestrationStatus::Running { .. }
            | runtime::OrchestrationStatus::NotFound => {
                // Expected: child was never started, abandoned, or possibly cancelled
            }
        }
    }
    // If SlowChild was never scheduled, that's also fine - the race completed before scheduling

    rt.shutdown(None).await;
}

/// Nested sub-orchestration: parent calls child which calls grandchild
#[tokio::test]
async fn async_block_nested_suborchestration_chain() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Leaf", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("leaf:{input}"))
        })
        .build();

    // Grandchild: just does an activity
    let grandchild = |ctx: OrchestrationContext, input: String| async move {
        let r = ctx.schedule_activity("Leaf", input).await?;
        Ok(format!("grandchild:{r}"))
    };

    // Child: calls grandchild + does its own activity
    let child = |ctx: OrchestrationContext, input: String| async move {
        // Parallel: grandchild + own activity
        let grandchild_fut = ctx.schedule_sub_orchestration("Grandchild", format!("gc-{input}"));
        let own_activity = ctx.schedule_activity("Leaf", format!("child-{input}"));

        let (gc_result, own_result) = ctx.join2(grandchild_fut, own_activity).await;
        Ok(format!("child:[{},{}]", gc_result?, own_result?))
    };

    // Parent: calls two children in parallel
    let parent = |ctx: OrchestrationContext, input: String| async move {
        let child1 = async {
            let r = ctx.schedule_sub_orchestration("Child", format!("c1-{input}")).await?;
            Ok::<_, String>(format!("block1:{r}"))
        };

        let child2 = async {
            // Timer + child call
            ctx.schedule_timer(std::time::Duration::from_millis(5)).await;
            let r = ctx.schedule_sub_orchestration("Child", format!("c2-{input}")).await?;
            Ok::<_, String>(format!("block2:timer+{r}"))
        };

        let (r1, r2) = ctx.join2(child1, child2).await;
        Ok(format!("{},{}", r1?, r2?))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("Grandchild", grandchild)
        .register("Child", child)
        .register("NestedParent", parent)
        .build();

    let options = RuntimeOptions {
        orchestration_concurrency: 5, // Parent + 2 children + 2 grandchildren
        worker_concurrency: 2,
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("nested-chain-1", "NestedParent", "root")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("nested-chain-1", std::time::Duration::from_secs(15))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Both blocks should complete with nested results
            assert!(output.contains("block1:"), "Should have block1: {output}");
            assert!(
                output.contains("block2:timer+"),
                "Should have block2 with timer: {output}"
            );
            assert!(
                output.contains("grandchild:"),
                "Should have grandchild results: {output}"
            );
            assert!(output.contains("child:"), "Should have child results: {output}");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;
}

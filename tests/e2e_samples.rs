//! End-to-end samples: start here to learn the API by example.
//!
//! Each test demonstrates a common orchestration pattern using
//! `OrchestrationContext` and the in-process `Runtime`.
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::EventKind;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, Client, Either2, OrchestrationContext, OrchestrationRegistry};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
mod common;

/// Hello World: define one activity and call it from an orchestrator.
///
/// Highlights:
/// - Register an activity in an `ActivityRegistry`
/// - Start the `Runtime` with a provider (filesystem here)
/// - Schedule an activity and await its typed completion
#[tokio::test]
async fn sample_hello_world_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Register a simple activity: "Hello" -> format a greeting
    let activity_registry = ActivityRegistry::builder()
        .register("Hello", |ctx: ActivityContext, input: String| async move {
            ctx.trace_info("Hello activity started");
            let greeting = format!("Hello, {input}!");
            ctx.trace_info(format!("Hello activity completed -> {greeting}"));
            Ok(greeting)
        })
        .build();

    // Orchestrator: emit a trace, call Hello twice, return result using input
    let orchestration = |ctx: OrchestrationContext, input: String| async move {
        ctx.trace_info("hello_world started");
        let res = ctx.schedule_activity("Hello", "Rust").await?;
        ctx.trace_info(format!("hello_world result={res} "));
        let res1 = ctx.schedule_activity("Hello", input).await?;
        ctx.trace_info(format!("hello_world result={res1} "));
        Ok(res1)
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("HelloWorld", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-sample-hello-1", "HelloWorld", "World")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-sample-hello-1", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "Hello, World!"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }
    rt.shutdown(None).await;
}

/// Basic control flow: branch on a flag returned by an activity.
///
/// Highlights:
/// - Call an activity to fetch a decision
/// - Use standard Rust control flow to drive subsequent activities
#[tokio::test]
async fn sample_basic_control_flow_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Register activities that return a flag and branch outcomes
    let activity_registry = ActivityRegistry::builder()
        .register("GetFlag", |_ctx: ActivityContext, _input: String| async move {
            Ok("yes".to_string())
        })
        .register("SayYes", |_ctx: ActivityContext, _in: String| async move {
            Ok("picked_yes".to_string())
        })
        .register("SayNo", |_ctx: ActivityContext, _in: String| async move {
            Ok("picked_no".to_string())
        })
        .build();

    // Orchestrator: get a flag and branch
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let flag = ctx.schedule_activity("GetFlag", "").await.unwrap();
        ctx.trace_info(format!("control_flow flag decided = {flag}"));
        if flag == "yes" {
            Ok(ctx.schedule_activity("SayYes", "").await.unwrap())
        } else {
            Ok(ctx.schedule_activity("SayNo", "").await.unwrap())
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ControlFlow", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-sample-cflow-1", "ControlFlow", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-sample-cflow-1", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "picked_yes"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }
    rt.shutdown(None).await;
}

/// Loops and accumulation: call an activity repeatedly and build up a value.
///
/// Highlights:
/// - Use a for-loop in the orchestrator
/// - Emit replay-safe traces per iteration
#[tokio::test]
async fn sample_loop_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Register an activity that appends "x" to its input
    let activity_registry = ActivityRegistry::builder()
        .register("Append", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("{input}x"))
        })
        .build();

    // Orchestrator: loop three times, updating an accumulator
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let mut acc = String::from("start");
        for i in 0..3 {
            acc = ctx.schedule_activity("Append", acc).await.unwrap();
            ctx.trace_info(format!("loop iteration {i} completed acc={acc}"));
        }
        Ok(acc)
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("LoopOrchestration", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-sample-loop-1", "LoopOrchestration", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-sample-loop-1", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "startxxx"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }
    rt.shutdown(None).await;
}

/// Error handling and compensation: recover from a failed activity.
///
/// Highlights:
/// - Activities return `Result<String, String>` and map into `Ok/Err`
/// - On failure, run a compensating activity and log what happened
#[tokio::test]
async fn sample_error_handling_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Register a fragile activity that may fail, and a recovery activity
    let activity_registry = ActivityRegistry::builder()
        .register("Fragile", |_ctx: ActivityContext, input: String| async move {
            if input == "bad" {
                Err("boom".to_string())
            } else {
                Ok("ok".to_string())
            }
        })
        .register("Recover", |_ctx: ActivityContext, _input: String| async move {
            Ok("recovered".to_string())
        })
        .build();

    // Orchestrator: try fragile, on error call Recover
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        match ctx.schedule_activity("Fragile", "bad").await {
            Ok(v) => {
                ctx.trace_info(format!("fragile succeeded value={v}"));
                Ok(v)
            }
            Err(e) => {
                ctx.trace_warn(format!("fragile failed error={e}"));
                let rec = ctx.schedule_activity("Recover", "").await.unwrap();
                if rec != "recovered" {
                    ctx.trace_error(format!("unexpected recovery value={rec}"));
                }
                Ok(rec)
            }
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ErrorHandling", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-sample-err-1", "ErrorHandling", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-sample-err-1", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "recovered"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }
    rt.shutdown(None).await;
}

/// Timeouts via racing a long-running activity against a timer.
///
/// Highlights:
/// - Schedule a long-running activity and a short timer
/// - Use `ctx.select` to deterministically pick the earliest completion in history
/// - If the timer wins, return an error to the user
#[tokio::test]
async fn sample_timeout_with_timer_race_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Register a long-running activity that sleeps before returning
    let activity_registry = ActivityRegistry::builder()
        .register("LongOp", |ctx: ActivityContext, _input: String| async move {
            ctx.trace_info("LongOp started");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            ctx.trace_info("LongOp finished");
            Ok("done".to_string())
        })
        .build();

    // Orchestration: race LongOp vs 100ms timer and error if timer wins
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let act = ctx.schedule_activity("LongOp", "");
        let t = async {
            ctx.schedule_timer(Duration::from_millis(100)).await;
            Err::<String, String>("timeout".into())
        };
        let (idx, out) = ctx.select2(act, t).await.into_tuple();
        match idx {
            0 => out,
            1 => out,
            _ => unreachable!(),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("TimeoutSample", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-timeout-sample", "TimeoutSample", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-timeout-sample", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Failed { details, .. } => assert_eq!(details.display_message(), "timeout"),
        runtime::OrchestrationStatus::Completed { output, .. } => panic!("expected timeout failure, got: {output}"),
        _ => panic!("unexpected orchestration status"),
    }
    rt.shutdown(None).await;
}

/// Mixed race with select2: activity vs external event, demonstrate using the winner index.
///
/// Highlights:
/// - Schedule a slow activity and subscribe to an external event
/// - Use `ctx.select2(activity, external)` to pick the earliest completion
/// - Use the usize index from select2 to branch on which completed first
#[tokio::test]
async fn sample_select2_activity_vs_external_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Sleep", |ctx: ActivityContext, _input: String| async move {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            ctx.trace_info("Sleep activity finished");
            Ok("slept".to_string())
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let act = ctx.schedule_activity("Sleep", "");
        let evt = async { Ok::<String, String>(ctx.schedule_wait("Go").await) };
        let (idx, out) = ctx.select2(act, evt).await.into_tuple();
        // Demonstrate using the index to branch
        match idx {
            0 => out.map(|s| format!("activity:{s}")),
            1 => out.map(|payload| format!("event:{payload}")),
            _ => unreachable!(),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("Select2ActVsEvt", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;

    // Start orchestration, then raise external after subscription is recorded
    let store_for_wait = store.clone();
    tokio::spawn(async move {
        let sfw = store_for_wait.clone();
        let _ = common::wait_for_subscription(sfw.clone(), "inst-s2-mixed", "Go", 1000).await;
        let client = Client::new(sfw);
        let _ = client.raise_event("inst-s2-mixed", "Go", "ok").await;
    });
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-s2-mixed", "Select2ActVsEvt", "")
        .await
        .unwrap();

    let s = match client
        .wait_for_orchestration("inst-s2-mixed", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => output,
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    };
    // External event should win (idx==1) because activity sleeps 300ms
    assert_eq!(s, "event:ok");
    rt.shutdown(None).await;
}

/// Parallel fan-out/fan-in: run two activities concurrently and join results.
///
/// Highlights:
/// - Use `ctx.join` to await multiple `DurableFuture`s concurrently in history order
/// - Deterministic replay ensures join order follows history
#[tokio::test]
async fn dtf_legacy_gabbar_greetings_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Register a greeting activity used by both branches
    let activity_registry = ActivityRegistry::builder()
        .register("Greetings", |ctx: ActivityContext, input: String| async move {
            ctx.trace_info("Greeting activity started");
            ctx.trace_debug(format!("Original input: {input}"));
            let output = format!("Hello, {input}!");
            ctx.trace_info(format!("Greeting activity completed -> {output}"));
            Ok(output)
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Schedule two greetings in parallel using deterministic join
        let a = ctx.schedule_activity("Greetings", "Gabbar");
        let b = ctx.schedule_activity("Greetings", "Samba");
        let outs = ctx.join(vec![a, b]).await;
        let mut vals: Vec<String> = outs
            .into_iter()
            .map(|o| match o {
                Ok(s) => s,
                Err(e) => panic!("activity failed: {e}"),
            })
            .collect();
        // For a stable assertion build a canonical order
        vals.sort();
        Ok(format!("{}, {}", vals[0].clone(), vals[1].clone()))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("Greetings", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-dtf-greetings", "Greetings", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-dtf-greetings", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "Hello, Gabbar!, Hello, Samba!"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }
    rt.shutdown(None).await;
}

/// System activities: use built-in activities to get wall-clock time and a new GUID.
///
/// Highlights:
/// - Call `ctx.utc_now()` and `ctx.new_guid()`
/// - Log and validate basic formatting of results
#[tokio::test]
async fn sample_system_activities_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder().build();

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let now = ctx.utc_now().await?;
        let guid = ctx.new_guid().await?;

        // Convert SystemTime to milliseconds for display
        let now_ms = now
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis() as u64;
        ctx.trace_info(format!("system now={now_ms}ms, guid={guid}"));
        Ok(format!("n={now_ms},g={guid}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("SystemActivities", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-system-acts", "SystemActivities", "")
        .await
        .unwrap();

    let out = match client
        .wait_for_orchestration("inst-system-acts", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => output,
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    };
    // Basic assertions
    assert!(out.contains("n=") && out.contains(",g="));
    let parts: Vec<&str> = out.split([',', '=']).collect();
    // parts like ["n", now, "g", guid]
    assert!(parts.len() >= 4);
    let now_val: u64 = parts[1].parse().unwrap_or(0);
    let guid_str = parts[3];
    assert!(now_val > 0);
    // GUID format: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" (36 chars with hyphens)
    assert_eq!(guid_str.len(), 36);
    assert!(guid_str.chars().filter(|c| *c != '-').all(|c| c.is_ascii_hexdigit()));

    rt.shutdown(None).await;
}

/// Sample: start an orchestration and poll its status until completion.
#[tokio::test]
async fn sample_status_polling_fs() {
    use duroxide::OrchestrationStatus;
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder().build();
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_timer(Duration::from_millis(20)).await;
        Ok("done".to_string())
    };
    let orchestration_registry = OrchestrationRegistry::builder()
        .register("StatusSample", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-status-sample", "StatusSample", "")
        .await
        .unwrap();

    // New helper: wait until terminal (Completed/Failed) or timeout.
    match client
        .wait_for_orchestration("inst-status-sample", std::time::Duration::from_secs(2))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "done"),
        OrchestrationStatus::Failed { details, .. } => panic!("unexpected failure: {}", details.display_message()),
        _ => unreachable!(),
    }
    rt.shutdown(None).await;
}

/// Sub-orchestrations: simple parent/child orchestration.
///
/// Highlights:
/// - Parent calls a child orchestration and awaits its result
/// - Child uses an activity and returns its output
#[tokio::test]
async fn sample_sub_orchestration_basic_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Upper", |ctx: ActivityContext, input: String| async move {
            ctx.trace_info("Upper activity converting string");
            let result = input.to_uppercase();
            ctx.trace_info(format!("Upper activity result -> {result}"));
            Ok(result)
        })
        .build();

    let child_upper = |ctx: OrchestrationContext, input: String| async move {
        let up = ctx.schedule_activity("Upper", input).await.unwrap();
        Ok(up)
    };
    let parent = |ctx: OrchestrationContext, input: String| async move {
        let r = ctx.schedule_sub_orchestration("ChildUpper", input).await.unwrap();
        Ok(format!("parent:{r}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ChildUpper", child_upper)
        .register("Parent", parent)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-sub-basic", "Parent", "hi")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-sub-basic", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "parent:HI"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }
    rt.shutdown(None).await;
}

/// Sub-orchestrations: fan-out to multiple children and join.
///
/// Highlights:
/// - Parent starts two child orchestrations in parallel
/// - Uses `ctx.join` to await both in history order and aggregates results
#[tokio::test]
async fn sample_sub_orchestration_fanout_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Add", |_ctx: ActivityContext, input: String| async move {
            let mut it = input.split(',');
            let a = it.next().unwrap_or("0").parse::<i64>().unwrap_or(0);
            let b = it.next().unwrap_or("0").parse::<i64>().unwrap_or(0);
            Ok((a + b).to_string())
        })
        .build();

    let child_sum = |ctx: OrchestrationContext, input: String| async move {
        let s = ctx.schedule_activity("Add", input).await.unwrap();
        Ok(s)
    };
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        let a = ctx.schedule_sub_orchestration("ChildSum", "1,2");
        let b = ctx.schedule_sub_orchestration("ChildSum", "3,4");
        let outs = ctx.join(vec![a, b]).await;
        let mut nums: Vec<i64> = outs
            .into_iter()
            .map(|o| match o {
                Ok(s) => s.parse::<i64>().unwrap(),
                Err(e) => panic!("child failed: {e}"),
            })
            .collect();
        let total: i64 = nums.drain(..).sum();
        Ok(format!("total={total}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ChildSum", child_sum)
        .register("ParentFan", parent)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-sub-fan", "ParentFan", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-sub-fan", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "total=10"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }
    rt.shutdown(None).await;
}

/// Sub-orchestrations: chained (root -> mid -> leaf).
///
/// Highlights:
/// - Root calls Mid; Mid calls Leaf; each returns a transformed value
/// - Demonstrates nested sub-orchestrations
#[tokio::test]
async fn sample_sub_orchestration_chained_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("AppendX", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("{input}x"))
        })
        .build();

    let leaf = |ctx: OrchestrationContext, input: String| async move {
        Ok(ctx.schedule_activity("AppendX", input).await.unwrap())
    };
    let mid = |ctx: OrchestrationContext, input: String| async move {
        let r = ctx.schedule_sub_orchestration("Leaf", input).await.unwrap();
        Ok(format!("{r}-mid"))
    };
    let root = |ctx: OrchestrationContext, input: String| async move {
        let r = ctx.schedule_sub_orchestration("Mid", input).await.unwrap();
        Ok(format!("root:{r}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("Leaf", leaf)
        .register("Mid", mid)
        .register("Root", root)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client.start_orchestration("inst-sub-chain", "Root", "a").await.unwrap();

    match client
        .wait_for_orchestration("inst-sub-chain", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "root:ax-mid"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }
    rt.shutdown(None).await;
}

/// Detached orchestration scheduling: start independent orchestrations without awaiting.
///
/// Highlights:
/// - Use `ctx.schedule_orchestration(name, instance, input)` with explicit instance IDs
/// - No parent/child semantics; scheduled orchestrations are independent roots
/// - Verify scheduled instances complete via status polling
#[tokio::test]
async fn sample_detached_orchestration_scheduling_fs() {
    use duroxide::OrchestrationStatus;
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();

    let chained = |ctx: OrchestrationContext, input: String| async move {
        ctx.schedule_timer(Duration::from_millis(5)).await;
        Ok(ctx.schedule_activity("Echo", input).await.unwrap())
    };
    let coordinator = |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_orchestration("Chained", "W1", "A");
        ctx.schedule_orchestration("Chained", "W2", "B");
        Ok("scheduled".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("Chained", chained)
        .register("Coordinator", coordinator)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("CoordinatorRoot", "Coordinator", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("CoordinatorRoot", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "scheduled"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // The scheduled instances are plain W1/W2 (no prefixing)
    let insts = vec!["W1".to_string(), "W2".to_string()];
    for inst in insts {
        match client
            .wait_for_orchestration(&inst, std::time::Duration::from_secs(5))
            .await
            .unwrap()
        {
            OrchestrationStatus::Completed { output, .. } => {
                assert!(output == "A" || output == "B");
            }
            OrchestrationStatus::Failed { details, .. } => {
                panic!("scheduled orchestration failed: {}", details.display_message())
            }
            _ => unreachable!(),
        }
    }

    rt.shutdown(None).await;
}

/// Detached orchestration followed by activity: tests that fire-and-forget scheduling
/// is correctly recorded in history for determinism on replay.
///
/// This test will fail with nondeterminism if OrchestrationChained events are not recorded,
/// because on replay the engine will try to match StartOrchestrationDetached action against
/// ActivityScheduled event.
///
/// Highlights:
/// - Fire-and-forget with `ctx.schedule_orchestration()` followed by awaited activity
/// - Verifies both the parent and child orchestrations complete correctly
#[tokio::test]
async fn sample_detached_then_activity_fs() {
    use duroxide::OrchestrationStatus;
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();

    let child = |ctx: OrchestrationContext, input: String| async move {
        ctx.schedule_timer(Duration::from_millis(5)).await;
        Ok(format!("child-{input}"))
    };
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        // Fire-and-forget: schedule detached orchestration
        ctx.schedule_orchestration("Child", "detached-child", "payload");
        // Then await an activity - this requires OrchestrationChained to be recorded
        // for replay to work correctly
        let result = ctx.schedule_activity("Echo", "hello").await?;
        Ok(result)
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("Child", child)
        .register("Parent", parent)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("ParentInstance", "Parent", "")
        .await
        .unwrap();

    // Parent should complete with Echo result
    match client
        .wait_for_orchestration("ParentInstance", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "hello"),
        OrchestrationStatus::Failed { details, .. } => {
            panic!("parent orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Child should also complete
    match client
        .wait_for_orchestration("detached-child", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "child-payload"),
        OrchestrationStatus::Failed { details, .. } => {
            panic!("child orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected child status"),
    }

    rt.shutdown(None).await;
}

/// ContinueAsNew sample: roll over input across executions until a condition is met.
///
/// Highlights:
/// - Use `ctx.continue_as_new(new_input)` to terminate current execution and start a new one
/// - Provider keeps all execution histories; latest execution holds the final result
#[tokio::test]
async fn sample_continue_as_new_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder().build();
    let orch = |ctx: OrchestrationContext, input: String| async move {
        let n: u32 = input.parse().unwrap_or(0);
        if n < 3 {
            ctx.trace_info(format!("CAN sample n={n} -> continue"));
            return ctx.continue_as_new((n + 1).to_string()).await;
        } else {
            Ok(format!("final:{n}"))
        }
    };
    let orchestration_registry = OrchestrationRegistry::builder().register("CanSample", orch).build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-sample-can", "CanSample", "0")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-sample-can", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "final:3"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }
    // Check executions exist
    let mgmt = store.as_management_capability().expect("ProviderAdmin required");
    let execs = mgmt.list_executions("inst-sample-can").await.unwrap_or_default();
    assert_eq!(execs, vec![1, 2, 3, 4]);
    rt.shutdown(None).await;
}

// Typed samples

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
struct Ack {
    ok: bool,
}

/// Typed activity + typed orchestration: Add two numbers and return a struct
#[tokio::test]
async fn sample_typed_activity_and_orchestration_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register_typed::<AddReq, AddRes, _, _>("Add", |_ctx: ActivityContext, req| async move {
            Ok(AddRes { sum: req.a + req.b })
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, req: AddReq| async move {
        let out: AddRes = ctx.schedule_activity_typed::<AddReq, AddRes>("Add", &req).await?;
        Ok(out)
    };
    let orchestration_registry = OrchestrationRegistry::builder()
        .register_typed::<AddReq, AddRes, _, _>("Adder", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration_typed::<AddReq>("inst-typed-add", "Adder", AddReq { a: 2, b: 3 })
        .await
        .unwrap();

    match client
        .wait_for_orchestration_typed::<AddRes>("inst-typed-add", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        Ok(result) => assert_eq!(result, AddRes { sum: 5 }),
        Err(error) => panic!("orchestration failed: {error}"),
    }
    rt.shutdown(None).await;
}

/// Typed external event sample: await Ack { ok } from an event
#[tokio::test]
async fn sample_typed_event_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder().build();
    let orch = |ctx: OrchestrationContext, _in: ()| async move {
        let ack: Ack = ctx.schedule_wait_typed::<Ack>("Ready").await;
        Ok::<_, String>(serde_json::to_string(&ack).unwrap())
    };
    let orchestration_registry = OrchestrationRegistry::builder()
        .register_typed::<(), String, _, _>("WaitAck", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let store_for_wait = store.clone();
    tokio::spawn(async move {
        let sfw = store_for_wait.clone();
        let _ = common::wait_for_subscription(sfw.clone(), "inst-typed-ack", "Ready", 1000).await;
        // Raise typed event by serializing payload
        let payload = serde_json::to_string(&Ack { ok: true }).unwrap();
        let client = Client::new(sfw);
        let _ = client.raise_event("inst-typed-ack", "Ready", payload).await;
    });
    let client = Client::new(store.clone());
    client
        .start_orchestration_typed::<()>("inst-typed-ack", "WaitAck", ())
        .await
        .unwrap();

    match client
        .wait_for_orchestration_typed::<String>("inst-typed-ack", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        Ok(result) => assert_eq!(result, serde_json::to_string(&Ack { ok: true }).unwrap()),
        Err(error) => panic!("orchestration failed: {error}"),
    }
    rt.shutdown(None).await;
}

/// Mixed string and typed activities with typed orchestration, showcasing select on typed+string
#[tokio::test]
async fn sample_mixed_string_and_typed_typed_orch_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // String activity: returns uppercased string
    // Typed activity: Add two numbers
    let activity_registry = ActivityRegistry::builder()
        .register("Upper", |_ctx: ActivityContext, input: String| async move {
            Ok(input.to_uppercase())
        })
        .register_typed::<AddReq, AddRes, _, _>("Add", |_ctx: ActivityContext, req| async move {
            Ok(AddRes { sum: req.a + req.b })
        })
        .build();

    // Typed orchestrator input/output
    let orch = |ctx: OrchestrationContext, req: AddReq| async move {
        // Kick off a typed activity and a string activity, race them with deterministic select
        // Wrap both to return Result<String, String> for select2 type compatibility
        let f_typed = async {
            let res: Result<AddRes, String> = ctx.schedule_activity_typed::<AddReq, AddRes>("Add", &req).await;
            res.map(|r| format!("sum={}", r.sum))
        };
        let f_str = async {
            let res: Result<String, String> = ctx.schedule_activity("Upper", "hello").await;
            res.map(|s| format!("up={s}"))
        };
        let (_idx, out) = ctx.select2(f_typed, f_str).await.into_tuple();
        out
    };
    let orchestration_registry = OrchestrationRegistry::builder()
        .register_typed::<AddReq, String, _, _>("MixedTypedOrch", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration_typed::<AddReq>("inst-mixed-typed", "MixedTypedOrch", AddReq { a: 1, b: 2 })
        .await
        .unwrap();
    let client = Client::new(store.clone());

    let s = match client
        .wait_for_orchestration_typed::<String>("inst-mixed-typed", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        Ok(result) => result,
        Err(error) => panic!("orchestration failed: {error}"),
    };
    assert!(s == "sum=3" || s == "up=HELLO");
    rt.shutdown(None).await;
}

/// Mixed string and typed activities with string orchestration, showcasing select on typed+string
#[tokio::test]
async fn sample_mixed_string_and_typed_string_orch_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Upper", |_ctx: ActivityContext, input: String| async move {
            Ok(input.to_uppercase())
        })
        .register_typed::<AddReq, AddRes, _, _>("Add", |_ctx: ActivityContext, req| async move {
            Ok(AddRes { sum: req.a + req.b })
        })
        .build();

    // String orchestrator mixes typed and string activity calls
    let orch = |ctx: OrchestrationContext, _in: String| async move {
        // Wrap both futures to return Result<String, String> for select2 type compatibility
        let f_typed = async {
            let res: Result<AddRes, String> = ctx
                .schedule_activity_typed::<AddReq, AddRes>("Add", &AddReq { a: 5, b: 7 })
                .await;
            res.map(|r| format!("sum={}", r.sum))
        };
        let f_str = async {
            let res: Result<String, String> = ctx.schedule_activity("Upper", "race").await;
            res.map(|s| format!("up={s}"))
        };
        let (_idx, out) = ctx.select2(f_typed, f_str).await.into_tuple();
        out
    };
    let orch_reg = OrchestrationRegistry::builder()
        .register("MixedStringOrch", orch)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orch_reg).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-mixed-string", "MixedStringOrch", "")
        .await
        .unwrap();

    let s = match client
        .wait_for_orchestration("inst-mixed-string", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => output,
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    };
    assert!(s == "sum=12" || s == "up=RACE");
    rt.shutdown(None).await;
}

/// Versioning: default latest vs pinned exact on start
///
/// Highlights:
/// - Register two versions of the same orchestration using semver (1.0.0 and 2.0.0)
/// - Default policy (Latest) picks the highest on new starts
/// - Changing policy to Exact pins new starts to a specific version
#[tokio::test]
async fn sample_versioning_start_latest_vs_exact_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Two versions: return a string indicating which version executed
    let v1 = |_: OrchestrationContext, _in: String| async move { Ok("v1".to_string()) };
    let v2 = |_: OrchestrationContext, _in: String| async move { Ok("v2".to_string()) };

    let reg = OrchestrationRegistry::builder()
        // Default registration is 1.0.0
        .register("Versioned", v1)
        // Add a later version 2.0.0
        .register_versioned("Versioned", "2.0.0", v2)
        .build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, reg.clone()).await;

    // With default policy (Latest), a new start should run v2
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-vers-latest", "Versioned", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-vers-latest", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "v2"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Pin new starts to 1.0.0 via policy, verify it runs v1
    reg.set_version_policy(
        "Versioned",
        duroxide::runtime::VersionPolicy::Exact(semver::Version::parse("1.0.0").unwrap()),
    );
    client
        .start_orchestration("inst-vers-exact", "Versioned", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-vers-exact", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "v1"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Versioning: sub-orchestration explicit version vs default policy
///
/// Highlights:
/// - Parent calls child once with an explicit version and once without
/// - The explicit call uses 1.0.0; the policy (Latest) uses 2.0.0
#[tokio::test]
async fn sample_versioning_sub_orchestration_explicit_vs_policy_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let child_v1 = |_: OrchestrationContext, _in: String| async move { Ok("c1".to_string()) };
    let child_v2 = |_: OrchestrationContext, _in: String| async move { Ok("c2".to_string()) };
    let parent = |ctx: OrchestrationContext, _in: String| async move {
        // Explicit versioned call -> expect c1
        let a = ctx
            .schedule_sub_orchestration_versioned("Child", Some("1.0.0".to_string()), "exp")
            .await
            .unwrap();
        // Policy-based call (Latest) -> expect c2
        let b = ctx.schedule_sub_orchestration("Child", "pol").await.unwrap();
        Ok(format!("{a}-{b}"))
    };

    let reg = OrchestrationRegistry::builder()
        .register("ParentVers", parent)
        .register("Child", child_v1)
        .register_versioned("Child", "2.0.0", child_v2)
        .build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, reg).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-sub-vers", "ParentVers", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-sub-vers", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "c1-c2"),
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Versioning + ContinueAsNew: safe upgrade of a long-running (infinite) orchestration
///
/// Highlights:
/// - Use `continue_as_new(new_input)` to roll to a fresh execution that picks the default version
///   from the registry policy (Latest by default, or a pinned Exact if set)
/// - Avoids nondeterminism because the new execution starts fresh at the version boundary
/// - Carry forward state via the CAN input, or transform as needed during upgrade
#[tokio::test]
async fn sample_versioning_continue_as_new_upgrade_fs() {
    use duroxide::OrchestrationStatus;
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // v1: simulate deciding to upgrade at a maintenance boundary (e.g., at the end of a cycle)
    // In a real infinite loop, you'd do some work (timer/activity), then CAN to v2.
    let v1 = |ctx: OrchestrationContext, input: String| async move {
        ctx.trace_info("v1: upgrading via ContinueAsNew (default policy)".to_string());
        // Roll to a fresh execution, marking the payload so we can attribute it to v1 deterministically
        return ctx.continue_as_new(format!("v1:{input}")).await;
    };
    // v2: represents the upgraded logic. Here we just simulate one step and complete for the sample.
    let v2 = |ctx: OrchestrationContext, input: String| async move {
        ctx.trace_info(format!("v2: resumed with input={input}"));
        Ok(format!("upgraded:{input}"))
    };

    let reg = OrchestrationRegistry::builder()
        .register("LongRunner", v1) // implicit 1.0.0
        .register_versioned("LongRunner", "2.0.0", v2)
        .build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, reg).await;

    // Start on v1; the first handle will resolve at the CAN boundary
    // Pin initial start to v1 explicitly to demonstrate upgrade via CAN; default policy remains Latest (v2)
    let client = Client::new(store.clone());
    client
        .start_orchestration_versioned("inst-can-upgrade", "LongRunner", "1.0.0", "state")
        .await
        .unwrap();

    // Poll for the new execution (v2) to complete
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match client.get_orchestration_status("inst-can-upgrade").await.unwrap() {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!(output, "upgraded:v1:state");
                break;
            }
            OrchestrationStatus::Failed { details, .. } => panic!("unexpected failure: {}", details.display_message()),
            _ if std::time::Instant::now() < deadline => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            _ => panic!("timeout waiting for upgraded completion"),
        }
    }

    // Verify two executions exist, exec1 continued-as-new, exec2 completed with v2 output
    let mgmt2 = store.as_management_capability().expect("ProviderAdmin required");
    let execs = mgmt2.list_executions("inst-can-upgrade").await.unwrap_or_default();
    assert_eq!(execs, vec![1, 2]);
    let e1 = mgmt2
        .read_history_with_execution_id("inst-can-upgrade", 1)
        .await
        .unwrap_or_default();
    assert!(
        e1.iter()
            .any(|e| matches!(&e.kind, duroxide::EventKind::OrchestrationContinuedAsNew { .. }))
    );
    // Exec2 must start with the v1-marked payload, proving v1 ran first and handed off via CAN
    let e2 = mgmt2
        .read_history_with_execution_id("inst-can-upgrade", 2)
        .await
        .unwrap_or_default();
    assert!(
        e2.iter()
            .any(|e| matches!(&e.kind, duroxide::EventKind::OrchestrationStarted { input, .. } if input == "v1:state"))
    );

    rt.shutdown(None).await;
}

/// Cancellation: cancel a parent orchestration and observe cascading cancellation to children.
///
/// Highlights:
/// - Parent starts a child and awaits it
/// - We cancel the parent instance via the runtime API
/// - The parent fails deterministically with a canonical "canceled: <reason>"
/// - The child is also canceled (downward propagation), and its history shows cancellation
#[tokio::test]
async fn sample_cancellation_parent_cascades_to_children_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Child: waits forever (until canceled). This demonstrates cooperative cancellation via runtime.
    let child = |ctx: OrchestrationContext, _input: String| async move {
        let _ = ctx.schedule_wait("Go").await;
        Ok("done".to_string())
    };

    // Parent: starts child and awaits its completion.
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        let _ = ctx.schedule_sub_orchestration("ChildSample", "seed").await?;
        Ok::<_, String>("parent_done".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ChildSample", child)
        .register("ParentSample", parent)
        .build();
    let activity_registry = ActivityRegistry::builder().build();

    // Use faster polling for cancellation timing test
    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };
    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    // Start the parent orchestration
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-sample-cancel", "ParentSample", "")
        .await
        .unwrap();

    // Allow scheduling turn to run and child to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Cancel the parent; the runtime will append OrchestrationCancelRequested and then OrchestrationFailed
    let _ = client.cancel_instance("inst-sample-cancel", "user_request").await;

    // Wait for the parent to fail deterministically with a canceled error
    let ok = common::wait_for_history(
        store.clone(),
        "inst-sample-cancel",
        |hist| {
            hist.iter().rev().any(|e| {
                matches!(
                    &e.kind,
                    EventKind::OrchestrationFailed { details, .. } if matches!(
                        details,
                        duroxide::ErrorDetails::Application {
                            kind: duroxide::AppErrorKind::Cancelled { reason },
                            ..
                        } if reason == "user_request"
                    )
                )
            })
        },
        5_000,
    )
    .await;
    assert!(ok, "timeout waiting for parent cancel failure");

    // Find child instance (prefix is parent::sub::<id>) and check it was canceled too
    let mgmt = store.as_management_capability().expect("ProviderAdmin required");
    let children: Vec<String> = mgmt
        .list_instances()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|i| i.starts_with("inst-sample-cancel::"))
        .collect();
    assert!(!children.is_empty());
    for child in children {
        let ok_child = common::wait_for_history(
            store.clone(),
            &child,
            |hist| {
                hist.iter()
                    .any(|e| matches!(&e.kind, EventKind::OrchestrationCancelRequested { .. }))
                    && hist.iter().any(|e| {
                        matches!(
                            &e.kind,
                            EventKind::OrchestrationFailed { details, .. } if matches!(
                                details,
                                duroxide::ErrorDetails::Application {
                                    kind: duroxide::AppErrorKind::Cancelled { reason },
                                    ..
                                } if reason == "parent canceled"
                            )
                        )
                    })
            },
            5_000,
        )
        .await;
        assert!(ok_child, "timeout waiting for child cancel for {child}");
    }

    rt.shutdown(None).await;
}

/// Error handling: basic activity failure
///
/// Highlights:
/// - Activity that can fail
/// - Error propagation from activity to orchestration
/// - Simple error handling pattern
#[tokio::test]
async fn sample_basic_error_handling_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Register an activity that can fail
    let activity_registry = ActivityRegistry::builder()
        .register("ValidateInput", |_ctx: ActivityContext, input: String| async move {
            if input.is_empty() {
                Err("Input cannot be empty".to_string())
            } else {
                Ok(format!("Valid: {input}"))
            }
        })
        .build();

    // Simple orchestration that calls the activity
    let orchestration = |ctx: OrchestrationContext, input: String| async move {
        ctx.trace_info("Starting validation");
        let result = ctx.schedule_activity("ValidateInput", input).await?;
        ctx.trace_info(format!("Validation result: {result}"));
        Ok(result)
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("BasicErrorHandling", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    // Test successful case
    client
        .start_orchestration("inst-basic-error-1", "BasicErrorHandling", "test")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-basic-error-1", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "Valid: test");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Test error case
    client
        .start_orchestration("inst-basic-error-2", "BasicErrorHandling", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-basic-error-2", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Failed { details, .. } => {
            assert!(details.display_message().contains("Input cannot be empty"));
        }
        runtime::OrchestrationStatus::Completed { output, .. } => panic!("Expected failure but got success: {output}"),
        _ => panic!("unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Error handling: nested function with `?` operator
///
/// Highlights:
/// - Nested function that can fail
/// - Using `?` operator for error propagation
/// - Clean error handling pattern
#[tokio::test]
async fn sample_nested_function_error_handling_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Register activities
    let activity_registry = ActivityRegistry::builder()
        .register("ProcessData", |_ctx: ActivityContext, input: String| async move {
            if input.contains("error") {
                Err("Processing failed".to_string())
            } else {
                Ok(format!("Processed: {input}"))
            }
        })
        .register("FormatOutput", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("Final: {input}"))
        })
        .build();

    // Nested function that can fail with `?`
    async fn process_and_format(ctx: &OrchestrationContext, data: &str) -> Result<String, String> {
        ctx.trace_info("Starting processing");
        let processed = ctx.schedule_activity("ProcessData", data.to_string()).await?;
        ctx.trace_info("Starting formatting");
        let formatted = ctx.schedule_activity("FormatOutput", processed).await?;
        Ok(formatted)
    }

    // Orchestration that uses nested function with `?`
    let orchestration = |ctx: OrchestrationContext, input: String| async move {
        ctx.trace_info("Starting orchestration");
        let result = process_and_format(&ctx, &input).await?;
        ctx.trace_info("Orchestration completed");
        Ok(result)
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("NestedErrorHandling", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    // Test successful case
    client
        .start_orchestration("inst-nested-error-1", "NestedErrorHandling", "test")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-nested-error-1", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "Final: Processed: test");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Test error case
    client
        .start_orchestration("inst-nested-error-2", "NestedErrorHandling", "error")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-nested-error-2", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Failed { details, .. } => {
            assert!(details.display_message().contains("Processing failed"));
        }
        runtime::OrchestrationStatus::Completed { output, .. } => panic!("Expected failure but got success: {output}"),
        _ => panic!("unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Error handling: error recovery with logging
///
/// Highlights:
/// - Explicit error handling with match statements
/// - Error recovery and logging
/// - Graceful failure handling
#[tokio::test]
async fn sample_error_recovery_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Register activities
    let activity_registry = ActivityRegistry::builder()
        .register("ProcessData", |_ctx: ActivityContext, input: String| async move {
            if input.contains("error") {
                Err("Processing failed".to_string())
            } else {
                Ok(format!("Processed: {input}"))
            }
        })
        .register("LogError", |_ctx: ActivityContext, error: String| async move {
            Ok(format!("Logged: {error}"))
        })
        .build();

    // Orchestration with explicit error recovery
    let orchestration = |ctx: OrchestrationContext, input: String| async move {
        ctx.trace_info("Starting orchestration");

        match ctx.schedule_activity("ProcessData", input.clone()).await {
            Ok(result) => {
                ctx.trace_info("Processing succeeded");
                Ok(result)
            }
            Err(e) => {
                ctx.trace_info("Processing failed, logging error");
                let _ = ctx.schedule_activity("LogError", e.clone()).await;
                Err(format!("Failed to process '{input}': {e}"))
            }
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ErrorRecovery", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    // Test successful case
    client
        .start_orchestration("inst-recovery-1", "ErrorRecovery", "test")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-recovery-1", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "Processed: test");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Test error recovery case
    client
        .start_orchestration("inst-recovery-2", "ErrorRecovery", "error")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("inst-recovery-2", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Failed { details, .. } => {
            let error_msg = details.display_message();
            assert!(error_msg.contains("Failed to process 'error'"));
            assert!(error_msg.contains("Processing failed"));
        }
        runtime::OrchestrationStatus::Completed { output, .. } => panic!("Expected failure but got success: {output}"),
        _ => panic!("unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Self-Pruning Eternal Orchestration: uses `ctx.get_client()` to prune old executions.
///
/// Pattern: An eternal orchestration that processes work in batches using ContinueAsNew,
/// and prunes its own old executions to bound storage growth.
///
/// Highlights:
/// - `ctx.get_client()` on ActivityContext to access management API
/// - Self-pruning pattern for eternal workflows
/// - ContinueAsNew with cleanup
#[tokio::test]
async fn sample_self_pruning_eternal_orchestration() {
    use duroxide::providers::PruneOptions;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Track how many times we pruned and total executions deleted
    let prune_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let executions_pruned = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let prune_count_clone = prune_count.clone();
    let executions_pruned_clone = executions_pruned.clone();

    let activity_registry = ActivityRegistry::builder()
        .register("ProcessBatch", |_ctx: ActivityContext, batch_num: String| async move {
            // Simulate batch processing
            Ok(format!("Processed batch {batch_num}"))
        })
        .register("PruneSelf", move |ctx: ActivityContext, _input: String| {
            let prune_count = prune_count_clone.clone();
            let executions_pruned = executions_pruned_clone.clone();
            async move {
                let client = ctx.get_client();
                let instance_id = ctx.instance_id().to_string();

                // Prune all but the current execution (keep_last: 1)
                let result = client
                    .prune_executions(
                        &instance_id,
                        PruneOptions {
                            keep_last: Some(1),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| e.to_string())?;

                prune_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                executions_pruned.fetch_add(result.executions_deleted, std::sync::atomic::Ordering::SeqCst);

                Ok(format!("Pruned {} executions", result.executions_deleted))
            }
        })
        .build();

    // Eternal orchestration that processes 5 batches then completes
    let orchestration = |ctx: OrchestrationContext, state_str: String| async move {
        #[derive(Serialize, Deserialize)]
        struct State {
            batch_num: u32,
            total_batches: u32,
        }

        let state: State = serde_json::from_str(&state_str).unwrap_or(State {
            batch_num: 0,
            total_batches: 5,
        });

        // Process current batch
        let _result = ctx
            .schedule_activity("ProcessBatch", state.batch_num.to_string())
            .await?;

        // Prune old executions (keep only current) - do this on every iteration
        let _prune_result = ctx.schedule_activity("PruneSelf", "".to_string()).await?;

        if state.batch_num >= state.total_batches - 1 {
            // Done processing all batches (after pruning)
            return Ok(format!("Completed {} batches", state.total_batches));
        }

        // Continue with next batch
        let next_state = State {
            batch_num: state.batch_num + 1,
            total_batches: state.total_batches,
        };
        ctx.continue_as_new(serde_json::to_string(&next_state).unwrap()).await
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("SelfPruningOrch", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;

    let client = Client::new(store.clone());

    // Start the self-pruning orchestration
    client
        .start_orchestration("inst-self-prune", "SelfPruningOrch", "{}")
        .await
        .unwrap();

    // Wait for completion (5 batches = 5 executions, prune after each)
    match client
        .wait_for_orchestration("inst-self-prune", std::time::Duration::from_secs(30))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("Completed 5 batches"));
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Verify pruning occurred (5 times, once per batch)
    let prunes = prune_count.load(std::sync::atomic::Ordering::SeqCst);
    assert!(prunes >= 4, "Should have pruned at least 4 times");

    let pruned = executions_pruned.load(std::sync::atomic::Ordering::SeqCst);
    assert!(pruned >= 3, "Should have pruned at least 3 executions total");

    // Verify only 1 execution remains (the final one)
    let executions = client.list_executions("inst-self-prune").await.unwrap();
    assert_eq!(
        executions.len(),
        1,
        "Only final execution should remain after self-pruning"
    );

    rt.shutdown(None).await;
}

/// Config hot-reload with persistent events: a long-running orchestration checks for
/// config updates between activity cycles. Updates arrive independently — zero, one,
/// or many between cycles — and none are lost.
///
/// Highlights:
/// - `dequeue_event("ConfigUpdate")` buffers arrivals until consumed
/// - `select2(persistent_wait, timer)` drains buffered updates without blocking
/// - Events that arrive while the orchestration is busy are queued, not dropped
/// - Demonstrates the "mailbox" pattern: decouple sender timing from consumer readiness
///
/// Note: Events enqueued *before* the orchestration starts may be dropped by the
/// provider (orphan message semantics). This test only enqueues events after start.
#[tokio::test]
async fn sample_config_hot_reload_persistent_events_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Activity that "applies" a config value
    let activity_registry = ActivityRegistry::builder()
        .register("ApplyConfig", |_ctx: ActivityContext, config: String| async move {
            // In production this would reconfigure a service, toggle a feature flag, etc.
            Ok(format!("applied:{config}"))
        })
        .build();

    // Orchestration: run 3 work cycles. Between each cycle, drain any pending
    // ConfigUpdate events that arrived (zero or more).
    // After the final cycle, do one last drain to pick up any late arrivals.
    // Emits cycle boundary markers so we can verify drain-to-cycle assignment.
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let mut log: Vec<String> = Vec::new();

        for cycle in 0..3 {
            // Drain pending config updates (non-blocking)
            loop {
                let config_wait = ctx.dequeue_event("ConfigUpdate");
                let drain_timeout = ctx.schedule_timer(Duration::from_millis(100));
                match ctx.select2(config_wait, drain_timeout).await {
                    Either2::First(config_json) => {
                        let result = ctx.schedule_activity("ApplyConfig", &config_json).await?;
                        log.push(result);
                    }
                    Either2::Second(_) => break, // no more pending updates
                }
            }

            // Mark cycle boundary, then simulate work.
            // Use a longer timer (1s) so there is a wide window for v3 to arrive mid-flight.
            ctx.schedule_timer(Duration::from_millis(1000)).await;
            let _ = ctx.schedule_activity("ApplyConfig", format!("cycle_{cycle}")).await?;
            log.push(format!("cycle:{cycle}"));
        }

        // Final drain: pick up any events that arrived during the last cycle
        loop {
            let config_wait = ctx.dequeue_event("ConfigUpdate");
            let drain_timeout = ctx.schedule_timer(Duration::from_millis(100));
            match ctx.select2(config_wait, drain_timeout).await {
                Either2::First(config_json) => {
                    let result = ctx.schedule_activity("ApplyConfig", &config_json).await?;
                    log.push(result);
                }
                Either2::Second(_) => break,
            }
        }

        Ok(log.join(","))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ConfigHotReload", orchestration)
        .build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    // Start the orchestration first — events enqueued before start may be
    // dropped by the provider (orphan message semantics).
    client
        .start_orchestration("inst-hot-reload", "ConfigHotReload", "")
        .await
        .unwrap();

    // Push two config updates immediately after start.
    // The orchestration's first drain cycle will pick them up.
    client
        .enqueue_event("inst-hot-reload", "ConfigUpdate", "v1")
        .await
        .unwrap();
    client
        .enqueue_event("inst-hot-reload", "ConfigUpdate", "v2")
        .await
        .unwrap();

    // Wait for the orchestration to progress past cycle:0's drain phase before
    // sending v3. A fixed delay is unreliable because dispatch latency is
    // variable under load. Instead, observe history for the cycle_0
    // activity being scheduled, which proves the drain loop has completed,
    // the 1s cycle timer has fired, and the cycle boundary activity is in flight.
    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-hot-reload",
            |hist| {
                hist.iter().any(|e| {
                    matches!(
                        &e.kind,
                        EventKind::ActivityScheduled { input, .. } if input == "cycle_0"
                    )
                })
            },
            15_000,
        )
        .await,
        "Timed out waiting for cycle:0 to progress past its drain phase"
    );
    client
        .enqueue_event("inst-hot-reload", "ConfigUpdate", "v3")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-hot-reload", Duration::from_secs(20))
        .await
        .unwrap();
    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            let entries: Vec<&str> = output.split(',').collect();

            // All three events should be present
            let pos_v1 = entries
                .iter()
                .position(|e| *e == "applied:v1")
                .unwrap_or_else(|| panic!("v1 missing from output: {output}"));
            let pos_v2 = entries
                .iter()
                .position(|e| *e == "applied:v2")
                .unwrap_or_else(|| panic!("v2 missing from output: {output}"));
            let pos_v3 = entries
                .iter()
                .position(|e| *e == "applied:v3")
                .unwrap_or_else(|| panic!("v3 missing from output: {output}"));
            let pos_cycle0 = entries
                .iter()
                .position(|e| *e == "cycle:0")
                .unwrap_or_else(|| panic!("cycle:0 missing from output: {output}"));

            // FIFO ordering: v1 must come before v2, v2 before v3
            assert!(pos_v1 < pos_v2, "v1 should come before v2 (FIFO), got: {output}");
            assert!(pos_v2 < pos_v3, "v2 should come before v3 (FIFO), got: {output}");

            // v3 arrived mid-flight → must come after cycle:0
            assert!(
                pos_v3 > pos_cycle0,
                "v3 arrived mid-flight, should be drained after cycle:0, got: {output}"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

// ============================================================================
// Activity Tagging Samples
// ============================================================================

/// Heterogeneous workers: route activities to specialized workers by tag.
///
/// Pattern: An orchestration fans out work across GPU and CPU workers.
/// Each worker runtime is configured with a different `TagFilter` so it only
/// picks up activities meant for its hardware class (GPU vs CPU).
/// A generalist worker using `DefaultAnd` can handle untagged activities and
/// a specific tag.
///
/// Highlights:
/// - `.with_tag("gpu")` routes an activity to GPU-capable workers
/// - `TagFilter::tags(["gpu"])` — specialist worker accepts only GPU work
/// - `TagFilter::default_and(["cpu"])` — generalist handles untagged + CPU
/// - `TagFilter::Any` — catch-all worker for draining / debugging
/// - Tags are persisted in event history and replayed deterministically
#[tokio::test]
async fn sample_heterogeneous_workers_with_tags() {
    use duroxide::TagFilter;
    use duroxide::runtime::RuntimeOptions;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Render", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("rendered:{input}"))
        })
        .register("Encode", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("encoded:{input}"))
        })
        .register("Upload", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("uploaded:{input}"))
        })
        .build();

    // Orchestration: fan out => GPU render, CPU encode, untagged upload
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // GPU-intensive rendering
        let rendered = ctx.schedule_activity("Render", "frame42").with_tag("gpu").await?;

        // CPU-intensive encoding (could run on a different machine class)
        let encoded = ctx.schedule_activity("Encode", rendered).with_tag("cpu").await?;

        // Plain upload — any default worker can handle this
        let uploaded = ctx.schedule_activity("Upload", encoded).await?;

        Ok(uploaded)
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("VideoPipeline", orchestration)
        .build();

    // Single runtime with DefaultAnd(["gpu","cpu"]) — handles everything here,
    // but in production you'd run separate runtimes per machine class.
    let opts = RuntimeOptions {
        worker_tag_filter: TagFilter::default_and(["gpu", "cpu"]),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, opts).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("video-1", "VideoPipeline", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("video-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "uploaded:encoded:rendered:frame42");

            // Verify tags are persisted in history
            let history = store.read("video-1").await.unwrap();
            let tags: Vec<Option<String>> = history
                .iter()
                .filter_map(|e| match &e.kind {
                    EventKind::ActivityScheduled { tag, .. } => Some(tag.clone()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                tags,
                vec![Some("gpu".to_string()), Some("cpu".to_string()), None],
                "History should preserve per-activity tags"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Starvation-safe tagged activity: protect against missing workers with a timeout.
///
/// Pattern: Schedule a tagged activity but race it against a timer so the
/// orchestration never hangs indefinitely if no worker has the right TagFilter.
/// This is the recommended safety pattern whenever you use `.with_tag()`.
///
/// Highlights:
/// - `select2(tagged_activity, timer)` — first-to-complete wins
/// - `Either2::Second` means the timer fired (no matching worker available)
/// - Graceful fallback instead of unbounded hang
/// - Works with any tag / TagFilter combination
#[tokio::test]
async fn sample_starvation_safe_tagged_activity() {
    use duroxide::TagFilter;
    use duroxide::runtime::RuntimeOptions;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("GpuInference", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("inference:{input}"))
        })
        .register("CpuFallback", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("cpu_fallback:{input}"))
        })
        .build();

    // Orchestration: try GPU, fall back to CPU if no GPU worker responds in time
    let orchestration = |ctx: OrchestrationContext, input: String| async move {
        let gpu_activity = ctx.schedule_activity("GpuInference", input.clone()).with_tag("gpu");
        let timeout = ctx.schedule_timer(Duration::from_millis(500));

        match ctx.select2(gpu_activity, timeout).await {
            Either2::First(Ok(result)) => {
                // GPU worker picked it up in time
                Ok(result)
            }
            Either2::First(Err(e)) => Err(e),
            Either2::Second(()) => {
                // No GPU worker available — fall back to CPU
                let result = ctx.schedule_activity("CpuFallback", input).await?;
                Ok(result)
            }
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("InferenceWithFallback", orchestration)
        .build();

    // Only a DefaultOnly worker — no GPU workers exist
    let opts = RuntimeOptions {
        worker_tag_filter: TagFilter::default(),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, opts).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("infer-1", "InferenceWithFallback", "model-v3")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("infer-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Timer wins, CPU fallback executes
            assert_eq!(output, "cpu_fallback:model-v3");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Dual runtimes: orchestrator + specialist GPU worker on the same store.
///
/// Pattern: Two separate `Runtime` instances share the same backing store.
/// Runtime A runs the orchestration dispatcher and handles untagged (default)
/// activities. Runtime B is a worker-only node that handles only gpu-tagged
/// activities. The orchestration schedules both kinds, proving that the two
/// runtimes cooperate to drive it to completion.
///
/// Highlights:
/// - Two `Runtime::start_with_options` on the same store (shared queue)
/// - Runtime A: orchestration dispatcher + `DefaultOnly` worker
/// - Runtime B: orchestration dispatcher disabled (`orchestration_concurrency: 0`)
///   + `Tags(["gpu"])` worker — pure activity executor
/// - The orchestration completes only if **both** runtimes participate
#[tokio::test]
async fn sample_dual_runtime_tag_cooperation() {
    use duroxide::TagFilter;
    use duroxide::runtime::RuntimeOptions;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // --- Shared registries (both runtimes need the same activity names) ---
    let make_activities = || {
        ActivityRegistry::builder()
            .register("PreProcess", |_ctx: ActivityContext, input: String| async move {
                Ok(format!("preprocessed:{input}"))
            })
            .register("GpuTrain", |_ctx: ActivityContext, input: String| async move {
                // In production this would run on a GPU node
                Ok(format!("trained:{input}"))
            })
            .register("SaveModel", |_ctx: ActivityContext, input: String| async move {
                Ok(format!("saved:{input}"))
            })
            .build()
    };

    let make_orchestrations = || {
        OrchestrationRegistry::builder()
            .register("MLPipeline", |ctx: OrchestrationContext, input: String| async move {
                // Step 1: CPU preprocessing (untagged — handled by Runtime A)
                let preprocessed = ctx.schedule_activity("PreProcess", input).await?;

                // Step 2: GPU training (tagged — handled by Runtime B)
                let model = ctx.schedule_activity("GpuTrain", preprocessed).with_tag("gpu").await?;

                // Step 3: Save model (untagged — handled by Runtime A)
                let saved = ctx.schedule_activity("SaveModel", model).await?;

                Ok(saved)
            })
            .build()
    };

    // --- Runtime A: orchestrator + default worker (CPU work) ---
    let rt_a = runtime::Runtime::start_with_options(
        store.clone(),
        make_activities(),
        make_orchestrations(),
        RuntimeOptions {
            worker_tag_filter: TagFilter::default(),
            ..Default::default()
        },
    )
    .await;

    // --- Runtime B: GPU worker only (no orchestration dispatcher) ---
    let rt_b = runtime::Runtime::start_with_options(
        store.clone(),
        make_activities(),
        make_orchestrations(),
        RuntimeOptions {
            orchestration_concurrency: 0, // worker-only node
            worker_tag_filter: TagFilter::tags(["gpu"]),
            ..Default::default()
        },
    )
    .await;

    // Start the orchestration — it needs BOTH runtimes to complete
    let client = Client::new(store.clone());
    client
        .start_orchestration("ml-1", "MLPipeline", "dataset-v5")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("ml-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "saved:trained:preprocessed:dataset-v5");

            // Verify the tag routing in history
            let history = store.read("ml-1").await.unwrap();
            let scheduled: Vec<(&str, Option<&str>)> = history
                .iter()
                .filter_map(|e| match &e.kind {
                    EventKind::ActivityScheduled { name, tag, .. } => Some((name.as_str(), tag.as_deref())),
                    _ => None,
                })
                .collect();
            assert_eq!(
                scheduled,
                vec![("PreProcess", None), ("GpuTrain", Some("gpu")), ("SaveModel", None),],
                "History should show correct tag routing"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt_b.shutdown(None).await;
    rt_a.shutdown(None).await;
}

/// KV Store: Client ↔ Orchestration request/response via KV + external events.
///
/// Pattern: A long-running "server" orchestration receives requests via external
/// events and writes responses to its KV store. Clients poll `client.get_kv_value()`
/// to read responses. This eliminates the need for a separate response channel.
///
/// Highlights:
/// - `ctx.set_kv_value()` / `ctx.get_kv_value()` for in-orchestration state
/// - `client.get_kv_value()` for external reads of orchestration KV
/// - `ctx.schedule_wait()` to receive requests as external events
/// - Request/response correlation via KV keys like `"response:{op_id}"`
/// - KV survives replay — safe across restarts
#[tokio::test]
async fn sample_kv_request_response() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("ProcessCommand", |_ctx: ActivityContext, input: String| async move {
            // Simulate processing: reverse the input string
            Ok(input.chars().rev().collect::<String>())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "RequestServer",
            |ctx: OrchestrationContext, _input: String| async move {
                ctx.set_kv_value("status", "ready");

                // Process up to 3 requests then shut down
                for _ in 0..3 {
                    // Wait for a request event
                    let request_json = ctx.schedule_wait("request").await;
                    let request: serde_json::Value = serde_json::from_str(&request_json).unwrap();
                    let op_id = request["op_id"].as_str().unwrap().to_string();
                    let command = request["command"].as_str().unwrap().to_string();

                    ctx.set_kv_value("status", "processing");

                    // Process the command via an activity
                    let result = ctx
                        .schedule_activity("ProcessCommand", command)
                        .await
                        .unwrap_or_else(|e| format!("error: {e}"));

                    // Write response to KV keyed by op_id
                    ctx.set_kv_value(format!("response:{op_id}"), &result);
                    ctx.set_kv_value("status", "ready");
                }

                ctx.set_kv_value("status", "shutdown");
                Ok("served 3 requests".to_string())
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, Default::default()).await;

    let client = Client::new(store.clone());

    // Start the server orchestration
    client
        .start_orchestration("req-resp-server", "RequestServer", "")
        .await
        .unwrap();

    // Wait for the server to be ready
    let status = client
        .wait_for_kv_value("req-resp-server", "status", Duration::from_secs(5))
        .await
        .expect("Server never became ready");
    assert_eq!(status, "ready");

    // Send 3 requests and verify each response
    let requests = vec![("op-1", "hello"), ("op-2", "world"), ("op-3", "rust")];

    for (op_id, command) in &requests {
        let event_data = serde_json::json!({ "op_id": op_id, "command": command }).to_string();
        client
            .raise_event("req-resp-server", "request", &event_data)
            .await
            .unwrap();

        // Wait for response
        let response_key = format!("response:{op_id}");
        let response = client
            .wait_for_kv_value("req-resp-server", &response_key, Duration::from_secs(5))
            .await
            .unwrap_or_else(|_| panic!("Timed out waiting for response to {op_id}"));

        let expected: String = command.chars().rev().collect();
        assert_eq!(
            response, expected,
            "Response for {op_id} should be the reversed command"
        );
    }

    // Wait for server to complete
    match client
        .wait_for_orchestration("req-resp-server", Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "served 3 requests");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    // All KV values still accessible after completion
    assert_eq!(
        client.get_kv_value("req-resp-server", "status").await.unwrap(),
        Some("shutdown".to_string())
    );
    assert_eq!(
        client.get_kv_value("req-resp-server", "response:op-1").await.unwrap(),
        Some("olleh".to_string())
    );
    assert_eq!(
        client.get_kv_value("req-resp-server", "response:op-2").await.unwrap(),
        Some("dlrow".to_string())
    );
    assert_eq!(
        client.get_kv_value("req-resp-server", "response:op-3").await.unwrap(),
        Some("tsur".to_string())
    );

    rt.shutdown(None).await;
}

/// KV Store: Cross-orchestration reads via `get_value_from_instance()`.
///
/// Pattern: Orchestration B reads KV values set by Orchestration A using
/// the system activity `get_value_from_instance()`. This enables coordination
/// between independent orchestrations without shared state or external systems.
///
/// Highlights:
/// - `ctx.get_kv_value_from_instance()` reads another orchestration's KV via a system activity
/// - Cross-instance reads are replay-safe (recorded as `ActivityCompleted`)
/// - Values set by one orchestration are immediately visible to others (after ack)
/// - `client.wait_for_kv_value()` for efficient polling
#[tokio::test]
async fn sample_kv_cross_orchestration_read() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder()
        .register("ComputeResult", |_ctx: ActivityContext, input: String| async move {
            let n: i64 = input.parse().map_err(|e| format!("parse: {e}"))?;
            Ok((n * n).to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        // Producer: computes values and stores them in KV
        .register("Producer", |ctx: OrchestrationContext, input: String| async move {
            let n: i64 = input.parse().unwrap();
            ctx.set_kv_value("status", "computing");

            let squared = ctx.schedule_activity("ComputeResult", n.to_string()).await?;
            ctx.set_kv_value("result", &squared);
            ctx.set_kv_value("status", "done");

            // Wait for consumer to acknowledge before completing
            ctx.schedule_wait("ack").await;
            Ok(format!("produced:{squared}"))
        })
        // Consumer: reads the producer's KV values
        .register(
            "Consumer",
            |ctx: OrchestrationContext, producer_id: String| async move {
                // Poll producer's status via cross-instance read
                let mut attempts = 0;
                loop {
                    let status = ctx
                        .get_kv_value_from_instance(&producer_id, "status")
                        .await
                        .map_err(|e| format!("read status: {e}"))?;
                    if status.as_deref() == Some("done") {
                        break;
                    }
                    attempts += 1;
                    if attempts > 20 {
                        return Err("producer never finished".to_string());
                    }
                    ctx.schedule_timer(Duration::from_millis(100)).await;
                }

                // Read the computed result from producer's KV
                let result = ctx
                    .get_kv_value_from_instance(&producer_id, "result")
                    .await
                    .map_err(|e| format!("read result: {e}"))?;

                let result = result.ok_or_else(|| "result key missing".to_string())?;

                Ok(format!("consumed:{result}"))
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, Default::default()).await;

    let client = Client::new(store.clone());

    // Start the producer
    client.start_orchestration("producer-1", "Producer", "7").await.unwrap();

    // Wait for producer to have a result via client polling
    let result = client
        .wait_for_kv_value("producer-1", "result", Duration::from_secs(5))
        .await
        .expect("Producer never set result");
    assert_eq!(result, "49"); // 7*7

    // Start the consumer, pointing it at the producer
    client
        .start_orchestration("consumer-1", "Consumer", "producer-1")
        .await
        .unwrap();

    // Wait for consumer to complete
    match client
        .wait_for_orchestration("consumer-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "consumed:49");
        }
        other => panic!("Expected consumer Completed, got: {other:?}"),
    }

    // Acknowledge the producer so it completes too
    client.raise_event("producer-1", "ack", "").await.unwrap();

    match client
        .wait_for_orchestration("producer-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "produced:49");
        }
        other => panic!("Expected producer Completed, got: {other:?}"),
    }

    // Cross-instance reads also work via client after both are terminal
    assert_eq!(
        client.get_kv_value("producer-1", "result").await.unwrap(),
        Some("49".to_string())
    );
    assert_eq!(
        client.get_kv_value("producer-1", "status").await.unwrap(),
        Some("done".to_string())
    );

    rt.shutdown(None).await;
}

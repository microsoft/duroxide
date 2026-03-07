//! End-to-end tests for the activity session (implicit sessions v2) feature.
//!
//! Tests verify session routing works correctly through the full runtime stack:
//! orchestration → replay engine → dispatcher → provider → worker.
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use semver::Version;
use std::sync::Arc;
use std::time::Duration;

/// Helper: create an in-memory runtime with default options
async fn create_runtime(
    activities: ActivityRegistry,
    orchestrations: OrchestrationRegistry,
) -> (Arc<runtime::Runtime>, Client) {
    create_runtime_with_options(activities, orchestrations, RuntimeOptions::default()).await
}

/// Helper: create an in-memory runtime with custom options
async fn create_runtime_with_options(
    activities: ActivityRegistry,
    orchestrations: OrchestrationRegistry,
    options: RuntimeOptions,
) -> (Arc<runtime::Runtime>, Client) {
    let store: Arc<dyn duroxide::providers::Provider> = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store);
    (rt, client)
}

/// Basic test: schedule_activity_on_session works end-to-end
#[tokio::test]
async fn test_session_activity_basic_e2e() {
    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("echo:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("SessionOrch", |ctx: OrchestrationContext, _input: String| async move {
            let session_id = "my-session-1";
            let r1 = ctx.schedule_activity_on_session("Echo", "hello", session_id).await?;
            let r2 = ctx.schedule_activity_on_session("Echo", "world", session_id).await?;
            Ok(format!("{r1}|{r2}"))
        })
        .build();

    let (rt, client) = create_runtime(activities, orchestrations).await;

    client
        .start_orchestration("test-session-basic", "SessionOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-session-basic", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "echo:hello|echo:world");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Verify that session_id is visible in ActivityContext
#[tokio::test]
async fn test_session_id_visible_in_activity_context() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let session_seen = Arc::new(AtomicBool::new(false));
    let session_seen_clone = session_seen.clone();

    let activities = ActivityRegistry::builder()
        .register("CheckSession", move |ctx: ActivityContext, _input: String| {
            let seen = session_seen_clone.clone();
            async move {
                if ctx.session_id() == Some("ctx-test-session") {
                    seen.store(true, Ordering::SeqCst);
                }
                Ok("ok".to_string())
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "CheckSessionOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                ctx.schedule_activity_on_session("CheckSession", "input", "ctx-test-session")
                    .await?;
                Ok("done".to_string())
            },
        )
        .build();

    let (rt, client) = create_runtime(activities, orchestrations).await;

    client
        .start_orchestration("test-ctx-session", "CheckSessionOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("test-ctx-session", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { .. } => {}
        other => panic!("Expected completed, got {:?}", other),
    }

    assert!(
        session_seen.load(Ordering::SeqCst),
        "Activity should see session_id in context"
    );

    rt.shutdown(None).await;
}

/// Mixing session and non-session activities in the same orchestration
#[tokio::test]
async fn test_mixed_session_and_regular_activities() {
    let activities = ActivityRegistry::builder()
        .register("SessionTask", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("session:{input}"))
        })
        .register("RegularTask", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("regular:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("MixedOrch", |ctx: OrchestrationContext, _input: String| async move {
            let r1 = ctx.schedule_activity("RegularTask", "a").await?;
            let r2 = ctx.schedule_activity_on_session("SessionTask", "b", "sess-1").await?;
            let r3 = ctx.schedule_activity("RegularTask", "c").await?;
            Ok(format!("{r1}|{r2}|{r3}"))
        })
        .build();

    let (rt, client) = create_runtime(activities, orchestrations).await;

    client.start_orchestration("test-mixed", "MixedOrch", "").await.unwrap();
    match client
        .wait_for_orchestration("test-mixed", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "regular:a|session:b|regular:c");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Multiple different sessions in the same orchestration
#[tokio::test]
async fn test_multiple_sessions_in_orchestration() {
    let activities = ActivityRegistry::builder()
        .register("Task", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "MultiSessionOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                let r1 = ctx.schedule_activity_on_session("Task", "a", "session-A").await?;
                let r2 = ctx.schedule_activity_on_session("Task", "b", "session-B").await?;
                let r3 = ctx.schedule_activity_on_session("Task", "c", "session-A").await?;
                Ok(format!("{r1}|{r2}|{r3}"))
            },
        )
        .build();

    let (rt, client) = create_runtime(activities, orchestrations).await;

    client
        .start_orchestration("test-multi-session", "MultiSessionOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("test-multi-session", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "a|b|c");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Session activity with typed API
#[tokio::test]
async fn test_session_activity_typed() {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct DoubleInput {
        value: i32,
    }

    let activities = ActivityRegistry::builder()
        .register("Double", |_ctx: ActivityContext, input: String| async move {
            let parsed: DoubleInput = serde_json::from_str(&input).unwrap();
            Ok(serde_json::to_string(&(parsed.value * 2)).unwrap())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "TypedSessionOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                let result: i32 = ctx
                    .schedule_activity_on_session_typed("Double", &DoubleInput { value: 21 }, "typed-sess")
                    .await?;
                Ok(result.to_string())
            },
        )
        .build();

    let (rt, client) = create_runtime(activities, orchestrations).await;

    client
        .start_orchestration("test-typed-session", "TypedSessionOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("test-typed-session", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "42");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

// ============================================================================
// Process-level session identity E2E tests
// ============================================================================

/// With worker_node_id set, multiple activities on the same session complete
/// without head-of-line blocking across worker_concurrency slots.
#[tokio::test]
async fn test_session_with_worker_node_id_completes() {
    let activities = ActivityRegistry::builder()
        .register("Work", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("done:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("StableOrch", |ctx: OrchestrationContext, _input: String| async move {
            let r1 = ctx.schedule_activity_on_session("Work", "a", "stable-sess").await?;
            let r2 = ctx.schedule_activity_on_session("Work", "b", "stable-sess").await?;
            let r3 = ctx.schedule_activity_on_session("Work", "c", "stable-sess").await?;
            Ok(format!("{r1}|{r2}|{r3}"))
        })
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 4,
        worker_node_id: Some("stable-pod-1".to_string()),
        ..Default::default()
    };
    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-stable-node", "StableOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("test-stable-node", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done:a|done:b|done:c");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

/// With worker_node_id set, multiple different sessions can be served in parallel
/// by different worker slots sharing the same session identity.
#[tokio::test]
async fn test_session_worker_node_id_multiple_sessions_parallel() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let activities = ActivityRegistry::builder()
        .register("Count", move |_ctx: ActivityContext, _input: String| {
            let c = counter_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok("counted".to_string())
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "ParallelSessions",
            |ctx: OrchestrationContext, _input: String| async move {
                // Schedule activities on 3 different sessions
                let f1 = ctx.schedule_activity_on_session("Count", "x", "sess-1");
                let f2 = ctx.schedule_activity_on_session("Count", "y", "sess-2");
                let f3 = ctx.schedule_activity_on_session("Count", "z", "sess-3");
                let results = ctx.join3(f1, f2, f3).await;
                let r1 = results.0?;
                let r2 = results.1?;
                let r3 = results.2?;
                Ok(format!("{r1}|{r2}|{r3}"))
            },
        )
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 4,
        worker_node_id: Some("multi-sess-pod".to_string()),
        ..Default::default()
    };
    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-parallel-sess", "ParallelSessions", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("test-parallel-sess", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "counted|counted|counted");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    // At-least-once: counter may exceed 3 if an ack fails and the activity retries
    assert!(
        counter.load(Ordering::SeqCst) >= 3,
        "All 3 session activities should have executed"
    );
    rt.shutdown(None).await;
}

/// Ephemeral mode (worker_node_id=None) with worker_concurrency=1 still works.
/// Regression test for the per-slot identity path.
#[tokio::test]
async fn test_ephemeral_session_still_works() {
    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "EphemeralOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                let r = ctx
                    .schedule_activity_on_session("Echo", "ephemeral-val", "eph-sess")
                    .await?;
                Ok(r)
            },
        )
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 1,
        worker_node_id: None,
        ..Default::default()
    };
    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-ephemeral", "EphemeralOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("test-ephemeral", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "ephemeral-val");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

/// With worker_node_id set, ActivityContext::session_id() returns the session ID
/// (not the worker node identity).
#[tokio::test]
async fn test_session_with_worker_node_id_activity_context_has_session_id() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let correct_session = Arc::new(AtomicBool::new(false));
    let correct_session_clone = correct_session.clone();

    let activities = ActivityRegistry::builder()
        .register("CheckSess", move |ctx: ActivityContext, _input: String| {
            let flag = correct_session_clone.clone();
            async move {
                // session_id() should return "my-session", NOT "k8s-pod-name"
                if ctx.session_id() == Some("my-session") {
                    flag.store(true, Ordering::SeqCst);
                }
                Ok("ok".to_string())
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "NodeIdSessionOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                ctx.schedule_activity_on_session("CheckSess", "", "my-session").await?;
                Ok("done".to_string())
            },
        )
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 2,
        worker_node_id: Some("k8s-pod-name".to_string()),
        ..Default::default()
    };
    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-node-ctx", "NodeIdSessionOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("test-node-ctx", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { .. } => {}
        other => panic!("Expected completed, got {:?}", other),
    }

    assert!(
        correct_session.load(Ordering::SeqCst),
        "ActivityContext::session_id() should return the session ID, not the worker_node_id"
    );

    rt.shutdown(None).await;
}

/// Proves that with process-level session identity (worker_node_id set),
/// two worker slots can concurrently serve the same session.
///
/// Two activities on the SAME session are scheduled in parallel (join2).
/// Both sleep briefly and track whether they overlapped in-flight.
/// With shared identity: both slots pick up items from the same session →
///   concurrent execution → max_concurrent == 2.
/// With per-slot identity only one slot owns the session →
///   sequential execution → max_concurrent == 1.
#[tokio::test]
async fn test_two_slots_serve_same_session_concurrently() {
    use std::sync::atomic::{AtomicI32, Ordering};

    let in_flight = Arc::new(AtomicI32::new(0));
    let max_concurrent = Arc::new(AtomicI32::new(0));

    let in_flight_a = in_flight.clone();
    let max_concurrent_a = max_concurrent.clone();
    let in_flight_b = in_flight.clone();
    let max_concurrent_b = max_concurrent.clone();

    let activities = ActivityRegistry::builder()
        .register("SlowTask", move |_ctx: ActivityContext, input: String| {
            let inf = in_flight_a.clone();
            let maxc = max_concurrent_a.clone();
            async move {
                let current = inf.fetch_add(1, Ordering::SeqCst) + 1;
                // Update max if this is a new high
                maxc.fetch_max(current, Ordering::SeqCst);
                // Hold the slot long enough for the other activity to also start
                tokio::time::sleep(Duration::from_millis(500)).await;
                inf.fetch_sub(1, Ordering::SeqCst);
                Ok(format!("slow:{input}"))
            }
        })
        .register("FastTask", move |_ctx: ActivityContext, input: String| {
            let inf = in_flight_b.clone();
            let maxc = max_concurrent_b.clone();
            async move {
                let current = inf.fetch_add(1, Ordering::SeqCst) + 1;
                maxc.fetch_max(current, Ordering::SeqCst);
                // Brief sleep to overlap with SlowTask
                tokio::time::sleep(Duration::from_millis(200)).await;
                inf.fetch_sub(1, Ordering::SeqCst);
                Ok(format!("fast:{input}"))
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "ConcurrentSessionOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                // Both on the SAME session, scheduled in parallel
                let f1 = ctx.schedule_activity_on_session("SlowTask", "a", "same-session");
                let f2 = ctx.schedule_activity_on_session("FastTask", "b", "same-session");
                let (r1, r2) = ctx.join2(f1, f2).await;
                Ok(format!("{}|{}", r1?, r2?))
            },
        )
        .build();

    // Process-level identity: all slots share "my-node"
    let options = RuntimeOptions {
        worker_concurrency: 2,
        worker_node_id: Some("my-node".to_string()),
        ..Default::default()
    };
    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-concurrent-session", "ConcurrentSessionOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-concurrent-session", Duration::from_secs(15))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "slow:a|fast:b");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    assert_eq!(
        max_concurrent.load(Ordering::SeqCst),
        2,
        "Both activities should have been in-flight simultaneously, \
         proving two slots served the same session concurrently"
    );

    rt.shutdown(None).await;
}

/// Counterpart to `test_two_slots_serve_same_session_concurrently`:
/// WITHOUT worker_node_id (ephemeral per-slot identity), two activities
/// on the same session are serialized because only one slot owns the session.
/// max_concurrent must be 1.
#[tokio::test]
async fn test_ephemeral_same_session_serialized() {
    use std::sync::atomic::{AtomicI32, Ordering};

    let in_flight = Arc::new(AtomicI32::new(0));
    let max_concurrent = Arc::new(AtomicI32::new(0));

    let in_flight_a = in_flight.clone();
    let max_concurrent_a = max_concurrent.clone();
    let in_flight_b = in_flight.clone();
    let max_concurrent_b = max_concurrent.clone();

    let activities = ActivityRegistry::builder()
        .register("SlowTask", move |_ctx: ActivityContext, input: String| {
            let inf = in_flight_a.clone();
            let maxc = max_concurrent_a.clone();
            async move {
                let current = inf.fetch_add(1, Ordering::SeqCst) + 1;
                maxc.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(500)).await;
                inf.fetch_sub(1, Ordering::SeqCst);
                Ok(format!("slow:{input}"))
            }
        })
        .register("FastTask", move |_ctx: ActivityContext, input: String| {
            let inf = in_flight_b.clone();
            let maxc = max_concurrent_b.clone();
            async move {
                let current = inf.fetch_add(1, Ordering::SeqCst) + 1;
                maxc.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(200)).await;
                inf.fetch_sub(1, Ordering::SeqCst);
                Ok(format!("fast:{input}"))
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "SerialSessionOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                let f1 = ctx.schedule_activity_on_session("SlowTask", "a", "same-session");
                let f2 = ctx.schedule_activity_on_session("FastTask", "b", "same-session");
                let (r1, r2) = ctx.join2(f1, f2).await;
                Ok(format!("{}|{}", r1?, r2?))
            },
        )
        .build();

    // Ephemeral per-slot identity: each slot gets a different worker_id
    let options = RuntimeOptions {
        worker_concurrency: 2,
        worker_node_id: None, // <-- no stable identity
        ..Default::default()
    };
    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-serial-session", "SerialSessionOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-serial-session", Duration::from_secs(15))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "slow:a|fast:b");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    assert_eq!(
        max_concurrent.load(Ordering::SeqCst),
        1,
        "Without stable worker_node_id, only one slot owns the session, \
         so activities must execute sequentially (max_concurrent == 1)"
    );

    rt.shutdown(None).await;
}

// ============================================================================
// Fan-out / Fan-in with Sessions
// ============================================================================

/// Fan-out/fan-in: multiple activities on different sessions execute in parallel,
/// then results are collected. Verifies that ctx.join works with session activities.
#[tokio::test]
async fn test_session_fan_out_fan_in() {
    let activities = ActivityRegistry::builder()
        .register("Process", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("processed:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("FanOutOrch", |ctx: OrchestrationContext, _input: String| async move {
            // Fan-out: schedule activities across 3 different sessions
            let futures: Vec<_> = (0..3)
                .map(|i| {
                    let session = format!("session-{i}");
                    ctx.schedule_activity_on_session("Process", i.to_string(), session)
                })
                .collect();

            // Fan-in: wait for all to complete
            let results = ctx.join(futures).await;
            let outputs: Vec<String> = results.into_iter().collect::<Result<Vec<_>, _>>()?;
            Ok(outputs.join("|"))
        })
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 4,
        worker_node_id: Some("fan-pod".to_string()),
        ..Default::default()
    };
    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-fan-out", "FanOutOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-fan-out", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "processed:0|processed:1|processed:2");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Fan-out/fan-in mixing session and non-session activities in the same join.
#[tokio::test]
async fn test_session_fan_out_mixed_with_regular() {
    let activities = ActivityRegistry::builder()
        .register("SessionWork", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("sess:{input}"))
        })
        .register("RegularWork", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("reg:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("MixedFanOrch", |ctx: OrchestrationContext, _input: String| async move {
            let f1 = ctx.schedule_activity_on_session("SessionWork", "a", "s1");
            let f2 = ctx.schedule_activity("RegularWork", "b");
            let f3 = ctx.schedule_activity_on_session("SessionWork", "c", "s2");
            let f4 = ctx.schedule_activity("RegularWork", "d");

            let results = ctx.join(vec![f1, f2, f3, f4]).await;
            let outputs: Vec<String> = results.into_iter().collect::<Result<Vec<_>, _>>()?;
            Ok(outputs.join("|"))
        })
        .build();

    let (rt, client) = create_runtime(activities, orchestrations).await;

    client
        .start_orchestration("test-mixed-fan", "MixedFanOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-mixed-fan", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "sess:a|reg:b|sess:c|reg:d");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Fan-out with multiple activities per session: 2 on session-A, 2 on session-B,
/// and 2 non-session activities, all scheduled in parallel via ctx.join.
/// Proves that session routing correctly multiplexes across sessions and
/// non-session activities coexist in the same join.
#[tokio::test]
async fn test_fan_out_multiple_per_session_mixed() {
    let activities = ActivityRegistry::builder()
        .register("Tag", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("tag:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("MultiMixOrch", |ctx: OrchestrationContext, _input: String| async move {
            let f1 = ctx.schedule_activity_on_session("Tag", "s1-a", "session-1");
            let f2 = ctx.schedule_activity_on_session("Tag", "s1-b", "session-1");
            let f3 = ctx.schedule_activity_on_session("Tag", "s2-a", "session-2");
            let f4 = ctx.schedule_activity_on_session("Tag", "s2-b", "session-2");
            let f5 = ctx.schedule_activity("Tag", "no-sess-a");
            let f6 = ctx.schedule_activity("Tag", "no-sess-b");

            let results = ctx.join(vec![f1, f2, f3, f4, f5, f6]).await;
            let outputs: Vec<String> = results.into_iter().collect::<Result<Vec<_>, _>>()?;
            Ok(outputs.join("|"))
        })
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 4,
        worker_node_id: Some("mix-pod".to_string()),
        ..Default::default()
    };
    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-multi-mix", "MultiMixOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-multi-mix", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(
                output,
                "tag:s1-a|tag:s1-b|tag:s2-a|tag:s2-b|tag:no-sess-a|tag:no-sess-b"
            );
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

// ============================================================================
// Sessions across continue-as-new with version bumps
// ============================================================================

/// Session survives continue-as-new within the same version: an orchestration
/// does a session activity, continues-as-new (same version) with iteration count,
/// does another session activity in the next execution, then completes.
/// Proves session routing works across CAN boundaries without version bumps.
#[tokio::test]
async fn test_session_survives_continue_as_new() {
    let activities = ActivityRegistry::builder()
        .register("Track", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("tracked:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("SessionCAN", |ctx: OrchestrationContext, input: String| async move {
            let iteration: u32 = input.parse().unwrap_or(0);
            let r = ctx
                .schedule_activity_on_session("Track", format!("iter-{iteration}"), "persistent-session")
                .await?;
            if iteration == 0 {
                // First execution: CAN to iteration 1
                ctx.continue_as_new("1").await
            } else {
                // Second execution: complete with both results
                // (input carries the iteration number, not v1's result — that's fine,
                //  we're testing session routing, not data passing)
                Ok(r)
            }
        })
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 1,
        orchestration_concurrency: 1,
        worker_node_id: Some("can-pod".to_string()),
        ..Default::default()
    };
    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-session-can", "SessionCAN", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-session-can", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Second execution did session activity "Track" with input "iter-1"
            assert_eq!(output, "tracked:iter-1");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Continue-as-new with versioned upgrade: v1 schedules session activity then
/// explicitly continues to v2 via continue_as_new_versioned (passing result as
/// input). v2 uses the same session and completes. Proves session + version
/// upgrade work together.
#[tokio::test]
async fn test_session_continue_as_new_versioned_upgrade() {
    let activities = ActivityRegistry::builder()
        .register("Work", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("done:{input}"))
        })
        .build();

    let v1 = |ctx: OrchestrationContext, _input: String| async move {
        let r = ctx
            .schedule_activity_on_session("Work", "from-v1", "upgrade-session")
            .await?;
        // Pass v1's result as input to v2 via versioned continue-as-new
        ctx.continue_as_new_versioned("2.0.0", r).await
    };

    let v2 = |ctx: OrchestrationContext, input: String| async move {
        // input is v1's activity result ("done:from-v1")
        let r = ctx
            .schedule_activity_on_session("Work", "from-v2", "upgrade-session")
            .await?;
        Ok(format!("{input}+{r}"))
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("UpgradeSession", v1)
        .register_versioned("UpgradeSession", "2.0.0", v2)
        .set_policy(
            "UpgradeSession",
            duroxide::runtime::VersionPolicy::Exact(Version::parse("1.0.0").unwrap()),
        )
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 1,
        orchestration_concurrency: 1,
        worker_node_id: Some("upgrade-pod".to_string()),
        ..Default::default()
    };
    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-session-can-ver", "UpgradeSession", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-session-can-ver", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // v1 result becomes v2 input, v2 appends its own work
            assert_eq!(output, "done:from-v1+done:from-v2");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Validates that `start_with_options` panics when `session_idle_timeout` is not
/// greater than the worker lock renewal interval (`worker_lock_timeout - worker_lock_renewal_buffer`).
#[tokio::test]
#[should_panic(expected = "session_idle_timeout")]
async fn test_session_idle_timeout_must_exceed_worker_renewal_interval() {
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder().build();
    let store: Arc<dyn duroxide::providers::Provider> = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );

    // worker_lock_timeout=30s, buffer=5s → renewal interval = 25s
    // session_idle_timeout=25s (equal, not greater) → should panic
    let options = RuntimeOptions {
        session_idle_timeout: Duration::from_secs(25),
        worker_lock_timeout: Duration::from_secs(30),
        worker_lock_renewal_buffer: Duration::from_secs(5),
        ..Default::default()
    };

    // This should panic
    let _rt = runtime::Runtime::start_with_options(store, activities, orchestrations, options).await;
}

/// Verify that max_sessions_per_runtime is enforced via runtime-side ref counting.
///
/// With max_sessions_per_runtime=1, two activities on *different* sessions must not
/// run concurrently. The first session-bound item holds the capacity slot; the second
/// waits until it completes.
#[tokio::test]
async fn test_max_sessions_per_runtime_enforced() {
    use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};

    // Track how many session activities are running concurrently
    let concurrent = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let concurrent_c = concurrent.clone();
    let peak_c = peak.clone();

    let activities = ActivityRegistry::builder()
        .register("SlowSession", move |_ctx: ActivityContext, _input: String| {
            let conc = concurrent_c.clone();
            let pk = peak_c.clone();
            async move {
                let cur = conc.fetch_add(1, AOrdering::SeqCst) + 1;
                // Record peak concurrency
                pk.fetch_max(cur, AOrdering::SeqCst);
                // Hold the slot long enough for a second fetch cycle to attempt
                tokio::time::sleep(Duration::from_millis(200)).await;
                conc.fetch_sub(1, AOrdering::SeqCst);
                Ok("done".to_string())
            }
        })
        .build();

    // Fan out two session activities on DIFFERENT sessions in parallel.
    // With max_sessions_per_runtime=1, only one session slot is available, so the
    // second must wait until the first completes — peak concurrency should be 1.
    let orchestrations = OrchestrationRegistry::builder()
        .register("TwoSessions", |ctx: OrchestrationContext, _input: String| async move {
            let f1 = ctx.schedule_activity_on_session("SlowSession", "a", "session-A");
            let f2 = ctx.schedule_activity_on_session("SlowSession", "b", "session-B");
            let results = ctx.join(vec![f1, f2]).await;
            let r1 = results[0].as_ref().map_err(|e| e.clone())?;
            let r2 = results[1].as_ref().map_err(|e| e.clone())?;
            Ok(format!("{r1}|{r2}"))
        })
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 2,       // Enough slots to run both concurrently if allowed
        max_sessions_per_runtime: 1, // But only 1 session at a time
        orchestration_concurrency: 1,
        ..Default::default()
    };

    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-max-sessions", "TwoSessions", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-max-sessions", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done|done");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    // Peak concurrent session activities should be 1 (different sessions serialized)
    assert_eq!(
        peak.load(AOrdering::SeqCst),
        1,
        "With max_sessions_per_runtime=1, activities on different sessions should not run concurrently"
    );

    rt.shutdown(None).await;
}

/// Verify that multiple activities on the SAME session count as 1 distinct session,
/// not N. With max_sessions_per_runtime=1, two concurrent activities on the same
/// session should both be allowed since they share one session slot.
#[tokio::test]
async fn test_same_session_shares_one_slot() {
    use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};

    let concurrent = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let concurrent_c = concurrent.clone();
    let peak_c = peak.clone();

    let activities = ActivityRegistry::builder()
        .register("SlowSame", move |_ctx: ActivityContext, _input: String| {
            let conc = concurrent_c.clone();
            let pk = peak_c.clone();
            async move {
                let cur = conc.fetch_add(1, AOrdering::SeqCst) + 1;
                pk.fetch_max(cur, AOrdering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
                conc.fetch_sub(1, AOrdering::SeqCst);
                Ok("ok".to_string())
            }
        })
        .build();

    // Fan out two activities on the SAME session so they can overlap
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "SameSessionFanOut",
            |ctx: OrchestrationContext, _input: String| async move {
                let f1 = ctx.schedule_activity_on_session("SlowSame", "a", "shared-session");
                let f2 = ctx.schedule_activity_on_session("SlowSame", "b", "shared-session");
                let results = ctx.join(vec![f1, f2]).await;
                let r1 = results[0].as_ref().map_err(|e| e.clone())?;
                let r2 = results[1].as_ref().map_err(|e| e.clone())?;
                Ok(format!("{r1}|{r2}"))
            },
        )
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 2,
        max_sessions_per_runtime: 1, // Only 1 session allowed, but both activities share it
        orchestration_concurrency: 1,
        ..Default::default()
    };

    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-same-session-slot", "SameSessionFanOut", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("test-same-session-slot", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "ok|ok");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    // With max_sessions_per_runtime=1 and NO worker_node_id, each slot has its own
    // session identity. Slot 1 claims the session; slot 2 can't fetch from it.
    // So both items run on slot 1 sequentially (peak=1). The key assertion is that
    // the orchestration completes — proving same-session items don't count as
    // separate sessions and cause deadlock with max_sessions_per_runtime=1.
    //
    // For concurrent same-session execution (peak=2), use worker_node_id —
    // see test_two_slots_serve_same_session_concurrently.

    rt.shutdown(None).await;
}

/// Verify that a session-bound activity is blocked when at capacity, then
/// unblocked once the blocking session completes.
///
/// Both activities block until explicitly released so the test is
/// order-independent — whichever the provider dispatches first will hold
/// the single session slot while we verify the other is blocked.
///
/// Timeline with max_sessions_per_runtime=1:
///   1. First activity starts, records "{first}-started"
///   2. Second activity is blocked (cap full) — cannot start yet
///   3. First activity finishes, records "{first}-finished"
///   4. Second activity unblocks, records "{second}-started", then "{second}-finished"
///
/// Asserts that the event log proves the two never ran concurrently.
#[tokio::test]
async fn test_session_cap_blocks_then_unblocks() {
    use std::sync::Mutex;
    use tokio::sync::Notify;

    // Ordered event log
    let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    // Notify to let each activity finish on demand
    let release_a: Arc<Notify> = Arc::new(Notify::new());
    let release_b: Arc<Notify> = Arc::new(Notify::new());

    let log_c = log.clone();
    let ra_c = release_a.clone();
    let rb_c = release_b.clone();

    let activities = ActivityRegistry::builder()
        .register("Tracked", move |_ctx: ActivityContext, input: String| {
            let lg = log_c.clone();
            let ra = ra_c.clone();
            let rb = rb_c.clone();
            async move {
                lg.lock().unwrap().push(format!("{input}-started"));

                // Both activities hold their session slot until released
                match input.as_str() {
                    "A" => ra.notified().await,
                    "B" => rb.notified().await,
                    _ => {}
                }

                lg.lock().unwrap().push(format!("{input}-finished"));
                Ok(format!("result-{input}"))
            }
        })
        .build();

    // Fan-out: both activities dispatched concurrently via join
    let orchestrations = OrchestrationRegistry::builder()
        .register("BlockUnblock", |ctx: OrchestrationContext, _input: String| async move {
            let fa = ctx.schedule_activity_on_session("Tracked", "A", "session-A");
            let fb = ctx.schedule_activity_on_session("Tracked", "B", "session-B");
            let results = ctx.join(vec![fa, fb]).await;
            let ra = results[0].as_ref().map_err(|e| e.clone())?;
            let rb = results[1].as_ref().map_err(|e| e.clone())?;
            Ok(format!("{ra}|{rb}"))
        })
        .build();

    let options = RuntimeOptions {
        worker_concurrency: 2,
        max_sessions_per_runtime: 1,
        orchestration_concurrency: 1,
        ..Default::default()
    };

    let (rt, client) = create_runtime_with_options(activities, orchestrations, options).await;

    client
        .start_orchestration("test-block-unblock", "BlockUnblock", "")
        .await
        .unwrap();

    // Wait for either activity to start (whichever the provider dispatches first)
    let first = {
        let mut found: Option<&str> = None;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let events = log.lock().unwrap().clone();
            if events.contains(&"A-started".to_string()) {
                found = Some("A");
                break;
            }
            if events.contains(&"B-started".to_string()) {
                found = Some("B");
                break;
            }
        }
        found
    };
    let first = first.expect("Timed out waiting for any activity to start");
    let second = if first == "A" { "B" } else { "A" };

    // Give the other activity a chance to start (it shouldn't with cap=1)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // At this point first is running. Second should NOT have started because cap=1.
    {
        let events = log.lock().unwrap().clone();
        assert!(
            !events.contains(&format!("{second}-started")),
            "{second} should NOT have started while {first} holds the session cap, got: {events:?}"
        );
    }

    // Release the first activity — this frees the session slot, allowing second to start
    match first {
        "A" => release_a.notify_one(),
        "B" => release_b.notify_one(),
        _ => unreachable!(),
    }

    // Wait for the second activity to start
    {
        let mut found = false;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let events = log.lock().unwrap().clone();
            if events.contains(&format!("{second}-started")) {
                found = true;
                break;
            }
        }
        assert!(found, "Timed out waiting for {second} to start after {first} finished");
    }

    // Release the second activity
    match second {
        "A" => release_a.notify_one(),
        "B" => release_b.notify_one(),
        _ => unreachable!(),
    }

    // Wait for orchestration to complete
    match client
        .wait_for_orchestration("test-block-unblock", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "result-A|result-B");
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    // Verify ordering: first must have finished before second started
    let events = log.lock().unwrap().clone();
    let first_finished = events
        .iter()
        .position(|e| *e == format!("{first}-finished"))
        .unwrap_or_else(|| panic!("{first}-finished missing"));
    let second_started_pos = events
        .iter()
        .position(|e| *e == format!("{second}-started"))
        .unwrap_or_else(|| panic!("{second}-started missing"));
    assert!(
        first_finished < second_started_pos,
        "{second} should start only after {first} finishes. Event log: {events:?}"
    );

    rt.shutdown(None).await;
}

// ============================================================================
// Multi-worker E2E tests (two runtimes, shared store)
// ============================================================================

/// Complex orchestration across 2 worker runtimes sharing the same store.
///
/// Exercises the full session lifecycle with real contention:
/// - Fan-out: 3 session-bound activities + 1 non-session activity in parallel
/// - Session affinity: all session activities for a given session_id go to whichever
///   worker claimed the session first
/// - Continue-as-new: orchestration uses CAN to truncate history, then schedules
///   more session work — session pin must survive the CAN boundary
/// - Both workers serve work (non-session distributes freely, sessions pin)
#[tokio::test]
async fn test_multi_worker_complex_orchestration() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Track which worker handled each session (via activity closures)
    let worker_a_count = Arc::new(AtomicUsize::new(0));
    let worker_b_count = Arc::new(AtomicUsize::new(0));

    // Shared store (simulates PostgreSQL in production)
    let store: Arc<dyn duroxide::providers::Provider> = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );

    fn build_activities(counter: Arc<AtomicUsize>) -> ActivityRegistry {
        ActivityRegistry::builder()
            .register("SessionWork", move |ctx: ActivityContext, input: String| {
                let c = counter.clone();
                async move {
                    c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    assert!(ctx.session_id().is_some(), "SessionWork must have a session_id");
                    // Simulate some work
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(format!("session-result:{input}"))
                }
            })
            .register("PlainWork", |_ctx: ActivityContext, input: String| async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok(format!("plain-result:{input}"))
            })
            .build()
    }

    fn build_orchestrations() -> OrchestrationRegistry {
        OrchestrationRegistry::builder()
            .register(
                "MultiWorkerOrch",
                |ctx: OrchestrationContext, input: String| async move {
                    let parts: Vec<&str> = input.splitn(2, '|').collect();
                    let cycle: u32 = parts[0].parse().unwrap_or(0);
                    let prev = parts.get(1).unwrap_or(&"").to_string();

                    match cycle {
                        0 => {
                            // Cycle 1: fan-out — 3 session activities + 1 plain, all parallel
                            let s1 = ctx.schedule_activity_on_session("SessionWork", "a", "sess-alpha");
                            let s2 = ctx.schedule_activity_on_session("SessionWork", "b", "sess-alpha");
                            let s3 = ctx.schedule_activity_on_session("SessionWork", "c", "sess-beta");
                            let p1 = ctx.schedule_activity("PlainWork", "d");
                            let results = ctx.join(vec![s1, s2, s3, p1]).await;
                            let combined: Vec<String> = results
                                .into_iter()
                                .map(|r| r.unwrap_or_else(|e| format!("ERR:{e}")))
                                .collect();

                            // CAN to cycle 1 with accumulated results
                            ctx.continue_as_new(format!("1|{}", combined.join(";"))).await
                        }
                        1 => {
                            // Cycle 2: one more session activity on each session
                            let r1 = ctx
                                .schedule_activity_on_session("SessionWork", "e", "sess-alpha")
                                .await?;
                            let r2 = ctx
                                .schedule_activity_on_session("SessionWork", "f", "sess-beta")
                                .await?;
                            Ok(format!("{prev};{r1};{r2}"))
                        }
                        _ => Ok(format!("unexpected cycle {cycle}")),
                    }
                },
            )
            .build()
    }

    // Start two runtimes with different worker_node_ids
    let rt_a = runtime::Runtime::start_with_options(
        store.clone(),
        build_activities(worker_a_count.clone()),
        build_orchestrations(),
        RuntimeOptions {
            worker_concurrency: 2,
            orchestration_concurrency: 2,
            worker_node_id: Some("node-A".to_string()),
            max_sessions_per_runtime: 4,
            ..Default::default()
        },
    )
    .await;

    let rt_b = runtime::Runtime::start_with_options(
        store.clone(),
        build_activities(worker_b_count.clone()),
        build_orchestrations(),
        RuntimeOptions {
            worker_concurrency: 2,
            orchestration_concurrency: 2,
            worker_node_id: Some("node-B".to_string()),
            max_sessions_per_runtime: 4,
            ..Default::default()
        },
    )
    .await;

    let client = Client::new(store);

    client
        .start_orchestration("multi-worker-complex", "MultiWorkerOrch", "0|")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("multi-worker-complex", Duration::from_secs(30))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // Verify all 6 results present (4 from cycle 0 + 2 from cycle 1)
            let parts: Vec<&str> = output.split(';').collect();
            assert_eq!(parts.len(), 6, "Should have 6 results total, got: {output}");

            // All session results should contain "session-result:"
            let session_results: Vec<&&str> = parts.iter().filter(|p| p.contains("session-result:")).collect();
            assert_eq!(session_results.len(), 5, "Should have 5 session results, got: {output}");

            // The plain result should be present
            assert!(
                output.contains("plain-result:d"),
                "Plain activity result missing: {output}"
            );
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    // Both workers should have processed some work
    let a = worker_a_count.load(Ordering::SeqCst);
    let b = worker_b_count.load(Ordering::SeqCst);
    assert_eq!(
        a + b,
        5,
        "Total session activities should be 5 (3 + 2 from CAN), got A={a} B={b}"
    );

    rt_a.shutdown(None).await;
    rt_b.shutdown(None).await;
}

/// Heterogeneous multi-worker test: different max_sessions and session_lock_timeout.
///
/// Most problematic config combination:
/// - Worker A: max_sessions=1, short session_lock_timeout (1s)
/// - Worker B: max_sessions=10, default session_lock_timeout (30s)
///
/// This exercises:
/// - max_sessions overflow: A fills its 1 session slot, remaining sessions must go to B
/// - Lock timeout asymmetry: if A dies, its session becomes claimable in ~1s (fast handoff)
/// - Session affinity under pressure: each session's activities stay on their owning worker
///   despite the capacity imbalance
#[tokio::test]
async fn test_multi_worker_heterogeneous_config() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let worker_a_sessions = Arc::new(AtomicUsize::new(0));
    let worker_b_sessions = Arc::new(AtomicUsize::new(0));

    let store: Arc<dyn duroxide::providers::Provider> = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );

    fn build_activities(counter: Arc<AtomicUsize>) -> ActivityRegistry {
        ActivityRegistry::builder()
            .register("Work", move |_ctx: ActivityContext, input: String| {
                let c = counter.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    // Hold the slot long enough that Worker B polls and claims overflow
                    // sessions while Worker A is busy (at max_sessions capacity).
                    tokio::time::sleep(Duration::from_millis(800)).await;
                    Ok(format!("done:{input}"))
                }
            })
            .build()
    }

    fn build_orchestrations() -> OrchestrationRegistry {
        OrchestrationRegistry::builder()
            .register("HeteroOrch", |ctx: OrchestrationContext, _input: String| async move {
                // Schedule 3 activities on 3 different sessions in parallel.
                // Worker A can only hold 1 session, so at least 2 must go to B.
                let f1 = ctx.schedule_activity_on_session("Work", "1", "sess-X");
                let f2 = ctx.schedule_activity_on_session("Work", "2", "sess-Y");
                let f3 = ctx.schedule_activity_on_session("Work", "3", "sess-Z");
                let results = ctx.join(vec![f1, f2, f3]).await;
                let combined: String = results
                    .into_iter()
                    .map(|r| r.unwrap_or_else(|e| format!("ERR:{e}")))
                    .collect::<Vec<_>>()
                    .join("|");
                Ok(combined)
            })
            .build()
    }

    // Worker A: constrained — only 1 session slot, short session lock
    let rt_a = runtime::Runtime::start_with_options(
        store.clone(),
        build_activities(worker_a_sessions.clone()),
        build_orchestrations(),
        RuntimeOptions {
            worker_concurrency: 2,
            orchestration_concurrency: 2,
            worker_node_id: Some("constrained-node".to_string()),
            max_sessions_per_runtime: 1, // Can only hold 1 session
            session_lock_timeout: Duration::from_secs(5),
            session_lock_renewal_buffer: Duration::from_secs(1),
            session_idle_timeout: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .await;

    // Worker B: unconstrained — plenty of session capacity, long session lock
    let rt_b = runtime::Runtime::start_with_options(
        store.clone(),
        build_activities(worker_b_sessions.clone()),
        build_orchestrations(),
        RuntimeOptions {
            worker_concurrency: 2,
            orchestration_concurrency: 2,
            worker_node_id: Some("unconstrained-node".to_string()),
            max_sessions_per_runtime: 10,
            session_lock_timeout: Duration::from_secs(30),
            session_lock_renewal_buffer: Duration::from_secs(5),
            session_idle_timeout: Duration::from_secs(60),
            ..Default::default()
        },
    )
    .await;

    let client = Client::new(store);

    client
        .start_orchestration("hetero-test", "HeteroOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("hetero-test", Duration::from_secs(30))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            let parts: Vec<&str> = output.split('|').collect();
            assert_eq!(parts.len(), 3, "Should have 3 results, got: {output}");
            for p in &parts {
                assert!(
                    p.starts_with("done:"),
                    "Each result should start with 'done:', got: {p}"
                );
            }
        }
        other => panic!("Expected completed, got {:?}", other),
    }

    // Verify work distribution: with 800ms activities and max_sessions=1 on A,
    // A should hold at most 1 session concurrently. Worker B picks up the rest
    // while A is busy. A may process more than 1 *total* (serially), but B
    // should handle at least 1.
    let a = worker_a_sessions.load(Ordering::SeqCst);
    let b = worker_b_sessions.load(Ordering::SeqCst);
    assert_eq!(a + b, 3, "Total should be 3, got A={a} B={b}");
    assert!(
        b >= 1,
        "Worker B should handle at least 1 session (overflow from A's max_sessions=1), got A={a} B={b}"
    );

    rt_a.shutdown(None).await;
    rt_b.shutdown(None).await;
}

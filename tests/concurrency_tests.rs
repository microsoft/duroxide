// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::EventKind;
use duroxide::providers::Provider;
mod common;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, Client, Either3, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc as StdArc;
use std::time::Duration;

use std::future::Future;
use std::pin::Pin;

// Type alias for boxed futures with heterogeneous outputs (Activity, External, Timer)
type BoxedJoinFuture<'a> = Pin<Box<dyn Future<Output = Either3<Result<String, String>, String, ()>> + Send + 'a>>;

async fn concurrent_orchestrations_different_activities_with(store: StdArc<dyn Provider>) {
    let o1 = |ctx: OrchestrationContext, _input: String| async move {
        // Wrap heterogeneous futures in boxed futures with common type
        let f_a: BoxedJoinFuture = Box::pin(async { Either3::First(ctx.schedule_activity("Add", "2,3").await) });
        let f_e: BoxedJoinFuture = Box::pin(async { Either3::Second(ctx.schedule_wait("Go").await) });
        let f_t: BoxedJoinFuture = Box::pin(async {
            ctx.schedule_timer(Duration::from_millis(1)).await;
            Either3::Third(())
        });
        let results = ctx.join(vec![f_a, f_e, f_t]).await;

        // Find activity and external results (order may vary due to history ordering)
        let mut a = None;
        let mut e = None;
        for result in results {
            match result {
                Either3::First(Ok(activity_result)) => {
                    a = Some(activity_result);
                }
                Either3::Second(external_data) => {
                    e = Some(external_data);
                }
                _ => {} // Ignore timer for now
            }
        }

        let a = a.expect("activity result not found");
        let e = e.expect("external result not found");

        Ok(format!("o1:sum={a};evt={e}"))
    };
    let o2 = |ctx: OrchestrationContext, _input: String| async move {
        // Wrap heterogeneous futures in boxed futures with common type
        let f_a: BoxedJoinFuture = Box::pin(async { Either3::First(ctx.schedule_activity("Upper", "hi").await) });
        let f_e: BoxedJoinFuture = Box::pin(async { Either3::Second(ctx.schedule_wait("Go").await) });
        let f_t: BoxedJoinFuture = Box::pin(async {
            ctx.schedule_timer(Duration::from_millis(1)).await;
            Either3::Third(())
        });
        let results = ctx.join(vec![f_a, f_e, f_t]).await;

        // Find activity and external results (order may vary due to history ordering)
        let mut a = None;
        let mut e = None;
        for result in results {
            match result {
                Either3::First(Ok(activity_result)) => {
                    a = Some(activity_result);
                }
                Either3::Second(external_data) => {
                    e = Some(external_data);
                }
                _ => {} // Ignore timer for now
            }
        }

        let a = a.expect("activity result not found");
        let e = e.expect("external result not found");

        Ok(format!("o2:up={a};evt={e}"))
    };

    let activity_registry = ActivityRegistry::builder()
        .register("Add", |_ctx: ActivityContext, input: String| async move {
            let mut parts = input.split(',');
            let a = parts.next().unwrap_or("0").parse::<i64>().unwrap_or(0);
            let b = parts.next().unwrap_or("0").parse::<i64>().unwrap_or(0);
            Ok((a + b).to_string())
        })
        .register("Upper", |_ctx: ActivityContext, input: String| async move {
            Ok(input.to_uppercase())
        })
        .build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("AddOrchestration", o1)
        .register("UpperOrchestration", o2)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    let _ = client.start_orchestration("inst-multi-1", "AddOrchestration", "").await;
    let _ = client
        .start_orchestration("inst-multi-2", "UpperOrchestration", "")
        .await;

    let store_for_wait1 = store.clone();
    tokio::spawn(async move {
        let sfw = store_for_wait1.clone();
        let _ = common::wait_for_subscription(sfw.clone(), "inst-multi-1", "Go", 3000).await;
        let client = Client::new(sfw);
        let _ = client.raise_event("inst-multi-1", "Go", "E1").await;
    });
    let store_for_wait2 = store.clone();
    tokio::spawn(async move {
        let sfw = store_for_wait2.clone();
        let _ = common::wait_for_subscription(sfw.clone(), "inst-multi-2", "Go", 3000).await;
        let client = Client::new(sfw);
        let _ = client.raise_event("inst-multi-2", "Go", "E2").await;
    });

    let out1_result = Client::new(store.clone())
        .wait_for_orchestration("inst-multi-1", std::time::Duration::from_secs(10))
        .await;

    if out1_result.is_err() {
        let hist1 = store.read("inst-multi-1").await.unwrap_or_default();
        println!("inst-multi-1 history ({} events):", hist1.len());
        for (i, e) in hist1.iter().enumerate() {
            println!("  {i}: {e:?}");
        }
    }

    let out1 = match out1_result.unwrap() {
        duroxide::OrchestrationStatus::Completed { output, .. } => Ok(output),
        duroxide::OrchestrationStatus::Failed { details, .. } => Err(details.display_message()),
        _ => panic!("unexpected orchestration status"),
    };

    let out2_result = Client::new(store.clone())
        .wait_for_orchestration("inst-multi-2", std::time::Duration::from_secs(10))
        .await;

    if out2_result.is_err() {
        let hist2 = store.read("inst-multi-2").await.unwrap_or_default();
        println!("inst-multi-2 history ({} events):", hist2.len());
        for (i, e) in hist2.iter().enumerate() {
            println!("  {i}: {e:?}");
        }
    }

    let out2 = match out2_result.unwrap() {
        duroxide::OrchestrationStatus::Completed { output, .. } => Ok(output),
        duroxide::OrchestrationStatus::Failed { details, .. } => Err(details.display_message()),
        _ => panic!("unexpected orchestration status"),
    };

    assert!(
        out1.as_ref().unwrap().contains("o1:sum=5;evt=E1"),
        "unexpected out1: {out1:?}"
    );
    assert!(
        out2.as_ref().unwrap().contains("o2:up=HI;evt=E2"),
        "unexpected out2: {out2:?}"
    );

    // Check histories
    let hist1 = client.read_execution_history("inst-multi-1", 1).await.unwrap();
    let hist2 = client.read_execution_history("inst-multi-2", 1).await.unwrap();

    assert!(
        hist1
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ActivityCompleted { result, .. } if e.source_event_id == Some(2) && result == "5"))
    );
    assert!(
        hist2
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ActivityCompleted { result, .. } if e.source_event_id == Some(2) && result == "HI"))
    );
    assert!(
        hist1
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalEvent { data, .. } if data == "E1"))
    );
    assert!(
        hist2
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalEvent { data, .. } if data == "E2"))
    );
    assert!(hist1.iter().any(|e| matches!(&e.kind, EventKind::TimerFired { .. })));
    assert!(hist2.iter().any(|e| matches!(&e.kind, EventKind::TimerFired { .. })));

    rt.shutdown(None).await;
}

#[tokio::test]
async fn concurrent_orchestrations_different_activities_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    concurrent_orchestrations_different_activities_with(store).await;
}

async fn concurrent_orchestrations_same_activities_with(store: StdArc<dyn Provider>) {
    let o1 = |ctx: OrchestrationContext, _input: String| async move {
        let f_a: BoxedJoinFuture = Box::pin(async { Either3::First(ctx.schedule_activity("Proc", "10").await) });
        let f_e: BoxedJoinFuture = Box::pin(async { Either3::Second(ctx.schedule_wait("Go").await) });
        let f_t: BoxedJoinFuture = Box::pin(async {
            ctx.schedule_timer(Duration::from_millis(1)).await;
            Either3::Third(())
        });
        let results = ctx.join(vec![f_a, f_e, f_t]).await;

        // Find activity and external results (order may vary due to history ordering)
        let mut a = None;
        let mut e = None;
        for result in results {
            match result {
                Either3::First(Ok(activity_result)) => {
                    a = Some(activity_result);
                }
                Either3::Second(external_data) => {
                    e = Some(external_data);
                }
                _ => {} // Ignore timer for now
            }
        }

        let a = a.expect("activity result not found");
        let e = e.expect("external result not found");

        Ok(format!("o1:a={a};evt={e}"))
    };
    let o2 = |ctx: OrchestrationContext, _input: String| async move {
        let _guid = ctx.new_guid().await?;
        let f_a: BoxedJoinFuture = Box::pin(async { Either3::First(ctx.schedule_activity("Proc", "20").await) });
        let f_e: BoxedJoinFuture = Box::pin(async { Either3::Second(ctx.schedule_wait("Go").await) });
        let f_t: BoxedJoinFuture = Box::pin(async {
            ctx.schedule_timer(Duration::from_millis(1)).await;
            Either3::Third(())
        });
        let results = ctx.join(vec![f_a, f_e, f_t]).await;

        // Find activity and external results (order may vary due to history ordering)
        let mut a = None;
        let mut e = None;
        for result in results {
            match result {
                Either3::First(Ok(activity_result)) => {
                    a = Some(activity_result);
                }
                Either3::Second(external_data) => {
                    e = Some(external_data);
                }
                _ => {} // Ignore timer for now
            }
        }

        let a = a.expect("activity result not found");
        let e = e.expect("external result not found");

        Ok(format!("o2:a={a};evt={e}"))
    };

    let activity_registry = ActivityRegistry::builder()
        .register("Proc", |_ctx: ActivityContext, input: String| async move {
            let n = input.parse::<i64>().unwrap_or(0);
            Ok((n + 1).to_string())
        })
        .build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ProcOrchestration1", o1)
        .register("ProcOrchestration2", o2)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());
    let _ = client
        .start_orchestration("inst-same-acts-1", "ProcOrchestration1", "")
        .await;
    let _ = client
        .start_orchestration("inst-same-acts-2", "ProcOrchestration2", "")
        .await;

    let store_for_wait3 = store.clone();
    tokio::spawn(async move {
        let sfw = store_for_wait3.clone();
        let _ = common::wait_for_subscription(sfw.clone(), "inst-same-acts-1", "Go", 3000).await;
        let client = Client::new(sfw);
        let _ = client.raise_event("inst-same-acts-1", "Go", "P1").await;
    });
    let store_for_wait4 = store.clone();
    tokio::spawn(async move {
        let sfw = store_for_wait4.clone();
        let _ = common::wait_for_subscription(sfw.clone(), "inst-same-acts-2", "Go", 3000).await;
        let client = Client::new(sfw);
        let _ = client.raise_event("inst-same-acts-2", "Go", "P2").await;
    });

    match Client::new(store.clone())
        .wait_for_orchestration("inst-same-acts-1", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "o1:a=11;evt=P1"),
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    match Client::new(store.clone())
        .wait_for_orchestration("inst-same-acts-2", std::time::Duration::from_secs(10))
        .await
        .unwrap()
    {
        duroxide::OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "o2:a=21;evt=P2"),
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    // Check histories
    let hist1 = client.read_execution_history("inst-same-acts-1", 1).await.unwrap();
    let hist2 = client.read_execution_history("inst-same-acts-2", 1).await.unwrap();

    assert!(
        hist1
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ActivityCompleted { result, .. } if result == "11"))
    );
    assert!(
        hist2
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ActivityCompleted { result, .. } if result == "21"))
    );
    assert!(
        hist1
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalEvent { data, .. } if data == "P1"))
    );
    assert!(
        hist2
            .iter()
            .any(|e| matches!(&e.kind, EventKind::ExternalEvent { data, .. } if data == "P2"))
    );
    assert!(hist1.iter().any(|e| matches!(&e.kind, EventKind::TimerFired { .. })));
    assert!(hist2.iter().any(|e| matches!(&e.kind, EventKind::TimerFired { .. })));

    rt.shutdown(None).await;
}

#[tokio::test]
async fn concurrent_orchestrations_same_activities_fs() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    concurrent_orchestrations_same_activities_with(store).await;
}

#[tokio::test]
async fn single_orchestration_with_join_test() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    // Just run ONE orchestration (same as o1 from the concurrent test)
    let o1 = |ctx: OrchestrationContext, _input: String| async move {
        let f_a: BoxedJoinFuture = Box::pin(async { Either3::First(ctx.schedule_activity("Proc", "10").await) });
        let f_e: BoxedJoinFuture = Box::pin(async { Either3::Second(ctx.schedule_wait("Go").await) });
        let f_t: BoxedJoinFuture = Box::pin(async {
            ctx.schedule_timer(Duration::from_millis(1)).await;
            Either3::Third(())
        });
        let results = ctx.join(vec![f_a, f_e, f_t]).await;

        let mut a = None;
        let mut e = None;
        for result in results {
            match result {
                Either3::First(Ok(activity_result)) => {
                    a = Some(activity_result);
                }
                Either3::Second(external_data) => {
                    e = Some(external_data);
                }
                _ => {}
            }
        }

        let a = a.expect("activity result not found");
        let e = e.expect("external result not found");
        Ok(format!("o1:a={a};evt={e}"))
    };

    let activity_registry = ActivityRegistry::builder()
        .register("Proc", |_ctx: ActivityContext, input: String| async move {
            let n = input.parse::<i64>().unwrap_or(0);
            Ok((n + 1).to_string())
        })
        .build();

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ProcOrchestration1", o1)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    // Start only ONE orchestration
    let _ = client
        .start_orchestration("inst-single", "ProcOrchestration1", "")
        .await;

    // Raise event
    let store_for_wait = store.clone();
    tokio::spawn(async move {
        let _ = common::wait_for_subscription(store_for_wait.clone(), "inst-single", "Go", 3000).await;
        let client = Client::new(store_for_wait);
        let _ = client.raise_event("inst-single", "Go", "P1").await;
    });

    // Wait for completion
    let result = client
        .wait_for_orchestration("inst-single", std::time::Duration::from_secs(10))
        .await;

    if result.is_err() {
        let hist = store.read("inst-single").await.unwrap_or_default();
        println!("❌ Timeout! History ({} events):", hist.len());
        for (i, e) in hist.iter().enumerate() {
            println!("  {i}: {e:?}");
        }
    }

    match result.unwrap() {
        duroxide::OrchestrationStatus::Completed { output, .. } => {
            println!("✅ Single orch completed: {output}");
            assert_eq!(output, "o1:a=11;evt=P1");
        }
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        _ => panic!("unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

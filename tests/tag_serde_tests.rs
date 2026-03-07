#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::EventKind;

mod common;

/// Verify ActivityScheduled with tag roundtrips correctly through serde.
#[test]
fn activity_scheduled_with_tag_roundtrips() {
    let kind = EventKind::ActivityScheduled {
        name: "Build".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: Some("gpu".to_string()),
    };
    let json = serde_json::to_string(&kind).unwrap();
    let deser: EventKind = serde_json::from_str(&json).unwrap();
    match deser {
        EventKind::ActivityScheduled { tag, .. } => {
            assert_eq!(tag, Some("gpu".to_string()));
        }
        _ => panic!("Expected ActivityScheduled"),
    }
}

/// Verify that tag: None omits the tag key from JSON output.
#[test]
fn activity_scheduled_none_tag_omitted_in_json() {
    let kind = EventKind::ActivityScheduled {
        name: "Build".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    let json = serde_json::to_string(&kind).unwrap();
    assert!(
        !json.contains("\"tag\""),
        "tag: None should be omitted from JSON, got: {}",
        json
    );
}

/// Verify that old JSON without a tag field deserializes as tag: None (backward compat).
#[test]
fn activity_scheduled_missing_tag_deserializes_as_none() {
    let json = r#"{"type":"ActivityScheduled","name":"Build","input":"{}"}"#;
    let kind: EventKind = serde_json::from_str(json).unwrap();
    match kind {
        EventKind::ActivityScheduled { tag, .. } => {
            assert_eq!(tag, None);
        }
        _ => panic!("Expected ActivityScheduled"),
    }
}

/// Verify WorkItem::ActivityExecute with tag roundtrips correctly.
#[test]
fn work_item_activity_execute_with_tag_roundtrips() {
    use duroxide::providers::WorkItem;

    let item = WorkItem::ActivityExecute {
        instance: "i".to_string(),
        execution_id: 1,
        id: 1,
        name: "Build".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: Some("gpu".to_string()),
    };
    let json = serde_json::to_string(&item).unwrap();
    let deser: WorkItem = serde_json::from_str(&json).unwrap();
    assert_eq!(deser, item);
}

/// Verify WorkItem::ActivityExecute with no tag omits the key.
#[test]
fn work_item_activity_execute_none_tag_omitted_in_json() {
    use duroxide::providers::WorkItem;

    let item = WorkItem::ActivityExecute {
        instance: "i".to_string(),
        execution_id: 1,
        id: 1,
        name: "Build".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    let json = serde_json::to_string(&item).unwrap();
    assert!(
        !json.contains("\"tag\""),
        "tag: None should be omitted from JSON, got: {}",
        json
    );
}

/// Verify old WorkItem JSON without tag field deserializes correctly (backward compat).
#[test]
fn work_item_activity_execute_missing_tag_deserializes_as_none() {
    use duroxide::providers::WorkItem;

    let json = r#"{"ActivityExecute":{"instance":"i","execution_id":1,"id":1,"name":"Build","input":"{}"}}"#;
    let item: WorkItem = serde_json::from_str(json).unwrap();
    match item {
        WorkItem::ActivityExecute { tag, .. } => {
            assert_eq!(tag, None);
        }
        _ => panic!("Expected ActivityExecute"),
    }
}

// ============================================================================
// E2E Integration Tests: Activity Tags Through Full Runtime
// ============================================================================

/// A worker with `Tags(["gpu"])` picks up gpu-tagged activities and ignores untagged ones.
///
/// Schedules both a tagged and an untagged activity. The gpu-only worker
/// processes the tagged one; the untagged one is raced against a timer
/// that wins (proving it was never picked up).
#[tokio::test]
async fn e2e_tagged_activity_runs_on_matching_worker() {
    use duroxide::runtime::{self, RuntimeOptions, registry::ActivityRegistry};
    use duroxide::{ActivityContext, Client, Either2, OrchestrationContext, OrchestrationRegistry, TagFilter};
    use std::time::Duration;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Compute", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("computed:{input}"))
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        // Tagged activity — should be picked up by Tags(["gpu"]) worker
        let gpu_result = ctx.schedule_activity("Compute", "gpu-work").with_tag("gpu").await?;

        // Untagged activity — NO worker has DefaultOnly, so this should stall.
        // Race it against a short timer to prove it's never picked up.
        let untagged = ctx.schedule_activity("Compute", "default-work");
        let timeout = ctx.schedule_timer(Duration::from_millis(500));

        let untagged_status = match ctx.select2(untagged, timeout).await {
            Either2::First(Ok(_)) => "untagged_completed",
            Either2::First(Err(_)) => "untagged_error",
            Either2::Second(()) => "untagged_timed_out",
        };

        Ok(format!("{gpu_result}|{untagged_status}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("TagTest", orchestration)
        .build();

    // Worker ONLY processes gpu-tagged activities — untagged will stall
    let opts = RuntimeOptions {
        worker_tag_filter: TagFilter::tags(["gpu"]),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, opts).await;

    let client = Client::new(store.clone());
    client.start_orchestration("tag-e2e-1", "TagTest", "").await.unwrap();

    match client
        .wait_for_orchestration("tag-e2e-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(
                output, "computed:gpu-work|untagged_timed_out",
                "GPU activity should complete, untagged should time out"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

/// with_tag() produces ActivityScheduled event with the correct tag in history.
#[tokio::test]
async fn e2e_with_tag_persists_tag_in_event_history() {
    use duroxide::runtime::{self, RuntimeOptions, registry::ActivityRegistry};
    use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, TagFilter};
    use std::time::Duration;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("GpuTask", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("gpu:{input}"))
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("GpuTask", "frame1").with_tag("gpu").await?;
        Ok(result)
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("GpuOrch", orchestration)
        .build();

    // Worker accepts both default and gpu-tagged activities
    let opts = RuntimeOptions {
        worker_tag_filter: TagFilter::default_and(["gpu"]),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, opts).await;

    let client = Client::new(store.clone());
    client.start_orchestration("tag-e2e-2", "GpuOrch", "").await.unwrap();

    match client
        .wait_for_orchestration("tag-e2e-2", Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "gpu:frame1");

            // Verify the tag was persisted in event history
            let history = store.read("tag-e2e-2").await.unwrap();
            let scheduled = history
                .iter()
                .find(|e| matches!(&e.kind, EventKind::ActivityScheduled { tag: Some(t), .. } if t == "gpu"));
            assert!(
                scheduled.is_some(),
                "Expected ActivityScheduled with tag='gpu' in history: {:?}",
                history.iter().map(|e| &e.kind).collect::<Vec<_>>()
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Starvation safety: a tagged activity with no matching worker times out.
///
/// Demonstrates the recommended pattern for protecting against tag starvation:
/// use `select2(tagged_activity, timer)` so the orchestration doesn't hang
/// forever when no worker has the right TagFilter.
#[tokio::test]
async fn e2e_tagged_activity_starvation_times_out() {
    use duroxide::runtime::{self, RuntimeOptions, registry::ActivityRegistry};
    use duroxide::{ActivityContext, Client, Either2, OrchestrationContext, OrchestrationRegistry, TagFilter};
    use std::time::Duration;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("GpuRender", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("rendered:{input}"))
        })
        .build();

    // Orchestrator schedules a "gpu"-tagged activity but races it against a short timer.
    // Since our worker only has DefaultOnly, the tagged activity will never be picked up.
    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let activity = ctx.schedule_activity("GpuRender", "frame1").with_tag("gpu");
        let timeout = ctx.schedule_timer(Duration::from_millis(500));

        match ctx.select2(activity, timeout).await {
            Either2::First(Ok(result)) => Ok(format!("completed:{result}")),
            Either2::First(Err(e)) => Err(format!("activity_error:{e}")),
            Either2::Second(()) => Ok("timeout:no_gpu_worker".to_string()),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("StarvationTest", orchestration)
        .build();

    // Worker has DefaultOnly — will NOT pick up "gpu"-tagged activity
    let opts = RuntimeOptions {
        worker_tag_filter: TagFilter::default(),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, opts).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("starvation-1", "StarvationTest", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("starvation-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(
                output, "timeout:no_gpu_worker",
                "Timer should win because no GPU worker exists"
            );
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

/// An activity tag exceeding MAX_TAG_NAME_BYTES (256) fails the orchestration.
#[tokio::test]
async fn e2e_oversized_tag_fails_orchestration() {
    use duroxide::runtime::{self, RuntimeOptions, limits, registry::ActivityRegistry};
    use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, TagFilter};
    use std::time::Duration;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Work", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("done:{input}"))
        })
        .build();

    let oversized_tag = "x".repeat(limits::MAX_TAG_NAME_BYTES + 1);
    let orchestration = move |ctx: OrchestrationContext, _input: String| {
        let tag = oversized_tag.clone();
        async move {
            let result = ctx.schedule_activity("Work", "data").with_tag(&tag).await?;
            Ok(result)
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("OversizedTagTest", orchestration)
        .build();

    let opts = RuntimeOptions {
        worker_tag_filter: TagFilter::Any,
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, opts).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("oversized-1", "OversizedTagTest", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("oversized-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(
                msg.contains("tag size") && msg.contains("exceeds limit"),
                "Expected tag size limit error, got: {msg}"
            );
        }
        runtime::OrchestrationStatus::Completed { output, .. } => {
            panic!("Expected failure but got completion: {output}")
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

/// A tag at exactly MAX_TAG_NAME_BYTES (256 bytes) succeeds.
#[tokio::test]
async fn e2e_tag_at_boundary_succeeds() {
    use duroxide::runtime::{self, RuntimeOptions, limits, registry::ActivityRegistry};
    use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, TagFilter};
    use std::time::Duration;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Work", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("done:{input}"))
        })
        .build();

    let boundary_tag = "x".repeat(limits::MAX_TAG_NAME_BYTES);
    let orchestration = move |ctx: OrchestrationContext, _input: String| {
        let tag = boundary_tag.clone();
        async move {
            let result = ctx.schedule_activity("Work", "data").with_tag(&tag).await?;
            Ok(result)
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("BoundaryTagTest", orchestration)
        .build();

    let opts = RuntimeOptions {
        worker_tag_filter: TagFilter::Any,
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, opts).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("boundary-1", "BoundaryTagTest", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("boundary-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done:data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!(
                "Expected success but orchestration failed: {}",
                details.display_message()
            )
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

// ============================================================================
// Additional Integration Tests: Tag Scenarios
// ============================================================================

/// ActivityContext::tag() is visible inside the activity handler.
///
/// Schedules a tagged activity whose handler reads and returns ctx.tag().
#[tokio::test]
async fn e2e_activity_context_tag_visible_in_handler() {
    use duroxide::runtime::{self, RuntimeOptions, registry::ActivityRegistry};
    use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, TagFilter};
    use std::time::Duration;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("EchoTag", |ctx: ActivityContext, _input: String| async move {
            // Return the tag the handler sees
            Ok(ctx.tag().unwrap_or("none").to_string())
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let tag_value = ctx.schedule_activity("EchoTag", "").with_tag("gpu").await?;
        Ok(tag_value)
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("EchoTagOrch", orchestration)
        .build();

    let opts = RuntimeOptions {
        worker_tag_filter: TagFilter::default_and(["gpu"]),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, opts).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("echo-tag-1", "EchoTagOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("echo-tag-1", Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "gpu", "Activity handler should see tag='gpu'");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

/// ActivityContext::tag() returns None for untagged activities.
#[tokio::test]
async fn e2e_activity_context_tag_none_for_untagged() {
    use duroxide::runtime::{self, registry::ActivityRegistry};
    use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
    use std::time::Duration;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("EchoTag", |ctx: ActivityContext, _input: String| async move {
            Ok(ctx.tag().unwrap_or("none").to_string())
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let tag_value = ctx.schedule_activity("EchoTag", "").await?;
        Ok(tag_value)
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("EchoTagOrchDefault", orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("echo-tag-2", "EchoTagOrchDefault", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("echo-tag-2", Duration::from_secs(5))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "none", "Untagged activity should have tag=None");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Tags survive continue_as_new: the new execution can use .with_tag() and route correctly.
#[tokio::test]
async fn e2e_tag_routing_survives_continue_as_new() {
    use duroxide::runtime::{self, RuntimeOptions, registry::ActivityRegistry};
    use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, TagFilter};
    use std::time::Duration;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("GpuWork", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("gpu:{input}"))
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, input: String| async move {
        let count: u32 = input.parse().unwrap_or(0);
        if count < 1 {
            // First execution: continue_as_new with incremented counter
            return ctx.continue_as_new("1").await;
        }
        // Second execution: schedule a tagged activity
        let result = ctx
            .schedule_activity("GpuWork", format!("iter{count}"))
            .with_tag("gpu")
            .await?;
        Ok(result)
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("CanTagOrch", orchestration)
        .build();

    let opts = RuntimeOptions {
        worker_tag_filter: TagFilter::default_and(["gpu"]),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, opts).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("can-tag-1", "CanTagOrch", "0")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("can-tag-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "gpu:iter1");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

/// Two separate runtimes with different TagFilters process only their own tagged activities.
///
/// Runtime A: DefaultOnly (untagged only)
/// Runtime B: Tags(["gpu"]) (gpu only)
/// The orchestration schedules both tagged and untagged activities; completion
/// proves mutual exclusion.
#[tokio::test]
async fn e2e_two_workers_different_tags_no_overlap() {
    use duroxide::runtime::{self, RuntimeOptions, registry::ActivityRegistry};
    use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, TagFilter};
    use std::time::Duration;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let make_activities = || {
        ActivityRegistry::builder()
            .register("Work", |_ctx: ActivityContext, input: String| async move {
                Ok(format!("done:{input}"))
            })
            .build()
    };

    let make_orchestrations = || {
        OrchestrationRegistry::builder()
            .register("TwoPoolOrch", |ctx: OrchestrationContext, _input: String| async move {
                // Untagged → Runtime A
                let cpu = ctx.schedule_activity("Work", "cpu").await?;
                // Tagged → Runtime B
                let gpu = ctx.schedule_activity("Work", "gpu").with_tag("gpu").await?;
                Ok(format!("{cpu}|{gpu}"))
            })
            .build()
    };

    // Runtime A: orchestrator + default worker
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

    // Runtime B: worker-only, gpu
    let rt_b = runtime::Runtime::start_with_options(
        store.clone(),
        make_activities(),
        make_orchestrations(),
        RuntimeOptions {
            orchestration_concurrency: 0,
            worker_tag_filter: TagFilter::tags(["gpu"]),
            ..Default::default()
        },
    )
    .await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("two-pool-1", "TwoPoolOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("two-pool-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done:cpu|done:gpu");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt_b.shutdown(None).await;
    rt_a.shutdown(None).await;
}

/// Dropping a tagged DurableFuture triggers cancellation (select2 loser).
///
/// Schedules a tagged activity and races it against a short timer. The timer
/// wins, the tagged future is dropped, and the orchestration completes with
/// the timeout branch — proving cancellation semantics work with tags.
#[tokio::test]
async fn e2e_dropped_tagged_future_triggers_cancellation() {
    use duroxide::runtime::{self, RuntimeOptions, registry::ActivityRegistry};
    use duroxide::{ActivityContext, Client, Either2, OrchestrationContext, OrchestrationRegistry, TagFilter};
    use std::time::Duration;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("SlowGpu", |_ctx: ActivityContext, _input: String| async move {
            // Simulate slow GPU work — will be cancelled before completing
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok("should_not_reach".to_string())
        })
        .build();

    let orchestration = |ctx: OrchestrationContext, _input: String| async move {
        let gpu = ctx.schedule_activity("SlowGpu", "data").with_tag("gpu");
        let timeout = ctx.schedule_timer(Duration::from_millis(200));

        match ctx.select2(gpu, timeout).await {
            Either2::First(Ok(r)) => Ok(format!("gpu_won:{r}")),
            Either2::First(Err(e)) => Err(e),
            Either2::Second(()) => Ok("timeout_won".to_string()),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("CancelTagOrch", orchestration)
        .build();

    let opts = RuntimeOptions {
        worker_tag_filter: TagFilter::default_and(["gpu"]),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, opts).await;

    let client = Client::new(store.clone());
    client
        .start_orchestration("cancel-tag-1", "CancelTagOrch", "")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("cancel-tag-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "timeout_won", "Timer should win, tagged activity cancelled");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected status: {:?}", other),
    }

    rt.shutdown(None).await;
}

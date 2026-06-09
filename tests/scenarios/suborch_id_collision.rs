//! Sub-orchestration instance-id collision scenario.
//!
//! Child sub-orchestration instance ids reserve the `sub::` marker: the first parent
//! execution uses `{parent}::sub::{event_id}` and executions after continue-as-new use
//! `{parent}::sub::{execution_id}_{event_id}`. If an instance with that exact id already
//! exists in a terminal state when the parent schedules its child, the parent must not
//! hang forever waiting for a completion that never arrives — it must observe a
//! sub-orchestration failure and reach a terminal state.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use duroxide::providers::Provider;
use duroxide::providers::WorkItem;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::ExecutionMetadata;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{Client, Event, EventKind, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::sync::Arc;
use std::time::Duration;

#[path = "../common/mod.rs"]
mod common;

/// A pre-existing terminal instance occupying the parent's auto-generated child id
/// must not cause the parent to hang. The parent should reach a terminal state.
///
/// The colliding instance is enqueued directly through the provider to model a
/// client that does not validate instance ids (e.g. an older node during a rolling
/// upgrade), so this exercises the dispatcher's defense independently of the
/// client-side validation.
#[tokio::test]
async fn parent_does_not_hang_when_child_id_already_terminal() {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    // Parent's first action is a sub-orchestration call. Event 1 = OrchestrationStarted,
    // Event 2 = SubOrchestrationScheduled, so the auto-generated child id is
    // "{parent}::sub::2".
    let parent = |ctx: OrchestrationContext, _input: String| async move {
        match ctx.schedule_sub_orchestration("Child", "child-input").await {
            Ok(r) => Ok(format!("parent-got:{r}")),
            Err(e) => Err(format!("child-failed:{e}")),
        }
    };
    let child = |_ctx: OrchestrationContext, input: String| async move { Ok(format!("child-done:{input}")) };
    // Unrelated orchestration that completes immediately, used to occupy the child id.
    let squatter = |_ctx: OrchestrationContext, _input: String| async move { Ok("squatted".to_string()) };

    let orchs = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Child", child)
        .register("Squatter", squatter)
        .build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, orchs).await;
    let client = Client::new(store.clone());

    // Occupy the predicted child id with an unrelated, already-completed instance.
    // Enqueued directly (bypassing client-side validation) to model a non-validating client.
    let squat_id = "job-1::sub::2";
    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: squat_id.to_string(),
                orchestration: "Squatter".to_string(),
                input: String::new(),
                version: None,
                parent_instance: None,
                parent_id: None,
                execution_id: 1,
            },
            None,
        )
        .await
        .unwrap();
    let squat_status = client
        .wait_for_orchestration(squat_id, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(squat_status, OrchestrationStatus::Completed { .. }),
        "squatter must complete first; got {squat_status:?}"
    );

    // Start the parent. Its child id collides with the terminal squatter instance.
    client.start_orchestration("job-1", "Parent", "").await.unwrap();

    let status = client
        .wait_for_orchestration("job-1", Duration::from_secs(10))
        .await
        .expect("parent must reach a terminal state, not hang");

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(
                msg.contains("already exists"),
                "failure should reflect the child-id collision; got {msg:?}"
            );
        }
        other => panic!("parent should fail fast due to the child-id collision; got {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Genuine at-least-once redelivery of a completed child's own `StartOrchestration`
/// must not spuriously fail the parent. The child id already names a terminal instance,
/// but the incoming work item's parent matches that instance's recorded parent, so the
/// dispatcher must skip the collision notification and leave the parent completed.
#[tokio::test]
async fn redelivered_child_start_does_not_fail_parent() {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    let parent = |ctx: OrchestrationContext, _input: String| async move {
        let r = ctx.schedule_sub_orchestration("Child", "child-input").await?;
        Ok(format!("parent-got:{r}"))
    };
    let child = |_ctx: OrchestrationContext, input: String| async move { Ok(format!("child-done:{input}")) };

    let orchs = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Child", child)
        .build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, orchs).await;
    let client = Client::new(store.clone());

    client.start_orchestration("job-2", "Parent", "").await.unwrap();
    let status = client
        .wait_for_orchestration("job-2", Duration::from_secs(10))
        .await
        .unwrap();
    assert!(
        matches!(&status, OrchestrationStatus::Completed { output, .. } if output == "parent-got:child-done:child-input"),
        "parent should complete normally first; got {status:?}"
    );

    // Snapshot the parent's history before redelivery so we can prove nothing was appended.
    let parent_history_before = store.read("job-2").await.unwrap();

    // Redeliver the completed child's own StartOrchestration (same parent linkage).
    // The child id "job-2::sub::2" is now terminal; the dispatcher must treat this as
    // redelivery and not enqueue a SubOrchFailed for the parent.
    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "job-2::sub::2".to_string(),
                orchestration: "Child".to_string(),
                input: "child-input".to_string(),
                version: None,
                parent_instance: Some("job-2".to_string()),
                parent_id: Some(2),
                execution_id: 1,
            },
            None,
        )
        .await
        .unwrap();

    // Wait deterministically until the redelivered child start has drained from the
    // orchestrator queue (and the queue has settled), rather than sleeping a fixed time.
    wait_for_orchestrator_queue_drained(&store, Duration::from_secs(10)).await;

    // A spurious notification would be a parent-targeted SubOrchFailed. Assert none is
    // queued and none was appended to the parent's history. (Checking only that the parent
    // still reports Completed is insufficient: a SubOrchFailed delivered to an already
    // terminal parent is discarded by the terminal fast-ack path without a trace.)
    let depths = store
        .as_management_capability()
        .unwrap()
        .get_queue_depths()
        .await
        .unwrap();
    assert_eq!(
        depths.orchestrator_queue, 0,
        "no parent-targeted SubOrchFailed should remain queued after redelivery"
    );

    let parent_history_after = store.read("job-2").await.unwrap();
    assert_eq!(
        parent_history_after.len(),
        parent_history_before.len(),
        "redelivery must not append any event (e.g. SubOrchestrationFailed) to the parent"
    );
    assert!(
        !parent_history_after
            .iter()
            .any(|e| matches!(e.kind, duroxide::EventKind::OrchestrationFailed { .. })),
        "parent history must not contain an OrchestrationFailed event after redelivery"
    );

    let after = client.get_orchestration_status("job-2").await.unwrap();
    assert!(
        matches!(after, OrchestrationStatus::Completed { .. }),
        "redelivery must not fail the parent; got {after:?}"
    );

    rt.shutdown(None).await;
}

/// A parent that schedules a sub-orchestration on every continue-as-new iteration
/// must not collide with itself. Event ids reset on continue-as-new, so without
/// execution-scoped child ids the second iteration would regenerate the same child
/// id as the first (now terminal) and hang. The auto-generated id includes the
/// parent execution after the first execution, keeping each iteration's child unique.
#[tokio::test]
async fn parent_with_suborch_survives_continue_as_new() {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    // Each execution's first action is a sub-orchestration call, then it continues as
    // new with an incremented counter until it reaches the limit.
    let parent = |ctx: OrchestrationContext, input: String| async move {
        let n: u32 = input.parse().unwrap_or(0);
        let r = ctx.schedule_sub_orchestration("Child", n.to_string()).await?;
        if n < 3 {
            return ctx.continue_as_new((n + 1).to_string()).await;
        }
        Ok(format!("done:{n}:{r}"))
    };
    let child = |_ctx: OrchestrationContext, input: String| async move { Ok(format!("child-done:{input}")) };

    let orchs = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Child", child)
        .build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, orchs).await;
    let client = Client::new(store.clone());

    client.start_orchestration("can-job", "Parent", "0").await.unwrap();

    let status = client
        .wait_for_orchestration("can-job", Duration::from_secs(10))
        .await
        .expect("parent must run through all continue-as-new iterations, not hang");

    assert!(
        matches!(&status, OrchestrationStatus::Completed { output, .. } if output == "done:3:child-done:3"),
        "parent should complete after looping with sub-orchestrations; got {status:?}"
    );

    rt.shutdown(None).await;
}

/// Regression for execution-scoped routing of the terminal-collision failure within a
/// single end-to-end run.
///
/// A parent continues as new and, on execution 2, schedules a sub-orchestration whose
/// auto-generated child id (`{parent}::sub::{execution_id}_{event_id}`) already names a
/// terminal instance. The collision failure must be recorded in execution 2, not
/// misrouted to execution 1. This drives the full flow through one runtime; the
/// `terminal_collision_routes_to_parent_current_execution_on_fresh_runtime` test below
/// is the stronger cross-runtime guard.
#[tokio::test]
async fn parent_on_execution_two_fails_fast_on_child_id_collision() {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    // On execution 1 the parent immediately continues as new; on execution 2 its first
    // action is a sub-orchestration call. Event 1 = OrchestrationStarted, event 2 =
    // SubOrchestrationScheduled, so the execution-2 child id is "coll::sub::2_2".
    let parent = |ctx: OrchestrationContext, input: String| async move {
        let n: u32 = input.parse().unwrap_or(0);
        if n == 0 {
            return ctx.continue_as_new("1").await;
        }
        match ctx.schedule_sub_orchestration("Child", "x").await {
            Ok(r) => Ok(format!("parent-got:{r}")),
            Err(e) => Err(format!("child-failed:{e}")),
        }
    };
    let child = |_ctx: OrchestrationContext, input: String| async move { Ok(format!("child-done:{input}")) };
    let squatter = |_ctx: OrchestrationContext, _input: String| async move { Ok("squatted".to_string()) };

    let squat_id = "coll::sub::2_2";

    // Runtime A occupies the predicted execution-2 child id with an unrelated terminal
    // instance, then shuts down so it holds no in-memory routing state for the parent.
    {
        let orchs = OrchestrationRegistry::builder().register("Squatter", squatter).build();
        let acts = ActivityRegistry::builder().build();
        let rt_a = runtime::Runtime::start_with_store(store.clone(), acts, orchs).await;
        let client_a = Client::new(store.clone());
        store
            .enqueue_for_orchestrator(
                WorkItem::StartOrchestration {
                    instance: squat_id.to_string(),
                    orchestration: "Squatter".to_string(),
                    input: String::new(),
                    version: None,
                    parent_instance: None,
                    parent_id: None,
                    execution_id: 1,
                },
                None,
            )
            .await
            .unwrap();
        let squat_status = client_a
            .wait_for_orchestration(squat_id, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            matches!(squat_status, OrchestrationStatus::Completed { .. }),
            "squatter must complete first; got {squat_status:?}"
        );
        rt_a.shutdown(None).await;
    }

    // Runtime B is a fresh runtime (no cached execution ids). It drives the parent to
    // execution 2, where the child id collides with the terminal squatter.
    let orchs = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Child", child)
        .build();
    let acts = ActivityRegistry::builder().build();
    let rt_b = runtime::Runtime::start_with_store(store.clone(), acts, orchs).await;
    let client_b = Client::new(store.clone());

    client_b.start_orchestration("coll", "Parent", "0").await.unwrap();

    let status = client_b
        .wait_for_orchestration("coll", Duration::from_secs(10))
        .await
        .expect("parent on execution 2 must fail fast, not hang");

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(
                msg.contains("already exists"),
                "failure should reflect the child-id collision; got {msg:?}"
            );
        }
        other => panic!("parent should fail fast due to the child-id collision; got {other:?}"),
    }

    // The failure must be recorded in execution 2 (proves the notification routed to the
    // parent's current execution, not execution 1).
    let exec2 = store.read_with_execution("coll", 2).await.unwrap();
    assert!(
        exec2
            .iter()
            .any(|e| matches!(e.kind, duroxide::EventKind::OrchestrationFailed { .. })),
        "execution 2 history must contain the OrchestrationFailed event"
    );

    rt_b.shutdown(None).await;
}

/// Stronger cross-runtime regression for terminal-collision routing.
///
/// Here the runtime that processes the colliding child start has *never* run the parent,
/// so it holds no in-memory association between the parent and its current execution. The
/// parent's execution-2 state (parked awaiting a sub-orchestration whose id collides with a
/// foreign terminal instance) is seeded directly into the provider. A fresh runtime must
/// read the parent's current execution (2) from durable provider state when routing the
/// `SubOrchFailed`, so the failure lands in execution 2 and the parent fails fast. If the
/// failure were routed to execution 1 (as a process-local cache miss would default to), the
/// parent's replay would filter it out and the parent would hang.
#[tokio::test]
async fn terminal_collision_routes_to_parent_current_execution_on_fresh_runtime() {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    let parent_id = "seeded-parent";
    let child_id = "seeded-parent::sub::2_2";

    // 1. Seed a foreign terminal instance occupying the parent's execution-2 child id.
    common::seed_history_turn(
        store.as_ref(),
        WorkItem::StartOrchestration {
            instance: child_id.to_string(),
            orchestration: "Squatter".to_string(),
            input: String::new(),
            version: Some("1.0.0".to_string()),
            parent_instance: None,
            parent_id: None,
            execution_id: 1,
        },
        1,
        vec![
            Event::with_event_id(
                1,
                child_id,
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "Squatter".to_string(),
                    version: "1.0.0".to_string(),
                    input: String::new(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            ),
            Event::with_event_id(
                2,
                child_id,
                1,
                None,
                EventKind::OrchestrationCompleted {
                    output: "squatted".to_string(),
                },
            ),
        ],
        vec![],
        ExecutionMetadata {
            orchestration_name: Some("Squatter".to_string()),
            orchestration_version: Some("1.0.0".to_string()),
            ..Default::default()
        },
    )
    .await;

    // 2. Seed the parent directly on execution 2, parked awaiting the colliding child.
    //    On execution 2 its first action is the sub-orchestration call: event 1 =
    //    OrchestrationStarted, event 2 = SubOrchestrationScheduled, id "...::sub::2_2".
    common::seed_history_turn(
        store.as_ref(),
        WorkItem::StartOrchestration {
            instance: parent_id.to_string(),
            orchestration: "Parent".to_string(),
            input: "1".to_string(),
            version: Some("1.0.0".to_string()),
            parent_instance: None,
            parent_id: None,
            execution_id: 2,
        },
        2,
        vec![
            Event::with_event_id(
                1,
                parent_id,
                2,
                None,
                EventKind::OrchestrationStarted {
                    name: "Parent".to_string(),
                    version: "1.0.0".to_string(),
                    input: "1".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            ),
            Event::with_event_id(
                2,
                parent_id,
                2,
                None,
                EventKind::SubOrchestrationScheduled {
                    name: "Child".to_string(),
                    instance: child_id.to_string(),
                    input: "x".to_string(),
                },
            ),
        ],
        vec![],
        ExecutionMetadata {
            orchestration_name: Some("Parent".to_string()),
            orchestration_version: Some("1.0.0".to_string()),
            ..Default::default()
        },
    )
    .await;

    // Sanity: durable state reports the parent on execution 2.
    assert_eq!(
        store.read(parent_id).await.unwrap().iter().map(|e| e.execution_id).max(),
        Some(2),
        "seeded parent must be on execution 2"
    );

    // 3. Enqueue the colliding child start, as a runtime would have when the parent
    //    scheduled the sub-orchestration. Its target id is already terminal (the foreign
    //    squatter), and its parent differs, so this is a genuine collision.
    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: child_id.to_string(),
                orchestration: "Child".to_string(),
                input: "x".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: Some(parent_id.to_string()),
                parent_id: Some(2),
                execution_id: 1,
            },
            None,
        )
        .await
        .unwrap();

    // 4. A fresh runtime that never ran the parent processes the collision and must route
    //    the failure to the parent's current execution (2), read from durable state.
    let parent = |ctx: OrchestrationContext, input: String| async move {
        let n: u32 = input.parse().unwrap_or(0);
        if n == 0 {
            return ctx.continue_as_new("1").await;
        }
        match ctx.schedule_sub_orchestration("Child", "x").await {
            Ok(r) => Ok(format!("parent-got:{r}")),
            Err(e) => Err(format!("child-failed:{e}")),
        }
    };
    let child = |_ctx: OrchestrationContext, input: String| async move { Ok(format!("child-done:{input}")) };

    let orchs = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Child", child)
        .build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, orchs).await;
    let client = Client::new(store.clone());

    let status = client
        .wait_for_orchestration(parent_id, Duration::from_secs(10))
        .await
        .expect("fresh runtime must route the failure to execution 2, not hang");

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(
                msg.contains("already exists"),
                "failure should reflect the child-id collision; got {msg:?}"
            );
        }
        other => panic!("parent should fail fast due to the child-id collision; got {other:?}"),
    }

    let exec2 = store.read_with_execution(parent_id, 2).await.unwrap();
    assert!(
        exec2
            .iter()
            .any(|e| matches!(e.kind, EventKind::OrchestrationFailed { .. })),
        "execution 2 history must contain the OrchestrationFailed event"
    );

    rt.shutdown(None).await;
}
/// `sub::` marker validation that `Client::start_orchestration` enforces. An orchestration
/// may therefore use an id that a top-level client start would reject, and it is used as
/// the exact child instance id.
#[tokio::test]
async fn explicit_sub_orchestration_id_bypasses_reserved_marker_validation() {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    // An id a top-level client start would reject (contains the reserved `::sub::` infix),
    // used verbatim as an explicit child id.
    let explicit_id = "tenant::sub::99";

    let parent = |ctx: OrchestrationContext, _input: String| async move {
        let r = ctx
            .schedule_sub_orchestration_with_id("Child", "tenant::sub::99", "child-input")
            .await?;
        Ok(format!("parent-got:{r}"))
    };
    let child = |_ctx: OrchestrationContext, input: String| async move { Ok(format!("child-done:{input}")) };

    let orchs = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Child", child)
        .build();
    let acts = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), acts, orchs).await;
    let client = Client::new(store.clone());

    // The same id is rejected for a top-level start.
    let rejected = client.start_orchestration(explicit_id, "Parent", "").await;
    assert!(
        matches!(rejected, Err(duroxide::ClientError::InvalidInput { .. })),
        "top-level start with the reserved marker must be rejected; got {rejected:?}"
    );

    // But the explicit sub-orchestration escape hatch allows it.
    client.start_orchestration("tenant-parent", "Parent", "").await.unwrap();
    let status = client
        .wait_for_orchestration("tenant-parent", Duration::from_secs(10))
        .await
        .expect("parent using an explicit reserved-shaped child id should complete");
    assert!(
        matches!(&status, OrchestrationStatus::Completed { output, .. } if output == "parent-got:child-done:child-input"),
        "parent should complete with the explicit child id; got {status:?}"
    );

    // The child ran under the exact explicit id (no parent prefix).
    let child_status = client.get_orchestration_status(explicit_id).await.unwrap();
    assert!(
        matches!(child_status, OrchestrationStatus::Completed { .. }),
        "explicit child id must be used verbatim; got {child_status:?}"
    );

    rt.shutdown(None).await;
}

/// Poll until the orchestrator queue has drained and stayed empty across several reads,
/// so a transient parent-targeted `SubOrchFailed` (if one were wrongly enqueued) is given
/// a chance to appear before we assert its absence.
async fn wait_for_orchestrator_queue_drained(store: &Arc<dyn Provider>, timeout: Duration) {
    let mgmt = store.as_management_capability().expect("management capability");
    let deadline = std::time::Instant::now() + timeout;
    let mut consecutive_empty = 0;
    loop {
        let depth = mgmt.get_queue_depths().await.unwrap().orchestrator_queue;
        if depth == 0 {
            consecutive_empty += 1;
            if consecutive_empty >= 5 {
                return;
            }
        } else {
            consecutive_empty = 0;
        }
        if std::time::Instant::now() >= deadline {
            panic!("orchestrator queue did not drain within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

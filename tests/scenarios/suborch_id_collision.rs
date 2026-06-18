//! Sub-orchestration instance-id collision scenarios.
//!
//! Child sub-orchestration instance ids reserve the `sub::` marker. The suffix is produced
//! by [`duroxide::auto_sub_orch_suffix`]: the first parent execution uses
//! `{parent}::sub::{event_id}`, and executions after `continue_as_new` use
//! `{parent}::sub::{execution_id}_{event_id}`.
//!
//! ## The real collision class: same parent across continue-as-new
//!
//! Because the full child id embeds the parent instance id, children of *different* parents
//! never collide as long as root instance ids are unique across the provider. The genuine
//! collision the execution-scoped suffix defends against is a single parent that schedules a
//! sub-orchestration at the same event position on each `continue_as_new` generation: event
//! ids reset on continue-as-new, so without the execution-scoped suffix execution 2 would
//! regenerate execution 1's (now terminal) child id and the parent would hang forever. The
//! `continue_as_new_*` tests below are the primary regressions for this.
//!
//! ## Legacy / provider-bypass defense
//!
//! If an id with the reserved marker somehow already names a terminal instance when a parent
//! schedules its child — e.g. an older, non-validating client enqueued it directly through
//! the provider during a rolling upgrade — the parent must still not hang: the dispatcher
//! observes the terminal collision and fails the parent fast. The tests that *inject* a
//! foreign terminal "squatter" via the provider exercise that defense and the durable
//! routing of the failure notification; they are not the normal auto-generated collision
//! case and are labeled accordingly.

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

/// Legacy / provider-bypass defense (not the normal auto-generated collision case).
///
/// A pre-existing terminal instance occupying the parent's auto-generated child id must not
/// cause the parent to hang. The parent should reach a terminal state.
///
/// Under unique root ids this collision cannot arise from auto-generated ids alone, so the
/// colliding instance is enqueued directly through the provider to model a client that does
/// not validate instance ids (e.g. an older node during a rolling upgrade). This exercises
/// the dispatcher's terminal-collision defense independently of client-side validation.
#[tokio::test]
async fn legacy_provider_bypass_terminal_collision_does_not_hang_parent() {
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
                parent_execution_id: None,
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
                parent_execution_id: None,
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

/// PRIMARY regression for the real collision class: a parent that schedules a
/// sub-orchestration on every continue-as-new iteration must not collide with itself.
///
/// Event ids reset on continue-as-new, so without execution-scoped child ids the second
/// iteration would regenerate the same child id as the first (now terminal) and hang. The
/// auto-generated suffix includes the parent execution after the first execution, keeping
/// each iteration's child unique. This asserts both that the parent completes and that the
/// per-execution child suffixes are exactly `sub::2`, `sub::2_2`, `sub::3_2`, `sub::4_2`.
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

    // Each execution must schedule a distinctly-suffixed child: the first keeps the legacy
    // `sub::{event_id}` form; later executions include the execution id, so no two iterations
    // ever regenerate the same (now terminal) child id.
    let mut suffixes = Vec::new();
    for execution_id in 1..=4 {
        suffixes.push(scheduled_child_suffix(&store, "can-job", execution_id).await);
    }
    assert_eq!(
        suffixes,
        vec![
            "sub::2".to_string(),
            "sub::2_2".to_string(),
            "sub::3_2".to_string(),
            "sub::4_2".to_string(),
        ],
        "each continue-as-new execution must generate a unique, execution-scoped child id"
    );

    rt.shutdown(None).await;
}

/// Focused regression (affandar): after the first continue-as-new generation the child id
/// must include the execution id, so a parent that schedules a sub-orchestration at the same
/// event position on each generation never reuses a now-terminal child id.
///
/// With the old id generation this hangs: execution 2 tries to start `P::sub::2`, finds
/// execution 1's child already terminal, and the parent never receives a completion. The
/// assertion pins the exact generated suffixes (`sub::2`, `sub::2_2`, `sub::3_2`).
#[tokio::test]
async fn continue_as_new_suborch_child_ids_include_execution_after_first() {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let parent_id = "can-child-id-job";

    let parent = |ctx: OrchestrationContext, input: String| async move {
        let n: u32 = input.parse().unwrap_or(0);
        let result = ctx.schedule_sub_orchestration("Child", n.to_string()).await?;
        if n < 2 {
            return ctx.continue_as_new((n + 1).to_string()).await;
        }
        Ok(format!("done:{n}:{result}"))
    };
    let child = |_ctx: OrchestrationContext, input: String| async move { Ok(format!("child-done:{input}")) };

    let orchestrations = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Child", child)
        .build();
    let activities = ActivityRegistry::builder().build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    client.start_orchestration(parent_id, "Parent", "0").await.unwrap();

    let status = client
        .wait_for_orchestration(parent_id, Duration::from_secs(5))
        .await
        .expect("parent must not hang from reusing the same child id after continue-as-new");

    assert!(
        matches!(&status, OrchestrationStatus::Completed { output, .. } if output == "done:2:child-done:2"),
        "parent should complete after three executions; got {status:?}"
    );

    let mut scheduled_child_suffixes = Vec::new();
    for execution_id in 1..=3 {
        scheduled_child_suffixes.push(scheduled_child_suffix(&store, parent_id, execution_id).await);
    }
    assert_eq!(
        scheduled_child_suffixes,
        vec!["sub::2".to_string(), "sub::2_2".to_string(), "sub::3_2".to_string()],
    );

    rt.shutdown(None).await;
}

/// Read the auto-generated child suffix recorded by the `SubOrchestrationScheduled` event in
/// the given parent execution. The event stores the suffix (e.g. `sub::2`), not the full
/// `{parent}::sub::...` id.
async fn scheduled_child_suffix(store: &Arc<dyn Provider>, parent_instance: &str, execution_id: u64) -> String {
    let history = store.read_with_execution(parent_instance, execution_id).await.unwrap();
    history
        .iter()
        .find_map(|event| match &event.kind {
            EventKind::SubOrchestrationScheduled { instance, .. } => Some(instance.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("execution {execution_id} should schedule a sub-orchestration"))
}

/// Legacy / provider-bypass defense: execution-scoped routing of a terminal-collision
/// failure within a single end-to-end run.
///
/// A parent continues as new and, on execution 2, schedules a sub-orchestration whose
/// auto-generated child id (`{parent}::sub::{execution_id}_{event_id}`) already names a
/// terminal instance injected via the provider. The collision failure must be recorded in
/// execution 2, not misrouted to execution 1. Here the failure is produced while the parent's
/// own turn is running, so the `parent_execution_id` is stamped onto the colliding start and
/// used directly. This drives the full flow through one runtime; the
/// `legacy_provider_bypass_terminal_collision_routes_via_fallback_on_fresh_runtime` test below
/// is the stronger cross-runtime guard for the provider-read fallback.
#[tokio::test]
async fn legacy_provider_bypass_terminal_collision_fails_fast_on_execution_two() {
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
                    parent_execution_id: None,
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

/// Legacy / provider-bypass defense: stronger cross-runtime regression for the
/// terminal-collision routing *fallback*.
///
/// Here the runtime that processes the colliding child start has *never* run the parent, and
/// the colliding start carries no stamped `parent_execution_id` (it is seeded directly into
/// the provider, modeling an old work item). The parent's execution-2 state (parked awaiting a
/// sub-orchestration whose id collides with a foreign terminal instance) is seeded directly
/// into the provider. With no stamp to use, the dispatcher must fall back to reading the
/// parent's current execution (2) from durable provider state when routing the `SubOrchFailed`,
/// so the failure lands in execution 2 and the parent fails fast. If the failure were routed to
/// execution 1 (as a process-local cache miss would default to), the parent's replay would
/// filter it out and the parent would hang.
#[tokio::test]
async fn legacy_provider_bypass_terminal_collision_routes_via_fallback_on_fresh_runtime() {
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
            parent_execution_id: None,
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
                    parent_execution_id: None,
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
            parent_execution_id: None,
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
                    parent_execution_id: None,
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

    // 3. Enqueue the colliding child start with NO stamped parent_execution_id, modeling a
    //    work item produced before this field existed (rolling upgrade). Its target id is
    //    already terminal (the foreign squatter), and its parent differs, so this is a
    //    genuine collision. With no stamp, routing must fall back to a durable provider read.
    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: child_id.to_string(),
                orchestration: "Child".to_string(),
                input: "x".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: Some(parent_id.to_string()),
                parent_id: Some(2),
                parent_execution_id: None,
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

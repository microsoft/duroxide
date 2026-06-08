//! Sub-orchestration instance-id collision scenario.
//!
//! Child sub-orchestration instance ids are auto-generated as
//! `{parent}::sub::{event_id}`. If an instance with that exact id already exists
//! in a terminal state when the parent schedules its child, the parent must not
//! hang forever waiting for a completion that never arrives — it must observe a
//! sub-orchestration failure and reach a terminal state.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use duroxide::providers::Provider;
use duroxide::providers::WorkItem;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::sync::Arc;
use std::time::Duration;

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

    // Redeliver the completed child's own StartOrchestration (same parent linkage).
    // The child id "job-2::sub::2" is now terminal; the dispatcher must treat this as
    // redelivery and not fail the parent.
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

    // Give the dispatcher time to process the redelivered item, then confirm the parent
    // is still Completed (not spuriously failed).
    tokio::time::sleep(Duration::from_secs(1)).await;
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

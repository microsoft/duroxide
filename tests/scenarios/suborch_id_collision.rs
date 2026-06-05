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

    assert!(
        matches!(status, OrchestrationStatus::Failed { .. }),
        "parent should fail fast due to the child-id collision; got {status:?}"
    );

    rt.shutdown(None).await;
}

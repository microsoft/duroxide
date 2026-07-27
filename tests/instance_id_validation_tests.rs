//! Instance id validation: reserved sub-orchestration markers are rejected.
//!
//! Child sub-orchestration ids are auto-generated as `{parent}::sub::{event_id}` on the
//! first parent execution and `{parent}::sub::{execution_id}_{event_id}` after
//! `continue_as_new` (see `duroxide::auto_sub_orch_suffix`). Two different rules guard
//! that namespace, because they answer two different questions:
//!
//! - **`Client::start_orchestration`** rejects a leading `sub::` *and* the `::sub::` infix.
//!   A top-level id must not occupy a name some future child will need, and child names
//!   carry the marker anywhere in the string.
//! - **`ctx.schedule_sub_orchestration_with_id`** rejects only a leading `sub::`. That
//!   prefix is a control signal the runtime reads to mean "auto-generated suffix, add the
//!   parent prefix", so an explicit id starting with it would be silently rewritten. The
//!   infix must stay legal for child ids: the runtime generates it itself, e.g. a
//!   grandchild of `root` is `root::sub::2::sub::2`.
//!
//! Other uses of `::` (e.g. `i4::child-1`) remain valid everywhere.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::sync::Arc;
use std::time::Duration;

async fn client() -> Client {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    Client::new(store)
}

#[tokio::test]
async fn start_orchestration_rejects_reserved_infix() {
    let err = client()
        .await
        .start_orchestration("victim::sub::2", "AnyOrch", "")
        .await
        .expect_err("instance id containing the reserved '::sub::' marker must be rejected");

    assert!(
        matches!(err, duroxide::ClientError::InvalidInput { .. }),
        "expected InvalidInput, got {err:?}"
    );
}

#[tokio::test]
async fn start_orchestration_rejects_reserved_prefix() {
    let err = client()
        .await
        .start_orchestration("sub::pending_1", "AnyOrch", "")
        .await
        .expect_err("instance id starting with the reserved 'sub::' marker must be rejected");

    assert!(matches!(err, duroxide::ClientError::InvalidInput { .. }), "got {err:?}");
}

#[tokio::test]
async fn start_orchestration_versioned_rejects_reserved_infix() {
    let err = client()
        .await
        .start_orchestration_versioned("a::sub::3", "AnyOrch", "1.0.0", "")
        .await
        .expect_err("instance id containing the reserved '::sub::' marker must be rejected");

    assert!(matches!(err, duroxide::ClientError::InvalidInput { .. }), "got {err:?}");
}

#[tokio::test]
async fn start_orchestration_accepts_normal_id() {
    client()
        .await
        .start_orchestration("order-123", "AnyOrch", "")
        .await
        .expect("normal instance id must be accepted");
}

#[tokio::test]
async fn start_orchestration_accepts_non_reserved_double_colon() {
    // `::` is only reserved in the `sub::` form; other uses remain valid.
    client()
        .await
        .start_orchestration("i4::child-1", "AnyOrch", "")
        .await
        .expect("non-reserved '::' instance id must be accepted");
}

// ---------------------------------------------------------------------------
// Detached starts: same rule as `Client::start_orchestration`.
//
// `ctx.schedule_orchestration()` creates a *root* instance with a caller-supplied id, used
// verbatim with no parent prefix, so it must honour the root rule rather than the narrower
// child rule. It returns `()`, so the rule is enforced by panic — which the runtime turns
// into a deterministic orchestration failure.
// ---------------------------------------------------------------------------

/// Drive a parent that detaches one orchestration with the given id, and return its
/// terminal status plus the set of instances that exist afterwards.
async fn run_parent_with_detached_id(detached_id: &'static str) -> (OrchestrationStatus, Vec<String>) {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    let parent = move |ctx: OrchestrationContext, _input: String| async move {
        ctx.schedule_orchestration("Detached", detached_id, "payload");
        Ok("scheduled".to_string())
    };
    let detached = |_ctx: OrchestrationContext, _input: String| async move { Ok("detached-done".to_string()) };

    let orchs = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Detached", detached)
        .build();
    let rt = runtime::Runtime::start_with_store(store.clone(), ActivityRegistry::builder().build(), orchs).await;
    let client = Client::new(store.clone());

    client.start_orchestration("dp", "Parent", "").await.unwrap();
    let status = client
        .wait_for_orchestration("dp", Duration::from_secs(10))
        .await
        .expect("parent must reach a terminal state");

    // Give a detached start, if one were emitted, a chance to materialize.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let instances = store
        .as_management_capability()
        .expect("management capability")
        .list_instances()
        .await
        .unwrap();

    rt.shutdown(None).await;
    (status, instances)
}

/// A detached root id starting with `sub::` must be rejected, like a top-level client start.
#[tokio::test]
async fn schedule_orchestration_rejects_reserved_prefix() {
    let (status, instances) = run_parent_with_detached_id("sub::squatter").await;

    assert!(
        matches!(&status, OrchestrationStatus::Failed { details, .. }
            if details.display_message().contains("reserved sub-orchestration marker")),
        "parent should fail with the reserved-marker error; got {status:?}"
    );
    assert_eq!(
        instances,
        vec!["dp".to_string()],
        "no detached instance may be created for a rejected id"
    );
}

/// The `::sub::` infix is reserved for root ids too — it is the shape of a full child id,
/// so a detached instance using it could squat a child of `victim`.
#[tokio::test]
async fn schedule_orchestration_rejects_reserved_infix() {
    let (status, instances) = run_parent_with_detached_id("victim::sub::2").await;

    assert!(
        matches!(&status, OrchestrationStatus::Failed { details, .. }
            if details.display_message().contains("reserved sub-orchestration marker")),
        "parent should fail with the reserved-marker error; got {status:?}"
    );
    assert_eq!(
        instances,
        vec!["dp".to_string()],
        "no detached instance may be created for a rejected id"
    );
}

/// Ordinary detached ids — including non-reserved `::` — still work and start verbatim.
#[tokio::test]
async fn schedule_orchestration_accepts_normal_id() {
    let (status, mut instances) = run_parent_with_detached_id("tenant-7::order-42").await;

    assert!(
        matches!(&status, OrchestrationStatus::Completed { output, .. } if output == "scheduled"),
        "parent should complete; got {status:?}"
    );
    instances.sort();
    assert_eq!(
        instances,
        vec!["dp".to_string(), "tenant-7::order-42".to_string()],
        "detached instance must be created with the id used verbatim"
    );
}

// ---------------------------------------------------------------------------
// Explicit sub-orchestration ids: only a leading `sub::` is rejected.
// ---------------------------------------------------------------------------

/// Drive a parent that schedules one child with the given explicit id, and return the
/// parent's terminal output/error together with the set of instances that exist afterwards.
async fn run_parent_with_explicit_child_id(explicit_id: &'static str) -> (OrchestrationStatus, Vec<String>) {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    let parent = move |ctx: OrchestrationContext, _input: String| async move {
        match ctx
            .schedule_sub_orchestration_with_id("Child", explicit_id, "child-input")
            .await
        {
            Ok(r) => Ok(format!("ok:{r}")),
            Err(e) => Err(format!("rejected:{e}")),
        }
    };
    let child = |_ctx: OrchestrationContext, input: String| async move { Ok(format!("child-done:{input}")) };

    let orchs = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Child", child)
        .build();
    let rt = runtime::Runtime::start_with_store(store.clone(), ActivityRegistry::builder().build(), orchs).await;
    let client = Client::new(store.clone());

    client.start_orchestration("p", "Parent", "").await.unwrap();
    let status = client
        .wait_for_orchestration("p", Duration::from_secs(10))
        .await
        .expect("parent must reach a terminal state");

    let instances = store
        .as_management_capability()
        .expect("management capability")
        .list_instances()
        .await
        .unwrap();

    rt.shutdown(None).await;
    (status, instances)
}

/// A leading `sub::` is the runtime's marker for "auto-generated suffix, add the parent
/// prefix". Passing it as an explicit id used to be silently rewritten to
/// `{parent}::sub::my-child`; it must now be refused instead, with no child created.
#[tokio::test]
async fn sub_orchestration_with_id_rejects_leading_reserved_marker() {
    let (status, instances) = run_parent_with_explicit_child_id("sub::my-child").await;

    match &status {
        OrchestrationStatus::Failed { details, .. } => {
            let msg = details.display_message();
            assert!(
                msg.contains("reserved marker") && msg.contains("sub::my-child"),
                "error should name the reserved marker and the offending id; got {msg:?}"
            );
        }
        other => panic!("parent should fail with the rejection error; got {other:?}"),
    }

    assert_eq!(
        instances,
        vec!["p".to_string()],
        "no child instance may be created for a rejected id (in particular not the \
         silently-prefixed 'p::sub::my-child')"
    );
}

/// `sub::pending_` is an internal placeholder that `update_action_event_id` replaces
/// wholesale, so an explicit id with that prefix used to be discarded outright and the
/// child ran under an auto-generated name. It must now be refused.
#[tokio::test]
async fn sub_orchestration_with_id_rejects_pending_placeholder_prefix() {
    let (status, instances) = run_parent_with_explicit_child_id("sub::pending_99").await;

    assert!(
        matches!(&status, OrchestrationStatus::Failed { details, .. }
            if details.display_message().contains("reserved marker")),
        "parent should fail with the rejection error; got {status:?}"
    );
    assert_eq!(
        instances,
        vec!["p".to_string()],
        "the placeholder-shaped id must not be silently replaced by an auto-generated child"
    );
}

/// The `::sub::` infix stays legal for explicit child ids and is used verbatim — the
/// runtime itself generates ids of that shape for grandchildren.
#[tokio::test]
async fn sub_orchestration_with_id_allows_reserved_marker_infix() {
    let (status, mut instances) = run_parent_with_explicit_child_id("tenant::sub::99").await;

    assert!(
        matches!(&status, OrchestrationStatus::Completed { output, .. } if output == "ok:child-done:child-input"),
        "an infix marker must be accepted; got {status:?}"
    );
    instances.sort();
    assert_eq!(
        instances,
        vec!["p".to_string(), "tenant::sub::99".to_string()],
        "the explicit id must be used verbatim, with no parent prefix added"
    );
}

/// The versioned overload funnels through the same validation.
#[tokio::test]
async fn sub_orchestration_versioned_with_id_rejects_leading_reserved_marker() {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    let parent = |ctx: OrchestrationContext, _input: String| async move {
        match ctx
            .schedule_sub_orchestration_versioned_with_id("Child", Some("1.0.0".to_string()), "sub::v", "x")
            .await
        {
            Ok(r) => Ok(format!("ok:{r}")),
            Err(e) => Err(format!("rejected:{e}")),
        }
    };
    let child = |_ctx: OrchestrationContext, _input: String| async move { Ok("child-done".to_string()) };

    let orchs = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Child", child)
        .build();
    let rt = runtime::Runtime::start_with_store(store.clone(), ActivityRegistry::builder().build(), orchs).await;
    let client = Client::new(store.clone());

    client.start_orchestration("pv", "Parent", "").await.unwrap();
    let status = client
        .wait_for_orchestration("pv", Duration::from_secs(10))
        .await
        .expect("parent must reach a terminal state");

    assert!(
        matches!(&status, OrchestrationStatus::Failed { details, .. }
            if details.display_message().contains("reserved marker")),
        "versioned overload should reject the same ids; got {status:?}"
    );

    rt.shutdown(None).await;
}

/// Rejection must not write anything to history: no `SubOrchestrationScheduled` event is
/// recorded, so replay stays consistent and no work item is ever enqueued for a child.
#[tokio::test]
async fn rejected_explicit_id_writes_no_scheduling_event() {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    let parent = |ctx: OrchestrationContext, _input: String| async move {
        // Swallow the rejection and complete normally, so the parent's history is readable
        // and we can assert on exactly which events were written.
        let outcome = match ctx.schedule_sub_orchestration_with_id("Child", "sub::nope", "x").await {
            Ok(_) => "scheduled",
            Err(_) => "rejected",
        };
        Ok(outcome.to_string())
    };
    let child = |_ctx: OrchestrationContext, _input: String| async move { Ok("child-done".to_string()) };

    let orchs = OrchestrationRegistry::builder()
        .register("Parent", parent)
        .register("Child", child)
        .build();
    let rt = runtime::Runtime::start_with_store(store.clone(), ActivityRegistry::builder().build(), orchs).await;
    let client = Client::new(store.clone());

    client.start_orchestration("ph", "Parent", "").await.unwrap();
    let status = client
        .wait_for_orchestration("ph", Duration::from_secs(10))
        .await
        .expect("parent must complete");
    assert!(
        matches!(&status, OrchestrationStatus::Completed { output, .. } if output == "rejected"),
        "the future should resolve to Err without scheduling; got {status:?}"
    );

    let history = store.read("ph").await.unwrap();
    assert!(
        !history
            .iter()
            .any(|e| matches!(e.kind, duroxide::EventKind::SubOrchestrationScheduled { .. })),
        "a rejected id must not produce a SubOrchestrationScheduled event; got {history:#?}"
    );

    rt.shutdown(None).await;
}

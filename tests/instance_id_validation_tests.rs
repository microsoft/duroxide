//! Instance id validation: reserved sub-orchestration markers are rejected.
//!
//! Child sub-orchestration instance ids are auto-generated as
//! `{parent}::sub::{event_id}`. User-supplied instance ids must not collide with
//! that reserved form, otherwise they can squat a future child id. Other uses of
//! `::` (e.g. `i4::child-1`) remain valid.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use duroxide::Client;
use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use std::sync::Arc;

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

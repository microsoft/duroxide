//! Scenario tests for name and identifier size limits (Phase 2).
//!
//! These tests validate that `RuntimeOptions::limits` is respected at both the
//! Tier-1 (client-side) and Tier-2 (`validate_limits()`) enforcement points.
//!
//! Defaults remain permissive (`Limits::permissive()`), so every test that
//! exercises a tightened limit must create its own runtime/client with
//! explicit `RuntimeOptions::limits` overrides.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

#[path = "../common/mod.rs"]
mod common;

use duroxide::runtime::limits::{LimitViolation, Limits, NameKind, measured_len};
use duroxide::runtime::{RuntimeOptions, registry::ActivityRegistry};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use std::time::Duration;

// ============================================================================
// Unit-level tests (no runtime needed)
// ============================================================================

#[test]
fn measured_len_byte_length_not_char_length() {
    // Each crab emoji is 4 UTF-8 bytes
    let emoji_64 = "🦀".repeat(64); // 256 bytes, 64 chars
    assert_eq!(measured_len(&emoji_64), 256);
    assert_eq!(emoji_64.chars().count(), 64);

    let emoji_65 = "🦀".repeat(65); // 260 bytes, 65 chars
    assert_eq!(measured_len(&emoji_65), 260);
}

#[test]
fn limits_permissive_allows_everything() {
    let l = Limits::permissive();
    assert_eq!(l.max_name_bytes, usize::MAX);
    assert_eq!(l.max_identifier_bytes, usize::MAX);
}

#[test]
fn limits_recommended_has_256_byte_values() {
    let l = Limits::recommended();
    assert_eq!(l.max_name_bytes, 256);
    assert_eq!(l.max_identifier_bytes, 256);
}

#[test]
fn limits_default_is_permissive() {
    // Phases 1–6: default must be permissive to avoid regressions.
    let l = Limits::default();
    assert_eq!(l.max_name_bytes, usize::MAX);
}

#[test]
fn runtime_options_includes_limits_field() {
    // Smoke-test that RuntimeOptions::limits is accessible and defaults to permissive.
    let opts = RuntimeOptions::default();
    assert_eq!(opts.limits.max_name_bytes, usize::MAX);
}

// ============================================================================
// LimitViolation encode/decode round-trip
// ============================================================================

#[test]
fn limit_violation_roundtrip_activity_name() {
    let v = LimitViolation::NameTooLong {
        kind: NameKind::ActivityName,
        name: "my_activity".to_string(),
        size: 512,
        limit: 256,
    };
    let encoded = v.encode_into_message();
    assert!(encoded.starts_with(LimitViolation::MESSAGE_PREFIX));
    let decoded = LimitViolation::parse_from_message(&encoded).expect("should round-trip");
    assert_eq!(decoded, v);
}

#[test]
fn limit_violation_roundtrip_instance_id() {
    let v = LimitViolation::NameTooLong {
        kind: NameKind::InstanceId,
        name: "very-long-instance-id".to_string(),
        size: 300,
        limit: 256,
    };
    let encoded = v.encode_into_message();
    let decoded = LimitViolation::parse_from_message(&encoded).expect("should round-trip");
    assert_eq!(decoded, v);
}

#[test]
fn limit_violation_parse_ignores_plain_strings() {
    assert!(LimitViolation::parse_from_message("plain error").is_none());
    assert!(LimitViolation::parse_from_message("").is_none());
    // Wrong prefix
    assert!(LimitViolation::parse_from_message("__other_prefix:{\"v\":\"NameTooLong\"}").is_none());
}

#[test]
fn limit_violation_display_message_is_human_readable() {
    let v = LimitViolation::NameTooLong {
        kind: NameKind::OrchestrationName,
        name: "short".to_string(),
        size: 300,
        limit: 256,
    };
    let msg = v.display_message();
    assert!(msg.contains("orchestration_name"));
    assert!(msg.contains("300"));
    assert!(msg.contains("256"));
}

// ============================================================================
// Tier-1 client checks (with tightened limits)
// ============================================================================

#[tokio::test]
async fn client_rejects_oversized_instance_id() {
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let client = Client::new_with_limits(store, Limits::recommended());

    let oversized_instance = "i".repeat(Limits::recommended().max_identifier_bytes + 1);
    let result = client
        .start_orchestration(&oversized_instance, "MyOrch", "input")
        .await;

    assert!(result.is_err(), "should reject oversized instance ID");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("instance_id"),
        "error should mention instance_id: {err}"
    );
}

#[tokio::test]
async fn client_accepts_max_size_instance_id() {
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let client = Client::new_with_limits(store, Limits::recommended());

    let max_instance = "i".repeat(Limits::recommended().max_identifier_bytes);
    // Should not fail with InvalidInput (may fail for other reasons like no running runtime)
    let result = client
        .start_orchestration(&max_instance, "MyOrch", "input")
        .await;

    // The limit check itself should pass (no InvalidInput error)
    match result {
        Err(duroxide::ClientError::InvalidInput { .. }) => {
            panic!("should not reject max-size instance ID: {result:?}")
        }
        _ => {} // Provider or other error is fine — we only care about no limit rejection
    }
}

#[tokio::test]
async fn client_rejects_oversized_orchestration_name() {
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let client = Client::new_with_limits(store, Limits::recommended());

    let oversized_name = "n".repeat(Limits::recommended().max_name_bytes + 1);
    let result = client
        .start_orchestration("my-instance", &oversized_name, "input")
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("orchestration_name"));
}

#[tokio::test]
async fn client_rejects_oversized_event_name() {
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let client = Client::new_with_limits(store, Limits::recommended());

    let oversized_name = "e".repeat(Limits::recommended().max_name_bytes + 1);
    let result = client
        .raise_event("my-instance", &oversized_name, "data")
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("event_name"));
}

#[tokio::test]
async fn client_rejects_oversized_queue_name() {
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let client = Client::new_with_limits(store, Limits::recommended());

    let oversized_name = "q".repeat(Limits::recommended().max_name_bytes + 1);
    let result = client
        .enqueue_event("my-instance", &oversized_name, "data")
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("queue_name"));
}

#[tokio::test]
async fn client_rejects_oversized_pinned_version() {
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let client = Client::new_with_limits(store, Limits::recommended());

    let oversized_version = "v".repeat(Limits::recommended().max_name_bytes + 1);
    let result = client
        .start_orchestration_versioned("my-instance", "MyOrch", &oversized_version, "input")
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("pinned_version"));
}

#[tokio::test]
async fn client_with_default_limits_allows_long_names() {
    // Default (permissive) limits must not reject anything.
    let (store, _dir) = common::create_sqlite_store_disk().await;
    let client = Client::new(store); // default = permissive

    let very_long = "x".repeat(1024);
    let result = client
        .start_orchestration(&very_long, &very_long, "input")
        .await;

    // Should not get InvalidInput — may get a provider error but not a limit rejection.
    match result {
        Err(duroxide::ClientError::InvalidInput { .. }) => {
            panic!("permissive client should not reject long names")
        }
        _ => {}
    }
}

// ============================================================================
// Tier-2 validate_limits() checks (requires running runtime)
// ============================================================================

#[tokio::test]
async fn tier2_oversized_activity_name_fails_orchestration() {
    let (store, _dir) = common::create_sqlite_store_disk().await;

    let oversized_name = "a".repeat(Limits::recommended().max_name_bytes + 1);
    let orch_name = oversized_name.clone();

    let activity_registry = ActivityRegistry::builder()
        .register("dummy", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();

    let orchestration = move |ctx: OrchestrationContext, _input: String| {
        let name = orch_name.clone();
        async move {
            // schedule_activity with an oversized name; limit check fires in validate_limits()
            let result = ctx.schedule_activity(&name, "").await;
            Ok(result.unwrap_or_else(|e| e))
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder()
        .register("TestOrch", orchestration)
        .build();

    let rt = duroxide::runtime::Runtime::start_with_options(
        store.clone(),
        activity_registry,
        orchestration_registry,
        RuntimeOptions {
            orchestration_concurrency: 1,
            worker_concurrency: 1,
            limits: Limits::recommended(),
            ..RuntimeOptions::default()
        },
    )
    .await;

    // The client uses permissive limits (default) here intentionally — we want to test
    // that Tier-2 (runtime validate_limits) catches the oversized activity name even when
    // Tier-1 (client-side) would have allowed it through. The orchestration name itself
    // ("TestOrch", 8 bytes) is within limits, so start_orchestration succeeds.
    let client = Client::new(store.clone());
    client.start_orchestration("inst-act-name", "TestOrch", "").await.unwrap();

    // Wait for orchestration to finish (it should fail due to the limit)
    let result = client
        .wait_for_orchestration("inst-act-name", Duration::from_secs(5))
        .await;

    rt.shutdown(None).await;

    match result {
        Ok(duroxide::OrchestrationStatus::Failed { details, .. }) => {
            let msg = details.display_message();
            assert!(
                msg.contains("activity_name") || msg.contains("limit exceeded"),
                "failure message should mention activity_name or limit exceeded: {msg}"
            );
        }
        other => panic!("expected Failed, got: {other:?}"),
    }
}

#[tokio::test]
async fn tier2_oversized_sub_orchestration_name_fails_orchestration() {
    let (store, _dir) = common::create_sqlite_store_disk().await;

    let oversized_name = "s".repeat(Limits::recommended().max_name_bytes + 1);
    let orch_name = oversized_name.clone();

    let activity_registry = ActivityRegistry::builder().build();

    let parent_fn = move |ctx: OrchestrationContext, _input: String| {
        let name = orch_name.clone();
        async move {
            // This tries to schedule a sub-orch with an oversized name
            let result = ctx.schedule_sub_orchestration(&name, "").await;
            Ok(result.unwrap_or_else(|e| e))
        }
    };

    // We can't register a handler with oversized name (registry check would panic),
    // so we just attempt to schedule a non-existent oversized-name sub-orchestration
    // and verify that validate_limits() fires before the "not found" path.
    let orchestration_registry = OrchestrationRegistry::builder()
        .register("ParentOrch", parent_fn)
        .build();

    let rt = duroxide::runtime::Runtime::start_with_options(
        store.clone(),
        activity_registry,
        orchestration_registry,
        RuntimeOptions {
            orchestration_concurrency: 1,
            worker_concurrency: 1,
            limits: Limits::recommended(),
            ..RuntimeOptions::default()
        },
    )
    .await;

    // The client uses permissive limits (default) intentionally — we test that Tier-2
    // (runtime validate_limits) catches the oversized sub-orchestration name even when
    // Tier-1 would allow it through. The orchestration name "ParentOrch" is within limits.
    let client = Client::new(store.clone());
    client
        .start_orchestration("inst-sub-name", "ParentOrch", "")
        .await
        .unwrap();

    let result = client
        .wait_for_orchestration("inst-sub-name", Duration::from_secs(5))
        .await;

    rt.shutdown(None).await;

    match result {
        Ok(duroxide::OrchestrationStatus::Failed { details, .. }) => {
            let msg = details.display_message();
            assert!(
                msg.contains("sub_orchestration_name") || msg.contains("limit exceeded"),
                "failure message should mention sub_orchestration_name or limit exceeded: {msg}"
            );
        }
        other => panic!("expected Failed, got: {other:?}"),
    }
}

// ============================================================================
// Registry-time name limit checks
// ============================================================================

#[test]
#[should_panic(expected = "handler registration rejected")]
fn registry_panics_on_oversized_orchestration_name() {
    let oversized = "o".repeat(Limits::recommended().max_name_bytes + 1);
    let _reg = OrchestrationRegistry::builder()
        .register(&oversized, |_ctx: OrchestrationContext, _: String| async move { Ok("done".to_string()) })
        .build();
}

#[test]
#[should_panic(expected = "handler registration rejected")]
fn registry_panics_on_oversized_activity_name() {
    let oversized = "a".repeat(Limits::recommended().max_name_bytes + 1);
    let _reg = ActivityRegistry::builder()
        .register(&oversized, |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
}

#[test]
fn registry_accepts_max_size_name() {
    let max = "m".repeat(Limits::recommended().max_name_bytes);

    // Should not panic for exactly 256 bytes
    let _reg = OrchestrationRegistry::builder()
        .register(&max, |_ctx: OrchestrationContext, _: String| async move { Ok("done".to_string()) })
        .build();

    let _reg2 = ActivityRegistry::builder()
        .register(&max, |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
}

#[test]
fn registry_accepts_multibyte_utf8_name_at_limit() {
    // 64 emoji × 4 bytes each = 256 bytes = exactly recommended max_name_bytes
    let emoji_name = "🦀".repeat(64);
    assert_eq!(measured_len(&emoji_name), 256);

    // Should not panic
    let _reg = ActivityRegistry::builder()
        .register(&emoji_name, |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
}

#[test]
#[should_panic(expected = "handler registration rejected")]
fn registry_rejects_multibyte_utf8_name_over_limit() {
    // 65 emoji × 4 bytes = 260 bytes > recommended max_name_bytes (256)
    let emoji_name = "🦀".repeat(65);
    assert_eq!(measured_len(&emoji_name), 260);

    let _reg = ActivityRegistry::builder()
        .register(&emoji_name, |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
}

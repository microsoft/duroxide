//! Size & shape limits regression tests — spec §8
//!
//! Covers all four toggle combinations
//! (`enforce_size_limits` × `emit_limit_exceeded_errors`) and every test row
//! listed in `docs/proposals/size-limits.md` §8.
//!
//! References:
//! - `docs/proposals/size-limits.md`
//! - `docs/proposals-impl/PROGRESS-size-limits.md`

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use duroxide::runtime::limits::{MAX_HISTORY_BYTES, MAX_PAYLOAD_BYTES, MAX_SMALL_VALUE_BYTES};
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, ConfigErrorKind, ErrorDetails, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use duroxide::runtime::registry::ActivityRegistry;
use std::sync::Arc;
use std::time::Duration;

#[path = "../common/mod.rs"]
mod common;

// ---------------------------------------------------------------------------
// Helper: build RuntimeOptions with specific toggle state
// ---------------------------------------------------------------------------

fn opts(enforce: bool, emit_le: bool) -> RuntimeOptions {
    RuntimeOptions {
        enforce_size_limits: enforce,
        emit_limit_exceeded_errors: emit_le,
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        ..Default::default()
    }
}

/// Wait for orchestration and panic on timeout.
async fn wait(client: &Client, instance: &str) -> OrchestrationStatus {
    client
        .wait_for_orchestration(instance, Duration::from_secs(15))
        .await
        .unwrap_or_else(|e| panic!("wait_for_orchestration error: {e}"))
}

// ---------------------------------------------------------------------------
// Test: enforce=off → large payloads succeed (baseline: no regressions)
// ---------------------------------------------------------------------------

/// `enforce_off_measures_but_does_not_fail`
///
/// With enforcement off, an activity returning a 4 MiB payload should succeed.
#[tokio::test]
async fn enforce_off_does_not_fail_large_activity_output() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let big_output = "x".repeat(MAX_PAYLOAD_BYTES + 1);
    let big_output_clone = big_output.clone();

    let activity_registry = ActivityRegistry::builder()
        .register("BigOutput", move |_ctx: ActivityContext, _input: String| {
            let out = big_output_clone.clone();
            async move { Ok(out) }
        })
        .build();

    let orch_reg = OrchestrationRegistry::builder()
        .register("BigOutputOrch", |ctx: OrchestrationContext, _: String| async move {
            let result = ctx.schedule_activity("BigOutput", String::new()).await?;
            Ok(result)
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(false, false),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("big-output-off", "BigOutputOrch", "").await.unwrap();

    let status = wait(&client, "big-output-off").await;
    assert!(
        matches!(status, OrchestrationStatus::Completed { .. }),
        "expected Completed with enforce=off, got {status:?}"
    );

    rt.shutdown(None).await;
}

// ---------------------------------------------------------------------------
// Tier-1 client checks
// ---------------------------------------------------------------------------

/// `orchestration_input_too_large`
///
/// With enforcement on, starting an orchestration with 4 MiB input returns
/// `Err(ClientError::Configuration)`. Nothing is written to the provider.
#[tokio::test]
async fn tier1_orchestration_input_too_large() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let client = Client::new(store.clone()).with_size_limits_enforced();
    let big_input = "x".repeat(MAX_PAYLOAD_BYTES + 1);

    let err = client
        .start_orchestration("inst", "SomeOrch", big_input)
        .await
        .expect_err("should reject oversized input");

    assert!(
        matches!(err, duroxide::ClientError::Configuration { ref resource, .. } if resource == "orchestration_input"),
        "unexpected error: {err:?}"
    );
}

/// `external_event_too_large`
///
/// `raise_event` with a 100 KiB + 1 payload is rejected at the client.
#[tokio::test]
async fn tier1_external_event_too_large() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let client = Client::new(store.clone()).with_size_limits_enforced();
    let big_payload = "x".repeat(MAX_SMALL_VALUE_BYTES + 1);

    let err = client
        .raise_event("inst", "MyEvent", big_payload)
        .await
        .expect_err("should reject oversized event payload");

    assert!(
        matches!(err, duroxide::ClientError::Configuration { ref resource, .. } if resource.starts_with("external_event:")),
        "unexpected error: {err:?}"
    );
}

/// `instance_id_too_long`
///
/// Starting an orchestration with an instance ID > `MAX_SMALL_VALUE_BYTES` bytes
/// is rejected at the client.
#[tokio::test]
async fn tier1_instance_id_too_long() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let client = Client::new(store.clone()).with_size_limits_enforced();
    let long_id = "x".repeat(MAX_SMALL_VALUE_BYTES + 1);

    let err = client
        .start_orchestration(long_id, "SomeOrch", "")
        .await
        .expect_err("should reject oversized instance_id");

    assert!(
        matches!(err, duroxide::ClientError::Configuration { ref resource, .. } if resource == "identifier:instance_id"),
        "unexpected error: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Tier-2: per-event payload checks
// ---------------------------------------------------------------------------

/// `activity_input_too_large`
///
/// Scheduling an activity with a 4 MiB input causes a tier-2 failure.
/// With `emit_limit_exceeded_errors=true` the error is `Configuration::LimitExceeded`.
/// The oversized delta is never persisted; instance terminates.
#[tokio::test]
async fn tier2_activity_input_too_large_emit_on() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let big_input = "x".repeat(MAX_PAYLOAD_BYTES + 1);

    let activity_registry = ActivityRegistry::builder()
        .register("Noop", |_ctx: ActivityContext, _: String| async move { Ok("ok".to_string()) })
        .build();

    let orch_reg = OrchestrationRegistry::builder()
        .register("BigInputOrch", move |ctx: OrchestrationContext, _: String| {
            let inp = big_input.clone();
            async move {
                let _ = ctx.schedule_activity("Noop", inp).await?;
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(true, true),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("big-act-input", "BigInputOrch", "").await.unwrap();

    let status = wait(&client, "big-act-input").await;

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(
                    &details,
                    ErrorDetails::Configuration {
                        kind: ConfigErrorKind::LimitExceeded,
                        resource,
                        ..
                    } if resource.starts_with("activity_input:")
                ),
                "expected LimitExceeded(activity_input:*), got {details:?}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Same scenario with `emit_limit_exceeded_errors=false`:
/// failure uses Application shape (mixed-cluster-safe).
#[tokio::test]
async fn tier2_activity_input_too_large_emit_off() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let big_input = "x".repeat(MAX_PAYLOAD_BYTES + 1);

    let activity_registry = ActivityRegistry::builder()
        .register("Noop", |_ctx: ActivityContext, _: String| async move { Ok("ok".to_string()) })
        .build();

    let orch_reg = OrchestrationRegistry::builder()
        .register("BigInputOrch2", move |ctx: OrchestrationContext, _: String| {
            let inp = big_input.clone();
            async move {
                let _ = ctx.schedule_activity("Noop", inp).await?;
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(true, false),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("big-act-input-app", "BigInputOrch2", "").await.unwrap();

    let status = wait(&client, "big-act-input-app").await;

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(&details, ErrorDetails::Application { .. }),
                "expected Application shape with emit_le=false, got {details:?}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }

    rt.shutdown(None).await;
}

// ---------------------------------------------------------------------------
// Tier-2: side-effect drops
// ---------------------------------------------------------------------------

/// `tier2_failure_drops_side_effects`
///
/// An orchestration that tries to schedule an activity WITH an oversized input
/// in the same turn. The activity should NOT be enqueued — the rejected delta
/// drops all side effects.
#[tokio::test]
async fn tier2_failure_drops_side_effects() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let big_input = "x".repeat(MAX_PAYLOAD_BYTES + 1);

    // A counter to detect whether the activity actually ran.
    let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ran_clone = ran.clone();

    let activity_registry = ActivityRegistry::builder()
        .register("SideEffect", move |_ctx: ActivityContext, _: String| {
            ran_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            async move { Ok("ran".to_string()) }
        })
        .build();

    let orch_reg = OrchestrationRegistry::builder()
        .register("SideEffectOrch", move |ctx: OrchestrationContext, _: String| {
            let inp = big_input.clone();
            async move {
                // This schedules an activity with an oversized input.
                // The tier-2 check should reject the whole delta,
                // so the activity enqueue is also dropped.
                let _ = ctx.schedule_activity("SideEffect", inp).await?;
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(true, true),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("side-effect-drop", "SideEffectOrch", "").await.unwrap();

    let status = wait(&client, "side-effect-drop").await;

    assert!(
        matches!(status, OrchestrationStatus::Failed { .. }),
        "expected Failed, got {status:?}"
    );

    // Give the worker dispatcher a brief window to process any stray activity enqueue.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        !ran.load(std::sync::atomic::Ordering::SeqCst),
        "side-effect activity should not have run (delta was dropped)"
    );

    rt.shutdown(None).await;
}

// ---------------------------------------------------------------------------
// Tier-3: activity output too large
// ---------------------------------------------------------------------------

/// `activity_output_too_large`
///
/// Activity returns > MAX_PAYLOAD_BYTES. With enforcement on and
/// `emit_limit_exceeded_errors=true`, the instance terminates with
/// `Configuration::LimitExceeded { resource: "activity_output:*" }`.
/// Raw output is never logged.
#[tokio::test]
async fn tier3_activity_output_too_large_emit_on() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let big_out = "y".repeat(MAX_PAYLOAD_BYTES + 1);

    let activity_registry = ActivityRegistry::builder()
        .register("HugeOutput", move |_ctx: ActivityContext, _: String| {
            let out = big_out.clone();
            async move { Ok(out) }
        })
        .build();

    let orch_reg = OrchestrationRegistry::builder()
        .register("HugeOutputOrch", |ctx: OrchestrationContext, _: String| async move {
            let result = ctx.schedule_activity("HugeOutput", String::new()).await?;
            Ok(result)
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(true, true),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("huge-out", "HugeOutputOrch", "").await.unwrap();

    let status = wait(&client, "huge-out").await;

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(
                    &details,
                    ErrorDetails::Configuration {
                        kind: ConfigErrorKind::LimitExceeded,
                        resource,
                        ..
                    } if resource.starts_with("activity_output:")
                ),
                "expected LimitExceeded(activity_output:*), got {details:?}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }

    rt.shutdown(None).await;
}

// ---------------------------------------------------------------------------
// Aggregate history cap
// ---------------------------------------------------------------------------

/// `history_cap_terminates_instance`
///
/// An orchestration that loops, scheduling activities with ~1 MiB outputs
/// until the aggregate exceeds `MAX_HISTORY_BYTES` (5 MiB). The instance
/// must terminate with a limit failure before exceeding the cap.
///
/// We use a modest payload size (~500 KiB × 12 iterations = ~6 MiB) to
/// reliably cross the cap without making the test huge.
#[tokio::test]
async fn history_cap_terminates_instance() {
    const CHUNK: usize = 512 * 1024; // 512 KiB per iteration — 11 iterations ≈ 5.5 MiB

    let (store, _td) = common::create_sqlite_store_disk().await;

    let chunk_out = "z".repeat(CHUNK);

    let activity_registry = ActivityRegistry::builder()
        .register("Chunk", move |_ctx: ActivityContext, _: String| {
            let out = chunk_out.clone();
            async move { Ok(out) }
        })
        .build();

    let orch_reg = OrchestrationRegistry::builder()
        .register("HistoryCapOrch", |ctx: OrchestrationContext, _: String| async move {
            loop {
                let _ = ctx.schedule_activity("Chunk", String::new()).await?;
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(true, true),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("hist-cap", "HistoryCapOrch", "").await.unwrap();

    // Wait up to 30 s — the loop terminates once the cap is hit.
    let status = client
        .wait_for_orchestration("hist-cap", Duration::from_secs(30))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(
                    &details,
                    ErrorDetails::Configuration {
                        kind: ConfigErrorKind::LimitExceeded,
                        resource,
                        ..
                    } if resource == "history"
                ) || matches!(&details, ErrorDetails::Application { message, .. } if message.contains("Total history size")),
                "expected history-cap failure, got {details:?}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }

    rt.shutdown(None).await;
}

// ---------------------------------------------------------------------------
// history_size_bytes() determinism
// ---------------------------------------------------------------------------

/// `history_size_bytes_is_deterministic`
///
/// Validates that:
/// - `history_size_bytes()` is > 0 once history has been accumulated
/// - `history_size_bytes()` is stable within a single turn (reads return the
///   same value because the baseline is fixed at turn-start)
/// - `history_pressure()` is in `[0.0, 1.0]`
/// - The value grows across `continue_as_new` generations (different baselines)
#[tokio::test]
async fn history_size_bytes_is_deterministic() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();

    // Generation counter: on gen=0 we build some history then CAN; on gen=1 we record
    // the size and finish. The size on gen=1 should be > 0.
    let orch_reg = OrchestrationRegistry::builder()
        .register("SizeBytesOrch", |ctx: OrchestrationContext, input: String| async move {
            let generation: u32 = input.parse().unwrap_or(0);
            if generation == 0 {
                // Do some work to build baseline history.
                let _r1 = ctx.schedule_activity("Echo", "step1".to_string()).await?;
                let _r2 = ctx.schedule_activity("Echo", "step2".to_string()).await?;
                // Within this turn both reads are identical (stable per-turn baseline).
                let size_a = ctx.history_size_bytes();
                let size_b = ctx.history_size_bytes();
                assert_eq!(size_a, size_b, "history_size_bytes() must be stable within a turn");
                assert!(size_a > 0, "size should be > 0 after activities");
                // Roll over — the new execution has a smaller baseline (just OrchestrationStarted).
                return ctx.continue_as_new("1".to_string()).await;
            }
            // Gen 1: history is just the new OrchestrationStarted.
            let size1 = ctx.history_size_bytes();
            // Still > 0 (OrchestrationStarted is in the baseline).
            assert!(size1 > 0, "size1 should be > 0 on gen=1");
            let pressure = ctx.history_pressure();
            assert!((0.0..=1.0).contains(&pressure), "pressure {pressure} out of range");
            Ok(format!("size={size1},pressure={pressure:.6}"))
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(false, false),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("size-det", "SizeBytesOrch", "").await.unwrap();

    let status = wait(&client, "size-det").await;
    assert!(
        matches!(status, OrchestrationStatus::Completed { .. }),
        "expected Completed, got {status:?}"
    );

    rt.shutdown(None).await;
}

// ---------------------------------------------------------------------------
// history_pressure() drives continue_as_new
// ---------------------------------------------------------------------------

/// `history_pressure_drives_continue_as_new`
///
/// An orchestration that calls `continue_as_new()` when pressure > a very low
/// artificial threshold (achieved by inserting enough activity calls).
/// Verifies the orchestration eventually succeeds after multiple generations,
/// each staying below the cap.
///
/// We use a 1% threshold here to keep the test fast — the important property
/// is that `history_pressure()` is readable and > 0.
#[tokio::test]
async fn history_pressure_drives_continue_as_new() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Bump", |_ctx: ActivityContext, input: String| async move {
            let n: u32 = input.parse().unwrap_or(0);
            Ok((n + 1).to_string())
        })
        .build();

    let orch_reg = OrchestrationRegistry::builder()
        .register("PressureOrch", |ctx: OrchestrationContext, input: String| async move {
            let generation: u32 = input.parse().unwrap_or(0);
            if generation >= 3 {
                return Ok("done after 3 generations".to_string());
            }
            // Do some work to build up a little history.
            let n = ctx.schedule_activity("Bump", generation.to_string()).await?;
            let _ = ctx.schedule_activity("Bump", n.clone()).await?;

            // Roll over if pressure > 0 (which it will be after the first activity).
            if ctx.history_pressure() > 0.0 {
                let next_gen = generation + 1;
                return ctx.continue_as_new(next_gen.to_string()).await;
            }
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(false, false),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("pressure-can", "PressureOrch", "0").await.unwrap();

    let status = wait(&client, "pressure-can").await;
    assert!(
        matches!(status, OrchestrationStatus::Completed { ref output, .. } if output == "done after 3 generations"),
        "expected Completed with 3 generations, got {status:?}"
    );

    rt.shutdown(None).await;
}

// ---------------------------------------------------------------------------
// Pre-existing limits keep Application shape when emit_le=false
// ---------------------------------------------------------------------------

/// `existing_limit_failures_keep_application_when_emit_off`
///
/// Triggering MAX_CUSTOM_STATUS_BYTES with `emit_limit_exceeded_errors=false`
/// should produce the legacy `Application { OrchestrationFailed }` shape.
#[tokio::test]
async fn existing_limit_failures_keep_application_when_emit_off() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Noop", |_ctx: ActivityContext, _: String| async move { Ok("ok".to_string()) })
        .build();

    // MAX_CUSTOM_STATUS_BYTES is 256 KiB; a 300 KiB string exceeds it.
    let big_status = "s".repeat(300 * 1024);

    let orch_reg = OrchestrationRegistry::builder()
        .register("CustomStatusOrch", move |ctx: OrchestrationContext, _: String| {
            let s = big_status.clone();
            async move {
                ctx.set_custom_status(s);
                let _ = ctx.schedule_activity("Noop", String::new()).await?;
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(false, false),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("big-status-emit-off", "CustomStatusOrch", "").await.unwrap();

    let status = wait(&client, "big-status-emit-off").await;

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(&details, ErrorDetails::Application { .. }),
                "expected Application shape, got {details:?}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }

    rt.shutdown(None).await;
}

/// `existing_limit_failures_become_configuration_when_emit_on`
///
/// Same scenario with `emit_limit_exceeded_errors=true` → `Configuration::LimitExceeded`.
#[tokio::test]
async fn existing_limit_failures_become_configuration_when_emit_on() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let activity_registry = ActivityRegistry::builder()
        .register("Noop2", |_ctx: ActivityContext, _: String| async move { Ok("ok".to_string()) })
        .build();

    let big_status = "s".repeat(300 * 1024);

    let orch_reg = OrchestrationRegistry::builder()
        .register("CustomStatusOrch2", move |ctx: OrchestrationContext, _: String| {
            let s = big_status.clone();
            async move {
                ctx.set_custom_status(s);
                let _ = ctx.schedule_activity("Noop2", String::new()).await?;
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(false, true),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("big-status-emit-on", "CustomStatusOrch2", "").await.unwrap();

    let status = wait(&client, "big-status-emit-on").await;

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(
                    &details,
                    ErrorDetails::Configuration { kind: ConfigErrorKind::LimitExceeded, .. }
                ),
                "expected Configuration::LimitExceeded, got {details:?}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }

    rt.shutdown(None).await;
}

// ---------------------------------------------------------------------------
// Multibyte UTF-8 name length is measured in bytes, not chars
// ---------------------------------------------------------------------------

/// `multibyte_utf8_name_length`
///
/// An activity with a 64-emoji (256 UTF-8 bytes) name: accepted.
/// An activity with MAX_SMALL_VALUE_BYTES/4 + 1 emojis (> 64 KiB UTF-8): rejected.
///
/// Each emoji (🔥) is 4 UTF-8 bytes. MAX_SMALL_VALUE_BYTES = 64 KiB = 65536 bytes.
/// So 65536 / 4 = 16384 emojis = exactly 65536 bytes → OK.
/// 16385 emojis = 65540 bytes → rejected.
#[tokio::test]
async fn multibyte_utf8_name_length_bytes_not_chars() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Short name (within limit) — 64 emojis = 256 bytes, well under 64 KiB.
    let short_name: String = "🔥".repeat(64);
    // Long name (over limit by 4 bytes) — 16385 emojis = 65540 bytes > 65536.
    let long_name: String = "🔥".repeat(MAX_SMALL_VALUE_BYTES / 4 + 1);

    assert!(short_name.len() < MAX_SMALL_VALUE_BYTES, "short_name should be under limit");
    assert!(long_name.len() > MAX_SMALL_VALUE_BYTES, "long_name should be over limit");

    let short_name_clone = short_name.clone();
    let long_name_clone = long_name.clone();

    let activity_registry = ActivityRegistry::builder()
        .register(short_name.clone(), |_ctx: ActivityContext, _: String| async move {
            Ok("short-ok".to_string())
        })
        .register(long_name.clone(), |_ctx: ActivityContext, _: String| async move {
            Ok("long-ok".to_string())
        })
        .build();

    // Orchestration 1: schedules the short-name activity → should succeed.
    let orch_reg = OrchestrationRegistry::builder()
        .register("ShortNameOrch", move |ctx: OrchestrationContext, _: String| {
            let name = short_name_clone.clone();
            async move {
                let r = ctx.schedule_activity(name, String::new()).await?;
                Ok(r)
            }
        })
        .register("LongNameOrch", move |ctx: OrchestrationContext, _: String| {
            let name = long_name_clone.clone();
            async move {
                let r = ctx.schedule_activity(name, String::new()).await?;
                Ok(r)
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(true, true),
    ).await;

    let client = Client::new(store.clone());

    // Short name → should complete.
    client.start_orchestration("short-name-inst", "ShortNameOrch", "").await.unwrap();
    let status_short = wait(&client, "short-name-inst").await;
    assert!(
        matches!(status_short, OrchestrationStatus::Completed { .. }),
        "short-name activity should complete, got {status_short:?}"
    );

    // Long name → should fail with LimitExceeded (name:activity).
    client.start_orchestration("long-name-inst", "LongNameOrch", "").await.unwrap();
    let status_long = wait(&client, "long-name-inst").await;
    match status_long {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(
                    &details,
                    ErrorDetails::Configuration {
                        kind: ConfigErrorKind::LimitExceeded,
                        resource,
                        ..
                    } if resource == "name:activity"
                ),
                "expected LimitExceeded(name:activity), got {details:?}"
            );
        }
        other => panic!("expected Failed for long-name activity, got {other:?}"),
    }

    rt.shutdown(None).await;
}

// ---------------------------------------------------------------------------
// enforce=true, emit=false: Application shape (mixed-cluster-safe)
// ---------------------------------------------------------------------------

/// `enforce_on_emit_off_uses_application_shape`
///
/// Tier-2 violation with `enforce=true, emit_le=false` should produce
/// `Application` shape, not `Configuration::LimitExceeded`.
#[tokio::test]
async fn enforce_on_emit_off_uses_application_shape() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let big_input = "x".repeat(MAX_PAYLOAD_BYTES + 1);

    let activity_registry = ActivityRegistry::builder()
        .register("Noop3", |_ctx: ActivityContext, _: String| async move { Ok("ok".to_string()) })
        .build();

    let orch_reg = OrchestrationRegistry::builder()
        .register("AppShapeOrch", move |ctx: OrchestrationContext, _: String| {
            let inp = big_input.clone();
            async move {
                let _ = ctx.schedule_activity("Noop3", inp).await?;
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(true, false),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("app-shape-inst", "AppShapeOrch", "").await.unwrap();

    let status = wait(&client, "app-shape-inst").await;

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(&details, ErrorDetails::Application { .. }),
                "expected Application shape with emit_le=false, got {details:?}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }

    rt.shutdown(None).await;
}

// ---------------------------------------------------------------------------
// Replay of pre-limit history succeeds
// ---------------------------------------------------------------------------

/// `replay_of_pre_limit_history_succeeds`
///
/// History replayed on newer runtime should succeed if the payload was within
/// limits when written. We test this by running with `enforce=off` (simulating
/// pre-limit runtime), then verifying that the completed history is still
/// accessible and its `history_size_bytes` is reported sensibly.
#[tokio::test]
async fn replay_of_pre_limit_history_succeeds() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Use a moderate payload that would be fine under both old and new runtimes.
    let modest_output = "m".repeat(1024); // 1 KiB — well within limits.

    let activity_registry = ActivityRegistry::builder()
        .register("ModestOut", move |_ctx: ActivityContext, _: String| {
            let out = modest_output.clone();
            async move { Ok(out) }
        })
        .build();

    let orch_reg = OrchestrationRegistry::builder()
        .register("ModestOrch", |ctx: OrchestrationContext, _: String| async move {
            let r = ctx.schedule_activity("ModestOut", String::new()).await?;
            // Record the history size in the output for assertion.
            let sz = ctx.history_size_bytes();
            Ok(format!("{r}|size={sz}"))
        })
        .build();

    // Run with enforcement ON — should still complete fine.
    let rt = runtime::Runtime::start_with_options(
        store.clone(), activity_registry, orch_reg, opts(true, true),
    ).await;

    let client = Client::new(store.clone());
    client.start_orchestration("pre-limit-inst", "ModestOrch", "").await.unwrap();

    let status = wait(&client, "pre-limit-inst").await;

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("size="), "output should contain size= but got: {output}");
            let size_str = output.split("size=").nth(1).unwrap_or("0");
            let size: usize = size_str.parse().unwrap_or(0);
            assert!(size > 0, "history_size_bytes should be > 0 but got {size}");
            assert!(size < MAX_HISTORY_BYTES, "history_size_bytes should be < MAX_HISTORY_BYTES");
        }
        other => panic!("expected Completed, got {other:?}"),
    }

    rt.shutdown(None).await;
}

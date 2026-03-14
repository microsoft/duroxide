// KV store e2e tests
//
// Validates set_value(), get_value(), clear_value(), clear_all_values(),
// get_value_from_instance(), and Client::get_value() across various scenarios.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

mod common;

use duroxide::runtime::{self, OrchestrationStatus, registry::ActivityRegistry};
use duroxide::{ActivityContext, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Basic set / get
// =============================================================================

/// Orchestration sets a KV value before completing.
/// Verify the value is readable via Client::get_value after completion.
#[tokio::test]
async fn kv_set_value_visible_via_client() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("SetKV", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("progress", "50%");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-set-1", "SetKV", "").await.unwrap();

    let status = client
        .wait_for_orchestration("kv-set-1", Duration::from_secs(5))
        .await
        .unwrap();

    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    let val = client.get_kv_value("kv-set-1", "progress").await.unwrap();
    assert_eq!(val, Some("50%".to_string()));

    rt.shutdown(None).await;
}

// =============================================================================
// Get value returns None for missing key
// =============================================================================

#[tokio::test]
async fn kv_get_missing_key_returns_none() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("NoKV", |_ctx: OrchestrationContext, _input: String| async move {
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-miss", "NoKV", "").await.unwrap();
    client
        .wait_for_orchestration("kv-miss", Duration::from_secs(5))
        .await
        .unwrap();

    let val = client.get_kv_value("kv-miss", "nope").await.unwrap();
    assert_eq!(val, None);

    rt.shutdown(None).await;
}

// =============================================================================
// Multiple values set in same turn
// =============================================================================

#[tokio::test]
async fn kv_set_multiple_values_same_turn() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("MultiKV", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("a", "1");
            ctx.set_kv_value("b", "2");
            ctx.set_kv_value("c", "3");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-multi", "MultiKV", "").await.unwrap();
    client
        .wait_for_orchestration("kv-multi", Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(
        client.get_kv_value("kv-multi", "a").await.unwrap(),
        Some("1".to_string())
    );
    assert_eq!(
        client.get_kv_value("kv-multi", "b").await.unwrap(),
        Some("2".to_string())
    );
    assert_eq!(
        client.get_kv_value("kv-multi", "c").await.unwrap(),
        Some("3".to_string())
    );

    rt.shutdown(None).await;
}

// =============================================================================
// Overwrite in same turn
// =============================================================================

#[tokio::test]
async fn kv_overwrite_same_key() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("OverwriteKV", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("k", "first");
            ctx.set_kv_value("k", "second");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-ow", "OverwriteKV", "").await.unwrap();
    client
        .wait_for_orchestration("kv-ow", Duration::from_secs(5))
        .await
        .unwrap();

    let val = client.get_kv_value("kv-ow", "k").await.unwrap();
    assert_eq!(val, Some("second".to_string()));

    rt.shutdown(None).await;
}

// =============================================================================
// Clear single value
// =============================================================================

#[tokio::test]
async fn kv_clear_single_value() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ClearKV", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("x", "10");
            ctx.clear_kv_value("x");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-clr", "ClearKV", "").await.unwrap();
    client
        .wait_for_orchestration("kv-clr", Duration::from_secs(5))
        .await
        .unwrap();

    let val = client.get_kv_value("kv-clr", "x").await.unwrap();
    assert_eq!(val, None);

    rt.shutdown(None).await;
}

// =============================================================================
// Clear all values
// =============================================================================

#[tokio::test]
async fn kv_clear_all_values() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ClearAllKV", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("a", "1");
            ctx.set_kv_value("b", "2");
            ctx.clear_all_kv_values();
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-clra", "ClearAllKV", "").await.unwrap();
    client
        .wait_for_orchestration("kv-clra", Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(client.get_kv_value("kv-clra", "a").await.unwrap(), None);
    assert_eq!(client.get_kv_value("kv-clra", "b").await.unwrap(), None);

    rt.shutdown(None).await;
}

// =============================================================================
// KV persists across activity turns
// =============================================================================

#[tokio::test]
async fn kv_persists_across_turns() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("DoWork", |_ctx: ActivityContext, _input: String| async move {
            Ok("result".to_string())
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("KVTurns", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("step", "before_activity");
            let _ = ctx.schedule_activity("DoWork", "").await;
            // After activity completion, a new turn starts — KV should persist.
            let val = ctx.get_kv_value("step");
            assert_eq!(val, Some("before_activity".to_string()));
            ctx.set_kv_value("step", "after_activity");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-turns", "KVTurns", "").await.unwrap();
    client
        .wait_for_orchestration("kv-turns", Duration::from_secs(5))
        .await
        .unwrap();

    let val = client.get_kv_value("kv-turns", "step").await.unwrap();
    assert_eq!(val, Some("after_activity".to_string()));

    rt.shutdown(None).await;
}

// =============================================================================
// Typed KV via set_value_typed / get_value_typed
// =============================================================================

#[tokio::test]
async fn kv_typed_round_trip() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("TypedKV", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value_typed("count", &42i32);
            let val: Option<i32> = ctx.get_kv_value_typed("count").unwrap();
            assert_eq!(val, Some(42));
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-typed", "TypedKV", "").await.unwrap();
    client
        .wait_for_orchestration("kv-typed", Duration::from_secs(5))
        .await
        .unwrap();

    // Also verify via typed client API
    let val: Option<i32> = client.get_kv_value_typed("kv-typed", "count").await.unwrap();
    assert_eq!(val, Some(42));

    rt.shutdown(None).await;
}

// =============================================================================
// In-orchestration get_value reads local state
// =============================================================================

#[tokio::test]
async fn kv_get_value_reads_local_state() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("LocalKV", |ctx: OrchestrationContext, _input: String| async move {
            assert_eq!(ctx.get_kv_value("x"), None);
            ctx.set_kv_value("x", "hello");
            assert_eq!(ctx.get_kv_value("x"), Some("hello".to_string()));
            ctx.clear_kv_value("x");
            assert_eq!(ctx.get_kv_value("x"), None);
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-local", "LocalKV", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-local", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    rt.shutdown(None).await;
}

// =============================================================================
// KV survives continue_as_new
// =============================================================================

#[tokio::test]
async fn kv_survives_continue_as_new() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let iteration = Arc::new(AtomicU32::new(0));

    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let iter_clone = iteration.clone();
    let orchestrations = OrchestrationRegistry::builder()
        .register("CANKVV", move |ctx: OrchestrationContext, _input: String| {
            let iter = iter_clone.clone();
            async move {
                let i = iter.fetch_add(1, Ordering::SeqCst);
                if i == 0 {
                    ctx.set_kv_value("counter", "1");
                    let _ = ctx.continue_as_new("next").await;
                    Ok("continued".to_string())
                } else {
                    // After continue_as_new, the KV value should still be visible
                    let val = ctx.get_kv_value("counter");
                    assert_eq!(val, Some("1".to_string()), "KV should survive continue_as_new");
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-can", "CANKVV", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-can", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "done"),
        other => panic!("Expected Completed, got: {other:?}"),
    }

    let val = client.get_kv_value("kv-can", "counter").await.unwrap();
    assert_eq!(val, Some("1".to_string()));

    rt.shutdown(None).await;
}

// =============================================================================
// get_value_from_instance reads another instance's KV
// =============================================================================

#[tokio::test]
async fn kv_get_value_from_instance() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Noop", |_ctx: ActivityContext, _input: String| async move {
            Ok("ok".to_string())
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Writer", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("secret", "42");
            // Wait for an event to keep the orchestration alive
            ctx.schedule_wait("done").await;
            Ok("done".to_string())
        })
        .register("Reader", |ctx: OrchestrationContext, _input: String| async move {
            let val = ctx.get_kv_value_from_instance("kv-writer", "secret").await?;
            Ok(val.unwrap_or_else(|| "missing".to_string()))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());

    // Start the writer and wait for it to set the KV value
    client.start_orchestration("kv-writer", "Writer", "").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start the reader
    client.start_orchestration("kv-reader", "Reader", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-reader", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "42");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    // Cleanup: complete the writer
    client.raise_event("kv-writer", "done", "").await.unwrap();
    client
        .wait_for_orchestration("kv-writer", Duration::from_secs(5))
        .await
        .unwrap();

    rt.shutdown(None).await;
}

// =============================================================================
// KV deleted when instance deleted
// =============================================================================

#[tokio::test]
async fn kv_deleted_with_instance() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("KVDel", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("foo", "bar");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-del", "KVDel", "").await.unwrap();
    client
        .wait_for_orchestration("kv-del", Duration::from_secs(5))
        .await
        .unwrap();

    // Verify KV exists
    assert_eq!(
        client.get_kv_value("kv-del", "foo").await.unwrap(),
        Some("bar".to_string())
    );

    // Delete the instance
    client.delete_instance("kv-del", true).await.unwrap();

    // KV should be gone
    assert_eq!(client.get_kv_value("kv-del", "foo").await.unwrap(), None);

    rt.shutdown(None).await;
}

// =============================================================================
// Exceeding MAX_KV_KEYS limit fails the orchestration
// =============================================================================

#[tokio::test]
async fn kv_exceeding_key_limit_fails_orchestration() {
    use duroxide::runtime::limits::MAX_KV_KEYS;

    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("TooManyKeys", |ctx: OrchestrationContext, _input: String| async move {
            // Set MAX_KV_KEYS + 1 keys to exceed the limit
            for i in 0..=MAX_KV_KEYS {
                ctx.set_kv_value(format!("key_{i}"), format!("val_{i}"));
            }
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-limit", "TooManyKeys", "").await.unwrap();

    let status = client
        .wait_for_orchestration("kv-limit", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            let msg = format!("{details:?}");
            assert!(msg.contains("KV key count"), "Expected KV key count error, got: {msg}");
        }
        other => panic!("Expected Failed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// Exceeding MAX_KV_VALUE_BYTES fails the orchestration
// =============================================================================

#[tokio::test]
async fn kv_exceeding_value_size_limit_fails_orchestration() {
    use duroxide::runtime::limits::MAX_KV_VALUE_BYTES;

    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let oversized = "x".repeat(MAX_KV_VALUE_BYTES + 1);
    let orchestrations = OrchestrationRegistry::builder()
        .register("BigValue", move |ctx: OrchestrationContext, _input: String| {
            let big = oversized.clone();
            async move {
                ctx.set_kv_value("big", &big);
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-big", "BigValue", "").await.unwrap();

    let status = client
        .wait_for_orchestration("kv-big", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Failed { details, .. } => {
            let msg = format!("{details:?}");
            assert!(
                msg.contains("KV value") && msg.contains("exceeds limit"),
                "Expected KV value size error, got: {msg}"
            );
        }
        other => panic!("Expected Failed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

// =============================================================================
// At exactly MAX_KV_KEYS succeeds
// =============================================================================

#[tokio::test]
async fn kv_at_key_limit_succeeds() {
    use duroxide::runtime::limits::MAX_KV_KEYS;

    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ExactKeys", |ctx: OrchestrationContext, _input: String| async move {
            for i in 0..MAX_KV_KEYS {
                ctx.set_kv_value(format!("key_{i}"), format!("val_{i}"));
            }
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-exact", "ExactKeys", "").await.unwrap();

    let status = client
        .wait_for_orchestration("kv-exact", Duration::from_secs(5))
        .await
        .unwrap();

    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    rt.shutdown(None).await;
}

// =============================================================================
// KV clear_all reduces effective key count below limit
// =============================================================================

#[tokio::test]
async fn kv_clear_all_resets_key_count() {
    use duroxide::runtime::limits::MAX_KV_KEYS;

    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "ClearAndRefill",
            |ctx: OrchestrationContext, _input: String| async move {
                // Fill to max
                for i in 0..MAX_KV_KEYS {
                    ctx.set_kv_value(format!("old_{i}"), "v");
                }
                // Clear all — this should reset the count
                ctx.clear_all_kv_values();
                // Set new keys up to max again — should succeed
                for i in 0..MAX_KV_KEYS {
                    ctx.set_kv_value(format!("new_{i}"), "v");
                }
                Ok("done".to_string())
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-clr-refill", "ClearAndRefill", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("kv-clr-refill", Duration::from_secs(5))
        .await
        .unwrap();

    assert!(
        matches!(status, OrchestrationStatus::Completed { .. }),
        "Expected Completed after clear_all + refill, got: {status:?}"
    );

    rt.shutdown(None).await;
}

// =============================================================================
// KV set_value / get_value inside select2 branch
// =============================================================================

#[tokio::test]
async fn kv_works_in_select_branch() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("FastTask", |_ctx: ActivityContext, _input: String| async move {
            Ok("fast".to_string())
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("SelectKV", |ctx: OrchestrationContext, _input: String| async move {
            let timer = ctx.schedule_timer(Duration::from_secs(60));
            let activity = ctx.schedule_activity("FastTask", "");

            match ctx.select2(timer, activity).await {
                duroxide::Either2::First(()) => {
                    ctx.set_kv_value("winner", "timer");
                }
                duroxide::Either2::Second(result) => {
                    let _ = result?;
                    ctx.set_kv_value("winner", "activity");
                }
            }

            let winner = ctx.get_kv_value("winner").unwrap_or_default();
            Ok(winner)
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-sel", "SelectKV", "").await.unwrap();

    let status = client
        .wait_for_orchestration("kv-sel", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "activity", "Activity should win the select2");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    let val = client.get_kv_value("kv-sel", "winner").await.unwrap();
    assert_eq!(val, Some("activity".to_string()));

    rt.shutdown(None).await;
}

// =============================================================================
// Terminal-state accessibility
// =============================================================================

/// KV values remain accessible after orchestration completes successfully.
#[tokio::test]
async fn kv_accessible_after_completion() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = duroxide::Client::new(store.clone());

    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Completer", |ctx: OrchestrationContext, _: String| async move {
            ctx.set_kv_value("status", "done");
            ctx.set_kv_value("result", "42");
            Ok("completed".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    client
        .start_orchestration("kv-complete", "Completer", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-complete", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    // Values should still be accessible
    assert_eq!(
        client.get_kv_value("kv-complete", "status").await.unwrap(),
        Some("done".to_string())
    );
    assert_eq!(
        client.get_kv_value("kv-complete", "result").await.unwrap(),
        Some("42".to_string())
    );

    rt.shutdown(None).await;
}

/// KV values remain accessible after orchestration fails.
#[tokio::test]
async fn kv_accessible_after_failure() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = duroxide::Client::new(store.clone());

    let activities = ActivityRegistry::builder()
        .register("FailActivity", |_ctx: ActivityContext, _: String| async move {
            Err("boom".to_string())
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Failer", |ctx: OrchestrationContext, _: String| async move {
            ctx.set_kv_value("progress", "started");
            ctx.set_kv_value("step", "pre-activity");
            // This activity will fail, causing the orchestration to fail
            ctx.schedule_activity("FailActivity", "".to_string()).await?;
            Ok("unreachable".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    client.start_orchestration("kv-fail", "Failer", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-fail", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Failed { .. }));

    // Values set before the failure should still be accessible
    assert_eq!(
        client.get_kv_value("kv-fail", "progress").await.unwrap(),
        Some("started".to_string())
    );
    assert_eq!(
        client.get_kv_value("kv-fail", "step").await.unwrap(),
        Some("pre-activity".to_string())
    );

    rt.shutdown(None).await;
}

/// KV values are NOT accessible after instance is deleted.
#[tokio::test]
async fn kv_not_accessible_after_deletion() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = duroxide::Client::new(store.clone());

    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("KvOrch", |ctx: OrchestrationContext, _: String| async move {
            ctx.set_kv_value("important", "data");
            ctx.set_kv_value("other", "stuff");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    client.start_orchestration("kv-del", "KvOrch", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-del", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    // Values accessible before deletion
    assert_eq!(
        client.get_kv_value("kv-del", "important").await.unwrap(),
        Some("data".to_string())
    );

    // Delete the instance
    let result = client.delete_instance("kv-del", false).await.unwrap();
    assert!(result.instances_deleted >= 1);

    // Values should be gone
    assert_eq!(client.get_kv_value("kv-del", "important").await.unwrap(), None);
    assert_eq!(client.get_kv_value("kv-del", "other").await.unwrap(), None);

    rt.shutdown(None).await;
}

// =============================================================================
// Pruning integration
// =============================================================================

/// Pruning old executions removes KV keys that are orphaned (last written by pruned exec).
/// Keys overwritten in a newer execution survive.
#[tokio::test]
async fn kv_prune_execution_removes_orphan_keys() {
    use duroxide::providers::PruneOptions;

    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = duroxide::Client::new(store.clone());

    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("PruneKv", |ctx: OrchestrationContext, input: String| async move {
            let exec: u32 = input.parse().unwrap_or(0);
            if exec == 0 {
                // First execution: set both keys
                ctx.set_kv_value("ephemeral", "from_exec_1");
                ctx.set_kv_value("persistent", "from_exec_1");
                ctx.continue_as_new("1".to_string()).await
            } else {
                // Second execution: overwrite only "persistent"
                ctx.set_kv_value("persistent", "from_exec_2");
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    client.start_orchestration("kv-prune", "PruneKv", "0").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-prune", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    // Both keys present before prune
    assert_eq!(
        client.get_kv_value("kv-prune", "ephemeral").await.unwrap(),
        Some("from_exec_1".to_string())
    );
    assert_eq!(
        client.get_kv_value("kv-prune", "persistent").await.unwrap(),
        Some("from_exec_2".to_string())
    );

    // Prune keeping only last 1 execution (removes exec 1)
    let result = client
        .prune_executions(
            "kv-prune",
            PruneOptions {
                keep_last: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(result.executions_deleted >= 1);

    // "ephemeral" was only written in exec 1 → survives (KV is instance-scoped)
    assert_eq!(
        client.get_kv_value("kv-prune", "ephemeral").await.unwrap(),
        Some("from_exec_1".to_string()),
    );
    // "persistent" was overwritten in exec 2 → survives
    assert_eq!(
        client.get_kv_value("kv-prune", "persistent").await.unwrap(),
        Some("from_exec_2".to_string())
    );

    rt.shutdown(None).await;
}

/// Deleting an instance removes all its KV entries.
#[tokio::test]
async fn kv_delete_instance_removes_all_kv() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let client = duroxide::Client::new(store.clone());

    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("KvDelInst", |ctx: OrchestrationContext, input: String| async move {
            let exec: u32 = input.parse().unwrap_or(0);
            if exec == 0 {
                ctx.set_kv_value("a", "1");
                ctx.set_kv_value("b", "2");
                ctx.continue_as_new("1".to_string()).await
            } else {
                ctx.set_kv_value("c", "3");
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    client
        .start_orchestration("kv-delinst", "KvDelInst", "0")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-delinst", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    // All three KV entries present across two executions
    assert_eq!(
        client.get_kv_value("kv-delinst", "a").await.unwrap(),
        Some("1".to_string())
    );
    assert_eq!(
        client.get_kv_value("kv-delinst", "b").await.unwrap(),
        Some("2".to_string())
    );
    assert_eq!(
        client.get_kv_value("kv-delinst", "c").await.unwrap(),
        Some("3".to_string())
    );

    // Delete the instance entirely
    let result = client.delete_instance("kv-delinst", false).await.unwrap();
    assert!(result.instances_deleted >= 1);

    // All KV entries gone
    assert_eq!(client.get_kv_value("kv-delinst", "a").await.unwrap(), None);
    assert_eq!(client.get_kv_value("kv-delinst", "b").await.unwrap(), None);
    assert_eq!(client.get_kv_value("kv-delinst", "c").await.unwrap(), None);

    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-03: Progressive updates across turns
// =============================================================================

#[tokio::test]
async fn kv_update_across_turns() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Noop", |_ctx: ActivityContext, _input: String| async move {
            Ok("ok".to_string())
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("UpdateAcross", |ctx: OrchestrationContext, _input: String| async move {
            // Turn 1
            ctx.set_kv_value("counter", "1");
            ctx.schedule_activity("Noop", "").await?;
            // Turn 2
            let val = ctx.get_kv_value("counter");
            assert_eq!(val, Some("1".to_string()));
            ctx.set_kv_value("counter", "2");
            ctx.schedule_activity("Noop", "").await?;
            // Turn 3
            let val = ctx.get_kv_value("counter");
            assert_eq!(val, Some("2".to_string()));
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-upd", "UpdateAcross", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-upd", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));
    assert_eq!(
        client.get_kv_value("kv-upd", "counter").await.unwrap(),
        Some("2".to_string())
    );
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-04b: clear_value then re-set same key
// =============================================================================

#[tokio::test]
async fn kv_clear_value_then_reset() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ClearReset", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("key", "old");
            ctx.clear_kv_value("key");
            ctx.set_kv_value("key", "new");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-clreset", "ClearReset", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-clreset", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));
    assert_eq!(
        client.get_kv_value("kv-clreset", "key").await.unwrap(),
        Some("new".to_string())
    );
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-06: Overwrite after CAN
// =============================================================================

#[tokio::test]
async fn kv_overwrite_after_can() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let iteration = Arc::new(AtomicU32::new(0));
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let iter_clone = iteration.clone();
    let orchestrations = OrchestrationRegistry::builder()
        .register("CanOverwrite", move |ctx: OrchestrationContext, _input: String| {
            let iter = iter_clone.clone();
            async move {
                let i = iter.fetch_add(1, Ordering::SeqCst);
                if i == 0 {
                    ctx.set_kv_value("A", "old");
                    let _ = ctx.continue_as_new("next").await;
                    Ok("continued".to_string())
                } else {
                    ctx.set_kv_value("A", "new");
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-canovr", "CanOverwrite", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-canovr", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));
    assert_eq!(
        client.get_kv_value("kv-canovr", "A").await.unwrap(),
        Some("new".to_string())
    );
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-07: Clear after CAN
// =============================================================================

#[tokio::test]
async fn kv_clear_after_can() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let iteration = Arc::new(AtomicU32::new(0));
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let iter_clone = iteration.clone();
    let orchestrations = OrchestrationRegistry::builder()
        .register("CanClear", move |ctx: OrchestrationContext, _input: String| {
            let iter = iter_clone.clone();
            async move {
                let i = iter.fetch_add(1, Ordering::SeqCst);
                if i == 0 {
                    ctx.set_kv_value("A", "val_a");
                    ctx.set_kv_value("B", "val_b");
                    let _ = ctx.continue_as_new("next").await;
                    Ok("continued".to_string())
                } else {
                    ctx.clear_all_kv_values();
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-canclr", "CanClear", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-canclr", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));
    assert_eq!(client.get_kv_value("kv-canclr", "A").await.unwrap(), None);
    assert_eq!(client.get_kv_value("kv-canclr", "B").await.unwrap(), None);
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-08: CAN chain accumulation
// =============================================================================

#[tokio::test]
async fn kv_can_chain_accumulation() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let iteration = Arc::new(AtomicU32::new(0));
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let iter_clone = iteration.clone();
    let orchestrations = OrchestrationRegistry::builder()
        .register("CanAccum", move |ctx: OrchestrationContext, _input: String| {
            let iter = iter_clone.clone();
            async move {
                let i = iter.fetch_add(1, Ordering::SeqCst);
                match i {
                    0 => {
                        ctx.set_kv_value("count", "1");
                        let _ = ctx.continue_as_new("next").await;
                        Ok("continued".to_string())
                    }
                    1 => {
                        let val = ctx.get_kv_value("count").unwrap_or("0".to_string());
                        let n: u32 = val.parse().unwrap();
                        ctx.set_kv_value("count", (n + 1).to_string());
                        let _ = ctx.continue_as_new("next2").await;
                        Ok("continued".to_string())
                    }
                    _ => {
                        let val = ctx.get_kv_value("count");
                        assert_eq!(val, Some("2".to_string()));
                        Ok("done".to_string())
                    }
                }
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-accum", "CanAccum", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-accum", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));
    assert_eq!(
        client.get_kv_value("kv-accum", "count").await.unwrap(),
        Some("2".to_string())
    );
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-11: Bulk prune
// =============================================================================

#[tokio::test]
async fn kv_bulk_prune() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let iteration = Arc::new(AtomicU32::new(0));
    let activities = ActivityRegistry::builder().build();
    let iter_clone = iteration.clone();
    let orchestrations = OrchestrationRegistry::builder()
        .register("BulkPruneKV", move |ctx: OrchestrationContext, _input: String| {
            let iter = iter_clone.clone();
            async move {
                let i = iter.fetch_add(1, Ordering::SeqCst);
                if i == 0 {
                    ctx.set_kv_value("first_only", "exec1");
                    ctx.set_kv_value("shared", "exec1");
                    let _ = ctx.continue_as_new("next").await;
                    Ok("continued".to_string())
                } else {
                    ctx.set_kv_value("shared", "exec2");
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-bprune", "BulkPruneKV", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-bprune", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    // Prune old executions
    let prune_result = client
        .prune_executions(
            "kv-bprune",
            duroxide::providers::PruneOptions {
                keep_last: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(prune_result.executions_deleted >= 1);

    // "first_only" was only set in exec 1 → survives (KV is instance-scoped)
    assert_eq!(
        client.get_kv_value("kv-bprune", "first_only").await.unwrap(),
        Some("exec1".to_string()),
    );
    // "shared" was overwritten in exec 2 → should survive
    assert_eq!(
        client.get_kv_value("kv-bprune", "shared").await.unwrap(),
        Some("exec2".to_string()),
    );

    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-12: Client reads KV while orchestration is running
// =============================================================================

#[tokio::test]
async fn client_get_value_running() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("RunningKV", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("progress", "initialized");
            ctx.schedule_wait("finish").await;
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-running", "RunningKV", "").await.unwrap();

    // Wait for KV to appear while orchestration is still running
    let val = client
        .wait_for_kv_value("kv-running", "progress", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(val, "initialized");

    // Complete orchestration
    client.raise_event("kv-running", "finish", "").await.unwrap();
    client
        .wait_for_orchestration("kv-running", Duration::from_secs(5))
        .await
        .unwrap();
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-CI-02: Cross-instance, key missing
// =============================================================================

#[tokio::test]
async fn kv_get_value_from_instance_key_missing() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Writer2", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("exists", "yes");
            ctx.schedule_wait("done").await;
            Ok("done".to_string())
        })
        .register("Reader2", |ctx: OrchestrationContext, _input: String| async move {
            let val = ctx.get_kv_value_from_instance("kv-writer2", "nonexistent").await?;
            Ok(val.unwrap_or("none".to_string()))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-writer2", "Writer2", "").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    client.start_orchestration("kv-reader2", "Reader2", "").await.unwrap();

    let status = client
        .wait_for_orchestration("kv-reader2", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "none"),
        other => panic!("Expected Completed, got: {other:?}"),
    }

    client.raise_event("kv-writer2", "done", "").await.unwrap();
    client
        .wait_for_orchestration("kv-writer2", Duration::from_secs(5))
        .await
        .unwrap();
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-CI-03: Cross-instance, unknown instance
// =============================================================================

#[tokio::test]
async fn kv_get_value_from_instance_unknown_instance() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "ReaderUnknown",
            |ctx: OrchestrationContext, _input: String| async move {
                let val = ctx.get_kv_value_from_instance("no-such-inst", "key").await?;
                Ok(val.unwrap_or("none".to_string()))
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-unknown", "ReaderUnknown", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-unknown", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "none"),
        other => panic!("Expected Completed, got: {other:?}"),
    }
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-CI-04: Cross-instance, typed
// =============================================================================

#[tokio::test]
async fn kv_get_value_from_instance_typed_e2e() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("TypedWriter", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value_typed("config", &vec![1u32, 2, 3]);
            ctx.schedule_wait("done").await;
            Ok("done".to_string())
        })
        .register("TypedReader", |ctx: OrchestrationContext, _input: String| async move {
            let val: Option<Vec<u32>> = ctx.get_kv_value_from_instance_typed("kv-tw", "config").await?;
            Ok(format!("{:?}", val))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-tw", "TypedWriter", "").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    client.start_orchestration("kv-tr", "TypedReader", "").await.unwrap();

    let status = client
        .wait_for_orchestration("kv-tr", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "Some([1, 2, 3])");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    client.raise_event("kv-tw", "done", "").await.unwrap();
    client
        .wait_for_orchestration("kv-tw", Duration::from_secs(5))
        .await
        .unwrap();
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-CI-05: Cross-instance replay safe
// =============================================================================

#[tokio::test]
async fn kv_get_value_from_instance_replay_safe() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Noop", |_ctx: ActivityContext, _input: String| async move {
            Ok("ok".to_string())
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("SourceOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("data", "source_value");
            ctx.schedule_wait("done").await;
            Ok("done".to_string())
        })
        .register("ReplayReader", |ctx: OrchestrationContext, _input: String| async move {
            // Read from other instance (system activity)
            let val = ctx.get_kv_value_from_instance("kv-source", "data").await?;
            // Schedule an activity so replay can be exercised
            ctx.schedule_activity("Noop", "").await?;
            Ok(val.unwrap_or("missing".to_string()))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());

    client.start_orchestration("kv-source", "SourceOrch", "").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    client
        .start_orchestration("kv-replayer", "ReplayReader", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-replayer", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "source_value"),
        other => panic!("Expected Completed, got: {other:?}"),
    }

    client.raise_event("kv-source", "done", "").await.unwrap();
    client
        .wait_for_orchestration("kv-source", Duration::from_secs(5))
        .await
        .unwrap();
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-CI-06: Cross-instance after source update
// =============================================================================

#[tokio::test]
async fn kv_get_value_from_instance_after_source_update() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("LiveSource", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("v", "1");
            ctx.schedule_wait("update").await;
            ctx.set_kv_value("v", "2");
            ctx.schedule_wait("done").await;
            Ok("done".to_string())
        })
        .register("LiveReader", |ctx: OrchestrationContext, _input: String| async move {
            // First read
            let v1 = ctx.get_kv_value_from_instance("kv-lsrc", "v").await?;
            ctx.set_kv_value("read1", v1.clone().unwrap_or_default());
            // Wait for source to update
            ctx.schedule_wait("source_updated").await;
            // Second read — should see updated value
            let v2 = ctx.get_kv_value_from_instance("kv-lsrc", "v").await?;
            ctx.set_kv_value("read2", v2.clone().unwrap_or_default());
            Ok(format!("{},{}", v1.unwrap_or_default(), v2.unwrap_or_default()))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());

    client.start_orchestration("kv-lsrc", "LiveSource", "").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    client.start_orchestration("kv-lrdr", "LiveReader", "").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Update source
    client.raise_event("kv-lsrc", "update", "").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Tell reader to do second read
    client.raise_event("kv-lrdr", "source_updated", "").await.unwrap();

    let status = client
        .wait_for_orchestration("kv-lrdr", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "1,2");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    client.raise_event("kv-lsrc", "done", "").await.unwrap();
    client
        .wait_for_orchestration("kv-lsrc", Duration::from_secs(5))
        .await
        .unwrap();
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-15: Parent-child KV isolation
// =============================================================================

#[tokio::test]
async fn kv_parent_child_isolation() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Parent", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("role", "parent");
            let child_result = ctx.schedule_sub_orchestration("Child", "").await?;
            Ok(child_result)
        })
        .register("Child", |ctx: OrchestrationContext, _input: String| async move {
            // KV is instance-scoped — child should NOT see parent's "role"
            let parent_val = ctx.get_kv_value("role");
            assert_eq!(parent_val, None, "child should not see parent KV");
            ctx.set_kv_value("role", "child");
            Ok("child_done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-parent-iso", "Parent", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-parent-iso", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    // Parent's KV has "parent"
    assert_eq!(
        client.get_kv_value("kv-parent-iso", "role").await.unwrap(),
        Some("parent".to_string()),
    );

    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-16: Delete parent cascades child KV
// =============================================================================

#[tokio::test]
async fn kv_delete_parent_cascades_child_kv() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ParentDel", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("parent_key", "parent_val");
            let _ = ctx.schedule_sub_orchestration("ChildDel", "").await?;
            Ok("done".to_string())
        })
        .register("ChildDel", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("child_key", "child_val");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-pdel", "ParentDel", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-pdel", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    // Delete parent (which cascades to child)
    let del = client.delete_instance("kv-pdel", false).await.unwrap();
    assert!(del.instances_deleted >= 1);

    assert_eq!(client.get_kv_value("kv-pdel", "parent_key").await.unwrap(), None);

    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-21: KV interleaved with activities and timers
// =============================================================================

#[tokio::test]
async fn kv_interleaved_with_activities_and_timers() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Work", |_ctx: ActivityContext, _input: String| async move {
            Ok("worked".to_string())
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Interleave", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("step", "1");
            ctx.schedule_activity("Work", "").await?;
            ctx.set_kv_value("step", "2");
            ctx.schedule_timer(Duration::from_millis(1)).await;
            ctx.clear_all_kv_values();
            ctx.set_kv_value("step", "final");
            Ok(ctx.get_kv_value("step").unwrap_or_default())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-interl", "Interleave", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-interl", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "final"),
        other => panic!("Expected Completed, got: {other:?}"),
    }
    assert_eq!(
        client.get_kv_value("kv-interl", "step").await.unwrap(),
        Some("final".to_string())
    );
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-23: Fan-out fan-in with KV after join
// =============================================================================

#[tokio::test]
async fn kv_in_fan_out_fan_in() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Process", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("result_{input}"))
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("FanOutKV", |ctx: OrchestrationContext, _input: String| async move {
            let mut futures = Vec::new();
            for i in 0..5 {
                futures.push(ctx.schedule_activity("Process", i.to_string()));
            }
            let results = ctx.join(futures).await;
            for (i, r) in results.iter().enumerate() {
                if let Ok(val) = r {
                    ctx.set_kv_value(format!("result_{i}"), val);
                }
            }
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-fanout", "FanOutKV", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-fanout", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    for i in 0..5 {
        let val = client.get_kv_value("kv-fanout", &format!("result_{i}")).await.unwrap();
        assert_eq!(val, Some(format!("result_{i}")));
    }
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-24: JSON value stored as-is
// =============================================================================

#[tokio::test]
async fn kv_json_value() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("JsonKV", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("data", r#"{"nested": [1,2,3]}"#);
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-json", "JsonKV", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-json", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));
    assert_eq!(
        client.get_kv_value("kv-json", "data").await.unwrap(),
        Some(r#"{"nested": [1,2,3]}"#.to_string()),
    );
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-25: Empty key is valid
// =============================================================================

#[tokio::test]
async fn kv_empty_key() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("EmptyKeyKV", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("", "value_for_empty_key");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-emptykey", "EmptyKeyKV", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-emptykey", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));
    assert_eq!(
        client.get_kv_value("kv-emptykey", "").await.unwrap(),
        Some("value_for_empty_key".to_string()),
    );
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-25a: clear_value then set same key in one turn
// =============================================================================

#[tokio::test]
async fn kv_clear_value_then_set_same_key() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ClearSetSame", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("A", "1");
            ctx.clear_kv_value("A");
            ctx.set_kv_value("A", "2");
            Ok(ctx.get_kv_value("A").unwrap_or_default())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-clrset", "ClearSetSame", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-clrset", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "2"),
        other => panic!("Expected Completed, got: {other:?}"),
    }
    assert_eq!(
        client.get_kv_value("kv-clrset", "A").await.unwrap(),
        Some("2".to_string())
    );
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-25b: get_value_from_instance(self) reads own materialized KV
// =============================================================================

#[tokio::test]
async fn kv_get_value_from_instance_self() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Noop", |_ctx: ActivityContext, _input: String| async move {
            Ok("ok".to_string())
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("SelfRead", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("key", "local_val");
            // Force a turn so the KV is materialized
            ctx.schedule_activity("Noop", "").await?;
            // Read own instance via get_value_from_instance (system activity)
            let val = ctx.get_kv_value_from_instance("kv-self", "key").await?;
            Ok(val.unwrap_or("missing".to_string()))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-self", "SelfRead", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-self", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "local_val"),
        other => panic!("Expected Completed, got: {other:?}"),
    }
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-25c: Typed wrong-type deserialization error
// =============================================================================

#[tokio::test]
async fn kv_get_value_typed_wrong_type() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("TypeMismatch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value_typed("data", &vec![1u32, 2, 3]);
            let result = ctx.get_kv_value_typed::<std::collections::HashMap<String, String>>("data");
            match result {
                Err(_) => Ok("error_caught".to_string()),
                Ok(None) => Ok("none".to_string()),
                Ok(Some(v)) => Ok(format!("unexpected: {v:?}")),
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-typemis", "TypeMismatch", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-typemis", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "error_caught"),
        other => panic!("Expected Completed, got: {other:?}"),
    }
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-26: Single-threaded runtime basic
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn kv_single_thread_basic() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Task", |_ctx: ActivityContext, _input: String| async move {
            Ok("ok".to_string())
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("STKv", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("key", "val");
            let _ = ctx.schedule_activity("Task", "").await?;
            let v = ctx.get_kv_value("key");
            Ok(v.unwrap_or_default())
        })
        .build();

    let opts = runtime::RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        ..Default::default()
    };
    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, opts).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-st", "STKv", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-st", Duration::from_secs(10))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "val"),
        other => panic!("Expected Completed, got: {other:?}"),
    }
    assert_eq!(
        client.get_kv_value("kv-st", "key").await.unwrap(),
        Some("val".to_string())
    );
    rt.shutdown(None).await;
}

// =============================================================================
// E2E-KV-27: Single-threaded CAN with KV
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn kv_single_thread_can_with_kv() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let iteration = Arc::new(AtomicU32::new(0));
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let iter_clone = iteration.clone();
    let orchestrations = OrchestrationRegistry::builder()
        .register("STCAN", move |ctx: OrchestrationContext, _input: String| {
            let iter = iter_clone.clone();
            async move {
                let i = iter.fetch_add(1, Ordering::SeqCst);
                if i == 0 {
                    ctx.set_kv_value("count", "1");
                    let _ = ctx.continue_as_new("next").await;
                    Ok("continued".to_string())
                } else {
                    let val = ctx.get_kv_value("count");
                    assert_eq!(val, Some("1".to_string()));
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let opts = runtime::RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        ..Default::default()
    };
    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, opts).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-stcan", "STCAN", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-stcan", Duration::from_secs(10))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));
    assert_eq!(
        client.get_kv_value("kv-stcan", "count").await.unwrap(),
        Some("1".to_string())
    );
    rt.shutdown(None).await;
}

// =============================================================================
// STRESS-KV-01: Concurrent KV writes from parallel orchestrations
// =============================================================================

#[tokio::test]
async fn kv_stress_concurrent_writes() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ConcWrite", |ctx: OrchestrationContext, input: String| async move {
            for i in 0..5 {
                ctx.set_kv_value(format!("key_{i}"), format!("val_{input}_{i}"));
            }
            Ok(format!("wrote_{input}"))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());

    // Launch 10 concurrent orchestrations
    let mut handles = Vec::new();
    for i in 0..10 {
        let s = store.clone();
        let id = format!("kv-conc-{i}");
        handles.push(tokio::spawn(async move {
            let c = duroxide::Client::new(s);
            c.start_orchestration(&id, "ConcWrite", &i.to_string()).await.unwrap();
            c.wait_for_orchestration(&id, Duration::from_secs(10)).await.unwrap()
        }));
    }

    let mut completed = 0;
    for h in handles {
        if let Ok(OrchestrationStatus::Completed { .. }) = h.await {
            completed += 1;
        }
    }

    assert_eq!(completed, 10, "All 10 orchestrations should complete");

    // Verify a sample of KV values
    for i in [0, 3, 7, 9] {
        assert_eq!(
            client.get_kv_value(&format!("kv-conc-{i}"), "key_0").await.unwrap(),
            Some(format!("val_{i}_0")),
        );
    }

    rt.shutdown(None).await;
}

// =============================================================================
// STRESS-KV-02: High key count per instance
// =============================================================================

#[tokio::test]
async fn kv_stress_high_key_count() {
    use duroxide::runtime::limits::MAX_KV_KEYS;
    use std::sync::atomic::{AtomicU32, Ordering};

    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder()
        .register("Noop", |_ctx: ActivityContext, _input: String| async move {
            Ok("ok".to_string())
        })
        .build();

    let turn_counter = Arc::new(AtomicU32::new(0));
    let tc = turn_counter.clone();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ManyKeys", move |ctx: OrchestrationContext, _input: String| {
            let tc = tc.clone();
            async move {
                // Write MAX_KV_KEYS keys (at the limit but not over).
                // Use a fixed value so the set_kv_value is deterministic across replays.
                for i in 0..MAX_KV_KEYS {
                    ctx.set_kv_value(format!("k{i}"), format!("v{i}"));
                }
                let t = tc.fetch_add(1, Ordering::SeqCst);
                if t < 5 {
                    ctx.schedule_activity("Noop", "").await?;
                    // Overwrite with the same deterministic values (safe for replay)
                    for i in 0..MAX_KV_KEYS {
                        ctx.set_kv_value(format!("k{i}"), format!("v{i}"));
                    }
                }
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-manykeys", "ManyKeys", "").await.unwrap();
    let status = client
        .wait_for_orchestration("kv-manykeys", Duration::from_secs(10))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));

    // Check that all MAX_KV_KEYS keys exist with final turn values
    for i in 0..MAX_KV_KEYS {
        let val = client.get_kv_value("kv-manykeys", &format!("k{i}")).await.unwrap();
        assert!(val.is_some(), "key k{i} should exist");
    }

    rt.shutdown(None).await;
}

// =============================================================================
// STRESS-KV-03: Cross-instance reads under load
// =============================================================================

#[tokio::test]
async fn kv_stress_cross_instance_reads() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("SharedWriter", |ctx: OrchestrationContext, _input: String| async move {
            ctx.set_kv_value("shared", "producer_value");
            ctx.schedule_wait("done").await;
            Ok("done".to_string())
        })
        .register("ConcReader", |ctx: OrchestrationContext, _input: String| async move {
            let val = ctx.get_kv_value_from_instance("kv-shared-src", "shared").await?;
            Ok(val.unwrap_or("missing".to_string()))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());

    // Start shared producer
    client
        .start_orchestration("kv-shared-src", "SharedWriter", "")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Launch 10 concurrent readers
    let mut handles = Vec::new();
    for i in 0..10 {
        let s = store.clone();
        let id = format!("kv-reader-{i}");
        handles.push(tokio::spawn(async move {
            let c = duroxide::Client::new(s);
            c.start_orchestration(&id, "ConcReader", "").await.unwrap();
            c.wait_for_orchestration(&id, Duration::from_secs(10)).await.unwrap()
        }));
    }

    let mut completed = 0;
    for h in handles {
        if let Ok(OrchestrationStatus::Completed { output, .. }) = h.await {
            assert_eq!(output, "producer_value");
            completed += 1;
        }
    }
    assert_eq!(completed, 10, "All 10 readers should complete");

    client.raise_event("kv-shared-src", "done", "").await.unwrap();
    rt.shutdown(None).await;
}

// =============================================================================
// STRESS-KV-04: KV + CAN throughput
// =============================================================================

#[tokio::test]
async fn kv_stress_can_throughput() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();

    // Each instance does 5 CAN iterations, each setting KV
    let iteration = Arc::new(AtomicU32::new(0));
    let iter_clone = iteration.clone();
    let orchestrations = OrchestrationRegistry::builder()
        .register("CANThroughput", move |ctx: OrchestrationContext, _input: String| {
            let iter = iter_clone.clone();
            async move {
                let i = iter.fetch_add(1, Ordering::SeqCst);
                ctx.set_kv_value("iter", i.to_string());
                if i < 4 {
                    let _ = ctx.continue_as_new(&format!("iter_{}", i + 1)).await;
                    Ok("continuing".to_string())
                } else {
                    Ok("done".to_string())
                }
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-can-tput", "CANThroughput", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("kv-can-tput", Duration::from_secs(10))
        .await
        .unwrap();
    assert!(matches!(status, OrchestrationStatus::Completed { .. }));
    assert_eq!(
        client.get_kv_value("kv-can-tput", "iter").await.unwrap(),
        Some("4".to_string())
    );
    rt.shutdown(None).await;
}

// =============================================================================
// Client::get_kv_all_values
// =============================================================================

/// Client::get_kv_all_values returns all entries set by the orchestration.
#[tokio::test]
async fn kv_client_get_all_values() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("SetMulti", |ctx: OrchestrationContext, _: String| async move {
            ctx.set_kv_value("a", "1");
            ctx.set_kv_value("b", "2");
            ctx.set_kv_value("c", "3");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-all-1", "SetMulti", "").await.unwrap();
    client
        .wait_for_orchestration("kv-all-1", Duration::from_secs(5))
        .await
        .unwrap();

    let all = client.get_kv_all_values("kv-all-1").await.unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all.get("a").map(String::as_str), Some("1"));
    assert_eq!(all.get("b").map(String::as_str), Some("2"));
    assert_eq!(all.get("c").map(String::as_str), Some("3"));

    rt.shutdown(None).await;
}

/// Client::get_kv_all_values returns empty map for instance with no KV.
#[tokio::test]
async fn kv_client_get_all_values_empty() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("NoKV", |_ctx: OrchestrationContext, _: String| async move {
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-all-empty", "NoKV", "").await.unwrap();
    client
        .wait_for_orchestration("kv-all-empty", Duration::from_secs(5))
        .await
        .unwrap();

    let all = client.get_kv_all_values("kv-all-empty").await.unwrap();
    assert!(all.is_empty());

    rt.shutdown(None).await;
}

/// Client::get_kv_all_values reflects the latest overwritten values.
#[tokio::test]
async fn kv_client_get_all_values_after_overwrite() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Overwrite", |ctx: OrchestrationContext, _: String| async move {
            ctx.set_kv_value("x", "old");
            ctx.set_kv_value("y", "keep");
            ctx.set_kv_value("x", "new");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-all-ow", "Overwrite", "").await.unwrap();
    client
        .wait_for_orchestration("kv-all-ow", Duration::from_secs(5))
        .await
        .unwrap();

    let all = client.get_kv_all_values("kv-all-ow").await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all.get("x").map(String::as_str), Some("new"));
    assert_eq!(all.get("y").map(String::as_str), Some("keep"));

    rt.shutdown(None).await;
}

/// Client::get_kv_all_values excludes cleared keys.
#[tokio::test]
async fn kv_client_get_all_values_after_clear() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("ClearSome", |ctx: OrchestrationContext, _: String| async move {
            ctx.set_kv_value("a", "1");
            ctx.set_kv_value("b", "2");
            ctx.set_kv_value("c", "3");
            ctx.clear_kv_value("b");
            Ok("done".to_string())
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-all-clr", "ClearSome", "").await.unwrap();
    client
        .wait_for_orchestration("kv-all-clr", Duration::from_secs(5))
        .await
        .unwrap();

    let all = client.get_kv_all_values("kv-all-clr").await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all.get("a").map(String::as_str), Some("1"));
    assert_eq!(all.get("c").map(String::as_str), Some("3"));
    assert!(!all.contains_key("b"));

    rt.shutdown(None).await;
}

/// Client::get_kv_all_values returns data across ContinueAsNew executions.
#[tokio::test]
async fn kv_client_get_all_values_across_can() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("CANKV", |ctx: OrchestrationContext, input: String| async move {
            let n: u32 = input.parse().unwrap_or(0);
            ctx.set_kv_value(format!("key_{n}"), format!("val_{n}"));
            if n < 2 {
                ctx.continue_as_new((n + 1).to_string()).await
            } else {
                Ok("done".to_string())
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-all-can", "CANKV", "0").await.unwrap();
    client
        .wait_for_orchestration("kv-all-can", Duration::from_secs(5))
        .await
        .unwrap();

    // All keys from all executions should be present (instance-scoped KV)
    let all = client.get_kv_all_values("kv-all-can").await.unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all.get("key_0").map(String::as_str), Some("val_0"));
    assert_eq!(all.get("key_1").map(String::as_str), Some("val_1"));
    assert_eq!(all.get("key_2").map(String::as_str), Some("val_2"));

    rt.shutdown(None).await;
}

// =============================================================================
// prune_kv_values_updated_before
// =============================================================================

/// prune_kv_values_updated_before removes old keys loaded from the snapshot
/// and leaves current-turn keys intact.
///
/// Strategy: exec 1 sets keys "old1" and "old2". ContinueAsNew to exec 2 which
/// inherits the snapshot with timestamps from exec 1, then sets a new key and
/// prunes old entries by timestamp.
#[tokio::test]
async fn kv_prune_old_values_by_timestamp() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("PruneTS", |ctx: OrchestrationContext, input: String| async move {
            let n: u32 = input.parse().unwrap_or(0);
            if n == 0 {
                // Exec 1: set timestamped keys, then CAN
                ctx.set_kv_value("old1", "v1");
                ctx.set_kv_value("old2", "v2");
                ctx.continue_as_new("1".to_string()).await
            } else {
                // Exec 2: snapshot has old1, old2 with their original timestamps.
                // Sleep briefly so that "now" is definitely after the snapshot timestamps.
                ctx.schedule_timer(Duration::from_millis(50)).await;

                // Capture the cutoff BEFORE setting "fresh"
                let now = ctx
                    .utc_now()
                    .await
                    .map_err(|e| e.to_string())?
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                // Set a new key AFTER the cutoff — its timestamp will be > now
                ctx.set_kv_value("fresh", "v3");

                // Prune everything updated before the cutoff (should catch old1 + old2)
                let removed = ctx.prune_kv_values_updated_before(now);

                // old1 and old2 should be pruned; fresh should survive
                Ok(format!("removed:{removed}"))
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client.start_orchestration("kv-prune-ts", "PruneTS", "0").await.unwrap();

    let status = client
        .wait_for_orchestration("kv-prune-ts", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "removed:2"),
        OrchestrationStatus::Failed { details, .. } => panic!("failed: {}", details.display_message()),
        _ => panic!("unexpected status"),
    }

    // After completion: old1 and old2 should be cleared, fresh should exist
    assert_eq!(client.get_kv_value("kv-prune-ts", "old1").await.unwrap(), None);
    assert_eq!(client.get_kv_value("kv-prune-ts", "old2").await.unwrap(), None);
    assert_eq!(
        client.get_kv_value("kv-prune-ts", "fresh").await.unwrap(),
        Some("v3".to_string())
    );

    rt.shutdown(None).await;
}

/// prune_kv_values_updated_before with threshold 0 does nothing (all timestamps >= 0).
#[tokio::test]
async fn kv_prune_with_zero_threshold_does_nothing() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("PruneZero", |ctx: OrchestrationContext, input: String| async move {
            let n: u32 = input.parse().unwrap_or(0);
            if n == 0 {
                ctx.set_kv_value("a", "1");
                ctx.set_kv_value("b", "2");
                ctx.continue_as_new("1".to_string()).await
            } else {
                // Prune with threshold 0 — nothing should be prunable
                let removed = ctx.prune_kv_values_updated_before(0);
                let a = ctx.get_kv_value("a").unwrap_or_default();
                let b = ctx.get_kv_value("b").unwrap_or_default();
                Ok(format!("removed:{removed},a:{a},b:{b}"))
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-prune-0", "PruneZero", "0")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("kv-prune-0", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "removed:0,a:1,b:2");
        }
        OrchestrationStatus::Failed { details, .. } => panic!("failed: {}", details.display_message()),
        _ => panic!("unexpected status"),
    }

    rt.shutdown(None).await;
}

/// prune_kv_values_updated_before on an empty KV store returns 0.
#[tokio::test]
async fn kv_prune_empty_returns_zero() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("PruneEmpty", |ctx: OrchestrationContext, _: String| async move {
            let removed = ctx.prune_kv_values_updated_before(u64::MAX);
            Ok(format!("removed:{removed}"))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-prune-empty", "PruneEmpty", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("kv-prune-empty", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "removed:0");
        }
        OrchestrationStatus::Failed { details, .. } => panic!("failed: {}", details.display_message()),
        _ => panic!("unexpected status"),
    }

    rt.shutdown(None).await;
}

/// prune_kv_values_updated_before with u64::MAX prunes everything, including current-turn keys.
#[tokio::test]
async fn kv_prune_current_turn_keys_with_max_threshold() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("PruneMax", |ctx: OrchestrationContext, _: String| async move {
            ctx.set_kv_value("x", "1");
            ctx.set_kv_value("y", "2");
            let removed = ctx.prune_kv_values_updated_before(u64::MAX);
            let x = ctx.get_kv_value("x").unwrap_or("gone".to_string());
            let y = ctx.get_kv_value("y").unwrap_or("gone".to_string());
            Ok(format!("removed:{removed},x:{x},y:{y}"))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-prune-max", "PruneMax", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("kv-prune-max", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "removed:2,x:gone,y:gone");
        }
        OrchestrationStatus::Failed { details, .. } => panic!("failed: {}", details.display_message()),
        _ => panic!("unexpected status"),
    }

    rt.shutdown(None).await;
}

/// prune_kv_values_updated_before with a wall-clock cutoff prunes keys set in the
/// same turn that haven't been persisted yet.
#[tokio::test]
async fn kv_prune_unpersisted_current_turn_key_with_wall_clock() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("PruneWall", |ctx: OrchestrationContext, _: String| async move {
            ctx.set_kv_value("ephemeral", "will_be_pruned");

            // HACK: Using std::time::SystemTime::now() inside an orchestration is
            // non-deterministic and must NEVER be done in real code — use ctx.utc_now()
            // instead. We use it here only to get a wall-clock timestamp that is
            // guaranteed to be >= the key's timestamp, proving that unpersisted
            // current-turn keys are prunable by real timestamps.
            std::thread::sleep(Duration::from_millis(1));
            let wall_now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let removed = ctx.prune_kv_values_updated_before(wall_now);
            let val = ctx.get_kv_value("ephemeral").unwrap_or("gone".to_string());
            Ok(format!("removed:{removed},ephemeral:{val}"))
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-prune-wall", "PruneWall", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("kv-prune-wall", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "removed:1,ephemeral:gone");
        }
        OrchestrationStatus::Failed { details, .. } => panic!("failed: {}", details.display_message()),
        _ => panic!("unexpected status"),
    }

    // Key should not exist in the provider either
    assert_eq!(client.get_kv_value("kv-prune-wall", "ephemeral").await.unwrap(), None);

    rt.shutdown(None).await;
}

/// prune_kv_values_updated_before only removes keys older than the threshold,
/// keeping keys set later (in previous CAN executions that are still "recent").
#[tokio::test]
async fn kv_prune_selective_by_age() {
    let store = Arc::new(
        duroxide::providers::sqlite::SqliteProvider::new_in_memory()
            .await
            .unwrap(),
    );
    let activities = ActivityRegistry::builder().build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("PruneSel", |ctx: OrchestrationContext, input: String| async move {
            let parts: Vec<&str> = input.splitn(2, ':').collect();
            let phase: u32 = parts[0].parse().unwrap_or(0);
            match phase {
                0 => {
                    // Exec 1: set "old_key" then CAN
                    ctx.set_kv_value("old_key", "old_val");
                    ctx.continue_as_new("1".to_string()).await
                }
                1 => {
                    // Exec 2: wait a bit, then record boundary, set "recent_key", then CAN
                    ctx.schedule_timer(Duration::from_millis(50)).await;
                    let boundary = ctx
                        .utc_now()
                        .await
                        .map_err(|e| e.to_string())?
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    ctx.set_kv_value("recent_key", "recent_val");
                    ctx.continue_as_new(format!("2:{boundary}")).await
                }
                _ => {
                    // Exec 3: prune using the boundary from exec 2
                    let boundary: u64 = parts[1].parse().unwrap();
                    ctx.schedule_timer(Duration::from_millis(10)).await;
                    let removed = ctx.prune_kv_values_updated_before(boundary);

                    // "old_key" was set in exec 1 (before boundary) → pruned
                    // "recent_key" was set in exec 2 (after boundary) → kept
                    let old = ctx.get_kv_value("old_key").unwrap_or("gone".to_string());
                    let recent = ctx.get_kv_value("recent_key").unwrap_or("gone".to_string());
                    Ok(format!("removed:{removed},old:{old},recent:{recent}"))
                }
            }
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("kv-prune-sel", "PruneSel", "0")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("kv-prune-sel", Duration::from_secs(10))
        .await
        .unwrap();
    match status {
        OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "removed:1,old:gone,recent:recent_val");
        }
        OrchestrationStatus::Failed { details, .. } => panic!("failed: {}", details.display_message()),
        _ => panic!("unexpected status"),
    }

    rt.shutdown(None).await;
}

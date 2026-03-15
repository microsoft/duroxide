//! Provider validation tests for the KV store.
//!
//! These tests validate that a Provider implementation correctly handles
//! KV events (KeyValueSet, KeyValueCleared, KeyValuesCleared) during ack,
//! materializes them in the KV store table, and supports get_kv_value.
//! Snapshot correctness is verified through fetch_orchestration_item.

use crate::EventKind;
use crate::provider_validation::{Event, ExecutionMetadata, create_instance};
use crate::provider_validations::ProviderFactory;
use crate::providers::{PruneOptions, WorkItem};
use std::time::Duration;

/// Helper to enqueue an ExternalRaised message to trigger a fetch cycle.
fn poke_item(instance: &str) -> WorkItem {
    WorkItem::ExternalRaised {
        instance: instance.to_string(),
        name: "poke".to_string(),
        data: "{}".to_string(),
    }
}

/// Helper: enqueue → fetch → ack with given history delta.
async fn ack_with_delta(
    provider: &dyn crate::providers::Provider,
    instance: &str,
    execution_id: u64,
    history_delta: Vec<Event>,
) {
    provider
        .enqueue_for_orchestrator(poke_item(instance), None)
        .await
        .unwrap();

    let (_, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    provider
        .ack_orchestration_item(
            &lock_token,
            execution_id,
            history_delta,
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// Set and get KV values
// =============================================================================

/// Acking with KeyValueSet writes the value and get_kv_value returns it.
pub async fn test_kv_set_and_get<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-set").await.unwrap();

    ack_with_delta(
        &*provider,
        "kv-set",
        1,
        vec![Event::with_event_id(
            100,
            "kv-set",
            1,
            None,
            EventKind::KeyValueSet {
                key: "counter".to_string(),
                value: "42".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    let val = provider.get_kv_value("kv-set", "counter").await.unwrap();
    assert_eq!(val, Some("42".to_string()));
}

// =============================================================================
// Overwrite existing key
// =============================================================================

/// Acking with KeyValueSet for an existing key overwrites the value.
pub async fn test_kv_overwrite<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-over").await.unwrap();

    // Set initial value
    ack_with_delta(
        &*provider,
        "kv-over",
        1,
        vec![Event::with_event_id(
            100,
            "kv-over",
            1,
            None,
            EventKind::KeyValueSet {
                key: "status".to_string(),
                value: "old".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // Verify initial value was set
    let val = provider.get_kv_value("kv-over", "status").await.unwrap();
    assert_eq!(val, Some("old".to_string()), "initial value should be 'old'");

    // Overwrite
    ack_with_delta(
        &*provider,
        "kv-over",
        1,
        vec![Event::with_event_id(
            101,
            "kv-over",
            1,
            None,
            EventKind::KeyValueSet {
                key: "status".to_string(),
                value: "new".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    let val = provider.get_kv_value("kv-over", "status").await.unwrap();
    assert_eq!(val, Some("new".to_string()));
}

// =============================================================================
// Clear single key
// =============================================================================

/// Acking with KeyValueCleared removes the specified key.
pub async fn test_kv_clear_single<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-clr1").await.unwrap();

    // Set a value
    ack_with_delta(
        &*provider,
        "kv-clr1",
        1,
        vec![Event::with_event_id(
            100,
            "kv-clr1",
            1,
            None,
            EventKind::KeyValueSet {
                key: "remove_me".to_string(),
                value: "x".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // Verify value was set
    let val = provider.get_kv_value("kv-clr1", "remove_me").await.unwrap();
    assert_eq!(val, Some("x".to_string()), "value should be set before clearing");

    // Clear it
    ack_with_delta(
        &*provider,
        "kv-clr1",
        1,
        vec![Event::with_event_id(
            101,
            "kv-clr1",
            1,
            None,
            EventKind::KeyValueCleared {
                key: "remove_me".to_string(),
            },
        )],
    )
    .await;

    let val = provider.get_kv_value("kv-clr1", "remove_me").await.unwrap();
    assert_eq!(val, None);
}

// =============================================================================
// Clear all keys
// =============================================================================

/// Acking with KeyValuesCleared removes all keys for the instance.
pub async fn test_kv_clear_all<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-clra").await.unwrap();

    // Set multiple values
    ack_with_delta(
        &*provider,
        "kv-clra",
        1,
        vec![
            Event::with_event_id(
                100,
                "kv-clra",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "a".to_string(),
                    value: "1".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
            Event::with_event_id(
                101,
                "kv-clra",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "b".to_string(),
                    value: "2".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
        ],
    )
    .await;

    // Verify values were set
    assert_eq!(
        provider.get_kv_value("kv-clra", "a").await.unwrap(),
        Some("1".to_string()),
        "key 'a' should be set"
    );
    assert_eq!(
        provider.get_kv_value("kv-clra", "b").await.unwrap(),
        Some("2".to_string()),
        "key 'b' should be set"
    );

    // Clear all
    ack_with_delta(
        &*provider,
        "kv-clra",
        1,
        vec![Event::with_event_id(
            102,
            "kv-clra",
            1,
            None,
            EventKind::KeyValuesCleared,
        )],
    )
    .await;

    assert_eq!(provider.get_kv_value("kv-clra", "a").await.unwrap(), None);
    assert_eq!(provider.get_kv_value("kv-clra", "b").await.unwrap(), None);
}

// =============================================================================
// Get nonexistent key
// =============================================================================

/// get_kv_value returns None for a key that doesn't exist.
pub async fn test_kv_get_nonexistent<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-none").await.unwrap();

    let val = provider.get_kv_value("kv-none", "nope").await.unwrap();
    assert_eq!(val, None);
}

// =============================================================================
// KV snapshot loaded in fetch_orchestration_item
// =============================================================================

/// After set, the next fetch_orchestration_item returns the full KV snapshot.
pub async fn test_kv_snapshot_in_fetch<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-fetch").await.unwrap();

    // Set multiple values in execution 1
    ack_with_delta(
        &*provider,
        "kv-fetch",
        1,
        vec![
            Event::with_event_id(
                100,
                "kv-fetch",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "x".to_string(),
                    value: "10".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
            Event::with_event_id(
                101,
                "kv-fetch",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "y".to_string(),
                    value: "20".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
        ],
    )
    .await;

    // Complete execution 1 so values merge into kv_store
    complete_instance(&*provider, "kv-fetch", 1).await;

    // Enqueue another poke to trigger fetch for a new turn
    provider
        .enqueue_for_orchestrator(poke_item("kv-fetch"), None)
        .await
        .unwrap();

    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    // Snapshot contains values from the completed execution (now in kv_store)
    assert_eq!(item.kv_snapshot.len(), 2);
    assert_eq!(item.kv_snapshot.get("x").map(|e| &*e.value), Some("10"));
    assert_eq!(item.kv_snapshot.get("y").map(|e| &*e.value), Some("20"));

    // Clean up: ack without changes
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// KV snapshot reflects cleared single key
// =============================================================================

/// After clearing a single key and completing execution, fetch snapshot omits it.
pub async fn test_kv_snapshot_after_clear_single<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-fclr1").await.unwrap();

    // Set two keys
    ack_with_delta(
        &*provider,
        "kv-fclr1",
        1,
        vec![
            Event::with_event_id(
                100,
                "kv-fclr1",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "keep".to_string(),
                    value: "yes".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
            Event::with_event_id(
                101,
                "kv-fclr1",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "remove".to_string(),
                    value: "bye".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
        ],
    )
    .await;

    // Clear one key
    ack_with_delta(
        &*provider,
        "kv-fclr1",
        1,
        vec![Event::with_event_id(
            102,
            "kv-fclr1",
            1,
            None,
            EventKind::KeyValueCleared {
                key: "remove".to_string(),
            },
        )],
    )
    .await;

    // Complete execution so delta merges into kv_store
    complete_instance(&*provider, "kv-fclr1", 1).await;

    // Fetch and verify snapshot
    provider
        .enqueue_for_orchestrator(poke_item("kv-fclr1"), None)
        .await
        .unwrap();

    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    assert_eq!(item.kv_snapshot.len(), 1);
    assert_eq!(item.kv_snapshot.get("keep").map(|e| &*e.value), Some("yes"));
    assert_eq!(item.kv_snapshot.get("remove"), None);

    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// KV snapshot reflects clear-all
// =============================================================================

/// After clearing all keys, fetch_orchestration_item returns an empty snapshot.
pub async fn test_kv_snapshot_after_clear_all<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-fclra").await.unwrap();

    // Set values
    ack_with_delta(
        &*provider,
        "kv-fclra",
        1,
        vec![
            Event::with_event_id(
                100,
                "kv-fclra",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "a".to_string(),
                    value: "1".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
            Event::with_event_id(
                101,
                "kv-fclra",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "b".to_string(),
                    value: "2".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
        ],
    )
    .await;

    // Clear all
    ack_with_delta(
        &*provider,
        "kv-fclra",
        1,
        vec![Event::with_event_id(
            102,
            "kv-fclra",
            1,
            None,
            EventKind::KeyValuesCleared,
        )],
    )
    .await;

    // Fetch and verify snapshot is empty
    provider
        .enqueue_for_orchestrator(poke_item("kv-fclra"), None)
        .await
        .unwrap();

    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    assert!(
        item.kv_snapshot.is_empty(),
        "expected empty snapshot, got: {:?}",
        item.kv_snapshot
    );

    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// Execution ID tracking (last-writer-wins for pruning)
// =============================================================================

/// When a later execution overwrites a key, the execution_id is updated.
/// Pruning the earlier execution should NOT delete the key because the
/// last-writer execution still exists.
pub async fn test_kv_execution_id_tracking<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    create_instance(&*provider, "kv-exec").await.unwrap();

    // Set a key in execution 1
    ack_with_delta(
        &*provider,
        "kv-exec",
        1,
        vec![Event::with_event_id(
            100,
            "kv-exec",
            1,
            None,
            EventKind::KeyValueSet {
                key: "shared".to_string(),
                value: "from_exec_1".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    let val = provider.get_kv_value("kv-exec", "shared").await.unwrap();
    assert_eq!(val, Some("from_exec_1".to_string()));

    // ContinueAsNew → exec 2: overwrite the same key, updating execution_id
    continue_as_new(&*provider, "kv-exec", 2).await;
    ack_with_delta(
        &*provider,
        "kv-exec",
        2,
        vec![Event::with_event_id(
            200,
            "kv-exec",
            2,
            None,
            EventKind::KeyValueSet {
                key: "shared".to_string(),
                value: "from_exec_2".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    let val = provider.get_kv_value("kv-exec", "shared").await.unwrap();
    assert_eq!(val, Some("from_exec_2".to_string()), "should reflect exec 2 value");

    // Prune exec 1 — key must survive because exec 2 is the last writer
    mgmt.prune_executions(
        "kv-exec",
        PruneOptions {
            keep_last: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let val = provider.get_kv_value("kv-exec", "shared").await.unwrap();
    assert_eq!(
        val,
        Some("from_exec_2".to_string()),
        "key must survive pruning because last-writer (exec 2) is not pruned",
    );
}

// =============================================================================
// Cross-execution: set in exec 1, read after exec 2 overwrites
// =============================================================================

/// KV set in exec 1 is readable. Overwriting in exec 2 returns the new value.
pub async fn test_kv_cross_execution_overwrite<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-xexec").await.unwrap();

    // Set in execution 1
    ack_with_delta(
        &*provider,
        "kv-xexec",
        1,
        vec![Event::with_event_id(
            100,
            "kv-xexec",
            1,
            None,
            EventKind::KeyValueSet {
                key: "k".to_string(),
                value: "v1".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    let val = provider.get_kv_value("kv-xexec", "k").await.unwrap();
    assert_eq!(val, Some("v1".to_string()), "should see value from exec 1");

    // ContinueAsNew → exec 2, overwrite same key
    continue_as_new(&*provider, "kv-xexec", 2).await;
    ack_with_delta(
        &*provider,
        "kv-xexec",
        2,
        vec![Event::with_event_id(
            200,
            "kv-xexec",
            2,
            None,
            EventKind::KeyValueSet {
                key: "k".to_string(),
                value: "v2".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    let val = provider.get_kv_value("kv-xexec", "k").await.unwrap();
    assert_eq!(val, Some("v2".to_string()), "exec 2 should overwrite exec 1 value");
}

// =============================================================================
// Cross-execution: set in exec 1, removed in exec 2, re-added in exec 3
// =============================================================================

/// Key set in exec 1, cleared in exec 2, re-set in exec 3 returns the exec 3 value.
pub async fn test_kv_cross_execution_remove_readd<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-xrm").await.unwrap();

    // Exec 1: set key
    ack_with_delta(
        &*provider,
        "kv-xrm",
        1,
        vec![Event::with_event_id(
            100,
            "kv-xrm",
            1,
            None,
            EventKind::KeyValueSet {
                key: "cycle".to_string(),
                value: "exec1".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;
    assert_eq!(
        provider.get_kv_value("kv-xrm", "cycle").await.unwrap(),
        Some("exec1".to_string()),
    );

    // ContinueAsNew → exec 2: clear the key
    continue_as_new(&*provider, "kv-xrm", 2).await;
    ack_with_delta(
        &*provider,
        "kv-xrm",
        2,
        vec![Event::with_event_id(
            200,
            "kv-xrm",
            2,
            None,
            EventKind::KeyValueCleared {
                key: "cycle".to_string(),
            },
        )],
    )
    .await;
    assert_eq!(
        provider.get_kv_value("kv-xrm", "cycle").await.unwrap(),
        None,
        "key should be cleared in exec 2",
    );

    // ContinueAsNew → exec 3: re-add key
    continue_as_new(&*provider, "kv-xrm", 3).await;
    ack_with_delta(
        &*provider,
        "kv-xrm",
        3,
        vec![Event::with_event_id(
            300,
            "kv-xrm",
            3,
            None,
            EventKind::KeyValueSet {
                key: "cycle".to_string(),
                value: "exec3".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;
    assert_eq!(
        provider.get_kv_value("kv-xrm", "cycle").await.unwrap(),
        Some("exec3".to_string()),
        "key should be re-set from exec 3",
    );
}

// =============================================================================
// Pruning: prune old execution preserves keys overwritten by newer execution
// =============================================================================

/// Key set in exec 1 and overwritten in exec 2. Pruning exec 1 must NOT
/// delete the key because exec 2 now owns it.
pub async fn test_kv_prune_preserves_overwritten<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    create_instance(&*provider, "kv-prn1").await.unwrap();

    // Exec 1: set key
    ack_with_delta(
        &*provider,
        "kv-prn1",
        1,
        vec![Event::with_event_id(
            100,
            "kv-prn1",
            1,
            None,
            EventKind::KeyValueSet {
                key: "survive".to_string(),
                value: "v1".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // ContinueAsNew → exec 2: overwrite
    continue_as_new(&*provider, "kv-prn1", 2).await;
    ack_with_delta(
        &*provider,
        "kv-prn1",
        2,
        vec![Event::with_event_id(
            200,
            "kv-prn1",
            2,
            None,
            EventKind::KeyValueSet {
                key: "survive".to_string(),
                value: "v2".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // Prune exec 1
    mgmt.prune_executions(
        "kv-prn1",
        PruneOptions {
            keep_last: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Key must still exist with exec 2's value
    let val = provider.get_kv_value("kv-prn1", "survive").await.unwrap();
    assert_eq!(
        val,
        Some("v2".to_string()),
        "overwritten key must survive pruning of old execution"
    );
}

// =============================================================================
// Pruning: KV entries survive execution pruning (instance-scoped lifetime)
// =============================================================================

/// Key set only in exec 1 (orphan). After pruning exec 1, the key should SURVIVE
/// because KV lifetime is tied to the instance, not to individual executions.
pub async fn test_kv_prune_preserves_all_keys<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    create_instance(&*provider, "kv-prn2").await.unwrap();

    // Exec 1: set a key
    ack_with_delta(
        &*provider,
        "kv-prn2",
        1,
        vec![Event::with_event_id(
            100,
            "kv-prn2",
            1,
            None,
            EventKind::KeyValueSet {
                key: "orphan".to_string(),
                value: "survives".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // ContinueAsNew → exec 2: set a different key
    continue_as_new(&*provider, "kv-prn2", 2).await;
    ack_with_delta(
        &*provider,
        "kv-prn2",
        2,
        vec![Event::with_event_id(
            200,
            "kv-prn2",
            2,
            None,
            EventKind::KeyValueSet {
                key: "keeper".to_string(),
                value: "alive".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // Verify both exist before prune
    assert_eq!(
        provider.get_kv_value("kv-prn2", "orphan").await.unwrap(),
        Some("survives".to_string()),
    );
    assert_eq!(
        provider.get_kv_value("kv-prn2", "keeper").await.unwrap(),
        Some("alive".to_string()),
    );

    // Prune exec 1
    mgmt.prune_executions(
        "kv-prn2",
        PruneOptions {
            keep_last: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Both keys survive — KV is instance-scoped, not execution-scoped
    assert_eq!(
        provider.get_kv_value("kv-prn2", "orphan").await.unwrap(),
        Some("survives".to_string()),
        "KV entries must survive execution pruning (instance-scoped lifetime)",
    );
    assert_eq!(
        provider.get_kv_value("kv-prn2", "keeper").await.unwrap(),
        Some("alive".to_string()),
        "exec 2 key must survive",
    );
}

// =============================================================================
// Instance isolation: KV from one instance not visible to another
// =============================================================================

/// Two instances with the same key name have independent values.
pub async fn test_kv_instance_isolation<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-iso-a").await.unwrap();
    create_instance(&*provider, "kv-iso-b").await.unwrap();

    ack_with_delta(
        &*provider,
        "kv-iso-a",
        1,
        vec![Event::with_event_id(
            100,
            "kv-iso-a",
            1,
            None,
            EventKind::KeyValueSet {
                key: "shared_name".to_string(),
                value: "from_a".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    ack_with_delta(
        &*provider,
        "kv-iso-b",
        1,
        vec![Event::with_event_id(
            100,
            "kv-iso-b",
            1,
            None,
            EventKind::KeyValueSet {
                key: "shared_name".to_string(),
                value: "from_b".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    assert_eq!(
        provider.get_kv_value("kv-iso-a", "shared_name").await.unwrap(),
        Some("from_a".to_string()),
    );
    assert_eq!(
        provider.get_kv_value("kv-iso-b", "shared_name").await.unwrap(),
        Some("from_b".to_string()),
    );
}

// =============================================================================
// Delete instance cascades KV cleanup
// =============================================================================

/// Deleting an instance removes all its KV entries.
pub async fn test_kv_delete_instance_cascades<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    create_instance(&*provider, "kv-del").await.unwrap();

    ack_with_delta(
        &*provider,
        "kv-del",
        1,
        vec![Event::with_event_id(
            100,
            "kv-del",
            1,
            None,
            EventKind::KeyValueSet {
                key: "doomed".to_string(),
                value: "bye".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // Complete the instance so it can be deleted without force
    complete_instance(&*provider, "kv-del", 1).await;

    assert_eq!(
        provider.get_kv_value("kv-del", "doomed").await.unwrap(),
        Some("bye".to_string()),
        "value should exist before deletion",
    );

    mgmt.delete_instance("kv-del", false).await.unwrap();

    assert_eq!(
        provider.get_kv_value("kv-del", "doomed").await.unwrap(),
        None,
        "KV should be gone after instance deletion",
    );
}

// =============================================================================
// Clear nonexistent key is idempotent
// =============================================================================

/// Acking with KeyValueCleared for a key that was never set does not error.
pub async fn test_kv_clear_nonexistent_key<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-clrne").await.unwrap();

    // Clear a key that was never set — should not error
    ack_with_delta(
        &*provider,
        "kv-clrne",
        1,
        vec![Event::with_event_id(
            100,
            "kv-clrne",
            1,
            None,
            EventKind::KeyValueCleared {
                key: "never_existed".to_string(),
            },
        )],
    )
    .await;

    let val = provider.get_kv_value("kv-clrne", "never_existed").await.unwrap();
    assert_eq!(val, None);
}

// =============================================================================
// get_kv_value for unknown instance
// =============================================================================

/// get_kv_value returns None (not error) for an instance that doesn't exist.
pub async fn test_kv_get_unknown_instance<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    let val = provider.get_kv_value("no-such-instance", "key").await.unwrap();
    assert_eq!(val, None);
}

// =============================================================================
// Set after clear in same ack
// =============================================================================

/// Clear all, then set "X" in same ack — "X" should exist, old keys should not.
pub async fn test_kv_set_after_clear<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-sac").await.unwrap();

    // Set initial keys
    ack_with_delta(
        &*provider,
        "kv-sac",
        1,
        vec![
            Event::with_event_id(
                100,
                "kv-sac",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "old_a".to_string(),
                    value: "1".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
            Event::with_event_id(
                101,
                "kv-sac",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "old_b".to_string(),
                    value: "2".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
        ],
    )
    .await;

    // Clear all, then set new key — in same ack
    ack_with_delta(
        &*provider,
        "kv-sac",
        1,
        vec![
            Event::with_event_id(102, "kv-sac", 1, None, EventKind::KeyValuesCleared),
            Event::with_event_id(
                103,
                "kv-sac",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "new_x".to_string(),
                    value: "fresh".to_string(),
                    last_updated_at_ms: 0,
                },
            ),
        ],
    )
    .await;

    assert_eq!(provider.get_kv_value("kv-sac", "old_a").await.unwrap(), None);
    assert_eq!(provider.get_kv_value("kv-sac", "old_b").await.unwrap(), None);
    assert_eq!(
        provider.get_kv_value("kv-sac", "new_x").await.unwrap(),
        Some("fresh".to_string()),
    );
}

// =============================================================================
// Empty value
// =============================================================================

/// Setting a key to "" (empty string) returns Some(""), not None.
pub async fn test_kv_empty_value<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-empty").await.unwrap();

    ack_with_delta(
        &*provider,
        "kv-empty",
        1,
        vec![Event::with_event_id(
            100,
            "kv-empty",
            1,
            None,
            EventKind::KeyValueSet {
                key: "blank".to_string(),
                value: "".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    let val = provider.get_kv_value("kv-empty", "blank").await.unwrap();
    assert_eq!(val, Some("".to_string()), "empty string is a valid value, not None");
}

// =============================================================================
// Large value
// =============================================================================

/// A key with a 64KB value should be stored and retrieved correctly.
pub async fn test_kv_large_value<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-big").await.unwrap();

    let big_val = "x".repeat(16 * 1024);
    ack_with_delta(
        &*provider,
        "kv-big",
        1,
        vec![Event::with_event_id(
            100,
            "kv-big",
            1,
            None,
            EventKind::KeyValueSet {
                key: "payload".to_string(),
                value: big_val.clone(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    let val = provider.get_kv_value("kv-big", "payload").await.unwrap();
    assert_eq!(val.as_deref(), Some(big_val.as_str()));
}

// =============================================================================
// Special characters in key
// =============================================================================

/// Keys with spaces, unicode, dots, and slashes should all work.
pub async fn test_kv_special_chars_in_key<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-chars").await.unwrap();

    let keys = vec![
        ("key with spaces", "v1"),
        ("日本語キー", "v2"),
        ("dotted.key.name", "v3"),
        ("path/like/key", "v4"),
        ("emoji🎉key", "v5"),
    ];

    let events: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, (k, v))| {
            Event::with_event_id(
                100 + i as u64,
                "kv-chars",
                1,
                None,
                EventKind::KeyValueSet {
                    key: k.to_string(),
                    value: v.to_string(),
                    last_updated_at_ms: 0,
                },
            )
        })
        .collect();

    ack_with_delta(&*provider, "kv-chars", 1, events).await;

    for (k, v) in &keys {
        let val = provider.get_kv_value("kv-chars", k).await.unwrap();
        assert_eq!(val, Some(v.to_string()), "key '{k}' should be retrievable");
    }
}

// =============================================================================
// Snapshot empty
// =============================================================================

/// get_kv_snapshot (via fetch) returns empty HashMap for instance with no KV.
pub async fn test_kv_snapshot_empty<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-sempty").await.unwrap();

    // Fetch without setting any KV
    provider
        .enqueue_for_orchestrator(poke_item("kv-sempty"), None)
        .await
        .unwrap();

    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    assert!(
        item.kv_snapshot.is_empty(),
        "expected empty snapshot for fresh instance"
    );

    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// Snapshot cross execution
// =============================================================================

/// Keys from exec 1 and exec 2 both appear in snapshot.
pub async fn test_kv_snapshot_cross_execution<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-scross").await.unwrap();

    // Exec 1: set "A"
    ack_with_delta(
        &*provider,
        "kv-scross",
        1,
        vec![Event::with_event_id(
            100,
            "kv-scross",
            1,
            None,
            EventKind::KeyValueSet {
                key: "A".to_string(),
                value: "from_exec1".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // ContinueAsNew → exec 2 (merges exec 1 delta into kv_store): set "B"
    continue_as_new(&*provider, "kv-scross", 2).await;
    ack_with_delta(
        &*provider,
        "kv-scross",
        2,
        vec![Event::with_event_id(
            200,
            "kv-scross",
            2,
            None,
            EventKind::KeyValueSet {
                key: "B".to_string(),
                value: "from_exec2".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // Fetch and check snapshot:
    // "A" is in kv_store (merged at CAN boundary) → in snapshot
    // "B" is in kv_delta (current execution) → NOT in snapshot
    provider
        .enqueue_for_orchestrator(poke_item("kv-scross"), None)
        .await
        .unwrap();

    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    assert_eq!(
        item.kv_snapshot.len(),
        1,
        "snapshot should only contain prior-execution state"
    );
    assert_eq!(item.kv_snapshot.get("A").map(|e| &*e.value), Some("from_exec1"));
    // B is in kv_delta, not in snapshot — it will be rebuilt during replay
    assert_eq!(
        item.kv_snapshot.get("B"),
        None,
        "current-execution key should not be in snapshot"
    );

    provider
        .ack_orchestration_item(
            &lock_token,
            2,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// Prune current execution protected
// =============================================================================

/// Pruning with keep_last=1 on a single-execution instance preserves KV.
pub async fn test_kv_prune_current_execution_protected<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    create_instance(&*provider, "kv-prncur").await.unwrap();

    ack_with_delta(
        &*provider,
        "kv-prncur",
        1,
        vec![Event::with_event_id(
            100,
            "kv-prncur",
            1,
            None,
            EventKind::KeyValueSet {
                key: "alive".to_string(),
                value: "yes".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // Prune with keep_last=1 — only execution exists, so nothing pruned
    mgmt.prune_executions(
        "kv-prncur",
        PruneOptions {
            keep_last: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let val = provider.get_kv_value("kv-prncur", "alive").await.unwrap();
    assert_eq!(
        val,
        Some("yes".to_string()),
        "current execution KV must survive pruning"
    );
}

// =============================================================================
// Delete instance with children cascades KV
// =============================================================================

/// Deleting a parent instance also removes child instance KV.
pub async fn test_kv_delete_instance_with_children<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    // Create parent instance
    create_instance(&*provider, "kv-parent").await.unwrap();

    // Set KV on parent
    ack_with_delta(
        &*provider,
        "kv-parent",
        1,
        vec![Event::with_event_id(
            100,
            "kv-parent",
            1,
            None,
            EventKind::KeyValueSet {
                key: "parent_key".to_string(),
                value: "parent_val".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // Create child instance with parent reference — use StartOrchestration with parent_id set
    let child_start = WorkItem::StartOrchestration {
        instance: "kv-child".to_string(),
        orchestration: "TestOrch".to_string(),
        version: Some("1.0.0".to_string()),
        input: "{}".to_string(),
        parent_instance: Some("kv-parent".to_string()),
        parent_id: Some(1),
        execution_id: crate::INITIAL_EXECUTION_ID,
    };
    provider.enqueue_for_orchestrator(child_start, None).await.unwrap();

    let (_, child_lock, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected child orchestration item");

    provider
        .ack_orchestration_item(
            &child_lock,
            1,
            vec![Event::with_event_id(
                1,
                "kv-child",
                1,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: Some("kv-parent".to_string()),
                    parent_id: Some(1),
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                parent_instance_id: Some("kv-parent".to_string()),
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // Set KV on child
    ack_with_delta(
        &*provider,
        "kv-child",
        1,
        vec![Event::with_event_id(
            100,
            "kv-child",
            1,
            None,
            EventKind::KeyValueSet {
                key: "child_key".to_string(),
                value: "child_val".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // Complete both so they can be deleted
    complete_instance(&*provider, "kv-child", 1).await;
    complete_instance(&*provider, "kv-parent", 1).await;

    // Delete parent — cascades to child
    mgmt.delete_instance("kv-parent", false).await.unwrap();

    assert_eq!(
        provider.get_kv_value("kv-parent", "parent_key").await.unwrap(),
        None,
        "parent KV should be removed",
    );
    assert_eq!(
        provider.get_kv_value("kv-child", "child_key").await.unwrap(),
        None,
        "child KV should be removed when parent is deleted",
    );
}

// =============================================================================
// Clear isolation between instances
// =============================================================================

/// Clearing KV on one instance does not affect another.
pub async fn test_kv_clear_isolation<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-ciso-a").await.unwrap();
    create_instance(&*provider, "kv-ciso-b").await.unwrap();

    // Set same key on both
    ack_with_delta(
        &*provider,
        "kv-ciso-a",
        1,
        vec![Event::with_event_id(
            100,
            "kv-ciso-a",
            1,
            None,
            EventKind::KeyValueSet {
                key: "shared".to_string(),
                value: "from_a".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    ack_with_delta(
        &*provider,
        "kv-ciso-b",
        1,
        vec![Event::with_event_id(
            100,
            "kv-ciso-b",
            1,
            None,
            EventKind::KeyValueSet {
                key: "shared".to_string(),
                value: "from_b".to_string(),
                last_updated_at_ms: 0,
            },
        )],
    )
    .await;

    // Clear all on instance A
    ack_with_delta(
        &*provider,
        "kv-ciso-a",
        1,
        vec![Event::with_event_id(
            101,
            "kv-ciso-a",
            1,
            None,
            EventKind::KeyValuesCleared,
        )],
    )
    .await;

    // Instance A cleared
    assert_eq!(provider.get_kv_value("kv-ciso-a", "shared").await.unwrap(), None);
    // Instance B unaffected
    assert_eq!(
        provider.get_kv_value("kv-ciso-b", "shared").await.unwrap(),
        Some("from_b".to_string()),
        "clearing instance A must not affect instance B",
    );
}

// =============================================================================
// KV Delta: Snapshot only contains prior-execution state
// =============================================================================

/// After multiple acks within the same execution, the snapshot returned by
/// fetch_orchestration_item must only contain values from PRIOR executions
/// (i.e., values that were merged into kv_store at execution completion).
/// Values set during the current (not-yet-completed) execution must NOT
/// appear in the snapshot — they'll be rebuilt by replay.
///
/// NOTE: This test describes the desired behavior after the kv_delta fix.
/// Under the current implementation it will FAIL because kv_store is written
/// every turn. This test is a specification for the kv_delta change.
pub async fn test_kv_delta_snapshot_excludes_current_execution<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-ds1").await.unwrap();

    // Turn 1: set counter=1 in execution 1 (not completed yet)
    ack_with_delta(
        &*provider,
        "kv-ds1",
        1,
        vec![Event::with_event_id(
            100,
            "kv-ds1",
            1,
            None,
            EventKind::KeyValueSet {
                key: "counter".to_string(),
                value: "1".to_string(),
                last_updated_at_ms: 100,
            },
        )],
    )
    .await;

    // Client read should see "1" (live state = kv_store + kv_delta)
    assert_eq!(
        provider.get_kv_value("kv-ds1", "counter").await.unwrap(),
        Some("1".to_string()),
        "client read should see current-execution value",
    );

    // Fetch snapshot — should NOT contain current-execution value
    provider
        .enqueue_for_orchestrator(poke_item("kv-ds1"), None)
        .await
        .unwrap();
    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    assert!(
        item.kv_snapshot.is_empty(),
        "snapshot must be empty — counter was set in current execution (not completed), \
         should not be in snapshot. Got: {:?}",
        item.kv_snapshot,
    );

    // Clean up
    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// KV Delta: Snapshot contains prior-execution values after CAN
// =============================================================================

/// After an execution completes (via CAN), its KV values are merged into
/// kv_store. The next execution's snapshot should see those values.
pub async fn test_kv_delta_snapshot_includes_completed_execution<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-ds2").await.unwrap();

    // Execution 1: set counter=5
    ack_with_delta(
        &*provider,
        "kv-ds2",
        1,
        vec![Event::with_event_id(
            100,
            "kv-ds2",
            1,
            None,
            EventKind::KeyValueSet {
                key: "counter".to_string(),
                value: "5".to_string(),
                last_updated_at_ms: 100,
            },
        )],
    )
    .await;

    // CAN → execution 2
    continue_as_new(&*provider, "kv-ds2", 2).await;

    // Fetch snapshot for execution 2 — should see counter=5 from exec 1
    provider
        .enqueue_for_orchestrator(poke_item("kv-ds2"), None)
        .await
        .unwrap();
    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    assert_eq!(
        item.kv_snapshot.get("counter").map(|e| &*e.value),
        Some("5"),
        "snapshot must contain prior-execution value after CAN",
    );

    provider
        .ack_orchestration_item(
            &lock_token,
            2,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// KV Delta: Client reads merge kv_store + kv_delta
// =============================================================================

/// Client reads must see the merged view: kv_store (prior execution) +
/// kv_delta (current execution mutations). Delta overrides store.
pub async fn test_kv_delta_client_reads_merged<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-dm").await.unwrap();

    // Execution 1: set color=red, size=large
    ack_with_delta(
        &*provider,
        "kv-dm",
        1,
        vec![
            Event::with_event_id(
                100,
                "kv-dm",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "color".to_string(),
                    value: "red".to_string(),
                    last_updated_at_ms: 100,
                },
            ),
            Event::with_event_id(
                101,
                "kv-dm",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "size".to_string(),
                    value: "large".to_string(),
                    last_updated_at_ms: 101,
                },
            ),
        ],
    )
    .await;

    // Complete execution 1 → values merge into kv_store
    complete_instance(&*provider, "kv-dm", 1).await;

    // Start execution 2 (simulate re-start, not CAN for simplicity)
    // Execution 2: overwrite color=blue, set new key shape=circle
    ack_with_delta(
        &*provider,
        "kv-dm",
        2,
        vec![
            Event::with_event_id(
                200,
                "kv-dm",
                2,
                None,
                EventKind::KeyValueSet {
                    key: "color".to_string(),
                    value: "blue".to_string(),
                    last_updated_at_ms: 200,
                },
            ),
            Event::with_event_id(
                201,
                "kv-dm",
                2,
                None,
                EventKind::KeyValueSet {
                    key: "shape".to_string(),
                    value: "circle".to_string(),
                    last_updated_at_ms: 201,
                },
            ),
        ],
    )
    .await;

    // Client reads:
    // color = "blue" (delta overrides store)
    assert_eq!(
        provider.get_kv_value("kv-dm", "color").await.unwrap(),
        Some("blue".to_string()),
        "delta should override kv_store value",
    );
    // size = "large" (from kv_store, no delta for this key)
    assert_eq!(
        provider.get_kv_value("kv-dm", "size").await.unwrap(),
        Some("large".to_string()),
        "kv_store value should be visible when no delta exists",
    );
    // shape = "circle" (only in delta)
    assert_eq!(
        provider.get_kv_value("kv-dm", "shape").await.unwrap(),
        Some("circle".to_string()),
        "delta-only key should be visible",
    );
}

// =============================================================================
// KV Delta: Tombstone overrides kv_store value
// =============================================================================

/// When a key exists in kv_store from a prior execution and the current
/// execution clears it, client reads must return None (tombstone wins).
pub async fn test_kv_delta_tombstone_overrides_store<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-dt").await.unwrap();

    // Execution 1: set key
    ack_with_delta(
        &*provider,
        "kv-dt",
        1,
        vec![Event::with_event_id(
            100,
            "kv-dt",
            1,
            None,
            EventKind::KeyValueSet {
                key: "temp".to_string(),
                value: "exists".to_string(),
                last_updated_at_ms: 100,
            },
        )],
    )
    .await;

    // Complete execution 1 → merges to kv_store
    complete_instance(&*provider, "kv-dt", 1).await;

    // Execution 2: clear the key
    ack_with_delta(
        &*provider,
        "kv-dt",
        2,
        vec![Event::with_event_id(
            200,
            "kv-dt",
            2,
            None,
            EventKind::KeyValueCleared {
                key: "temp".to_string(),
            },
        )],
    )
    .await;

    // Client read: should return None (tombstone overrides kv_store)
    assert_eq!(
        provider.get_kv_value("kv-dt", "temp").await.unwrap(),
        None,
        "tombstone in delta must override kv_store value",
    );
}

// =============================================================================
// KV Delta: clear_all tombstones all kv_store keys
// =============================================================================

/// clear_all_values in the current execution must hide all keys from kv_store.
pub async fn test_kv_delta_clear_all_tombstones_store<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-dca").await.unwrap();

    // Execution 1: set multiple keys
    ack_with_delta(
        &*provider,
        "kv-dca",
        1,
        vec![
            Event::with_event_id(
                100,
                "kv-dca",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "a".to_string(),
                    value: "1".to_string(),
                    last_updated_at_ms: 100,
                },
            ),
            Event::with_event_id(
                101,
                "kv-dca",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "b".to_string(),
                    value: "2".to_string(),
                    last_updated_at_ms: 101,
                },
            ),
        ],
    )
    .await;

    // Complete execution 1
    complete_instance(&*provider, "kv-dca", 1).await;

    // Execution 2: clear all, then set one new key
    ack_with_delta(
        &*provider,
        "kv-dca",
        2,
        vec![
            Event::with_event_id(200, "kv-dca", 2, None, EventKind::KeyValuesCleared),
            Event::with_event_id(
                201,
                "kv-dca",
                2,
                None,
                EventKind::KeyValueSet {
                    key: "c".to_string(),
                    value: "3".to_string(),
                    last_updated_at_ms: 201,
                },
            ),
        ],
    )
    .await;

    // a and b should be gone (tombstoned by clear_all)
    assert_eq!(
        provider.get_kv_value("kv-dca", "a").await.unwrap(),
        None,
        "clear_all must tombstone kv_store key 'a'",
    );
    assert_eq!(
        provider.get_kv_value("kv-dca", "b").await.unwrap(),
        None,
        "clear_all must tombstone kv_store key 'b'",
    );
    // c should exist (set after clear_all, in delta)
    assert_eq!(
        provider.get_kv_value("kv-dca", "c").await.unwrap(),
        Some("3".to_string()),
        "key set after clear_all should be visible",
    );
}

// =============================================================================
// KV Delta: Merged on completion
// =============================================================================

/// When an execution completes, kv_delta is merged into kv_store and then
/// cleared. Subsequent snapshot reads should see the merged state.
pub async fn test_kv_delta_merged_on_completion<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-dmc").await.unwrap();

    // Execution 1: set key
    ack_with_delta(
        &*provider,
        "kv-dmc",
        1,
        vec![Event::with_event_id(
            100,
            "kv-dmc",
            1,
            None,
            EventKind::KeyValueSet {
                key: "result".to_string(),
                value: "42".to_string(),
                last_updated_at_ms: 100,
            },
        )],
    )
    .await;

    // Complete execution 1 → delta merged into kv_store
    complete_instance(&*provider, "kv-dmc", 1).await;

    // Client read should still see the value (now in kv_store)
    assert_eq!(
        provider.get_kv_value("kv-dmc", "result").await.unwrap(),
        Some("42".to_string()),
        "value should be in kv_store after completion merge",
    );
}

// =============================================================================
// KV Delta: Merged on CAN, new execution sees it in snapshot
// =============================================================================

/// After CAN, the delta from execution 1 is merged into kv_store, and
/// execution 2's fetch snapshot reflects the merged state.
pub async fn test_kv_delta_merged_on_can<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;

    create_instance(&*provider, "kv-dmc2").await.unwrap();

    // Execution 1: set two keys, overwrite one
    ack_with_delta(
        &*provider,
        "kv-dmc2",
        1,
        vec![
            Event::with_event_id(
                100,
                "kv-dmc2",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "version".to_string(),
                    value: "1".to_string(),
                    last_updated_at_ms: 100,
                },
            ),
            Event::with_event_id(
                101,
                "kv-dmc2",
                1,
                None,
                EventKind::KeyValueSet {
                    key: "progress".to_string(),
                    value: "50%".to_string(),
                    last_updated_at_ms: 101,
                },
            ),
        ],
    )
    .await;

    // Update progress in a second turn (still execution 1)
    ack_with_delta(
        &*provider,
        "kv-dmc2",
        1,
        vec![Event::with_event_id(
            102,
            "kv-dmc2",
            1,
            None,
            EventKind::KeyValueSet {
                key: "progress".to_string(),
                value: "100%".to_string(),
                last_updated_at_ms: 102,
            },
        )],
    )
    .await;

    // CAN → execution 2
    continue_as_new(&*provider, "kv-dmc2", 2).await;

    // Fetch snapshot for new execution
    provider
        .enqueue_for_orchestrator(poke_item("kv-dmc2"), None)
        .await
        .unwrap();
    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    assert_eq!(
        item.kv_snapshot.get("version").map(|e| &*e.value),
        Some("1"),
        "version should carry over via kv_store after CAN merge",
    );
    assert_eq!(
        item.kv_snapshot.get("progress").map(|e| &*e.value),
        Some("100%"),
        "latest progress value should carry over",
    );

    provider
        .ack_orchestration_item(
            &lock_token,
            2,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// KV Delta: Delete instance cascades to delta table
// =============================================================================

/// Deleting an instance must clean up kv_delta rows in addition to kv_store.
pub async fn test_kv_delta_delete_instance_cascades<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    create_instance(&*provider, "kv-ddel").await.unwrap();

    // Set a value in current execution (goes to delta)
    ack_with_delta(
        &*provider,
        "kv-ddel",
        1,
        vec![Event::with_event_id(
            100,
            "kv-ddel",
            1,
            None,
            EventKind::KeyValueSet {
                key: "data".to_string(),
                value: "important".to_string(),
                last_updated_at_ms: 100,
            },
        )],
    )
    .await;

    assert_eq!(
        provider.get_kv_value("kv-ddel", "data").await.unwrap(),
        Some("important".to_string()),
    );

    // Delete instance
    mgmt.delete_instance("kv-ddel", true).await.unwrap();

    // Value should be gone
    assert_eq!(
        provider.get_kv_value("kv-ddel", "data").await.unwrap(),
        None,
        "KV delta must be cleaned up on instance deletion",
    );
}

// =============================================================================
// KV Delta: Prune does not affect untouched KV from pruned executions
// =============================================================================

/// A key set in execution 1 and never referenced in any subsequent execution
/// must survive pruning of execution 1. The key lives in kv_store (merged at
/// the CAN boundary) and prune only deletes history/execution rows.
///
/// Flow:
///   Exec 1: set "stale_config" → kv_delta
///   CAN → exec 2: delta merged to kv_store. Exec 2 sets unrelated key.
///   CAN → exec 3: delta merged to kv_store. Exec 3 is running, never touches "stale_config".
///   Prune execs 1+2 (keep_last=1).
///   Assert "stale_config" still readable via client AND via snapshot.
pub async fn test_kv_delta_prune_untouched_key_survives<F: ProviderFactory>(factory: &F) {
    let provider = factory.create_provider().await;
    let mgmt = provider
        .as_management_capability()
        .expect("Provider should implement ProviderAdmin");

    create_instance(&*provider, "kv-dprn").await.unwrap();

    // Exec 1: set "stale_config" — only execution that ever touches this key
    ack_with_delta(
        &*provider,
        "kv-dprn",
        1,
        vec![Event::with_event_id(
            100,
            "kv-dprn",
            1,
            None,
            EventKind::KeyValueSet {
                key: "stale_config".to_string(),
                value: "from_exec_1".to_string(),
                last_updated_at_ms: 100,
            },
        )],
    )
    .await;

    // CAN → exec 2 (merges exec 1 delta into kv_store)
    continue_as_new(&*provider, "kv-dprn", 2).await;

    // Exec 2: set a completely unrelated key
    ack_with_delta(
        &*provider,
        "kv-dprn",
        2,
        vec![Event::with_event_id(
            200,
            "kv-dprn",
            2,
            None,
            EventKind::KeyValueSet {
                key: "other".to_string(),
                value: "unrelated".to_string(),
                last_updated_at_ms: 200,
            },
        )],
    )
    .await;

    // CAN → exec 3 (merges exec 2 delta into kv_store)
    continue_as_new(&*provider, "kv-dprn", 3).await;

    // Exec 3: running — never touches "stale_config" or "other"
    // (no ack_with_delta for exec 3, just the OrchestrationStarted from CAN)

    // Verify both keys exist before prune
    assert_eq!(
        provider.get_kv_value("kv-dprn", "stale_config").await.unwrap(),
        Some("from_exec_1".to_string()),
    );
    assert_eq!(
        provider.get_kv_value("kv-dprn", "other").await.unwrap(),
        Some("unrelated".to_string()),
    );

    // Prune execs 1 + 2 (keep only exec 3)
    mgmt.prune_executions(
        "kv-dprn",
        PruneOptions {
            keep_last: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // "stale_config" must survive — it's in kv_store, prune doesn't touch kv_store
    assert_eq!(
        provider.get_kv_value("kv-dprn", "stale_config").await.unwrap(),
        Some("from_exec_1".to_string()),
        "KV key from pruned execution must survive — KV lifetime is instance-scoped",
    );
    assert_eq!(
        provider.get_kv_value("kv-dprn", "other").await.unwrap(),
        Some("unrelated".to_string()),
        "KV key from pruned execution 2 must also survive",
    );

    // Snapshot for exec 3 should also contain both (they're in kv_store)
    provider
        .enqueue_for_orchestrator(poke_item("kv-dprn"), None)
        .await
        .unwrap();
    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item");

    assert_eq!(
        item.kv_snapshot.get("stale_config").map(|e| &*e.value),
        Some("from_exec_1"),
        "snapshot must include untouched key from pruned execution",
    );
    assert_eq!(
        item.kv_snapshot.get("other").map(|e| &*e.value),
        Some("unrelated"),
        "snapshot must include key from pruned execution 2",
    );

    provider
        .ack_orchestration_item(
            &lock_token,
            3,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

// =============================================================================
// Helpers
// =============================================================================

/// Trigger a ContinueAsNew cycle for the given instance, advancing to the
/// specified execution_id. Marks the previous execution as "ContinuedAsNew"
/// then enqueues and processes the ContinueAsNew work item.
async fn continue_as_new(provider: &dyn crate::providers::Provider, instance: &str, new_execution_id: u64) {
    let prev_execution_id = new_execution_id - 1;

    // Mark previous execution as ContinuedAsNew so it's eligible for pruning
    provider
        .enqueue_for_orchestrator(poke_item(instance), None)
        .await
        .unwrap();
    let (_, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item to mark ContinuedAsNew");
    provider
        .ack_orchestration_item(
            &lock_token,
            prev_execution_id,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata {
                status: Some("ContinuedAsNew".to_string()),
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    // Now enqueue and process the ContinueAsNew work item
    let work_item = WorkItem::ContinueAsNew {
        instance: instance.to_string(),
        orchestration: "TestOrch".to_string(),
        input: "{}".to_string(),
        version: Some("1.0.0".to_string()),
        carry_forward_events: vec![],
        initial_custom_status: None,
    };

    provider.enqueue_for_orchestrator(work_item, None).await.unwrap();

    let (_item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item for ContinueAsNew");

    provider
        .ack_orchestration_item(
            &lock_token,
            new_execution_id,
            vec![Event::with_event_id(
                1,
                instance,
                new_execution_id,
                None,
                EventKind::OrchestrationStarted {
                    name: "TestOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();
}

/// Complete an instance by acking with OrchestrationCompleted and setting
/// the execution status to "Completed" in metadata.
async fn complete_instance(provider: &dyn crate::providers::Provider, instance: &str, execution_id: u64) {
    provider
        .enqueue_for_orchestrator(poke_item(instance), None)
        .await
        .unwrap();

    let (_, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("expected orchestration item for completion");

    provider
        .ack_orchestration_item(
            &lock_token,
            execution_id,
            vec![Event::with_event_id(
                9999,
                instance,
                execution_id,
                None,
                EventKind::OrchestrationCompleted {
                    output: "done".to_string(),
                },
            )],
            vec![],
            vec![],
            ExecutionMetadata {
                status: Some("Completed".to_string()),
                output: Some("done".to_string()),
                orchestration_name: Some("TestOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();
}

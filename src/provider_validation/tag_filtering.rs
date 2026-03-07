//! Tag filtering validation tests for providers.
//!
//! These tests verify that `fetch_work_item` correctly filters activities by tag
//! based on the `TagFilter` parameter.

use crate::provider_validations::ProviderFactory;
use crate::providers::{TagFilter, WorkItem};
use std::collections::HashSet;
use std::time::Duration;

/// Helper to create a tagged activity work item
fn tagged_activity(instance: &str, id: u64, tag: Option<&str>) -> WorkItem {
    WorkItem::ActivityExecute {
        instance: instance.to_string(),
        execution_id: 1,
        id,
        name: format!("Activity{id}"),
        input: "{}".to_string(),
        session_id: None,
        tag: tag.map(|t| t.to_string()),
    }
}

/// DefaultOnly filter fetches only untagged items
pub async fn test_default_only_fetches_untagged(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_worker(tagged_activity("inst-1", 1, None))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 2, Some("gpu")))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 3, None))
        .await
        .unwrap();

    // DefaultOnly should only get untagged
    let r1 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &TagFilter::DefaultOnly)
        .await
        .unwrap();
    assert!(r1.is_some(), "Should fetch first untagged item");
    let (item1, _, _) = r1.unwrap();
    match &item1 {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 1);
            assert!(tag.is_none());
        }
        _ => panic!("Expected ActivityExecute"),
    }

    let r2 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &TagFilter::DefaultOnly)
        .await
        .unwrap();
    assert!(r2.is_some(), "Should fetch second untagged item");
    let (item2, _, _) = r2.unwrap();
    match &item2 {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 3);
            assert!(tag.is_none());
        }
        _ => panic!("Expected ActivityExecute"),
    }

    // No more untagged items
    let r3 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &TagFilter::DefaultOnly)
        .await
        .unwrap();
    assert!(r3.is_none(), "No more untagged items should be available");
}

/// Tags filter fetches only items with matching tags
pub async fn test_tags_fetches_only_matching(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_worker(tagged_activity("inst-1", 1, None))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 2, Some("gpu")))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 3, Some("cpu")))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 4, Some("gpu")))
        .await
        .unwrap();

    let filter = TagFilter::Tags(HashSet::from(["gpu".to_string()]));

    let r1 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap();
    assert!(r1.is_some());
    match &r1.unwrap().0 {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 2);
            assert_eq!(tag.as_deref(), Some("gpu"));
        }
        _ => panic!("Expected ActivityExecute"),
    }

    let r2 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap();
    assert!(r2.is_some());
    match &r2.unwrap().0 {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 4);
            assert_eq!(tag.as_deref(), Some("gpu"));
        }
        _ => panic!("Expected ActivityExecute"),
    }

    // No more gpu items
    let r3 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap();
    assert!(r3.is_none());
}

/// DefaultAnd filter fetches both untagged and matching tagged items
pub async fn test_default_and_fetches_untagged_and_matching(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_worker(tagged_activity("inst-1", 1, None))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 2, Some("gpu")))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 3, Some("cpu")))
        .await
        .unwrap();

    let filter = TagFilter::DefaultAnd(HashSet::from(["gpu".to_string()]));

    // Should get untagged (id=1) first, then gpu (id=2), but NOT cpu (id=3)
    let r1 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap()
        .expect("Should fetch untagged item");
    match &r1.0 {
        WorkItem::ActivityExecute { id, .. } => assert_eq!(*id, 1),
        _ => panic!("Expected ActivityExecute"),
    }

    let r2 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap()
        .expect("Should fetch gpu item");
    match &r2.0 {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 2);
            assert_eq!(tag.as_deref(), Some("gpu"));
        }
        _ => panic!("Expected ActivityExecute"),
    }

    // cpu item shouldn't be fetchable
    let r3 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap();
    assert!(
        r3.is_none(),
        "cpu-tagged item should not be fetched by DefaultAnd([gpu])"
    );
}

/// None filter returns no items (orchestrator-only mode)
pub async fn test_none_filter_returns_nothing(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_worker(tagged_activity("inst-1", 1, None))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 2, Some("gpu")))
        .await
        .unwrap();

    let r = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &TagFilter::None)
        .await
        .unwrap();
    assert!(r.is_none(), "TagFilter::None should never return items");
}

/// Multi-tag Tags filter fetches items matching any tag in the set
pub async fn test_multi_tag_filter(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_worker(tagged_activity("inst-1", 1, Some("gpu")))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 2, Some("tpu")))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 3, Some("cpu")))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 4, None))
        .await
        .unwrap();

    let filter = TagFilter::Tags(HashSet::from(["gpu".to_string(), "tpu".to_string()]));

    let r1 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap()
        .expect("Should fetch gpu item");
    match &r1.0 {
        WorkItem::ActivityExecute { tag, .. } => assert_eq!(tag.as_deref(), Some("gpu")),
        _ => panic!("Expected ActivityExecute"),
    }

    let r2 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap()
        .expect("Should fetch tpu item");
    match &r2.0 {
        WorkItem::ActivityExecute { tag, .. } => assert_eq!(tag.as_deref(), Some("tpu")),
        _ => panic!("Expected ActivityExecute"),
    }

    // cpu and untagged should not be returned
    let r3 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap();
    assert!(r3.is_none());
}

/// Tag is preserved through enqueue/fetch round-trip
pub async fn test_tag_round_trip_preservation(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_worker(tagged_activity("inst-1", 1, Some("gpu")))
        .await
        .unwrap();

    let filter = TagFilter::Tags(HashSet::from(["gpu".to_string()]));
    let (item, _, _) = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap()
        .expect("Should fetch tagged item");

    match item {
        WorkItem::ActivityExecute { tag, name, .. } => {
            assert_eq!(tag.as_deref(), Some("gpu"), "Tag should be preserved");
            assert_eq!(name, "Activity1");
        }
        _ => panic!("Expected ActivityExecute"),
    }
}

/// TagFilter::Any fetches all items regardless of tag
pub async fn test_any_filter_fetches_everything(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_worker(tagged_activity("inst-1", 1, None))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-2", 2, Some("gpu")))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-3", 3, Some("cpu")))
        .await
        .unwrap();

    let filter = TagFilter::Any;

    // Should fetch all 3 items regardless of tag
    let r1 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap();
    assert!(r1.is_some(), "Any filter should fetch untagged item");

    let r2 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap();
    assert!(r2.is_some(), "Any filter should fetch gpu-tagged item");

    let r3 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap();
    assert!(r3.is_some(), "Any filter should fetch cpu-tagged item");

    // Queue should be empty now
    let r4 = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap();
    assert!(r4.is_none(), "Queue should be empty");
}

/// Tag survives lock → abandon → refetch cycle
pub async fn test_tag_survives_abandon_and_refetch(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    provider
        .enqueue_for_worker(tagged_activity("inst-1", 1, Some("gpu")))
        .await
        .unwrap();
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 2, None))
        .await
        .unwrap();

    // Fetch the gpu item
    let filter = TagFilter::tags(["gpu"]);
    let (item, token, _) = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap()
        .expect("Should fetch gpu item");
    match &item {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 1);
            assert_eq!(tag.as_deref(), Some("gpu"));
        }
        _ => panic!("Expected ActivityExecute"),
    }

    // Abandon it
    provider.abandon_work_item(&token, None, false).await.unwrap();

    // Refetch with same filter — tag must still be correct
    let (item2, token2, attempt) = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &filter)
        .await
        .unwrap()
        .expect("Abandoned item should reappear");
    match &item2 {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 1, "Should get the same item back");
            assert_eq!(tag.as_deref(), Some("gpu"), "Tag must survive abandon");
        }
        _ => panic!("Expected ActivityExecute"),
    }
    assert!(attempt >= 2, "Attempt count should increase after abandon");
    assert_ne!(token, token2, "Lock token should differ");

    // DefaultOnly filter must NOT see the gpu item — only the untagged one
    let (untagged, _, _) = provider
        .fetch_work_item(Duration::from_secs(5), Duration::ZERO, None, &TagFilter::DefaultOnly)
        .await
        .unwrap()
        .expect("Untagged item should be available");
    match &untagged {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 2);
            assert!(tag.is_none());
        }
        _ => panic!("Expected ActivityExecute"),
    }
}

/// Multiple concurrent runtimes with different TagFilters on the same queue.
///
/// Tests three scenarios in one:
/// - **Mutually exclusive**: DefaultOnly vs Tags(["gpu"]) — zero overlap
/// - **Partial overlap**: Tags(["gpu","cpu"]) vs Tags(["cpu","fpga"]) — share "cpu"
/// - **Full overlap**: Any vs Tags(["gpu"]) — Any is a superset
pub async fn test_multi_runtime_tag_isolation(factory: &dyn ProviderFactory) {
    let provider = factory.create_provider().await;

    // Populate queue with diverse tags
    provider
        .enqueue_for_worker(tagged_activity("inst-1", 1, None))
        .await
        .unwrap(); // untagged
    provider
        .enqueue_for_worker(tagged_activity("inst-2", 2, Some("gpu")))
        .await
        .unwrap(); // gpu
    provider
        .enqueue_for_worker(tagged_activity("inst-3", 3, Some("cpu")))
        .await
        .unwrap(); // cpu
    provider
        .enqueue_for_worker(tagged_activity("inst-4", 4, Some("fpga")))
        .await
        .unwrap(); // fpga
    provider
        .enqueue_for_worker(tagged_activity("inst-5", 5, Some("gpu")))
        .await
        .unwrap(); // gpu (second)

    // --- Mutually exclusive: DefaultOnly sees only untagged ---
    let default_filter = TagFilter::DefaultOnly;
    let r = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &default_filter)
        .await
        .unwrap()
        .expect("DefaultOnly should get untagged item");
    match &r.0 {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 1);
            assert!(tag.is_none());
        }
        _ => panic!("Expected ActivityExecute"),
    }
    // No more untagged
    assert!(
        provider
            .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &default_filter)
            .await
            .unwrap()
            .is_none(),
        "No more untagged items"
    );

    // --- Partial overlap: gpu+cpu filter takes gpu(id=2) and cpu(id=3) ---
    let gpu_cpu = TagFilter::tags(["gpu", "cpu"]);
    let got1 = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &gpu_cpu)
        .await
        .unwrap()
        .expect("gpu+cpu filter should find item");
    let id1 = match &got1.0 {
        WorkItem::ActivityExecute { id, .. } => *id,
        _ => panic!("Expected ActivityExecute"),
    };

    let got2 = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &gpu_cpu)
        .await
        .unwrap()
        .expect("gpu+cpu filter should find second item");
    let id2 = match &got2.0 {
        WorkItem::ActivityExecute { id, .. } => *id,
        _ => panic!("Expected ActivityExecute"),
    };

    let mut fetched_ids = vec![id1, id2];
    fetched_ids.sort();
    assert_eq!(fetched_ids, vec![2, 3], "gpu+cpu filter should get gpu(2) and cpu(3)");

    // --- Partial overlap: cpu+fpga filter — cpu(3) already locked, so only fpga(4) ---
    let cpu_fpga = TagFilter::tags(["cpu", "fpga"]);
    let got3 = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &cpu_fpga)
        .await
        .unwrap()
        .expect("cpu+fpga filter should find fpga item");
    match &got3.0 {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 4);
            assert_eq!(tag.as_deref(), Some("fpga"));
        }
        _ => panic!("Expected ActivityExecute"),
    }
    // cpu is already locked by gpu_cpu filter, fpga consumed — nothing left
    assert!(
        provider
            .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &cpu_fpga)
            .await
            .unwrap()
            .is_none(),
        "cpu locked + fpga consumed = nothing for cpu+fpga"
    );

    // --- Full overlap: Any picks up remaining gpu(5), since 1-4 are all locked ---
    let any = TagFilter::Any;
    let got4 = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &any)
        .await
        .unwrap()
        .expect("Any filter should find remaining gpu item");
    match &got4.0 {
        WorkItem::ActivityExecute { id, tag, .. } => {
            assert_eq!(*id, 5);
            assert_eq!(tag.as_deref(), Some("gpu"));
        }
        _ => panic!("Expected ActivityExecute"),
    }

    // Queue fully consumed/locked
    assert!(
        provider
            .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &any)
            .await
            .unwrap()
            .is_none(),
        "All items consumed or locked"
    );
}

/// Verifies that tags on worker items survive the `ack_orchestration_item` path.
///
/// This is the critical path: when the orchestration dispatcher acks an orchestration
/// turn, it passes worker items (ActivityExecute) to `ack_orchestration_item`, which
/// must persist the `tag` field so that `fetch_work_item` can filter correctly.
///
/// This test catches providers whose `ack_orchestration_item` stored procedure inserts
/// worker items into the worker queue without extracting/storing the tag column.
pub async fn test_tag_preserved_through_ack_orchestration_item(factory: &dyn ProviderFactory) {
    use crate::provider_validation::{Event, EventKind, ExecutionMetadata, start_item};

    let provider = factory.create_provider().await;

    // Step 1: Set up an orchestration instance by enqueuing and fetching
    provider
        .enqueue_for_orchestrator(start_item("tag-ack-inst"), None)
        .await
        .unwrap();

    let (_item, lock_token, _attempt) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("should get orchestration item");

    // Step 2: Ack with worker items — one tagged "gpu", one untagged
    let gpu_activity = WorkItem::ActivityExecute {
        instance: "tag-ack-inst".to_string(),
        execution_id: 1,
        id: 10,
        name: "GpuWork".to_string(),
        input: r#""model-v3""#.to_string(),
        session_id: None,
        tag: Some("gpu".to_string()),
    };
    let cpu_activity = WorkItem::ActivityExecute {
        instance: "tag-ack-inst".to_string(),
        execution_id: 1,
        id: 11,
        name: "CpuWork".to_string(),
        input: r#""data""#.to_string(),
        session_id: None,
        tag: None,
    };

    provider
        .ack_orchestration_item(
            &lock_token,
            1,
            vec![Event::with_event_id(
                1,
                "tag-ack-inst".to_string(),
                1,
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
            vec![gpu_activity, cpu_activity],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Step 3: DefaultOnly should fetch ONLY the untagged item
    let default_result = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::DefaultOnly)
        .await
        .unwrap()
        .expect("DefaultOnly should get the untagged activity");

    match &default_result.0 {
        WorkItem::ActivityExecute { name, tag, .. } => {
            assert_eq!(name, "CpuWork", "DefaultOnly must return untagged item");
            assert!(tag.is_none(), "DefaultOnly item must have no tag");
        }
        other => panic!("expected ActivityExecute, got {:?}", other),
    }
    // Ack the item to remove it from the queue
    provider.ack_work_item(&default_result.1, None).await.unwrap();

    // Step 4: DefaultOnly should now find nothing (only gpu-tagged item remains)
    let empty = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::DefaultOnly)
        .await
        .unwrap();
    assert!(
        empty.is_none(),
        "DefaultOnly must NOT see gpu-tagged item — got {:?}",
        empty
    );

    // Step 5: Tags(["gpu"]) should fetch the gpu-tagged item
    let gpu_filter = TagFilter::Tags(["gpu".to_string()].into_iter().collect());
    let gpu_result = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &gpu_filter)
        .await
        .unwrap()
        .expect("Tags([gpu]) should get the gpu-tagged activity");

    match &gpu_result.0 {
        WorkItem::ActivityExecute { name, tag, .. } => {
            assert_eq!(name, "GpuWork", "Tags([gpu]) must return gpu item");
            assert_eq!(tag.as_deref(), Some("gpu"), "tag must be preserved as 'gpu'");
        }
        other => panic!("expected ActivityExecute, got {:?}", other),
    }
}

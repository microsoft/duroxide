#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::Event;
use duroxide::EventKind;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::{ExecutionMetadata, Provider, TagFilter, WorkItem};
use std::sync::Arc;
use std::time::Duration;

mod common;
use common::test_create_execution;

/// Verify provider ignores work items after a terminal event by just acking
#[tokio::test]
async fn test_ignore_work_after_terminal_event() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store: Arc<SqliteProvider> = Arc::new(SqliteProvider::new(&db_url, None).await.unwrap());

    let instance = "inst-terminal";

    // Seed terminal history: OrchestrationCompleted (using test helper)
    let _ = test_create_execution(store.as_ref(), instance, "TermOrch", "1.0.0", "seed", None, None)
        .await
        .unwrap();
    // Use event_id=1 (first event)
    store
        .append_with_execution(
            instance,
            1,
            vec![Event::with_event_id(
                2,
                instance.to_string(),
                1,
                None,
                EventKind::OrchestrationCompleted {
                    output: "done".to_string(),
                },
            )],
        )
        .await
        .unwrap();

    // Enqueue arbitrary work item that should be ignored by runtime
    store
        .enqueue_for_orchestrator(
            WorkItem::ExternalRaised {
                instance: instance.to_string(),
                name: "Ignored".to_string(),
                data: "x".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    // Fetch orchestration item - runtime would bail and just ack
    let (item, lock_token, _attempt_count) = store
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.instance, instance);
    assert_eq!(item.messages.len(), 1);

    // Simulate runtime acking empty because it's terminal
    Provider::ack_orchestration_item(
        store.as_ref(),
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

    // Queue should now be empty
    assert!(
        store
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );

    // History should remain unchanged (no new events)
    let hist = store.read(instance).await.unwrap_or_default();
    assert!(
        hist.iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationCompleted { .. }))
    );
}

#[tokio::test]
async fn test_fetch_orchestration_item_new_instance() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store: Arc<SqliteProvider> = Arc::new(SqliteProvider::new(&db_url, None).await.unwrap());

    // Enqueue start work (provider will create instance lazily on fetch)
    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "test-instance".to_string(),
                orchestration: "TestOrch".to_string(),
                input: "test-input".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: duroxide::INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .unwrap();

    // Fetch orchestration item
    let (item, _lock_token, _attempt_count) = store
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(item.instance, "test-instance");
    assert_eq!(item.orchestration_name, "TestOrch");
    assert_eq!(item.version, "1.0.0");
    assert_eq!(item.execution_id, 1);
    assert!(item.history.is_empty());
    assert_eq!(item.messages.len(), 1);
    assert!(matches!(
        &item.messages[0],
        WorkItem::StartOrchestration { orchestration, .. } if orchestration == "TestOrch"
    ));
}

#[tokio::test]
async fn test_fetch_orchestration_item_existing_instance() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store: Arc<SqliteProvider> = Arc::new(SqliteProvider::new(&db_url, None).await.unwrap());

    // Seed instance history using test helper
    test_create_execution(
        store.as_ref(),
        "test-instance",
        "TestOrch",
        "1.0.0",
        "test-input",
        None,
        None,
    )
    .await
    .unwrap();
    store
        .append_with_execution(
            "test-instance",
            1,
            vec![Event::with_event_id(
                2,
                "test-instance".to_string(),
                1,
                None,
                EventKind::ActivityScheduled {
                    name: "TestActivity".to_string(),
                    input: "activity-input".to_string(),
                    session_id: None,
                    tag: None,
                },
            )],
        )
        .await
        .unwrap();

    // Enqueue completion
    store
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "test-instance".to_string(),
                execution_id: 1,
                id: 1,
                result: "activity-result".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    // Fetch orchestration item
    let (item, _lock_token, _attempt_count) = store
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(item.instance, "test-instance");
    assert_eq!(item.orchestration_name, "TestOrch");
    assert_eq!(item.version, "1.0.0");
    assert_eq!(item.execution_id, 1);
    assert_eq!(item.history.len(), 2);
    assert_eq!(item.messages.len(), 1);
    assert!(matches!(
        &item.messages[0],
        WorkItem::ActivityCompleted { result, .. } if result == "activity-result"
    ));
}

#[tokio::test]
async fn test_fetch_orchestration_item_no_work() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store: Arc<SqliteProvider> = Arc::new(SqliteProvider::new(&db_url, None).await.unwrap());

    // No work items
    let item = store
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap();
    assert!(item.is_none());
}

#[tokio::test]
async fn test_ack_orchestration_item_atomic() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store: Arc<SqliteProvider> = Arc::new(SqliteProvider::new(&db_url, None).await.unwrap());

    // Setup: enqueue start work; provider will create instance lazily
    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "test-instance".to_string(),
                orchestration: "TestOrch".to_string(),
                input: "test-input".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: duroxide::INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .unwrap();

    // Fetch and get lock token
    let (_item, lock_token, _attempt_count) = store
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Prepare updates
    let history_delta = vec![
        Event::with_event_id(
            1,
            "test-instance".to_string(),
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "TestOrch".to_string(),
                version: "1.0.0".to_string(),
                input: "test-input".to_string(),
                parent_instance: None,
                parent_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        ),
        Event::with_event_id(
            2,
            "test-instance".to_string(),
            1,
            None,
            EventKind::ActivityScheduled {
                name: "TestActivity".to_string(),
                input: "activity-input".to_string(),
                session_id: None,
                tag: None,
            },
        ),
    ];

    let worker_items = vec![WorkItem::ActivityExecute {
        instance: "test-instance".to_string(),
        execution_id: 1,
        id: 1,
        name: "TestActivity".to_string(),
        input: "activity-input".to_string(),
        session_id: None,
        tag: None,
    }];

    // Ack with updates
    store
        .ack_orchestration_item(
            &lock_token,
            1,
            history_delta,
            worker_items,
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Verify history was updated
    let history = store.read("test-instance").await.unwrap_or_default();
    assert_eq!(history.len(), 2);
    assert!(matches!(&history[0].kind, EventKind::OrchestrationStarted { .. }));
    assert!(matches!(&history[1].kind, EventKind::ActivityScheduled { .. }));

    // Verify worker item was enqueued
    let (worker_item, _, _) = store
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO, None, &TagFilter::default())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(worker_item, WorkItem::ActivityExecute { .. }));

    // Verify orchestrator queue is empty (item was acked)
    assert!(
        store
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_ack_orchestration_item_error_handling() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store: Arc<SqliteProvider> = Arc::new(SqliteProvider::new(&db_url, None).await.unwrap());

    // Try to ack with invalid token
    let result = store
        .ack_orchestration_item(
            "invalid-token",
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Invalid lock token"));
}

#[tokio::test]
async fn test_abandon_orchestration_item() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store: Arc<SqliteProvider> = Arc::new(SqliteProvider::new(&db_url, None).await.unwrap());

    // Setup: enqueue start work; provider will create instance lazily
    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "test-instance".to_string(),
                orchestration: "TestOrch".to_string(),
                input: "test-input".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: duroxide::INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .unwrap();

    // Fetch and get lock token
    let (_item, lock_token, _attempt_count) = store
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Abandon the item
    store
        .abandon_orchestration_item(&lock_token, None, false)
        .await
        .unwrap();

    // Verify item is back in queue
    let (item2, _lock_token2, _attempt_count2) = store
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "test-instance");
    assert!(matches!(
        &item2.messages[0],
        WorkItem::StartOrchestration { orchestration, .. } if orchestration == "TestOrch"
    ));
}

#[tokio::test]
async fn test_abandon_orchestration_item_with_delay() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store: Arc<SqliteProvider> = Arc::new(SqliteProvider::new(&db_url, None).await.unwrap());

    // Setup: enqueue start work; provider will create instance lazily
    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "test-instance".to_string(),
                orchestration: "TestOrch".to_string(),
                input: "test-input".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: duroxide::INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .unwrap();

    // Fetch and get lock token
    let lock_timeout = Duration::from_secs(30);
    let (_item, lock_token, _attempt_count) = store
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    // Abandon with delay (sqlite supports delayed visibility)
    store
        .abandon_orchestration_item(&lock_token, Some(Duration::from_millis(500)), false)
        .await
        .unwrap();
    // Should not be visible immediately
    assert!(
        store
            .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );
    // After delay, it should be visible
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let (item2, _lock_token2, _attempt_count2) = store
        .fetch_orchestration_item(lock_timeout, Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item2.instance, "test-instance");
}

#[tokio::test]
async fn test_abandon_orchestration_item_error_handling() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store: Arc<SqliteProvider> = Arc::new(SqliteProvider::new(&db_url, None).await.unwrap());

    // Try to abandon with invalid token
    let result = store.abandon_orchestration_item("invalid-token", None, false).await;

    // sqlite provider returns error for invalid tokens
    assert!(result.is_err());
}

#[tokio::test]
async fn test_in_memory_provider_atomic_operations() {
    let store: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    // Enqueue work (in-memory will lazily create instance on fetch)
    store
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "test-instance".to_string(),
                orchestration: "TestOrch".to_string(),
                input: "test-input".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: duroxide::INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .unwrap();

    // Test fetch
    let (item, lock_token, _attempt_count) = store
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.instance, "test-instance");
    assert_eq!(item.orchestration_name, "TestOrch");

    // Test ack with updates
    let history_delta = vec![Event::with_event_id(
        1,
        "test-instance".to_string(),
        1,
        None,
        EventKind::OrchestrationStarted {
            name: "TestOrch".to_string(),
            version: "1.0.0".to_string(),
            input: "test-input".to_string(),
            parent_instance: None,
            parent_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    )];

    store
        .ack_orchestration_item(
            &lock_token,
            1,
            history_delta,
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();

    // Verify history
    let history = store.read("test-instance").await.unwrap_or_default();
    assert_eq!(history.len(), 1);
    assert!(matches!(&history[0].kind, EventKind::OrchestrationStarted { .. }));

    // Test abandon
    store
        .enqueue_for_orchestrator(
            WorkItem::ActivityCompleted {
                instance: "test-instance".to_string(),
                execution_id: 1,
                id: 1,
                result: "result".to_string(),
            },
            None,
        )
        .await
        .unwrap();

    let (_item2, lock_token2, _attempt_count2) = store
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();

    store
        .abandon_orchestration_item(&lock_token2, None, false)
        .await
        .unwrap();

    // Should be available again
    let (item3, _lock_token3, _attempt_count3) = store
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item3.instance, "test-instance");
}

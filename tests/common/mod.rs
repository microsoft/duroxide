// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

pub mod fault_injection;
#[allow(dead_code)]
pub mod tracing_capture;

use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::{ExecutionMetadata, Provider, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use std::sync::Arc as StdArc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[allow(dead_code)]
pub async fn wait_for_history<F>(store: StdArc<dyn Provider>, instance: &str, predicate: F, timeout_ms: u64) -> bool
where
    F: Fn(&[Event]) -> bool,
{
    wait_for_history_event(
        store,
        instance,
        |hist| if predicate(hist) { Some(()) } else { None },
        timeout_ms,
    )
    .await
    .is_some()
}

#[allow(dead_code)]
pub async fn wait_for_subscription(store: StdArc<dyn Provider>, instance: &str, name: &str, timeout_ms: u64) -> bool {
    wait_for_history(
        store,
        instance,
        |hist| {
            hist.iter().any(|e| {
                matches!(&e.kind, EventKind::ExternalSubscribed { name: n } if n == name)
                    || matches!(&e.kind, EventKind::QueueSubscribed { name: n } if n == name)
            })
        },
        timeout_ms,
    )
    .await
}

#[allow(dead_code)]
pub async fn wait_for_history_event<T, F>(
    store: StdArc<dyn Provider>,
    instance: &str,
    selector: F,
    timeout_ms: u64,
) -> Option<T>
where
    T: Clone,
    F: Fn(&[Event]) -> Option<T>,
{
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let hist = store.read(instance).await.unwrap_or_default();
        if let Some(e) = selector(&hist) {
            return Some(e);
        }
        if Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[allow(dead_code)]
pub async fn create_sqlite_store_disk() -> (StdArc<dyn Provider>, TempDir) {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    std::fs::File::create(&db_path).unwrap();
    let db_url = format!("sqlite:{}", db_path.display());
    let store = StdArc::new(SqliteProvider::new(&db_url, None).await.unwrap()) as StdArc<dyn Provider>;
    (store, td)
}

/// Test helper to create a new orchestration instance with initial history.
///
/// This replicates what the runtime does in production by using real provider APIs:
/// 1. Enqueues StartOrchestration work item
/// 2. Fetches it to get a lock token
/// 3. Acks with OrchestrationStarted event
///
/// Use this to seed test state without spinning up a full runtime.
#[allow(dead_code)]
pub async fn test_create_execution(
    provider: &dyn Provider,
    instance: &str,
    orchestration: &str,
    version: &str,
    input: &str,
    parent_instance: Option<&str>,
    parent_id: Option<u64>,
) -> Result<u64, String> {
    // Calculate next execution ID (max + 1, or INITIAL if none exist)
    // Try to get ProviderAdmin capability, otherwise assume no executions exist
    let execs = if let Some(mgmt) = provider.as_management_capability() {
        mgmt.list_executions(instance).await.unwrap_or_default()
    } else {
        Vec::new()
    };
    let next_execution_id = if execs.is_empty() {
        duroxide::INITIAL_EXECUTION_ID
    } else {
        execs.iter().max().copied().unwrap() + 1
    };

    // Enqueue StartOrchestration work item with calculated execution_id
    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: instance.to_string(),
                orchestration: orchestration.to_string(),
                version: Some(version.to_string()),
                input: input.to_string(),
                parent_instance: parent_instance.map(|s| s.to_string()),
                parent_id,
                execution_id: next_execution_id,
            },
            None,
        )
        .await
        .map_err(|e| e.to_string())?;

    // Fetch to get lock token
    let (_item, lock_token, _attempt_count) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Failed to fetch orchestration item".to_string())?;

    // The fetched item should have the execution_id we enqueued
    let execution_id = next_execution_id;

    // Ack with OrchestrationStarted event and proper metadata
    provider
        .ack_orchestration_item(
            &lock_token,
            execution_id,
            vec![Event::with_event_id(
                duroxide::INITIAL_EVENT_ID,
                instance,
                execution_id,
                None,
                EventKind::OrchestrationStarted {
                    name: orchestration.to_string(),
                    version: version.to_string(),
                    input: input.to_string(),
                    parent_instance: parent_instance.map(|s| s.to_string()),
                    parent_id,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
            vec![], // no worker items
            vec![], // no orchestrator items
            ExecutionMetadata {
                orchestration_name: Some(orchestration.to_string()),
                orchestration_version: Some(version.to_string()),
                ..Default::default()
            },
            vec![], // no cancelled activities
        )
        .await
        .map_err(|e| e.to_string())?;

    Ok(execution_id)
}

/// Seed an orchestrator turn by enqueueing a trigger, fetching it, and acking with a history delta.
///
/// This is the fundamental building block for constructing test history via provider APIs.
/// Each call simulates one complete orchestrator turn: enqueue work item → fetch → ack.
///
/// Handles the race condition where fetch may return a different instance's item by
/// looping and abandoning non-matching items.
///
/// # Arguments
/// * `provider` - Provider to operate on
/// * `trigger` - Work item to enqueue (e.g., StartOrchestration, ExternalRaised)
/// * `execution_id` - Execution ID for the ack
/// * `events` - History delta events to append
/// * `orchestrator_items` - Follow-up work items to enqueue atomically with the ack
/// * `metadata` - Execution metadata (orchestration name/version, pinned_duroxide_version)
#[allow(dead_code)]
pub async fn seed_history_turn(
    provider: &dyn Provider,
    trigger: WorkItem,
    execution_id: u64,
    events: Vec<Event>,
    orchestrator_items: Vec<WorkItem>,
    metadata: ExecutionMetadata,
) {
    provider.enqueue_for_orchestrator(trigger, None).await.unwrap();

    let lock_token;
    let mut abandoned_tokens = Vec::new();
    let expected_instance = events.first().map(|e| e.instance_id.clone()).unwrap_or_default();
    loop {
        let (item, token, _) = provider
            .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
            .await
            .unwrap()
            .expect("should have work available");
        if item.instance == expected_instance {
            lock_token = token;
            break;
        }
        abandoned_tokens.push(token);
    }

    provider
        .ack_orchestration_item(
            &lock_token,
            execution_id,
            events,
            vec![], // no worker items
            orchestrator_items,
            metadata,
            vec![], // no cancelled activities
        )
        .await
        .unwrap();

    for token in abandoned_tokens {
        let _ = provider.abandon_orchestration_item(&token, None, true).await;
    }
}

/// Seed an instance with a single OrchestrationStarted event stamped with a specific
/// pinned duroxide version. Enqueues a follow-up ExternalRaised so the item remains fetchable.
///
/// This is the standard way to create a test instance that appears to have been
/// created by a specific duroxide version (for capability filtering tests).
#[allow(dead_code)]
pub async fn seed_instance_with_pinned_version(
    provider: &dyn Provider,
    instance: &str,
    orchestration: &str,
    pinned_version: semver::Version,
) {
    let version_str = pinned_version.to_string();
    let mut started_event = Event::with_event_id(
        INITIAL_EVENT_ID,
        instance,
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: orchestration.to_string(),
            version: "1.0.0".to_string(),
            input: "{}".to_string(),
            parent_instance: None,
            parent_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    );
    started_event.duroxide_version = version_str;

    seed_history_turn(
        provider,
        WorkItem::StartOrchestration {
            instance: instance.to_string(),
            orchestration: orchestration.to_string(),
            input: "{}".to_string(),
            version: Some("1.0.0".to_string()),
            parent_instance: None,
            parent_id: None,
            execution_id: INITIAL_EXECUTION_ID,
        },
        INITIAL_EXECUTION_ID,
        vec![started_event],
        vec![WorkItem::ExternalRaised {
            instance: instance.to_string(),
            name: "ping".to_string(),
            data: "{}".to_string(),
        }],
        ExecutionMetadata {
            orchestration_name: Some(orchestration.to_string()),
            orchestration_version: Some("1.0.0".to_string()),
            pinned_duroxide_version: Some(pinned_version),
            ..Default::default()
        },
    )
    .await;
}

/// Create an event stamped with a specific duroxide version.
///
/// Convenience wrapper around `Event::with_event_id` that overrides the
/// `duroxide_version` field. Useful for seeding history that appears to
/// have been written by an older runtime.
#[allow(dead_code)]
pub fn make_versioned_event(
    event_id: u64,
    instance: &str,
    execution_id: u64,
    source_event_id: Option<u64>,
    kind: EventKind,
    duroxide_version: &str,
) -> Event {
    let mut event = Event::with_event_id(event_id, instance, execution_id, source_event_id, kind);
    event.duroxide_version = duroxide_version.to_string();
    event
}

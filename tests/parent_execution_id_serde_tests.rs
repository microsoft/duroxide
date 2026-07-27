// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Serde compatibility for the optional `parent_execution_id` field.
//!
//! `parent_execution_id` was added to `EventKind::OrchestrationStarted` and
//! `WorkItem::StartOrchestration` to route sub-orchestration completions back to the exact
//! parent execution that scheduled the child. Both are `Option<u64>` carrying
//! `#[serde(default, skip_serializing_if = "Option::is_none")]`, which is what makes rolling
//! upgrades safe in both directions:
//!
//! - **Old -> new:** history and work items written before this field existed decode with
//!   `parent_execution_id: None`, and routing falls back to a durable provider read.
//! - **New -> old:** a node still running the previous version ignores the added field.
//! - **`None` is omitted entirely,** so top-level starts (which never carry a parent execution)
//!   serialize to bytes identical to the previous release.
//!
//! These properties are load-bearing for mixed-version clusters, so they are pinned here
//! rather than left implicit. Mirrors the existing pattern in `tests/tag_serde_tests.rs`.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use duroxide::providers::WorkItem;
use duroxide::{Event, EventKind};

// ---------------------------------------------------------------------------
// Mirrors of the pre-upgrade shapes.
//
// These stand in for a node still running the previous duroxide version: the same
// variants and serde attributes, minus `parent_execution_id`. Decoding new payloads
// into these proves an old node tolerates the added field.
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum PreUpgradeWorkItem {
    StartOrchestration {
        instance: String,
        orchestration: String,
        input: String,
        version: Option<String>,
        parent_instance: Option<String>,
        parent_id: Option<u64>,
        execution_id: u64,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
enum PreUpgradeEventKind {
    OrchestrationStarted {
        name: String,
        version: String,
        input: String,
        parent_instance: Option<String>,
        parent_id: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        carry_forward_events: Option<Vec<(String, String)>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        initial_custom_status: Option<String>,
    },
}

fn started(parent_execution_id: Option<u64>) -> EventKind {
    EventKind::OrchestrationStarted {
        name: "Child".to_string(),
        version: "1.0.0".to_string(),
        input: "child-input".to_string(),
        parent_instance: Some("parent-1".to_string()),
        parent_id: Some(2),
        parent_execution_id,
        carry_forward_events: None,
        initial_custom_status: None,
    }
}

fn start_work_item(parent_execution_id: Option<u64>) -> WorkItem {
    WorkItem::StartOrchestration {
        instance: "parent-1::sub::2".to_string(),
        orchestration: "Child".to_string(),
        input: "child-input".to_string(),
        version: Some("1.0.0".to_string()),
        parent_instance: Some("parent-1".to_string()),
        parent_id: Some(2),
        parent_execution_id,
        execution_id: duroxide::INITIAL_EXECUTION_ID,
    }
}

// ---------------------------------------------------------------------------
// EventKind::OrchestrationStarted
// ---------------------------------------------------------------------------

/// A stamped parent execution survives a serde round trip.
#[test]
fn orchestration_started_with_parent_execution_id_roundtrips() {
    let json = serde_json::to_string(&started(Some(3))).unwrap();
    let deser: EventKind = serde_json::from_str(&json).unwrap();
    match deser {
        EventKind::OrchestrationStarted {
            parent_execution_id, ..
        } => assert_eq!(parent_execution_id, Some(3)),
        other => panic!("Expected OrchestrationStarted, got {other:?}"),
    }
}

/// `None` omits the key entirely. Root orchestrations never carry a parent execution, so
/// their history must serialize to bytes identical to the previous release.
#[test]
fn orchestration_started_none_parent_execution_id_omitted_in_json() {
    let json = serde_json::to_string(&started(None)).unwrap();
    assert!(
        !json.contains("parent_execution_id"),
        "parent_execution_id: None should be omitted from JSON, got: {json}"
    );
}

/// History written before this field existed decodes as `None`, which is what selects the
/// durable provider-read fallback at routing time.
#[test]
fn orchestration_started_missing_parent_execution_id_deserializes_as_none() {
    let json = r#"{"type":"OrchestrationStarted","name":"Child","version":"1.0.0","input":"child-input","parent_instance":"parent-1","parent_id":2}"#;
    let kind: EventKind = serde_json::from_str(json).unwrap();
    match kind {
        EventKind::OrchestrationStarted {
            parent_execution_id,
            parent_instance,
            parent_id,
            ..
        } => {
            assert_eq!(parent_execution_id, None);
            // The rest of the parent link must still decode intact.
            assert_eq!(parent_instance, Some("parent-1".to_string()));
            assert_eq!(parent_id, Some(2));
        }
        other => panic!("Expected OrchestrationStarted, got {other:?}"),
    }
}

/// A node still running the previous version ignores the added history field.
#[test]
fn orchestration_started_with_parent_execution_id_decodes_on_pre_upgrade_node() {
    let json = serde_json::to_string(&started(Some(3))).unwrap();
    assert!(json.contains("parent_execution_id"), "precondition: field present");

    let old: PreUpgradeEventKind =
        serde_json::from_str(&json).expect("a pre-upgrade node must tolerate the added field");
    match old {
        PreUpgradeEventKind::OrchestrationStarted {
            parent_instance,
            parent_id,
            ..
        } => {
            assert_eq!(parent_instance, Some("parent-1".to_string()));
            assert_eq!(parent_id, Some(2));
        }
    }
}

/// The field survives the full `Event` envelope, which flattens `kind` into the outer object.
#[test]
fn full_event_envelope_roundtrips_parent_execution_id() {
    let event = Event::with_event_id(1, "parent-1::sub::2", 1, None, started(Some(4)));
    let json = serde_json::to_string(&event).unwrap();
    let deser: Event = serde_json::from_str(&json).unwrap();
    assert_eq!(deser, event);
    match deser.kind {
        EventKind::OrchestrationStarted {
            parent_execution_id, ..
        } => assert_eq!(parent_execution_id, Some(4)),
        other => panic!("Expected OrchestrationStarted, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// WorkItem::StartOrchestration
// ---------------------------------------------------------------------------

/// A stamped parent execution survives a serde round trip on the queue payload.
#[test]
fn work_item_start_orchestration_with_parent_execution_id_roundtrips() {
    let item = start_work_item(Some(3));
    let json = serde_json::to_string(&item).unwrap();
    let deser: WorkItem = serde_json::from_str(&json).unwrap();
    assert_eq!(deser, item);
}

/// `None` omits the key entirely, so top-level starts enqueue bytes identical to the
/// previous release.
#[test]
fn work_item_start_orchestration_none_parent_execution_id_omitted_in_json() {
    let json = serde_json::to_string(&start_work_item(None)).unwrap();
    assert!(
        !json.contains("parent_execution_id"),
        "parent_execution_id: None should be omitted from JSON, got: {json}"
    );
}

/// Work items enqueued before this field existed decode as `None`.
#[test]
fn work_item_start_orchestration_missing_parent_execution_id_deserializes_as_none() {
    let json = r#"{"StartOrchestration":{"instance":"parent-1::sub::2","orchestration":"Child","input":"child-input","version":"1.0.0","parent_instance":"parent-1","parent_id":2,"execution_id":1}}"#;
    let item: WorkItem = serde_json::from_str(json).unwrap();
    match item {
        WorkItem::StartOrchestration {
            parent_execution_id,
            parent_instance,
            parent_id,
            ..
        } => {
            assert_eq!(parent_execution_id, None);
            assert_eq!(parent_instance, Some("parent-1".to_string()));
            assert_eq!(parent_id, Some(2));
        }
        other => panic!("Expected StartOrchestration, got {other:?}"),
    }
}

/// A node still running the previous version ignores the added work-item field.
#[test]
fn work_item_start_orchestration_with_parent_execution_id_decodes_on_pre_upgrade_node() {
    let json = serde_json::to_string(&start_work_item(Some(3))).unwrap();
    assert!(json.contains("parent_execution_id"), "precondition: field present");

    let old: PreUpgradeWorkItem =
        serde_json::from_str(&json).expect("a pre-upgrade node must tolerate the added field");
    match old {
        PreUpgradeWorkItem::StartOrchestration {
            parent_instance,
            parent_id,
            ..
        } => {
            assert_eq!(parent_instance, Some("parent-1".to_string()));
            assert_eq!(parent_id, Some(2));
        }
    }
}

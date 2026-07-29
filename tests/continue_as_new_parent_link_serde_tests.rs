// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Serde compatibility for the optional parent-link fields on `WorkItem::ContinueAsNew`.
//!
//! `parent_instance`, `parent_id`, and `parent_execution_id` were added to
//! `WorkItem::ContinueAsNew` so a sub-orchestration that rolls over can still notify its
//! parent when a later execution completes or fails. All three are `Option` carrying
//! `#[serde(default, skip_serializing_if = "Option::is_none")]`, which is what makes rolling
//! upgrades safe in both directions:
//!
//! - **Old -> new:** work items enqueued before these fields existed decode as `None`. The
//!   parent link is simply not preserved for that execution — the pre-existing behaviour.
//! - **New -> old:** a node still running the previous version ignores the added fields.
//! - **`None` is omitted entirely,** so a `ContinueAsNew` from a root orchestration
//!   serializes to bytes identical to the previous release.
//!
//! Without these tests the backcompat claim rests entirely on the serde attributes: dropping
//! them would keep every other test green while old nodes start seeing `"parent_instance":null`
//! on the wire. Mirrors `tests/parent_execution_id_serde_tests.rs`.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use duroxide::providers::WorkItem;

// ---------------------------------------------------------------------------
// Mirror of the pre-upgrade shape.
//
// Stands in for a node still running the previous duroxide version: the same variant and
// serde attributes, minus the three parent-link fields. Decoding new payloads into this
// proves an old node tolerates the added keys.
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum PreUpgradeWorkItem {
    ContinueAsNew {
        instance: String,
        orchestration: String,
        input: String,
        version: Option<String>,
        #[serde(default)]
        carry_forward_events: Vec<(String, String)>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        initial_custom_status: Option<String>,
    },
}

fn continue_as_new(
    parent_instance: Option<&str>,
    parent_id: Option<u64>,
    parent_execution_id: Option<u64>,
) -> WorkItem {
    WorkItem::ContinueAsNew {
        instance: "parent-1::sub::2".to_string(),
        orchestration: "Child".to_string(),
        input: "next-input".to_string(),
        version: Some("1.0.0".to_string()),
        parent_instance: parent_instance.map(str::to_string),
        parent_id,
        parent_execution_id,
        carry_forward_events: vec![("Evt".to_string(), "payload".to_string())],
        initial_custom_status: None,
    }
}

/// A stamped parent link survives a serde round trip on the queue payload.
#[test]
fn continue_as_new_with_parent_link_roundtrips() {
    let item = continue_as_new(Some("parent-1"), Some(2), Some(3));
    let json = serde_json::to_string(&item).unwrap();
    let deser: WorkItem = serde_json::from_str(&json).unwrap();
    assert_eq!(deser, item);
}

/// `None` omits the keys entirely. A root orchestration never carries a parent link, so its
/// `ContinueAsNew` must enqueue bytes identical to the previous release.
#[test]
fn continue_as_new_none_parent_link_omitted_in_json() {
    let json = serde_json::to_string(&continue_as_new(None, None, None)).unwrap();
    for key in ["parent_instance", "parent_id", "parent_execution_id"] {
        assert!(
            !json.contains(key),
            "{key}: None should be omitted from JSON, got: {json}"
        );
    }
}

/// Work items enqueued before these fields existed decode as `None`.
#[test]
fn continue_as_new_missing_parent_link_deserializes_as_none() {
    let json = r#"{"ContinueAsNew":{"instance":"parent-1::sub::2","orchestration":"Child","input":"next-input","version":"1.0.0","carry_forward_events":[["Evt","payload"]]}}"#;
    let item: WorkItem = serde_json::from_str(json).unwrap();
    match item {
        WorkItem::ContinueAsNew {
            parent_instance,
            parent_id,
            parent_execution_id,
            carry_forward_events,
            ..
        } => {
            assert_eq!(parent_instance, None);
            assert_eq!(parent_id, None);
            assert_eq!(parent_execution_id, None);
            // The rest of the payload must still decode intact.
            assert_eq!(carry_forward_events, vec![("Evt".to_string(), "payload".to_string())]);
        }
        other => panic!("Expected ContinueAsNew, got {other:?}"),
    }
}

/// A node still running the previous version ignores the added work-item fields.
#[test]
fn continue_as_new_with_parent_link_decodes_on_pre_upgrade_node() {
    let json = serde_json::to_string(&continue_as_new(Some("parent-1"), Some(2), Some(3))).unwrap();
    for key in ["parent_instance", "parent_id", "parent_execution_id"] {
        assert!(json.contains(key), "precondition: {key} present, got: {json}");
    }

    let old: PreUpgradeWorkItem =
        serde_json::from_str(&json).expect("a pre-upgrade node must tolerate the added fields");
    match old {
        PreUpgradeWorkItem::ContinueAsNew {
            instance,
            orchestration,
            carry_forward_events,
            ..
        } => {
            assert_eq!(instance, "parent-1::sub::2");
            assert_eq!(orchestration, "Child");
            assert_eq!(carry_forward_events, vec![("Evt".to_string(), "payload".to_string())]);
        }
    }
}

/// A pre-upgrade node's payload decodes on a new node, and the new node's payload for a root
/// orchestration is byte-identical to what the old node would have written.
#[test]
fn continue_as_new_wire_format_unchanged_without_parent_link() {
    let old = PreUpgradeWorkItem::ContinueAsNew {
        instance: "parent-1::sub::2".to_string(),
        orchestration: "Child".to_string(),
        input: "next-input".to_string(),
        version: Some("1.0.0".to_string()),
        carry_forward_events: vec![("Evt".to_string(), "payload".to_string())],
        initial_custom_status: None,
    };
    let old_json = serde_json::to_string(&old).unwrap();
    let new_json = serde_json::to_string(&continue_as_new(None, None, None)).unwrap();
    assert_eq!(new_json, old_json);
}

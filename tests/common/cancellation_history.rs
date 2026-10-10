// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use duroxide::Event;

pub fn decision_delta(events: &[Event]) -> Vec<serde_json::Value> {
    let mut values: Vec<_> = events
        .iter()
        .map(|event| {
            let mut value = serde_json::to_value(event).expect("event must serialize");
            let object = value.as_object_mut().expect("event must be an object");
            object.remove("timestamp_ms");
            object.remove("duroxide_version");
            object.remove("fire_at_ms");
            value
        })
        .collect();
    // Only independent contiguous dropped-future blocks have unordered IDs.
    // Keep event positions, type order, reasons, source IDs and multiplicity.
    let is_independent_cancel = |value: &serde_json::Value| {
        matches!(
            value["type"].as_str(),
            Some(
                "ExternalSubscribedPersistentCancelled"
                    | "ExternalSubscribedCancelled"
                    | "ActivityCancelRequested"
                    | "SubOrchestrationCancelRequested"
            )
        ) && value["reason"].as_str() == Some("dropped_future")
    };
    let mut index = 0;
    while index < values.len() {
        if !is_independent_cancel(&values[index]) {
            index += 1;
            continue;
        }
        let start = index;
        let kind = values[start]["type"].clone();
        while index < values.len() && is_independent_cancel(&values[index]) && values[index]["type"] == kind {
            index += 1;
        }
        let mut block = values[start..index].to_vec();
        let ids: Vec<_> = block.iter().map(|value| value["event_id"].clone()).collect();
        block.sort_by_key(|value| {
            (
                value["type"].as_str().expect("cancellation type").to_string(),
                value["source_event_id"].as_u64().expect("cancellation source ID"),
                value["reason"].as_str().expect("cancellation reason").to_string(),
            )
        });
        for (slot, mut value) in block.into_iter().enumerate() {
            value["event_id"] = ids[slot].clone();
            values[start + slot] = value;
        }
    }
    values
}

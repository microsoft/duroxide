// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use metrics_util::debugging::{DebugValue, Snapshotter};

pub fn retirement_gauge(snapshotter: &Snapshotter) -> f64 {
    let values: Vec<_> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == "duroxide_pending_foreign_retirements")
        .map(|(key, _, _, value)| {
            assert_eq!(key.key().labels().count(), 0, "retirement gauge must be unlabeled");
            match value {
                DebugValue::Gauge(value) => value.into_inner(),
                _ => panic!("foreign retirements must be a gauge"),
            }
        })
        .collect();
    assert_eq!(values.len(), 1, "one retirement gauge series per recorder");
    values[0]
}

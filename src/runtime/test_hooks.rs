// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Test hooks for simulating various conditions during testing.
//!
//! This module provides test-only hooks that allow tests to inject delays
//! or other behaviors at strategic points in the runtime.
//!
//! Enable with the `test-hooks` feature:
//! ```toml
//! [dev-dependencies]
//! duroxide = { path = ".", features = ["test-hooks"] }
//! ```

// Test hooks use Mutex locks - test code intentionally uses unwrap
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

/// Delay to inject after spawning orchestration lock renewal task, before processing.
/// Stored as milliseconds. 0 means no delay.
static ORCH_PROCESSING_DELAY_MS: AtomicU64 = AtomicU64::new(0);

/// Set of instance prefixes that should have the delay applied.
/// If empty, the delay applies to all instances.
static ORCH_DELAY_INSTANCES: Mutex<Option<HashSet<String>>> = Mutex::new(None);

/// Set a delay to be injected after spawning the orchestration lock renewal task.
///
/// This simulates slow orchestration processing (e.g., slow replay of large history)
/// to test that lock renewal works correctly.
///
/// # Arguments
/// * `delay` - Duration to sleep before processing. Use `Duration::ZERO` to disable.
/// * `instance_prefix` - Optional instance name prefix to limit which instances are affected.
///   If `None`, affects all instances (not recommended for parallel tests).
pub fn set_orch_processing_delay(delay: Duration, instance_prefix: Option<&str>) {
    ORCH_PROCESSING_DELAY_MS.store(delay.as_millis() as u64, Ordering::SeqCst);
    if let Some(prefix) = instance_prefix {
        let mut guard = ORCH_DELAY_INSTANCES.lock().unwrap();
        let set = guard.get_or_insert_with(HashSet::new);
        set.insert(prefix.to_string());
    }
}

/// Get the current orchestration processing delay for a specific instance.
///
/// Returns `None` if no delay is set, delay is zero, or the instance doesn't match.
pub fn get_orch_processing_delay(instance: &str) -> Option<Duration> {
    let ms = ORCH_PROCESSING_DELAY_MS.load(Ordering::SeqCst);
    if ms == 0 {
        return None;
    }

    // Check if instance matches any registered prefix
    let guard = ORCH_DELAY_INSTANCES.lock().unwrap();
    if let Some(prefixes) = guard.as_ref()
        && !prefixes.is_empty()
        && !prefixes.iter().any(|p| instance.starts_with(p))
    {
        return None;
    }

    Some(Duration::from_millis(ms))
}

/// Clear the orchestration processing delay.
pub fn clear_orch_processing_delay() {
    ORCH_PROCESSING_DELAY_MS.store(0, Ordering::SeqCst);
    let mut guard = ORCH_DELAY_INSTANCES.lock().unwrap();
    *guard = None;
}

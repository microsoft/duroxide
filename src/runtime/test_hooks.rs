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

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

mod provider;
pub use provider::TestProvider;

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

/// The nine runtime-owned task roles; counters are scoped to a single hook instance.
pub use super::task_group::TaskRole as LifecycleTask;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProviderOperation {
    FetchOrchestration,
    FetchActivity,
    EnqueueOrchestration,
    EnqueueActivity,
    AcknowledgeOrchestration,
    AcknowledgeActivity,
    RenewOrchestration,
    RenewActivity,
    SessionMaintenance,
    Read,
    SystemMetrics,
    QueueDepths,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LifecyclePoint {
    StartupBeforeIo,
    StartupAfterSpawn(LifecycleTask),
    ShutdownCoordinator,
    ForceRequested,
    ParentWork(LifecycleTask),
    ProviderEnter(ProviderOperation),
    ProviderReturn(ProviderOperation),
    ProviderCommit(ProviderOperation),
    ForeignCleanup,
}

#[derive(Clone, Default)]
pub struct LifecycleGate {
    entered: CancellationToken,
    released: CancellationToken,
}

impl LifecycleGate {
    pub async fn entered(&self) {
        self.entered.cancelled().await;
    }

    pub fn release(&self) {
        self.released.cancel();
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LifecycleCounts {
    pub started: usize,
    pub completed: usize,
    pub active: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HookError {
    AlreadyArmed,
    TasksActive,
}

#[derive(Default)]
struct LifecycleState {
    gates: HashMap<LifecyclePoint, LifecycleGate>,
    faults: HashMap<LifecyclePoint, String>,
    counts: [LifecycleCounts; 9],
    hits: HashMap<LifecyclePoint, usize>,
    cleanup: LifecycleCounts,
}

/// No process-global lifecycle hooks: a fixture owns and passes its own instance.
#[derive(Clone, Default)]
pub struct LifecycleHooks(Arc<Mutex<LifecycleState>>);

impl LifecycleHooks {
    pub fn gate(&self, point: LifecyclePoint) -> Option<LifecycleGate> {
        self.0.lock().unwrap().gates.get(&point).cloned()
    }

    /// # Errors
    /// Returns `AlreadyArmed` rather than replacing a gate with live waiters.
    pub fn hold(&self, point: LifecyclePoint) -> Result<LifecycleGate, HookError> {
        let mut state = self.0.lock().unwrap();
        if state.gates.contains_key(&point) {
            return Err(HookError::AlreadyArmed);
        }
        let gate = LifecycleGate::default();
        state.gates.insert(point, gate.clone());
        Ok(gate)
    }

    /// # Errors
    /// Returns `AlreadyArmed` when an unconsumed fault is already configured.
    pub fn fail_once(&self, point: LifecyclePoint) -> Result<(), HookError> {
        self.fail_once_with_message(point, "injected lifecycle fault")
    }

    /// # Errors
    /// Rejects replacement of a still-armed fault.
    pub fn fail_once_with_message(&self, point: LifecyclePoint, message: impl Into<String>) -> Result<(), HookError> {
        let mut state = self.0.lock().unwrap();
        match state.faults.entry(point) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(message.into());
                Ok(())
            }
            std::collections::hash_map::Entry::Occupied(_) => Err(HookError::AlreadyArmed),
        }
    }

    pub async fn checkpoint(&self, point: LifecyclePoint) {
        let (gate, fail) = {
            let mut state = self.0.lock().unwrap();
            *state.hits.entry(point).or_default() += 1;
            (state.gates.get(&point).cloned(), state.faults.remove(&point))
        };
        if let Some(gate) = gate {
            gate.entered.cancel();
            gate.released.cancelled().await;
        }
        if let Some(message) = fail {
            panic!("{message}");
        }
    }

    pub fn track(&self, task: LifecycleTask) -> LifecycleTaskGuard {
        let mut state = self.0.lock().unwrap();
        let counts = &mut state.counts[task as usize];
        counts.started += 1;
        counts.active += 1;
        LifecycleTaskGuard {
            hooks: self.clone(),
            task: Some(task),
        }
    }

    pub(super) fn track_cleanup(&self) -> LifecycleTaskGuard {
        let mut state = self.0.lock().unwrap();
        state.cleanup.started += 1;
        state.cleanup.active += 1;
        LifecycleTaskGuard {
            hooks: self.clone(),
            task: None,
        }
    }

    pub fn cleanup_counts(&self) -> LifecycleCounts {
        self.0.lock().unwrap().cleanup
    }

    pub fn counts(&self) -> [LifecycleCounts; 9] {
        self.0.lock().unwrap().counts
    }

    pub fn hits(&self, point: LifecyclePoint) -> usize {
        self.0.lock().unwrap().hits.get(&point).copied().unwrap_or(0)
    }

    /// # Errors
    /// Active tasks must retire before their counters can be reset.
    pub fn reset(&self) -> Result<(), HookError> {
        let old = {
            let mut state = self.0.lock().unwrap();
            if state.cleanup.active != 0 || state.counts.iter().any(|counts| counts.active != 0) {
                return Err(HookError::TasksActive);
            }
            std::mem::take(&mut *state)
        };
        for gate in old.gates.into_values() {
            gate.release();
        }
        Ok(())
    }
}

pub struct LifecycleTaskGuard {
    hooks: LifecycleHooks,
    task: Option<LifecycleTask>,
}

impl Drop for LifecycleTaskGuard {
    fn drop(&mut self) {
        let mut state = self.hooks.0.lock().unwrap();
        let counts = match self.task {
            Some(task) => &mut state.counts[task as usize],
            None => &mut state.cleanup,
        };
        counts.active -= 1;
        counts.completed += 1;
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[tokio::test]
    async fn lifecycle_hooks_are_scoped_and_retirement_counts_all_roles() {
        let first = LifecycleHooks::default();
        let independent = LifecycleHooks::default();
        for task in LifecycleTask::ALL {
            let gate = first.hold(LifecyclePoint::ParentWork(task)).unwrap();
            let hooks = first.clone();
            let child = tokio::spawn(async move {
                let _guard = hooks.track(task);
                hooks.checkpoint(LifecyclePoint::ParentWork(task)).await;
            });
            gate.entered().await;
            assert_eq!(first.counts()[task as usize].active, 1);
            assert_eq!(first.reset(), Err(HookError::TasksActive));
            assert_eq!(independent.counts(), [LifecycleCounts::default(); 9]);
            gate.release();
            child.await.unwrap();
            assert_eq!(
                first.counts()[task as usize],
                LifecycleCounts {
                    started: 1,
                    completed: 1,
                    active: 0
                }
            );
            if let Some(parent) = task.parent() {
                assert_ne!(task, parent);
            }
        }
        first.reset().unwrap();
        assert_eq!(first.counts(), [LifecycleCounts::default(); 9]);
    }

    #[tokio::test]
    async fn lifecycle_fault_is_one_shot_and_does_not_poison_hook_state() {
        let hooks = LifecycleHooks::default();
        hooks.fail_once(LifecyclePoint::StartupBeforeIo).unwrap();
        assert_eq!(
            hooks.fail_once(LifecyclePoint::StartupBeforeIo),
            Err(HookError::AlreadyArmed)
        );
        let fault = hooks.clone();
        let failed = tokio::spawn(async move { fault.checkpoint(LifecyclePoint::StartupBeforeIo).await });
        assert!(failed.await.unwrap_err().is_panic());
        hooks.checkpoint(LifecyclePoint::StartupBeforeIo).await;
        hooks.reset().unwrap();
    }

    #[tokio::test]
    async fn lifecycle_gates_reject_replacement_and_reset_releases_waiters() {
        let hooks = LifecycleHooks::default();
        let gate = hooks.hold(LifecyclePoint::ForeignCleanup).unwrap();
        assert!(matches!(
            hooks.hold(LifecyclePoint::ForeignCleanup),
            Err(HookError::AlreadyArmed)
        ));
        let held = hooks.clone();
        let work = tokio::spawn(async move { held.checkpoint(LifecyclePoint::ForeignCleanup).await });
        gate.entered().await;
        hooks.reset().unwrap();
        work.await.unwrap();
        hooks.hold(LifecyclePoint::ForeignCleanup).unwrap().release();
        hooks.checkpoint(LifecyclePoint::ForeignCleanup).await;
    }
}

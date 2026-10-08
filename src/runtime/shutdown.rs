// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Immutable stop acceptance and reusable observation, separate from the cleanup owner.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::task_group::TaskFailure;

const CLEANUP_HEADROOM: Duration = Duration::from_secs(5);
const MAX_TIMEOUT: Duration = Duration::from_nanos(u64::MAX);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ShutdownError {
    InvalidTimeouts,
    DeadlineOverflow,
    NotRequested,
    TimedOut,
    AlreadyCompleted,
    InvalidCompletion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Deadlines {
    pub grace: Instant,
    pub total: Instant,
}

impl Deadlines {
    pub fn from_timeouts(now: Instant, grace: Duration, total: Option<Duration>) -> Result<Self, ShutdownError> {
        let total = match total {
            Some(total) => total,
            None => grace
                .checked_add(CLEANUP_HEADROOM)
                .ok_or(ShutdownError::DeadlineOverflow)?,
        };
        if total < grace {
            return Err(ShutdownError::InvalidTimeouts);
        }
        if total > MAX_TIMEOUT {
            return Err(ShutdownError::DeadlineOverflow);
        }
        let deadlines = Self {
            grace: now.checked_add(grace).ok_or(ShutdownError::DeadlineOverflow)?,
            total: now.checked_add(total).ok_or(ShutdownError::DeadlineOverflow)?,
        };
        deadlines.validate(now)?;
        Ok(deadlines)
    }

    fn validate(self, now: Instant) -> Result<(), ShutdownError> {
        if self.total < self.grace {
            return Err(ShutdownError::InvalidTimeouts);
        }
        // Tokio rounds deadlines up by a millisecond. Do not accept a deadline
        // whose timer would overflow, clamp, or panic after lifecycle mutation.
        if self.total.saturating_duration_since(now) > MAX_TIMEOUT
            || self.total.checked_add(Duration::from_millis(1)).is_none()
        {
            return Err(ShutdownError::DeadlineOverflow);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Completion {
    Drained,
    Forced,
    Failed {
        failures: Vec<TaskFailure>,
        quiescent: bool,
    },
}

#[derive(Default)]
pub(super) struct ShutdownState {
    accepted: Mutex<Option<Arc<ShutdownRequest>>>,
    graceful: CancellationToken,
    force: CancellationToken,
}

pub(super) struct ShutdownRequest {
    deadlines: Deadlines,
    accepted_at: Instant,
    graceful: CancellationToken,
    force: CancellationToken,
    force_required: AtomicBool,
    timeout_observed: AtomicBool,
    completion: watch::Sender<Option<Arc<Completion>>>,
}

pub(super) struct ShutdownDiagnostics {
    pub grace_remaining_ns: u64,
    pub total_remaining_ns: u64,
    pub elapsed_ms: u64,
    pub grace_due_at_acceptance: bool,
    pub timeout_observed: bool,
}

impl ShutdownState {
    /// The winner must install the cleanup owner; observers never own its joins.
    pub fn accept(&self, deadlines: Deadlines) -> Result<(Arc<ShutdownRequest>, bool), ShutdownError> {
        deadlines.validate(Instant::now())?;
        let mut accepted = self.accepted.lock().unwrap();
        if let Some(request) = accepted.as_ref() {
            return Ok((Arc::clone(request), false));
        }
        let request = Arc::new(ShutdownRequest {
            deadlines,
            accepted_at: Instant::now(),
            graceful: self.graceful.clone(),
            force: self.force.clone(),
            force_required: AtomicBool::new(false),
            timeout_observed: AtomicBool::new(false),
            completion: watch::channel(None).0,
        });
        *accepted = Some(Arc::clone(&request));
        drop(accepted);
        request.graceful.cancel();
        Ok((request, true))
    }

    pub fn accepted(&self) -> Result<Arc<ShutdownRequest>, ShutdownError> {
        self.accepted
            .lock()
            .unwrap()
            .as_ref()
            .map(Arc::clone)
            .ok_or(ShutdownError::NotRequested)
    }

    pub fn graceful_signal(&self) -> &CancellationToken {
        &self.graceful
    }

    pub fn force_signal(&self) -> &CancellationToken {
        &self.force
    }
}

impl ShutdownRequest {
    pub fn deadlines(&self) -> Deadlines {
        self.deadlines
    }

    pub fn diagnostics(&self) -> ShutdownDiagnostics {
        ShutdownDiagnostics {
            grace_remaining_ns: self
                .deadlines
                .grace
                .saturating_duration_since(self.accepted_at)
                .as_nanos() as u64,
            total_remaining_ns: self
                .deadlines
                .total
                .saturating_duration_since(self.accepted_at)
                .as_nanos() as u64,
            elapsed_ms: self.accepted_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
            grace_due_at_acceptance: self.deadlines.grace <= self.accepted_at,
            timeout_observed: self.timeout_observed.load(Ordering::Acquire),
        }
    }

    #[cfg(test)]
    pub fn graceful_signal(&self) -> &CancellationToken {
        &self.graceful
    }

    #[cfg(test)]
    pub async fn force_requested(&self) {
        self.force.cancelled().await;
    }

    /// The retained coordinator drives grace; no observer drives or cancels cleanup.
    pub fn request_force_if_due(&self, now: Instant) -> bool {
        self.completion.send_if_modified(|slot| {
            if now >= self.deadlines.grace && slot.is_none() {
                self.force_required.store(true, Ordering::Release);
            }
            false
        });
        let required = self.force_required.load(Ordering::Acquire);
        if required {
            self.force.cancel();
        }
        required
    }

    pub fn publish(&self, failures: Vec<TaskFailure>, quiescent: bool) -> Result<(), ShutdownError> {
        if failures.is_empty() && !quiescent {
            return Err(ShutdownError::InvalidCompletion);
        }
        let published = self.completion.send_if_modified(|slot| {
            if slot.is_some() {
                return false;
            }
            *slot = Some(Arc::new(if !failures.is_empty() {
                Completion::Failed { failures, quiescent }
            } else if self.force_required.load(Ordering::Acquire) {
                Completion::Forced
            } else {
                Completion::Drained
            }));
            true
        });
        if published {
            Ok(())
        } else {
            Err(ShutdownError::AlreadyCompleted)
        }
    }

    pub async fn wait(&self) -> Result<Arc<Completion>, ShutdownError> {
        let mut completion = self.completion.subscribe();
        loop {
            {
                // The read guard fixes result selection relative to concurrent publication.
                let result = completion.borrow();
                if let Some(result) = result.as_ref() {
                    return Ok(Arc::clone(result));
                }
                if Instant::now() >= self.deadlines.total {
                    self.timeout_observed.store(true, Ordering::Release);
                    return Err(ShutdownError::TimedOut);
                }
            }
            tokio::select! {
                biased;
                result = completion.changed() => result.expect("request retains its completion sender"),
                () = tokio::time::sleep_until(self.deadlines.total.into()) => {},
            }
        }
    }

    pub async fn wait_for_completion(&self) -> Arc<Completion> {
        let mut completion = self.completion.subscribe();
        loop {
            if let Some(result) = completion.borrow_and_update().as_ref() {
                return Arc::clone(result);
            }
            completion
                .changed()
                .await
                .expect("request retains its completion sender");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::task_group::{FailureKind, run_owned};
    use futures_util::FutureExt;
    use tokio::sync::oneshot;

    #[test]
    fn relative_inputs_preserve_precision_and_reject_order_or_overflow() {
        let now = Instant::now();
        for grace in [Duration::ZERO, Duration::from_nanos(1), Duration::from_secs(30)] {
            let deadlines = Deadlines::from_timeouts(now, grace, None).unwrap();
            assert_eq!(deadlines.grace.duration_since(now), grace);
            assert_eq!(deadlines.total.duration_since(now), grace + CLEANUP_HEADROOM);
            assert!(Deadlines::from_timeouts(now, grace, Some(grace)).is_ok());
        }
        assert_eq!(
            Deadlines::from_timeouts(now, Duration::from_secs(1), Some(Duration::ZERO)),
            Err(ShutdownError::InvalidTimeouts)
        );
        assert_eq!(
            Deadlines::from_timeouts(now, Duration::MAX, None),
            Err(ShutdownError::DeadlineOverflow)
        );
        assert_eq!(
            Deadlines::from_timeouts(now, Duration::MAX, Some(Duration::MAX)),
            Err(ShutdownError::DeadlineOverflow)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_acceptance_wins_without_renewing_either_deadline() {
        let state = Arc::new(ShutdownState::default());
        assert!(matches!(state.accepted(), Err(ShutdownError::NotRequested)));
        let now = Instant::now();
        let original = Deadlines::from_timeouts(now, Duration::from_secs(1), None).unwrap();
        let (first, won) = state.accept(original).unwrap();
        assert!(won);
        assert!(first.graceful_signal().is_cancelled());
        let mut contenders = Vec::new();
        for seconds in 0..100 {
            let state = Arc::clone(&state);
            contenders.push(tokio::spawn(async move {
                state
                    .accept(Deadlines::from_timeouts(now, Duration::from_secs(seconds), None).unwrap())
                    .unwrap()
            }));
        }
        for contender in contenders {
            let (request, won) = contender.await.unwrap();
            assert!(!won);
            assert!(Arc::ptr_eq(&first, &request));
            assert_eq!(request.deadlines(), original);
        }
        let invalid = Deadlines {
            grace: original.total,
            total: original.grace,
        };
        assert!(matches!(state.accept(invalid), Err(ShutdownError::InvalidTimeouts)));
        assert_eq!(state.accepted().unwrap().deadlines(), original);
    }

    #[tokio::test]
    async fn total_expiry_and_dropped_waiters_do_not_own_the_cleanup() {
        let state = ShutdownState::default();
        let past = Instant::now() - Duration::from_secs(1);
        let (request, _) = state
            .accept(Deadlines {
                grace: past,
                total: past,
            })
            .unwrap();
        let (release, held) = oneshot::channel();
        let (entered, started) = oneshot::channel();
        let publication = Arc::clone(&request);
        let owner = tokio::spawn(async move {
            let result = run_owned("root", CancellationToken::new(), move |group| {
                group.spawn("held-child", async move {
                    entered.send(()).unwrap();
                    held.await.unwrap();
                    Ok(())
                });
                Box::pin(async { Ok(()) })
            })
            .await;
            publication.publish(result.err().unwrap_or_default(), true).unwrap();
        });
        started.await.unwrap();
        assert!(request.request_force_if_due(Instant::now()));
        assert!(request.force_requested().now_or_never().is_some());
        assert_eq!(request.wait().await, Err(ShutdownError::TimedOut));
        assert!(request.wait_for_completion().now_or_never().is_none());
        let dropped = Arc::clone(&request);
        let waiter = tokio::spawn(async move { dropped.wait_for_completion().await });
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(!owner.is_finished());
        release.send(()).unwrap();
        owner.await.unwrap();
        assert_eq!(*request.wait_for_completion().await, Completion::Forced);
        assert_eq!(*request.wait().await.unwrap(), Completion::Forced);
    }

    #[tokio::test]
    async fn published_completion_wins_even_for_zero_or_expired_total() {
        let now = Instant::now();
        let state = ShutdownState::default();
        let (request, _) = state.accept(Deadlines { grace: now, total: now }).unwrap();
        assert_eq!(
            request.publish(Vec::new(), false),
            Err(ShutdownError::InvalidCompletion)
        );
        request.publish(Vec::new(), true).unwrap();
        assert!(!request.request_force_if_due(now + Duration::from_secs(1)));
        assert_eq!(*request.wait().await.unwrap(), Completion::Drained);
        assert_eq!(request.publish(Vec::new(), true), Err(ShutdownError::AlreadyCompleted));
    }

    #[tokio::test]
    async fn operational_error_and_quiescence_are_separate_facts() {
        for quiescent in [false, true] {
            let state = ShutdownState::default();
            let (request, _) = state
                .accept(Deadlines::from_timeouts(Instant::now(), Duration::ZERO, None).unwrap())
                .unwrap();
            let failures = vec![TaskFailure {
                task: "cleanup",
                kind: FailureKind::Operation,
            }];
            request.publish(failures.clone(), quiescent).unwrap();
            assert_eq!(
                *request.wait().await.unwrap(),
                Completion::Failed { failures, quiescent }
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn force_and_completion_publication_share_one_ordering_boundary() {
        for _ in 0..100 {
            let state = ShutdownState::default();
            let now = Instant::now();
            let (request, _) = state.accept(Deadlines { grace: now, total: now }).unwrap();
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let publisher = Arc::clone(&request);
            let publication_barrier = Arc::clone(&barrier);
            let publication = tokio::spawn(async move {
                publication_barrier.wait().await;
                publisher.publish(Vec::new(), true).unwrap();
            });
            barrier.wait().await;
            let forced = request.request_force_if_due(now);
            publication.await.unwrap();
            let result = request.wait().await.unwrap();
            assert_eq!(
                *result,
                if forced {
                    Completion::Forced
                } else {
                    Completion::Drained
                }
            );
        }
    }
}

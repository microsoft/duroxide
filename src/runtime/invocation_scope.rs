// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

#[cfg(feature = "test-hooks")]
use super::task_safety::protect_future;
use super::task_safety::{FailureKind, TaskFailure, TaskFuture, TaskResult, append_failures, record_panic};
use futures_util::FutureExt;
#[cfg(any(test, feature = "test-hooks"))]
use tokio_util::sync::CancellationToken;

/// Rejection of a foreign-cleanup attachment before foreign execution may begin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachError {
    /// Standalone or synchronous replay has no asynchronous cleanup owner.
    Unsupported,
    /// This turn already owns its single cleanup attachment.
    AlreadyAttached,
    /// Finalization or retirement has closed the attachment scope.
    Closed,
}

impl std::fmt::Display for AttachError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unsupported => "invocation cleanup requires runtime-owned asynchronous execution",
            Self::AlreadyAttached => "invocation cleanup is already attached to this turn",
            Self::Closed => "the invocation cleanup scope is closed",
        })
    }
}

impl std::error::Error for AttachError {}

#[derive(Default)]
struct State {
    closed: bool,
    attached: bool,
    finalized: bool,
    finalization_failed: bool,
    finalization: Option<Box<dyn FnMut() + Send + 'static>>,
    cleanup: Option<TaskFuture<'static>>,
    failures: Vec<TaskFailure>,
    #[cfg(feature = "test-hooks")]
    hooks: Option<super::test_hooks::LifecycleHooks>,
}

/// Handlers receive an attachment; the turn guard owns retirement.
pub(super) struct InvocationScope(Arc<Mutex<State>>);

pub(super) struct InvocationGuard {
    scope: Option<InvocationScope>,
    runtime: Arc<super::Runtime>,
}

impl InvocationGuard {
    pub fn new(runtime: Arc<super::Runtime>) -> Self {
        let scope = InvocationScope::new();
        #[cfg(feature = "test-hooks")]
        let scope = scope.with_hooks(runtime.hooks());
        Self {
            scope: Some(scope),
            runtime,
        }
    }

    pub fn attachment(&self) -> CleanupAttachment {
        self.scope.as_ref().expect("live invocation scope").attachment()
    }
}

impl Drop for InvocationGuard {
    fn drop(&mut self) {
        let scope = self.scope.take().expect("invocation retires once");
        scope.attachment().finalize();
        let (needs_retirement, has_cleanup) = {
            let mut state = scope.0.lock().unwrap();
            state.closed = true;
            (state.attached || !state.failures.is_empty(), state.cleanup.is_some())
        };
        if !needs_retirement {
            return;
        }
        let runtime = self.runtime.clone();
        let executor = runtime
            .lifecycle
            .lock()
            .unwrap()
            .executor
            .clone()
            .expect("executing turn has an executor");
        let tracker = runtime.task_tracker.clone();
        if has_cleanup {
            runtime.record_foreign_retirement(true);
        }
        // Registration occurs before the parent can leave the tracker. This cleanup
        // is deliberately not an abortable dispatcher child.
        tracker.spawn_on(
            async move {
                let _ = scope.retire_reporting(|failures| runtime.record_faults(failures)).await;
                // Only a returned completion proves retirement, including an Err.
                // Dropping or panicking an observer must not decrement this count.
                if has_cleanup {
                    runtime.record_foreign_retirement(false);
                }
            },
            &executor,
        );
    }
}

#[derive(Clone, Default)]
pub(crate) struct CleanupAttachment(Option<Arc<Mutex<State>>>);

impl InvocationScope {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(State::default())))
    }

    pub fn attachment(&self) -> CleanupAttachment {
        CleanupAttachment(Some(Arc::clone(&self.0)))
    }

    #[cfg(test)]
    pub async fn retire(self) -> TaskResult {
        self.retire_reporting(|_| {}).await
    }

    pub async fn retire_reporting(self, report_faults: impl Fn(&[TaskFailure]) + Send) -> TaskResult {
        self.attachment().finalize();
        #[cfg(feature = "test-hooks")]
        let hooks = self.0.lock().unwrap().hooks.clone();
        let (cleanup, mut failures) = {
            let mut state = self.0.lock().unwrap();
            state.closed = true;
            (state.cleanup.take(), std::mem::take(&mut state.failures))
        };
        report_faults(&failures);
        if let Some(mut cleanup) = cleanup {
            #[cfg(feature = "test-hooks")]
            let _guard = hooks.as_ref().map(|hooks| hooks.track_cleanup());
            #[cfg(feature = "test-hooks")]
            if let Some(hooks) = hooks
                && let Err(errors) = protect_future(
                    "invocation-cleanup-hook",
                    async {
                        hooks
                            .checkpoint(super::test_hooks::LifecyclePoint::ForeignCleanup)
                            .await;
                        Ok(())
                    },
                    &CancellationToken::new(),
                )
                .await
            {
                report_faults(&errors);
                failures.extend(errors);
            }
            match AssertUnwindSafe(cleanup.as_mut()).catch_unwind().await {
                Ok(Ok(())) => {}
                Ok(Err(errors)) => append_failures("invocation-cleanup", errors, &mut failures),
                Err(payload) => {
                    record_panic("invocation-cleanup", FailureKind::PollPanicked, payload, &mut failures);
                    report_faults(&failures);
                    // The completion observer failed, not the foreign work it observes.
                    // Retain even the failed future's captures; neither drop nor a second
                    // poll can establish quiescence. Ordinary waits still have a deadline.
                    std::future::pending::<()>().await;
                }
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(cleanup))) {
                record_panic(
                    "invocation-cleanup",
                    FailureKind::DestructionPanicked,
                    payload,
                    &mut failures,
                );
            }
        }
        report_faults(&failures);
        if failures.is_empty() { Ok(()) } else { Err(failures) }
    }

    #[cfg(feature = "test-hooks")]
    pub fn with_hooks(self, hooks: super::test_hooks::LifecycleHooks) -> Self {
        self.0.lock().unwrap().hooks = Some(hooks);
        self
    }
}

impl CleanupAttachment {
    pub(super) fn attach(&self, cleanup: impl Future<Output = TaskResult> + Send + 'static) -> Result<(), AttachError> {
        self.attach_with_finalization(cleanup, None)
    }

    fn attach_with_finalization(
        &self,
        cleanup: impl Future<Output = TaskResult> + Send + 'static,
        finalization: Option<Box<dyn FnMut() + Send + 'static>>,
    ) -> Result<(), AttachError> {
        let state = self.0.as_ref().ok_or(AttachError::Unsupported)?;
        let mut state = state.lock().unwrap();
        if state.closed || state.finalized {
            return Err(AttachError::Closed);
        }
        if state.attached {
            return Err(AttachError::AlreadyAttached);
        }
        state.cleanup = Some(Box::pin(cleanup));
        state.finalization = finalization;
        state.attached = true;
        Ok(())
    }

    pub(crate) fn register(
        &self,
        cleanup: impl Future<Output = Result<(), String>> + Send + 'static,
    ) -> Result<(), AttachError> {
        self.attach(cleanup.map(|result| {
            result.map_err(|_| {
                vec![TaskFailure {
                    task: "invocation-cleanup",
                    kind: FailureKind::Operation,
                }]
            })
        }))
    }

    pub(crate) fn register_with_finalization(
        &self,
        cleanup: impl Future<Output = Result<(), String>> + Send + 'static,
        finalization: impl FnMut() + Send + 'static,
    ) -> Result<(), AttachError> {
        self.attach_with_finalization(
            cleanup.map(|result| {
                result.map_err(|_| {
                    vec![TaskFailure {
                        task: "invocation-cleanup",
                        kind: FailureKind::Operation,
                    }]
                })
            }),
            Some(Box::new(finalization)),
        )
    }

    /// Close foreign mutation admission before the final durable snapshot, outside the scope lock.
    pub(super) fn finalize(&self) -> bool {
        let Some(state) = &self.0 else {
            return false;
        };
        let callback = {
            let mut state = state.lock().unwrap();
            state.finalized = true;
            state.finalization.take()
        };
        let Some(mut callback) = callback else {
            return false;
        };
        let mut failures = Vec::new();
        if let Err(payload) = catch_unwind(AssertUnwindSafe(&mut callback)) {
            record_panic(
                "invocation-finalization",
                FailureKind::PollPanicked,
                payload,
                &mut failures,
            );
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(callback))) {
            record_panic(
                "invocation-finalization",
                FailureKind::DestructionPanicked,
                payload,
                &mut failures,
            );
        }
        let mut state = state.lock().unwrap();
        state.finalization_failed |= !failures.is_empty();
        state.failures.extend(failures);
        true
    }

    pub(super) fn finalization_failed(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|state| state.lock().unwrap().finalization_failed)
    }

    #[cfg(feature = "test-hooks")]
    pub(super) fn before_turn_decision(&self) {
        let hooks = self.0.as_ref().and_then(|state| state.lock().unwrap().hooks.clone());
        if let Some(hooks) = hooks {
            hooks.panic_if_armed(super::test_hooks::LifecyclePoint::BeforeTurnDecision);
        }
    }

    pub(super) fn destroy_driver(&self, driver: impl Sized) {
        let Some(state) = &self.0 else {
            drop(driver);
            return;
        };
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(driver))) {
            let mut failures = Vec::new();
            record_panic(
                "orchestration-driver",
                FailureKind::DestructionPanicked,
                payload,
                &mut failures,
            );
            tracing::error!(
                target: "duroxide::runtime::lifecycle",
                category = "invocation_destruction_failed",
                "Orchestration driver destruction failed; invocation cleanup remains owned"
            );
            state.lock().unwrap().failures.extend(failures);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::task_safety::{FailureKind, TaskFailure};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn no_attachment_completes_and_cannot_reopen() {
        let scope = InvocationScope::new();
        let attachment = scope.attachment();
        assert_eq!(scope.retire().await, Ok(()));
        assert_eq!(attachment.attach(async { Ok(()) }), Err(AttachError::Closed));
        assert_eq!(
            CleanupAttachment::default().attach(async { Ok(()) }),
            Err(AttachError::Unsupported)
        );
    }

    #[tokio::test]
    async fn attachment_is_single_use_and_closed_before_cleanup_completes() {
        let scope = InvocationScope::new();
        let attachment = scope.attachment();
        let (release, held) = oneshot::channel();
        let (entered, started) = oneshot::channel();
        attachment
            .attach(async move {
                entered.send(()).unwrap();
                held.await.unwrap();
                Ok(())
            })
            .unwrap();
        assert_eq!(attachment.attach(async { Ok(()) }), Err(AttachError::AlreadyAttached));
        let retirement = tokio::spawn(scope.retire());
        started.await.unwrap();
        assert_eq!(attachment.attach(async { Ok(()) }), Err(AttachError::Closed));
        assert!(!retirement.is_finished());
        release.send(()).unwrap();
        assert_eq!(retirement.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    #[cfg(feature = "sqlite")]
    async fn cancellation_of_invocation_body_cannot_drop_its_attachment() {
        let runtime = super::super::Runtime::start_with_options(
            Arc::new(crate::providers::sqlite::SqliteProvider::new_in_memory().await.unwrap()),
            super::super::registry::ActivityRegistry::builder().build(),
            crate::OrchestrationRegistry::builder().build(),
            super::super::RuntimeOptions {
                orchestration_concurrency: 0,
                worker_concurrency: 0,
                ..Default::default()
            },
        )
        .await;
        let scope = InvocationGuard::new(runtime.clone());
        let attachment = scope.attachment();
        let (release, held) = oneshot::channel();
        let (entered, started) = oneshot::channel();
        attachment
            .attach(async move {
                entered.send(()).unwrap();
                held.await.unwrap();
                Ok(())
            })
            .unwrap();
        let (body_entered, body_started) = oneshot::channel();
        let parent = tokio::spawn(async move {
            let _scope = scope;
            body_entered.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        body_started.await.unwrap();
        parent.abort();
        assert!(parent.await.unwrap_err().is_cancelled());
        started.await.unwrap();
        assert!(
            runtime
                .clone()
                .shutdown_with_grace(std::time::Duration::ZERO)
                .now_or_never()
                .is_none()
        );
        release.send(()).unwrap();
        assert_eq!(
            runtime.shutdown_with_grace(std::time::Duration::ZERO).await,
            Ok(super::super::ShutdownOutcome::Forced)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn attachment_and_close_have_one_serialized_boundary() {
        for _ in 0..100 {
            let scope = InvocationScope::new();
            let attachment = scope.attachment();
            let completed = Arc::new(AtomicUsize::new(0));
            let marker = Arc::clone(&completed);
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let attach_barrier = Arc::clone(&barrier);
            let attempt = tokio::spawn(async move {
                attach_barrier.wait().await;
                attachment.attach(async move {
                    marker.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });
            barrier.wait().await;
            assert_eq!(scope.retire().await, Ok(()));
            match attempt.await.unwrap() {
                Ok(()) => assert_eq!(completed.load(Ordering::SeqCst), 1),
                Err(AttachError::Closed) => assert_eq!(completed.load(Ordering::SeqCst), 0),
                other => panic!("unexpected attachment result: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn cleanup_failure_remains_an_error() {
        let scope = InvocationScope::new();
        let failure = TaskFailure {
            task: "foreign-completion",
            kind: FailureKind::Operation,
        };
        let reported = failure.clone();
        scope.attachment().attach(async move { Err(vec![reported]) }).unwrap();
        assert_eq!(scope.retire().await, Err(vec![failure]));
    }

    #[tokio::test]
    async fn finalization_is_once_outside_the_scope_lock_and_does_not_replace_completion() {
        let scope = InvocationScope::new();
        let attachment = scope.attachment();
        let finalized = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&finalized);
        let check_closed = attachment.clone();
        let (release, held) = oneshot::channel();
        attachment
            .register_with_finalization(
                async move {
                    held.await.unwrap();
                    Ok(())
                },
                move || {
                    count.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(check_closed.attach(async { Ok(()) }), Err(AttachError::Closed));
                },
            )
            .unwrap();
        assert!(attachment.finalize());
        assert!(!attachment.finalize());
        assert_eq!(finalized.load(Ordering::SeqCst), 1);
        let retirement = tokio::spawn(scope.retire());
        assert!(!retirement.is_finished());
        release.send(()).unwrap();
        assert_eq!(retirement.await.unwrap(), Ok(()));
        assert_eq!(finalized.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn finalization_poll_and_capture_destruction_faults_are_separately_retained() {
        struct PanicOnDrop;
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("private finalizer capture");
            }
        }
        let scope = InvocationScope::new();
        let capture = PanicOnDrop;
        let (release, held) = oneshot::channel();
        scope
            .attachment()
            .register_with_finalization(
                async move {
                    held.await.unwrap();
                    Ok(())
                },
                move || {
                    std::hint::black_box(&capture);
                    panic!("private finalizer body");
                },
            )
            .unwrap();
        assert!(scope.attachment().finalize());
        assert!(scope.attachment().finalization_failed());
        let retirement = tokio::spawn(scope.retire());
        assert!(!retirement.is_finished());
        release.send(()).unwrap();
        let failures = retirement.await.unwrap().unwrap_err();
        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0].kind, FailureKind::PollPanicked);
        assert_eq!(failures[1].kind, FailureKind::DestructionPanicked);
        assert!(failures.iter().all(|failure| failure.task == "invocation-finalization"));
    }

    #[tokio::test]
    async fn finalization_precedes_snapshot_and_driver_drop_without_capturing_drop_mutations() {
        finalization_and_driver_destruction(false).await;
    }

    #[tokio::test]
    async fn post_decision_driver_destruction_failure_preserves_the_valid_decision() {
        finalization_and_driver_destruction(true).await;
    }

    async fn finalization_and_driver_destruction(fail_driver: bool) {
        use crate::runtime::replay_engine::{ReplayEngine, TurnResult};
        use crate::{Action, Event, EventKind, OrchestrationContext, OrchestrationHandler};

        struct DriverDrop {
            context: OrchestrationContext,
            order: Arc<Mutex<Vec<&'static str>>>,
            fail: bool,
        }
        impl Drop for DriverDrop {
            fn drop(&mut self) {
                self.order.lock().unwrap().push("driver-drop");
                self.context.set_kv_value("after-snapshot", "must-not-be-committed");
                assert!(!self.fail, "post-decision driver destruction");
            }
        }
        struct Handler(Arc<Mutex<Vec<&'static str>>>, bool);
        #[async_trait::async_trait]
        impl OrchestrationHandler for Handler {
            async fn invoke(&self, context: OrchestrationContext, _: String) -> Result<String, String> {
                let completed = Arc::clone(&self.0);
                let finalized = Arc::clone(&self.0);
                let last_admitted_mutation = context.clone();
                context
                    .register_invocation_cleanup_with_finalization(
                        async move {
                            completed.lock().unwrap().push("completion");
                            Ok(())
                        },
                        move || {
                            finalized.lock().unwrap().push("finalization");
                            last_admitted_mutation.set_kv_value("before-snapshot", "included");
                        },
                    )
                    .unwrap();
                let _driver = DriverDrop {
                    context: context.clone(),
                    order: Arc::clone(&self.0),
                    fail: self.1,
                };
                context.schedule_activity("held", "input").await
            }
        }

        let order = Arc::new(Mutex::new(Vec::new()));
        let scope = InvocationScope::new();
        let event = Event::with_event_id(
            1,
            "fenced",
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "flow".into(),
                version: "1.0.0".into(),
                input: String::new(),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        );
        let mut engine = ReplayEngine::new("fenced".into(), 1, vec![event]);
        let result = engine.execute_orchestration_scoped(
            Arc::new(Handler(Arc::clone(&order), fail_driver)),
            String::new(),
            "flow".into(),
            "1.0.0".into(),
            "test-worker",
            scope.attachment(),
        );
        assert!(matches!(result, TurnResult::Continue));
        assert!(!scope.attachment().finalization_failed());
        assert_eq!(*order.lock().unwrap(), ["finalization", "driver-drop"]);
        assert!(engine.pending_actions.iter().any(|action| matches!(
            action, Action::SetKeyValue { key, value, .. } if key == "before-snapshot" && value == "included"
        )));
        assert!(!engine.pending_actions.iter().any(|action| matches!(
            action, Action::SetKeyValue { key, .. } if key == "after-snapshot"
        )));
        let retirement = scope.retire().await;
        if fail_driver {
            assert_eq!(
                retirement,
                Err(vec![TaskFailure {
                    task: "orchestration-driver",
                    kind: FailureKind::DestructionPanicked,
                }])
            );
        } else {
            assert_eq!(retirement, Ok(()));
        }
        assert_eq!(*order.lock().unwrap(), ["finalization", "driver-drop", "completion"]);
    }
}

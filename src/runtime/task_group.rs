// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Parent-owned children survive cancellation or unwind of the borrowed work body.
//! The owner/epilogue itself must never be aborted; only explicitly marked leaves may be.

use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;

use futures_util::FutureExt;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum TaskRole {
    GaugePoller,
    OrchestrationDispatcher,
    OrchestrationSlot,
    OrchestrationRenewal,
    WorkDispatcher,
    WorkerSlot,
    SessionManager,
    ActivityManager,
    ActivityInvocation,
}

impl TaskRole {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::GaugePoller => "gauge-poller",
            Self::OrchestrationDispatcher => "orchestration-dispatcher",
            Self::OrchestrationSlot => "orchestration-slot",
            Self::OrchestrationRenewal => "orchestration-renewal",
            Self::WorkDispatcher => "work-dispatcher",
            Self::WorkerSlot => "worker-slot",
            Self::SessionManager => "session-manager",
            Self::ActivityManager => "activity-manager",
            Self::ActivityInvocation => "activity-invocation",
        }
    }

    #[cfg(feature = "test-hooks")]
    pub const ALL: [Self; 9] = [
        Self::GaugePoller,
        Self::OrchestrationDispatcher,
        Self::OrchestrationSlot,
        Self::OrchestrationRenewal,
        Self::WorkDispatcher,
        Self::WorkerSlot,
        Self::SessionManager,
        Self::ActivityManager,
        Self::ActivityInvocation,
    ];

    /// `None` denotes direct ownership by the runtime.
    #[cfg(feature = "test-hooks")]
    pub fn parent(self) -> Option<Self> {
        match self {
            Self::GaugePoller | Self::OrchestrationDispatcher | Self::WorkDispatcher => None,
            Self::OrchestrationSlot => Some(Self::OrchestrationDispatcher),
            Self::OrchestrationRenewal => Some(Self::OrchestrationSlot),
            Self::WorkerSlot | Self::SessionManager => Some(Self::WorkDispatcher),
            Self::ActivityManager | Self::ActivityInvocation => Some(Self::WorkerSlot),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FailureKind {
    Operation,
    ConstructionPanicked,
    PollPanicked,
    DestructionPanicked,
    PanicPayloadPanicked,
    JoinPanicked,
    JoinCancelled,
    ReportingPanicked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TaskFailure {
    pub task: &'static str,
    pub kind: FailureKind,
}

pub(super) type TaskResult = Result<(), Vec<TaskFailure>>;
pub(super) type TaskFuture<'a> = Pin<Box<dyn Future<Output = TaskResult> + Send + 'a>>;

pub(super) fn append_failures(task: &'static str, mut errors: Vec<TaskFailure>, failures: &mut Vec<TaskFailure>) {
    if errors.is_empty() {
        errors.push(TaskFailure {
            task,
            kind: FailureKind::Operation,
        });
    }
    failures.extend(errors);
}

pub(super) fn record_panic(
    task: &'static str,
    kind: FailureKind,
    payload: Box<dyn Any + Send>,
    failures: &mut Vec<TaskFailure>,
) {
    failures.push(TaskFailure { task, kind });
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        failures.push(TaskFailure {
            task,
            kind: FailureKind::PanicPayloadPanicked,
        });
        // A second arbitrary panic payload cannot safely be destroyed recursively.
        std::mem::forget(secondary);
    }
}

pub(super) struct BodyReport {
    pub failures: Vec<TaskFailure>,
    pub cancelled: bool,
}

pub(super) async fn protect_future(
    task: &'static str,
    future: impl Future<Output = TaskResult> + Send,
    cancellation: &CancellationToken,
) -> BodyReport {
    protect_future_observing_panic(task, future, cancellation, |_| {}).await
}

pub(super) async fn protect_future_observing_panic(
    task: &'static str,
    future: impl Future<Output = TaskResult> + Send,
    cancellation: &CancellationToken,
    on_poll_panic: impl FnOnce(&(dyn Any + Send)) + Send,
) -> BodyReport {
    let mut future = Box::pin(future);
    let mut report = BodyReport {
        failures: Vec::new(),
        cancelled: false,
    };
    {
        // Catch polling through a borrow, so destruction happens after unwinding ends.
        let poll = AssertUnwindSafe(future.as_mut()).catch_unwind();
        tokio::select! {
            biased;
            result = poll => match result {
                Ok(Ok(())) => {}
                Ok(Err(failures)) => append_failures(task, failures, &mut report.failures),
                Err(payload) => {
                    on_poll_panic(payload.as_ref());
                    record_panic(task, FailureKind::PollPanicked, payload, &mut report.failures);
                },
            },
            () = cancellation.cancelled() => report.cancelled = true,
        }
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(future))) {
        record_panic(task, FailureKind::DestructionPanicked, payload, &mut report.failures);
    }
    report
}

struct Child {
    task: &'static str,
    handle: Option<JoinHandle<TaskResult>>,
    result: Option<TaskResult>,
    leaf: bool,
    abort_requested: bool,
    stop_before_join: Option<CancellationToken>,
}

#[derive(Clone, Copy)]
pub(super) struct TaskId(usize);

#[derive(Default)]
pub(super) struct TaskGroup {
    children: Vec<Child>,
    cleanup: Vec<(&'static str, TaskFuture<'static>)>,
}

impl TaskGroup {
    pub fn spawn(&mut self, task: &'static str, future: impl Future<Output = TaskResult> + Send + 'static) -> TaskId {
        self.add(task, future, false)
    }

    pub fn spawn_leaf(
        &mut self,
        task: &'static str,
        future: impl Future<Output = TaskResult> + Send + 'static,
    ) -> TaskId {
        self.add(task, future, true)
    }

    fn add(
        &mut self,
        task: &'static str,
        future: impl Future<Output = TaskResult> + Send + 'static,
        leaf: bool,
    ) -> TaskId {
        let id = TaskId(self.children.len());
        self.children.push(Child {
            task,
            handle: Some(tokio::spawn(future)),
            result: None,
            leaf,
            abort_requested: false,
            stop_before_join: None,
        });
        id
    }

    /// Keep maintenance running until all earlier siblings have retired.
    pub fn spawn_until_join(
        &mut self,
        task: &'static str,
        future: impl Future<Output = TaskResult> + Send + 'static,
        stop: CancellationToken,
    ) {
        let id = self.add(task, future, false);
        self.children[id.0].stop_before_join = Some(stop);
    }

    pub fn retain_cleanup(&mut self, task: &'static str, future: impl Future<Output = TaskResult> + Send + 'static) {
        self.cleanup.push((task, Box::pin(future)));
    }

    pub fn abort_leaves(&mut self) {
        for child in &mut self.children {
            if child.leaf {
                child.abort_requested = true;
                if let Some(handle) = &child.handle {
                    handle.abort();
                }
            }
        }
    }

    /// The join stays in the parent even if the borrowing work body unwinds.
    pub async fn join(&mut self, id: TaskId) -> TaskResult {
        let child = &mut self.children[id.0];
        if let Some(handle) = child.handle.as_mut() {
            if let Some(stop) = child.stop_before_join.take() {
                stop.cancel();
            }
            let joined = handle.await;
            child.handle = None;
            let mut failures = Vec::new();
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(errors)) => append_failures(child.task, errors, &mut failures),
                Err(error) if error.is_cancelled() && child.abort_requested => {}
                Err(error) if error.is_panic() => {
                    record_panic(child.task, FailureKind::JoinPanicked, error.into_panic(), &mut failures);
                }
                Err(_) => failures.push(TaskFailure {
                    task: child.task,
                    kind: FailureKind::JoinCancelled,
                }),
            }
            child.result = Some(if failures.is_empty() { Ok(()) } else { Err(failures) });
        }
        child.result.as_ref().expect("joined child has a result").clone()
    }

    async fn finish(mut self, mut failures: Vec<TaskFailure>) -> TaskResult {
        for index in 0..self.children.len() {
            if let Err(errors) = self.join(TaskId(index)).await {
                failures.extend(errors);
            }
        }
        for (task, cleanup) in self.cleanup {
            let report = protect_future(task, cleanup, &CancellationToken::new()).await;
            failures.extend(report.failures);
        }
        if failures.is_empty() { Ok(()) } else { Err(failures) }
    }
}

/// Cancellation applies only to the work body, never to owned children or cleanup.
/// Callers must not use it to cancel a body that owns non-cancel-safe provider I/O.
#[cfg(test)]
pub(super) async fn run_owned(
    task: &'static str,
    cancellation: CancellationToken,
    make_work: impl for<'a> FnOnce(&'a mut TaskGroup) -> TaskFuture<'a>,
) -> TaskResult {
    run_owned_reporting(task, cancellation, make_work, |_| {}).await
}

/// Report work faults before joining, so a held child cannot delay stop acceptance.
pub(super) async fn run_owned_reporting(
    task: &'static str,
    cancellation: CancellationToken,
    make_work: impl for<'a> FnOnce(&'a mut TaskGroup) -> TaskFuture<'a>,
    report_faults: impl Fn(&[TaskFailure]),
) -> TaskResult {
    let mut group = TaskGroup::default();
    let report = {
        match catch_unwind(AssertUnwindSafe(|| make_work(&mut group))) {
            Ok(work) => protect_future(task, work, &cancellation).await,
            Err(payload) => {
                let mut failures = Vec::new();
                record_panic(task, FailureKind::ConstructionPanicked, payload, &mut failures);
                BodyReport {
                    failures,
                    cancelled: false,
                }
            }
        }
    };
    if report.cancelled || !report.failures.is_empty() {
        group.abort_leaves();
    }
    report_faults(&report.failures);
    let result = group.finish(report.failures).await;
    if let Err(failures) = &result {
        report_faults(failures);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tokio::sync::oneshot;

    struct Marker(Arc<AtomicUsize>);

    impl Drop for Marker {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct FaultyFuture {
        panic_poll: bool,
        panic_drop: bool,
        drops: Arc<AtomicUsize>,
    }

    impl Future for FaultyFuture {
        type Output = TaskResult;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            assert!(!self.panic_poll, "injected work poll failure");
            Poll::Ready(Ok(()))
        }
    }

    impl Drop for FaultyFuture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_drop, "injected work destruction failure");
        }
    }

    #[tokio::test]
    async fn polling_and_destruction_panics_are_independently_contained() {
        for (panic_poll, panic_drop) in [(false, false), (true, false), (false, true), (true, true)] {
            let drops = Arc::new(AtomicUsize::new(0));
            let report = protect_future(
                "work",
                FaultyFuture {
                    panic_poll,
                    panic_drop,
                    drops: Arc::clone(&drops),
                },
                &CancellationToken::new(),
            )
            .await;
            let expected: Vec<_> = [
                panic_poll.then_some(FailureKind::PollPanicked),
                panic_drop.then_some(FailureKind::DestructionPanicked),
            ]
            .into_iter()
            .flatten()
            .collect();
            assert_eq!(report.failures.iter().map(|f| f.kind).collect::<Vec<_>>(), expected);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn construction_panic_still_joins_previously_registered_children() {
        let (release, held) = oneshot::channel();
        let (entered, started) = oneshot::channel();
        let owner = tokio::spawn(run_owned("parent", CancellationToken::new(), move |group| {
            group.spawn("child", async move {
                entered.send(()).unwrap();
                held.await.unwrap();
                Ok(())
            });
            panic!("injected construction failure")
        }));
        started.await.unwrap();
        assert!(!owner.is_finished());
        release.send(()).unwrap();
        let failures = owner.await.unwrap().unwrap_err();
        assert_eq!(failures[0].kind, FailureKind::ConstructionPanicked);
    }

    #[tokio::test]
    async fn parent_faults_do_not_skip_other_children_or_cleanup() {
        for kind in [
            FailureKind::Operation,
            FailureKind::PollPanicked,
            FailureKind::DestructionPanicked,
        ] {
            let markers = Arc::new(AtomicUsize::new(0));
            let child_marker = Marker(Arc::clone(&markers));
            let cleanup_marker = Marker(Arc::clone(&markers));
            let (release, held) = oneshot::channel();
            let (entered, started) = oneshot::channel();
            let owner = tokio::spawn(run_owned("parent", CancellationToken::new(), move |group| {
                group.spawn("panic-child", async { panic!("injected child failure") });
                group.spawn("held-child", async move {
                    let _marker = child_marker;
                    entered.send(()).unwrap();
                    held.await.unwrap();
                    Ok(())
                });
                group.retain_cleanup("cleanup", async move {
                    drop(cleanup_marker);
                    Ok(())
                });
                match kind {
                    FailureKind::Operation => Box::pin(async move { Err(vec![TaskFailure { task: "parent", kind }]) }),
                    FailureKind::PollPanicked | FailureKind::DestructionPanicked => Box::pin(FaultyFuture {
                        panic_poll: kind == FailureKind::PollPanicked,
                        panic_drop: kind == FailureKind::DestructionPanicked,
                        drops: Arc::new(AtomicUsize::new(0)),
                    }),
                    _ => unreachable!(),
                }
            }));
            started.await.unwrap();
            assert!(!owner.is_finished());
            assert_eq!(markers.load(Ordering::SeqCst), 0);
            release.send(()).unwrap();
            let failures = owner.await.unwrap().unwrap_err();
            assert!(failures.iter().any(|f| f.task == "parent" && f.kind == kind));
            assert!(failures.iter().any(|f| f.kind == FailureKind::JoinPanicked));
            assert_eq!(markers.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test]
    async fn cleanup_fault_does_not_skip_later_cleanup() {
        let markers = Arc::new(AtomicUsize::new(0));
        let marker = Marker(Arc::clone(&markers));
        let result = run_owned("parent", CancellationToken::new(), move |group| {
            group.retain_cleanup(
                "faulty-cleanup",
                FaultyFuture {
                    panic_poll: true,
                    panic_drop: true,
                    drops: Arc::new(AtomicUsize::new(0)),
                },
            );
            group.retain_cleanup("final-cleanup", async move {
                drop(marker);
                Ok(())
            });
            Box::pin(async { Ok(()) })
        })
        .await;
        assert_eq!(result.unwrap_err().len(), 2);
        assert_eq!(markers.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancellation_destroys_body_but_retains_foreign_cleanup() {
        let stop = CancellationToken::new();
        let (release, held) = oneshot::channel();
        let (entered, started) = oneshot::channel();
        let owner = tokio::spawn(run_owned("parent", stop.clone(), move |group| {
            group.retain_cleanup("foreign", async move {
                entered.send(()).unwrap();
                held.await.unwrap();
                Ok(())
            });
            Box::pin(std::future::pending())
        }));
        stop.cancel();
        started.await.unwrap();
        assert!(!owner.is_finished());
        release.send(()).unwrap();
        assert_eq!(owner.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn unexpected_child_cancellation_is_an_explicit_error() {
        let mut group = TaskGroup::default();
        group.spawn("unexpectedly-aborted", std::future::pending());
        group.children[0].handle.as_ref().unwrap().abort();
        assert_eq!(
            group.finish(Vec::new()).await,
            Err(vec![TaskFailure {
                task: "unexpectedly-aborted",
                kind: FailureKind::JoinCancelled,
            }])
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_request_does_not_complete_a_blocking_leaf_or_abort_an_owner() {
        let (release, held) = std::sync::mpsc::channel();
        let (entered, started) = oneshot::channel();
        let (owner_release, owner_held) = oneshot::channel();
        let (owner_entered, owner_started) = oneshot::channel();
        let mut group = TaskGroup::default();
        group.spawn_leaf("blocking-leaf", async move {
            tokio::task::block_in_place(|| {
                entered.send(()).unwrap();
                held.recv().unwrap();
            });
            Ok(())
        });
        group.spawn("owned-parent", async move {
            owner_entered.send(()).unwrap();
            owner_held.await.unwrap();
            Ok(())
        });
        started.await.unwrap();
        owner_started.await.unwrap();
        group.abort_leaves();
        assert!(group.children[0].abort_requested);
        assert!(!group.children[0].handle.as_ref().unwrap().is_finished());
        assert!(!group.children[1].abort_requested);
        let parent = tokio::spawn(group.finish(Vec::new()));
        assert!(!parent.is_finished());
        release.send(()).unwrap();
        assert!(!parent.is_finished());
        owner_release.send(()).unwrap();
        assert_eq!(parent.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn empty_error_lists_cannot_become_success() {
        let report = protect_future("body", async { Err(Vec::new()) }, &CancellationToken::new()).await;
        assert_eq!(
            report.failures,
            vec![TaskFailure {
                task: "body",
                kind: FailureKind::Operation
            }]
        );
        let mut group = TaskGroup::default();
        group.spawn("child", async { Err(Vec::new()) });
        assert!(group.finish(Vec::new()).await.is_err());
    }

    #[tokio::test]
    async fn panic_payload_destruction_is_reported_separately() {
        struct Payload;
        impl Drop for Payload {
            fn drop(&mut self) {
                panic!("injected payload destructor failure");
            }
        }
        let report = protect_future(
            "payload",
            async { std::panic::panic_any(Payload) },
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(
            report.failures.iter().map(|failure| failure.kind).collect::<Vec<_>>(),
            vec![FailureKind::PollPanicked, FailureKind::PanicPayloadPanicked],
        );
    }
}

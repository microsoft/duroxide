// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::FutureExt;
use tokio_util::sync::CancellationToken;

use super::Runtime;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(usize)]
#[non_exhaustive]
pub enum TaskRole {
    GaugePoller,
    OrchestrationSlot,
    OrchestrationRenewal,
    WorkerSlot,
    SessionManager,
    ActivityManager,
    ActivityInvocation,
}

impl TaskRole {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::GaugePoller => "gauge-poller",
            Self::OrchestrationSlot => "orchestration-slot",
            Self::OrchestrationRenewal => "orchestration-renewal",
            Self::WorkerSlot => "worker-slot",
            Self::SessionManager => "session-manager",
            Self::ActivityManager => "activity-manager",
            Self::ActivityInvocation => "activity-invocation",
        }
    }

    #[cfg(feature = "test-hooks")]
    pub const ALL: [Self; 7] = [
        Self::GaugePoller,
        Self::OrchestrationSlot,
        Self::OrchestrationRenewal,
        Self::WorkerSlot,
        Self::SessionManager,
        Self::ActivityManager,
        Self::ActivityInvocation,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FailureKind {
    Operation,
    PollPanicked,
    DestructionPanicked,
    PanicPayloadPanicked,
    JoinPanicked,
    ReportingPanicked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TaskFailure {
    pub task: &'static str,
    pub kind: FailureKind,
}

pub(super) type TaskResult = Result<(), Vec<TaskFailure>>;
pub(super) type TaskFuture<'a> = Pin<Box<dyn Future<Output = TaskResult> + Send + 'a>>;

pub(super) struct UserOperation {
    pub instance: String,
    pub execution_id: u64,
    pub name: String,
    pub activity_id: Option<u64>,
}

struct UserPayloadDisposal {
    failures: u8,
    skipped: bool,
}

fn dispose_user_panic_payload(mut payload: Box<dyn Any + Send>) -> UserPayloadDisposal {
    let mut failures = 0;
    for _ in 0..2 {
        match catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            Ok(()) => {
                return UserPayloadDisposal {
                    failures,
                    skipped: false,
                };
            }
            Err(next) => {
                failures += 1;
                payload = next;
            }
        }
    }
    // A destructor can keep producing new panicking payloads. Retain the last
    // allocation rather than attempting unbounded cleanup; report it as skipped.
    std::mem::forget(payload);
    UserPayloadDisposal {
        failures,
        skipped: true,
    }
}

pub(super) fn contain_user_panic(
    identity: &UserOperation,
    stage: &'static str,
    payload: Box<dyn Any + Send>,
) -> Vec<TaskFailure> {
    report_user_cleanup(identity, stage, dispose_user_panic_payload(payload))
}

pub(super) fn dispose_user_payload(
    identity: &UserOperation,
    stage: &'static str,
    payload: Box<dyn Any + Send>,
) -> Vec<TaskFailure> {
    let disposal = dispose_user_panic_payload(payload);
    if disposal.failures == 0 {
        Vec::new()
    } else {
        report_user_cleanup(identity, stage, disposal)
    }
}

fn report_user_cleanup(
    identity: &UserOperation,
    stage: &'static str,
    disposal: UserPayloadDisposal,
) -> Vec<TaskFailure> {
    let payload_fate = if disposal.skipped {
        "cleanup-skipped"
    } else if disposal.failures > 0 {
        "secondary-disposed"
    } else {
        "disposed"
    };
    let mut failures = Vec::new();
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
        tracing::error!(
            target: "duroxide::runtime::cleanup",
            category = "contained_user_cleanup",
            instance = %identity.instance,
            execution_id = identity.execution_id,
            operation = %identity.name,
            activity_id = ?identity.activity_id,
            task_id = ?tokio::task::try_id(),
            stage,
            disposition = "operation-local",
            payload_fate,
            payload_disposal_failures = disposal.failures,
            payload_cleanup_skipped = disposal.skipped,
            "User cleanup failed; application state is not repaired"
        );
    })) {
        record_panic(
            "user-cleanup-reporting",
            FailureKind::ReportingPanicked,
            payload,
            &mut failures,
        );
    }
    failures
}

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
        std::mem::forget(secondary);
    }
}

/// Keep destruction inside the tracked future, including cancellation/unwind.
pub(super) struct TrackedTask<F> {
    future: Option<Pin<Box<F>>>,
    runtime: Arc<Runtime>,
    name: &'static str,
}

impl<F> TrackedTask<F> {
    pub fn new(runtime: Arc<Runtime>, name: &'static str, future: F) -> Self {
        Self {
            future: Some(Box::pin(future)),
            runtime,
            name,
        }
    }

    fn fail(&self, kind: FailureKind, payload: Box<dyn Any + Send>) {
        let mut failures = Vec::new();
        record_panic(self.name, kind, payload, &mut failures);
        self.runtime.record_faults(&failures);
    }

    fn destroy(&mut self) {
        if let Some(future) = self.future.take()
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(future)))
        {
            self.fail(FailureKind::DestructionPanicked, payload);
        }
    }
}

impl<F: Future> Future for TrackedTask<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match catch_unwind(AssertUnwindSafe(|| {
            this.future
                .as_mut()
                .expect("task polled after completion")
                .as_mut()
                .poll(cx)
        })) {
            Ok(Poll::Ready(result)) => {
                this.destroy();
                Poll::Ready(result)
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                this.fail(FailureKind::PollPanicked, payload);
                this.destroy();
                std::panic::resume_unwind(Box::new("runtime task failed"))
            }
        }
    }
}

impl<F> Drop for TrackedTask<F> {
    fn drop(&mut self) {
        self.destroy();
    }
}

pub(super) async fn protect_future(
    task: &'static str,
    future: impl Future<Output = TaskResult> + Send,
    cancellation: &CancellationToken,
) -> TaskResult {
    let mut future = Box::pin(future);
    let mut failures = Vec::new();
    {
        let poll = AssertUnwindSafe(future.as_mut()).catch_unwind();
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {},
            result = poll => match result {
                Ok(Ok(())) => {}
                Ok(Err(errors)) => append_failures(task, errors, &mut failures),
                Err(payload) => record_panic(task, FailureKind::PollPanicked, payload, &mut failures),
            },
        }
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(future))) {
        record_panic(task, FailureKind::DestructionPanicked, payload, &mut failures);
    }
    if failures.is_empty() { Ok(()) } else { Err(failures) }
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::clone_on_ref_ptr)]

#[path = "replay_engine/helpers.rs"]
#[allow(dead_code)]
mod helpers;

#[path = "common/cancellation_history.rs"]
mod cancellation_history;

use async_trait::async_trait;
use duroxide::providers::WorkItem;
use duroxide::runtime::replay_engine::TurnResult;
use duroxide::{Action, Event, EventKind, OrchestrationContext, OrchestrationHandler};
use helpers::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const RUNTIME_DEADLINE: Duration = Duration::from_secs(30);
const HISTORY_DEADLINE_MS: u64 = 30_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arm {
    Q,
    Q2,
    Signal,
    Signal2,
    Timer,
    Activity,
    Child,
}

// Contract-specific expectations belong here, not in public provider validation.
// Signals require a bound open slot. Same-batch signals before binding are
// early; an unpolled dropped holder discards its assigned stale answer.
fn assert_signal_contract_read(observations: &[Observation], fresh: &str) {
    let expected = fresh;
    assert!(
        observations
            .last()
            .unwrap()
            .live_reads
            .contains(&Read::Signal(expected.into())),
        "bound-slot signal contract expected {expected}: {observations:#?}"
    );
}

#[test]
fn ms1_positional_barrier_delivers_fresh_signal_and_replays() {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_and_replay(
            stamp,
            Handler::new(Shape::PositionalBarrier, 1, vec![]),
            &[
                vec![Completion::Timer(0), Completion::Signal("a")],
                vec![Completion::Activity(0)],
                vec![Completion::Signal("b")],
            ],
        );
        if stamp == "0.1.31" {
            assert_stable(&observations, "MS1 fixed");
            assert_signal_contract_read(&observations, "b");
            assert!(
                observations
                    .last()
                    .unwrap()
                    .live_reads
                    .contains(&Read::Signal("b".into()))
            );
            assert!(
                !observations
                    .last()
                    .unwrap()
                    .live_reads
                    .contains(&Read::Signal("a".into()))
            );
        } else {
            assert!(
                observations
                    .iter()
                    .any(|o| o.replay.contains("history schedule but no emitted action"))
            );
        }
    }
}

#[test]
fn ms3_resolve_at_creation_discards_dropped_slots_old_value() {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_and_replay(
            stamp,
            Handler::new(Shape::ResolveAtCreation, 1, vec![]),
            &[
                vec![Completion::Signal("a"), Completion::Activity(0)],
                vec![Completion::Activity(1)],
                vec![Completion::Signal("b")],
            ],
        );
        if stamp == "0.1.31" {
            assert_stable(&observations, "MS3 fixed");
            assert_signal_contract_read(&observations, "b");
            assert!(
                observations
                    .last()
                    .unwrap()
                    .live_reads
                    .contains(&Read::Signal("b".into()))
            );
        } else {
            assert!(
                observations
                    .iter()
                    .any(|o| o.replay.contains("history schedule but no emitted action"))
            );
        }
    }
}

#[tokio::test]
async fn ms4_sqlite_continue_as_new_preserves_unconsumed_queue_messages() {
    use duroxide::runtime::registry::ActivityRegistry;
    use duroxide::runtime::{Runtime, RuntimeOptions};
    use duroxide::{Client, OrchestrationRegistry, OrchestrationStatus};
    for stamp in ["0.1.30", "0.1.31"] {
        for (drain, held) in [(false, false), (true, false), (false, true), (true, true)] {
            let (store, _temporary) = common::create_sqlite_store_disk().await;
            let mut start = started_event(1);
            start.duroxide_version = stamp.into();
            if let EventKind::OrchestrationStarted { input, .. } = &mut start.kind {
                *input = drain.to_string();
            }
            common::seed_history_turn(
                store.as_ref(),
                WorkItem::StartOrchestration {
                    instance: TEST_INSTANCE.into(),
                    orchestration: TEST_ORCH_NAME.into(),
                    input: drain.to_string(),
                    version: Some(TEST_ORCH_VERSION.into()),
                    parent_instance: None,
                    parent_id: None,
                    parent_execution_id: None,
                    execution_id: 1,
                },
                1,
                vec![
                    start,
                    Event::with_event_id(
                        2,
                        TEST_INSTANCE,
                        1,
                        None,
                        EventKind::QueueSubscribed { name: "q".into() },
                    ),
                    Event::with_event_id(3, TEST_INSTANCE, 1, None, EventKind::TimerCreated { fire_at_ms: 1000 }),
                ],
                if drain {
                    vec![
                        timer_fired_msg(3, 1000),
                        WorkItem::QueueMessage {
                            instance: TEST_INSTANCE.into(),
                            name: "q".into(),
                            data: "m1".into(),
                        },
                        WorkItem::QueueMessage {
                            instance: TEST_INSTANCE.into(),
                            name: "q".into(),
                            data: "m2".into(),
                        },
                    ]
                } else {
                    vec![
                        timer_fired_msg(3, 1000),
                        WorkItem::QueueMessage {
                            instance: TEST_INSTANCE.into(),
                            name: "q".into(),
                            data: "m1".into(),
                        },
                    ]
                },
                duroxide::providers::ExecutionMetadata {
                    orchestration_name: Some(TEST_ORCH_NAME.into()),
                    orchestration_version: Some(TEST_ORCH_VERSION.into()),
                    ..Default::default()
                },
            )
            .await;
            let registry = OrchestrationRegistry::builder()
                .register(
                    TEST_ORCH_NAME,
                    move |ctx: OrchestrationContext, input: String| async move {
                        if ctx.execution_id() == 1 {
                            if held {
                                let mut wait = ctx.dequeue_event("q");
                                let _ = ctx
                                    .select2(&mut wait, ctx.schedule_timer(Duration::from_millis(10)))
                                    .await;
                                if drain {
                                    ctx.dequeue_event("q").await;
                                }
                                return ctx.continue_as_new("successor").await;
                            }
                            let _ = ctx
                                .select2(ctx.dequeue_event("q"), ctx.schedule_timer(Duration::from_millis(10)))
                                .await;
                            if input == "true" {
                                ctx.dequeue_event("q").await;
                            }
                            ctx.continue_as_new("successor").await
                        } else {
                            let a = ctx.dequeue_event("q").await;
                            Ok(a)
                        }
                    },
                )
                .build();
            let runtime = Runtime::start_with_options(
                store.clone(),
                ActivityRegistry::builder().build(),
                registry,
                RuntimeOptions {
                    dispatcher_min_poll_interval: Duration::from_millis(5),
                    ..Default::default()
                },
            )
            .await;
            let client = Client::new(store.clone());
            assert!(
                common::wait_for_history(
                    store.clone(),
                    TEST_INSTANCE,
                    |history| history.first().is_some_and(|event| event.execution_id == 2),
                    HISTORY_DEADLINE_MS
                )
                .await
            );
            if stamp == "0.1.30" {
                client.enqueue_event(TEST_INSTANCE, "q", "fresh").await.unwrap();
            }
            let result = client
                .wait_for_orchestration(TEST_INSTANCE, RUNTIME_DEADLINE)
                .await
                .unwrap();
            let expected = if stamp == "0.1.30" {
                "fresh"
            } else if drain && !held {
                "m2"
            } else {
                "m1"
            };
            assert!(
                matches!(&result,OrchestrationStatus::Completed{output,..}if output==expected),
                "stamp={stamp} drain={drain}: {result:?}"
            );
            let old = client.read_execution_history(TEST_INSTANCE, 1).await.unwrap();
            let cancels = old
                .iter()
                .filter(|event| {
                    matches!(&event.kind,
                EventKind::QueueSubscriptionCancelled{reason}if reason=="dropped_future")
                })
                .count();
            assert_eq!(cancels, 0, "continue-as-new writes no dropped_future rows");
            runtime.shutdown(None).await;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Read {
    Queue(&'static str, String),
    Signal(String),
    Signal2(String),
    Timer,
    Activity(String),
    Child(String),
}

type Branch<'a> = Pin<Box<dyn Future<Output = Read> + Send + 'a>>;

struct TrackedBranch<'a> {
    inner: Branch<'a>,
    arm: Arm,
    index: Option<usize>,
    ready: bool,
    lifecycle: Arc<Mutex<Vec<(Arm, bool)>>>,
}
impl Future for TrackedBranch<'_> {
    type Output = Read;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Read> {
        let this = self.get_mut();
        if this.index.is_none() {
            let mut log = this.lifecycle.lock().unwrap();
            this.index = Some(log.len());
            log.push((this.arm, false));
        }
        let result = this.inner.as_mut().poll(cx);
        this.ready = result.is_ready();
        result
    }
}
impl Drop for TrackedBranch<'_> {
    fn drop(&mut self) {
        if !self.ready
            && let Some(index) = self.index
        {
            self.lifecycle.lock().unwrap()[index].1 = true;
        }
    }
}

#[derive(Clone, Debug)]
enum Shape {
    Race(Vec<Arm>),
    PartialJoin,
    Nested,
    UnboundDrop,
    UnboundImmediate,
    BoundDrop,
    SignalBefore,
    Timers,
    Continue,
    PositionalContinue,
    PositionalBarrier,
    ResolveAtCreation,
    HeldContinue(bool),
    QuiescenceDrop,
    ContinueWithOtherDrops,
    WrongNameBeforeWait,
    UnboundSignalDrop,
    UnboundSignalDropBarrier,
    HeldSignalWait(bool),
    LazySignalCompetition,
    DeadlineSignals,
}

struct Handler {
    shape: Shape,
    iterations: usize,
    drain: Vec<Arm>,
    reads: Arc<Mutex<Vec<Read>>>,
    eager: bool,
    expected_queue_counts: Option<(usize, usize)>,
    lifecycle: Arc<Mutex<Vec<(Arm, bool)>>>,
    logical_cancellations: Arc<Mutex<Vec<usize>>>,
}

impl Handler {
    fn new(shape: Shape, iterations: usize, drain: Vec<Arm>) -> Arc<Self> {
        Arc::new(Self {
            shape,
            iterations,
            drain,
            reads: Arc::new(Mutex::new(Vec::new())),
            eager: false,
            expected_queue_counts: None,
            lifecycle: Default::default(),
            logical_cancellations: Default::default(),
        })
    }

    fn branch<'a>(&'a self, ctx: &'a OrchestrationContext, arm: Arm) -> Branch<'a> {
        let inner = self.branch_inner(ctx, arm);
        if !matches!(self.shape, Shape::Race(_)) {
            return inner;
        }
        let index = if self.eager {
            let mut log = self.lifecycle.lock().unwrap();
            let index = log.len();
            log.push((arm, false));
            Some(index)
        } else {
            None
        };
        Box::pin(TrackedBranch {
            inner,
            arm,
            index,
            ready: false,
            lifecycle: self.lifecycle.clone(),
        })
    }

    fn branch_inner<'a>(&'a self, ctx: &'a OrchestrationContext, arm: Arm) -> Branch<'a> {
        if self.eager {
            let reads = self.reads.clone();
            let record = move |read: Read| {
                reads.lock().unwrap().push(read.clone());
                read
            };
            return match arm {
                Arm::Q => Box::pin(ctx.dequeue_event("q").map(move |value| record(Read::Queue("q", value)))),
                Arm::Q2 => Box::pin(
                    ctx.dequeue_event("q2")
                        .map(move |value| record(Read::Queue("q2", value))),
                ),
                Arm::Signal => Box::pin(ctx.schedule_wait("s").map(move |value| record(Read::Signal(value)))),
                Arm::Signal2 => Box::pin(ctx.schedule_wait("s2").map(move |value| record(Read::Signal2(value)))),
                Arm::Timer => Box::pin(
                    ctx.schedule_timer(Duration::from_millis(10))
                        .map(move |()| record(Read::Timer)),
                ),
                Arm::Activity => Box::pin(
                    ctx.schedule_activity("a", "")
                        .map(move |value| record(Read::Activity(value.unwrap()))),
                ),
                Arm::Child => Box::pin(
                    ctx.schedule_sub_orchestration("child", "")
                        .map(move |value| record(Read::Child(value.unwrap()))),
                ),
            };
        }
        Box::pin(async move {
            let read = match arm {
                Arm::Q => Read::Queue("q", ctx.dequeue_event("q").await),
                Arm::Q2 => Read::Queue("q2", ctx.dequeue_event("q2").await),
                Arm::Signal => Read::Signal(ctx.schedule_wait("s").await),
                Arm::Signal2 => Read::Signal2(ctx.schedule_wait("s2").await),
                Arm::Timer => {
                    ctx.schedule_timer(Duration::from_millis(10)).await;
                    Read::Timer
                }
                Arm::Activity => Read::Activity(ctx.schedule_activity("a", "").await.unwrap()),
                Arm::Child => Read::Child(ctx.schedule_sub_orchestration("child", "").await.unwrap()),
            };
            self.reads.lock().unwrap().push(read.clone());
            read
        })
    }

    async fn race(&self, ctx: &OrchestrationContext, arms: &[Arm]) -> Read {
        let winner = match arms {
            [a] => self.branch(ctx, *a).await,
            [a, b] => {
                ctx.select2(self.branch(ctx, *a), self.branch(ctx, *b))
                    .await
                    .into_tuple()
                    .1
            }
            [a, b, c] => {
                ctx.select3(self.branch(ctx, *a), self.branch(ctx, *b), self.branch(ctx, *c))
                    .await
                    .into_tuple()
                    .1
            }
            _ => panic!("one to three arms required"),
        };
        // Freeze logical drops here. Disposing the in-memory replay future at
        // the turn boundary must not masquerade as orchestration cancellation.
        if matches!(self.shape, Shape::Race(_)) {
            *self.logical_cancellations.lock().unwrap() = self
                .lifecycle
                .lock()
                .unwrap()
                .iter()
                .enumerate()
                .filter_map(|(index, (arm, cancelled))| (*cancelled && *arm != Arm::Timer).then_some(index))
                .collect();
        }
        winner
    }
}

#[async_trait]
impl OrchestrationHandler for Handler {
    async fn invoke(&self, ctx: OrchestrationContext, _: String) -> Result<String, String> {
        self.reads.lock().unwrap().clear();
        self.lifecycle.lock().unwrap().clear();
        self.logical_cancellations.lock().unwrap().clear();
        for _ in 0..self.iterations {
            match &self.shape {
                Shape::Race(arms) => {
                    self.race(&ctx, arms).await;
                }
                Shape::PartialJoin => {
                    let join = ctx.join2(self.branch(&ctx, Arm::Q), self.branch(&ctx, Arm::Q));
                    let _ = ctx.select2(join, self.branch(&ctx, Arm::Timer)).await;
                }
                Shape::Nested => {
                    let inner = ctx.select2(self.branch(&ctx, Arm::Q), self.branch(&ctx, Arm::Timer));
                    let _ = ctx
                        .select2(inner, async {
                            ctx.schedule_timer(Duration::from_millis(100)).await;
                            self.reads.lock().unwrap().push(Read::Timer);
                        })
                        .await;
                }
                Shape::UnboundDrop => {
                    drop(ctx.dequeue_event("q"));
                    self.branch(&ctx, Arm::Timer).await;
                }
                Shape::UnboundImmediate => {
                    drop(ctx.dequeue_event("q"));
                    self.branch(&ctx, Arm::Q).await;
                }
                Shape::BoundDrop => {
                    let wait = ctx.dequeue_event("q");
                    self.branch(&ctx, Arm::Timer).await;
                    drop(wait);
                }
                Shape::SignalBefore => {
                    self.branch(&ctx, Arm::Timer).await;
                    self.branch(&ctx, Arm::Signal).await;
                }
                Shape::Timers => {
                    let _ = ctx
                        .select2(
                            ctx.schedule_timer(Duration::from_millis(10)),
                            ctx.schedule_timer(Duration::from_millis(100)),
                        )
                        .await;
                }
                Shape::Continue => {
                    let _ = ctx
                        .select2(ctx.dequeue_event("q"), ctx.schedule_timer(Duration::from_millis(10)))
                        .await;
                    return ctx.continue_as_new("next").await;
                }
                Shape::PositionalContinue => {
                    let _ = ctx
                        .select2(ctx.schedule_wait("s"), ctx.schedule_timer(Duration::from_millis(10)))
                        .await;
                    return ctx.continue_as_new("next").await;
                }
                Shape::PositionalBarrier => {
                    self.race(&ctx, &[Arm::Signal, Arm::Timer]).await;
                    ctx.schedule_activity("tick", "").await?;
                    self.branch(&ctx, Arm::Signal).await;
                }
                Shape::ResolveAtCreation => {
                    let wait = ctx.schedule_wait("s");
                    ctx.schedule_activity("work", "").await?;
                    drop(wait);
                    ctx.schedule_activity("work2", "").await?;
                    self.branch(&ctx, Arm::Signal).await;
                }
                Shape::HeldContinue(read_newer) => {
                    let mut held = ctx.dequeue_event("q");
                    let _ = ctx
                        .select2(&mut held, ctx.schedule_timer(Duration::from_millis(10)))
                        .await;
                    if *read_newer {
                        self.branch(&ctx, Arm::Q).await;
                    }
                    return ctx.continue_as_new("next").await;
                }
                Shape::QuiescenceDrop => {
                    let held = ctx.dequeue_event("q2");
                    self.branch(&ctx, Arm::Q).await;
                    drop(held);
                    self.branch(&ctx, Arm::Q2).await;
                }
                Shape::ContinueWithOtherDrops => {
                    let activity = ctx.schedule_activity("held", "");
                    let child = ctx.schedule_sub_orchestration("held-child", "");
                    let queue = ctx.dequeue_event("q");
                    let signal = ctx.schedule_wait("s");
                    ctx.schedule_timer(Duration::from_millis(10)).await;
                    drop((activity, child, queue, signal));
                    return ctx.continue_as_new("next").await;
                }
                Shape::WrongNameBeforeWait => {
                    self.branch(&ctx, Arm::Timer).await;
                    self.branch(&ctx, Arm::Signal).await;
                    self.branch(&ctx, Arm::Activity).await;
                    self.branch(&ctx, Arm::Signal2).await;
                }
                Shape::UnboundSignalDrop | Shape::UnboundSignalDropBarrier => {
                    self.branch(&ctx, Arm::Timer).await;
                    drop(ctx.schedule_wait("s"));
                    if matches!(self.shape, Shape::UnboundSignalDropBarrier) {
                        self.branch(&ctx, Arm::Activity).await;
                    }
                    self.branch(&ctx, Arm::Signal).await;
                }
                Shape::HeldSignalWait(drop_held) => {
                    let held = ctx.schedule_wait("s");
                    self.branch(&ctx, Arm::Timer).await;
                    if *drop_held {
                        drop(held);
                        self.branch(&ctx, Arm::Signal).await;
                    } else {
                        let value = held.await;
                        self.reads.lock().unwrap().push(Read::Signal(value));
                    }
                }
                Shape::LazySignalCompetition => {
                    let validate = ctx.schedule_activity("validate", "");
                    let prepare = ctx.schedule_activity("prepare", "");
                    let _ = ctx
                        .select2(
                            async {
                                validate.await.unwrap();
                                self.branch(&ctx, Arm::Signal).await
                            },
                            async {
                                prepare.await.unwrap();
                                self.branch(&ctx, Arm::Signal2).await
                            },
                        )
                        .await;
                }
                Shape::DeadlineSignals => {
                    let mut deadline = ctx.schedule_timer(Duration::from_secs(30));
                    loop {
                        if matches!(
                            ctx.select2(self.branch(&ctx, Arm::Signal), &mut deadline).await,
                            duroxide::Either2::Second(())
                        ) {
                            break;
                        }
                    }
                    self.branch(&ctx, Arm::Signal).await;
                }
            }
        }
        for arm in &self.drain {
            self.branch(&ctx, *arm).await;
        }
        if let Some((q_count, q2_count)) = self.expected_queue_counts {
            for (arm, queue, total) in [(Arm::Q, "q", q_count), (Arm::Q2, "q2", q2_count)] {
                while self
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|read| matches!(read,Read::Queue(name,_)if *name==queue))
                    .count()
                    < total
                {
                    self.branch(&ctx, arm).await;
                }
            }
        }
        // Persist consumption in an ordinary activity input so replay must match it,
        // even when the handler has not yet completed.
        let payload = format!("{:?}", self.reads.lock().unwrap());
        ctx.schedule_activity("checkpoint", &payload).await?;
        Ok(payload)
    }
}

#[derive(Clone, Debug)]
enum Completion {
    Q(&'static str),
    Q2(&'static str),
    Signal(&'static str),
    Signal2(&'static str),
    Timer(usize),
    Activity(usize),
    Child(usize),
}

fn make_messages(history: &[Event], batch: &[Completion]) -> Vec<WorkItem> {
    batch
        .iter()
        .map(|completion| match completion {
            Completion::Q(data) | Completion::Q2(data) => WorkItem::QueueMessage {
                instance: TEST_INSTANCE.into(),
                name: if matches!(completion, Completion::Q(_)) {
                    "q"
                } else {
                    "q2"
                }
                .into(),
                data: (*data).into(),
            },
            Completion::Signal(data) => external_raised_msg("s", data),
            Completion::Signal2(data) => external_raised_msg("s2", data),
            Completion::Timer(index) => {
                let event = history
                    .iter()
                    .filter(|e| matches!(e.kind, EventKind::TimerCreated { .. }))
                    .nth(*index)
                    .unwrap();
                let EventKind::TimerCreated { fire_at_ms } = event.kind else {
                    unreachable!()
                };
                timer_fired_msg(event.event_id, fire_at_ms)
            }
            Completion::Activity(index) => {
                let event = history
                    .iter()
                    .filter(|e| matches!(e.kind, EventKind::ActivityScheduled { .. }))
                    .nth(*index)
                    .unwrap();
                activity_completed_msg(event.event_id, "ok")
            }
            Completion::Child(index) => {
                let event = history
                    .iter()
                    .filter(|e| matches!(e.kind, EventKind::SubOrchestrationScheduled { .. }))
                    .nth(*index)
                    .unwrap();
                sub_orch_completed_msg(event.event_id, "ok")
            }
        })
        .collect()
}

fn outcome(result: &TurnResult) -> String {
    match result {
        TurnResult::Failed(error) => format!("Failed: {}", error.display_message()),
        other => format!("{other:?}"),
    }
}

fn action_shapes(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .map(|action| match action {
            Action::CreateTimer {
                scheduling_event_id, ..
            } => format!("Timer:{scheduling_event_id}"),
            _ => format!("{action:?}"),
        })
        .collect()
}

#[derive(Clone, Debug)]
struct Observation {
    live: String,
    replay: String,
    live_reads: Vec<Read>,
    logical_cancellations: Option<Vec<usize>>,
    replay_reads: Vec<Read>,
    live_actions: Vec<String>,
    replay_actions: Vec<String>,
    replay_delta: Vec<Event>,
    history: Vec<Event>,
}

fn record_and_replay(stamp: &str, handler: Arc<Handler>, batches: &[Vec<Completion>]) -> Vec<Observation> {
    record_with_initial_batch(stamp, handler, batches, &[])
}

fn record_with_initial_batch(
    stamp: &str,
    handler: Arc<Handler>,
    batches: &[Vec<Completion>],
    initial: &[Completion],
) -> Vec<Observation> {
    record_with_fresh_completions(stamp, handler, batches, initial, false)
}

fn record_with_fresh_completions(
    stamp: &str,
    handler: Arc<Handler>,
    batches: &[Vec<Completion>],
    initial: &[Completion],
    complete_last: bool,
) -> Vec<Observation> {
    let mut start = started_event(1);
    start.duroxide_version = stamp.into();
    let mut history = vec![start];
    let mut observations = Vec::new();
    let initial = initial.to_vec();
    for (turn, batch) in std::iter::once(&initial).chain(batches.iter()).enumerate() {
        let mut engine = create_engine(history.clone());
        let mut completions = batch.clone();
        if complete_last && turn == batches.len() {
            let finished: std::collections::HashSet<_> = history
                .iter()
                .filter(|event| {
                    matches!(
                        event.kind,
                        EventKind::TimerFired { .. }
                            | EventKind::ActivityCompleted { .. }
                            | EventKind::ActivityFailed { .. }
                            | EventKind::SubOrchestrationCompleted { .. }
                            | EventKind::SubOrchestrationFailed { .. }
                            | EventKind::ActivityCancelRequested { .. }
                            | EventKind::SubOrchestrationCancelRequested { .. }
                    )
                })
                .filter_map(|event| event.source_event_id)
                .collect();
            for arm in [Arm::Timer, Arm::Activity, Arm::Child] {
                for (index, event) in history
                    .iter()
                    .filter(|event| match arm {
                        Arm::Timer => matches!(event.kind, EventKind::TimerCreated { .. }),
                        Arm::Activity => matches!(event.kind, EventKind::ActivityScheduled { .. }),
                        Arm::Child => matches!(event.kind, EventKind::SubOrchestrationScheduled { .. }),
                        _ => unreachable!(),
                    })
                    .enumerate()
                {
                    let already_supplied = completions.iter().any(|completion| match (arm, completion) {
                        (Arm::Timer, Completion::Timer(i))
                        | (Arm::Activity, Completion::Activity(i))
                        | (Arm::Child, Completion::Child(i)) => *i == index,
                        _ => false,
                    });
                    if !finished.contains(&event.event_id) && !already_supplied {
                        completions.push(match arm {
                            Arm::Timer => Completion::Timer(index),
                            Arm::Activity => Completion::Activity(index),
                            Arm::Child => Completion::Child(index),
                            _ => unreachable!(),
                        });
                    }
                }
            }
        }
        engine.prep_completions(make_messages(&history, &completions));
        let live = execute(&mut engine, handler.clone());
        let live_reads = handler.reads.lock().unwrap().clone();
        let logical_cancellations =
            matches!(handler.shape, Shape::Race(_)).then(|| handler.logical_cancellations.lock().unwrap().clone());
        history.extend_from_slice(engine.history_delta());
        let mut replay = create_engine(history.clone());
        let replay_result = execute(&mut replay, handler.clone());
        let replay_reads = handler.reads.lock().unwrap().clone();
        observations.push(Observation {
            live: outcome(&live),
            replay: outcome(&replay_result),
            live_reads,
            logical_cancellations,
            replay_reads,
            live_actions: action_shapes(engine.pending_actions()),
            replay_actions: action_shapes(replay.pending_actions()),
            replay_delta: replay.history_delta().to_vec(),
            history: history.clone(),
        });
        if matches!(
            live,
            TurnResult::Failed(_) | TurnResult::Completed(_) | TurnResult::ContinueAsNew { .. }
        ) {
            break;
        }
    }
    observations
}

fn assert_stable(observations: &[Observation], scenario: &str) {
    for observation in observations {
        assert!(
            !observation.live.starts_with("Failed"),
            "{scenario}: live run failed: {observation:#?}"
        );
        assert!(
            !observation.replay.starts_with("Failed"),
            "{scenario}: replay failed: {observation:#?}"
        );
        assert_eq!(observation.live, observation.replay, "{scenario}: {observation:#?}");
        assert_eq!(
            observation.live_reads, observation.replay_reads,
            "{scenario}: read sequence changed"
        );
        assert!(
            observation.replay_actions.is_empty(),
            "{scenario}: replay emitted new decisions: {observation:#?}"
        );
        assert!(
            observation.replay_delta.is_empty(),
            "{scenario}: replay emitted history: {observation:#?}"
        );
        let mut canceled = std::collections::HashSet::new();
        for event in &observation.history {
            if matches!(&event.kind, EventKind::QueueSubscriptionCancelled { reason } | EventKind::ExternalSubscribedCancelled { reason }
                | EventKind::ActivityCancelRequested {reason} | EventKind::SubOrchestrationCancelRequested {reason}
                if reason == "dropped_future")
            {
                assert!(
                    canceled.insert(event.source_event_id),
                    "{scenario}: duplicate cancel breadcrumb"
                );
                assert!(
                    observation
                        .history
                        .iter()
                        .any(|schedule| Some(schedule.event_id) == event.source_event_id),
                    "{scenario}: cancellation has no bound schedule"
                );
            }
        }
        if let Some(indices) = &observation.logical_cancellations {
            let schedules: Vec<_> = observation
                .history
                .iter()
                .filter(|e| match &e.kind {
                    EventKind::QueueSubscribed { .. }
                    | EventKind::ExternalSubscribed { .. }
                    | EventKind::TimerCreated { .. }
                    | EventKind::SubOrchestrationScheduled { .. } => true,
                    EventKind::ActivityScheduled { name, .. } => name == "a",
                    _ => false,
                })
                .collect();
            let expected: std::collections::HashSet<_> = indices
                .iter()
                .map(|index| {
                    Some(
                        schedules
                            .get(*index)
                            .expect("dropped emitted future was never bound")
                            .event_id(),
                    )
                })
                .collect();
            assert_eq!(
                canceled, expected,
                "{scenario}: dropped-future breadcrumb set is incomplete or unexpected"
            );
        }
    }
}

fn queue_values(observations: &[Observation]) -> Vec<(&'static str, String)> {
    observations
        .last()
        .unwrap()
        .live_reads
        .iter()
        .filter_map(|read| {
            if let Read::Queue(name, value) = read {
                Some((*name, value.clone()))
            } else {
                None
            }
        })
        .collect()
}

fn queue_scenario(
    name: &str,
    shape: Shape,
    drain: Vec<Arm>,
    batches: Vec<Vec<Completion>>,
    expected: Vec<(&'static str, &'static str)>,
) {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_and_replay(stamp, Handler::new(shape.clone(), 1, drain.clone()), &batches);
        let stable = observations
            .iter()
            .all(|o| o.live == o.replay && o.live_reads == o.replay_reads && o.replay_actions.is_empty());
        println!(
            "SCENARIO {name} stamp={stamp} stable={stable} outcome={}",
            observations.last().unwrap().replay
        );
        if stamp == "0.1.31" {
            assert_stable(&observations, name);
            let actual = queue_values(&observations);
            assert_eq!(
                actual,
                expected.iter().map(|(q, v)| (*q, (*v).into())).collect::<Vec<_>>(),
                "{name}: FIFO consumption"
            );
        } else {
            assert_eq!(
                observations.last().unwrap().live,
                "Continue",
                "{name}: changed legacy live outcome"
            );
            assert_eq!(
                queue_values(&observations),
                expected.iter().map(|(q, v)| (*q, (*v).into())).collect::<Vec<_>>(),
                "{name}: changed legacy live reads"
            );
            let expected_replay = if name == "T1" || name == "Q1/count0" {
                "Continue"
            } else {
                "Failed: nondeterministic: history schedule but no emitted action"
            };
            assert_eq!(
                observations.last().unwrap().replay,
                expected_replay,
                "{name}: changed retained legacy replay outcome"
            );
        }
    }
}

#[test]
fn q2_first_dequeue_wins_dropped_sibling_preserves_second_message() {
    queue_scenario(
        "Q2",
        Shape::Race(vec![Arm::Q, Arm::Q, Arm::Timer]),
        vec![Arm::Q],
        vec![vec![Completion::Q("first"), Completion::Q("second")]],
        vec![("q", "first"), ("q", "second")],
    );
}

#[test]
fn q3_same_queue_select2_fifo() {
    queue_scenario(
        "Q3",
        Shape::Race(vec![Arm::Q, Arm::Q]),
        vec![Arm::Q],
        vec![vec![Completion::Q("first"), Completion::Q("second")]],
        vec![("q", "first"), ("q", "second")],
    );
}

#[test]
fn q4_cross_queue_cancellations_are_independent() {
    queue_scenario(
        "Q4",
        Shape::Race(vec![Arm::Q, Arm::Q2, Arm::Timer]),
        vec![Arm::Q, Arm::Q2],
        vec![vec![
            Completion::Timer(0),
            Completion::Q("first"),
            Completion::Q2("second"),
        ]],
        vec![("q", "first"), ("q2", "second")],
    );
}

#[test]
fn q5_partial_join_resolved_child_drop_does_not_duplicate() {
    queue_scenario(
        "Q5",
        Shape::PartialJoin,
        vec![Arm::Q],
        vec![vec![
            Completion::Q("first"),
            Completion::Timer(0),
            Completion::Q("second"),
        ]],
        vec![("q", "first"), ("q", "second")],
    );
}

#[test]
fn q6_nested_select_queue_and_two_timers() {
    for outer_first in [false, true] {
        let timer = if outer_first { 1 } else { 0 };
        queue_scenario(
            "Q6",
            Shape::Nested,
            vec![Arm::Q],
            vec![vec![Completion::Timer(timer), Completion::Q("first")]],
            vec![("q", "first")],
        );
    }
}

#[test]
fn q7_queue_activity_timer_cancellation() {
    queue_scenario(
        "Q7",
        Shape::Race(vec![Arm::Q, Arm::Activity, Arm::Timer]),
        vec![Arm::Q],
        vec![vec![
            Completion::Timer(0),
            Completion::Activity(0),
            Completion::Q("first"),
        ]],
        vec![("q", "first")],
    );
}

#[test]
fn q8_queue_child_completion_same_batch() {
    queue_scenario(
        "Q8",
        Shape::Race(vec![Arm::Q, Arm::Child]),
        vec![Arm::Q],
        vec![vec![Completion::Child(0), Completion::Q("first")]],
        vec![("q", "first")],
    );
}

#[test]
fn q9_unbound_drop_followed_by_message() {
    for stamp in ["0.1.30", "0.1.31"] {
        let obs = record_with_initial_batch(
            stamp,
            Handler::new(Shape::UnboundImmediate, 1, vec![]),
            &[],
            &[Completion::Q("first"), Completion::Q("second")],
        );
        assert_stable(&obs, &format!("Q9 initial buffered bind/{stamp}"));
        assert_eq!(
            queue_values(&obs),
            vec![("q", if stamp == "0.1.31" { "first" } else { "second" }.into())]
        );
    }
}

#[test]
fn q10_explicit_bound_drop_followed_by_message() {
    queue_scenario(
        "Q10",
        Shape::BoundDrop,
        vec![Arm::Q],
        vec![vec![Completion::Timer(0), Completion::Q("first")]],
        vec![("q", "first")],
    );
}

#[test]
fn q11_catch_up_batch_fifty_arrivals() {
    // Unique data plus explicit queued-read tracing checks catch-up order without wall-clock timing.
    let messages: Vec<&'static str> = (0..50)
        .map(|index| &*Box::leak(index.to_string().into_boxed_str()))
        .collect();
    let mut batch = vec![Completion::Timer(0)];
    batch.extend(messages.iter().map(|value| Completion::Q(value)));
    queue_scenario(
        "Q11",
        Shape::Race(vec![Arm::Q, Arm::Timer]),
        vec![Arm::Q; 50],
        vec![batch],
        messages.into_iter().map(|value| ("q", value)).collect(),
    );
}

#[test]
fn t1_timer_loser_does_not_consume_queue_arrival() {
    queue_scenario(
        "T1",
        Shape::Timers,
        vec![Arm::Q],
        vec![vec![Completion::Timer(0), Completion::Timer(1), Completion::Q("first")]],
        vec![("q", "first")],
    );
}

#[test]
fn q14_invalid_or_missing_stamp_retains_legacy_policy() {
    let mut reference = None;
    for stamp in ["0.1.30", "", "not-a-version"] {
        let observations = record_and_replay(
            stamp,
            Handler::new(Shape::Race(vec![Arm::Q, Arm::Q, Arm::Timer]), 1, vec![Arm::Q]),
            &[vec![Completion::Timer(0), Completion::Q("first")]],
        );
        let actual = observations
            .iter()
            .map(|o| {
                (
                    o.live.clone(),
                    o.replay.clone(),
                    o.live_reads.clone(),
                    o.replay_reads.clone(),
                    o.replay_actions.clone(),
                )
            })
            .collect::<Vec<_>>();
        if let Some(reference) = &reference {
            assert_eq!(reference, &actual);
        } else {
            reference = Some(actual);
        }
    }
}

#[test]
fn ms8_gate_boundary_prerelease_build_and_invalid_are_replay_stable() {
    let (logs, _guard) = common::tracing_capture::install_tracing_capture();
    for (stamp, immediate) in [
        ("", false),
        ("garbage", false),
        ("0.1.30", false),
        ("0.1.31", true),
        ("0.1.31-rc.1", false),
        ("0.1.31+build", true),
        ("0.2.0", true),
    ] {
        logs.lock().unwrap().clear();
        let handler = Handler::new(Shape::Race(vec![Arm::Q, Arm::Q, Arm::Timer]), 1, vec![Arm::Q]);
        let observations = record_and_replay(
            stamp,
            handler.clone(),
            &[vec![Completion::Timer(0), Completion::Q("first")]],
        );
        let last = observations.last().unwrap();
        if immediate {
            assert_stable(&observations, stamp);
            assert_eq!(queue_values(&observations), vec![("q", "first".into())]);
        } else {
            assert!(
                last.replay.contains("history schedule but no emitted action"),
                "{stamp}: {last:?}"
            );
        }
        for _ in 0..2 {
            let mut replay = create_engine(last.history.clone());
            assert_eq!(last.replay, outcome(&execute(&mut replay, handler.clone())));
        }
        assert_eq!(
            logs.lock()
                .unwrap()
                .iter()
                .any(|event| event.level == tracing::Level::WARN && event.message.contains("Invalid pinned version")),
            stamp.is_empty() || stamp == "garbage",
            "warning did not match stamp {stamp:?}"
        );
    }
}

#[test]
fn q14_missing_version_field_is_a_deserialization_error_not_legacy() {
    let mut value = serde_json::to_value(started_event(1)).unwrap();
    value.as_object_mut().unwrap().remove("duroxide_version");
    let error = serde_json::from_value::<Event>(value).unwrap_err();
    assert!(error.to_string().contains("missing field `duroxide_version`"));
}

#[tokio::test]
async fn q14_missing_version_is_poisoned_by_sqlite_dispatcher() {
    use duroxide::OrchestrationRegistry;
    use duroxide::runtime::registry::ActivityRegistry;
    use duroxide::runtime::{Runtime, RuntimeOptions};
    let (store, temporary) = common::create_sqlite_store_disk().await;
    common::seed_instance_with_pinned_version(
        store.as_ref(),
        TEST_INSTANCE,
        TEST_ORCH_NAME,
        semver::Version::new(0, 1, 30),
    )
    .await;
    let url = format!("sqlite:{}", temporary.path().join("test.db").display());
    let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
    sqlx::query("UPDATE history SET event_data=json_remove(event_data,'$.duroxide_version') WHERE instance_id=?")
        .bind(TEST_INSTANCE)
        .execute(&pool)
        .await
        .unwrap();
    let runtime = Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder().build(),
        RuntimeOptions {
            max_attempts: 0,
            dispatcher_min_poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    )
    .await;
    // Normal history readers must reject the corrupt start. Observe the poison
    // terminal through storage instead, with bounded, paced polling.
    let failed = tokio::time::timeout(RUNTIME_DEADLINE, async {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            tick.tick().await;
            let output: Option<String> =
                sqlx::query_scalar("SELECT output FROM executions WHERE instance_id=? AND status='Failed'")
                    .bind(TEST_INSTANCE)
                    .fetch_optional(&pool)
                    .await
                    .unwrap()
                    .flatten();
            if let Some(output) = output {
                break output;
            }
        }
    })
    .await
    .expect("poison terminal was not persisted");
    assert!(
        failed.contains("deserialization") || failed.contains("deserialize"),
        "{failed}"
    );
    let terminal: String = sqlx::query_scalar("SELECT event_data FROM history WHERE instance_id=? AND event_id=99999")
        .bind(TEST_INSTANCE)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(terminal.contains("FailedDeserialization"));
    runtime.shutdown(None).await;
}

#[test]
fn ms10_legacy_hazard_remains_detectable_after_upgrade() {
    let observations = record_and_replay(
        "0.1.30",
        Handler::new(Shape::Race(vec![Arm::Q, Arm::Timer]), 1, vec![Arm::Q]),
        &[vec![Completion::Timer(0), Completion::Q("first")]],
    );
    assert!(
        observations
            .last()
            .unwrap()
            .replay
            .contains("history schedule but no emitted action")
    );
}

#[test]
fn ms7_quiescence_drop_is_recorded_and_policy_pinned() {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_with_initial_batch(
            stamp,
            Handler::new(Shape::Race(vec![Arm::Q, Arm::Q2]), 1, vec![Arm::Q2]),
            &[],
            &[Completion::Q("first"), Completion::Q2("other")],
        );
        assert_stable(&observations, "MS7 quiescence");
        if stamp == "0.1.31" {
            assert_stable(&observations, "MS7 immediate");
            assert_eq!(
                queue_values(&observations),
                vec![("q", "first".into()), ("q2", "other".into())]
            );
        } else {
            assert_eq!(
                queue_values(&observations),
                vec![("q", "first".into())],
                "legacy policy must keep the quiescence replacement pending"
            );
        }
    }
}

#[test]
fn q12_continue_as_new_retains_policy_and_fresh_execution_selects_its_own() {
    for stamp in ["0.1.30", "0.1.31"] {
        let handler = Handler::new(Shape::Continue, 1, vec![]);
        let mut history = vec![started_event(1)];
        history[0].duroxide_version = stamp.into();
        let mut first = create_engine(history.clone());
        assert_continue(&execute(&mut first, handler.clone()));
        history.extend_from_slice(first.history_delta());
        let mut second = create_engine(history.clone());
        second.prep_completions(make_messages(&history, &[Completion::Timer(0), Completion::Q("first")]));
        let result = execute(&mut second, handler.clone());
        assert!(matches!(result, TurnResult::ContinueAsNew { .. }));
        history.extend_from_slice(second.history_delta());
        let mut replay = create_engine(history);
        assert_eq!(outcome(&result), outcome(&execute(&mut replay, handler)));

        // A replacement execution's own start stamp chooses the policy; the
        // former execution's stamp does not flow into its queue matching.
        let observations = record_and_replay(
            "0.1.31",
            Handler::new(Shape::Race(vec![Arm::Q, Arm::Timer]), 1, vec![Arm::Q]),
            &[vec![Completion::Timer(0), Completion::Q("first")]],
        );
        assert_stable(&observations, "Q12/Q13 replacement");
        assert_eq!(queue_values(&observations), vec![("q", "first".into())]);
    }
}

#[test]
fn positional_drop_before_continue_as_new_replays_for_both_policies() {
    for stamp in ["0.1.30", "0.1.31"] {
        let handler = Handler::new(Shape::PositionalContinue, 1, vec![]);
        let observations = record_and_replay(
            stamp,
            handler,
            &[vec![Completion::Timer(0), Completion::Signal("stale")]],
        );
        let observation = observations.last().unwrap();
        assert!(observation.live.starts_with("ContinueAsNew"));
        assert_eq!(observation.live, observation.replay);
        let canceled = observation
            .history
            .iter()
            .filter(|event| {
                matches!(&event.kind,
                EventKind::ExternalSubscribedCancelled{reason}if reason=="dropped_future")
            })
            .count();
        assert_eq!(canceled, 0, "continue-as-new writes no dropped_future rows");
        assert!(observation.replay_delta.is_empty());
        assert_eq!(observation.replay_actions.len(), 1);
        assert!(observation.replay_actions[0].starts_with("ContinueAsNew"));
    }
}

#[test]
fn positional_s2_two_waits_timer_same_batch_preserves_policy() {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_and_replay(
            stamp,
            Handler::new(
                Shape::Race(vec![Arm::Signal, Arm::Signal, Arm::Timer]),
                1,
                vec![Arm::Signal],
            ),
            &[
                vec![Completion::Timer(0), Completion::Signal("first")],
                vec![Completion::Signal("second")],
            ],
        );
        assert_stable(&observations, &format!("S2/{stamp}"));
        if stamp == "0.1.31" {
            assert_eq!(
                observations.last().unwrap().live_reads,
                vec![Read::Timer, Read::Signal("second".into())]
            );
        } else {
            assert_eq!(observations.last().unwrap().live_reads, vec![Read::Timer]);
        }
    }
}

#[test]
fn positional_s3_signal_queue_timer_same_batch_preserves_policy() {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_and_replay(
            stamp,
            Handler::new(
                Shape::Race(vec![Arm::Signal, Arm::Q, Arm::Timer]),
                1,
                vec![Arm::Signal, Arm::Q],
            ),
            &[
                vec![
                    Completion::Timer(0),
                    Completion::Signal("first"),
                    Completion::Q("first"),
                ],
                vec![Completion::Signal("second"), Completion::Q("second")],
            ],
        );
        if stamp == "0.1.31" {
            assert_stable(&observations, &format!("S3/{stamp}"));
            assert_eq!(
                observations.last().unwrap().live_reads,
                vec![
                    Read::Timer,
                    Read::Signal("second".into()),
                    Read::Queue("q", "first".into())
                ]
            );
        } else {
            assert_stable(&observations, "S3 legacy");
            assert_eq!(
                observations.last().unwrap().live_reads,
                vec![
                    Read::Timer,
                    Read::Signal("first".into()),
                    Read::Queue("q", "first".into())
                ]
            );
        }
    }
}

#[test]
fn s4_signal_before_subscription_is_dropped_consistently() {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_and_replay(
            stamp,
            Handler::new(Shape::SignalBefore, 1, vec![]),
            &[
                vec![Completion::Signal("stale"), Completion::Timer(0)],
                vec![Completion::Signal("fresh")],
            ],
        );
        assert_stable(&observations, &format!("S4/{stamp}"));
        assert!(
            !observations
                .last()
                .unwrap()
                .live_reads
                .contains(&Read::Signal("stale".into()))
        );
    }
}

#[test]
fn s5_two_signal_arrivals_match_active_waits_consistently() {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_and_replay(
            stamp,
            Handler::new(
                Shape::Race(vec![Arm::Signal, Arm::Signal, Arm::Timer]),
                1,
                vec![Arm::Signal],
            ),
            &[vec![Completion::Signal("first"), Completion::Signal("second")]],
        );
        assert_stable(&observations, &format!("S5/{stamp}"));
        assert_eq!(
            observations.last().unwrap().live_reads.first(),
            Some(&Read::Signal("first".into()))
        );
        assert_eq!(
            observations.last().unwrap().live_reads,
            vec![Read::Signal("first".into())]
        );
    }
}

#[test]
fn ms13_held_and_nonprefix_continue_as_new_carry_exact_unread_positions() {
    for stamp in ["0.1.30", "0.1.31"] {
        for read_newer in [false, true] {
            let observations = record_and_replay(
                stamp,
                Handler::new(Shape::HeldContinue(read_newer), 1, vec![]),
                &[vec![Completion::Timer(0), Completion::Q("m1"), Completion::Q("m2")]],
            );
            let last = observations.last().unwrap();
            assert_eq!(last.live, last.replay, "MS13/{stamp}/{read_newer}");
            assert!(last.replay_delta.is_empty());
            let expected = if stamp == "0.1.30" {
                "unconsumed_queue_arrivals: None"
            } else if read_newer {
                "unconsumed_queue_arrivals: Some([(\"q\", \"m1\")])"
            } else {
                "unconsumed_queue_arrivals: Some([(\"q\", \"m1\"), (\"q\", \"m2\")])"
            };
            assert!(last.live.contains(expected), "{}", last.live);
            assert_eq!(
                queue_values(&observations),
                if read_newer { vec![("q", "m2".into())] } else { vec![] }
            );
        }
    }
}

#[test]
fn continue_as_new_exact_unread_positions_preserve_cross_name_arrival_order() {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_and_replay(
            stamp,
            Handler::new(Shape::HeldContinue(true), 1, vec![]),
            &[vec![
                Completion::Timer(0),
                Completion::Q2("other-first"),
                Completion::Q("m1"),
                Completion::Q2("other-second"),
                Completion::Q("m2"),
            ]],
        );
        let last = observations.last().unwrap();
        assert_eq!(last.live, last.replay);
        assert!(last.replay_delta.is_empty());
        assert_eq!(queue_values(&observations), vec![("q", "m2".into())]);
        let expected = if stamp == "0.1.30" {
            "unconsumed_queue_arrivals: None"
        } else {
            "unconsumed_queue_arrivals: Some([(\"q2\", \"other-first\"), (\"q\", \"m1\"), (\"q2\", \"other-second\")])"
        };
        assert!(
            last.live.contains(expected),
            "cross-name carry-forward changed: {}",
            last.live
        );
    }
}

#[test]
fn ms14_unbound_replacement_drops_same_batch_signal_as_early() {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_and_replay(
            stamp,
            Handler::new(Shape::Race(vec![Arm::Signal, Arm::Timer]), 1, vec![Arm::Signal]),
            &[
                vec![Completion::Timer(0), Completion::Signal("a")],
                vec![Completion::Signal("b")],
            ],
        );
        assert_stable(&observations, &format!("MS14/{stamp}"));
        if stamp == "0.1.31" {
            assert_signal_contract_read(&observations, "b");
        }
        assert_eq!(
            observations.last().unwrap().live_reads,
            if stamp == "0.1.31" {
                vec![Read::Timer, Read::Signal("b".into())]
            } else {
                vec![Read::Timer]
            }
        );
        assert!(observations.last().unwrap().replay_delta.is_empty());
    }
}

#[test]
fn ms16_cancellation_blocks_are_deterministic_by_type_and_id() {
    for (stamp, arm) in ["0.1.30", "0.1.31"]
        .into_iter()
        .flat_map(|stamp| [Arm::Q, Arm::Signal, Arm::Activity, Arm::Child].map(|arm| (stamp, arm)))
    {
        let mut baseline = None;
        for _ in 0..16 {
            let obs = record_and_replay(
                stamp,
                Handler::new(Shape::Race(vec![arm, arm, Arm::Timer]), 1, vec![]),
                &[vec![Completion::Timer(0)]],
            );
            assert_stable(&obs, "MS16 cancellation order");
            let ids: Vec<u64> = obs
                .last()
                .unwrap()
                .history
                .iter()
                .filter(|event| {
                    matches!(
                        event.kind,
                        EventKind::QueueSubscriptionCancelled { .. }
                            | EventKind::ExternalSubscribedCancelled { .. }
                            | EventKind::ActivityCancelRequested { .. }
                            | EventKind::SubOrchestrationCancelRequested { .. }
                    )
                })
                .filter_map(|event| event.source_event_id)
                .collect();
            assert!(
                ids.len() >= 2 && ids.windows(2).all(|pair| pair[0] < pair[1]),
                "{stamp} {arm:?}: dropped-future cancellations must be in ascending id order: {ids:?}"
            );
            let normalized: Vec<_> = obs
                .last()
                .unwrap()
                .history
                .iter()
                .map(|event| {
                    let mut value = event.clone();
                    value.timestamp_ms = 0;
                    if let EventKind::TimerCreated { fire_at_ms } | EventKind::TimerFired { fire_at_ms } =
                        &mut value.kind
                    {
                        *fire_at_ms = 1;
                    }
                    value
                })
                .collect();
            if let Some(expected) = &baseline {
                assert_eq!(&normalized, expected);
            } else {
                baseline = Some(normalized);
            }
        }
    }
}

/// Independent same-type dropped-future breadcrumbs form a contiguous end-of-turn
/// block. The base enumerates them from a HashSet; replay validates source IDs as
/// sets and never polls between them. Permuting only that block preserves its
/// event-ID range and all next decisions, for both execution-pinned policies.
#[test]
fn independent_cancellation_block_permutations_preserve_replay_and_next_turn_for_both_stamps() {
    for stamp in ["0.1.30", "0.1.31"] {
        let handler = Handler::new(Shape::Race(vec![Arm::Q, Arm::Q, Arm::Timer]), 1, vec![]);
        let observations = record_and_replay(stamp, handler.clone(), &[vec![Completion::Timer(0)]]);
        assert_stable(&observations, "independent cancellation permutation");
        let original = observations.last().unwrap().history.clone();
        let indices: Vec<_> = original
            .iter()
            .enumerate()
            .filter_map(|(index, event)| {
                matches!(&event.kind, EventKind::QueueSubscriptionCancelled { reason } if reason == "dropped_future")
                    .then_some(index)
            })
            .collect();
        assert_eq!(indices.len(), 2);
        assert_eq!(indices[1], indices[0] + 1);
        let mut permuted = original.clone();
        let ids = (permuted[indices[0]].event_id, permuted[indices[1]].event_id);
        permuted.swap(indices[0], indices[1]);
        permuted[indices[0]].event_id = ids.0;
        permuted[indices[1]].event_id = ids.1;
        let mut next_decisions = None;
        for history in [original, permuted] {
            let mut replay = create_engine(history.clone());
            assert_continue(&execute(&mut replay, handler.clone()));
            assert!(replay.history_delta().is_empty());
            assert!(replay.pending_actions().is_empty());
            let mut next = create_engine(history.clone());
            next.prep_completions(make_messages(&history, &[Completion::Activity(0)]));
            let result = execute(&mut next, handler.clone());
            let decisions = (
                outcome(&result),
                action_shapes(next.pending_actions()),
                next.history_delta()
                    .iter()
                    .map(|event| (event.event_id, event.source_event_id, event.kind.clone()))
                    .collect::<Vec<_>>(),
            );
            if let Some(expected) = &next_decisions {
                assert_eq!(&decisions, expected);
            } else {
                next_decisions = Some(decisions);
            }
        }
    }
}

#[test]
fn continue_as_new_writes_no_drop_cancellations_or_child_side_effects() {
    for stamp in ["0.1.30", "0.1.31"] {
        let observations = record_and_replay(
            stamp,
            Handler::new(Shape::ContinueWithOtherDrops, 1, vec![]),
            &[vec![Completion::Timer(0), Completion::Q("m1")]],
        );
        let last = observations.last().unwrap();
        assert!(last.live.starts_with("ContinueAsNew"));
        assert_eq!(last.live, last.replay);
        assert!(last.replay_delta.is_empty());
        assert!(!last.history.iter().any(|event| matches!(
            event.kind,
            EventKind::ActivityCancelRequested { .. } | EventKind::SubOrchestrationCancelRequested { .. }
        )));
        assert_eq!(last.replay_actions.len(), 1);
        assert!(last.replay_actions[0].starts_with("ContinueAsNew"));
        let wait_cancellations = last
            .history
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    EventKind::ExternalSubscribedCancelled { .. } | EventKind::QueueSubscriptionCancelled { .. }
                )
            })
            .count();
        assert_eq!(wait_cancellations, 0);
    }
}

fn permutations(items: &[Completion]) -> Vec<Vec<Completion>> {
    if items.is_empty() {
        return vec![Vec::new()];
    }
    let mut result = Vec::new();
    for index in 0..items.len() {
        let mut rest = items.to_vec();
        let first = rest.remove(index);
        for mut tail in permutations(&rest) {
            tail.insert(0, first.clone());
            result.push(tail);
        }
    }
    result
}

/// The old engine prints these two HashSets in randomized order in cancellation
/// mismatch errors. Normalize only those numeric diagnostic sets for the model
/// fingerprint; history decisions and the delta comparator are not altered.
fn legacy_case_fingerprint(observations: &[Observation]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let records: Vec<_> = observations
        .iter()
        .map(|observation| {
            let history = serde_json::to_vec(&cancellation_history::decision_delta(&observation.history))
                .expect("canonical history must serialize");
            (
                legacy_model_outcome(&observation.live),
                legacy_model_outcome(&observation.replay),
                format!("{:?}", observation.live_reads),
                format!("{:?}", observation.replay_reads),
                &observation.live_actions,
                &observation.replay_actions,
                format!("{:x}", Sha256::digest(history)),
            )
        })
        .collect();
    serde_json::to_vec(&records).expect("legacy fingerprint must serialize")
}

#[test]
fn legacy_fingerprint_retains_history_and_live_decision_changes() {
    let event = |id, source, kind| Event::with_event_id(id, TEST_INSTANCE, 1, source, kind);
    let original = Observation {
        live: "Continue".into(),
        replay: "Continue".into(),
        live_reads: vec![],
        replay_reads: vec![],
        logical_cancellations: None,
        live_actions: vec!["next".into()],
        replay_actions: vec![],
        replay_delta: vec![],
        history: vec![
            started_event(1),
            event(2, None, EventKind::QueueSubscribed { name: "q".into() }),
            event(3, None, EventKind::QueueSubscribed { name: "q".into() }),
            event(
                4,
                Some(2),
                EventKind::QueueSubscriptionCancelled {
                    reason: "dropped_future".into(),
                },
            ),
            event(
                5,
                Some(3),
                EventKind::QueueSubscriptionCancelled {
                    reason: "dropped_future".into(),
                },
            ),
        ],
    };
    let fingerprint = legacy_case_fingerprint(std::slice::from_ref(&original));
    let mut reordered = original.clone();
    reordered.history.swap(3, 4);
    reordered.history[3].event_id = 4;
    reordered.history[4].event_id = 5;
    assert_eq!(fingerprint, legacy_case_fingerprint(&[reordered]));
    let mut changed = original.clone();
    changed.live_actions.push("additional action".into());
    assert_ne!(fingerprint, legacy_case_fingerprint(&[changed]));
    let mut changed = original.clone();
    changed.history[3].source_event_id = Some(7);
    assert_ne!(fingerprint, legacy_case_fingerprint(&[changed]));
    let mut changed = original.clone();
    changed.history[3].kind = EventKind::QueueSubscriptionCancelled {
        reason: "changed".into(),
    };
    assert_ne!(fingerprint, legacy_case_fingerprint(&[changed]));
    let mut changed = original.clone();
    changed.history.push(event(
        6,
        Some(2),
        EventKind::QueueSubscriptionCancelled {
            reason: "dropped_future".into(),
        },
    ));
    assert_ne!(fingerprint, legacy_case_fingerprint(&[changed]));
    let mut changed = original.clone();
    changed.history[1].kind = EventKind::QueueSubscribed { name: "other".into() };
    assert_ne!(fingerprint, legacy_case_fingerprint(&[changed]));
    let mut boundary = original.clone();
    boundary.history.push(event(
        6,
        None,
        EventKind::ActivityScheduled {
            name: "barrier".into(),
            input: "".into(),
            session_id: None,
            tag: None,
        },
    ));
    let before = legacy_case_fingerprint(std::slice::from_ref(&boundary));
    boundary.history.swap(4, 5);
    boundary.history[4].event_id = 5;
    boundary.history[5].event_id = 6;
    assert_ne!(before, legacy_case_fingerprint(&[boundary]));
}

fn legacy_model_outcome(outcome: &str) -> String {
    // The new Rust result struct adds an absent legacy carry-forward field.
    // Remove only that trailing field, never the same text in an output payload.
    if outcome.starts_with("ContinueAsNew {")
        && let Some(prefix) = outcome.strip_suffix(", unconsumed_queue_arrivals: None }")
    {
        return format!("{prefix} }}");
    }
    for kind in ["activities", "sub-orchestrations", "external waits", "persistent waits"] {
        let prefix = format!(
            "Failed: nondeterministic: cancellation mismatch ({kind}): baseline_dropped_future_cancel_requests={{"
        );
        let Some(sets) = outcome.strip_prefix(&prefix) else {
            continue;
        };
        let Some((baseline, context)) = sets.split_once("} ctx_cancelled_in_replayed_segment={") else {
            return outcome.into();
        };
        let Some(context) = context.strip_suffix('}') else {
            return outcome.into();
        };
        if baseline.contains(['{', '}']) || context.contains(['{', '}']) {
            return outcome.into();
        }
        let sorted = |set: &str| {
            let mut ids: Vec<u64> = if set.is_empty() {
                Vec::new()
            } else {
                set.split(", ")
                    .map(|value| {
                        assert!(
                            value.bytes().all(|byte| byte.is_ascii_digit()),
                            "diagnostic set must contain numeric IDs"
                        );
                        value.parse().expect("diagnostic ID must fit u64")
                    })
                    .collect()
            };
            ids.sort_unstable();
            ids.iter().map(u64::to_string).collect::<Vec<_>>().join(", ")
        };
        return format!(
            "{prefix}{}}} ctx_cancelled_in_replayed_segment={{{}}}",
            sorted(baseline),
            sorted(context)
        );
    }
    outcome.into()
}

#[test]
fn legacy_fingerprint_normalizes_only_named_numeric_diagnostic_sets() {
    use sha2::{Digest, Sha256};
    let original = "Failed: nondeterministic: cancellation mismatch (persistent waits): baseline_dropped_future_cancel_requests={3, 2} ctx_cancelled_in_replayed_segment={4, 2}";
    let reordered = "Failed: nondeterministic: cancellation mismatch (persistent waits): baseline_dropped_future_cancel_requests={2, 3} ctx_cancelled_in_replayed_segment={2, 4}";
    assert_eq!(legacy_model_outcome(original), legacy_model_outcome(reordered));
    let fingerprint = |text: &str| Sha256::digest(legacy_model_outcome(text).as_bytes());
    for changed in [
        original.replace("{3, 2}", "{3, 9}"),
        original.replace("{3, 2}", "{3, 2, 9}"),
        original.replace("{3, 2}", "{3}"),
        original.replace("{3, 2}", "{3, 2, 2}"),
        original.replace("{4, 2}", "{4, 9}"),
        original.replace("{4, 2}", "{4, 2, 9}"),
        original.replace("{4, 2}", "{4}"),
        original.replace("{4, 2}", "{4, 2, 2}"),
        original.replace("persistent waits", "activities"),
        original.replace("ctx_cancelled_in_replayed_segment", "other"),
    ] {
        assert_ne!(legacy_model_outcome(original), legacy_model_outcome(&changed));
        assert_ne!(fingerprint(original), fingerprint(&changed));
    }
    assert_eq!(legacy_model_outcome("Completed(\"{3, 2}\")"), "Completed(\"{3, 2}\")");
    let payload = "Completed(\"baseline_dropped_future_cancel_requests={3, 2}\")";
    assert_eq!(legacy_model_outcome(payload), payload);
    for elsewhere in [
        format!("prefix {original}"),
        format!("{original} extra"),
        original.replace("nondeterministic", "activity failure"),
        original.replace("persistent waits", "unknown kind"),
        "Completed(\", unconsumed_queue_arrivals: None\")".into(),
    ] {
        assert_eq!(legacy_model_outcome(&elsewhere), elsewhere);
    }
}

#[test]
#[should_panic(expected = "diagnostic set must contain numeric IDs")]
fn legacy_diagnostic_template_rejects_non_numeric_set_elements() {
    legacy_model_outcome(
        "Failed: nondeterministic: cancellation mismatch (persistent waits): baseline_dropped_future_cancel_requests={2, invalid} ctx_cancelled_in_replayed_segment={4}",
    );
}

#[test]
#[should_panic(expected = "diagnostic ID must fit u64")]
fn legacy_diagnostic_template_rejects_overflow_in_the_other_set() {
    legacy_model_outcome(
        "Failed: nondeterministic: cancellation mismatch (persistent waits): baseline_dropped_future_cancel_requests={2} ctx_cancelled_in_replayed_segment={18446744073709551616}",
    );
}

/// Legacy digests: what to do when one changes.
///
/// The two exhaustive generators fold every legacy-stamped case into a SHA-256 digest
/// (`b9ecaca1...` queue, `7f977b4e...` signal families). The expected value is the
/// unmodified 0.1.30 base's behaviour, so a mismatch means either a legacy decision
/// changed (a compatibility break) or the generated census changed. Never accept a new
/// value by copying this branch's printed digest:
///
/// 1. Copy this test file onto the isolated base (`6a45886`, duroxide 0.1.30) and run
///    the generator there with `DUROXIDE_RECORD_LEGACY_MODEL=1`. The run checks that
///    `src/lib.rs` and the replay engine are the unmodified base, and prints the digest
///    the base actually produces. Only that value may become the expected digest.
/// 2. If the base digest and this branch's digest differ, find the cases before
///    anything else: run the queue generator on both trees with
///    `DUROXIDE_LEGACY_MODEL_TRACE=<file>` and diff the files. Each line is one legacy
///    case (`index, shape/batch/iterations/eager/split, fingerprint`); a differing
///    fingerprint is a changed legacy decision and must be fixed, not re-recorded.
/// 3. `DUROXIDE_VERIFY_LEGACY_MODEL=1` runs only the legacy half of either generator
///    (for example 168 of the 336 signal-family cases) for a faster check.
#[test]
fn exhaustive_small_scope_queue_race_orders() {
    use sha2::{Digest, Sha256};
    use std::io::Write;
    let record_legacy = std::env::var_os("DUROXIDE_RECORD_LEGACY_MODEL").is_some();
    let verify_legacy = std::env::var_os("DUROXIDE_VERIFY_LEGACY_MODEL").is_some();
    let mut trace = std::env::var_os("DUROXIDE_LEGACY_MODEL_TRACE")
        .map(|path| std::io::BufWriter::new(std::fs::File::create(path).expect("create legacy model trace")));
    if record_legacy {
        assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.30");
        assert_eq!(
            format!("{:x}", Sha256::digest(include_str!("../src/lib.rs").as_bytes())),
            "c0f28773f23f9f0f1bab78d3472456a45c63e5dc6fbdd21854029fee2c1129a3",
            "legacy model recording requires the isolated unmodified base"
        );
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(include_str!("../src/runtime/replay_engine.rs").as_bytes())
            ),
            "b85bf87ec75d63db0befff18e9dc32d2385b721055b441b42c607477f0242deb",
            "legacy model recording requires the isolated unmodified replay engine"
        );
    }
    let mut legacy_digest = Sha256::new();
    // Keep the model census log small; targeted warning tests capture their own
    // diagnostics. Every case still runs the real engine and invariant checks.
    let _quiet = tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    let arms = [Arm::Q, Arm::Q2, Arm::Signal, Arm::Timer, Arm::Activity, Arm::Child];
    let mut programs = Vec::new();
    for a in arms {
        programs.push(vec![a]);
        for b in arms {
            programs.push(vec![a, b]);
            for c in arms {
                programs.push(vec![a, b, c]);
            }
        }
    }
    let mut cases = 0;
    let mut legacy_differences = 0;
    let mut asserted = 0;
    for program in programs {
        let mut inputs = Vec::new();
        if program.contains(&Arm::Q) {
            inputs.push(Completion::Q("first"));
            inputs.push(Completion::Q("second"));
            inputs.push(Completion::Q("third"));
        }
        if program.contains(&Arm::Q2) {
            inputs.push(Completion::Q2("other"));
            inputs.push(Completion::Q2("other-second"));
            inputs.push(Completion::Q2("other-third"));
        }
        if program.contains(&Arm::Signal) {
            inputs.push(Completion::Signal("first-signal"));
            inputs.push(Completion::Signal("second-signal"));
            inputs.push(Completion::Signal("third-signal"));
        }
        for index in 0..program.iter().filter(|arm| **arm == Arm::Timer).count() {
            inputs.push(Completion::Timer(index));
        }
        for index in 0..program.iter().filter(|arm| **arm == Arm::Activity).count() {
            inputs.push(Completion::Activity(index));
        }
        for index in 0..program.iter().filter(|arm| **arm == Arm::Child).count() {
            inputs.push(Completion::Child(index));
        }
        let mut subsets = Vec::new();
        for mask in 0..(1usize << inputs.len()) {
            if mask.count_ones() > 3 {
                continue;
            }
            let subset = inputs
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, completion)| completion.clone())
                .collect::<Vec<_>>();
            subsets.extend(permutations(&subset));
        }
        for batch in subsets {
            let follow: Vec<_> = [
                (Arm::Q, Completion::Q("fourth")),
                (Arm::Q2, Completion::Q2("other-fourth")),
                (Arm::Signal, Completion::Signal("fresh-signal")),
            ]
            .into_iter()
            .filter(|(arm, _)| program.contains(arm))
            .map(|(_, completion)| completion)
            .collect();
            for iterations in 1..=3 {
                for stamp in ["0.1.30", "0.1.31"] {
                    if (record_legacy || verify_legacy) && stamp != "0.1.30" {
                        continue;
                    }
                    for eager in [false, true] {
                        let buffered_placements = if batch.iter().all(|completion| {
                            matches!(completion, Completion::Q(_) | Completion::Q2(_) | Completion::Signal(_))
                        }) {
                            2
                        } else {
                            1
                        };
                        for placement in 0..buffered_placements {
                            let mut handler = Handler::new(Shape::Race(program.clone()), iterations, vec![]);
                            Arc::get_mut(&mut handler).unwrap().eager = eager;
                            Arc::get_mut(&mut handler).unwrap().expected_queue_counts = Some((
                                batch
                                    .iter()
                                    .chain(follow.iter())
                                    .filter(|completion| matches!(completion, Completion::Q(_)))
                                    .count(),
                                batch
                                    .iter()
                                    .chain(follow.iter())
                                    .filter(|completion| matches!(completion, Completion::Q2(_)))
                                    .count(),
                            ));
                            let observations = if placement == 0 {
                                record_with_fresh_completions(
                                    stamp,
                                    handler,
                                    &[batch.clone(), follow.clone()],
                                    &[],
                                    true,
                                )
                            } else {
                                record_with_fresh_completions(
                                    stamp,
                                    handler,
                                    std::slice::from_ref(&follow),
                                    &batch,
                                    true,
                                )
                            };
                            let stable = observations.iter().all(|o| {
                                o.live == o.replay && o.live_reads == o.replay_reads && o.replay_actions.is_empty()
                            });
                            if stamp == "0.1.31" {
                                assert_stable(&observations, &format!("exhaustive/{program:?}/{batch:?}/{iterations}"));
                                if iterations >= 2
                                    && (program == [Arm::Signal, Arm::Timer] || program == [Arm::Timer, Arm::Signal])
                                    && matches!(batch.first(), Some(Completion::Timer(0)))
                                    && batch
                                        .iter()
                                        .any(|completion| matches!(completion, Completion::Signal(_)))
                                {
                                    assert_signal_contract_read(&observations, "fresh-signal");
                                }
                                let reads = queue_values(&observations);
                                for queue in ["q", "q2"] {
                                    let expected = batch
                                        .iter()
                                        .chain(follow.iter())
                                        .filter_map(|completion| match completion {
                                            Completion::Q(v) if queue == "q" => Some(*v),
                                            Completion::Q2(v) if queue == "q2" => Some(*v),
                                            _ => None,
                                        })
                                        .collect::<Vec<_>>();
                                    let actual = reads
                                        .iter()
                                        .filter(|(name, _)| *name == queue)
                                        .map(|(_, value)| value.as_str())
                                        .collect::<Vec<_>>();
                                    // A pending program may not yet have read every offered value.
                                    assert_eq!(actual, expected[..actual.len()], "FIFO prefix changed");
                                    let last = observations.last().unwrap();
                                    let canceled: std::collections::HashSet<_> = last
                                        .history
                                        .iter()
                                        .filter(|event| {
                                            matches!(event.kind, EventKind::QueueSubscriptionCancelled { .. })
                                        })
                                        .filter_map(|event| event.source_event_id)
                                        .collect();
                                    let active = last
                                        .history
                                        .iter()
                                        .filter(|event| {
                                            matches!(&event.kind, EventKind::QueueSubscribed { name } if name == queue)
                                                && !canceled.contains(&event.event_id)
                                        })
                                        .count();
                                    if active > actual.len() {
                                        assert_eq!(
                                            actual.len(),
                                            expected.len(),
                                            "an active dequeue remained pending with an unread queued arrival"
                                        );
                                    }
                                    if observations.last().unwrap().history.iter().any(|event|
                                    matches!(&event.kind,EventKind::ActivityScheduled{name,..}if name=="checkpoint")) {
                                    assert_eq!(actual,expected,"completed model did not consume every queued input");
                                }
                                }
                                let offered: Vec<_> = batch
                                    .iter()
                                    .chain(follow.iter())
                                    .filter_map(|c| if let Completion::Signal(v) = c { Some(*v) } else { None })
                                    .collect();
                                let mut position = 0;
                                for read in &observations.last().unwrap().live_reads {
                                    if let Read::Signal(value) = read {
                                        let next = offered[position..]
                                            .iter()
                                            .position(|v| *v == value)
                                            .expect("signal duplicated, reordered, or never offered");
                                        position += next + 1;
                                    }
                                }
                                asserted += 1;
                            } else if !stable {
                                legacy_differences += 1;
                            }
                            if stamp == "0.1.30" {
                                let fingerprint = legacy_case_fingerprint(&observations);
                                legacy_digest.update(&fingerprint);
                                if let Some(trace) = &mut trace {
                                    writeln!(
                                        trace,
                                        "{cases}\t{program:?}/{batch:?}/{iterations}/{eager}/{placement}\t{}",
                                        std::str::from_utf8(&fingerprint).expect("JSON fingerprint must be UTF-8")
                                    )
                                    .expect("write legacy model trace");
                                }
                            }
                            cases += 1;
                        }
                    }
                }
            }
        }
    }
    // Additional real-engine program families, including explicit/unpolled
    // drops, a partial join, nested select and held/non-prefix CAN. Completion
    // partitions give separate-arrival turns as well as the same-batch shapes.
    for shape in [
        Shape::UnboundDrop,
        Shape::BoundDrop,
        Shape::PartialJoin,
        Shape::Nested,
        Shape::Continue,
        Shape::PositionalContinue,
        Shape::PositionalBarrier,
        Shape::ResolveAtCreation,
        Shape::HeldContinue(false),
        Shape::HeldContinue(true),
        Shape::QuiescenceDrop,
    ] {
        let inputs = vec![
            Completion::Timer(0),
            Completion::Q("first"),
            Completion::Signal("first-signal"),
        ];
        // ResolveAtCreation has activity, not timer, at the first boundary.
        let inputs = if matches!(shape, Shape::ResolveAtCreation) {
            vec![
                Completion::Signal("first-signal"),
                Completion::Activity(0),
                Completion::Q("first"),
            ]
        } else if matches!(shape, Shape::QuiescenceDrop) {
            vec![
                Completion::Q("first"),
                Completion::Q2("other"),
                Completion::Signal("first-signal"),
            ]
        } else {
            inputs
        };
        for batch in permutations(&inputs) {
            for split in 0..=batch.len() {
                for eager in [false, true] {
                    for iterations in 1..=3 {
                        for stamp in ["0.1.30", "0.1.31"] {
                            if (record_legacy || verify_legacy) && stamp != "0.1.30" {
                                continue;
                            }
                            let mut h = Handler::new(shape.clone(), iterations, vec![]);
                            Arc::get_mut(&mut h).unwrap().eager = eager;
                            let mut batches = vec![batch[..split].to_vec(), batch[split..].to_vec()];
                            // A third completion turn exercises later binding and fresh delivery.
                            batches.push(
                                if matches!(shape, Shape::PositionalBarrier | Shape::ResolveAtCreation) {
                                    vec![
                                        Completion::Activity(usize::from(matches!(shape, Shape::ResolveAtCreation))),
                                        Completion::Signal("fresh-signal"),
                                        Completion::Q("second"),
                                    ]
                                } else {
                                    vec![
                                        Completion::Q("second"),
                                        Completion::Q2("other"),
                                        Completion::Signal("fresh-signal"),
                                    ]
                                },
                            );
                            if stamp == "0.1.31" && matches!(shape, Shape::PositionalBarrier | Shape::ResolveAtCreation)
                            {
                                for completion in batches.last_mut().unwrap() {
                                    if let Completion::Signal(value) = completion {
                                        *value = "same-batch-early";
                                    }
                                }
                                batches.push(vec![Completion::Signal("fresh-signal")]);
                            }
                            let obs = record_with_fresh_completions(stamp, h, &batches, &[], true);
                            if stamp == "0.1.31" {
                                if matches!(shape, Shape::PositionalBarrier | Shape::ResolveAtCreation) {
                                    assert_signal_contract_read(&obs, "fresh-signal");
                                }
                                // CAN has its terminal action; all other decisions must replay without emission.
                                if obs.last().unwrap().live.starts_with("ContinueAsNew") {
                                    for o in &obs {
                                        assert_eq!(o.live, o.replay);
                                        assert_eq!(o.live_reads, o.replay_reads);
                                        assert!(o.replay_delta.is_empty());
                                    }
                                } else {
                                    assert_stable(&obs, &format!("family/{shape:?}/{batch:?}/{split}"));
                                }
                                asserted += 1;
                            } else {
                                if obs.iter().any(|o| o.live != o.replay || o.live_reads != o.replay_reads) {
                                    legacy_differences += 1;
                                }
                                let fingerprint = legacy_case_fingerprint(&obs);
                                legacy_digest.update(&fingerprint);
                                if let Some(trace) = &mut trace {
                                    writeln!(
                                        trace,
                                        "{cases}\t{shape:?}/{batch:?}/{iterations}/{eager}/{split}\t{}",
                                        std::str::from_utf8(&fingerprint).expect("JSON fingerprint must be UTF-8")
                                    )
                                    .expect("write legacy model trace");
                                }
                            }
                            cases += 1;
                        }
                    }
                }
            }
        }
    }
    let digest = format!("{:x}", legacy_digest.finalize());
    println!(
        "EXHAUSTIVE enumerated={cases} individually_asserted_new={asserted} legacy_known_differences={legacy_differences} legacy_sha256={digest} arms=1..3 completions=0..3 iterations=1..3"
    );
    if !record_legacy {
        // On mismatch follow "Legacy digests" above; never paste this branch's value.
        assert_eq!(
            digest,
            "b9ecaca1172c983ea9f1b1ed2b30b2544a13ca542210d02e7dcfc3de63a13d0e"
        );
        assert_eq!(legacy_differences, 115_640, "isolated-base characterization changed");
    }
}

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

#[test]
fn exhaustive_signal_slot_family_orders() {
    use sha2::{Digest, Sha256};
    let record_legacy = std::env::var_os("DUROXIDE_RECORD_LEGACY_MODEL").is_some();
    let verify_legacy = std::env::var_os("DUROXIDE_VERIFY_LEGACY_MODEL").is_some();
    let _quiet = tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    if record_legacy {
        assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.30");
        assert_eq!(
            format!("{:x}", Sha256::digest(include_str!("../src/lib.rs").as_bytes())),
            "c0f28773f23f9f0f1bab78d3472456a45c63e5dc6fbdd21854029fee2c1129a3"
        );
    }
    type SignalSequence = Vec<(&'static str, &'static str)>;
    let mut plans: Vec<(Shape, Vec<Vec<Completion>>, SignalSequence)> = Vec::new();
    for first in permutations(&[Completion::Timer(0), Completion::Signal("first")]) {
        let timer_first = matches!(first.first(), Some(Completion::Timer(0)));
        plans.push((
            Shape::UnboundSignalDrop,
            vec![first.clone(), vec![Completion::Signal("late")], vec![]],
            vec![("s", if timer_first { "first" } else { "late" })],
        ));
        plans.push((
            Shape::UnboundSignalDropBarrier,
            vec![
                first,
                vec![Completion::Activity(0)],
                vec![Completion::Signal("fresh")],
                vec![],
            ],
            vec![("s", "fresh")],
        ));
    }
    for first in permutations(&[Completion::Timer(0), Completion::Signal2("wrong-name-early")]) {
        for third in permutations(&[Completion::Activity(0), Completion::Signal2("fresh")]) {
            let accepted_fresh = matches!(third.first(), Some(Completion::Activity(0)));
            plans.push((
                Shape::WrongNameBeforeWait,
                vec![
                    first.clone(),
                    vec![Completion::Signal("alpha")],
                    third,
                    vec![Completion::Signal2("late")],
                    vec![],
                ],
                vec![("s", "alpha"), ("s2", if accepted_fresh { "fresh" } else { "late" })],
            ));
        }
    }
    for first in permutations(&[Completion::Signal("held"), Completion::Timer(0)]) {
        let timer_first = matches!(first.first(), Some(Completion::Timer(0)));
        for drop_held in [false, true] {
            plans.push((
                Shape::HeldSignalWait(drop_held),
                vec![first.clone(), vec![Completion::Signal("fresh")], vec![]],
                vec![("s", if drop_held && !timer_first { "fresh" } else { "held" })],
            ));
        }
    }
    for first in permutations(&[
        Completion::Activity(1),
        Completion::Signal2("R"),
        Completion::Activity(0),
        Completion::Signal("A"),
    ]) {
        let position = |activity: usize| {
            first
                .iter()
                .position(|value| matches!(value, Completion::Activity(index) if *index == activity))
                .unwrap()
        };
        let a = first
            .iter()
            .position(|value| matches!(value, Completion::Signal("A")))
            .unwrap();
        let r = first
            .iter()
            .position(|value| matches!(value, Completion::Signal2("R")))
            .unwrap();
        for follow in permutations(&[Completion::Signal("fresh-A"), Completion::Signal2("fresh-R")]) {
            let expected = if position(0) < a {
                ("s", "A")
            } else if position(1) < r {
                ("s2", "R")
            } else if matches!(follow.first(), Some(Completion::Signal(_))) {
                ("s", "fresh-A")
            } else {
                ("s2", "fresh-R")
            };
            plans.push((
                Shape::LazySignalCompetition,
                vec![first.clone(), follow, vec![]],
                vec![expected],
            ));
        }
    }
    for first in permutations(&[
        Completion::Signal("x"),
        Completion::Signal("stale"),
        Completion::Timer(0),
        Completion::Signal("fresh"),
    ]) {
        let deadline = first
            .iter()
            .position(|value| matches!(value, Completion::Timer(0)))
            .unwrap();
        let signal = |value: &Completion| match value {
            Completion::Signal(value) => Some(*value),
            _ => None,
        };
        let mut expected = Vec::new();
        if let Some(value) = first[..deadline].iter().find_map(signal) {
            expected.push(("s", value));
        }
        expected.push(("s", first[deadline + 1..].iter().find_map(signal).unwrap_or("late")));
        plans.push((
            Shape::DeadlineSignals,
            vec![first, vec![Completion::Signal("late")], vec![]],
            expected,
        ));
    }
    let mut legacy_digest = Sha256::new();
    let mut cases = 0;
    let mut asserted = 0;
    let mut legacy_differences = 0;
    for (shape, batches, expected) in plans {
        for eager in [false, true] {
            for stamp in ["0.1.30", "0.1.31"] {
                if (record_legacy || verify_legacy) && stamp != "0.1.30" {
                    continue;
                }
                let mut handler = Handler::new(shape.clone(), 1, vec![]);
                Arc::get_mut(&mut handler).unwrap().eager = eager;
                let expected = if stamp == "0.1.31" {
                    match &shape {
                        Shape::UnboundSignalDrop => vec![("s", "late")],
                        Shape::WrongNameBeforeWait => vec![("s", "alpha"), ("s2", "late")],
                        Shape::LazySignalCompetition => {
                            vec![if matches!(batches[1].first(), Some(Completion::Signal(_))) {
                                ("s", "fresh-A")
                            } else {
                                ("s2", "fresh-R")
                            }]
                        }
                        Shape::HeldSignalWait(true) => vec![("s", "fresh")],
                        Shape::DeadlineSignals => {
                            let mut values = Vec::new();
                            if let Some(Completion::Signal(value)) = batches[0].first() {
                                values.push(("s", *value));
                            }
                            values.push(("s", "late"));
                            values
                        }
                        _ => expected.clone(),
                    }
                } else {
                    expected.clone()
                };
                let observations = record_with_fresh_completions(stamp, handler, &batches, &[], true);
                if stamp == "0.1.31" {
                    assert_stable(&observations, &format!("signal-family/{shape:?}/{batches:?}/{eager}"));
                    let actual: Vec<_> = observations
                        .last()
                        .unwrap()
                        .live_reads
                        .iter()
                        .filter_map(|read| match read {
                            Read::Signal(value) => Some(("s", value.as_str())),
                            Read::Signal2(value) => Some(("s2", value.as_str())),
                            _ => None,
                        })
                        .collect();
                    assert_eq!(
                        actual, expected,
                        "{shape:?}/{batches:?}/{eager}: admitted signal lost, duplicated or assigned to the wrong name"
                    );
                    assert!(observations.last().unwrap().history.iter().any(|event|
                        matches!(&event.kind, EventKind::ActivityScheduled { name, .. } if name == "checkpoint")),
                        "signal-family program did not observe all required signals");
                    asserted += 1;
                } else {
                    legacy_digest.update(legacy_case_fingerprint(&observations));
                    if observations
                        .iter()
                        .any(|value| value.live != value.replay || value.live_reads != value.replay_reads)
                    {
                        legacy_differences += 1;
                    }
                }
                cases += 1;
            }
        }
    }
    let digest = format!("{:x}", legacy_digest.finalize());
    println!(
        "SIGNAL_FAMILIES enumerated={cases} individually_asserted_new={asserted} legacy_known_differences={legacy_differences} legacy_sha256={digest}"
    );
    assert_eq!(cases, if record_legacy || verify_legacy { 168 } else { 336 });
    if !record_legacy {
        // On mismatch follow "Legacy digests" above `exhaustive_small_scope_queue_race_orders`;
        // never paste this branch's value.
        assert_eq!(
            digest,
            "7f977b4e7f2c8fa329a55e6abd312bca2ea59d369adb8873523f35181f5704c0"
        );
    }
}

async fn runtime_seeded_scenario(shape: Shape, drain: Vec<Arm>, batch: Vec<Completion>) {
    use duroxide::providers::ExecutionMetadata;
    use duroxide::runtime::registry::ActivityRegistry;
    use duroxide::runtime::{OrchestrationStatus, Runtime, RuntimeOptions};
    use duroxide::{Client, OrchestrationRegistry};
    let (store, _temporary) = common::create_sqlite_store_disk().await;
    let handler = Handler::new(shape, 1, drain);
    let observations = record_and_replay("0.1.31", handler.clone(), &[batch]);
    assert_stable(&observations, "seeded SQLite runtime");
    let history = observations.last().unwrap().history.clone();
    common::seed_history_turn(
        store.as_ref(),
        WorkItem::StartOrchestration {
            instance: TEST_INSTANCE.into(),
            orchestration: TEST_ORCH_NAME.into(),
            version: Some(TEST_ORCH_VERSION.into()),
            input: "".into(),
            parent_instance: None,
            parent_id: None,
            parent_execution_id: None,
            execution_id: 1,
        },
        1,
        history.clone(),
        vec![],
        ExecutionMetadata {
            orchestration_name: Some(TEST_ORCH_NAME.into()),
            orchestration_version: Some(TEST_ORCH_VERSION.into()),
            ..Default::default()
        },
    )
    .await;
    let checkpoint = history
        .iter()
        .find(|event| matches!(&event.kind,EventKind::ActivityScheduled{name,..}if name=="checkpoint"));
    if checkpoint.is_none() {
        // This first turn must discard the stale positional signal. A fresh
        // signal is delivered in a later turn and must resolve the replacement.
        assert!(
            history
                .iter()
                .any(|e| matches!(e.kind, EventKind::ExternalSubscribedCancelled { .. }))
        );
        let handler_for_runtime = handler.clone();
        let registry = OrchestrationRegistry::builder()
            .register(TEST_ORCH_NAME, move |ctx, input| {
                let h = handler_for_runtime.clone();
                async move { h.invoke(ctx, input).await }
            })
            .build();
        let runtime = Runtime::start_with_options(
            store.clone(),
            ActivityRegistry::builder().build(),
            registry,
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .await;
        let client = Client::new(store.clone());
        client.raise_event(TEST_INSTANCE, "s", "fresh").await.unwrap();
        let processed = common::wait_for_history(
            store.clone(),
            TEST_INSTANCE,
            |events| {
                events
                    .iter()
                    .any(|event| matches!(&event.kind,EventKind::ExternalEvent{data,..}if data=="fresh"))
            },
            HISTORY_DEADLINE_MS,
        )
        .await;
        assert!(processed);
        assert!(
            common::wait_for_history(
                store.clone(),
                TEST_INSTANCE,
                |events| events
                    .iter()
                    .any(|event| matches!(&event.kind,EventKind::ActivityScheduled{name,input,..}
                if name=="checkpoint"&&input.contains("fresh"))),
                HISTORY_DEADLINE_MS
            )
            .await,
            "new policy did not resolve the replacement positional wait"
        );
        runtime.shutdown(None).await;
        return;
    }
    let checkpoint = checkpoint.unwrap();
    store
        .enqueue_for_orchestrator(activity_completed_msg(checkpoint.event_id, "ok"), None)
        .await
        .unwrap();
    let registry = OrchestrationRegistry::builder()
        .register(TEST_ORCH_NAME, move |ctx, input| {
            let handler = handler.clone();
            async move { handler.invoke(ctx, input).await }
        })
        .build();
    let runtime = Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        registry,
        RuntimeOptions {
            dispatcher_min_poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    )
    .await;
    let result = Client::new(store)
        .wait_for_orchestration(TEST_INSTANCE, RUNTIME_DEADLINE)
        .await
        .unwrap();
    assert!(matches!(result, OrchestrationStatus::Completed { .. }), "{result:?}");
    runtime.shutdown(None).await;
}

#[tokio::test]
async fn sqlite_q1_two_dequeue_losers_restart() {
    runtime_seeded_scenario(
        Shape::Race(vec![Arm::Q, Arm::Q, Arm::Timer]),
        vec![Arm::Q, Arm::Q],
        vec![Completion::Timer(0), Completion::Q("first"), Completion::Q("second")],
    )
    .await;
}

#[tokio::test]
async fn sqlite_q2_first_dequeue_wins_restart() {
    runtime_seeded_scenario(
        Shape::Race(vec![Arm::Q, Arm::Q, Arm::Timer]),
        vec![Arm::Q],
        vec![Completion::Q("first"), Completion::Q("second")],
    )
    .await;
}

#[tokio::test]
async fn sqlite_q6_nested_select_restart() {
    runtime_seeded_scenario(
        Shape::Nested,
        vec![Arm::Q],
        vec![Completion::Timer(1), Completion::Q("first")],
    )
    .await;
}

#[tokio::test]
async fn sqlite_s1_positional_same_batch_replays_after_restart() {
    runtime_seeded_scenario(
        Shape::Race(vec![Arm::Signal, Arm::Timer]),
        vec![Arm::Signal],
        vec![Completion::Timer(0), Completion::Signal("first")],
    )
    .await;
}

#[tokio::test]
async fn sqlite_s3_mixed_same_batch_replays_after_restart() {
    runtime_seeded_scenario(
        Shape::Race(vec![Arm::Signal, Arm::Q, Arm::Timer]),
        vec![Arm::Signal, Arm::Q],
        vec![
            Completion::Timer(0),
            Completion::Signal("first"),
            Completion::Q("first"),
        ],
    )
    .await;
}

#[tokio::test]
async fn sqlite_legacy_safe_history_keeps_old_decisions() {
    use duroxide::runtime::registry::ActivityRegistry;
    use duroxide::runtime::{Runtime, RuntimeOptions};
    use duroxide::{Client, OrchestrationRegistry, OrchestrationStatus};
    let (store, _temporary) = common::create_sqlite_store_disk().await;
    let handler = Handler::new(Shape::Race(vec![Arm::Q, Arm::Timer]), 1, vec![]);
    let observations = record_and_replay("0.1.30", handler.clone(), &[vec![Completion::Q("first")]]);
    assert_stable(&observations, "legacy-safe runtime");
    let history = observations.last().unwrap().history.clone();
    let checkpoint = history
        .iter()
        .find(|event| matches!(&event.kind,EventKind::ActivityScheduled{name,..}if name=="checkpoint"))
        .unwrap()
        .event_id;
    common::seed_history_turn(
        store.as_ref(),
        WorkItem::StartOrchestration {
            instance: TEST_INSTANCE.into(),
            orchestration: TEST_ORCH_NAME.into(),
            input: "".into(),
            version: Some(TEST_ORCH_VERSION.into()),
            parent_instance: None,
            parent_id: None,
            parent_execution_id: None,
            execution_id: 1,
        },
        1,
        history,
        vec![activity_completed_msg(checkpoint, "ok")],
        duroxide::providers::ExecutionMetadata {
            orchestration_name: Some(TEST_ORCH_NAME.into()),
            orchestration_version: Some(TEST_ORCH_VERSION.into()),
            ..Default::default()
        },
    )
    .await;
    let registry = OrchestrationRegistry::builder()
        .register(TEST_ORCH_NAME, move |ctx, input| {
            let h = handler.clone();
            async move { h.invoke(ctx, input).await }
        })
        .build();
    let runtime = Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        registry,
        RuntimeOptions {
            dispatcher_min_poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    )
    .await;
    let result = Client::new(store.clone())
        .wait_for_orchestration(TEST_INSTANCE, RUNTIME_DEADLINE)
        .await
        .unwrap();
    assert!(matches!(result,OrchestrationStatus::Completed{ref output,..}if output.contains("first")));
    assert_eq!(store.read(TEST_INSTANCE).await.unwrap()[0].duroxide_version, "0.1.30");
    runtime.shutdown(None).await;
}

#[tokio::test]
async fn sqlite_legacy_hazard_keeps_the_retained_failure() {
    use duroxide::runtime::registry::ActivityRegistry;
    use duroxide::runtime::{Runtime, RuntimeOptions};
    use duroxide::{Client, OrchestrationRegistry, OrchestrationStatus};
    let (store, _temporary) = common::create_sqlite_store_disk().await;
    let handler = Handler::new(Shape::Race(vec![Arm::Q, Arm::Timer]), 1, vec![Arm::Q]);
    let observations = record_and_replay(
        "0.1.30",
        handler.clone(),
        &[vec![Completion::Timer(0), Completion::Q("first")]],
    );
    let history = observations.last().unwrap().history.clone();
    common::seed_history_turn(
        store.as_ref(),
        WorkItem::StartOrchestration {
            instance: TEST_INSTANCE.into(),
            orchestration: TEST_ORCH_NAME.into(),
            input: "".into(),
            version: Some(TEST_ORCH_VERSION.into()),
            parent_instance: None,
            parent_id: None,
            parent_execution_id: None,
            execution_id: 1,
        },
        1,
        history,
        vec![WorkItem::QueueMessage {
            instance: TEST_INSTANCE.into(),
            name: "q".into(),
            data: "second".into(),
        }],
        duroxide::providers::ExecutionMetadata {
            orchestration_name: Some(TEST_ORCH_NAME.into()),
            orchestration_version: Some(TEST_ORCH_VERSION.into()),
            ..Default::default()
        },
    )
    .await;
    let registry = OrchestrationRegistry::builder()
        .register(TEST_ORCH_NAME, move |ctx, input| {
            let h = handler.clone();
            async move { h.invoke(ctx, input).await }
        })
        .build();
    let runtime = Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        registry,
        RuntimeOptions {
            dispatcher_min_poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    )
    .await;
    let result = Client::new(store)
        .wait_for_orchestration(TEST_INSTANCE, RUNTIME_DEADLINE)
        .await
        .unwrap();
    assert!(
        matches!(result,OrchestrationStatus::Failed{ref details,..}if details.display_message().contains("history schedule but no emitted action"))
    );
    runtime.shutdown(None).await;
}

/// Unread queue messages survive two continue-as-new boundaries. A 0.1.30 execution
/// carries by the legacy count (one read of three arrivals leaves two); executions it
/// continues into are stamped by this runtime and carry exact unread positions.
#[tokio::test]
async fn sqlite_unread_messages_carry_across_two_continue_as_new_for_both_policies() {
    use duroxide::runtime::registry::ActivityRegistry;
    use duroxide::runtime::{Runtime, RuntimeOptions};
    use duroxide::{Client, OrchestrationRegistry, OrchestrationStatus};
    for stamp in [semver::Version::new(0, 1, 30), semver::Version::new(0, 1, 31)] {
        let (store, _temporary) = common::create_sqlite_store_disk().await;
        common::seed_instance_with_pinned_version(store.as_ref(), TEST_INSTANCE, TEST_ORCH_NAME, stamp.clone()).await;
        let client = Client::new(store.clone());
        // All three arrive in execution 1's first batch; nothing is sent later.
        for message in ["a", "b", "c"] {
            client.enqueue_event(TEST_INSTANCE, "q", message).await.unwrap();
        }
        let registry = OrchestrationRegistry::builder()
            .register(TEST_ORCH_NAME, |ctx: OrchestrationContext, input: String| async move {
                let read = ctx.dequeue_event("q").await;
                let seen = if ctx.execution_id() == 1 {
                    read
                } else {
                    format!("{input},{read}")
                };
                if ctx.execution_id() < 3 {
                    ctx.continue_as_new(seen).await
                } else {
                    Ok(seen)
                }
            })
            .build();
        let runtime = Runtime::start_with_options(
            store.clone(),
            ActivityRegistry::builder().build(),
            registry,
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .await;
        let result = client.wait_for_orchestration(TEST_INSTANCE, RUNTIME_DEADLINE).await;
        runtime.shutdown(None).await;
        assert!(
            matches!(result, Ok(OrchestrationStatus::Completed { ref output, .. }) if output == "a,b,c"),
            "{stamp}: both unread messages must carry, then the last one again: {result:?}"
        );
    }
}

#[tokio::test]
async fn sqlite_q13_legacy_continue_as_new_gets_new_execution_policy() {
    use duroxide::runtime::registry::ActivityRegistry;
    use duroxide::runtime::{Runtime, RuntimeOptions};
    use duroxide::{Client, Either2, OrchestrationRegistry, OrchestrationStatus};
    let (store, _temporary) = common::create_sqlite_store_disk().await;
    common::seed_instance_with_pinned_version(
        store.as_ref(),
        TEST_INSTANCE,
        TEST_ORCH_NAME,
        semver::Version::new(0, 1, 30),
    )
    .await;
    let registry = OrchestrationRegistry::builder()
        .register(TEST_ORCH_NAME, |ctx: OrchestrationContext, _: String| async move {
            if ctx.execution_id() == 1 {
                let _ = ctx
                    .select2(ctx.dequeue_event("q"), ctx.schedule_timer(Duration::from_millis(10)))
                    .await;
                ctx.continue_as_new("successor").await
            } else {
                let first = ctx.dequeue_event("q");
                let second = ctx.dequeue_event("q");
                let t0 = ctx.schedule_timer(Duration::from_secs(60));
                let t1 = ctx.schedule_timer(Duration::from_secs(120));
                let _ = ctx.select2(first, t0).await;
                match ctx.select2(second, t1).await {
                    Either2::First(value) => Ok(value),
                    Either2::Second(()) => Ok("second-timeout".into()),
                }
            }
        })
        .build();
    let runtime = Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        registry.clone(),
        RuntimeOptions {
            dispatcher_min_poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    )
    .await;
    let client = Client::new(store.clone());
    assert!(
        common::wait_for_history(
            store.clone(),
            TEST_INSTANCE,
            |history| history.first().is_some_and(|event| event.execution_id == 2)
                && history
                    .iter()
                    .filter(|event| matches!(event.kind, EventKind::TimerCreated { .. }))
                    .count()
                    == 2,
            HISTORY_DEADLINE_MS
        )
        .await
    );
    runtime.shutdown(None).await;
    let successor = client.read_execution_history(TEST_INSTANCE, 2).await.unwrap();
    let timers: Vec<_> = successor
        .iter()
        .filter_map(|e| {
            if let EventKind::TimerCreated { fire_at_ms } = e.kind {
                Some(WorkItem::TimerFired {
                    instance: TEST_INSTANCE.into(),
                    execution_id: 2,
                    id: e.event_id(),
                    fire_at_ms,
                })
            } else {
                None
            }
        })
        .collect();
    for message in [
        timers[0].clone(),
        WorkItem::QueueMessage {
            instance: TEST_INSTANCE.into(),
            name: "q".into(),
            data: "first".into(),
        },
        timers[1].clone(),
    ] {
        store.enqueue_for_orchestrator(message, None).await.unwrap();
    }
    let runtime = Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        registry,
        RuntimeOptions::default(),
    )
    .await;
    let result = client
        .wait_for_orchestration(TEST_INSTANCE, RUNTIME_DEADLINE)
        .await
        .unwrap();
    assert!(matches!(result,OrchestrationStatus::Completed{ref output,..}if output=="first"));
    let old = client.read_execution_history(TEST_INSTANCE, 1).await.unwrap();
    let new = client.read_execution_history(TEST_INSTANCE, 2).await.unwrap();
    assert_eq!(old[0].duroxide_version, "0.1.30");
    assert!(semver::Version::parse(&new[0].duroxide_version).unwrap() >= semver::Version::new(0, 1, 31));
    runtime.shutdown(None).await;
}

#[test]
fn positional_s1_same_batch_preserves_bound_slot_signal_contract() {
    for stamp in ["0.1.30", "0.1.31"] {
        let handler = Handler::new(Shape::Race(vec![Arm::Signal, Arm::Timer]), 1, vec![Arm::Signal]);
        let observations = record_and_replay(
            stamp,
            handler,
            &[
                vec![Completion::Timer(0), Completion::Signal("first")],
                vec![Completion::Signal("second")],
            ],
        );
        assert_stable(&observations, &format!("S1/{stamp}"));
        let reads = &observations.last().unwrap().live_reads;
        if stamp == "0.1.31" {
            assert!(reads.contains(&Read::Signal("second".into())));
        } else {
            assert!(!reads.iter().any(|read| matches!(read, Read::Signal(_))));
        }
    }
}

#[test]
fn queue_q1_select3_two_losers_preserves_fifo_and_legacy_outcome() {
    for count in 0..=3 {
        let mut batch = vec![Completion::Timer(0)];
        batch.extend(
            [Completion::Q("first"), Completion::Q("second"), Completion::Q("third")]
                .into_iter()
                .take(count),
        );
        queue_scenario(
            &format!("Q1/count{count}"),
            Shape::Race(vec![Arm::Q, Arm::Q, Arm::Timer]),
            vec![Arm::Q; count],
            vec![batch],
            ["first", "second", "third"]
                .into_iter()
                .take(count)
                .map(|value| ("q", value))
                .collect(),
        );
    }
}

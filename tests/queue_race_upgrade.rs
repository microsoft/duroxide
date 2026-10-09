// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::clone_on_ref_ptr)]

#[path = "replay_engine/helpers.rs"]
#[allow(dead_code)]
mod helpers;

#[path = "common/cancellation_history.rs"]
mod cancellation_history;
use cancellation_history::decision_delta;

use async_trait::async_trait;
use duroxide::providers::WorkItem;
use duroxide::runtime::replay_engine::TurnResult;
use duroxide::{Action, Either2, Event, EventKind, OrchestrationContext, OrchestrationHandler};
use helpers::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

const OLD_SHA: &str = "6a458861763a7aa5b78a7c1c97691a6f00489a8b";
const OLD_LIB_SHA256: &str = "c0f28773f23f9f0f1bab78d3472456a45c63e5dc6fbdd21854029fee2c1129a3";
const OLD_ENGINE_SHA256: &str = "b85bf87ec75d63db0befff18e9dc32d2385b721055b441b42c607477f0242deb";

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum Mode {
    Ordinary,
    Loop,
    Drain,
    Barrier,
    PreboundCompetition,
    Unbound,
    PositionalBarrier,
    PositionalBlocking,
    ResolveAtCreation,
    ContinueDrop,
    ContinueDrain,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Handler {
    mode: Mode,
    lazy: bool,
}

impl Handler {
    async fn race(&self, ctx: &OrchestrationContext) -> Either2<String, ()> {
        if self.lazy {
            ctx.select2(async { ctx.dequeue_event("q").await }, async {
                ctx.schedule_timer(Duration::from_millis(20)).await
            })
            .await
        } else {
            ctx.select2(ctx.dequeue_event("q"), ctx.schedule_timer(Duration::from_millis(20)))
                .await
        }
    }
}

#[async_trait]
impl OrchestrationHandler for Handler {
    async fn invoke(&self, ctx: OrchestrationContext, _: String) -> Result<String, String> {
        let value = match self.mode {
            Mode::PositionalBarrier | Mode::PositionalBlocking => {
                let result = ctx
                    .select2(ctx.schedule_wait("s"), ctx.schedule_timer(Duration::from_millis(10)))
                    .await;
                if matches!(result, Either2::Second(())) && matches!(self.mode, Mode::PositionalBarrier) {
                    ctx.schedule_activity("tick", "").await?;
                }
                ctx.schedule_wait("s").await
            }
            Mode::ResolveAtCreation => {
                let wait = ctx.schedule_wait("s");
                ctx.schedule_activity("work", "").await?;
                drop(wait);
                ctx.schedule_activity("work2", "").await?;
                ctx.schedule_wait("s").await
            }
            Mode::ContinueDrop | Mode::ContinueDrain => {
                let _ = ctx
                    .select2(ctx.dequeue_event("q"), ctx.schedule_timer(Duration::from_millis(10)))
                    .await;
                if matches!(self.mode, Mode::ContinueDrain) {
                    ctx.dequeue_event("q").await;
                }
                return ctx.continue_as_new("successor").await;
            }
            Mode::Ordinary => {
                let result = ctx.schedule_activity("ordinary", "input").await?;
                ctx.schedule_timer(Duration::from_millis(20)).await;
                let first = ctx.dequeue_event("q").await;
                let second = ctx.dequeue_event("other").await;
                return Ok(format!("{result}:{first}:{second}"));
            }
            Mode::PreboundCompetition => {
                let first = ctx.dequeue_event("q");
                let second = ctx.dequeue_event("q");
                let first_timer = ctx.schedule_timer(Duration::from_millis(20));
                let second_timer = ctx.schedule_timer(Duration::from_millis(30));
                let _ = ctx.select2(first, first_timer).await;
                match ctx.select2(second, second_timer).await {
                    Either2::First(message) => message,
                    Either2::Second(()) => "second-timeout".to_string(),
                }
            }
            Mode::Unbound => {
                drop(ctx.dequeue_event("q"));
                ctx.dequeue_event("q").await
            }
            Mode::Loop | Mode::Drain | Mode::Barrier => {
                let mut messages = Vec::new();
                let mut timers = 0;
                while messages.len() < 2 {
                    match self.race(&ctx).await {
                        Either2::First(message) => messages.push(message),
                        Either2::Second(()) => {
                            timers += 1;
                            if matches!(self.mode, Mode::Barrier) {
                                ctx.schedule_activity("barrier", "").await?;
                            }
                            if matches!(self.mode, Mode::Drain | Mode::Barrier) {
                                messages.push(ctx.dequeue_event("q").await);
                            }
                        }
                    }
                }
                format!("{timers}:{}", messages.join(","))
            }
        };
        ctx.schedule_activity("checkpoint", &value).await?;
        Ok(value)
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
enum Outcome {
    Continue,
    Completed(String),
    Failed(String),
    ContinueAsNew(String),
}

fn outcome(result: TurnResult) -> Outcome {
    match result {
        TurnResult::Continue => Outcome::Continue,
        TurnResult::Completed(value) => Outcome::Completed(value),
        TurnResult::Failed(error) => Outcome::Failed(error.display_message()),
        TurnResult::ContinueAsNew { input, .. } => Outcome::ContinueAsNew(input),
        other => panic!("unexpected result {other:?}"),
    }
}

fn action_shapes(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .map(|action| match action {
            Action::CallActivity {
                scheduling_event_id,
                name,
                input,
                ..
            } => format!("{scheduling_event_id}:activity:{name}:{input}"),
            Action::CreateTimer {
                scheduling_event_id, ..
            } => format!("{scheduling_event_id}:timer"),
            Action::DequeueEvent {
                scheduling_event_id,
                name,
            } => format!("{scheduling_event_id}:queue:{name}"),
            Action::WaitExternal {
                scheduling_event_id,
                name,
            } => format!("{scheduling_event_id}:signal:{name}"),
            Action::ContinueAsNew { input, version } => format!("continue:{input}:{version:?}"),
            other => panic!("unexpected action {other:?}"),
        })
        .collect()
}

#[derive(Debug, Deserialize, Serialize)]
struct Snapshot {
    name: String,
    handler: Handler,
    history: Vec<Event>,
    old_live_outcome: Outcome,
    old_live_actions: Vec<String>,
    old_replay_outcome: Outcome,
    old_replay_actions: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Fixtures {
    recording_source: String,
    compiled_lib_sha256: String,
    compiled_replay_engine_sha256: String,
    recording_test_sha256: String,
    recording_comparator_sha256: String,
    snapshots: Vec<Snapshot>,
}

fn read_fixtures() -> Fixtures {
    let fixtures: Fixtures = serde_json::from_str(include_str!("fixtures/queue_race_old_engine.json")).unwrap();
    assert_eq!(fixtures.compiled_lib_sha256, OLD_LIB_SHA256);
    assert_eq!(fixtures.compiled_replay_engine_sha256, OLD_ENGINE_SHA256);
    assert_eq!(
        fixtures.recording_test_sha256,
        compiled_sha256(include_str!("queue_race_upgrade.rs"))
    );
    assert_eq!(
        fixtures.recording_comparator_sha256,
        compiled_sha256(include_str!("common/cancellation_history.rs"))
    );
    fixtures
}

fn compiled_sha256(source: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

#[test]
fn legacy_independent_cancellation_permutations_replay_identically() {
    let fixtures = read_fixtures();
    let snapshot = fixtures
        .snapshots
        .iter()
        .find(|snapshot| snapshot.name == "prebound-queue-versus-second-timer/turn-1")
        .unwrap();
    let mut reversed = snapshot.history.clone();
    let indices: Vec<_> = reversed
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            matches!(&event.kind,EventKind::QueueSubscriptionCancelled{reason}if reason=="dropped_future")
                .then_some(index)
        })
        .collect();
    assert_eq!(indices.len(), 2);
    let a = indices[0];
    let b = indices[1];
    assert_eq!(b, a + 1);
    let id_a = reversed[a].event_id;
    let id_b = reversed[b].event_id;
    reversed.swap(a, b);
    reversed[a].event_id = id_a;
    reversed[b].event_id = id_b;
    assert_eq!(decision_delta(&snapshot.history), decision_delta(&reversed));
    for history in [snapshot.history.clone(), reversed] {
        let mut engine = create_engine(history);
        assert_eq!(
            outcome(execute(&mut engine, Arc::new(snapshot.handler.clone()))),
            snapshot.old_replay_outcome
        );
        assert!(engine.history_delta().is_empty());
        assert_eq!(action_shapes(engine.pending_actions()), snapshot.old_replay_actions);
    }
    // A different cancellation source is not a harmless permutation.
    let mut changed = snapshot.history.clone();
    changed[a].source_event_id = Some(999);
    assert_ne!(decision_delta(&snapshot.history), decision_delta(&changed));
    let mut changed = snapshot.history.clone();
    if let EventKind::QueueSubscriptionCancelled { reason } = &mut changed[a].kind {
        *reason = "continued_as_new".into();
    }
    assert_ne!(decision_delta(&snapshot.history), decision_delta(&changed));
    let mut changed = snapshot.history.clone();
    changed[b].source_event_id = changed[a].source_event_id;
    assert_ne!(decision_delta(&snapshot.history), decision_delta(&changed));
    let mut changed = snapshot.history.clone();
    changed.swap(a, a - 1);
    assert_ne!(decision_delta(&snapshot.history), decision_delta(&changed));
}
#[test]
fn old_engine_recorded_queue_histories_replay_identically() {
    let fixtures = read_fixtures();
    assert_eq!(fixtures.snapshots.len(), 70, "missing old-engine recordings");
    let mut mismatches = Vec::new();
    for snapshot in fixtures.snapshots {
        assert!(snapshot.history.iter().all(|event| event.duroxide_version == "0.1.30"));
        let mut engine = create_engine(snapshot.history);
        let result = outcome(execute(&mut engine, Arc::new(snapshot.handler)));
        let actions = action_shapes(engine.pending_actions());
        assert!(
            engine.history_delta().is_empty(),
            "{}: new replay history",
            snapshot.name
        );
        if result != snapshot.old_replay_outcome || actions != snapshot.old_replay_actions {
            mismatches.push(format!(
                "{}: old={:?}/{:?}, new={result:?}/{actions:?}",
                snapshot.name, snapshot.old_replay_outcome, snapshot.old_replay_actions
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "old history replay changed:\n{}",
        mismatches.join("\n")
    );
}

#[test]
fn old_engine_executions_resume_identically_after_upgrade() {
    let fixtures = read_fixtures();
    let mut compared = 0;
    for pair in fixtures.snapshots.windows(2) {
        let [before, after] = pair else { unreachable!() };
        if before.name.rsplit_once('/').unwrap().0 != after.name.rsplit_once('/').unwrap().0 {
            continue;
        }
        assert_eq!(&after.history[..before.history.len()], before.history.as_slice());
        let completions = after.history[before.history.len()..]
            .iter()
            .filter_map(|event| match &event.kind {
                duroxide::EventKind::QueueEventDelivered { name, data } => Some(WorkItem::QueueMessage {
                    instance: TEST_INSTANCE.into(),
                    name: name.clone(),
                    data: data.clone(),
                }),
                duroxide::EventKind::TimerFired { fire_at_ms } => {
                    Some(timer_fired_msg(event.source_event_id.unwrap(), *fire_at_ms))
                }
                duroxide::EventKind::ActivityCompleted { result } => {
                    Some(activity_completed_msg(event.source_event_id.unwrap(), result))
                }
                duroxide::EventKind::ExternalEvent { name, data } => Some(WorkItem::ExternalRaised {
                    instance: TEST_INSTANCE.into(),
                    name: name.clone(),
                    data: data.clone(),
                }),
                _ => None,
            })
            .collect();
        let mut engine = create_engine(before.history.clone());
        engine.prep_completions(completions);
        assert_eq!(
            outcome(execute(&mut engine, Arc::new(before.handler.clone()))),
            after.old_live_outcome,
            "resuming {}",
            before.name
        );
        assert_eq!(
            action_shapes(engine.pending_actions()),
            after.old_live_actions,
            "new decisions after {}",
            before.name
        );
        assert_eq!(
            decision_delta(engine.history_delta()),
            decision_delta(&after.history[before.history.len()..]),
            "history delta after {}",
            before.name
        );
        compared += 1;
    }
    assert_eq!(compared, 52);
}

#[test]
fn new_engine_executions_use_immediate_queue_cancellation() {
    let handler = Handler {
        mode: Mode::PreboundCompetition,
        lazy: false,
    };
    let mut history = vec![started_event(1)];
    history[0].duroxide_version = "0.1.31".into();
    let mut first = create_engine(history.clone());
    assert_eq!(
        outcome(execute(&mut first, Arc::new(handler.clone()))),
        Outcome::Continue
    );
    history.extend_from_slice(first.history_delta());
    let mut second = create_engine(history.clone());
    second.prep_completions(messages(
        &history,
        vec![
            Completion::Timer(0),
            Completion::Message("q", "first"),
            Completion::Timer(1),
        ],
    ));
    assert_eq!(
        outcome(execute(&mut second, Arc::new(handler.clone()))),
        Outcome::Continue
    );
    assert!(
        action_shapes(second.pending_actions())
            .iter()
            .any(|action| action.ends_with(":checkpoint:first"))
    );
    history.extend_from_slice(second.history_delta());
    let mut replay = create_engine(history.clone());
    assert_eq!(
        outcome(execute(&mut replay, Arc::new(handler.clone()))),
        Outcome::Continue
    );
    assert!(replay.pending_actions().is_empty());
    let mut final_turn = create_engine(history.clone());
    final_turn.prep_completions(messages(&history, vec![Completion::Activity(0)]));
    assert_eq!(
        outcome(execute(&mut final_turn, Arc::new(handler))),
        Outcome::Completed("first".into())
    );
}

enum Completion {
    Message(&'static str, &'static str),
    Timer(usize),
    Activity(usize),
    Signal(&'static str),
}

fn messages(history: &[Event], completions: Vec<Completion>) -> Vec<WorkItem> {
    use duroxide::EventKind;
    completions
        .into_iter()
        .map(|completion| match completion {
            Completion::Signal(data) => external_raised_msg("s", data),
            Completion::Message(name, data) => WorkItem::QueueMessage {
                instance: TEST_INSTANCE.into(),
                name: name.into(),
                data: data.into(),
            },
            Completion::Timer(index) => {
                let event = history
                    .iter()
                    .filter(|e| matches!(e.kind, EventKind::TimerCreated { .. }))
                    .nth(index)
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
                    .nth(index)
                    .unwrap();
                activity_completed_msg(event.event_id, "ok")
            }
        })
        .collect()
}

fn record(snapshots: &mut Vec<Snapshot>, name: &str, handler: Handler, batches: Vec<Vec<Completion>>) {
    let mut history = vec![started_event(1)];
    history[0].duroxide_version = "0.1.30".into();
    for (index, batch) in batches.into_iter().enumerate() {
        let mut engine = create_engine(history.clone());
        engine.prep_completions(messages(&history, batch));
        let live = outcome(execute(&mut engine, Arc::new(handler.clone())));
        history.extend_from_slice(engine.history_delta());
        let mut replay = create_engine(history.clone());
        let replay_result = outcome(execute(&mut replay, Arc::new(handler.clone())));
        snapshots.push(Snapshot {
            name: format!("{name}/turn-{index}"),
            handler: handler.clone(),
            history: history.clone(),
            old_live_outcome: live,
            old_live_actions: action_shapes(engine.pending_actions()),
            old_replay_outcome: replay_result,
            old_replay_actions: action_shapes(replay.pending_actions()),
        });
    }
}

#[test]
#[ignore = "fixture recorder: run only against the pinned unmodified 0.1.30 engine"]
fn record_old_engine_queue_histories() {
    let lib_hash = compiled_sha256(include_str!("../src/lib.rs"));
    let engine_hash = compiled_sha256(include_str!("../src/runtime/replay_engine.rs"));
    assert_eq!(lib_hash, OLD_LIB_SHA256, "recorder compiled non-base lib.rs");
    assert_eq!(
        engine_hash, OLD_ENGINE_SHA256,
        "recorder compiled non-base replay engine"
    );
    let target = std::env::var("CARGO_TARGET_DIR").expect("fresh isolated CARGO_TARGET_DIR required");
    let marker = std::fs::read_to_string(std::path::Path::new(&target).join(".fixture-recording-fresh"))
        .expect("runner must create freshness marker before the first build in an empty target");
    assert_eq!(marker.trim(), OLD_SHA);
    let rlibs = std::fs::read_dir(std::path::Path::new(&target).join("debug/deps"))
        .unwrap()
        .map(|entry| entry.expect("read recorder dependency entry"))
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("libduroxide-") && name.ends_with(".rlib")
        })
        .count();
    assert_eq!(rlibs, 1, "recorder target contains multiple core libraries");
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.30");
    let destination = std::env::var("DUROXIDE_OLD_FIXTURE_OUT").unwrap();
    let mut snapshots = Vec::new();
    use Completion::{Activity, Message, Timer};
    record(
        &mut snapshots,
        "ordinary",
        Handler {
            mode: Mode::Ordinary,
            lazy: false,
        },
        vec![
            vec![],
            vec![Activity(0)],
            vec![Timer(0)],
            vec![Message("q", "first")],
            vec![Message("other", "second")],
        ],
    );
    for lazy in [false, true] {
        for mode in [Mode::Loop, Mode::Drain] {
            record(
                &mut snapshots,
                &format!("message-first-{mode:?}-lazy-{lazy}"),
                Handler { mode, lazy },
                vec![
                    vec![],
                    vec![Message("q", "first"), Timer(0)],
                    vec![Message("q", "second")],
                    vec![Activity(0)],
                ],
            );
            record(
                &mut snapshots,
                &format!("adjacent-turn-{mode:?}-lazy-{lazy}"),
                Handler { mode, lazy },
                vec![
                    vec![],
                    vec![Timer(0)],
                    vec![Message("q", "first")],
                    vec![Message("q", "second")],
                    vec![Activity(0)],
                ],
            );
        }
        record(
            &mut snapshots,
            &format!("timer-first-barrier-lazy-{lazy}"),
            Handler {
                mode: Mode::Barrier,
                lazy,
            },
            vec![
                vec![],
                vec![Timer(0), Message("q", "first")],
                vec![Activity(0)],
                vec![Message("q", "second")],
                vec![Activity(1)],
            ],
        );
    }
    record(
        &mut snapshots,
        "prebound-queue-versus-second-timer",
        Handler {
            mode: Mode::PreboundCompetition,
            lazy: false,
        },
        vec![
            vec![],
            vec![Timer(0), Message("q", "first"), Timer(1)],
            vec![Activity(0)],
        ],
    );
    record(
        &mut snapshots,
        "unbound-prearrival",
        Handler {
            mode: Mode::Unbound,
            lazy: false,
        },
        vec![
            vec![Message("q", "first")],
            vec![Message("q", "second")],
            vec![Activity(0)],
        ],
    );
    assert!(
        snapshots.iter().any(|snapshot| {
            snapshot.name == "prebound-queue-versus-second-timer/turn-2"
                && snapshot.old_live_outcome == Outcome::Completed("second-timeout".into())
        }),
        "the recorder did not execute legacy queue semantics"
    );
    assert!(
        snapshots
            .iter()
            .any(|snapshot| snapshot.name == "unbound-prearrival/turn-0"
                && snapshot
                    .old_live_actions
                    .iter()
                    .all(|action| !action.contains(":checkpoint:"))),
        "unbound-path canary did not use old binding semantics"
    );
    record(
        &mut snapshots,
        "positional-barrier",
        Handler {
            mode: Mode::PositionalBarrier,
            lazy: false,
        },
        vec![vec![], vec![Timer(0), Completion::Signal("a")], vec![Activity(0)]],
    );
    record(
        &mut snapshots,
        "positional-blocking",
        Handler {
            mode: Mode::PositionalBlocking,
            lazy: false,
        },
        vec![
            vec![],
            vec![Timer(0), Completion::Signal("a")],
            vec![Completion::Signal("b")],
        ],
    );
    record(
        &mut snapshots,
        "positional-resolve-at-creation",
        Handler {
            mode: Mode::ResolveAtCreation,
            lazy: false,
        },
        vec![vec![], vec![Completion::Signal("a"), Activity(0)], vec![Activity(1)]],
    );
    record(
        &mut snapshots,
        "continue-drop",
        Handler {
            mode: Mode::ContinueDrop,
            lazy: false,
        },
        vec![vec![], vec![Timer(0), Message("q", "a")]],
    );
    record(
        &mut snapshots,
        "continue-drain",
        Handler {
            mode: Mode::ContinueDrain,
            lazy: false,
        },
        vec![vec![], vec![Timer(0), Message("q", "a"), Message("q", "b")]],
    );
    let positional = snapshots
        .iter()
        .find(|s| s.name == "positional-blocking/turn-2")
        .unwrap();
    assert_eq!(
        positional.old_live_outcome,
        Outcome::Continue,
        "recorder repaired the legacy positional hang"
    );
    for name in ["continue-drop/turn-1", "continue-drain/turn-1"] {
        let snapshot = snapshots.iter().find(|s| s.name == name).unwrap();
        assert_eq!(snapshot.old_live_outcome, Outcome::ContinueAsNew("successor".into()));
        assert!(
            !snapshot
                .history
                .iter()
                .any(|e| matches!(e.kind, EventKind::QueueSubscriptionCancelled { .. })),
            "recorder used corrected continue-as-new cancellation"
        );
    }
    let fixtures = Fixtures {
        recording_source: OLD_SHA.into(),
        compiled_lib_sha256: lib_hash,
        compiled_replay_engine_sha256: engine_hash,
        recording_test_sha256: compiled_sha256(include_str!("queue_race_upgrade.rs")),
        recording_comparator_sha256: compiled_sha256(include_str!("common/cancellation_history.rs")),
        snapshots,
    };
    std::fs::write(destination, serde_json::to_string_pretty(&fixtures).unwrap()).unwrap();
}

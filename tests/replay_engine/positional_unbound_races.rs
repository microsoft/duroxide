// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use super::helpers::*;
use async_trait::async_trait;
use duroxide::providers::WorkItem;
use duroxide::{Either2, Event, EventKind, OrchestrationContext, OrchestrationHandler};
use std::sync::Arc;
use std::time::Duration;

struct UnboundDropHandler;

#[async_trait]
impl OrchestrationHandler for UnboundDropHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _: String) -> Result<String, String> {
        ctx.schedule_timer(Duration::from_millis(10)).await;
        drop(ctx.schedule_wait("s"));
        Ok(ctx.schedule_wait("s").await)
    }
}

fn scheduling_history(handler: Arc<dyn OrchestrationHandler>) -> Vec<Event> {
    let mut history = vec![started_event(1)];
    let mut first = create_engine(history.clone());
    assert_continue(&execute(&mut first, handler));
    history.extend_from_slice(first.history_delta());
    history
}

fn timer_completion(history: &[Event]) -> WorkItem {
    let timer = history
        .iter()
        .find(|event| matches!(event.kind, EventKind::TimerCreated { .. }))
        .expect("handler must schedule a timer");
    let EventKind::TimerCreated { fire_at_ms } = timer.kind else {
        unreachable!()
    };
    timer_fired_msg(timer.event_id, fire_at_ms)
}

fn assert_live_and_replay(
    handler: Arc<dyn OrchestrationHandler>,
    mut history: Vec<Event>,
    messages: Vec<WorkItem>,
    expected: &str,
) {
    let mut live = create_engine(history.clone());
    live.prep_completions(messages);
    assert_completed(&execute(&mut live, handler.clone()), expected);
    history.extend_from_slice(live.history_delta());
    let mut replay = create_engine(history);
    assert_completed(&execute(&mut replay, handler), expected);
    assert!(replay.history_delta().is_empty());
}

#[test]
fn ms17a_unbound_replacement_requires_a_fresh_signal_after_binding() {
    let handler: Arc<dyn OrchestrationHandler> = Arc::new(UnboundDropHandler);
    let history = scheduling_history(handler.clone());
    let timer = timer_completion(&history);
    assert_early_batch_then_fresh(handler, history, vec![timer, external_raised_msg("s", "a")], "b");
}

fn assert_early_batch_then_fresh(
    handler: Arc<dyn OrchestrationHandler>,
    mut history: Vec<Event>,
    batch: Vec<WorkItem>,
    expected: &str,
) {
    let mut live = create_engine(history.clone());
    live.prep_completions(batch);
    assert_continue(&execute(&mut live, handler.clone()));
    history.extend_from_slice(live.history_delta());
    let mut replay = create_engine(history.clone());
    assert_continue(&execute(&mut replay, handler.clone()));
    assert!(replay.history_delta().is_empty());
    assert_live_and_replay(handler, history, vec![external_raised_msg("s", "b")], expected);
}

struct DeadlineVotesHandler;

#[async_trait]
impl OrchestrationHandler for DeadlineVotesHandler {
    // The explicit match keeps the durable deadline-loser branch visible.
    #[allow(clippy::while_let_loop)]
    async fn invoke(&self, ctx: OrchestrationContext, _: String) -> Result<String, String> {
        let mut deadline = ctx.schedule_timer(Duration::from_secs(30));
        let mut votes = Vec::new();
        loop {
            match ctx.select2(ctx.schedule_wait("s"), &mut deadline).await {
                Either2::First(value) => votes.push(value),
                Either2::Second(()) => break,
            }
        }
        let ack = ctx.schedule_wait("s").await;
        Ok(format!("{}|{ack}", votes.join(",")))
    }
}

#[test]
fn ms17b_dropped_unbound_holder_does_not_refuse_fresh_signal() {
    let handler: Arc<dyn OrchestrationHandler> = Arc::new(DeadlineVotesHandler);
    let history = scheduling_history(handler.clone());
    let timer = timer_completion(&history);
    assert_early_batch_then_fresh(
        handler,
        history,
        vec![
            external_raised_msg("s", "x"),
            external_raised_msg("s", "stale"),
            timer,
            external_raised_msg("s", "fresh"),
        ],
        "x|b",
    );
}

#[test]
fn ms17b_deadline_drop_before_arrival_preserves_fresh_ack() {
    let handler: Arc<dyn OrchestrationHandler> = Arc::new(DeadlineVotesHandler);
    let history = scheduling_history(handler.clone());
    let timer = timer_completion(&history);
    assert_early_batch_then_fresh(
        handler,
        history,
        vec![external_raised_msg("s", "x"), timer, external_raised_msg("s", "fresh")],
        "x|b",
    );
}

struct LazyWaitsHandler {
    queue_second: bool,
}

#[async_trait]
impl OrchestrationHandler for LazyWaitsHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _: String) -> Result<String, String> {
        let validate = ctx.schedule_activity("validate", "");
        let prepare = ctx.schedule_activity("prepare", "");
        let result = ctx
            .select2(
                async {
                    validate.await?;
                    Ok::<_, String>(if self.queue_second {
                        ctx.dequeue_event("approve").await
                    } else {
                        ctx.schedule_wait("approve").await
                    })
                },
                async {
                    prepare.await?;
                    Ok::<_, String>(if self.queue_second {
                        ctx.dequeue_event("reject").await
                    } else {
                        ctx.schedule_wait("reject").await
                    })
                },
            )
            .await;
        let output = format!("{result:?}");
        ctx.schedule_activity("record", &output).await?;
        Ok(output)
    }
}

fn check_lazy_bind_order(queue_second: bool) {
    let handler: Arc<dyn OrchestrationHandler> = Arc::new(LazyWaitsHandler { queue_second });
    let mut history = scheduling_history(handler.clone());
    let schedule_id = |name: &str| {
        history
            .iter()
            .find(|event| matches!(&event.kind, EventKind::ActivityScheduled { name: n, .. } if n == name))
            .expect("handler must schedule both activities")
            .event_id
    };
    let reject = if queue_second {
        WorkItem::QueueMessage {
            instance: TEST_INSTANCE.into(),
            name: "reject".into(),
            data: "R".into(),
        }
    } else {
        external_raised_msg("reject", "R")
    };
    let mut live = create_engine(history.clone());
    live.prep_completions(vec![
        activity_completed_msg(schedule_id("prepare"), "ok"),
        reject,
        activity_completed_msg(schedule_id("validate"), "ok"),
        if queue_second {
            WorkItem::QueueMessage {
                instance: TEST_INSTANCE.into(),
                name: "approve".into(),
                data: "A".into(),
            }
        } else {
            external_raised_msg("approve", "A")
        },
    ]);
    assert_continue(&execute(&mut live, handler.clone()));
    if !queue_second {
        assert!(
            !live.history_delta().iter().any(|event| matches!(&event.kind,
            EventKind::ActivityScheduled { name, .. } if name == "record")),
            "both same-batch signals precede binding and must be dropped as early"
        );
        history.extend_from_slice(live.history_delta());
        let mut replay = create_engine(history.clone());
        assert_continue(&execute(&mut replay, handler.clone()));
        assert!(replay.history_delta().is_empty());
        let mut next = create_engine(history.clone());
        next.prep_completions(vec![external_raised_msg("approve", "fresh-A")]);
        assert_continue(&execute(&mut next, handler.clone()));
        let record_id = next
            .history_delta()
            .iter()
            .find(|event| {
                matches!(&event.kind, EventKind::ActivityScheduled { name, input, .. }
                if name == "record" && input == "First(Ok(\"fresh-A\"))")
            })
            .expect("fresh bound signal must choose the approve branch")
            .event_id;
        history.extend_from_slice(next.history_delta());
        assert_live_and_replay(
            handler,
            history,
            vec![activity_completed_msg(record_id, "ok")],
            "First(Ok(\"fresh-A\"))",
        );
        return;
    }
    let record = live
        .history_delta()
        .iter()
        .find(|event| matches!(&event.kind, EventKind::ActivityScheduled { name, .. } if name == "record"))
        .expect("live turn must record its selected winner");
    assert!(
        matches!(&record.kind, EventKind::ActivityScheduled { input, .. } if input == "First(Ok(\"A\"))"),
        "live selection must respect the handler's poll order: {record:?}"
    );
    history.extend_from_slice(live.history_delta());
    let mut replay = create_engine(history.clone());
    let result = execute(&mut replay, handler);
    assert!(
        matches!(result, duroxide::runtime::replay_engine::TurnResult::Failed(
        duroxide::ErrorDetails::Configuration {
            kind: duroxide::ConfigErrorKind::Nondeterminism, ref message, ..
        }) if message.as_deref().is_some_and(|text| text.contains("schedule mismatch")
            && text.contains("Second(Ok") && text.contains("First(Ok"))),
        "D2/MS18 queue variant must retain its known creation-versus-poll ordering defect: {result:?}"
    );
}

#[test]
fn ms18_lazy_positional_bind_order_preserves_live_winner_on_replay() {
    check_lazy_bind_order(false);
}

#[test]
fn ms18_lazy_queue_variant_pins_known_d2_replay_ordering_defect() {
    check_lazy_bind_order(true);
}

/// MS1 plus a wait dropped before it is bound. Neither policy cancels a dropped wait
/// when its subscription row binds. Doing so for a 0.1.30 execution changes its
/// recorded outcome: the stale "a" reaches the checkpoint and the next replay fails.
struct Ms1UnboundDropHandler;

#[async_trait]
impl OrchestrationHandler for Ms1UnboundDropHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _: String) -> Result<String, String> {
        if let Either2::Second(()) = ctx
            .select2(ctx.schedule_wait("s"), ctx.schedule_timer(Duration::from_millis(10)))
            .await
        {
            ctx.schedule_activity("tick", "").await?;
        }
        drop(ctx.schedule_wait("s"));
        let value = ctx.schedule_wait("s").await;
        ctx.schedule_activity("checkpoint", &value).await?;
        Ok(value)
    }
}

#[test]
fn ms1_wait_dropped_before_bind_keeps_each_policy_outcome() {
    for (stamp, expected) in [("0.1.30", vec![]), ("0.1.31", vec!["b".to_string()])] {
        let handler: Arc<dyn OrchestrationHandler> = Arc::new(Ms1UnboundDropHandler);
        let mut start = started_event(1);
        start.duroxide_version = stamp.into();
        let mut history = vec![start];
        let mut checkpoints = Vec::new();
        let mut turn = |history: &mut Vec<Event>, messages: Vec<WorkItem>| {
            let mut live = create_engine(history.clone());
            live.prep_completions(messages);
            assert_continue(&execute(&mut live, handler.clone()));
            checkpoints.extend(live.history_delta().iter().filter_map(|event| match &event.kind {
                EventKind::ActivityScheduled { name, input, .. } if name == "checkpoint" => Some(input.clone()),
                _ => None,
            }));
            history.extend_from_slice(live.history_delta());
            let mut replay = create_engine(history.clone());
            assert_continue(&execute(&mut replay, handler.clone()));
            assert!(replay.history_delta().is_empty(), "{stamp}: replay must not add rows");
        };
        turn(&mut history, vec![]);
        let timer = timer_completion(&history);
        turn(&mut history, vec![timer, external_raised_msg("s", "a")]);
        let tick = history
            .iter()
            .find(|event| matches!(&event.kind, EventKind::ActivityScheduled { name, .. } if name == "tick"))
            .expect("the timer must win and schedule tick")
            .event_id;
        turn(&mut history, vec![activity_completed_msg(tick, "ok")]);
        turn(&mut history, vec![external_raised_msg("s", "b")]);
        assert_eq!(checkpoints, expected, "{stamp}");
    }
}

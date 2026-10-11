// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use super::helpers::*;
use async_trait::async_trait;
use duroxide::providers::WorkItem;
use duroxide::{Either2, Event, EventKind, OrchestrationContext, OrchestrationHandler};
use std::sync::Arc;
use std::time::Duration;

struct QueueRaceHandler {
    drain: bool,
    lazy: bool,
}

#[async_trait]
impl OrchestrationHandler for QueueRaceHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _: String) -> Result<String, String> {
        let mut messages = Vec::new();
        let mut timers = 0;
        while messages.len() < 2 {
            let result = if self.lazy {
                // The Node bridge creates the durable children when their wrappers are polled.
                ctx.select2(async { ctx.dequeue_event("q").await }, async {
                    ctx.schedule_timer(Duration::from_millis(20)).await
                })
                .await
            } else {
                ctx.select2(ctx.dequeue_event("q"), ctx.schedule_timer(Duration::from_millis(20)))
                    .await
            };
            match result {
                Either2::First(message) => messages.push(message),
                Either2::Second(()) => {
                    timers += 1;
                    if self.drain {
                        // A production drain exits on timeout; the next phase blocks on a fresh dequeue.
                        messages.push(ctx.dequeue_event("q").await);
                    }
                }
            }
        }
        Ok(format!("{timers}:{}", messages.join(",")))
    }
}

fn queue_message(data: &str) -> WorkItem {
    WorkItem::QueueMessage {
        instance: TEST_INSTANCE.to_string(),
        name: "q".to_string(),
        data: data.to_string(),
    }
}

#[test]
fn queue_timer_race_replays_timer_then_message_same_batch() {
    for drain in [false, true] {
        for lazy in [false, true] {
            let handler = Arc::new(QueueRaceHandler { drain, lazy });
            let mut history = vec![started_event(1)];
            history[0].duroxide_version = "0.1.31".into();
            let mut first = create_engine(history.clone());
            assert_continue(&execute(&mut first, handler.clone()));
            history.extend_from_slice(first.history_delta());

            let timer = history
                .iter()
                .find(|event| matches!(event.kind, EventKind::TimerCreated { .. }))
                .unwrap();
            let timer_id = timer.event_id;
            let EventKind::TimerCreated { fire_at_ms } = timer.kind else {
                unreachable!();
            };

            let mut second = create_engine(history.clone());
            second.prep_completions(vec![timer_fired_msg(timer_id, fire_at_ms), queue_message("first")]);
            assert_continue(&execute(&mut second, handler.clone()));
            assert!(second.history_delta().iter().any(|event| {
                matches!(&event.kind, EventKind::QueueSubscriptionCancelled { reason } if reason == "dropped_future")
            }));
            history.extend_from_slice(second.history_delta());

            // Replaying the persisted turn must consume the same arrival before scheduling the next race.
            let mut replay = create_engine(history.clone());
            assert_continue(&execute(&mut replay, handler.clone()));
            assert!(replay.history_delta().is_empty());

            let mut third = create_engine(history);
            third.prep_completions(vec![queue_message("second")]);
            assert_completed(&execute(&mut third, handler), "1:first,second");
        }
    }
}

struct DroppedUnboundQueueHandler;

#[async_trait]
impl OrchestrationHandler for DroppedUnboundQueueHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _: String) -> Result<String, String> {
        drop(ctx.dequeue_event("q"));
        let first = ctx.dequeue_event("q").await;
        let second = ctx.dequeue_event("q").await;
        Ok(format!("{first},{second}"))
    }
}

#[test]
fn dropped_unbound_queue_does_not_reserve_an_arrival() {
    let mut history = vec![
        started_event(1),
        Event::with_event_id(
            2,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::QueueEventDelivered {
                name: "q".into(),
                data: "first".into(),
            },
        ),
        Event::with_event_id(
            3,
            TEST_INSTANCE,
            TEST_EXECUTION_ID,
            None,
            EventKind::QueueEventDelivered {
                name: "q".into(),
                data: "second".into(),
            },
        ),
    ];
    history[0].duroxide_version = "0.1.31".into();
    let mut engine = create_engine(history.clone());
    assert_completed(
        &execute(&mut engine, Arc::new(DroppedUnboundQueueHandler)),
        "first,second",
    );
    let mut persisted = history;
    persisted.extend_from_slice(engine.history_delta());
    let mut replay = create_engine(persisted);
    assert_completed(
        &execute(&mut replay, Arc::new(DroppedUnboundQueueHandler)),
        "first,second",
    );
    assert!(replay.history_delta().is_empty());
}

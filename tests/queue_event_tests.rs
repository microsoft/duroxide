#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::Client;
use duroxide::Either2;
use duroxide::EventKind;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self};
use duroxide::{ActivityContext, OrchestrationContext, OrchestrationRegistry};
mod common;
use std::time::Duration;

/// Basic persistent event delivery: raise then subscribe.
#[tokio::test]
async fn persistent_event_basic_delivery() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let data = ctx.dequeue_event("Signal").await;
        Ok(format!("got:{data}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-pers-basic", "TestOrch", "")
        .await
        .unwrap();

    // Wait for subscription to be registered
    assert!(
        common::wait_for_subscription(store.clone(), "inst-pers-basic", "Signal", 3000).await,
        "Subscription was never registered"
    );

    // Raise persistent event
    client
        .enqueue_event("inst-pers-basic", "Signal", "hello")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-pers-basic", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "got:hello"),
        "Expected got:hello, got {:?}",
        status
    );

    rt.shutdown(None).await;
}

/// Persistent event arrives BEFORE subscription — should still resolve.
#[tokio::test]
async fn persistent_event_arrives_before_subscription() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // Delay so the event arrives before we subscribe
        ctx.schedule_timer(Duration::from_millis(200)).await;
        let data = ctx.dequeue_event("Signal").await;
        Ok(format!("got:{data}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-pers-before", "TestOrch", "")
        .await
        .unwrap();

    // Raise event immediately — before timer fires and subscription is created
    client
        .enqueue_event("inst-pers-before", "Signal", "early_bird")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-pers-before", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "got:early_bird"),
        "Persistent event should resolve even when raised before subscription, got {:?}",
        status
    );

    rt.shutdown(None).await;
}

/// Persistent events use FIFO ordering: two raises + two waits match in order.
#[tokio::test]
async fn persistent_event_fifo_ordering() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let d1 = ctx.dequeue_event("Data").await;
        let d2 = ctx.dequeue_event("Data").await;
        Ok(format!("{d1},{d2}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-pers-fifo", "TestOrch", "")
        .await
        .unwrap();

    // Wait for first subscription
    assert!(
        common::wait_for_subscription(store.clone(), "inst-pers-fifo", "Data", 3000).await,
        "First subscription was never registered"
    );

    // Raise first event — first subscription resolves
    client.enqueue_event("inst-pers-fifo", "Data", "first").await.unwrap();

    // Wait for second subscription
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Raise second event
    client.enqueue_event("inst-pers-fifo", "Data", "second").await.unwrap();

    let status = client
        .wait_for_orchestration("inst-pers-fifo", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "first,second"),
        "FIFO ordering broken for persistent events, got {:?}",
        status
    );

    rt.shutdown(None).await;
}

/// Persistent event survives select cancellation — cancelled subscription doesn't consume the event.
/// A new persistent wait for the same name should still receive it.
///
/// Case 1: Both events arrive in the SAME orchestration turn (before terminal).
/// The unmatched "extra" event is materialized into history as QueueEventDelivered
/// but consumed by no subscription — it sits as an audit trail.
///
/// To guarantee both events land in the same batch, we stop the dispatcher after
/// phase 1 completes (2 subscriptions visible), enqueue both events while no
/// dispatcher is running, then restart the dispatcher to process them together.
#[tokio::test]
async fn persistent_event_survives_select_cancellation_same_batch() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let make_orch = || {
        |ctx: OrchestrationContext, _: String| async move {
            // Phase 1: persistent wait loses to timer → cancelled
            let wait1 = ctx.dequeue_event("Signal");
            let timeout1 = ctx.schedule_timer(Duration::from_millis(50));
            match ctx.select2(wait1, timeout1).await {
                Either2::First(_) => return Ok("unexpected_phase1".to_string()),
                Either2::Second(_) => {} // timer won
            }

            // Phase 2: new persistent wait for same name — should still resolve
            let data = ctx.dequeue_event("Signal").await;
            Ok(format!("got:{data}"))
        }
    };

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    // Phase A: Start runtime, let orchestration progress through phase 1,
    // then shut down so we can enqueue events without a racing dispatcher.
    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder()
            .register("TestOrch", make_orch())
            .build(),
        options.clone(),
    )
    .await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-pers-cancel-same", "TestOrch", "")
        .await
        .unwrap();

    // Wait for BOTH QueueSubscribed events (sub1 cancelled + sub2 active)
    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-pers-cancel-same",
            |hist| {
                hist.iter()
                    .filter(|e| matches!(&e.kind, EventKind::QueueSubscribed { name } if name == "Signal"))
                    .count()
                    >= 2
            },
            3000,
        )
        .await,
        "Second QueueSubscribed was never registered"
    );

    // Stop the dispatcher — no more polling
    rt.shutdown(None).await;

    // Phase B: Enqueue both events while dispatcher is stopped.
    // They sit in the orchestrator queue until we restart.
    client
        .enqueue_event("inst-pers-cancel-same", "Signal", "survived")
        .await
        .unwrap();
    client
        .enqueue_event("inst-pers-cancel-same", "Signal", "extra")
        .await
        .unwrap();

    // Phase C: Restart runtime — dispatcher picks up both events in one batch
    let rt2 = runtime::Runtime::start_with_options(
        store.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder()
            .register("TestOrch", make_orch())
            .build(),
        options,
    )
    .await;

    let status = client
        .wait_for_orchestration("inst-pers-cancel-same", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "got:survived"),
        "Persistent event should survive cancelled subscription, got {:?}",
        status
    );

    // Verify history: both events materialized, sub structure correct
    let history = store.read("inst-pers-cancel-same").await.unwrap();

    let has_cancel = history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::QueueSubscriptionCancelled { reason } if reason == "dropped_future"));
    assert!(
        has_cancel,
        "Missing QueueSubscriptionCancelled(dropped_future) for first wait"
    );

    // Both events MUST be in history — they were enqueued while the dispatcher was
    // stopped, so they're guaranteed to be batched in the same completing turn.
    let arrival_count = history
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::QueueEventDelivered { name, .. } if name == "Signal"))
        .count();
    assert_eq!(
        arrival_count, 2,
        "Both persistent events should be in history (same batch, dispatcher was stopped during enqueue)"
    );

    let active_sub_count = history
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::QueueSubscribed { name } if name == "Signal"))
        .count();
    let cancelled_sub_count = history
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::QueueSubscriptionCancelled { .. }))
        .count();
    assert_eq!(active_sub_count, 2, "Expected 2 total persistent subscriptions");
    assert_eq!(cancelled_sub_count, 1, "Expected 1 cancelled persistent subscription");

    rt2.shutdown(None).await;
}

/// Persistent event survives select cancellation — late-arriving unmatched event.
///
/// Case 2: The unmatched "extra" event arrives AFTER the orchestration is terminal.
/// The dispatcher's terminal bail path acks and discards it without recording
/// it in history, so only 1 QueueEventDelivered appears.
#[tokio::test]
async fn persistent_event_survives_select_cancellation_late_extra_discarded() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // Phase 1: persistent wait loses to timer → cancelled
        let wait1 = ctx.dequeue_event("Signal");
        let timeout1 = ctx.schedule_timer(Duration::from_millis(50));
        match ctx.select2(wait1, timeout1).await {
            Either2::First(_) => return Ok("unexpected_phase1".to_string()),
            Either2::Second(_) => {} // timer won
        }

        // Phase 2: new persistent wait for same name — should still resolve
        let data = ctx.dequeue_event("Signal").await;
        Ok(format!("got:{data}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-pers-cancel-late", "TestOrch", "")
        .await
        .unwrap();

    // Wait for both subscriptions to be registered
    assert!(
        common::wait_for_history(
            store.clone(),
            "inst-pers-cancel-late",
            |hist| {
                hist.iter()
                    .filter(|e| matches!(&e.kind, EventKind::QueueSubscribed { name } if name == "Signal"))
                    .count()
                    >= 2
            },
            3000,
        )
        .await,
        "Second QueueSubscribed was never registered"
    );

    // Send ONLY "survived" — this completes the orchestration
    client
        .enqueue_event("inst-pers-cancel-late", "Signal", "survived")
        .await
        .unwrap();

    // Wait for orchestration to complete BEFORE sending extra
    let status = client
        .wait_for_orchestration("inst-pers-cancel-late", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "got:survived"),
        "Persistent event should survive cancelled subscription, got {:?}",
        status
    );

    // NOW send "extra" — orchestration is already terminal
    client
        .enqueue_event("inst-pers-cancel-late", "Signal", "extra")
        .await
        .unwrap();

    // Give the dispatcher time to pick up and discard the late message
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify history: only 1 event materialized — "extra" was discarded by terminal bail path
    let history = store.read("inst-pers-cancel-late").await.unwrap();

    let has_cancel = history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::QueueSubscriptionCancelled { reason } if reason == "dropped_future"));
    assert!(
        has_cancel,
        "Missing QueueSubscriptionCancelled(dropped_future) for first wait"
    );

    let arrival_count = history
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::QueueEventDelivered { name, .. } if name == "Signal"))
        .count();
    assert_eq!(
        arrival_count, 1,
        "Only 'survived' should be in history — 'extra' arrived after terminal and was discarded"
    );

    let active_sub_count = history
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::QueueSubscribed { name } if name == "Signal"))
        .count();
    let cancelled_sub_count = history
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::QueueSubscriptionCancelled { .. }))
        .count();
    assert_eq!(active_sub_count, 2, "Expected 2 total persistent subscriptions");
    assert_eq!(cancelled_sub_count, 1, "Expected 1 cancelled persistent subscription");

    rt.shutdown(None).await;
}

/// Persistent and positional events are independent — no cross-contamination.
#[tokio::test]
async fn persistent_and_positional_are_independent() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // Subscribe to both types with the same event name
        let positional = ctx.schedule_wait("Signal");
        let persistent = ctx.dequeue_event("Signal");

        // Wait for both
        let (pos_data, pers_data) = ctx.join2(positional, persistent).await;
        Ok(format!("pos:{pos_data},pers:{pers_data}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-pers-indep", "TestOrch", "")
        .await
        .unwrap();

    // Wait for subscriptions to register
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Raise both types — each should only resolve its own subscription
    client
        .raise_event("inst-pers-indep", "Signal", "positional_data")
        .await
        .unwrap();
    client
        .enqueue_event("inst-pers-indep", "Signal", "persistent_data")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-pers-indep", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "pos:positional_data,pers:persistent_data"),
        "Persistent and positional should be independent, got {:?}",
        status
    );

    rt.shutdown(None).await;
}

// ─── Persistent event cancellation tests ──────────────────────────────────────

/// Verify that a persistent wait losing a select2 emits an
/// QueueSubscriptionCancelled breadcrumb with reason "dropped_future".
#[tokio::test]
async fn persistent_cancelled_subscription_emits_breadcrumb() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let wait = ctx.dequeue_event("Signal");
        let timeout = ctx.schedule_timer(Duration::from_millis(50));
        match ctx.select2(wait, timeout).await {
            Either2::First(_) => Ok("unexpected".to_string()),
            Either2::Second(_) => Ok("timer_won".to_string()),
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-pers-breadcrumb", "TestOrch", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-pers-breadcrumb", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "timer_won"),
        "Expected timer_won, got {:?}",
        status
    );

    let history = store.read("inst-pers-breadcrumb").await.unwrap();
    let has_cancel = history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::QueueSubscriptionCancelled { reason } if reason == "dropped_future"));
    assert!(
        has_cancel,
        "Missing QueueSubscriptionCancelled(dropped_future) breadcrumb"
    );

    rt.shutdown(None).await;
}

/// When an orchestration completes with an outstanding persistent subscription,
/// it should emit an QueueSubscriptionCancelled breadcrumb with reason
/// "orchestration_terminal".
#[tokio::test]
async fn persistent_outstanding_subscription_cancelled_on_completion() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // Schedule a persistent wait but never await it — use a block scoping trick
        let _unused = ctx.dequeue_event("NeverResolved");
        // Complete immediately by dropping the future
        ctx.schedule_timer(Duration::from_millis(50)).await;
        Ok("done_early".to_string())
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-pers-terminal", "TestOrch", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-pers-terminal", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "done_early"),
        "Expected done_early, got {:?}",
        status
    );

    let history = store.read("inst-pers-terminal").await.unwrap();

    // Should have both the subscription and its cancellation
    let has_sub = history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::QueueSubscribed { name } if name == "NeverResolved"));
    assert!(has_sub, "Missing QueueSubscribed event");

    let has_cancel = history.iter().any(|e| {
        matches!(&e.kind, EventKind::QueueSubscriptionCancelled { reason }
            if reason == "dropped_future" || reason == "orchestration_terminal")
    });
    assert!(
        has_cancel,
        "Missing QueueSubscriptionCancelled breadcrumb for outstanding subscription"
    );

    rt.shutdown(None).await;
}

/// Multiple cancelled persistent subscriptions: cancel two, keep one active.
/// Only the active subscription should receive the event.
#[tokio::test]
async fn persistent_multiple_cancelled_subscriptions_only_active_receives() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // Cancel first persistent wait via select2
        let wait1 = ctx.dequeue_event("Signal");
        let timeout1 = ctx.schedule_timer(Duration::from_millis(30));
        match ctx.select2(wait1, timeout1).await {
            Either2::First(_) => return Ok("unexpected1".to_string()),
            Either2::Second(_) => {}
        }

        // Cancel second persistent wait via select2
        let wait2 = ctx.dequeue_event("Signal");
        let timeout2 = ctx.schedule_timer(Duration::from_millis(30));
        match ctx.select2(wait2, timeout2).await {
            Either2::First(_) => return Ok("unexpected2".to_string()),
            Either2::Second(_) => {}
        }

        // Third persistent wait — this one should get the event
        let data = ctx.dequeue_event("Signal").await;
        Ok(format!("got:{data}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-pers-multi-cancel", "TestOrch", "")
        .await
        .unwrap();

    // Wait for both cancellations to settle
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Raise one event — should skip two cancelled subs and reach the third
    client
        .enqueue_event("inst-pers-multi-cancel", "Signal", "winner")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-pers-multi-cancel", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == "got:winner"),
        "Expected got:winner, got {:?}",
        status
    );

    // Verify two cancelled breadcrumbs exist
    let history = store.read("inst-pers-multi-cancel").await.unwrap();
    let cancel_count = history
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::QueueSubscriptionCancelled { reason } if reason == "dropped_future"))
        .count();
    assert_eq!(
        cancel_count, 2,
        "Expected 2 dropped_future cancellation breadcrumbs, got {cancel_count}"
    );

    rt.shutdown(None).await;
}

/// Complex multi-turn test: interleave persistent waits, positional waits, and activities
/// across multiple orchestrator turns. Tests edge conditions:
///   Turn 1: positional wait (first turn boundary)
///   Turn 2: activity → persistent wait
///   Turn 3: activity → positional wait (cancelled via select) → persistent wait
///   Turn 4: activity → persistent wait (last turn boundary)
#[tokio::test]
async fn complex_multi_turn_persistent_and_positional_with_activities() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let echo_activity = |_ctx: ActivityContext, input: String| async move { Ok(format!("echo:{input}")) };

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let mut results = Vec::new();

        // ── Turn 1: positional wait right at the start (first-turn edge) ──
        let pos1 = ctx.schedule_wait("PosFirst").await;
        results.push(format!("pos1:{pos1}"));

        // ── Turn 2: activity forces a new turn, then persistent wait ──
        let act1 = ctx.schedule_activity("Echo", "step2").await?;
        results.push(act1);

        let pers1 = ctx.dequeue_event("PersA").await;
        results.push(format!("pers1:{pers1}"));

        // ── Turn 3: activity, then positional wait cancelled by timer, then persistent wait ──
        let act2 = ctx.schedule_activity("Echo", "step3").await?;
        results.push(act2);

        // Positional wait that loses to timer (cancellation mid-flow)
        let pos_doomed = ctx.schedule_wait("PosDoomed");
        let timer = ctx.schedule_timer(Duration::from_millis(50));
        match ctx.select2(pos_doomed, timer).await {
            Either2::First(d) => results.push(format!("pos_doomed_unexpected:{d}")),
            Either2::Second(_) => results.push("pos_cancelled".to_string()),
        }

        // Persistent wait after the cancellation
        let pers2 = ctx.dequeue_event("PersB").await;
        results.push(format!("pers2:{pers2}"));

        // ── Turn 4: activity, then persistent wait at the very end (last-turn edge) ──
        let act3 = ctx.schedule_activity("Echo", "step4").await?;
        results.push(act3);

        let pers3 = ctx.dequeue_event("PersC").await;
        results.push(format!("pers3:{pers3}"));

        Ok(results.join(","))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("ComplexOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().register("Echo", echo_activity).build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-complex-multi", "ComplexOrch", "")
        .await
        .unwrap();

    // Turn 1: positional wait is the very first thing — raise it to unblock
    assert!(
        common::wait_for_subscription(store.clone(), "inst-complex-multi", "PosFirst", 3000).await,
        "PosFirst subscription never registered"
    );
    client
        .raise_event("inst-complex-multi", "PosFirst", "alpha")
        .await
        .unwrap();

    // Turn 2: wait for persistent subscription PersA, then raise
    assert!(
        common::wait_for_subscription(store.clone(), "inst-complex-multi", "PersA", 3000).await,
        "PersA subscription never registered"
    );
    client
        .enqueue_event("inst-complex-multi", "PersA", "beta")
        .await
        .unwrap();

    // Turn 3: the positional PosDoomed will cancel (timer wins), then PersB
    assert!(
        common::wait_for_subscription(store.clone(), "inst-complex-multi", "PersB", 3000).await,
        "PersB subscription never registered"
    );
    client
        .enqueue_event("inst-complex-multi", "PersB", "gamma")
        .await
        .unwrap();

    // Turn 4: persistent wait PersC at the very end
    assert!(
        common::wait_for_subscription(store.clone(), "inst-complex-multi", "PersC", 3000).await,
        "PersC subscription never registered"
    );
    client
        .enqueue_event("inst-complex-multi", "PersC", "delta")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-complex-multi", Duration::from_secs(10))
        .await
        .unwrap();
    let expected = "pos1:alpha,echo:step2,pers1:beta,echo:step3,pos_cancelled,pers2:gamma,echo:step4,pers3:delta";
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == expected),
        "Expected:\n  {expected}\nGot:\n  {:?}",
        status
    );

    // Verify history has the cancelled positional subscription breadcrumb
    let history = store.read("inst-complex-multi").await.unwrap();
    let has_pos_cancel = history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalSubscribedCancelled { reason } if reason == "dropped_future"));
    assert!(
        has_pos_cancel,
        "Missing positional ExternalSubscribedCancelled(dropped_future) breadcrumb"
    );

    // No persistent cancellations should exist (all 3 persistent waits resolved normally)
    let pers_cancel_count = history
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::QueueSubscriptionCancelled { .. }))
        .count();
    assert_eq!(
        pers_cancel_count, 0,
        "No persistent subscriptions should be cancelled, got {pers_cancel_count}"
    );

    rt.shutdown(None).await;
}

/// Select2 between persistent and positional, then join the remaining with another.
///
/// Phase 1 (select2): race a positional wait against a persistent wait.
///   - Persistent event arrives first → positional gets cancelled.
/// Phase 2 (join2): join a new positional wait with a new persistent wait.
///   - Both resolve concurrently.
///
/// Verifies: select cancellation breadcrumbs, join resolution, no cross-contamination.
#[tokio::test]
async fn select_then_join_persistent_and_positional() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let mut results = Vec::new();

        // ── Phase 1: select2(positional, persistent) ──
        // Persistent should win because we'll raise it first.
        let pos_race = ctx.schedule_wait("RacePos");
        let pers_race = ctx.dequeue_event("RacePers");
        match ctx.select2(pos_race, pers_race).await {
            Either2::First(d) => results.push(format!("select_pos:{d}")),
            Either2::Second(d) => results.push(format!("select_pers:{d}")),
        }

        // ── Phase 2: join2(positional, persistent) ──
        // Both must resolve — no cancellation.
        let pos_join = ctx.schedule_wait("JoinPos");
        let pers_join = ctx.dequeue_event("JoinPers");
        let (pos_data, pers_data) = ctx.join2(pos_join, pers_join).await;
        results.push(format!("join_pos:{pos_data}"));
        results.push(format!("join_pers:{pers_data}"));

        Ok(results.join(","))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TestOrch", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-sel-join", "TestOrch", "")
        .await
        .unwrap();

    // Phase 1: wait for both subscriptions, then raise only the persistent one
    assert!(
        common::wait_for_subscription(store.clone(), "inst-sel-join", "RacePers", 3000).await,
        "RacePers subscription never registered"
    );
    // Only raise persistent → it wins the select, positional gets cancelled
    client.enqueue_event("inst-sel-join", "RacePers", "fast").await.unwrap();

    // Phase 2: wait for join subscriptions, then raise both
    assert!(
        common::wait_for_subscription(store.clone(), "inst-sel-join", "JoinPos", 3000).await,
        "JoinPos subscription never registered"
    );
    // Raise both — order shouldn't matter since join awaits both
    client.raise_event("inst-sel-join", "JoinPos", "left").await.unwrap();
    client
        .enqueue_event("inst-sel-join", "JoinPers", "right")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-sel-join", Duration::from_secs(10))
        .await
        .unwrap();
    let expected = "select_pers:fast,join_pos:left,join_pers:right";
    assert!(
        matches!(status, runtime::OrchestrationStatus::Completed { ref output, .. } if output == expected),
        "Expected:\n  {expected}\nGot:\n  {:?}",
        status
    );

    // Verify positional RacePos was cancelled (lost the select2)
    let history = store.read("inst-sel-join").await.unwrap();
    let pos_cancel = history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalSubscribedCancelled { reason } if reason == "dropped_future"));
    assert!(
        pos_cancel,
        "Positional RacePos should have a dropped_future cancellation breadcrumb"
    );

    // No persistent cancellations — all persistent waits resolved
    let pers_cancel = history
        .iter()
        .any(|e| matches!(&e.kind, EventKind::QueueSubscriptionCancelled { .. }));
    assert!(!pers_cancel, "No persistent subscriptions should be cancelled");

    rt.shutdown(None).await;
}

// =============================================================================
// Typed API round-trip tests
// =============================================================================

/// raise_event_typed + schedule_wait_typed round-trip with a struct.
#[tokio::test]
async fn raise_event_typed_round_trip() {
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct Payload {
        id: u32,
        msg: String,
    }

    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let p: Payload = ctx.schedule_wait_typed("Signal").await;
        Ok(format!("id={},msg={}", p.id, p.msg))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TypedWait", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-typed-wait", "TypedWait", "")
        .await
        .unwrap();

    assert!(
        common::wait_for_subscription(store.clone(), "inst-typed-wait", "Signal", 3000).await,
        "Subscription was never registered"
    );

    let payload = Payload {
        id: 42,
        msg: "hello".into(),
    };
    client
        .raise_event_typed("inst-typed-wait", "Signal", &payload)
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-typed-wait", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "id=42,msg=hello");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// enqueue_event_typed + dequeue_event_typed round-trip with a struct.
#[tokio::test]
async fn enqueue_event_typed_round_trip() {
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct Command {
        action: String,
        value: i64,
    }

    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        let cmd: Command = ctx.dequeue_event_typed("commands").await;
        Ok(format!("{}:{}", cmd.action, cmd.value))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("TypedQueue", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activity_registry, orchestration_registry).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration("inst-typed-queue", "TypedQueue", "")
        .await
        .unwrap();

    assert!(
        common::wait_for_subscription(store.clone(), "inst-typed-queue", "commands", 3000).await,
        "Subscription was never registered"
    );

    let cmd = Command {
        action: "increment".into(),
        value: 99,
    };
    client
        .enqueue_event_typed("inst-typed-queue", "commands", &cmd)
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-typed-queue", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "increment:99");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Multiple independent queues are isolated: messages on queue "A" never leak
/// into queue "B", and FIFO ordering is maintained per-queue independently.
///
/// Orchestration subscribes to three queues in a specific order (orders, payments, notifications).
/// Messages are enqueued across queues in interleaved order. The test asserts:
///   1. Each dequeue_event only receives messages from its own queue name
///   2. FIFO ordering is preserved within each queue independently
///   3. Queues can be dequeued in any order relative to enqueue timing
#[tokio::test]
async fn multi_queue_isolation_and_independent_fifo() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // Dequeue from three independent queues
        let order1 = ctx.dequeue_event("orders").await;
        let order2 = ctx.dequeue_event("orders").await;
        let pay1 = ctx.dequeue_event("payments").await;
        let pay2 = ctx.dequeue_event("payments").await;
        let notif1 = ctx.dequeue_event("notifications").await;

        Ok(format!("{order1},{order2}|{pay1},{pay2}|{notif1}"))
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("MultiQueue", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    // Start orchestration FIRST — events enqueued before start are dropped as orphans.
    client
        .start_orchestration("inst-multi-q", "MultiQueue", "")
        .await
        .unwrap();

    // Enqueue messages across queues in interleaved order.
    // This tests that buffered messages are matched to the correct queue by name,
    // not by arrival order across all queues.
    client
        .enqueue_event("inst-multi-q", "payments", "pay_first")
        .await
        .unwrap();
    client
        .enqueue_event("inst-multi-q", "orders", "order_first")
        .await
        .unwrap();
    client
        .enqueue_event("inst-multi-q", "notifications", "alert_1")
        .await
        .unwrap();
    client
        .enqueue_event("inst-multi-q", "orders", "order_second")
        .await
        .unwrap();
    client
        .enqueue_event("inst-multi-q", "payments", "pay_second")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-multi-q", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // orders: order_first then order_second (FIFO within "orders")
            // payments: pay_first then pay_second (FIFO within "payments")
            // notifications: alert_1
            assert_eq!(output, "order_first,order_second|pay_first,pay_second|alert_1");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Multiple queues with staggered delivery: some messages arrive before the
/// orchestration subscribes, others arrive after. Verifies that per-queue FIFO
/// is maintained regardless of timing relative to subscription creation.
///
/// Note: `start_orchestration` must come before `enqueue_event` — events
/// enqueued before the orchestration exists are dropped as orphans.
#[tokio::test]
async fn multi_queue_staggered_delivery() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // First: dequeue from "commands" (will be pre-buffered)
        let cmd1 = ctx.dequeue_event("commands").await;

        // Force a new turn with an activity, then dequeue from "status" (will arrive later)
        let _ = ctx.schedule_activity("Noop", "x").await?;
        let stat1 = ctx.dequeue_event("status").await;

        // Dequeue another from each queue
        let cmd2 = ctx.dequeue_event("commands").await;
        let stat2 = ctx.dequeue_event("status").await;

        Ok(format!("cmd:{cmd1},{cmd2}|stat:{stat1},{stat2}"))
    };

    let noop_activity = |_ctx: ActivityContext, _input: String| async move { Ok("ok".to_string()) };

    let orchestration_registry = OrchestrationRegistry::builder().register("StaggeredQ", orch).build();
    let activity_registry = ActivityRegistry::builder().register("Noop", noop_activity).build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    // Start the orchestration FIRST, then buffer events.
    // Events enqueued before start_orchestration are dropped as orphans.
    client
        .start_orchestration("inst-staggered-q", "StaggeredQ", "")
        .await
        .unwrap();

    // Buffer events: two commands and one status arrive before orchestration subscribes
    client
        .enqueue_event("inst-staggered-q", "commands", "c1")
        .await
        .unwrap();
    client
        .enqueue_event("inst-staggered-q", "commands", "c2")
        .await
        .unwrap();
    client.enqueue_event("inst-staggered-q", "status", "s1").await.unwrap();

    // Wait for the orchestration to progress past the first dequeue + activity,
    // then send the second status message
    assert!(
        common::wait_for_subscription(store.clone(), "inst-staggered-q", "status", 3000).await,
        "Status subscription was never registered"
    );
    // Small delay to let the second "status" subscription register after replay
    tokio::time::sleep(Duration::from_millis(300)).await;
    client.enqueue_event("inst-staggered-q", "status", "s2").await.unwrap();

    let status = client
        .wait_for_orchestration("inst-staggered-q", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "cmd:c1,c2|stat:s1,s2");
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

/// Events enqueued before `start_orchestration` are dropped as orphans.
///
/// The provider detects QueueMessage items for a non-existent instance
/// and deletes them with a warning. The orchestration never sees them.
#[tokio::test]
async fn events_enqueued_before_start_orchestration_are_dropped() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _: String| async move {
        // Race dequeue against a short timer — if the pre-start event was
        // correctly dropped, the timer wins.
        let event = ctx.dequeue_event("pre-start-q");
        let timeout = ctx.schedule_timer(Duration::from_millis(500));

        match ctx.select2(event, timeout).await {
            Either2::First(data) => Ok(format!("unexpected:{data}")),
            Either2::Second(()) => {
                // Now enqueue a post-start event and verify it DOES arrive
                Ok("timeout_as_expected".to_string())
            }
        }
    };

    let orchestration_registry = OrchestrationRegistry::builder().register("OrphanTest", orch).build();
    let activity_registry = ActivityRegistry::builder().build();

    let options = runtime::RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;
    let client = Client::new(store.clone());

    // Enqueue event BEFORE the orchestration exists — should be dropped
    client
        .enqueue_event("inst-orphan", "pre-start-q", "should-be-dropped")
        .await
        .unwrap();

    // Small delay to let the provider poll and drop the orphan
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now start the orchestration
    client
        .start_orchestration("inst-orphan", "OrphanTest", "")
        .await
        .unwrap();

    let status = client
        .wait_for_orchestration("inst-orphan", Duration::from_secs(5))
        .await
        .unwrap();
    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(
                output, "timeout_as_expected",
                "Pre-start event should have been dropped; dequeue should have timed out"
            );
        }
        other => panic!("Expected Completed, got: {other:?}"),
    }

    rt.shutdown(None).await;
}

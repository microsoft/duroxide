//! Test helpers for replay engine tests
//!
//! Provides utilities for constructing test histories, mock handlers,
//! and asserting on TurnResult outcomes.

use async_trait::async_trait;
use duroxide::providers::WorkItem;
use duroxide::runtime::replay_engine::{ReplayEngine, TurnResult};
use duroxide::{ConfigErrorKind, ErrorDetails, Event, EventKind, OrchestrationContext, OrchestrationHandler};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

// ============================================================================
// Test Constants
// ============================================================================

pub const TEST_INSTANCE: &str = "test-instance";
pub const TEST_EXECUTION_ID: u64 = 1;
pub const TEST_ORCH_NAME: &str = "TestOrch";
pub const TEST_ORCH_VERSION: &str = "1.0.0";
pub const TEST_WORKER_ID: &str = "test-worker";

// ============================================================================
// Event Builders
// ============================================================================

/// Create an OrchestrationStarted event
pub fn started_event(event_id: u64) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: TEST_ORCH_NAME.to_string(),
            version: TEST_ORCH_VERSION.to_string(),
            input: "test-input".to_string(),
            parent_instance: None,
            parent_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    )
}

/// Create an ActivityScheduled event
pub fn activity_scheduled(event_id: u64, name: &str, input: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::ActivityScheduled {
            name: name.to_string(),
            input: input.to_string(),
            session_id: None,
            tag: None,
        },
    )
}

/// Create an ActivityCompleted event
pub fn activity_completed(event_id: u64, source_id: u64, result: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        Some(source_id),
        EventKind::ActivityCompleted {
            result: result.to_string(),
        },
    )
}

/// Create an ActivityFailed event
pub fn activity_failed(event_id: u64, source_id: u64, message: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        Some(source_id),
        EventKind::ActivityFailed {
            details: ErrorDetails::Application {
                kind: duroxide::AppErrorKind::ActivityFailed,
                message: message.to_string(),
                retryable: false,
            },
        },
    )
}

/// Create a TimerCreated event
pub fn timer_created(event_id: u64, fire_at_ms: u64) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::TimerCreated { fire_at_ms },
    )
}

/// Create a TimerFired event
pub fn timer_fired(event_id: u64, source_id: u64, fire_at_ms: u64) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        Some(source_id),
        EventKind::TimerFired { fire_at_ms },
    )
}

/// Create an ExternalSubscribed event
pub fn external_subscribed(event_id: u64, name: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::ExternalSubscribed { name: name.to_string() },
    )
}

/// Create an ExternalEvent event
pub fn external_event(event_id: u64, name: &str, data: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::ExternalEvent {
            name: name.to_string(),
            data: data.to_string(),
        },
    )
}

/// Create a SubOrchestrationScheduled event
pub fn sub_orch_scheduled(event_id: u64, name: &str, instance: &str, input: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::SubOrchestrationScheduled {
            name: name.to_string(),
            instance: instance.to_string(),
            input: input.to_string(),
        },
    )
}

/// Create a SubOrchestrationCompleted event
pub fn sub_orch_completed(event_id: u64, source_id: u64, result: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        Some(source_id),
        EventKind::SubOrchestrationCompleted {
            result: result.to_string(),
        },
    )
}

/// Create a SubOrchestrationFailed event
#[allow(dead_code)]
pub fn sub_orch_failed(event_id: u64, source_id: u64, message: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        Some(source_id),
        EventKind::SubOrchestrationFailed {
            details: ErrorDetails::Application {
                kind: duroxide::AppErrorKind::OrchestrationFailed,
                message: message.to_string(),
                retryable: false,
            },
        },
    )
}

/// Create an OrchestrationCompleted event
pub fn orchestration_completed(event_id: u64, output: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::OrchestrationCompleted {
            output: output.to_string(),
        },
    )
}

/// Create an OrchestrationFailed event
pub fn orchestration_failed(event_id: u64, message: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::OrchestrationFailed {
            details: ErrorDetails::Application {
                kind: duroxide::AppErrorKind::OrchestrationFailed,
                message: message.to_string(),
                retryable: false,
            },
        },
    )
}

/// Create an OrchestrationContinuedAsNew event
pub fn orchestration_continued_as_new(event_id: u64, input: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::OrchestrationContinuedAsNew {
            input: input.to_string(),
        },
    )
}

/// Create an OrchestrationCancelRequested event
pub fn cancel_requested(event_id: u64, reason: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::OrchestrationCancelRequested {
            reason: reason.to_string(),
        },
    )
}

/// Create an ActivityCancelRequested event
pub fn activity_cancel_requested(event_id: u64, source_id: u64, reason: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        Some(source_id),
        EventKind::ActivityCancelRequested {
            reason: reason.to_string(),
        },
    )
}

/// Create a SubOrchestrationCancelRequested event
pub fn sub_orch_cancel_requested(event_id: u64, source_id: u64, reason: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        Some(source_id),
        EventKind::SubOrchestrationCancelRequested {
            reason: reason.to_string(),
        },
    )
}

/// Create an OrchestrationChained event (fire-and-forget sub-orchestration start)
pub fn orchestration_chained(event_id: u64, name: &str, instance: &str, input: &str) -> Event {
    Event::with_event_id(
        event_id,
        TEST_INSTANCE,
        TEST_EXECUTION_ID,
        None,
        EventKind::OrchestrationChained {
            name: name.to_string(),
            instance: instance.to_string(),
            input: input.to_string(),
        },
    )
}

// ============================================================================
// WorkItem Builders (Completion Messages)
// ============================================================================

pub fn activity_completed_msg(id: u64, result: &str) -> WorkItem {
    WorkItem::ActivityCompleted {
        instance: TEST_INSTANCE.to_string(),
        execution_id: TEST_EXECUTION_ID,
        id,
        result: result.to_string(),
    }
}

pub fn activity_failed_msg(id: u64, message: &str) -> WorkItem {
    WorkItem::ActivityFailed {
        instance: TEST_INSTANCE.to_string(),
        execution_id: TEST_EXECUTION_ID,
        id,
        details: ErrorDetails::Application {
            kind: duroxide::AppErrorKind::ActivityFailed,
            message: message.to_string(),
            retryable: false,
        },
    }
}

pub fn activity_failed_infra_msg(id: u64, message: &str) -> WorkItem {
    WorkItem::ActivityFailed {
        instance: TEST_INSTANCE.to_string(),
        execution_id: TEST_EXECUTION_ID,
        id,
        details: ErrorDetails::Infrastructure {
            operation: "test".to_string(),
            message: message.to_string(),
            retryable: false,
        },
    }
}

pub fn activity_failed_config_msg(id: u64, message: &str) -> WorkItem {
    WorkItem::ActivityFailed {
        instance: TEST_INSTANCE.to_string(),
        execution_id: TEST_EXECUTION_ID,
        id,
        details: ErrorDetails::Configuration {
            kind: ConfigErrorKind::Nondeterminism,
            resource: "test".to_string(),
            message: Some(message.to_string()),
        },
    }
}

pub fn timer_fired_msg(id: u64, fire_at_ms: u64) -> WorkItem {
    WorkItem::TimerFired {
        instance: TEST_INSTANCE.to_string(),
        execution_id: TEST_EXECUTION_ID,
        id,
        fire_at_ms,
    }
}

pub fn external_raised_msg(name: &str, data: &str) -> WorkItem {
    WorkItem::ExternalRaised {
        instance: TEST_INSTANCE.to_string(),
        name: name.to_string(),
        data: data.to_string(),
    }
}

pub fn sub_orch_completed_msg(parent_id: u64, result: &str) -> WorkItem {
    WorkItem::SubOrchCompleted {
        parent_instance: TEST_INSTANCE.to_string(),
        parent_execution_id: TEST_EXECUTION_ID,
        parent_id,
        result: result.to_string(),
    }
}

pub fn sub_orch_failed_msg(parent_id: u64, message: &str) -> WorkItem {
    WorkItem::SubOrchFailed {
        parent_instance: TEST_INSTANCE.to_string(),
        parent_execution_id: TEST_EXECUTION_ID,
        parent_id,
        details: ErrorDetails::Application {
            kind: duroxide::AppErrorKind::OrchestrationFailed,
            message: message.to_string(),
            retryable: false,
        },
    }
}

pub fn cancel_instance_msg(reason: &str) -> WorkItem {
    WorkItem::CancelInstance {
        instance: TEST_INSTANCE.to_string(),
        reason: reason.to_string(),
    }
}

// ============================================================================
// Mock Handlers
// ============================================================================

/// A mock handler that returns a fixed result
pub struct ImmediateHandler {
    result: Result<String, String>,
}

impl ImmediateHandler {
    pub fn ok(result: &str) -> Arc<Self> {
        Arc::new(Self {
            result: Ok(result.to_string()),
        })
    }

    pub fn err(error: &str) -> Arc<Self> {
        Arc::new(Self {
            result: Err(error.to_string()),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for ImmediateHandler {
    async fn invoke(&self, _ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        self.result.clone()
    }
}

/// A mock handler that schedules one activity and awaits it
pub struct SingleActivityHandler {
    activity_name: String,
    activity_input: String,
}

impl SingleActivityHandler {
    pub fn new(name: &str, input: &str) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for SingleActivityHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let result = ctx.schedule_activity(&self.activity_name, &self.activity_input).await?;
        Ok(result)
    }
}

/// A mock handler that schedules a timer and awaits it
pub struct SingleTimerHandler {
    duration: Duration,
}

impl SingleTimerHandler {
    pub fn new(duration: Duration) -> Arc<Self> {
        Arc::new(Self { duration })
    }
}

#[async_trait]
impl OrchestrationHandler for SingleTimerHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.schedule_timer(self.duration).await;
        Ok("timer_done".to_string())
    }
}

/// A mock handler that waits for an external event
pub struct WaitExternalHandler {
    event_name: String,
}

impl WaitExternalHandler {
    pub fn new(name: &str) -> Arc<Self> {
        Arc::new(Self {
            event_name: name.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for WaitExternalHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let data = ctx.schedule_wait(&self.event_name).await;
        Ok(data)
    }
}

/// A mock handler that schedules a sub-orchestration
pub struct SubOrchHandler {
    sub_name: String,
    sub_input: String,
}

impl SubOrchHandler {
    pub fn new(name: &str, input: &str) -> Arc<Self> {
        Arc::new(Self {
            sub_name: name.to_string(),
            sub_input: input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for SubOrchHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let result = ctx.schedule_sub_orchestration(&self.sub_name, &self.sub_input).await?;
        Ok(result)
    }
}

/// A mock handler that calls continue_as_new
pub struct ContinueAsNewHandler {
    new_input: String,
}

impl ContinueAsNewHandler {
    pub fn new(input: &str) -> Arc<Self> {
        Arc::new(Self {
            new_input: input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for ContinueAsNewHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let _ = ctx.continue_as_new(&self.new_input).await;
        // continue_as_new suspends the orchestration, so we need to return something
        // but this should never be reached
        Ok("unreachable".to_string())
    }
}

/// A mock handler that schedules a sub-orchestration (fire-and-forget) then calls continue_as_new.
pub struct SubOrchThenContinueAsNewHandler {
    sub_name: String,
    sub_input: String,
    new_input: String,
}

impl SubOrchThenContinueAsNewHandler {
    pub fn new(sub_name: &str, sub_input: &str, new_input: &str) -> Arc<Self> {
        Arc::new(Self {
            sub_name: sub_name.to_string(),
            sub_input: sub_input.to_string(),
            new_input: new_input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for SubOrchThenContinueAsNewHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        drop(ctx.schedule_sub_orchestration(&self.sub_name, &self.sub_input));
        let _ = ctx.continue_as_new(&self.new_input).await;
        Ok("unreachable".to_string())
    }
}

/// A mock handler that starts a detached orchestration then schedules an activity (which suspends).
/// This is used to test OrchestrationChained nondeterminism - the handler suspends so the
/// OrchestrationChained event is processed during replay.
pub struct DetachedThenActivityHandler {
    orch_name: String,
    orch_instance: String,
    orch_input: String,
    activity_name: String,
    activity_input: String,
}

impl DetachedThenActivityHandler {
    pub fn new(
        orch_name: &str,
        orch_instance: &str,
        orch_input: &str,
        activity_name: &str,
        activity_input: &str,
    ) -> Arc<Self> {
        Arc::new(Self {
            orch_name: orch_name.to_string(),
            orch_instance: orch_instance.to_string(),
            orch_input: orch_input.to_string(),
            activity_name: activity_name.to_string(),
            activity_input: activity_input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for DetachedThenActivityHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        // Start detached orchestration (fire-and-forget)
        ctx.schedule_orchestration(&self.orch_name, &self.orch_instance, &self.orch_input);
        // Schedule activity which will suspend the orchestration
        let result = ctx.schedule_activity(&self.activity_name, &self.activity_input).await?;
        Ok(result)
    }
}

/// A mock handler that schedules multiple activities but returns immediately
pub struct MultiScheduleNoAwaitHandler {
    activities: Vec<(String, String)>,
}

impl MultiScheduleNoAwaitHandler {
    pub fn new(activities: Vec<(&str, &str)>) -> Arc<Self> {
        Arc::new(Self {
            activities: activities.iter().map(|(n, i)| (n.to_string(), i.to_string())).collect(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for MultiScheduleNoAwaitHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        // Schedule activities but don't await them (fire-and-forget)
        for (name, input) in &self.activities {
            drop(ctx.schedule_activity(name, input));
        }
        Ok("done".to_string())
    }
}

/// A mock handler that schedules two activities sequentially
pub struct TwoActivitiesHandler {
    first: (String, String),
    second: (String, String),
}

impl TwoActivitiesHandler {
    pub fn new(first: (&str, &str), second: (&str, &str)) -> Arc<Self> {
        Arc::new(Self {
            first: (first.0.to_string(), first.1.to_string()),
            second: (second.0.to_string(), second.1.to_string()),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for TwoActivitiesHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let r1 = ctx.schedule_activity(&self.first.0, &self.first.1).await?;
        let r2 = ctx.schedule_activity(&self.second.0, &self.second.1).await?;
        Ok(format!("{r1},{r2}"))
    }
}

/// A mock handler that schedules two timers sequentially
pub struct TwoTimersHandler {
    first: Duration,
    second: Duration,
}

impl TwoTimersHandler {
    pub fn new(first: Duration, second: Duration) -> Arc<Self> {
        Arc::new(Self { first, second })
    }
}

#[async_trait]
impl OrchestrationHandler for TwoTimersHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        ctx.schedule_timer(self.first).await;
        ctx.schedule_timer(self.second).await;
        Ok("done".to_string())
    }
}

/// A mock handler that schedules a sub-orchestration then an activity
pub struct SubOrchThenActivityHandler {
    sub_name: String,
    sub_input: String,
    activity_name: String,
    activity_input: String,
}

impl SubOrchThenActivityHandler {
    pub fn new(sub_name: &str, sub_input: &str, activity_name: &str, activity_input: &str) -> Arc<Self> {
        Arc::new(Self {
            sub_name: sub_name.to_string(),
            sub_input: sub_input.to_string(),
            activity_name: activity_name.to_string(),
            activity_input: activity_input.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for SubOrchThenActivityHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let r1 = ctx.schedule_sub_orchestration(&self.sub_name, &self.sub_input).await?;
        let r2 = ctx.schedule_activity(&self.activity_name, &self.activity_input).await?;
        Ok(format!("{r1},{r2}"))
    }
}

/// A mock handler that schedules an activity then a timer
pub struct ActivityThenTimerHandler {
    activity_name: String,
    activity_input: String,
    timer_duration: Duration,
}

impl ActivityThenTimerHandler {
    pub fn new(name: &str, input: &str, duration: Duration) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
            timer_duration: duration,
        })
    }
}

#[async_trait]
impl OrchestrationHandler for ActivityThenTimerHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let r = ctx.schedule_activity(&self.activity_name, &self.activity_input).await?;
        ctx.schedule_timer(self.timer_duration).await;
        Ok(r)
    }
}

/// A mock handler that does select2 on activity vs timer
pub struct Select2Handler {
    activity_name: String,
    activity_input: String,
    timer_duration: Duration,
}

impl Select2Handler {
    pub fn new(name: &str, input: &str, duration: Duration) -> Arc<Self> {
        Arc::new(Self {
            activity_name: name.to_string(),
            activity_input: input.to_string(),
            timer_duration: duration,
        })
    }
}

#[async_trait]
impl OrchestrationHandler for Select2Handler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let activity = ctx.schedule_activity(&self.activity_name, &self.activity_input);
        let timer = ctx.schedule_timer(self.timer_duration);
        match ctx.select2(activity, timer).await {
            duroxide::Either2::First(Ok(r)) => Ok(format!("activity:{r}")),
            duroxide::Either2::First(Err(e)) => Err(format!("activity_err:{e}")),
            duroxide::Either2::Second(()) => Ok("timeout".to_string()),
        }
    }
}

/// A mock handler that does join on multiple activities
pub struct JoinActivitiesHandler {
    activities: Vec<(String, String)>,
}

impl JoinActivitiesHandler {
    pub fn new(activities: Vec<(&str, &str)>) -> Arc<Self> {
        Arc::new(Self {
            activities: activities.iter().map(|(n, i)| (n.to_string(), i.to_string())).collect(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for JoinActivitiesHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        let futures: Vec<_> = self
            .activities
            .iter()
            .map(|(n, i)| ctx.schedule_activity(n, i))
            .collect();
        let results = ctx.join(futures).await;
        let combined: Result<Vec<_>, _> = results.into_iter().collect();
        match combined {
            Ok(r) => Ok(r.join(",")),
            Err(e) => Err(e),
        }
    }
}

/// A mock handler that panics
pub struct PanicHandler {
    message: String,
}

impl PanicHandler {
    pub fn new(msg: &str) -> Arc<Self> {
        Arc::new(Self {
            message: msg.to_string(),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for PanicHandler {
    async fn invoke(&self, _ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        panic!("{}", self.message);
    }
}

/// A mock handler that counts invocations
pub struct CountingHandler {
    count: AtomicUsize,
    result: Result<String, String>,
}

impl CountingHandler {
    pub fn new(result: Result<String, String>) -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
            result,
        })
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl OrchestrationHandler for CountingHandler {
    async fn invoke(&self, _ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.result.clone()
    }
}

/// A handler that checks is_replaying at different points
pub struct IsReplayingHandler {
    pub at_start: std::sync::Mutex<Option<bool>>,
}

impl IsReplayingHandler {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            at_start: std::sync::Mutex::new(None),
        })
    }
}

#[async_trait]
impl OrchestrationHandler for IsReplayingHandler {
    async fn invoke(&self, ctx: OrchestrationContext, _input: String) -> Result<String, String> {
        *self.at_start.lock().unwrap() = Some(ctx.is_replaying());
        Ok("done".to_string())
    }
}

// ============================================================================
// Engine Execution Helpers
// ============================================================================

/// Create an engine with the given baseline history
pub fn create_engine(history: Vec<Event>) -> ReplayEngine {
    ReplayEngine::new(TEST_INSTANCE.to_string(), TEST_EXECUTION_ID, history)
}

/// Create an engine with persisted_history_len set
pub fn create_engine_with_persisted_len(history: Vec<Event>, persisted_len: usize) -> ReplayEngine {
    ReplayEngine::new(TEST_INSTANCE.to_string(), TEST_EXECUTION_ID, history).with_persisted_history_len(persisted_len)
}

/// Execute an orchestration turn with the given handler
pub fn execute(engine: &mut ReplayEngine, handler: Arc<dyn OrchestrationHandler>) -> TurnResult {
    engine.execute_orchestration(
        handler,
        "test-input".to_string(),
        TEST_ORCH_NAME.to_string(),
        TEST_ORCH_VERSION.to_string(),
        TEST_WORKER_ID,
    )
}

// ============================================================================
// Assertions
// ============================================================================

/// Assert the result is TurnResult::Continue
pub fn assert_continue(result: &TurnResult) {
    assert!(
        matches!(result, TurnResult::Continue),
        "Expected TurnResult::Continue, got {result:?}"
    );
}

/// Assert the result is TurnResult::Completed with expected output
pub fn assert_completed(result: &TurnResult, expected: &str) {
    match result {
        TurnResult::Completed(output) => {
            assert_eq!(output, expected, "Unexpected completion output");
        }
        _ => panic!("Expected TurnResult::Completed, got {result:?}"),
    }
}

/// Assert the result is TurnResult::Failed
pub fn assert_failed(result: &TurnResult) {
    assert!(
        matches!(result, TurnResult::Failed(_)),
        "Expected TurnResult::Failed, got {result:?}"
    );
}

/// Assert the result is TurnResult::Failed with Nondeterminism
pub fn assert_nondeterminism(result: &TurnResult) {
    match result {
        TurnResult::Failed(ErrorDetails::Configuration {
            kind: ConfigErrorKind::Nondeterminism,
            ..
        }) => {}
        _ => panic!("Expected TurnResult::Failed(Nondeterminism), got {result:?}"),
    }
}

/// Assert the result is TurnResult::Failed with AppErrorKind::Panicked
pub fn assert_panicked(result: &TurnResult) {
    match result {
        TurnResult::Failed(ErrorDetails::Application {
            kind: duroxide::AppErrorKind::Panicked,
            ..
        }) => {}
        _ => panic!("Expected TurnResult::Failed(Panicked), got {result:?}"),
    }
}

/// Assert the result is TurnResult::Failed with expected message substring
pub fn assert_failed_with_message(result: &TurnResult, expected_substr: &str) {
    match result {
        TurnResult::Failed(details) => {
            let msg = details.display_message();
            assert!(
                msg.contains(expected_substr),
                "Expected error message to contain '{expected_substr}', got '{msg}'"
            );
        }
        _ => panic!("Expected TurnResult::Failed, got {result:?}"),
    }
}

/// Assert the result is TurnResult::ContinueAsNew with expected input
pub fn assert_continue_as_new(result: &TurnResult, expected_input: &str) {
    match result {
        TurnResult::ContinueAsNew { input, .. } => {
            assert_eq!(input, expected_input, "Unexpected continue_as_new input");
        }
        _ => panic!("Expected TurnResult::ContinueAsNew, got {result:?}"),
    }
}

/// Assert the result is TurnResult::Cancelled
pub fn assert_cancelled(result: &TurnResult) {
    assert!(
        matches!(result, TurnResult::Cancelled(_)),
        "Expected TurnResult::Cancelled, got {result:?}"
    );
}

/// Assert the result is TurnResult::Cancelled with expected reason
pub fn assert_cancelled_with_reason(result: &TurnResult, expected_reason: &str) {
    match result {
        TurnResult::Cancelled(reason) => {
            assert_eq!(reason, expected_reason, "Unexpected cancellation reason");
        }
        _ => panic!("Expected TurnResult::Cancelled, got {result:?}"),
    }
}

/// Check if pending_actions contains an activity with given name
pub fn has_activity_action(engine: &ReplayEngine, name: &str) -> bool {
    engine
        .pending_actions()
        .iter()
        .any(|a| matches!(a, duroxide::Action::CallActivity { name: n, .. } if n == name))
}

/// Check if pending_actions contains a timer
pub fn has_timer_action(engine: &ReplayEngine) -> bool {
    engine
        .pending_actions()
        .iter()
        .any(|a| matches!(a, duroxide::Action::CreateTimer { .. }))
}

/// Check if pending_actions contains an external wait for given name
pub fn has_external_action(engine: &ReplayEngine, name: &str) -> bool {
    engine
        .pending_actions()
        .iter()
        .any(|a| matches!(a, duroxide::Action::WaitExternal { name: n, .. } if n == name))
}

/// Check if pending_actions contains a sub-orchestration with given name
pub fn has_sub_orch_action(engine: &ReplayEngine, name: &str) -> bool {
    engine
        .pending_actions()
        .iter()
        .any(|a| matches!(a, duroxide::Action::StartSubOrchestration { name: n, .. } if n == name))
}

pub fn has_continue_as_new_action(engine: &ReplayEngine, expected_input: &str) -> bool {
    engine
        .pending_actions()
        .iter()
        .any(|a| matches!(a, duroxide::Action::ContinueAsNew { input, .. } if input == expected_input))
}

/// Check if history_delta contains an ActivityScheduled event
pub fn has_activity_scheduled_delta(engine: &ReplayEngine, name: &str) -> bool {
    engine
        .history_delta()
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ActivityScheduled { name: n, .. } if n == name))
}

/// Check if history_delta contains a TimerCreated event
pub fn has_timer_created_delta(engine: &ReplayEngine) -> bool {
    engine
        .history_delta()
        .iter()
        .any(|e| matches!(&e.kind, EventKind::TimerCreated { .. }))
}

/// Check if history_delta contains an ExternalSubscribed event
pub fn has_external_subscribed_delta(engine: &ReplayEngine, name: &str) -> bool {
    engine
        .history_delta()
        .iter()
        .any(|e| matches!(&e.kind, EventKind::ExternalSubscribed { name: n, .. } if n == name))
}

/// Get all event IDs from history_delta
pub fn delta_event_ids(engine: &ReplayEngine) -> Vec<u64> {
    engine.history_delta().iter().map(|e| e.event_id()).collect()
}

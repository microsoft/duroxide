// Runtime module: Mutex poisoning indicates a panic - all lock().unwrap()/expect() are intentional.
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]

//
use crate::providers::{ExecutionMetadata, Provider, WorkItem};
use crate::{Event, EventKind, OrchestrationContext};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::warn;

// ============================================================================
// Built-in System Activities
// ============================================================================

/// Inject built-in system activities into the activity registry.
/// This adds the new_guid and utc_now_ms activities that are used by
/// `OrchestrationContext::new_guid()` and `OrchestrationContext::utc_now()`.
fn inject_builtin_activities(user_registry: registry::ActivityRegistry) -> registry::ActivityRegistry {
    registry::ActivityRegistry::builder_from(&user_registry)
        .register_builtin(
            crate::SYSCALL_ACTIVITY_NEW_GUID,
            |_ctx: crate::ActivityContext, _input: String| async move { Ok(crate::generate_guid()) },
        )
        .register_builtin(
            crate::SYSCALL_ACTIVITY_UTC_NOW_MS,
            |_ctx: crate::ActivityContext, _input: String| async move {
                use std::time::{SystemTime, UNIX_EPOCH};
                let ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                Ok(ms.to_string())
            },
        )
        .build_result()
        .expect("builtin syscall activity registration should never fail")
}

/// Configuration for exponential backoff when encountering unregistered orchestrations/activities.
///
/// During rolling deployments, work items for unregistered handlers are abandoned
/// with exponential backoff instead of immediately failing. This allows the runtime
/// to wait for the handler to be registered on upgraded nodes.
///
/// # Backoff Calculation
///
/// For a work item with `attempt_count` (1-based):
/// - Delay = `base_delay * 2^(attempt_count - 1)`, capped at `max_delay`
///
/// # Example with Default Configuration
///
/// With default settings (`base_delay: 1s`, `max_delay: 60s`):
/// - Attempt 1: 1s delay
/// - Attempt 2: 2s delay
/// - Attempt 3: 4s delay
/// - Attempt 4: 8s delay
/// - Attempt 5: 16s delay
/// - Attempt 6: 32s delay
/// - Attempt 7+: 60s delay (capped)
#[derive(Debug, Clone)]
pub struct UnregisteredBackoffConfig {
    /// Base delay for the first backoff attempt.
    /// Default: 1 second
    pub base_delay: Duration,

    /// Maximum delay cap for any backoff attempt.
    /// Default: 60 seconds
    pub max_delay: Duration,
}

impl UnregisteredBackoffConfig {
    /// Maximum exponent for backoff calculation (caps at 64x base delay)
    const MAX_BACKOFF_EXPONENT: u32 = 6;
    /// Default base delay for unregistered handler backoff
    const DEFAULT_BASE_DELAY: Duration = Duration::from_secs(1);
    /// Default maximum delay for unregistered handler backoff
    const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(60);

    /// Calculate the backoff delay for a given attempt count (1-based).
    ///
    /// # Arguments
    ///
    /// * `attempt_count` - The fetch attempt number (1-based from provider)
    ///
    /// # Returns
    ///
    /// The backoff delay, capped at `max_delay`
    pub fn delay(&self, attempt_count: u32) -> Duration {
        // attempt_count is 1-based, so subtract 1 for exponent
        let exponent = attempt_count.saturating_sub(1).min(Self::MAX_BACKOFF_EXPONENT);
        let delay = self.base_delay.saturating_mul(1 << exponent);
        delay.min(self.max_delay)
    }
}

impl Default for UnregisteredBackoffConfig {
    fn default() -> Self {
        Self {
            base_delay: Self::DEFAULT_BASE_DELAY,
            max_delay: Self::DEFAULT_MAX_DELAY,
        }
    }
}

/// Configuration options for the Runtime.
///
/// # Example
///
/// ```rust,no_run
/// # use duroxide::runtime::{RuntimeOptions, ObservabilityConfig, LogFormat};
/// # use std::time::Duration;
/// let options = RuntimeOptions {
///     orchestration_concurrency: 4,
///     worker_concurrency: 8,
///     dispatcher_min_poll_interval: Duration::from_millis(25), // Polling backoff when queues idle
///     dispatcher_long_poll_timeout: Duration::from_secs(30),   // Long polling timeout
///     orchestrator_lock_timeout: Duration::from_secs(10),      // Orchestration turns retry after 10s
///     worker_lock_timeout: Duration::from_secs(300),        // Activities retry after 5 minutes
///     worker_lock_renewal_buffer: Duration::from_secs(30),  // Renew worker locks 30s early
///     observability: ObservabilityConfig {
///         log_format: LogFormat::Compact,
///        log_level: "info".to_string(),
///         ..Default::default()
///     },
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct RuntimeOptions {
    /// Minimum polling cycle duration when idle.
    ///
    /// If a provider returns 'None' (no work) faster than this duration,
    /// the dispatcher will sleep for the remainder of the time.
    /// This prevents hot loops for providers that do not support long polling
    /// or return early.
    ///
    /// Default: 100ms (10 Hz)
    pub dispatcher_min_poll_interval: Duration,

    /// Maximum time to wait for work inside the provider (Long Polling).
    ///
    /// Only used if the provider supports long polling.
    ///
    /// Default: 30 seconds
    pub dispatcher_long_poll_timeout: Duration,

    /// Number of concurrent orchestration workers.
    /// Each worker can process one orchestration turn at a time.
    /// Higher values = more parallel orchestration execution.
    /// Default: 2
    pub orchestration_concurrency: usize,

    /// Number of concurrent worker dispatchers.
    /// Each worker can execute one activity at a time.
    /// Higher values = more parallel activity execution.
    /// Default: 2
    pub worker_concurrency: usize,

    /// Lock timeout for orchestrator queue items.
    /// When an orchestration message is dequeued, it's locked for this duration.
    /// Orchestration turns are typically fast (milliseconds), so a shorter timeout is appropriate.
    /// If processing doesn't complete within this time, the lock expires and the message is retried.
    /// Default: 5 seconds
    pub orchestrator_lock_timeout: Duration,

    /// Buffer time before orchestration lock expiration to trigger renewal.
    ///
    /// Lock renewal strategy:
    /// - If `orchestrator_lock_timeout` ≥ 15s: renew at (`timeout - orchestrator_lock_renewal_buffer`)
    /// - If `orchestrator_lock_timeout` < 15s: renew at 0.5 × timeout (buffer ignored)
    ///
    /// Default: 2 seconds
    pub orchestrator_lock_renewal_buffer: Duration,

    /// Lock timeout for worker queue items (activities).
    /// When an activity is dequeued, it's locked for this duration.
    /// Activities can be long-running (minutes), so a longer timeout is appropriate.
    /// If processing doesn't complete within this time, the lock expires and the activity is retried.
    /// Higher values = more tolerance for long-running activities.
    /// Lower values = faster retry on failures, but may timeout legitimate work.
    /// Default: 30 seconds
    pub worker_lock_timeout: Duration,

    /// Buffer time before lock expiration to trigger renewal.
    ///
    /// Lock renewal strategy:
    /// - If `worker_lock_timeout` ≥ 15s: renew at (`timeout - worker_lock_renewal_buffer`)
    /// - If `worker_lock_timeout` < 15s: renew at 0.5 × timeout (buffer ignored)
    ///
    /// Example with default values (timeout=30s, buffer=5s):
    /// - Initial lock: expires at T+30s
    /// - First renewal: at T+25s (30-5), extends to T+55s
    /// - Second renewal: at T+50s (55-5), extends to T+80s
    ///
    /// Example with short timeout (timeout=10s, buffer ignored):
    /// - Initial lock: expires at T+10s
    /// - First renewal: at T+5s (10*0.5), extends to T+15s
    /// - Second renewal: at T+10s (15*0.5), extends to T+20s
    ///
    /// Default: 5 seconds
    pub worker_lock_renewal_buffer: Duration,

    /// Observability configuration for metrics and logging.
    /// Requires the `observability` feature flag for full functionality.
    /// Default: Disabled with basic logging
    pub observability: ObservabilityConfig,

    /// Configuration for backoff when encountering unregistered orchestrations/activities.
    ///
    /// During rolling deployments, work items for unregistered handlers are abandoned
    /// with exponential backoff instead of immediately failing. This allows the runtime
    /// to wait for the handler to be registered on upgraded nodes.
    ///
    /// Default: 1s base delay, 60s max delay
    pub unregistered_backoff: UnregisteredBackoffConfig,

    /// Maximum fetch attempts before a message is considered poison.
    ///
    /// After this many fetch attempts, the runtime will immediately fail
    /// the orchestration/activity with a Poison error instead of processing.
    ///
    /// Default: 10
    pub max_attempts: u32,

    /// Grace period for activity cancellation.
    ///
    /// When an orchestration reaches a terminal state, in-flight activities
    /// are notified via their cancellation token. This setting controls how
    /// long to wait for activities to complete gracefully before aborting
    /// the activity task to free worker capacity.
    ///
    /// After this grace period, if the activity has not completed:
    /// - The activity task is aborted (`JoinHandle::abort()`)
    /// - The worker queue message is dropped without notifying the orchestrator
    /// - A warning is logged
    ///
    /// Note: Child tasks/threads spawned by the activity that do not observe
    /// the cancellation token may outlive the abort (user responsibility).
    ///
    /// Default: 10 seconds
    pub activity_cancellation_grace_period: Duration,

    /// Override the replay-engine version range used for capability filtering.
    ///
    /// By default, the runtime uses `>=0.0.0, <=CURRENT_BUILD_VERSION`, meaning it
    /// can replay any execution pinned at or below its own semver. This is correct for
    /// most deployments since replay engines are backward-compatible.
    ///
    /// Set this to change the range for advanced scenarios:
    /// - **Narrowing:** Restrict a node to only process a specific version band
    ///   (e.g., `>=1.0.0, <=1.9.999` in a mixed-version cluster).
    /// - **Widening to drain stuck items:** Set a wide range like `>=0.0.0, <=99.0.0`
    ///   to fetch orchestrations pinned at any version. Items with unknown event types
    ///   will fail at provider-level deserialization (never reaching the replay engine)
    ///   and remain in the queue with escalating `attempt_count`.
    ///
    /// Default: `None` (uses `>=0.0.0, <=CURRENT_BUILD_VERSION`)
    pub supported_replay_versions: Option<crate::providers::SemverRange>,

    /// Lock timeout for session heartbeat lease.
    /// Controls crash recovery speed — if a worker dies, its sessions become
    /// claimable after this duration.
    /// Default: 30 seconds
    pub session_lock_timeout: Duration,

    /// Buffer time before session lock expiration to trigger renewal.
    /// Uses the same formula as `worker_lock_renewal_buffer`.
    /// Default: 5 seconds
    pub session_lock_renewal_buffer: Duration,

    /// How long a session stays pinned after the last activity is
    /// fetched, renewed, or completed. The session renewal thread
    /// stops heartbeating idle sessions, so their locks naturally expire.
    /// Default: 5 minutes
    pub session_idle_timeout: Duration,

    /// How often orphaned session rows are swept from the sessions table.
    /// Runs on the same background thread as session lock renewal.
    /// Default: 5 minutes
    pub session_cleanup_interval: Duration,

    /// Maximum number of distinct sessions this runtime will own concurrently,
    /// spanning **all** `worker_concurrency` slots.
    ///
    /// A single `SessionTracker` is shared across every worker slot in this
    /// runtime. When `distinct_count()` reaches this limit, **all** slots stop
    /// claiming new sessions (fetch switches to non-session mode) until an
    /// in-flight session activity completes and frees a session slot.
    ///
    /// Session activities and non-session activities share the same
    /// `worker_concurrency` slots.
    /// Default: 10
    pub max_sessions_per_runtime: usize,

    /// Stable worker identity for session ownership.
    /// If set, used directly as the session `worker_id` for session claims —
    /// all `worker_concurrency` slots share this single identity, so any idle
    /// slot can serve any session owned by this runtime (no head-of-line blocking).
    /// Also allows a restarted worker to reclaim its sessions without waiting
    /// for lock expiry.
    /// Example: Kubernetes StatefulSet pod name.
    /// If `None`, uses ephemeral per-slot identity (`work-{idx}-{runtime_id}`);
    /// sessions are pinned per-slot and cannot survive restarts.
    /// Note: Logging/tracing always includes the per-slot `work-{idx}-{node_id}`
    /// format regardless of this setting.
    /// Default: None
    pub worker_node_id: Option<String>,

    /// Tag filter for worker activity routing.
    ///
    /// Controls which activities this runtime's worker slots will process:
    /// - `DefaultOnly`: Only untagged activities (default)
    /// - `Tags(["gpu"])`: Only activities tagged `"gpu"`
    /// - `DefaultAnd(["gpu"])`: Both untagged and `"gpu"` activities
    /// - `None`: Disable worker (orchestrator-only mode)
    ///
    /// Default: `TagFilter::DefaultOnly` (untagged activities only)
    pub worker_tag_filter: crate::providers::TagFilter,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            dispatcher_min_poll_interval: Duration::from_millis(100),
            dispatcher_long_poll_timeout: Duration::from_secs(30), // 30 seconds
            orchestration_concurrency: 2,
            worker_concurrency: 2,
            orchestrator_lock_timeout: Duration::from_secs(5),
            orchestrator_lock_renewal_buffer: Duration::from_secs(2),
            worker_lock_timeout: Duration::from_secs(30),
            worker_lock_renewal_buffer: Duration::from_secs(5),
            observability: ObservabilityConfig::default(),
            unregistered_backoff: UnregisteredBackoffConfig::default(),
            max_attempts: 10,
            activity_cancellation_grace_period: Duration::from_secs(10),
            supported_replay_versions: None,
            session_lock_timeout: Duration::from_secs(30),
            session_lock_renewal_buffer: Duration::from_secs(5),
            session_idle_timeout: Duration::from_secs(300), // 5 minutes
            session_cleanup_interval: Duration::from_secs(300), // 5 minutes
            max_sessions_per_runtime: 10,
            worker_node_id: None,
            worker_tag_filter: crate::providers::TagFilter::default(),
        }
    }
}

mod dispatchers;
pub mod limits;
pub mod observability;
pub mod registry;
mod state_helpers;

#[cfg(feature = "test-hooks")]
pub mod test_hooks;

use async_trait::async_trait;
pub use state_helpers::{HistoryManager, WorkItemReader};

pub mod execution;
pub mod replay_engine;

pub use observability::{LogFormat, ObservabilityConfig};

/// High-level orchestration status derived from history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrchestrationStatus {
    /// Instance does not exist
    NotFound,
    /// Instance is currently executing
    Running {
        /// User-defined progress string set via `ctx.set_custom_status()`
        custom_status: Option<String>,
        /// Monotonically increasing version counter for change detection
        custom_status_version: u64,
    },
    /// Instance completed successfully with output
    Completed {
        output: String,
        /// Last custom status set before completion
        custom_status: Option<String>,
        /// Version at completion time
        custom_status_version: u64,
    },
    /// Instance failed with structured error details.
    /// Use `details.category()` to distinguish infrastructure/configuration/application errors.
    Failed {
        details: crate::ErrorDetails,
        /// Last custom status set before failure
        custom_status: Option<String>,
        /// Version at failure time
        custom_status_version: u64,
    },
}

/// Trait implemented by orchestration handlers that can be invoked by the runtime.
#[async_trait]
pub trait OrchestrationHandler: Send + Sync {
    async fn invoke(&self, ctx: OrchestrationContext, input: String) -> Result<String, String>;
}

/// Function wrapper that implements `OrchestrationHandler`.
pub struct FnOrchestration<F, Fut>(pub F)
where
    F: Fn(OrchestrationContext, String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<String, String>> + Send + 'static;

#[async_trait]
impl<F, Fut> OrchestrationHandler for FnOrchestration<F, Fut>
where
    F: Fn(OrchestrationContext, String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
{
    async fn invoke(&self, ctx: OrchestrationContext, input: String) -> Result<String, String> {
        (self.0)(ctx, input).await
    }
}

/// Trait implemented by activity handlers that can be invoked by the runtime.
#[async_trait]
pub trait ActivityHandler: Send + Sync {
    async fn invoke(&self, ctx: crate::ActivityContext, input: String) -> Result<String, String>;
}

/// Function wrapper that implements `ActivityHandler`.
pub struct FnActivity<F, Fut>(pub F)
where
    F: Fn(crate::ActivityContext, String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<String, String>> + Send + 'static;

#[async_trait]
impl<F, Fut> ActivityHandler for FnActivity<F, Fut>
where
    F: Fn(crate::ActivityContext, String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
{
    async fn invoke(&self, ctx: crate::ActivityContext, input: String) -> Result<String, String> {
        (self.0)(ctx, input).await
    }
}

/// Immutable registry mapping orchestration names to versioned handlers.
pub use crate::runtime::registry::{OrchestrationRegistry, OrchestrationRegistryBuilder, VersionPolicy};

pub fn kind_of(msg: &WorkItem) -> &'static str {
    match msg {
        WorkItem::StartOrchestration { .. } => "StartOrchestration",
        WorkItem::ActivityExecute { .. } => "ActivityExecute",
        WorkItem::ActivityCompleted { .. } => "ActivityCompleted",
        WorkItem::ActivityFailed { .. } => "ActivityFailed",
        WorkItem::TimerFired { .. } => "TimerFired",
        WorkItem::ExternalRaised { .. } => "ExternalRaised",
        WorkItem::QueueMessage { .. } => "ExternalRaisedPersistent",
        #[cfg(feature = "replay-version-test")]
        WorkItem::ExternalRaised2 { .. } => "ExternalRaised2",
        WorkItem::SubOrchCompleted { .. } => "SubOrchCompleted",
        WorkItem::SubOrchFailed { .. } => "SubOrchFailed",
        WorkItem::CancelInstance { .. } => "CancelInstance",
        WorkItem::ContinueAsNew { .. } => "ContinueAsNew",
    }
}

/// In-process runtime that executes activities and timers and persists
/// history via a `Provider`.
pub struct Runtime {
    joins: Mutex<Vec<JoinHandle<()>>>,
    history_store: Arc<dyn Provider>,
    orchestration_registry: OrchestrationRegistry,
    /// Track the current execution ID for each active instance
    current_execution_ids: Mutex<HashMap<String, u64>>,
    /// Shutdown flag checked by dispatchers
    shutdown_flag: Arc<AtomicBool>,
    /// Runtime configuration options
    options: RuntimeOptions,
    /// Observability handle for metrics and logging
    observability_handle: Option<observability::ObservabilityHandle>,
    /// Unique runtime instance ID (4-char hex, generated on start)
    runtime_id: String,
}

/// Introspection: descriptor of an orchestration derived from history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestrationDescriptor {
    pub name: String,
    pub version: String,
    pub parent_instance: Option<String>,
    pub parent_id: Option<u64>,
}

impl Runtime {
    /// Helper to get the metrics provider if available.
    #[inline]
    fn metrics_provider(&self) -> Option<&observability::MetricsProvider> {
        self.observability_handle
            .as_ref()
            .map(|h| h.metrics_provider().as_ref())
    }

    // New label-aware metric recording methods
    #[inline]
    fn record_orchestration_start(&self, orchestration_name: &str, version: &str, initiated_by: &str) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_orchestration_start(orchestration_name, version, initiated_by);
        }
    }

    #[inline]
    fn record_orchestration_completion_with_labels(
        &self,
        orchestration_name: &str,
        version: &str,
        status: &str,
        duration_seconds: f64,
        turn_count: u64,
        history_events: u64,
    ) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_orchestration_completion(
                orchestration_name,
                version,
                status,
                duration_seconds,
                turn_count,
                history_events,
            );
        }
    }

    #[inline]
    fn record_orchestration_failure_with_labels(
        &self,
        orchestration_name: &str,
        version: &str,
        error_type: &str,
        error_category: &str,
    ) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_orchestration_failure(orchestration_name, version, error_type, error_category);
        }
    }

    #[inline]
    fn record_continue_as_new(&self, orchestration_name: &str, execution_id: u64) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_continue_as_new(orchestration_name, execution_id);
        }
    }

    #[inline]
    fn increment_active_orchestrations(&self) {
        if let Some(provider) = self.metrics_provider() {
            provider.increment_active_orchestrations();
        }
    }

    #[inline]
    fn decrement_active_orchestrations(&self) {
        if let Some(provider) = self.metrics_provider() {
            provider.decrement_active_orchestrations();
        }
    }

    #[inline]
    fn record_activity_execution(
        &self,
        activity_name: &str,
        outcome: &str,
        duration_seconds: f64,
        retry_attempt: u32,
        tag: Option<&str>,
    ) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_activity_execution(activity_name, outcome, duration_seconds, retry_attempt, tag);
        }
    }

    // Simple metric recording methods (used by execution.rs and worker.rs)
    // These call MetricsProvider methods which emit both counter!() and atomic increments
    #[inline]
    fn record_orchestration_application_error(&self) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_orchestration_application_error();
        }
    }

    #[inline]
    fn record_orchestration_infrastructure_error(&self) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_orchestration_infrastructure_error();
        }
    }

    #[inline]
    fn record_orchestration_configuration_error(&self) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_orchestration_configuration_error();
        }
    }

    #[inline]
    fn record_activity_success(&self) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_activity_success();
        }
    }

    #[inline]
    fn record_activity_app_error(&self) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_activity_app_error();
        }
    }

    #[inline]
    fn record_activity_infra_error(&self) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_activity_infra_error();
        }
    }

    #[inline]
    fn record_orchestration_poison(&self) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_orchestration_poison();
        }
    }

    #[inline]
    fn record_activity_poison(&self) {
        if let Some(provider) = self.metrics_provider() {
            provider.record_activity_poison();
        }
    }

    pub fn metrics_snapshot(&self) -> Option<observability::MetricsSnapshot> {
        self.observability_handle
            .as_ref()
            .map(|handle| handle.metrics_snapshot())
    }

    /// Returns a reference to the observability handle, if observability is enabled.
    pub fn observability_handle(&self) -> Option<&observability::ObservabilityHandle> {
        self.observability_handle.as_ref()
    }

    /// Initialize all gauges that need to sync with persistent state on startup.
    ///
    /// Gauges (unlike counters) represent current state and must be initialized
    /// from the provider to reflect reality after a restart.
    ///
    /// This initializes:
    /// - `duroxide_active_orchestrations` - Current running orchestrations
    /// - `duroxide_orchestrator_queue_depth` - Current orchestrator queue backlog
    /// - `duroxide_worker_queue_depth` - Current worker queue backlog
    async fn initialize_gauges(self: Arc<Self>) {
        if let Some(admin) = self.history_store.as_management_capability() {
            // Query provider for current state (parallel for efficiency)
            let system_metrics_future = admin.get_system_metrics();
            let queue_depths_future = admin.get_queue_depths();

            let (system_result, queue_result) = tokio::join!(system_metrics_future, queue_depths_future);

            if let Some(provider) = self.observability_handle.as_ref().map(|h| h.metrics_provider()) {
                // Initialize active orchestrations gauge
                if let Ok(metrics) = system_result {
                    let active_count = metrics.running_instances as i64;
                    provider.set_active_orchestrations(active_count);
                    tracing::debug!(
                        target: "duroxide::runtime",
                        active_count = %active_count,
                        "Initialized active orchestrations gauge"
                    );
                }

                // Initialize queue depth gauges
                if let Ok(depths) = queue_result {
                    provider.update_queue_depths(depths.orchestrator_queue as u64, depths.worker_queue as u64);
                    tracing::debug!(
                        target: "duroxide::runtime",
                        orch_queue = %depths.orchestrator_queue,
                        worker_queue = %depths.worker_queue,
                        "Initialized queue depth gauges"
                    );
                }
            }
        }
    }

    /// Spawn a background task that periodically polls the provider for gauge values.
    ///
    /// Updates `duroxide_active_orchestrations`, `duroxide_orchestrator_queue_depth`,
    /// and `duroxide_worker_queue_depth` gauges from the database at the configured interval.
    fn start_gauge_poller(self: Arc<Self>) -> JoinHandle<()> {
        let interval = self.options.observability.gauge_poll_interval;
        let shutdown_flag = self.shutdown_flag.clone();

        tokio::spawn(async move {
            tracing::debug!(
                target: "duroxide::runtime",
                interval_secs = interval.as_secs(),
                "Gauge poller started"
            );

            loop {
                tokio::time::sleep(interval).await;

                if shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }

                self.clone().refresh_gauges().await;
            }
        })
    }

    /// Refresh all gauge metrics from the provider.
    async fn refresh_gauges(self: Arc<Self>) {
        let provider = &self
            .observability_handle
            .as_ref()
            .expect("gauge poller only runs when observability is enabled")
            .metrics_provider();
        let admin = match self.history_store.as_management_capability() {
            Some(admin) => admin,
            None => return,
        };

        let (system_result, queue_result) = tokio::join!(admin.get_system_metrics(), admin.get_queue_depths());

        if let Ok(metrics) = system_result {
            provider.set_active_orchestrations(metrics.running_instances as i64);
        }
        if let Ok(depths) = queue_result {
            provider.update_queue_depths(depths.orchestrator_queue as u64, depths.worker_queue as u64);
        }
    }

    /// Compute execution metadata from history delta without inspecting event contents.
    /// This allows the runtime to extract semantic information and pass it to the provider
    /// as pre-computed metadata, preventing the provider from needing orchestration knowledge.
    fn compute_execution_metadata(
        history_delta: &[Event],
        _orchestrator_items: &[WorkItem],
        _current_execution_id: u64,
    ) -> ExecutionMetadata {
        let mut metadata = ExecutionMetadata::default();

        // Scan history_delta for OrchestrationStarted (first event) and terminal events
        for event in history_delta {
            match &event.kind {
                EventKind::OrchestrationStarted {
                    name,
                    version,
                    parent_instance,
                    ..
                } => {
                    // Capture orchestration metadata from start event
                    metadata.orchestration_name = Some(name.clone());
                    metadata.orchestration_version = Some(version.clone());
                    // Capture parent instance for sub-orchestration tracking (cascading delete)
                    metadata.parent_instance_id = parent_instance.clone();
                    // Extract pinned duroxide version from the event's duroxide_version field.
                    // This is the version of the runtime that created this execution.
                    // The provider stores it for capability-filtered fetching.
                    metadata.pinned_duroxide_version = semver::Version::parse(&event.duroxide_version).ok();
                }
                EventKind::OrchestrationCompleted { output } => {
                    metadata.status = Some("Completed".to_string());
                    metadata.output = Some(output.clone());
                    break;
                }
                EventKind::OrchestrationFailed { details } => {
                    metadata.status = Some("Failed".to_string());
                    metadata.output = Some(details.display_message());
                    break;
                }
                EventKind::OrchestrationContinuedAsNew { input } => {
                    metadata.status = Some("ContinuedAsNew".to_string());
                    metadata.output = Some(input.clone());
                    // Don't set create_next_execution - the new execution will be started
                    // by WorkItem::ContinueAsNew being processed like StartOrchestration
                    break;
                }
                _ => {}
            }
        }

        metadata
    }

    // Execution engine: consumes provider queues and persists history atomically.
    /// Return the most recent descriptor `{ name, version, parent_instance?, parent_id? }` for an instance.
    /// Returns `None` if the instance/history does not exist or no OrchestrationStarted is present.
    pub async fn get_orchestration_descriptor(
        &self,
        instance: &str,
    ) -> Option<crate::runtime::OrchestrationDescriptor> {
        let hist = self.history_store.read(instance).await.unwrap_or_default();
        for e in hist.iter().rev() {
            if let EventKind::OrchestrationStarted {
                name,
                version,
                parent_instance,
                parent_id,
                ..
            } = &e.kind
            {
                return Some(crate::runtime::OrchestrationDescriptor {
                    name: name.clone(),
                    version: version.clone(),
                    parent_instance: parent_instance.clone(),
                    parent_id: *parent_id,
                });
            }
        }
        None
    }

    /// Get the current execution ID for an instance, or fetch from store if not tracked
    ///
    /// If `current_execution_id` is provided and the instance matches, use it directly.
    /// Otherwise, check in-memory tracking, then fall back to INITIAL_EXECUTION_ID.
    async fn get_execution_id_for_instance(&self, instance: &str, current_execution_id: Option<u64>) -> u64 {
        // If this is the current instance being processed, use the provided execution_id
        if let Some(exec_id) = current_execution_id {
            // Update in-memory tracking for future calls
            self.current_execution_ids
                .lock()
                .await
                .insert(instance.to_string(), exec_id);
            return exec_id;
        }

        // First check in-memory tracking
        if let Some(&exec_id) = self.current_execution_ids.lock().await.get(instance) {
            return exec_id;
        }

        // Fall back to INITIAL_EXECUTION_ID (no longer querying Provider::latest_execution_id)
        crate::INITIAL_EXECUTION_ID
    }

    /// Start a new runtime using the in-memory SQLite provider.
    ///
    /// Requires the `sqlite` feature.
    #[cfg(feature = "sqlite")]
    pub async fn start(
        activity_registry: registry::ActivityRegistry,
        orchestration_registry: OrchestrationRegistry,
    ) -> Arc<Self> {
        let history_store: Arc<dyn Provider> = Arc::new(
            crate::providers::sqlite::SqliteProvider::new_in_memory()
                .await
                .expect("in-memory SQLite provider creation should never fail"),
        );
        Self::start_with_store(history_store, activity_registry, orchestration_registry).await
    }

    /// Start a new runtime with a custom `Provider` implementation.
    pub async fn start_with_store(
        history_store: Arc<dyn Provider>,
        activity_registry: registry::ActivityRegistry,
        orchestration_registry: OrchestrationRegistry,
    ) -> Arc<Self> {
        Self::start_with_options(
            history_store,
            activity_registry,
            orchestration_registry,
            RuntimeOptions::default(),
        )
        .await
    }

    /// Start a new runtime with custom options.
    ///
    /// # Panics
    ///
    /// Panics if `session_idle_timeout` is not greater than the worker lock renewal interval
    /// (`worker_lock_timeout - worker_lock_renewal_buffer`). This prevents sessions from being
    /// unpinned during long-running activity execution.
    pub async fn start_with_options(
        history_store: Arc<dyn Provider>,
        activity_registry: registry::ActivityRegistry,
        orchestration_registry: OrchestrationRegistry,
        options: RuntimeOptions,
    ) -> Arc<Self> {
        // Validate session timeout invariant
        let worker_renewal_interval = options
            .worker_lock_timeout
            .checked_sub(options.worker_lock_renewal_buffer)
            .unwrap_or(Duration::from_secs(1));
        if options.session_idle_timeout <= worker_renewal_interval {
            panic!(
                "session_idle_timeout ({}s) must be greater than worker lock renewal interval ({}s). \
                 Sessions would unpin during long-running activity execution. \
                 Increase session_idle_timeout or decrease worker_lock_timeout.",
                options.session_idle_timeout.as_secs(),
                worker_renewal_interval.as_secs(),
            );
        }

        // Inject built-in system activities (new_guid, utc_now_ms)
        let activity_registry = inject_builtin_activities(activity_registry);

        // Wrap activity registry in Arc for internal sharing across worker threads
        let activity_registry = Arc::new(activity_registry);

        // Initialize observability (metrics + structured logging)
        let observability_handle = observability::ObservabilityHandle::init(&options.observability).ok(); // Gracefully degrade if observability fails to initialize

        // Print version on startup
        tracing::info!(
            target: "duroxide::runtime",
            "duroxide runtime ({}) starting with provider {} ({})",
            env!("CARGO_PKG_VERSION"),
            history_store.name(),
            history_store.version()
        );

        // Wrap provider with metrics instrumentation if metrics are enabled
        let history_store: Arc<dyn Provider> = if let Some(ref handle) = observability_handle {
            let metrics = handle.metrics_provider();
            Arc::new(crate::providers::instrumented::InstrumentedProvider::new(
                history_store,
                Some(metrics.clone()),
            ))
        } else {
            history_store
        };

        let joins: Vec<JoinHandle<()>> = Vec::new();

        // Generate unique runtime instance ID (4-char hex)
        use std::time::{SystemTime, UNIX_EPOCH};
        let runtime_id = format!(
            "{:04x}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| (d.as_nanos() & 0xFFFF) as u16)
                .unwrap_or(0)
        );

        // start request queue + worker
        let runtime = Arc::new(Self {
            joins: Mutex::new(joins),
            history_store,
            orchestration_registry,
            current_execution_ids: Mutex::new(HashMap::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),

            options,
            observability_handle,
            runtime_id,
        });

        // Initialize gauges from provider (if supported)
        runtime.clone().initialize_gauges().await;

        // Start periodic gauge polling if observability is enabled
        if runtime.observability_handle.is_some() {
            let gauge_handle = runtime.clone().start_gauge_poller();
            runtime.joins.lock().await.push(gauge_handle);
        }

        // background orchestrator dispatcher (extracted from inline poller)
        let handle = runtime.clone().start_orchestration_dispatcher();
        runtime.joins.lock().await.push(handle);

        // background work dispatcher (executes activities)
        let work_handle = runtime.clone().start_work_dispatcher(activity_registry);
        runtime.joins.lock().await.push(work_handle);

        runtime
    }

    /// Shutdown the runtime.
    ///
    /// # Parameters
    ///
    /// * `timeout_ms` - How long to wait for graceful shutdown:
    ///   - `None`: Default 1000ms
    ///   - `Some(Duration::ZERO)`: Immediate abort
    ///   - `Some(ms)`: Wait specified milliseconds
    pub async fn shutdown(self: Arc<Self>, timeout_ms: Option<u64>) {
        let timeout_ms = timeout_ms.unwrap_or(1000);

        if timeout_ms == 0 {
            warn!("Immediate shutdown - aborting all tasks");
            let mut joins = self.joins.lock().await;
            for j in joins.drain(..) {
                j.abort();
            }
            return;
        }

        // debug!("Graceful shutdown (timeout: {}ms)", timeout_ms);

        // Set shutdown flag - workers check this between iterations
        self.shutdown_flag.store(true, Ordering::Relaxed);

        // Give workers time to notice and exit gracefully
        tokio::time::sleep(std::time::Duration::from_millis(timeout_ms)).await;

        // Check if any tasks are still running (need to be aborted)
        let mut joins = self.joins.lock().await;

        // Abort any remaining tasks
        for j in joins.drain(..) {
            j.abort();
        }

        // debug!("Runtime shut down");

        // Shutdown observability last (after all workers stopped)
        // Note: We can't move out of Arc here, so observability shutdown happens when Runtime is dropped
        // or if we could restructure to take ownership in shutdown
    }
}

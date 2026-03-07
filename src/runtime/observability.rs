//! Observability infrastructure for metrics and structured logging.
//!
//! This module provides metrics via the `metrics` facade crate and structured logging
//! via `tracing`/`tracing-subscriber`. Users choose their own metrics backend by
//! installing a global recorder before starting the runtime.
//!
//! # Metrics Backend Options
//!
//! ```rust,ignore
//! // Option 1: Prometheus (direct scraping)
//! metrics_exporter_prometheus::PrometheusBuilder::new()
//!     .with_http_listener(([0, 0, 0, 0], 9090))
//!     .install()?;
//!
//! // Option 2: OpenTelemetry (via metrics-exporter-opentelemetry)
//! // metrics_exporter_opentelemetry::Recorder::builder("my-service").install_global()?;
//!
//! // Option 3: None - metrics become zero-cost no-ops
//! ```

// Observability uses Mutex locks - poison indicates a panic and should propagate
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]

use metrics::{counter, gauge, histogram};
use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64, Ordering},
};
use std::time::Duration;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Log format options for structured logging
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LogFormat {
    /// Structured JSON output for log aggregators
    Json,
    /// Human-readable format for development (with all fields)
    Pretty,
    /// Compact format: timestamp level module \[instance_id\] message
    #[default]
    Compact,
}

/// Observability configuration for metrics and logging.
///
/// Controls structured logging format and level. Metrics are always available
/// via the `metrics` facade - users install their preferred exporter.
///
/// # Example
///
/// ```rust,no_run
/// # use duroxide::runtime::{ObservabilityConfig, LogFormat};
/// // Simplest: Compact logs to stdout
/// let config = ObservabilityConfig {
///     log_format: LogFormat::Compact,
///     log_level: "info".to_string(),
///     ..Default::default()
/// };
///
/// // Production: JSON logs (metrics via user-installed exporter)
/// let config = ObservabilityConfig {
///     log_format: LogFormat::Json,
///     service_name: "my-app".to_string(),
///     ..Default::default()
/// };
/// ```
///
/// # Correlation Fields
///
/// All logs automatically include:
/// - `instance_id` - Orchestration instance identifier
/// - `execution_id` - Execution number (for ContinueAsNew)
/// - `orchestration_name` - Name of the orchestration
/// - `orchestration_version` - Semantic version
/// - `activity_name` - Activity name (in activity context)
/// - `worker_id` - Dispatcher worker ID
///
/// # See Also
///
/// - [Observability Guide](../../docs/observability-guide.md) - Full documentation
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    // Structured logging configuration
    /// Log output format
    pub log_format: LogFormat,
    /// Log level filter (e.g., "info", "debug")
    pub log_level: String,

    // Common configuration
    /// Service name for identification in logs/metrics
    pub service_name: String,
    /// Optional service version
    pub service_version: Option<String>,

    // Gauge polling configuration
    /// Interval for polling the provider to refresh gauge metrics
    /// (queue depths, active orchestrations). Defaults to 60 seconds.
    pub gauge_poll_interval: Duration,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Pretty,
            log_level: "info".to_string(),
            service_name: "duroxide".to_string(),
            service_version: None,
            gauge_poll_interval: Duration::from_secs(60),
        }
    }
}

fn default_filter_expression(level: &str) -> String {
    format!("warn,duroxide::orchestration={level},duroxide::activity={level}")
}

/// Snapshot of key observability metrics counters for tests and diagnostics.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub orch_starts: u64,
    pub orch_completions: u64,
    pub orch_failures: u64,
    pub orch_application_errors: u64,
    pub orch_infrastructure_errors: u64,
    pub orch_configuration_errors: u64,
    pub orch_poison: u64,
    pub activity_success: u64,
    pub activity_app_errors: u64,
    pub activity_infra_errors: u64,
    pub activity_config_errors: u64,
    pub activity_poison: u64,
    pub orch_dispatcher_items_fetched: u64,
    pub worker_dispatcher_items_fetched: u64,
    pub orch_continue_as_new: u64,
    pub suborchestration_calls: u64,
    pub provider_errors: u64,
}

/// Metrics provider using the `metrics` facade crate.
///
/// Emits metrics via `counter!`, `gauge!`, `histogram!` macros.
/// If no global recorder is installed, these are zero-cost no-ops.
/// Atomic counters are maintained for test snapshot assertions.
pub struct MetricsProvider {
    // Atomic counters for test snapshot assertions (mirrors facade metrics)
    orch_starts_atomic: AtomicU64,
    orch_completions_atomic: AtomicU64,
    orch_failures_atomic: AtomicU64,
    orch_application_errors_atomic: AtomicU64,
    orch_infrastructure_errors_atomic: AtomicU64,
    orch_configuration_errors_atomic: AtomicU64,
    orch_poison_atomic: AtomicU64,
    activity_success_atomic: AtomicU64,
    activity_app_errors_atomic: AtomicU64,
    activity_infra_errors_atomic: AtomicU64,
    activity_config_errors_atomic: AtomicU64,
    activity_poison_atomic: AtomicU64,

    // Dispatcher counters
    orch_dispatcher_items_fetched_atomic: AtomicU64,
    worker_dispatcher_items_fetched_atomic: AtomicU64,

    // Other counters
    orch_continue_as_new_atomic: AtomicU64,
    suborchestration_calls_atomic: AtomicU64,
    provider_errors_atomic: AtomicU64,

    // Queue depth tracking (updated by background task or direct calls)
    orch_queue_depth_atomic: Arc<AtomicU64>,
    worker_queue_depth_atomic: Arc<AtomicU64>,

    // Active orchestrations tracking (for gauge metrics)
    active_orchestrations_atomic: Arc<AtomicI64>,
}

impl MetricsProvider {
    /// Create a new metrics provider.
    ///
    /// # Errors
    ///
    /// Currently infallible, but returns Result for API compatibility.
    pub fn new(_config: &ObservabilityConfig) -> Result<Self, String> {
        Ok(Self {
            orch_starts_atomic: AtomicU64::new(0),
            orch_completions_atomic: AtomicU64::new(0),
            orch_failures_atomic: AtomicU64::new(0),
            orch_application_errors_atomic: AtomicU64::new(0),
            orch_infrastructure_errors_atomic: AtomicU64::new(0),
            orch_configuration_errors_atomic: AtomicU64::new(0),
            orch_poison_atomic: AtomicU64::new(0),
            activity_success_atomic: AtomicU64::new(0),
            activity_app_errors_atomic: AtomicU64::new(0),
            activity_infra_errors_atomic: AtomicU64::new(0),
            activity_config_errors_atomic: AtomicU64::new(0),
            activity_poison_atomic: AtomicU64::new(0),
            orch_dispatcher_items_fetched_atomic: AtomicU64::new(0),
            worker_dispatcher_items_fetched_atomic: AtomicU64::new(0),
            orch_continue_as_new_atomic: AtomicU64::new(0),
            suborchestration_calls_atomic: AtomicU64::new(0),
            provider_errors_atomic: AtomicU64::new(0),
            orch_queue_depth_atomic: Arc::new(AtomicU64::new(0)),
            worker_queue_depth_atomic: Arc::new(AtomicU64::new(0)),
            active_orchestrations_atomic: Arc::new(AtomicI64::new(0)),
        })
    }

    /// Shutdown the metrics provider gracefully.
    ///
    /// With the facade approach, there's nothing to shutdown - the global recorder
    /// (if any) is managed by the application.
    ///
    /// # Errors
    ///
    /// Currently infallible, but returns Result for API compatibility.
    pub async fn shutdown(self) -> Result<(), String> {
        Ok(())
    }

    // ========================================================================
    // Orchestration lifecycle methods
    // ========================================================================

    #[inline]
    pub fn record_orchestration_start(&self, orchestration_name: &str, version: &str, initiated_by: &str) {
        counter!(
            "duroxide_orchestration_starts_total",
            "orchestration_name" => orchestration_name.to_string(),
            "version" => version.to_string(),
            "initiated_by" => initiated_by.to_string(),
        )
        .increment(1);

        self.orch_starts_atomic.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_orchestration_completion(
        &self,
        orchestration_name: &str,
        version: &str,
        status: &str,
        duration_seconds: f64,
        turn_count: u64,
        history_events: u64,
    ) {
        let turn_bucket = match turn_count {
            1..=5 => "1-5",
            6..=10 => "6-10",
            11..=50 => "11-50",
            _ => "50+",
        };

        counter!(
            "duroxide_orchestration_completions_total",
            "orchestration_name" => orchestration_name.to_string(),
            "version" => version.to_string(),
            "status" => status.to_string(),
            "final_turn_count" => turn_bucket.to_string(),
        )
        .increment(1);

        histogram!(
            "duroxide_orchestration_duration_seconds",
            "orchestration_name" => orchestration_name.to_string(),
            "version" => version.to_string(),
            "status" => status.to_string(),
        )
        .record(duration_seconds);

        histogram!(
            "duroxide_orchestration_turns",
            "orchestration_name" => orchestration_name.to_string(),
        )
        .record(turn_count as f64);

        histogram!(
            "duroxide_orchestration_history_size",
            "orchestration_name" => orchestration_name.to_string(),
        )
        .record(history_events as f64);

        self.orch_completions_atomic.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_orchestration_failure(
        &self,
        orchestration_name: &str,
        version: &str,
        error_type: &str,
        error_category: &str,
    ) {
        counter!(
            "duroxide_orchestration_failures_total",
            "orchestration_name" => orchestration_name.to_string(),
            "version" => version.to_string(),
            "error_type" => error_type.to_string(),
            "error_category" => error_category.to_string(),
        )
        .increment(1);

        self.orch_failures_atomic.fetch_add(1, Ordering::Relaxed);

        match error_type {
            "app_error" => {
                self.orch_application_errors_atomic.fetch_add(1, Ordering::Relaxed);
            }
            "infrastructure_error" => {
                self.orch_infrastructure_errors_atomic.fetch_add(1, Ordering::Relaxed);
                counter!(
                    "duroxide_orchestration_infrastructure_errors_total",
                    "orchestration_name" => orchestration_name.to_string(),
                    "error_category" => error_category.to_string(),
                )
                .increment(1);
            }
            "config_error" => {
                self.orch_configuration_errors_atomic.fetch_add(1, Ordering::Relaxed);
                counter!(
                    "duroxide_orchestration_configuration_errors_total",
                    "orchestration_name" => orchestration_name.to_string(),
                    "error_category" => error_category.to_string(),
                )
                .increment(1);
            }
            _ => {}
        }
    }

    #[inline]
    pub fn record_orchestration_application_error(&self) {
        counter!("duroxide_orchestration_failures_total", "error_type" => "app_error").increment(1);
        self.orch_failures_atomic.fetch_add(1, Ordering::Relaxed);
        self.orch_application_errors_atomic.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_orchestration_infrastructure_error(&self) {
        counter!("duroxide_orchestration_failures_total", "error_type" => "infrastructure_error").increment(1);
        counter!("duroxide_orchestration_infrastructure_errors_total").increment(1);
        self.orch_failures_atomic.fetch_add(1, Ordering::Relaxed);
        self.orch_infrastructure_errors_atomic.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_orchestration_configuration_error(&self) {
        counter!("duroxide_orchestration_failures_total", "error_type" => "config_error").increment(1);
        counter!("duroxide_orchestration_configuration_errors_total").increment(1);
        self.orch_failures_atomic.fetch_add(1, Ordering::Relaxed);
        self.orch_configuration_errors_atomic.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_continue_as_new(&self, orchestration_name: &str, execution_id: u64) {
        counter!(
            "duroxide_orchestration_continue_as_new_total",
            "orchestration_name" => orchestration_name.to_string(),
            "execution_id" => execution_id.to_string(),
        )
        .increment(1);

        self.orch_continue_as_new_atomic.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================================
    // Activity execution methods
    // ========================================================================

    #[inline]
    pub fn record_activity_execution(
        &self,
        activity_name: &str,
        outcome: &str,
        duration_seconds: f64,
        retry_attempt: u32,
        tag: Option<&str>,
    ) {
        let retry_label = match retry_attempt {
            0 => "0",
            1 => "1",
            2 => "2",
            _ => "3+",
        };

        let tag_label = tag.unwrap_or("default");

        counter!(
            "duroxide_activity_executions_total",
            "activity_name" => activity_name.to_string(),
            "outcome" => outcome.to_string(),
            "retry_attempt" => retry_label.to_string(),
            "tag" => tag_label.to_string(),
        )
        .increment(1);

        histogram!(
            "duroxide_activity_duration_seconds",
            "activity_name" => activity_name.to_string(),
            "outcome" => outcome.to_string(),
            "tag" => tag_label.to_string(),
        )
        .record(duration_seconds);

        match outcome {
            "success" => {
                self.activity_success_atomic.fetch_add(1, Ordering::Relaxed);
            }
            "app_error" => {
                self.activity_app_errors_atomic.fetch_add(1, Ordering::Relaxed);
            }
            "infra_error" => {
                self.activity_infra_errors_atomic.fetch_add(1, Ordering::Relaxed);
                counter!(
                    "duroxide_activity_infrastructure_errors_total",
                    "activity_name" => activity_name.to_string(),
                )
                .increment(1);
            }
            "config_error" => {
                self.activity_config_errors_atomic.fetch_add(1, Ordering::Relaxed);
                counter!(
                    "duroxide_activity_configuration_errors_total",
                    "activity_name" => activity_name.to_string(),
                )
                .increment(1);
            }
            _ => {}
        }
    }

    #[inline]
    pub fn record_activity_success(&self) {
        counter!("duroxide_activity_executions_total", "outcome" => "success").increment(1);
        self.activity_success_atomic.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_activity_app_error(&self) {
        counter!("duroxide_activity_executions_total", "outcome" => "app_error").increment(1);
        self.activity_app_errors_atomic.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_activity_infra_error(&self) {
        counter!("duroxide_activity_executions_total", "outcome" => "infra_error").increment(1);
        counter!("duroxide_activity_infrastructure_errors_total").increment(1);
        self.activity_infra_errors_atomic.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_activity_config_error(&self) {
        counter!("duroxide_activity_executions_total", "outcome" => "config_error").increment(1);
        counter!("duroxide_activity_configuration_errors_total").increment(1);
        self.activity_config_errors_atomic.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_orchestration_poison(&self) {
        self.orch_poison_atomic.fetch_add(1, Ordering::Relaxed);
        counter!("duroxide_orchestration_poison_total").increment(1);
    }

    #[inline]
    pub fn record_activity_poison(&self) {
        self.activity_poison_atomic.fetch_add(1, Ordering::Relaxed);
        counter!("duroxide_activity_poison_total").increment(1);
    }

    // ========================================================================
    // Sub-orchestration methods
    // ========================================================================

    #[inline]
    pub fn record_suborchestration_call(&self, parent_orchestration: &str, child_orchestration: &str, outcome: &str) {
        counter!(
            "duroxide_suborchestration_calls_total",
            "parent_orchestration" => parent_orchestration.to_string(),
            "child_orchestration" => child_orchestration.to_string(),
            "outcome" => outcome.to_string(),
        )
        .increment(1);

        self.suborchestration_calls_atomic.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_suborchestration_duration(
        &self,
        parent_orchestration: &str,
        child_orchestration: &str,
        duration_seconds: f64,
        outcome: &str,
    ) {
        histogram!(
            "duroxide_suborchestration_duration_seconds",
            "parent_orchestration" => parent_orchestration.to_string(),
            "child_orchestration" => child_orchestration.to_string(),
            "outcome" => outcome.to_string(),
        )
        .record(duration_seconds);
    }

    // ========================================================================
    // Provider operation methods
    // ========================================================================

    #[inline]
    pub fn record_provider_operation(&self, operation: &str, duration_seconds: f64, status: &str) {
        histogram!(
            "duroxide_provider_operation_duration_seconds",
            "operation" => operation.to_string(),
            "status" => status.to_string(),
        )
        .record(duration_seconds);
    }

    #[inline]
    pub fn record_provider_error(&self, operation: &str, error_type: &str) {
        counter!(
            "duroxide_provider_errors_total",
            "operation" => operation.to_string(),
            "error_type" => error_type.to_string(),
        )
        .increment(1);

        self.provider_errors_atomic.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================================
    // Dispatcher metrics
    // ========================================================================

    #[inline]
    pub fn record_orch_dispatcher_items_fetched(&self, count: u64) {
        counter!("duroxide_orchestration_dispatcher_items_fetched").increment(count);
        self.orch_dispatcher_items_fetched_atomic
            .fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_orch_dispatcher_processing_duration(&self, duration_ms: u64) {
        histogram!("duroxide_orchestration_dispatcher_processing_duration_ms").record(duration_ms as f64);
    }

    #[inline]
    pub fn record_worker_dispatcher_items_fetched(&self, count: u64) {
        counter!("duroxide_worker_dispatcher_items_fetched").increment(count);
        self.worker_dispatcher_items_fetched_atomic
            .fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_worker_dispatcher_execution_duration(&self, duration_ms: u64) {
        histogram!("duroxide_worker_dispatcher_execution_duration_ms").record(duration_ms as f64);
    }

    // ========================================================================
    // Queue depth management (stateful gauges)
    // ========================================================================

    #[inline]
    pub fn update_queue_depths(&self, orch_depth: u64, worker_depth: u64) {
        self.orch_queue_depth_atomic.store(orch_depth, Ordering::Relaxed);
        self.worker_queue_depth_atomic.store(worker_depth, Ordering::Relaxed);

        // Update gauges
        gauge!("duroxide_orchestrator_queue_depth").set(orch_depth as f64);
        gauge!("duroxide_worker_queue_depth").set(worker_depth as f64);
    }

    #[inline]
    pub fn get_queue_depths(&self) -> (u64, u64) {
        (
            self.orch_queue_depth_atomic.load(Ordering::Relaxed),
            self.worker_queue_depth_atomic.load(Ordering::Relaxed),
        )
    }

    // ========================================================================
    // Active orchestrations tracking (stateful gauge)
    // ========================================================================

    #[inline]
    pub fn increment_active_orchestrations(&self) {
        let count = self.active_orchestrations_atomic.fetch_add(1, Ordering::Relaxed) + 1;
        gauge!("duroxide_active_orchestrations").set(count as f64);
    }

    #[inline]
    pub fn decrement_active_orchestrations(&self) {
        let count = self.active_orchestrations_atomic.fetch_sub(1, Ordering::Relaxed) - 1;
        gauge!("duroxide_active_orchestrations").set(count as f64);
    }

    #[inline]
    pub fn set_active_orchestrations(&self, count: i64) {
        self.active_orchestrations_atomic.store(count, Ordering::Relaxed);
        gauge!("duroxide_active_orchestrations").set(count as f64);
    }

    #[inline]
    pub fn get_active_orchestrations(&self) -> i64 {
        self.active_orchestrations_atomic.load(Ordering::Relaxed)
    }

    // ========================================================================
    // Snapshot for testing
    // ========================================================================

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            orch_starts: self.orch_starts_atomic.load(Ordering::Relaxed),
            orch_completions: self.orch_completions_atomic.load(Ordering::Relaxed),
            orch_failures: self.orch_failures_atomic.load(Ordering::Relaxed),
            orch_application_errors: self.orch_application_errors_atomic.load(Ordering::Relaxed),
            orch_infrastructure_errors: self.orch_infrastructure_errors_atomic.load(Ordering::Relaxed),
            orch_configuration_errors: self.orch_configuration_errors_atomic.load(Ordering::Relaxed),
            orch_poison: self.orch_poison_atomic.load(Ordering::Relaxed),
            activity_success: self.activity_success_atomic.load(Ordering::Relaxed),
            activity_app_errors: self.activity_app_errors_atomic.load(Ordering::Relaxed),
            activity_infra_errors: self.activity_infra_errors_atomic.load(Ordering::Relaxed),
            activity_config_errors: self.activity_config_errors_atomic.load(Ordering::Relaxed),
            activity_poison: self.activity_poison_atomic.load(Ordering::Relaxed),
            orch_dispatcher_items_fetched: self.orch_dispatcher_items_fetched_atomic.load(Ordering::Relaxed),
            worker_dispatcher_items_fetched: self.worker_dispatcher_items_fetched_atomic.load(Ordering::Relaxed),
            orch_continue_as_new: self.orch_continue_as_new_atomic.load(Ordering::Relaxed),
            suborchestration_calls: self.suborchestration_calls_atomic.load(Ordering::Relaxed),
            provider_errors: self.provider_errors_atomic.load(Ordering::Relaxed),
        }
    }
}

/// Initialize logging subsystem
///
/// # Errors
///
/// Returns an error if logging initialization fails.
pub fn init_logging(config: &ObservabilityConfig) -> Result<(), String> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_filter_expression(&config.log_level)));

    match config.log_format {
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().json())
                .try_init()
                .map_err(|e| format!("Failed to initialize JSON logging: {e}"))?;
        }
        LogFormat::Pretty => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .try_init()
                .map_err(|e| format!("Failed to initialize pretty logging: {e}"))?;
        }
        LogFormat::Compact => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().compact())
                .try_init()
                .map_err(|e| format!("Failed to initialize compact logging: {e}"))?;
        }
    }

    Ok(())
}

/// Observability handle that manages metrics and logging lifecycle
pub struct ObservabilityHandle {
    metrics_provider: Arc<MetricsProvider>,
}

impl ObservabilityHandle {
    /// Initialize observability with the given configuration
    ///
    /// Metrics are always available via the `metrics` facade. If no global recorder
    /// is installed by the application, metric calls are zero-cost no-ops.
    ///
    /// # Errors
    ///
    /// Returns an error if metrics initialization fails.
    pub fn init(config: &ObservabilityConfig) -> Result<Self, String> {
        // Initialize logging first, but tolerate failures (e.g., global subscriber already set)
        if let Err(_err) = init_logging(config) {
            // Silently ignore — this happens when multiple runtimes share a process
        }

        // Always create metrics provider (facade is zero-cost if no recorder installed)
        let metrics_provider = Arc::new(MetricsProvider::new(config)?);

        Ok(Self { metrics_provider })
    }

    /// Get the metrics provider
    #[inline]
    pub fn metrics_provider(&self) -> &Arc<MetricsProvider> {
        &self.metrics_provider
    }

    /// Shutdown observability gracefully
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown fails.
    pub async fn shutdown(self) -> Result<(), String> {
        // Take ownership out of Arc if we're the last reference
        if let Ok(provider) = Arc::try_unwrap(self.metrics_provider) {
            provider.shutdown().await?;
        }
        Ok(())
    }

    /// Get a snapshot of metrics for testing
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics_provider.snapshot()
    }
}

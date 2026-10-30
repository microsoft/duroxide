# Observability Guide

This guide covers how to enable and use observability features in duroxide for production monitoring and debugging.

## Overview

Duroxide provides production-grade observability through:
- **Structured Logging**: Correlated logs with instance_id, execution_id, orchestration_name, and more
- **Metrics Facade**: Counters, histograms, and gauges via the `metrics` crate - install your preferred exporter
- **Log Analytics Integration**: Ready-to-use patterns for Elasticsearch, Loki, CloudWatch, and Azure Monitor

## Quick Start

### Basic Structured Logging (Text to stdout)

The simplest setup uses the Compact format:

```rust
use duroxide::runtime::{RuntimeOptions, ObservabilityConfig, LogFormat};

let options = RuntimeOptions {
    observability: ObservabilityConfig {
        log_format: LogFormat::Compact,
        log_level: "info".to_string(),
        ..Default::default()
    },
    ..Default::default()
};

let rt = Runtime::start_with_options(store, activities, orchestrations, options).await;
```

**Output Format**:
```
2025-11-01T17:22:04.494Z INFO duroxide::runtime Orchestration started instance_id=order-123 execution_id=1 orchestration_name=ProcessOrder worker_id=orch-0-cd54
2025-11-01T17:22:04.596Z INFO duroxide::runtime Activity started instance_id=order-123 execution_id=1 activity_name=ValidatePayment activity_id=3 worker_id=work-1-cd54
2025-11-01T17:22:04.698Z INFO duroxide::runtime Activity completed instance_id=order-123 execution_id=1 activity_name=ValidatePayment worker_id=work-1-cd54 outcome="success" duration_ms=102
2025-11-01T17:22:04.806Z INFO duroxide::orchestration Payment validated successfully instance_id=order-123 execution_id=1 orchestration_name=ProcessOrder worker_id=orch-0-cd54
2025-11-01T17:22:05.010Z INFO duroxide::runtime Orchestration completed instance_id=order-123 execution_id=1 worker_id=orch-0-cd54 history_events=9
```

Session-related log events include:
```
... Session claimed/refreshed on fetch session_id=sess-abc owner_id=my-pod
... Session locks renewed sessions_renewed=3
... Session capacity exceeded after fetch (race), abandoning work item session_id=sess-xyz
```

Format: `timestamp level module message field1=value1 field2=value2 ...`

All correlation fields are included directly on each log line (flat structure, no span nesting).

## Log Formats

### Compact Format

Best for development and debugging with minimal noise:

```rust
ObservabilityConfig {
    log_format: LogFormat::Compact,
    log_level: "info".to_string(),
    ..Default::default()
}
```

Output: `timestamp level module [instance_id] [worker_id] message`

### Pretty Format

Full structured fields for detailed debugging:

```rust
ObservabilityConfig {
    log_format: LogFormat::Pretty,
    log_level: "debug".to_string(),
    ..Default::default()
}
```

Output includes all fields as `key=value` pairs:
```
2024-10-30T10:15:23.456Z INFO duroxide::runtime instance_id=order-123 execution_id=1 orchestration_name=ProcessOrder version=1.0.0 worker_id=orch-0-a3f9: Orchestration started
```

### JSON Format

For log aggregators and analytics platforms:

```rust
ObservabilityConfig {
    log_format: LogFormat::Json,
    log_level: "info".to_string(),
    ..Default::default()
}
```

Output: One JSON object per line with all structured fields

## Correlation Fields

Every log entry includes these fields for correlation:

- **instance_id** - Unique orchestration instance identifier
- **execution_id** - Execution number (1, 2, 3... for ContinueAsNew)
- **orchestration_name** - Name of the orchestration
- **orchestration_version** - Semantic version
- **activity_name** - Activity name (in activity context)
- **activity_id** - Event ID of the activity
- **worker_id** - Dispatcher worker ID (format: `work-{index}-{runtime_id}` or `orch-{index}-{runtime_id}`)
- **timestamp** - ISO8601 timestamp

### Example: Tracing an Orchestration

All logs for instance `order-123`:
```bash
# In Elasticsearch
instance_id:"order-123"

# In CloudWatch Insights
fields @timestamp, message | filter instance_id = "order-123" | sort @timestamp

# In Loki (LogQL)
{job="duroxide"} | json | instance_id="order-123"
```

## User Orchestration Logging

Use `ctx.trace_*()` methods in your orchestrations for replay-safe logging:

```rust
async fn process_order(ctx: OrchestrationContext, order: String) -> Result<String, String> {
    ctx.trace_info("Processing order started");
    
    let validated = ctx.schedule_activity("ValidateOrder", order.clone())
        .await?;
    
    ctx.trace_info(format!("Validation result: {}", validated));
    
    if validated == "invalid" {
        ctx.trace_error("Order validation failed");
        return Err("validation_failed".to_string());
    }
    
    ctx.trace_info("Order processing complete");
    Ok("processed".to_string())
}
```

**Output**:
```
2024-10-30T10:15:23.690Z INFO duroxide::orchestration [order-123] Processing order started
2024-10-30T10:15:23.750Z INFO duroxide::orchestration [order-123] Validation result: valid
2024-10-30T10:15:23.890Z INFO duroxide::orchestration [order-123] Order processing complete
```

**Key Points**:
- All `ctx.trace_*()` calls are deterministic (replay-safe)
- Context fields automatically included (instance_id, execution_id, etc.)
- Only logged on first execution (not during replay)
- Creates SystemCall events in history for determinism

## Metrics

Duroxide emits metrics via the [`metrics`](https://crates.io/crates/metrics) facade crate. To export metrics, install a recorder before starting the runtime:

```rust
// Option 1: Prometheus (direct scraping)
metrics_exporter_prometheus::PrometheusBuilder::new()
    .with_http_listener(([0, 0, 0, 0], 9090))
    .install()?;

// Option 2: OpenTelemetry (via metrics-exporter-opentelemetry)
// metrics_exporter_opentelemetry::Recorder::builder("my-service").install_global()?;

// Option 3: None - metrics become zero-cost no-ops (default)
```

### Available Metrics

All metrics follow Prometheus naming conventions with `_total` for counters and `_seconds` for time measurements.

## Metrics Summary Table

| Metric Name | Type | Category | Labels | Purpose |
|-------------|------|----------|--------|---------|
| **Orchestration Lifecycle** |
| `duroxide_orchestration_starts_total` | Counter | Orchestration | orchestration_name, version, initiated_by | Track which orchestrations start |
| `duroxide_orchestration_completions_total` | Counter | Orchestration | orchestration_name, version, status, final_turn_count | Success rate, completion tracking |
| `duroxide_orchestration_failures_total` | Counter | Orchestration | orchestration_name, version, error_type, error_category | Failure analysis |
| `duroxide_orchestration_duration_seconds` | Histogram | Orchestration | orchestration_name, version, status | E2E duration, p95/p99 latency |
| `duroxide_orchestration_history_size` | Histogram | Orchestration | orchestration_name | Detect history growth issues |
| `duroxide_orchestration_turns` | Histogram | Orchestration | orchestration_name | Identify multi-turn orchestrations |
| `duroxide_orchestration_continue_as_new_total` | Counter | Orchestration | orchestration_name, execution_id | Track CAN operations |
| `duroxide_active_orchestrations` | Gauge | Orchestration | _(none)_ | **Current** active count |
| **Activity Execution** |
| `duroxide_activity_executions_total` | Counter | Activity | activity_name, outcome, retry_attempt | Track success/failure rates |
| `duroxide_activity_admission_deferrals_total` | Counter | Activity | activity_name, tag | Track committed resource-admission deferrals |
| `duroxide_activity_duration_seconds` | Histogram | Activity | activity_name, outcome | Activity latency p95/p99 |
| `duroxide_activity_errors_total` | Counter | Activity | activity_name, error_type, retryable | Detailed error tracking |
| **Sub-Orchestration** |
| `duroxide_suborchestration_calls_total` | Counter | Sub-Orch | parent_orchestration, child_orchestration, outcome | Track sub-orch calls |
| `duroxide_suborchestration_duration_seconds` | Histogram | Sub-Orch | parent_orchestration, child_orchestration, outcome | Sub-orch latency |
| **Provider (Storage)** |
| `duroxide_provider_operation_duration_seconds` | Histogram | Provider | operation, status | Database operation latency |
| `duroxide_provider_errors_total` | Counter | Provider | operation, error_type | Storage layer errors |
| **Client Operations** |
| `duroxide_client_orchestration_starts_total` | Counter | Client | orchestration_name | Client-initiated starts |
| `duroxide_client_external_events_raised_total` | Counter | Client | event_name | External events raised |
| `duroxide_client_cancellations_total` | Counter | Client | _(none)_ | Cancellation requests |
| `duroxide_client_wait_duration_seconds` | Histogram | Client | _(none)_ | Client wait latency |
| **Internal (Dispatcher)** |
| `duroxide.orchestration.dispatcher.items_fetched` | Counter | Internal | _(none)_ | Items fetched by orch dispatcher |
| `duroxide.orchestration.dispatcher.processing_duration_ms` | Histogram | Internal | _(none)_ | Orch processing time |
| `duroxide.worker.dispatcher.items_fetched` | Counter | Internal | _(none)_ | Items fetched by worker dispatcher |
| `duroxide.worker.dispatcher.execution_duration_ms` | Histogram | Internal | _(none)_ | Worker execution time |

**Note**: Internal dispatcher metrics use dot notation and milliseconds (not exported to Prometheus by default).

#### Orchestration Lifecycle Metrics

**`duroxide_orchestration_starts_total`** (Counter)
- **Description**: Total orchestrations started
- **Labels**: `orchestration_name`, `version`, `initiated_by` (client|suborchestration|continueAsNew)
- **Use**: Track which orchestrations are running, identify version distribution

**`duroxide_orchestration_completions_total`** (Counter)  
- **Description**: Orchestrations that completed (successfully or failed)
- **Labels**: `orchestration_name`, `version`, `status` (success|failed|cancelled), `final_turn_count` (1-5|6-10|11-50|50+)
- **Use**: Success rate tracking, identify orchestrations with high turn counts

**`duroxide_orchestration_failures_total`** (Counter)
- **Description**: Orchestration failures with detailed error classification
- **Labels**: `orchestration_name`, `version`, `error_type` (app_error|infrastructure_error|config_error), `error_category`
- **Use**: Root cause analysis, distinguish infrastructure vs application errors

**`duroxide_orchestration_duration_seconds`** (Histogram)
- **Description**: End-to-end orchestration execution time
- **Labels**: `orchestration_name`, `version`, `status`
- **Buckets**: `[0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 600, 1800, 3600]` seconds
- **Use**: Identify slow orchestrations, track p50/p95/p99 latency

**`duroxide_orchestration_history_size`** (Histogram)
- **Description**: History event count at orchestration completion
- **Labels**: `orchestration_name`
- **Buckets**: `[10, 50, 100, 500, 1000, 5000, 10000]` events
- **Use**: Identify orchestrations with unbounded history growth

**`duroxide_orchestration_turns`** (Histogram)
- **Description**: Number of turns to orchestration completion
- **Labels**: `orchestration_name`
- **Buckets**: `[1, 2, 5, 10, 20, 50, 100, 200, 500]`
- **Use**: Detect orchestrations requiring many turns (potential optimization targets)

**`duroxide_orchestration_continue_as_new_total`** (Counter)
- **Description**: Continue-as-new operations performed
- **Labels**: `orchestration_name`, `execution_id`
- **Use**: Verify continue-as-new is working, identify long-running actors

**`duroxide_active_orchestrations`** (Gauge) 🆕
- **Description**: Current number of orchestration instances that are actively running (not completed/failed)
- **Value**: Count of orchestrations currently in progress
- **Use**: Track concurrent orchestrations, detect leaks, capacity planning
- **Critical**: This is a GAUGE (can increase/decrease), unlike counters that only increase
- **Note**: Continue-as-new does NOT change this count (orchestration stays active)

#### Activity Execution Metrics

**`duroxide_activity_admission_deferrals_total`** (Counter)
- **Description**: Admission deferrals whose abandon returned success, including a successful fallback after an abandon error
- **Labels**: `activity_name`, `tag`
- **Use**: Identify resource-constrained activities without counting deferrals as executions

The "Activity started" log appears only after admission succeeds. Admission
rejections, panics, and zero retry delays count as `app_error`; storage failures
while deferring count as `infra_error`, even if the fallback abandon succeeds.
Uncommitted deferrals do not increment the deferral counter and may consume a
fetch attempt on recovery. A deferral that commits before shutdown or a lost
response can restore its attempt without incrementing this counter. The counter
is not a durable audit of all storage commits. Activity metadata-read failures
before admission count as infrastructure errors, not deferrals.

**`duroxide_activity_executions_total`** (Counter)
- **Description**: Activity execution attempts
- **Labels**: `activity_name`, `outcome` (success|app_error|infra_error|config_error), `retry_attempt` (0|1|2|3+)
- **Use**: Identify flaky activities, track retry rates

**`duroxide_activity_duration_seconds`** (Histogram)
- **Description**: Activity execution time (wall clock)
- **Labels**: `activity_name`, `outcome`
- **Buckets**: `[0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300]` seconds
- **Use**: Identify slow activities, set appropriate timeouts

**`duroxide_activity_errors_total`** (Counter)
- **Description**: Detailed activity error tracking
- **Labels**: `activity_name`, `error_type` (app_error|infrastructure_error|config_error), `retryable` (true|false)
- **Use**: Distinguish transient vs permanent errors

#### Sub-Orchestration Metrics

**`duroxide_suborchestration_calls_total`** (Counter)
- **Description**: Sub-orchestration invocations
- **Labels**: `parent_orchestration`, `child_orchestration`, `outcome`
- **Use**: Understand orchestration composition, trace call graphs

**`duroxide_suborchestration_duration_seconds`** (Histogram)
- **Description**: Sub-orchestration execution time
- **Labels**: `parent_orchestration`, `child_orchestration`, `outcome`
- **Buckets**: `[0.1, 0.5, 1, 5, 10, 30, 60, 300, 600]` seconds

#### Provider Metrics

**`duroxide_provider_operation_duration_seconds`** (Histogram)
- **Description**: Database operation latency
- **Labels**: `operation` (fetch|ack|save_event|create_instance), `status` (success|error)
- **Buckets**: `[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5]` seconds
- **Use**: Database performance monitoring, identify slow queries

**`duroxide_provider_errors_total`** (Counter)
- **Description**: Provider/storage layer errors
- **Labels**: `operation`, `error_type` (timeout|connection|deadlock|corruption)
- **Use**: Database health monitoring, alerting

#### Client Metrics

**`duroxide_client_orchestration_starts_total`** (Counter)
- **Description**: Orchestrations started via client
- **Labels**: `orchestration_name`

**`duroxide_client_external_events_raised_total`** (Counter)
- **Description**: External events raised via client
- **Labels**: `event_name`

**`duroxide_client_cancellations_total`** (Counter)
- **Description**: Orchestration cancellations via client

**`duroxide_client_wait_duration_seconds`** (Histogram)
- **Description**: Client wait operation duration
- **Buckets**: `[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120]` seconds

## Error Classification

Duroxide categorizes errors into three types for better observability:

### Infrastructure Errors
Provider failures, data corruption, network issues:
- Transaction failures
- Lock timeouts
- Serialization errors
- Database connectivity issues

**Metrics**: `*.infrastructure_errors` counters

### Configuration Errors
Deployment/registration issues:
- Unregistered orchestrations/activities
- Missing versions
- Nondeterminism detected

**Metrics**: `*.configuration_errors` counters

### Application Errors
Business logic failures:
- Activity returns `Err(...)`
- User-initiated cancellations
- Business validation failures

**Metrics**: Activity/orchestration failure counters with `app_error` outcome

## Log Analytics Integration

### 1. Elasticsearch / OpenSearch

**Setup**:
```yaml
# otel-collector-config.yaml
exporters:
  elasticsearch:
    endpoints: ["http://elasticsearch:9200"]
    logs_index: duroxide-logs
```

**Queries**:
```
# All logs for an orchestration
instance_id:"order-123"

# Failed activities
activity_name:* AND outcome:"app_error"

# Nondeterminism errors
error_type:"nondeterminism" AND level:"ERROR"
```

### 2. Grafana Loki

**Setup**:
```yaml
# promtail-config.yaml
scrape_configs:
  - job_name: duroxide
    static_configs:
      - targets:
          - localhost
        labels:
          job: duroxide-worker
```

**LogQL**:
```logql
# Instance timeline
{job="duroxide"} | json | instance_id="order-123"

# Error rate
rate({job="duroxide", level="error"}[5m]) by (orchestration_name)
```

### 3. AWS CloudWatch Logs

**Setup**: Use CloudWatch Logs agent or OTLP exporter

**CloudWatch Insights Queries**:
```sql
# Orchestration trace
fields @timestamp, instance_id, orchestration_name, message
| filter instance_id = "order-123"
| sort @timestamp asc

# Activity success rate
fields activity_name, outcome
| filter activity_name != ""
| stats count() by activity_name, outcome
```

### 4. Azure Monitor / Application Insights

**Setup**: Use Azure Monitor OTLP exporter

**KQL Queries**:
```kql
// Full orchestration timeline
traces
| where customDimensions.instance_id == "order-123"
| project timestamp, severityLevel, message
| order by timestamp asc
```

## Debugging Workflows

### Trace a Single Orchestration

1. Note the instance_id when starting: `order-123`
2. Query logs by instance_id
3. Filter by execution_id if using ContinueAsNew
4. Trace activity execution within the orchestration

### Find Slow Activities

Query activity duration metrics:
```promql
# Prometheus query - p99 latency
histogram_quantile(0.99, 
  rate(duroxide_activity_duration_seconds_bucket[5m])
) by (activity_name)

# Activity success rate
rate(duroxide_activity_executions_total{outcome="success"}[5m])
/
rate(duroxide_activity_executions_total[5m])
by (activity_name)
```

### Identify Nondeterminism Issues

Search logs for configuration errors:
```
error_type:"nondeterminism" AND level:"ERROR"
```

These indicate scheduling order mismatches that need code fixes.

## Performance Impact

### Without a Metrics Recorder (default)
- Zero overhead from metrics collection (no-op fast path)
- Minimal logging overhead (only structured field extraction)
- Recommended for development and when metrics export is not needed

### With a Metrics Recorder Installed
- ~2-5% overhead for metrics collection
- Lock-free atomic operations
- Background export (no blocking)
- Recommended for production monitoring

### Log Format Impact
- **Compact**: Lowest overhead
- **Pretty**: Slight overhead for formatting
- **JSON**: Minimal additional overhead

## Environment Variables

Override configuration via environment variables:

```bash
# Logging level
RUST_LOG=info,duroxide=debug

# OpenTelemetry (if using OTLP exporter in your application)
# These are consumed by opentelemetry crates, not duroxide directly
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317
OTEL_SERVICE_NAME=duroxide-worker
OTEL_SERVICE_VERSION=1.0.0
```

## Example Prometheus Queries

### Orchestration Success Rate
```promql
rate(duroxide_orchestration_completions_total{status="success"}[5m])
/
rate(duroxide_orchestration_completions_total[5m])
by (orchestration_name)
```

### P95 Orchestration Duration
```promql
histogram_quantile(0.95,
  rate(duroxide_orchestration_duration_seconds_bucket[5m])
) by (orchestration_name)
```

### Activity Error Rate
```promql
rate(duroxide_activity_executions_total{outcome!="success"}[5m])
by (activity_name)
```

### Top 5 Slowest Activities
```promql
topk(5,
  histogram_quantile(0.95,
    rate(duroxide_activity_duration_seconds_bucket[5m])
  ) by (activity_name)
)
```

### Infrastructure Errors (Actionable)
```promql
rate(duroxide_orchestration_failures_total{error_type="infrastructure_error"}[5m])
by (orchestration_name)
```

### Orchestrations Requiring Many Turns
```promql
# Using completion labels (bucketed ranges)
sum(duroxide_orchestration_completions_total{final_turn_count="50+"})
by (orchestration_name)

# Using turns histogram (p95 turn count)
histogram_quantile(0.95,
  rate(duroxide_orchestration_turns_bucket[5m])
) by (orchestration_name)

# Orchestrations that took >100 turns
sum(rate(duroxide_orchestration_turns_bucket{le="+Inf"}[5m]))
-
sum(rate(duroxide_orchestration_turns_bucket{le="100"}[5m]))
```

### Currently Active Orchestrations (Gauge)
```promql
# Total active right now
duroxide_active_orchestrations

# Detect orchestration leaks (if this keeps growing, you have a leak)
increase(duroxide_active_orchestrations[1h]) > 100
```

## Best Practices

1. **Use Compact format in development** for readable logs
2. **Use JSON format in production** for log aggregation
3. **Include context in user traces**: Use descriptive messages
4. **Monitor infrastructure_errors** closely - indicates system issues
5. **Track configuration_errors** - may require deployment fixes
6. **Use instance_id** as primary correlation field
7. **Query by orchestration_name** to find patterns across instances
8. **Enable metrics in production** for performance insights
9. **Use histogram metrics** for p95/p99 latency tracking, not just averages
10. **Alert on error_type labels** to distinguish actionable errors from business logic failures

## Troubleshooting

### Logs Not Showing

Check the log level:
```rust
ObservabilityConfig {
    log_level: "debug".to_string(),  // Lower for more logs
    ..Default::default()
}
```

### Missing Context Fields

Ensure logs are within an orchestration or activity span. Framework logs automatically include context.

### Metrics Not Exporting

Verify:
1. `observability` feature flag is enabled
2. OTLP endpoint is accessible
3. Export interval allows time for collection

## Example: Full Production Configuration

```rust
use duroxide::runtime::{RuntimeOptions, ObservabilityConfig, LogFormat};

let options = RuntimeOptions {
    observability: ObservabilityConfig {
        // Metrics
        metrics_enabled: true,
        metrics_export_endpoint: Some("http://otel-collector:4317".to_string()),
        metrics_export_interval_ms: 10000,
        
        // Logging
        log_format: LogFormat::Json,
        log_level: "info".to_string(),
        
        // Service identification
        service_name: "payment-processor".to_string(),
        service_version: Some(env!("CARGO_PKG_VERSION").to_string()),
    },
    orchestration_concurrency: 4,
    worker_concurrency: 8,
    ..Default::default()
};
```

## Grafana Dashboard

A complete Grafana dashboard with 11 panels is available at:
- [examples/grafana-dashboard.json](../examples/grafana-dashboard.json)

Import this dashboard into Grafana to get:
- Orchestration success rate by name
- P95/P99 duration tracking
- Activity execution rates and latency
- Error classification
- Continue-as-new operations
- History size distribution
- Provider operation latency
- Turn count distribution

## See Also

- [Telemetry Implementation Summary](telemetry-implementation-summary.md) - Complete metrics specification
- [Provider Observability Guide](provider-observability.md) - For custom provider implementors
- [Library Observability Guide](library-observability.md) - For library developers
- [examples/with_observability.rs](../examples/with_observability.rs) - Working example
- [examples/grafana-dashboard.json](../examples/grafana-dashboard.json) - Production dashboard

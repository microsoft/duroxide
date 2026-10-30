# Duroxide Telemetry Specification for Production Operations

**Target Audience:** Duroxide Framework Developers  
**Purpose:** Improve observability metrics to enable effective production monitoring and debugging  
**Context:** Written by an administrator running toygres (PostgreSQL-as-a-Service orchestrator) built on duroxide

## Executive Summary

Current duroxide observability provides multi-dimensional metrics via the `metrics` facade crate, allowing users to choose their preferred exporter (Prometheus, OpenTelemetry, etc.).

**Current State:**
- ✅ Metrics via `metrics` facade (user chooses exporter)
- ✅ Integration with Prometheus/Grafana (via user-installed recorder)
- ✅ Rich labels (orchestration_name, activity_name, outcome, etc.)
- ✅ Duration histograms with percentiles
- ✅ Error classification visibility

**Implementation Notes:**
- Multi-dimensional metrics with rich labels
- Percentile-based latency tracking
- Fine-grained error attribution
- Queue depth and processing rate visibility
- See `docs/metrics-specification.md` for complete metric reference

---

## 1. Core Metric Requirements

### 1.1 Orchestration Lifecycle Metrics

#### `duroxide_orchestration_starts_total` (Counter)
**Description:** Total orchestrations started  
**Labels:**
- `orchestration_name` - Fully qualified name (e.g., "toygres::CreateInstance")
- `version` - Orchestration version (e.g., "1.0.0")
- `initiated_by` - How started: "client" | "suborchestration" | "continueAsNew" | "signal"

**Use Case:** Track which orchestrations are being used most, identify version distribution

**Example:**
```promql
# Orchestration start rate by type
rate(duroxide_orchestration_starts_total[5m]) by (orchestration_name)

# Version distribution
duroxide_orchestration_starts_total by (orchestration_name, version)
```

---

#### `duroxide_orchestration_completions_total` (Counter)
**Description:** Orchestrations that completed (successfully or failed)  
**Labels:**
- `orchestration_name`
- `version`
- `status` - "success" | "failed" | "cancelled"
- `final_turn_count` - Histogram bucket of turn count (e.g., "1-5", "6-10", "11-50", "50+")

**Use Case:** Success rate tracking, identify orchestrations with high turn counts

**Example:**
```promql
# Success rate
rate(duroxide_orchestration_completions_total{status="success"}[5m]) 
/ 
rate(duroxide_orchestration_completions_total[5m])

# Orchestrations requiring many turns (potential optimization targets)
duroxide_orchestration_completions_total{final_turn_count="50+"}
```

---

#### `duroxide_orchestration_duration_seconds` (Histogram)
**Description:** End-to-end orchestration execution time  
**Labels:**
- `orchestration_name`
- `version`
- `status` - "success" | "failed" | "cancelled"

**Buckets:** `[0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 600, 1800, 3600]` seconds

**Use Case:** Identify slow orchestrations, track performance over time

**Example:**
```promql
# p95 orchestration duration
histogram_quantile(0.95, 
  rate(duroxide_orchestration_duration_seconds_bucket[5m])
) by (orchestration_name)

# Orchestrations taking >5 minutes
rate(duroxide_orchestration_duration_seconds_bucket{le="300"}[5m])
```

---

#### `duroxide_orchestration_failures_total` (Counter)
**Description:** Orchestration failures with detailed error classification  
**Labels:**
- `orchestration_name`
- `version`
- `error_type` - "app_error" | "infrastructure_error" | "config_error" | "timeout" | "nondeterminism"
- `error_category` - High-level category (e.g., "database", "network", "logic", "validation")

**Use Case:** Root cause analysis, alerting on infrastructure issues

**Example:**
```promql
# Infrastructure failures (not user errors)
rate(duroxide_orchestration_failures_total{error_type="infrastructure_error"}[5m])

# Nondeterminism bugs (critical - requires code fix)
duroxide_orchestration_failures_total{error_type="nondeterminism"}
```

---

### 1.2 Activity Execution Metrics

#### `duroxide_activity_executions_total` (Counter)
**Description:** Activity execution attempts  
**Labels:**
- `activity_name` - Fully qualified name (e.g., "toygres::DeployPostgres")
- `outcome` - "success" | "app_error" | "infra_error" | "timeout" | "retry"
- `retry_attempt` - "0" (first try) | "1" | "2" | "3+"

**Use Case:** Identify flaky activities, track retry rates

**Example:**
```promql
# Activity failure rate
rate(duroxide_activity_executions_total{outcome!="success"}[5m]) 
by (activity_name)

# Activities requiring retries
rate(duroxide_activity_executions_total{retry_attempt!="0"}[5m])
```

---

#### `duroxide_activity_duration_seconds` (Histogram)
**Description:** Activity execution time (wall clock)  
**Labels:**
- `activity_name`
- `outcome`

**Buckets:** `[0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300]` seconds

**Use Case:** Identify slow activities, set appropriate timeouts

**Example:**
```promql
# p99 activity duration by outcome
histogram_quantile(0.99, 
  rate(duroxide_activity_duration_seconds_bucket[5m])
) by (activity_name, outcome)

# Activities timing out
rate(duroxide_activity_executions_total{outcome="timeout"}[5m])
```

---

#### `duroxide_activity_errors_total` (Counter)
**Description:** Detailed activity error tracking  
**Labels:**
- `activity_name`
- `error_type` - "app_error" | "infrastructure_error" | "config_error" | "timeout"
- `error_code` - Optional user-defined error code
- `retryable` - "true" | "false"

**Use Case:** Distinguish transient vs permanent errors, identify infrastructure issues

**Example:**
```promql
# Non-retryable errors (require intervention)
rate(duroxide_activity_errors_total{retryable="false"}[5m])

# Infrastructure errors by activity
rate(duroxide_activity_errors_total{error_type="infrastructure_error"}[5m])
by (activity_name)
```

---

#### `duroxide_activity_admission_deferrals_total` (Counter)

**Description:** Admission deferrals whose abandon returned success, including
a successful fallback after an abandon error.

**Labels:**
- `activity_name` - Registered activity name
- `tag` - Routing tag, or `"default"` for untagged work

**Semantics:** This counts observed successful deferrals, not handler executions.
Deferrals do not contribute activity execution counts or duration samples.
A commit with a lost response may not be counted. Abandon failures count as
`infra_error`; admission `Err`, panic, and zero delay produce terminal
`ActivityFailed` application results and count as `app_error`.

The in-process accessor is `MetricsProvider::activity_admission_deferrals()`,
without adding fields to the existing public `MetricsSnapshot`.

**Example:**
```promql
sum by (activity_name, tag) (rate(duroxide_activity_admission_deferrals_total[5m]))
```

---

### 1.3 Provider (Storage) Metrics

#### `duroxide_provider_operation_duration_seconds` (Histogram)
**Description:** Database operation latency  
**Labels:**
- `operation` - "fetch" | "ack" | "save_event" | "create_instance" | "query"
- `status` - "success" | "error"

**Buckets:** `[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5]` seconds

**Use Case:** Database performance monitoring, identify slow queries

Activity metadata is included with `operation="get_instance_info"` for the
lightweight lookup and `operation="read_with_execution"` for the history
fallback, each with `status="success"` or `"error"`. Errors also increment
`duroxide_provider_errors_total` with the same operation label.

**Example:**
```promql
# p95 database operation latency
histogram_quantile(0.95,
  rate(duroxide_provider_operation_duration_seconds_bucket[5m])
) by (operation)
```

---

#### `duroxide_provider_errors_total` (Counter)
**Description:** Provider/storage layer errors  
**Labels:**
- `operation`
- `error_type` - "timeout" | "connection" | "deadlock" | "corruption" | "constraint_violation"

**Use Case:** Database health monitoring, alerting

---

### 1.4 Runtime/Worker Metrics

#### `duroxide_worker_queue_depth` (Gauge)
**Description:** Current work items pending  
**Labels:**
- `worker_type` - "orchestration" | "activity"
- `worker_id` - Worker thread identifier

**Use Case:** Identify backlog, scaling decisions

**Example:**
```promql
# Total queue depth
sum(duroxide_worker_queue_depth) by (worker_type)

# Identify overloaded workers
duroxide_worker_queue_depth > 100
```

---

#### `duroxide_worker_processing_rate` (Gauge)
**Description:** Items processed per second per worker  
**Labels:**
- `worker_type`
- `worker_id`

**Use Case:** Worker utilization, load balancing

---

#### `duroxide_orchestration_history_size` (Histogram)
**Description:** History event count at completion  
**Labels:**
- `orchestration_name`

**Buckets:** `[10, 50, 100, 500, 1000, 5000, 10000]` events

**Use Case:** Identify orchestrations with unbounded history growth

---

### 1.5 Instance Actor / Long-Running Orchestration Metrics

#### `duroxide_orchestration_active_instances` (Gauge)
**Description:** Currently running orchestration instances  
**Labels:**
- `orchestration_name`
- `state` - "running" | "waiting" | "suspended"

**Use Case:** Track concurrent executions, resource planning

---

#### `duroxide_orchestration_continue_as_new_total` (Counter)
**Description:** Continue-as-new operations performed  
**Labels:**
- `orchestration_name`
- `execution_id` - Execution number

**Use Case:** Verify continue-as-new is working, identify long-running actors

---

### 1.6 Sub-Orchestration Metrics

#### `duroxide_suborchestration_calls_total` (Counter)
**Description:** Sub-orchestration invocations  
**Labels:**
- `parent_orchestration`
- `child_orchestration`
- `outcome` - "success" | "failed"

**Use Case:** Understand orchestration composition, trace call graphs

---

## 2. Critical Use Cases & Required Queries

### 2.1 Incident Response

**Question:** "Why are instance creations failing?"

**Required Metrics:**
```promql
# Overall failure rate
rate(duroxide_orchestration_completions_total{
  orchestration_name="CreateInstance",
  status="failed"
}[5m])

# Breakdown by error type
rate(duroxide_orchestration_failures_total{
  orchestration_name="CreateInstance"
}[5m]) by (error_type)

# Which activity is failing?
rate(duroxide_activity_errors_total{outcome="app_error"}[5m])
by (activity_name)
```

---

### 2.2 Performance Debugging

**Question:** "Why is instance creation so slow?"

**Required Metrics:**
```promql
# Overall p95 duration
histogram_quantile(0.95,
  rate(duroxide_orchestration_duration_seconds_bucket{
    orchestration_name="CreateInstance"
  }[5m])
)

# Per-activity duration breakdown
histogram_quantile(0.95,
  rate(duroxide_activity_duration_seconds_bucket[5m])
) by (activity_name)

# Database operation latency
histogram_quantile(0.95,
  rate(duroxide_provider_operation_duration_seconds_bucket[5m])
) by (operation)
```

---

### 2.3 Capacity Planning

**Question:** "How many workers do we need?"

**Required Metrics:**
```promql
# Queue depth over time
avg(duroxide_worker_queue_depth) by (worker_type)

# Processing rate
sum(rate(duroxide_activity_executions_total[5m]))

# Worker utilization
rate(duroxide_worker_active_time_seconds[5m]) 
/ 
duroxide_worker_count
```

---

### 2.4 SLA Monitoring

**Question:** "Are we meeting our 95% success rate SLA?"

**Required Metrics:**
```promql
# Success rate by orchestration
(
  rate(duroxide_orchestration_completions_total{status="success"}[5m])
  /
  rate(duroxide_orchestration_completions_total[5m])
) by (orchestration_name)

# Instances created per hour
increase(duroxide_orchestration_completions_total{
  orchestration_name="CreateInstance",
  status="success"
}[1h])
```

---

## 3. Implementation Guidelines

### 3.1 Label Cardinality Management

**Problem:** Too many labels = high cardinality = Prometheus performance issues

**Best Practices:**
1. **Avoid high-cardinality labels:**
   - ❌ `instance_id` - Unique per orchestration (thousands+)
   - ❌ `user_id` - Unique per user
   - ❌ `timestamp` - Unique per event
   
2. **Use bounded cardinality:**
   - ✅ `orchestration_name` - Limited set (10-100)
   - ✅ `activity_name` - Limited set (50-500)
   - ✅ `outcome` - Fixed set (success, failed, etc.)
   - ✅ `error_type` - Fixed set (5-10 types)

3. **Aggregate high-cardinality data:**
   - Instead of per-instance metrics, use histograms
   - Instead of per-user metrics, track totals
   - Use tracing/logs for unique identifiers

### 3.2 Metric Naming Conventions

Follow Prometheus best practices:
- Use base unit: `_seconds`, `_bytes`, `_total`
- Counter suffix: `_total`
- Histogram/Summary suffix: `_duration_seconds`, `_size_bytes`
- Gauge: no suffix

### 3.3 Histogram Bucket Design

Choose buckets that align with SLO thresholds:

**Activity Duration:**
```rust
// For short activities (API calls, database queries)
vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0]

// For long activities (instance provisioning)
vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]
```

**Orchestration Duration:**
```rust
// For typical orchestrations
vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0]
```

### 3.4 Sampling and Recording Rules

For high-throughput systems:

1. **Recording Rules** (pre-compute common queries):
```yaml
# prometheus-rules.yml
groups:
  - name: duroxide
    interval: 30s
    rules:
      - record: job:duroxide_orchestration_success_rate:rate5m
        expr: |
          rate(duroxide_orchestration_completions_total{status="success"}[5m])
          /
          rate(duroxide_orchestration_completions_total[5m])
```

2. **Sampling** (for very high frequency):
   - Sample 1 in N metrics for high-volume activities
   - Always capture errors (never sample failures)
   - Provide configuration: `DUROXIDE_METRICS_SAMPLE_RATE=0.1`

---

## 4. Configuration API Proposal

### 4.1 Rust Configuration

```rust
use duroxide::runtime::{RuntimeOptions, ObservabilityConfig, MetricsConfig};

let metrics = MetricsConfig {
    // Enable/disable specific metric categories
    orchestration_metrics: true,
    activity_metrics: true,
    provider_metrics: true,
    worker_metrics: true,
    
    // Sampling for high-volume metrics
    activity_sample_rate: 1.0,  // 1.0 = all, 0.1 = 10%
    
    // Include detailed error messages (may increase cardinality)
    include_error_messages: false,
    
    // Maximum label values to track
    max_error_types: 100,
    max_activity_names: 500,
};

let options = RuntimeOptions {
    observability: ObservabilityConfig {
        metrics_enabled: true,
        metrics_config: Some(metrics),
        ..Default::default()
    },
    ..Default::default()
};
```

### 4.2 Environment Variable Configuration

```bash
# Feature flags
DUROXIDE_METRICS_ORCHESTRATION=true
DUROXIDE_METRICS_ACTIVITY=true
DUROXIDE_METRICS_PROVIDER=true
DUROXIDE_METRICS_WORKER=true

# Sampling
DUROXIDE_METRICS_SAMPLE_RATE=1.0

# Cardinality control
DUROXIDE_METRICS_INCLUDE_ERROR_MESSAGES=false
DUROXIDE_METRICS_MAX_LABEL_VALUES=1000
```

---

## 5. Dashboard Requirements

### 5.1 Executive Dashboard
- Orchestration success rate (7-day trend)
- Current active orchestrations
- Error rate (last hour)
- p95 orchestration duration (last hour)

### 5.2 Orchestration Deep Dive
Per orchestration:
- Success/failure rate (time series)
- Duration percentiles (p50, p95, p99)
- Error breakdown (by type)
- Activity execution breakdown

### 5.3 Activity Performance
- Activity duration heatmap
- Failure rate by activity
- Retry rate by activity
- Timeout rate

### 5.4 Infrastructure Health
- Database operation latency (p95, p99)
- Database error rate
- Worker queue depth
- Worker processing rate

---

## 6. Testing & Validation

### 6.1 Load Testing Scenarios

Test metrics under:
1. **Normal load:** 10 orchestrations/sec, verify all metrics exported
2. **High load:** 100 orchestrations/sec, verify no metric loss
3. **Error spike:** Inject 50% failure rate, verify error attribution
4. **Long-running:** 1000+ turn orchestration, verify history metrics

### 6.2 Cardinality Testing

```bash
# Check metric cardinality in Prometheus
curl http://prometheus:9090/api/v1/label/__name__/values | \
  jq '.data | length'

# Should be < 10,000 total time series for good performance
```

---

## 7. Migration Path

### Phase 1: Core Labels (Breaking Change)
- Add `orchestration_name`, `activity_name`, `outcome` to existing metrics
- **Impact:** Users must update dashboards
- **Timeline:** Next minor version

### Phase 2: Histogram Metrics
- Add duration histograms for orchestrations and activities
- **Impact:** Additive, no breaking changes
- **Timeline:** Same release as Phase 1

### Phase 3: Advanced Metrics
- Add worker metrics, sub-orchestration tracking
- **Impact:** Additive
- **Timeline:** Follow-up release

---

## 8. Example: What Good Looks Like

### Before (Current State):
```
duroxide_activity_executions_total{job="toygres"} 1000
```
**Can't answer:** Which activities? Success or failure? How long did they take?

### After (Desired State):
```
duroxide_activity_executions_total{
  activity_name="DeployPostgres",
  outcome="success",
  retry_attempt="0"
} 850

duroxide_activity_executions_total{
  activity_name="DeployPostgres",
  outcome="app_error",
  retry_attempt="1"
} 50

duroxide_activity_duration_seconds_bucket{
  activity_name="DeployPostgres",
  outcome="success",
  le="5"
} 800

duroxide_activity_duration_seconds_bucket{
  activity_name="DeployPostgres",
  outcome="success",
  le="10"
} 850
```
**Can answer:** 
- 850 successful DeployPostgres (94% success rate)
- 50 retries (5.5% retry rate)
- p95 duration ~8 seconds (800 under 5s, 850 under 10s)

---

## 9. Appendix: Real-World toygres Use Cases

### Use Case 1: "Why is user X's instance stuck?"

**Needed:**
1. Query orchestrations by user_name (via logs, not metrics)
2. Metrics show: `WaitForReady` activity p99 = 120s
3. Database metrics show: `fetch_orchestration_item` p95 = 5s (slow!)
4. **Root cause:** Database under load, slowing orchestration processing

### Use Case 2: "Should we scale up workers?"

**Needed:**
1. `duroxide_worker_queue_depth` shows 500 pending items
2. `duroxide_worker_processing_rate` shows 10 items/sec per worker
3. Math: 500 / 10 = 50 seconds backlog
4. **Decision:** Add more workers or optimize slow activities

### Use Case 3: "Which activity should we optimize first?"

**Needed:**
1. Query `duroxide_activity_duration_seconds` by activity_name
2. Results:
   - `WaitForReady` p95 = 45s (highest)
   - `DeployPostgres` p95 = 8s
   - `TestConnection` p95 = 0.5s
3. **Decision:** Optimize WaitForReady (or make it asynchronous)

---

## 10. Success Criteria

This specification is successful if:

1. ✅ Administrators can answer "why is X slow?" in < 5 minutes
2. ✅ Administrators can identify failing orchestrations without reading logs
3. ✅ Dashboards provide actionable insights (not just pretty graphs)
4. ✅ Metrics support SLA monitoring and alerting
5. ✅ Cardinality stays under 10K time series for typical deployments
6. ✅ Performance overhead < 5% compared to no metrics

---

## Conclusion

The current duroxide observability foundation is solid. Adding rich labels and dimensions to metrics will transform it from "basic monitoring" to "production-grade observability." 

The key insight: **Metrics without labels are counters. Metrics with labels are answers.**

Focus on enabling these questions:
- "What is slow?"
- "What is failing?"
- "What needs scaling?"
- "What needs optimization?"

Everything else is secondary.

---

**Document Version:** 1.0  
**Last Updated:** 2025-11-22  
**Author:** Toygres Administrator (your friendly neighborhood duroxide user)  
**Feedback:** Implement this and I'll buy you a virtual coffee ☕


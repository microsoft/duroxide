// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Large payload stress test scenario.
//!
//! Tests memory consumption and history management with large event payloads.
//! This test exercises:
//! - Large inputs/outputs for activities (10KB, 50KB, 100KB)
//! - Large orchestration inputs and outputs
//! - Sub-orchestration with large payloads
//! - Moderate-length histories (~100 events)
//!
//! Designed to stress-test:
//! - Memory allocation patterns
//! - History concatenation overhead
//! - Event serialization/deserialization
//! - Provider storage efficiency

use super::core::{StressTestConfig, StressTestResult, run_stress_test};
use super::parallel_orchestrations::ProviderStressFactory;
use crate::runtime::registry::ActivityRegistry;
use crate::{ActivityContext, OrchestrationContext, OrchestrationRegistry};

/// Configuration for large payload stress tests
#[derive(Debug, Clone)]
pub struct LargePayloadConfig {
    /// Base stress test configuration
    pub base: StressTestConfig,
    /// Size of small payloads in KB (default: 10)
    pub small_payload_kb: usize,
    /// Size of medium payloads in KB (default: 50)
    pub medium_payload_kb: usize,
    /// Size of large payloads in KB (default: 100)
    pub large_payload_kb: usize,
    /// Number of activities to schedule (creates ~3x events: scheduled, completed, results)
    pub activity_count: usize,
    /// Number of sub-orchestrations (creates ~4x events each)
    pub sub_orch_count: usize,
}

impl Default for LargePayloadConfig {
    fn default() -> Self {
        Self {
            base: StressTestConfig {
                max_concurrent: 5, // Lower concurrency due to large payloads
                duration_secs: 10,
                tasks_per_instance: 1, // Not used, we have custom orchestration
                activity_delay_ms: 5,
                orch_concurrency: 1,
                worker_concurrency: 1,
                wait_timeout_secs: 120, // Higher for large payload tests
            },
            small_payload_kb: 10,
            medium_payload_kb: 50,
            large_payload_kb: 100,
            activity_count: 20, // 20 activities × 3 events = ~60 events
            sub_orch_count: 5,  // 5 sub-orch × 4 events = ~20 events
                                // Total: ~80-100 events per instance
        }
    }
}

/// Run the large payload stress test with a custom provider factory.
///
/// # Example
///
/// ```rust,ignore
/// use duroxide::provider_stress_tests::parallel_orchestrations::ProviderStressFactory;
/// use duroxide::provider_stress_tests::large_payload::run_large_payload_test;
/// use duroxide::providers::Provider;
/// use std::sync::Arc;
///
/// struct MyProviderFactory;
///
/// #[async_trait::async_trait]
/// impl ProviderStressFactory for MyProviderFactory {
///     async fn create_provider(&self) -> Arc<dyn Provider> {
///         Arc::new(MyProvider::new().await.unwrap())
///     }
/// }
///
/// #[tokio::test]
/// async fn large_payload_stress_test_my_provider() {
///     let factory = MyProviderFactory;
///     let result = run_large_payload_test(&factory).await.unwrap();
///     assert!(result.success_rate() > 99.0, "Success rate too low: {:.2}%", result.success_rate());
/// }
/// ```
///
/// # Errors
///
/// Returns an error if the stress test execution fails.
pub async fn run_large_payload_test(
    factory: &dyn ProviderStressFactory,
) -> Result<StressTestResult, Box<dyn std::error::Error>> {
    let config = LargePayloadConfig::default();
    run_large_payload_test_with_config(factory, config).await
}

/// Run the large payload stress test with a custom configuration.
///
/// # Errors
///
/// Returns an error if the stress test execution fails.
pub async fn run_large_payload_test_with_config(
    factory: &dyn ProviderStressFactory,
    config: LargePayloadConfig,
) -> Result<StressTestResult, Box<dyn std::error::Error>> {
    let provider = factory.create_provider().await;
    let activities = create_large_payload_activities(
        config.base.activity_delay_ms,
        config.small_payload_kb,
        config.medium_payload_kb,
        config.large_payload_kb,
    );
    let orchestrations = create_large_payload_orchestrations(
        config.small_payload_kb,
        config.medium_payload_kb,
        config.large_payload_kb,
        config.activity_count,
        config.sub_orch_count,
    );

    run_stress_test(config.base, provider, activities, orchestrations).await
}

/// Create activity registry with large payload activities
fn create_large_payload_activities(
    delay_ms: u64,
    small_kb: usize,
    medium_kb: usize,
    large_kb: usize,
) -> ActivityRegistry {
    ActivityRegistry::builder()
        // Small payload activity (~10KB output)
        .register("SmallPayloadTask", move |_ctx: ActivityContext, input: String| {
            let kb = small_kb;
            let delay = delay_ms;
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                let payload = generate_payload(kb);
                Ok(serde_json::to_string(&serde_json::json!({
                    "input": input,
                    "payload": payload,
                    "size_kb": kb
                }))
                .unwrap())
            }
        })
        // Medium payload activity (~50KB output)
        .register("MediumPayloadTask", move |_ctx: ActivityContext, input: String| {
            let kb = medium_kb;
            let delay = delay_ms;
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                let payload = generate_payload(kb);
                Ok(serde_json::to_string(&serde_json::json!({
                    "input": input,
                    "payload": payload,
                    "size_kb": kb
                }))
                .unwrap())
            }
        })
        // Large payload activity (~100KB output)
        .register("LargePayloadTask", move |_ctx: ActivityContext, input: String| {
            let kb = large_kb;
            let delay = delay_ms;
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                let payload = generate_payload(kb);
                Ok(serde_json::to_string(&serde_json::json!({
                    "input": input,
                    "payload": payload,
                    "size_kb": kb
                }))
                .unwrap())
            }
        })
        .build()
}

/// Create orchestration registry with large payload orchestrations
fn create_large_payload_orchestrations(
    small_kb: usize,
    medium_kb: usize,
    large_kb: usize,
    activity_count: usize,
    sub_orch_count: usize,
) -> OrchestrationRegistry {
    OrchestrationRegistry::builder()
        // Register as "FanoutOrchestration" for compatibility with stress test runner
        .register(
            "FanoutOrchestration",
            move |ctx: OrchestrationContext, input: String| {
                large_payload_orchestration(
                    ctx,
                    input,
                    small_kb,
                    medium_kb,
                    large_kb,
                    activity_count,
                    sub_orch_count,
                )
            },
        )
        // Also register with descriptive name
        .register(
            "LargePayloadOrchestration",
            move |ctx: OrchestrationContext, input: String| {
                large_payload_orchestration(
                    ctx,
                    input,
                    small_kb,
                    medium_kb,
                    large_kb,
                    activity_count,
                    sub_orch_count,
                )
            },
        )
        .register(
            "LargePayloadSubOrchestration",
            move |ctx: OrchestrationContext, input: String| {
                large_payload_sub_orchestration(ctx, input, small_kb, medium_kb)
            },
        )
        .build()
}

/// Main orchestration that creates a large history with large payloads
async fn large_payload_orchestration(
    ctx: OrchestrationContext,
    input: String,
    small_kb: usize,
    medium_kb: usize,
    large_kb: usize,
    activity_count: usize,
    sub_orch_count: usize,
) -> Result<String, String> {
    let _config: serde_json::Value = serde_json::from_str(&input).map_err(|e| format!("Invalid input: {e}"))?;

    let mut results = Vec::new();
    let mut event_count = 0;

    // Phase 1: Schedule multiple small payload activities
    let small_count = activity_count / 3;
    for i in 0..small_count {
        let input = generate_payload(small_kb);
        let result = ctx
            .schedule_activity(
                "SmallPayloadTask",
                serde_json::json!({"index": i, "data": input}).to_string(),
            )
            .await;
        results.push(result);
        event_count += 2; // ActivityScheduled + ActivityCompleted
    }

    // Phase 2: Schedule medium payload activities
    let medium_count = activity_count / 3;
    for i in 0..medium_count {
        let input = generate_payload(medium_kb / 2); // Half-size input
        let result = ctx
            .schedule_activity(
                "MediumPayloadTask",
                serde_json::json!({"index": i, "data": input}).to_string(),
            )
            .await;
        results.push(result);
        event_count += 2;
    }

    // Phase 3: Schedule large payload activities
    let large_count = activity_count - small_count - medium_count;
    for i in 0..large_count {
        let input = generate_payload(large_kb / 4); // Quarter-size input
        let result = ctx
            .schedule_activity(
                "LargePayloadTask",
                serde_json::json!({"index": i, "data": input}).to_string(),
            )
            .await;
        results.push(result);
        event_count += 2;
    }

    // Phase 4: Schedule sub-orchestrations with large payloads
    let mut sub_orch_futures = Vec::new();
    for i in 0..sub_orch_count {
        let input = generate_payload(medium_kb);
        let sub_input = serde_json::json!({
            "index": i,
            "payload": input
        })
        .to_string();

        sub_orch_futures.push(ctx.schedule_sub_orchestration("LargePayloadSubOrchestration", sub_input));
        event_count += 4; // SubOrchScheduled, Started, Completed, SubOrchCompleted
    }

    // Wait for all sub-orchestrations
    let sub_results = ctx.join(sub_orch_futures).await;

    // Phase 5: Do some additional small activities to pad the history
    for i in 0..10 {
        let input = format!("final-task-{i}");
        let result = ctx.schedule_activity("SmallPayloadTask", input).await;
        results.push(result);
        event_count += 2;
    }

    let success_count = results.iter().filter(|r| r.is_ok()).count();

    let sub_success_count = sub_results.iter().filter(|r| r.is_ok()).count();

    // Return large payload result
    let result_payload = generate_payload(large_kb);
    Ok(serde_json::to_string(&serde_json::json!({
        "activities_completed": success_count,
        "sub_orchestrations_completed": sub_success_count,
        "estimated_event_count": event_count,
        "result_payload": result_payload,
        "result_size_kb": large_kb
    }))
    .unwrap())
}

/// Sub-orchestration that processes large payloads
async fn large_payload_sub_orchestration(
    ctx: OrchestrationContext,
    input: String,
    small_kb: usize,
    medium_kb: usize,
) -> Result<String, String> {
    let config: serde_json::Value = serde_json::from_str(&input).map_err(|e| format!("Invalid input: {e}"))?;

    // Extract payload from input
    let _input_payload = config["payload"].as_str().unwrap_or("");

    // Process with a couple activities
    let task1_input = generate_payload(small_kb);
    let result1 = ctx.schedule_activity("SmallPayloadTask", task1_input).await;

    let task2_input = generate_payload(medium_kb / 2);
    let result2 = ctx.schedule_activity("MediumPayloadTask", task2_input).await;

    let success = result1.is_ok() && result2.is_ok();

    // Return medium payload
    let result_payload = generate_payload(medium_kb);
    Ok(serde_json::to_string(&serde_json::json!({
        "success": success,
        "result_payload": result_payload,
        "size_kb": medium_kb
    }))
    .unwrap())
}

/// Generate a payload of approximately the specified size in KB
fn generate_payload(kb: usize) -> String {
    // Generate a string of approximately kb kilobytes
    // Each character is 1 byte, so we need kb * 1024 characters
    // We'll use a repeating pattern for efficiency
    let pattern = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()";
    let target_bytes = kb * 1024;
    let pattern_len = pattern.len();
    let repeat_count = (target_bytes / pattern_len) + 1;

    pattern.repeat(repeat_count)[..target_bytes].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_payload_sizes() {
        let payload_10kb = generate_payload(10);
        assert_eq!(payload_10kb.len(), 10 * 1024);

        let payload_50kb = generate_payload(50);
        assert_eq!(payload_50kb.len(), 50 * 1024);

        let payload_100kb = generate_payload(100);
        assert_eq!(payload_100kb.len(), 100 * 1024);
    }
}

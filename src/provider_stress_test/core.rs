// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Core stress test infrastructure - types, runner, and utilities.

use crate::providers::Provider;
use crate::runtime::registry::ActivityRegistry;
use crate::runtime::{self, RuntimeOptions};
use crate::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

/// Configuration for stress tests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StressTestConfig {
    /// Maximum number of concurrent orchestrations
    pub max_concurrent: usize,
    /// Duration to run the test (seconds)
    pub duration_secs: u64,
    /// Number of tasks each orchestration fans out to
    pub tasks_per_instance: usize,
    /// Simulated activity execution time (ms)
    pub activity_delay_ms: u64,
    /// Orchestration dispatcher concurrency
    pub orch_concurrency: usize,
    /// Worker dispatcher concurrency
    pub worker_concurrency: usize,
    /// Timeout for wait_for_orchestration (seconds)
    /// Increase for high-latency remote databases
    #[serde(default = "default_wait_timeout_secs")]
    pub wait_timeout_secs: u64,
}

fn default_wait_timeout_secs() -> u64 {
    60
}

impl Default for StressTestConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 20,
            duration_secs: 10,
            tasks_per_instance: 5,
            activity_delay_ms: 10,
            orch_concurrency: 2,
            worker_concurrency: 2,
            wait_timeout_secs: default_wait_timeout_secs(),
        }
    }
}

/// Results from a stress test run
#[derive(Debug, Clone)]
pub struct StressTestResult {
    /// Number of orchestrations launched
    pub launched: usize,
    /// Number of orchestrations completed successfully
    pub completed: usize,
    /// Number of orchestrations that failed
    pub failed: usize,
    /// Number of orchestrations that failed due to infrastructure errors
    pub failed_infrastructure: usize,
    /// Number of orchestrations that failed due to configuration errors
    pub failed_configuration: usize,
    /// Number of orchestrations that failed due to application errors
    pub failed_application: usize,
    /// Total test duration
    pub total_time: std::time::Duration,
    /// Orchestration throughput (orchestrations per second)
    pub orch_throughput: f64,
    /// Activity throughput (activities per second)
    pub activity_throughput: f64,
    /// Average latency per orchestration
    pub avg_latency_ms: f64,
}

impl StressTestResult {
    /// Calculate success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.launched == 0 {
            return 0.0;
        }
        (self.completed as f64 / self.launched as f64) * 100.0
    }
}

/// Run a stress test with a generic provider
///
/// # Errors
///
/// Returns an error if the stress test execution fails.
pub async fn run_stress_test(
    config: StressTestConfig,
    provider: Arc<dyn Provider>,
    activities: ActivityRegistry,
    orchestrations: OrchestrationRegistry,
) -> Result<StressTestResult, Box<dyn std::error::Error>> {
    info!(
        "=== Starting stress test (orch={}, worker={}) ===",
        config.orch_concurrency, config.worker_concurrency
    );
    info!(
        "Config: max_concurrent={}, duration={}s, tasks_per_instance={}, activity_delay={}ms",
        config.max_concurrent, config.duration_secs, config.tasks_per_instance, config.activity_delay_ms
    );

    // Start runtime with custom options
    let options = RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(100),
        orchestration_concurrency: config.orch_concurrency,
        worker_concurrency: config.worker_concurrency,
        ..Default::default()
    };
    let rt = runtime::Runtime::start_with_options(provider.clone(), activities, orchestrations, options).await;

    // Create client
    let client = Arc::new(Client::new(provider.clone()));

    // Track results
    let launched = Arc::new(tokio::sync::Mutex::new(0_usize));
    let completed = Arc::new(tokio::sync::Mutex::new(0_usize));
    let failed = Arc::new(tokio::sync::Mutex::new(0_usize));
    let failed_infrastructure = Arc::new(tokio::sync::Mutex::new(0_usize));
    let failed_configuration = Arc::new(tokio::sync::Mutex::new(0_usize));
    let failed_application = Arc::new(tokio::sync::Mutex::new(0_usize));
    let active = Arc::new(tokio::sync::Mutex::new(0_usize));

    // Test input
    let input = serde_json::to_string(&serde_json::json!({
        "task_count": config.tasks_per_instance
    }))?;

    let start_time = Instant::now();
    let end_time = start_time + std::time::Duration::from_secs(config.duration_secs);
    let mut instance_id = 0_usize;

    // Continuous orchestration pump
    info!("Starting continuous orchestration pump...");
    loop {
        let now = Instant::now();
        if now >= end_time {
            info!("Duration elapsed, stopping pump...");
            break;
        }

        // Check if we can launch more orchestrations
        let current_active = *active.lock().await;
        if current_active >= config.max_concurrent {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            continue;
        }

        // Launch new orchestration
        instance_id += 1;
        let instance = format!("stress-test-{instance_id}");

        *active.lock().await += 1;
        *launched.lock().await += 1;

        let client_clone = Arc::clone(&client);
        let input_clone = input.clone();
        let completed_clone = Arc::clone(&completed);
        let failed_clone = Arc::clone(&failed);
        let failed_infrastructure_clone = Arc::clone(&failed_infrastructure);
        let failed_configuration_clone = Arc::clone(&failed_configuration);
        let failed_application_clone = Arc::clone(&failed_application);
        let active_clone = Arc::clone(&active);
        let config_clone = config.clone();

        tokio::spawn(async move {
            // Start orchestration
            let start_result = client_clone
                .start_orchestration(&instance, "FanoutOrchestration", input_clone)
                .await;

            if let Err(e) = start_result {
                tracing::error!("Failed to start {}: {}", instance, e);
                *failed_clone.lock().await += 1;
                *active_clone.lock().await -= 1;
                return;
            }

            // Wait for completion
            match client_clone
                .wait_for_orchestration(
                    &instance,
                    std::time::Duration::from_secs(config_clone.wait_timeout_secs),
                )
                .await
            {
                Ok(crate::OrchestrationStatus::Completed { .. }) => {
                    *completed_clone.lock().await += 1;
                }
                Ok(crate::OrchestrationStatus::Failed { details, .. }) => {
                    let category = details.category();
                    tracing::warn!(
                        category = category,
                        error = %details.display_message(),
                        "Orchestration {} failed",
                        instance
                    );

                    *failed_clone.lock().await += 1;

                    match details {
                        crate::ErrorDetails::Infrastructure { .. } => {
                            *failed_infrastructure_clone.lock().await += 1;
                        }
                        crate::ErrorDetails::Configuration { .. } => {
                            *failed_configuration_clone.lock().await += 1;
                        }
                        crate::ErrorDetails::Application { .. } => {
                            *failed_application_clone.lock().await += 1;
                        }
                        crate::ErrorDetails::Poison { .. } => {
                            *failed_infrastructure_clone.lock().await += 1;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Wait error for {}: {:?}", instance, e);
                    *failed_clone.lock().await += 1;
                }
                _ => {
                    *failed_clone.lock().await += 1;
                }
            }

            *active_clone.lock().await -= 1;
        });

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    // Wait for all active orchestrations to complete
    info!("Waiting for active orchestrations to complete...");
    let mut wait_iterations = 0;
    loop {
        let current_active = *active.lock().await;
        if current_active == 0 {
            break;
        }

        if wait_iterations % 100 == 0 {
            info!("Still waiting for {} active orchestrations...", current_active);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        wait_iterations += 1;

        // Timeout after 2 minutes
        if wait_iterations > 1200 {
            info!("Timeout waiting for orchestrations to complete");
            break;
        }
    }

    let total_time = start_time.elapsed();
    let final_launched = *launched.lock().await;
    let final_completed = *completed.lock().await;
    let final_failed = *failed.lock().await;
    let final_failed_infrastructure = *failed_infrastructure.lock().await;
    let final_failed_configuration = *failed_configuration.lock().await;
    let final_failed_application = *failed_application.lock().await;

    let orch_throughput = final_completed as f64 / total_time.as_secs_f64();
    let activity_throughput = (final_completed * config.tasks_per_instance) as f64 / total_time.as_secs_f64();
    let avg_latency_ms = total_time.as_millis() as f64 / final_completed.max(1) as f64;

    // Shutdown
    rt.shutdown(None).await;

    let result = StressTestResult {
        launched: final_launched,
        completed: final_completed,
        failed: final_failed,
        failed_infrastructure: final_failed_infrastructure,
        failed_configuration: final_failed_configuration,
        failed_application: final_failed_application,
        total_time,
        orch_throughput,
        activity_throughput,
        avg_latency_ms,
    };

    // Print results
    info!("=== Results ===");
    info!("Total time: {:?}", result.total_time);
    info!("Launched: {}", result.launched);
    info!("Completed: {}", result.completed);
    info!(
        "Failed: {} (infra: {}, config: {}, app: {})",
        result.failed, result.failed_infrastructure, result.failed_configuration, result.failed_application
    );
    info!("Success rate: {:.2}%", result.success_rate());
    info!("Throughput: {:.2} orchestrations/sec", result.orch_throughput);
    info!("Activity throughput: {:.2} activities/sec", result.activity_throughput);
    info!("Average latency: {:.2}ms", result.avg_latency_ms);

    Ok(result)
}

/// Print a comparison table of multiple test results
pub fn print_comparison_table(results: &[(String, String, StressTestResult)]) {
    info!("\n=== Comparison Table ===");
    info!(
        "{:<20} {:<10} {:<10} {:<10} {:<8} {:<8} {:<8} {:<10} {:<15} {:<15} {:<15}",
        "Provider",
        "Config",
        "Completed",
        "Failed",
        "Infra",
        "Config",
        "App",
        "Success %",
        "Orch/sec",
        "Activity/sec",
        "Avg Latency"
    );
    info!("{}", "-".repeat(150));

    for (provider, config, result) in results {
        info!(
            "{:<20} {:<10} {:<10} {:<10} {:<8} {:<8} {:<8} {:<10.2} {:<15.2} {:<15.2} {:<15.2}ms",
            provider,
            config,
            result.completed,
            result.failed,
            result.failed_infrastructure,
            result.failed_configuration,
            result.failed_application,
            result.success_rate(),
            result.orch_throughput,
            result.activity_throughput,
            result.avg_latency_ms
        );
    }
}

/// Create the default activity registry for stress tests
pub fn create_default_activities(delay_ms: u64) -> ActivityRegistry {
    ActivityRegistry::builder()
        .register("ProcessTask", move |_ctx: ActivityContext, input: String| {
            let delay = delay_ms;
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                Ok(format!("processed: {input}"))
            }
        })
        .build()
}

/// Create the default orchestration registry for stress tests
pub fn create_default_orchestrations() -> OrchestrationRegistry {
    OrchestrationRegistry::builder()
        .register("FanoutOrchestration", fanout_orchestration)
        .build()
}

/// Simple orchestration that fans out to N activities and waits for all
async fn fanout_orchestration(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    let config: serde_json::Value = serde_json::from_str(&input).map_err(|e| format!("Invalid input: {e}"))?;
    let task_count = config["task_count"].as_u64().unwrap_or(5) as usize;

    // Fan-out: schedule all activities in parallel
    let mut futures = Vec::new();
    for i in 0..task_count {
        let task_input = format!("task-{i}");
        futures.push(ctx.schedule_activity("ProcessTask", task_input));
    }

    // Fan-in: wait for all to complete
    let results = ctx.join(futures).await;

    let success_count = results.iter().filter(|r| r.is_ok()).count();

    Ok(format!("Completed {task_count} tasks ({success_count} succeeded)"))
}

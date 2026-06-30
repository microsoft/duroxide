// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Large Payload SQLite Stress Test Binary
//!
//! This binary runs the large payload stress test with SQLite provider.
//! Tests memory consumption with large event payloads and longer histories.
//!
//! Usage:
//!   cargo run --release --package duroxide-sqlite-stress --bin large-payload-stress [DURATION_SECS]
//!
//! Examples:
//!   cargo run --release --bin large-payload-stress       # Default 10 seconds
//!   cargo run --release --bin large-payload-stress 30    # Run for 30 seconds
//!   cargo run --release --bin large-payload-stress 5     # Quick 5 second test

use duroxide::provider_stress_tests::large_payload::{run_large_payload_test_with_config, LargePayloadConfig};
use duroxide::provider_stress_tests::parallel_orchestrations::ProviderStressFactory;
use duroxide::providers::{sqlite::SqliteProvider, Provider};
use std::sync::Arc;

struct SqliteLargePayloadFactory;

#[async_trait::async_trait]
impl ProviderStressFactory for SqliteLargePayloadFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        // Use in-memory SQLite for stress testing
        Arc::new(
            SqliteProvider::new_in_memory()
                .await
                .expect("Failed to create SQLite provider"),
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // Parse duration from command line args
    let duration_secs = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse::<u64>().ok())
        .unwrap_or(10);

    tracing::info!("=== Duroxide Large Payload Stress Test ===");
    tracing::info!("Duration: {} seconds", duration_secs);
    tracing::info!("");
    tracing::info!("This test creates:");
    tracing::info!("  - Large event payloads (10KB, 50KB, 100KB)");
    tracing::info!("  - Moderate-length histories (~80-100 events)");
    tracing::info!("  - Sub-orchestrations with large inputs/outputs");
    tracing::info!("");
    tracing::info!("Memory optimization impact should be visible in:");
    tracing::info!("  - Peak RSS (Resident Set Size)");
    tracing::info!("  - CPU usage (less allocation churn)");
    tracing::info!("");

    // Configure test with custom duration
    let config = LargePayloadConfig {
        base: duroxide::provider_stress_tests::StressTestConfig {
            max_concurrent: 5,
            duration_secs,
            tasks_per_instance: 1,
            activity_delay_ms: 5,
            orch_concurrency: 1,
            worker_concurrency: 1,
            wait_timeout_secs: 120, // Higher for large payload tests
        },
        small_payload_kb: 10,
        medium_payload_kb: 50,
        large_payload_kb: 100,
        activity_count: 20,
        sub_orch_count: 5,
    };

    // Run test
    let factory = SqliteLargePayloadFactory;
    let result = run_large_payload_test_with_config(&factory, config).await?;

    // Print results
    tracing::info!("");
    tracing::info!("=== Results ===");
    tracing::info!("Total time: {:?}", result.total_time);
    tracing::info!("Launched: {}", result.launched);
    tracing::info!("Completed: {}", result.completed);
    tracing::info!(
        "Failed: {} (infra: {}, config: {}, app: {})",
        result.failed,
        result.failed_infrastructure,
        result.failed_configuration,
        result.failed_application
    );
    tracing::info!("Success rate: {:.2}%", result.success_rate());
    tracing::info!("Throughput: {:.2} orchestrations/sec", result.orch_throughput);
    tracing::info!("Activity throughput: {:.2} activities/sec", result.activity_throughput);
    tracing::info!("Average latency: {:.2}ms", result.avg_latency_ms);

    Ok(())
}

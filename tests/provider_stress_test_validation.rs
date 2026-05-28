// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Minimal validation test for the provider stress test infrastructure.
//!
//! This test just ensures the stress test infrastructure doesn't break.
//! It runs a single orchestration to validate the plumbing works.
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]
#![cfg(feature = "provider-test")]

use duroxide::provider_stress_tests::StressTestConfig;
use duroxide::provider_stress_tests::parallel_orchestrations::{
    ProviderStressFactory, run_parallel_orchestrations_test_with_config,
};
use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use std::sync::Arc;

/// Minimal test factory for in-memory SQLite provider
struct InMemorySqliteFactory;

#[async_trait::async_trait]
impl ProviderStressFactory for InMemorySqliteFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(
            SqliteProvider::new_in_memory()
                .await
                .expect("Failed to create in-memory SQLite provider"),
        )
    }
}

#[tokio::test]
async fn test_stress_infrastructure_minimal() {
    // Initialize logging for the test
    let _ = tracing_subscriber::fmt()
        .with_env_filter("error")
        .with_test_writer()
        .try_init();

    let factory = InMemorySqliteFactory;

    // Minimal config: just run 1 orchestration to validate infrastructure
    let config = StressTestConfig {
        max_concurrent: 1,
        duration_secs: 1,
        tasks_per_instance: 2,
        activity_delay_ms: 5,
        orch_concurrency: 1,
        worker_concurrency: 1,
        wait_timeout_secs: 60,
    };

    let result = run_parallel_orchestrations_test_with_config(&factory, config)
        .await
        .expect("Stress test infrastructure is broken");

    // Just validate that at least one orchestration completed successfully
    assert!(
        result.completed > 0,
        "No orchestrations completed - infrastructure broken"
    );
    assert_eq!(result.failed, 0, "Orchestration failed - infrastructure broken");
}

#[tokio::test]
async fn test_large_payload_stress_infrastructure() {
    use duroxide::provider_stress_tests::large_payload::{LargePayloadConfig, run_large_payload_test_with_config};

    // Initialize logging for the test
    let _ = tracing_subscriber::fmt()
        .with_env_filter("error")
        .with_test_writer()
        .try_init();

    let factory = InMemorySqliteFactory;

    // Minimal config: run for 1 second with reduced concurrency and smaller payloads
    let config = LargePayloadConfig {
        base: StressTestConfig {
            max_concurrent: 2,
            duration_secs: 1,
            tasks_per_instance: 1, // Not used in large payload test
            activity_delay_ms: 5,
            orch_concurrency: 1,
            worker_concurrency: 1,
            wait_timeout_secs: 60,
        },
        small_payload_kb: 5,   // Reduced from 10
        medium_payload_kb: 10, // Reduced from 50
        large_payload_kb: 20,  // Reduced from 100
        activity_count: 10,    // Reduced from 20
        sub_orch_count: 2,     // Reduced from 5
    };

    let result = run_large_payload_test_with_config(&factory, config)
        .await
        .expect("Large payload stress test infrastructure is broken");

    // Validate that at least one orchestration completed successfully
    assert!(
        result.completed > 0,
        "No orchestrations completed - large payload infrastructure broken"
    );
    assert_eq!(
        result.failed, 0,
        "Orchestration failed - large payload infrastructure broken. Failed: {}",
        result.failed
    );
    // With reduced payloads and short duration, expect reasonable success rate
    assert!(
        result.success_rate() > 90.0,
        "Success rate too low: {:.2}%",
        result.success_rate()
    );
}

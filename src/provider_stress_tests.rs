// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Provider Stress Test Infrastructure
//!
//! This module provides reusable stress test infrastructure for validating custom Provider implementations
//! under load. Enable the `provider-test` feature to use these utilities.
//!
//! # Overview
//!
//! Stress tests validate that your provider can handle:
//! - High throughput (orchestrations/sec and activities/sec)
//! - Concurrent instance execution
//! - Sustained load over time
//! - Complex orchestration patterns (fan-out/fan-in)
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use duroxide::provider_stress_tests::parallel_orchestrations::{ProviderStressFactory, run_parallel_orchestrations_test};
//! use duroxide::providers::Provider;
//! use std::sync::Arc;
//!
//! struct MyProviderFactory;
//!
//! #[async_trait::async_trait]
//! impl ProviderStressFactory for MyProviderFactory {
//!     async fn create_provider(&self) -> Arc<dyn Provider> {
//!         Arc::new(MyProvider::new().await.unwrap())
//!     }
//! }
//!
//! #[tokio::test]
//! async fn stress_test_my_provider() {
//!     let factory = MyProviderFactory;
//!     let result = run_parallel_orchestrations_test(&factory).await.unwrap();
//!     
//!     // Assert performance requirements
//!     assert!(result.success_rate() > 99.0, "Success rate: {:.2}%", result.success_rate());
//!     assert!(result.orch_throughput > 1.0, "Throughput too low: {:.2} orch/sec", result.orch_throughput);
//! }
//! ```
//!
//! # Custom Configuration
//!
//! ```rust,ignore
//! use duroxide::provider_stress_tests::{StressTestConfig, parallel_orchestrations::run_parallel_orchestrations_test_with_config};
//!
//! let config = StressTestConfig {
//!     max_concurrent: 50,
//!     duration_secs: 30,
//!     tasks_per_instance: 10,
//!     activity_delay_ms: 5,
//!     orch_concurrency: 4,
//!     worker_concurrency: 4,
//! };
//!
//! let result = run_parallel_orchestrations_test_with_config(&factory, config).await.unwrap();
//! ```
//!
//! # Available Stress Tests
//!
//! ## Parallel Orchestrations (`parallel_orchestrations` module)
//!
//! Tests fan-out/fan-in pattern with concurrent orchestrations:
//! - `run_parallel_orchestrations_test` - Run with default config
//! - `run_parallel_orchestrations_test_with_config` - Run with custom config
//!
//! Default configuration:
//! - 20 max concurrent orchestrations
//! - 10 second duration
//! - 5 tasks per orchestration (fan-out)
//! - 10ms simulated activity delay
//! - 2 orchestration dispatchers
//! - 2 worker dispatchers
//!
//! ## Large Payload (`large_payload` module)
//!
//! Tests memory consumption and history management with large event payloads:
//! - `run_large_payload_test` - Run with default config
//! - `run_large_payload_test_with_config` - Run with custom config
//!
//! Default configuration:
//! - 5 max concurrent orchestrations
//! - 10 second duration
//! - Large event payloads (10KB, 50KB, 100KB)
//! - Moderate-length histories (~80-100 events)
//! - 20 activities + 5 sub-orchestrations per instance
//! - 5ms simulated activity delay
//!
//! ```rust,ignore
//! use duroxide::provider_stress_tests::parallel_orchestrations::ProviderStressFactory;
//! use duroxide::provider_stress_tests::large_payload::run_large_payload_test;
//!
//! // Use the same ProviderStressFactory implementation as parallel_orchestrations
//! struct MyProviderFactory;
//!
//! #[async_trait::async_trait]
//! impl ProviderStressFactory for MyProviderFactory {
//!     async fn create_provider(&self) -> Arc<dyn Provider> {
//!         Arc::new(MyProvider::new().await.unwrap())
//!     }
//! }
//!
//! #[tokio::test]
//! async fn large_payload_stress_test() {
//!     let factory = MyProviderFactory;
//!     let result = run_large_payload_test(&factory).await.unwrap();
//!     assert!(result.success_rate() > 99.0);
//! }
//! ```
//!
//! # Understanding Results
//!
//! The `StressTestResult` provides metrics for evaluation:
//!
//! ```rust,ignore
//! let result = run_parallel_orchestrations_test(&factory).await.unwrap();
//!
//! println!("Success rate: {:.2}%", result.success_rate());
//! println!("Throughput: {:.2} orch/sec", result.orch_throughput);
//! println!("Activity throughput: {:.2} activities/sec", result.activity_throughput);
//! println!("Average latency: {:.2}ms", result.avg_latency_ms);
//!
//! // Breakdown of failures by category
//! println!("Infrastructure failures: {}", result.failed_infrastructure);
//! println!("Configuration failures: {}", result.failed_configuration);
//! println!("Application failures: {}", result.failed_application);
//! ```
//!
//! ## Interpreting Results
//!
//! **Success Rate**: Should be 100% for production-ready providers. Any failures indicate issues:
//! - Infrastructure failures: Provider bugs, lock issues, data corruption
//! - Configuration failures: Missing implementations, nondeterminism detection
//! - Application failures: Expected orchestration/activity errors (should be rare in stress tests)
//!
//! **Throughput**: Varies by provider and hardware. Useful for:
//! - Comparing provider implementations
//! - Detecting performance regressions
//! - Understanding scalability characteristics
//!
//! **Latency**: Average time per orchestration. Lower is better.
//! - File-based providers typically have higher latency than in-memory
//! - Indicates how quickly orchestrations can complete under load
//!
//! # Comparison Testing
//!
//! Test multiple configurations and providers:
//!
//! ```rust,ignore
//! use duroxide::provider_stress_tests::print_comparison_table;
//!
//! let mut results = Vec::new();
//!
//! // Test different concurrency settings
//! for (orch, worker) in [(1, 1), (2, 2), (4, 4)] {
//!     let config = StressTestConfig {
//!         orch_concurrency: orch,
//!         worker_concurrency: worker,
//!         ..Default::default()
//!     };
//!     
//!     let result = run_parallel_orchestrations_test_with_config(&factory, config).await.unwrap();
//!     results.push((
//!         "MyProvider".to_string(),
//!         format!("{}/{}", orch, worker),
//!         result
//!     ));
//! }
//!
//! print_comparison_table(&results);
//! ```

#[cfg(feature = "provider-test")]
pub use crate::provider_stress_test::core::{
    StressTestConfig, StressTestResult, create_default_activities, create_default_orchestrations,
    print_comparison_table, run_stress_test,
};

#[cfg(feature = "provider-test")]
pub mod parallel_orchestrations {
    pub use crate::provider_stress_test::parallel_orchestrations::{
        ProviderStressFactory, run_parallel_orchestrations_test, run_parallel_orchestrations_test_with_config,
    };
}

#[cfg(feature = "provider-test")]
pub mod large_payload {
    pub use crate::provider_stress_test::large_payload::{
        LargePayloadConfig, run_large_payload_test, run_large_payload_test_with_config,
    };
}

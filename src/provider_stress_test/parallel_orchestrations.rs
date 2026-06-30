// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Parallel orchestrations stress test scenario.
//!
//! Tests fan-out/fan-in orchestration patterns with concurrent instance execution.

use super::core::{
    StressTestConfig, StressTestResult, create_default_activities, create_default_orchestrations, run_stress_test,
};
use crate::providers::Provider;
use std::sync::Arc;

/// Trait for creating providers in stress tests.
///
/// Implement this trait to provide a way to create your custom provider instance
/// for stress testing.
#[async_trait::async_trait]
pub trait ProviderStressFactory: Send + Sync {
    /// Create a new provider instance for stress testing.
    ///
    /// Each call should return a fresh, isolated provider instance.
    /// For stress tests, this typically means creating a new in-memory provider
    /// or a file-based provider with a unique temporary path.
    async fn create_provider(&self) -> Arc<dyn Provider>;

    /// Optional: Customize the stress test configuration.
    ///
    /// Override this to provide custom stress test parameters for your provider.
    fn stress_test_config(&self) -> StressTestConfig {
        StressTestConfig::default()
    }
}

/// Run the parallel orchestrations stress test with a custom provider factory.
///
/// # Example
///
/// ```rust,ignore
/// use duroxide::provider_stress_tests::parallel_orchestrations::{ProviderStressFactory, run_parallel_orchestrations_test};
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
/// async fn stress_test_my_provider() {
///     let factory = MyProviderFactory;
///     let result = run_parallel_orchestrations_test(&factory).await.unwrap();
///     assert!(result.success_rate() > 99.0, "Success rate too low: {:.2}%", result.success_rate());
/// }
/// ```
///
/// # Errors
///
/// Returns an error if the stress test execution fails.
pub async fn run_parallel_orchestrations_test(
    factory: &dyn ProviderStressFactory,
) -> Result<StressTestResult, Box<dyn std::error::Error>> {
    let config = factory.stress_test_config();
    let provider = factory.create_provider().await;
    let activities = create_default_activities(config.activity_delay_ms);
    let orchestrations = create_default_orchestrations();

    run_stress_test(config, provider, activities, orchestrations).await
}

/// Run the parallel orchestrations stress test with a custom configuration.
///
/// # Errors
///
/// Returns an error if the stress test execution fails.
pub async fn run_parallel_orchestrations_test_with_config(
    factory: &dyn ProviderStressFactory,
    config: StressTestConfig,
) -> Result<StressTestResult, Box<dyn std::error::Error>> {
    let provider = factory.create_provider().await;
    let activities = create_default_activities(config.activity_delay_ms);
    let orchestrations = create_default_orchestrations();

    run_stress_test(config, provider, activities, orchestrations).await
}

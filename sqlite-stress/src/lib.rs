// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! SQLite Stress Tests for Duroxide
//!
//! This library provides SQLite-specific stress test implementations for Duroxide,
//! using the provider stress test infrastructure from the main crate.
//!
//! # Quick Start
//!
//! Run the parallel orchestrations stress test:
//!
//! ```bash
//! cargo run --release --package duroxide-sqlite-stress --bin sqlite-stress [DURATION]
//! ```
//!
//! Run the large payload stress test:
//!
//! ```bash
//! cargo run --release --package duroxide-sqlite-stress --bin large-payload-stress [DURATION]
//! ```
//!
//! Or use from the workspace root:
//!
//! ```bash
//! ./run-stress-tests.sh [DURATION]
//! ```

use duroxide::provider_stress_tests::parallel_orchestrations::{
    run_parallel_orchestrations_test_with_config, ProviderStressFactory,
};
use duroxide::provider_stress_tests::{print_comparison_table, StressTestConfig};
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::Provider;
use std::sync::Arc;
use tracing::info;

// Re-export the stress test infrastructure for convenience
pub use duroxide::provider_stress_tests::{StressTestConfig as Config, StressTestResult};

/// Factory for creating in-memory SQLite providers for stress testing
pub struct InMemorySqliteFactory;

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

/// Factory for creating file-based SQLite providers for stress testing
pub struct FileSqliteFactory {
    db_path: String,
}

impl FileSqliteFactory {
    pub fn new() -> Self {
        let db_path = format!("/tmp/duroxide_stress_{}.db", std::process::id());
        // Create the file to ensure it exists
        if let Err(e) = std::fs::File::create(&db_path) {
            tracing::warn!("Failed to pre-create DB file {}: {}", db_path, e);
        }
        Self { db_path }
    }

    pub fn cleanup(&self) {
        if let Err(e) = std::fs::remove_file(&self.db_path) {
            tracing::warn!("Failed to remove temp DB file {}: {}", self.db_path, e);
        }
    }
}

#[async_trait::async_trait]
impl ProviderStressFactory for FileSqliteFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(
            SqliteProvider::new(&format!("sqlite:{}", self.db_path), None)
                .await
                .expect("Failed to create file-based SQLite provider"),
        )
    }
}

/// Run the parallel orchestrations stress test suite across SQLite providers and configurations
pub async fn run_test_suite(duration_secs: u64) -> Result<(), Box<dyn std::error::Error>> {
    info!("=== Duroxide SQLite Stress Test Suite ===");
    info!("Duration: {} seconds per test", duration_secs);

    let concurrency_combos = vec![(1, 1), (2, 2)];
    let mut results = Vec::new();

    // Test in-memory SQLite
    info!("\n--- Testing In-Memory SQLite Provider ---");
    let in_memory_factory = InMemorySqliteFactory;

    for (orch_conc, worker_conc) in &concurrency_combos {
        let config = StressTestConfig {
            max_concurrent: 20,
            duration_secs,
            tasks_per_instance: 5,
            activity_delay_ms: 10,
            orch_concurrency: *orch_conc,
            worker_concurrency: *worker_conc,
            wait_timeout_secs: 60,
        };

        match run_parallel_orchestrations_test_with_config(&in_memory_factory, config).await {
            Ok(result) => {
                results.push((
                    "In-Memory SQLite".to_string(),
                    format!("{}/{}", orch_conc, worker_conc),
                    result,
                ));
                info!("✓ Test completed");
            }
            Err(e) => {
                info!("✗ Test failed: {}", e);
            }
        }
    }

    // Test file-based SQLite
    info!("\n--- Testing File-Based SQLite Provider ---");

    for (orch_conc, worker_conc) in &concurrency_combos {
        let config = StressTestConfig {
            max_concurrent: 20,
            duration_secs,
            tasks_per_instance: 5,
            activity_delay_ms: 10,
            orch_concurrency: *orch_conc,
            worker_concurrency: *worker_conc,
            wait_timeout_secs: 60,
        };

        let file_factory = FileSqliteFactory::new();
        match run_parallel_orchestrations_test_with_config(&file_factory, config).await {
            Ok(result) => {
                results.push((
                    "File SQLite".to_string(),
                    format!("{}/{}", orch_conc, worker_conc),
                    result,
                ));
                info!("✓ Test completed");
                file_factory.cleanup();
            }
            Err(e) => {
                info!("✗ Test failed: {}", e);
                file_factory.cleanup();
            }
        }
    }

    // Print comparison table
    print_comparison_table(&results);

    Ok(())
}

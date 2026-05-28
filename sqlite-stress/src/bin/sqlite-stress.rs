// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! SQLite Stress Test Binary
//!
//! This binary runs the SQLite stress test suite across multiple configurations.
//!
//! Usage:
//!   cargo run --release --package duroxide-sqlite-stress --bin sqlite-stress [DURATION_SECS]
//!
//! Examples:
//!   cargo run --release --bin sqlite-stress       # Default 30 seconds
//!   cargo run --release --bin sqlite-stress 60    # Run for 60 seconds
//!   cargo run --release --bin sqlite-stress 5     # Quick 5 second test

use duroxide_sqlite_stress;

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
        .unwrap_or(30);

    // Run the test suite
    duroxide_sqlite_stress::run_test_suite(duration_secs).await?;

    Ok(())
}

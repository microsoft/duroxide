// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Example demonstrating duroxide with observability enabled.
//!
//! This example shows how to configure structured logging and metrics
//! for production observability.
//!
//! Metrics are emitted via the `metrics` facade. To export them, install a
//! recorder before starting the runtime:
//!
//! ```rust,ignore
//! // Prometheus:
//! metrics_exporter_prometheus::PrometheusBuilder::new().install()?;
//!
//! // Or OpenTelemetry:
//! metrics_exporter_opentelemetry::Recorder::builder("app").install_global()?;
//! ```
//!
//! Run with:
//! ```bash
//! cargo run --example with_observability
//! ```
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, LogFormat, ObservabilityConfig, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure observability with compact logging format
    // Metrics are always available via the `metrics` facade
    let observability = ObservabilityConfig {
        log_format: LogFormat::Compact,
        log_level: "info".to_string(),
        service_name: "duroxide-example".to_string(),
        service_version: Some("1.0.0".to_string()),
        ..Default::default()
    };

    let options = RuntimeOptions {
        observability,
        ..Default::default()
    };

    // Create provider
    let store = Arc::new(SqliteProvider::new_in_memory().await?);

    // Register activities
    let activities = ActivityRegistry::builder()
        .register("Greet", |ctx: ActivityContext, name: String| async move {
            ctx.trace_info("Greeting activity started");
            // Simulate some work
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            ctx.trace_info("Greeting activity complete");
            Ok(format!("Hello, {name}!"))
        })
        .register("Farewell", |ctx: ActivityContext, name: String| async move {
            ctx.trace_info("Farewell activity started");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            ctx.trace_info("Farewell activity complete");
            Ok(format!("Goodbye, {name}!"))
        })
        .build();

    // Define orchestration
    let greeting_orch = |ctx: OrchestrationContext, name: String| async move {
        ctx.trace_info("Starting greeting orchestration");

        let greeting = ctx.schedule_activity("Greet", name.clone()).await?;

        ctx.trace_info(format!("Got greeting: {greeting}"));

        let farewell = ctx.schedule_activity("Farewell", name).await?;

        ctx.trace_info("Orchestration completing");
        Ok::<_, String>(format!("{greeting} | {farewell}"))
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("GreetingWorkflow", greeting_orch)
        .build();

    // Start runtime with observability
    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    println!("Runtime started with observability enabled");
    println!("Watch the logs below with structured context fields:\n");

    // Create client and start orchestration
    let client = Client::new(store);

    client
        .start_orchestration("greeting-1", "GreetingWorkflow", "World")
        .await?;

    // Wait for completion
    match client
        .wait_for_orchestration("greeting-1", std::time::Duration::from_secs(5))
        .await
    {
        Ok(OrchestrationStatus::Completed { output, .. }) => {
            println!("\n✅ Orchestration completed successfully!");
            println!("Output: {output}");
        }
        Ok(OrchestrationStatus::Failed { details, .. }) => {
            println!("\n❌ Orchestration failed: {}", details.display_message());
        }
        Ok(_) => {
            println!("\n⏳ Orchestration still running");
        }
        Err(e) => {
            println!("\n⚠️ Wait error: {e:?}");
        }
    }

    // Shutdown gracefully
    rt.shutdown(None).await;

    println!("\nRuntime shut down");

    Ok(())
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::Client;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime;
use duroxide::runtime::registry::{ActivityRegistry, OrchestrationRegistry};
use duroxide::*;
use std::sync::Arc;

/// This example demonstrates the CORRECT way to handle delays and timeouts.
///
/// ⚠️ KEY CONCEPTS:
/// 1. Use timers for orchestration delays (not activities!)
/// 2. Activities can do any async operations (HTTP, sleep, etc.)
/// 3. Use select2 for timeout patterns
///
/// This example shows:
/// 1. ✅ CORRECT: Using timers for orchestration delays
/// 2. ✅ CORRECT: Using select2 for timeout patterns
/// 3. ✅ CORRECT: Activities can use tokio::time::sleep() and any async operations

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the runtime with SQLite store
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("delays_and_timeouts.db");
    std::fs::File::create(&db_path)?;
    let db_url = format!("sqlite:{}", db_path.to_str().unwrap());
    let store = Arc::new(SqliteProvider::new(&db_url, None).await?);

    // Register activities - these can do any async operations including delays
    let activities = ActivityRegistry::builder()
        .register("ProcessData", |_ctx: ActivityContext, input: String| async move {
            println!("Processing data: {input}");
            // ✅ Activities can be pure business logic
            Ok(format!("Processed: {input}"))
        })
        .register("SlowOperation", |ctx: ActivityContext, input: String| async move {
            println!("Starting slow operation: {input}");
            // ✅ Activities can use tokio::time::sleep(), HTTP calls, database queries, etc.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            println!("Slow operation completed");
            ctx.trace_info("Slow operation complete inside activity");
            Ok(format!("Slow result: {input}"))
        })
        .build();

    // Orchestration showing CORRECT timer usage
    let delay_orchestration = |ctx: OrchestrationContext, input: String| async move {
        ctx.trace_info("Starting delay example orchestration");

        // ✅ CORRECT: Use timer for delay
        ctx.trace_info("Waiting 2 seconds...");
        ctx.schedule_timer(std::time::Duration::from_secs(2)).await;
        ctx.trace_info("Timer fired! Processing data...");

        // Process some data after the delay
        let result = ctx.schedule_activity("ProcessData", input).await?;

        ctx.trace_info("Processing complete!");
        Ok(format!("Delayed result: {result}"))
    };

    // Orchestration showing CORRECT timeout usage
    let timeout_orchestration = |ctx: OrchestrationContext, input: String| async move {
        ctx.trace_info("Starting timeout example orchestration");

        // ✅ CORRECT: Use select2 for timeout pattern
        // Both arms must return the same type - wrap timer in async block
        let work = ctx.schedule_activity("SlowOperation", input.clone());
        let timeout = async {
            ctx.schedule_timer(std::time::Duration::from_secs(5)).await;
            Err::<String, String>("timeout".to_string())
        };

        ctx.trace_info("Racing work against timeout...");
        let (winner_index, result) = ctx.select2(work, timeout).await.into_tuple();

        match (winner_index, result) {
            (0, Ok(value)) => {
                // Work completed first
                ctx.trace_info("Work completed within timeout");
                Ok(format!("Success: {value}"))
            }
            (0, Err(e)) => {
                // Work failed
                ctx.trace_info("Work failed");
                Err(format!("Work failed: {e}"))
            }
            (1, Err(_)) => {
                // Timeout occurred first
                ctx.trace_info("Operation timed out");
                Err("Operation timed out after 5 seconds".to_string())
            }
            _ => unreachable!(),
        }
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("DelayExample", delay_orchestration)
        .register("TimeoutExample", timeout_orchestration)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;

    let client = Client::new(store.clone());

    println!("🚀 Running delay example...");

    // Run the delay example
    let delay_instance = format!(
        "delay-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    client
        .start_orchestration(&delay_instance, "DelayExample", "test data")
        .await?;
    match client
        .wait_for_orchestration(&delay_instance, std::time::Duration::from_secs(15))
        .await
        .map_err(|e| format!("Wait error: {e:?}"))?
    {
        OrchestrationStatus::Completed { output, .. } => {
            println!("✅ Delay example completed: {output}");
        }
        OrchestrationStatus::Failed { details, .. } => {
            println!("❌ Delay example failed: {}", details.display_message());
        }
        _ => println!("⏳ Delay example still running..."),
    }

    println!("\n🚀 Running timeout example...");

    // Run the timeout example
    let timeout_instance = format!(
        "timeout-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    client
        .start_orchestration(&timeout_instance, "TimeoutExample", "test data")
        .await?;
    match client
        .wait_for_orchestration(&timeout_instance, std::time::Duration::from_secs(15))
        .await
        .map_err(|e| format!("Wait error: {e:?}"))?
    {
        OrchestrationStatus::Completed { output, .. } => {
            println!("✅ Timeout example completed: {output}");
        }
        OrchestrationStatus::Failed { details, .. } => {
            println!("❌ Timeout example failed: {}", details.display_message());
        }
        _ => println!("⏳ Timeout example still running..."),
    }

    rt.shutdown(None).await;

    println!("\n📚 Key Takeaways:");
    println!("✅ Use ctx.schedule_timer(duration).await for orchestration delays");
    println!("✅ Use ctx.schedule_activity(name, input).await for work");
    println!("✅ Use ctx.select2(work, timeout) for timeout patterns");
    println!("✅ Activities can use tokio::time::sleep(), HTTP calls, database queries, etc.");
    println!("❌ Never use non-deterministic operations in orchestrations");

    Ok(())
}

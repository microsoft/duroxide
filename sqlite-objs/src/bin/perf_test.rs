//! Quick perf test for the sqlite-objs provider.
//!
//! Runs N orchestrations (each with M activities) end-to-end and reports throughput.
//! Compare output with the sqlite perf test in the main workspace.
//!
//! Set AZURE_STORAGE_CONNECTION_STRING in .env (parent dir) to also test Azure Storage.
//!
//! Usage:
//!   cd sqlite-objs && cargo run --release --bin perf-test [INSTANCES] [ACTIVITIES]
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use duroxide_sqlite_objs::SqliteObjsProvider;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let num_instances: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(50);
    let num_activities: usize = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(3);

    println!("=== sqlite-objs provider perf test ===");
    println!("Instances: {num_instances}, Activities/instance: {num_activities}");

    // --- In-memory test ---
    run_test("in-memory", num_instances, num_activities, || async {
        Arc::new(
            SqliteObjsProvider::new_in_memory()
                .await
                .expect("Failed to create in-memory provider"),
        ) as Arc<dyn duroxide::providers::Provider>
    })
    .await?;

    // --- File-based test ---
    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("perf.db").to_str().unwrap().to_string();
    run_test("file-based", num_instances, num_activities, move || {
        let p = db_path.clone();
        async move {
            Arc::new(
                SqliteObjsProvider::new_local(&p)
                    .await
                    .expect("Failed to create file provider"),
            ) as Arc<dyn duroxide::providers::Provider>
        }
    })
    .await?;

    // --- Azure Storage test ---
    // Requires AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_CONTAINER, AZURE_STORAGE_SAS env vars
    // Generate SAS with:
    //   az storage container generate-sas --name CONTAINER --account-name ACCOUNT \
    //     --account-key KEY --permissions rwdlac --expiry ... --output tsv
    // URL-encode with:
    //   python3 -c "import urllib.parse; print(urllib.parse.quote('$SASKEY'))"
    let account = std::env::var("AZURE_STORAGE_ACCOUNT").ok();
    let container = std::env::var("AZURE_STORAGE_CONTAINER").ok();
    let sas = std::env::var("AZURE_STORAGE_SAS").ok();

    if let (Some(account), Some(container), Some(sas)) = (account, container, sas) {
        let db_name = format!("perf-test-{}.db", std::process::id());
        println!("\nUsing Azure Storage (account: {account}, container: {container}, db: {db_name})");

        run_test("azure-storage", num_instances, num_activities, move || {
            let options = duroxide_sqlite_objs::SqliteObjsOptions {
                azure_account: account,
                azure_container: container,
                azure_sas: sas,
                db_name,
            };
            async move {
                Arc::new(
                    SqliteObjsProvider::new(options)
                        .await
                        .expect("Failed to create Azure Storage provider"),
                ) as Arc<dyn duroxide::providers::Provider>
            }
        })
        .await?;
    } else {
        println!("\n--- azure-storage: SKIPPED ---");
        println!("  Set AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_CONTAINER, AZURE_STORAGE_SAS env vars");
    }

    Ok(())
}

async fn run_test<F, Fut>(
    label: &str,
    num_instances: usize,
    num_activities: usize,
    make_provider: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Arc<dyn duroxide::providers::Provider>>,
{
    let store = make_provider().await;

    let activities = ActivityRegistry::builder()
        .register(
            "DoWork",
            |_ctx: ActivityContext, input: String| async move { Ok(format!("done:{input}")) },
        )
        .build();

    let act_count = num_activities;
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "PerfOrch",
            move |ctx: OrchestrationContext, input: String| async move {
                let mut futs = Vec::new();
                for i in 0..act_count {
                    futs.push(ctx.schedule_activity("DoWork", format!("{input}-{i}")));
                }
                let results = ctx.join(futs).await;
                let ok_count = results.iter().filter(|r| r.is_ok()).count();
                Ok(format!("{ok_count}/{act_count} activities completed"))
            },
        )
        .build();

    let opts = RuntimeOptions {
        orchestration_concurrency: 2,
        worker_concurrency: 2,
        ..Default::default()
    };
    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, opts).await;
    let client = Client::new(store);

    // Start all instances
    let start = Instant::now();

    for i in 0..num_instances {
        client
            .start_orchestration(&format!("perf-{i}"), "PerfOrch", format!("item-{i}"))
            .await?;
    }

    let enqueue_elapsed = start.elapsed();

    // Wait for all to complete
    let mut completed = 0usize;
    let mut failed = 0usize;
    for i in 0..num_instances {
        match client
            .wait_for_orchestration(&format!("perf-{i}"), std::time::Duration::from_secs(30))
            .await
        {
            Ok(duroxide::OrchestrationStatus::Completed { .. }) => completed += 1,
            Ok(duroxide::OrchestrationStatus::Failed { .. }) => failed += 1,
            Ok(_) => failed += 1,
            Err(_) => failed += 1,
        }
    }

    let total_elapsed = start.elapsed();
    rt.shutdown(None).await;

    let throughput = completed as f64 / total_elapsed.as_secs_f64();
    let total_activities = completed * num_activities;
    let act_throughput = total_activities as f64 / total_elapsed.as_secs_f64();

    println!("\n--- {label} ---");
    println!("  Enqueue time:       {enqueue_elapsed:.2?}");
    println!("  Total time:         {total_elapsed:.2?}");
    println!("  Completed:          {completed}/{num_instances}");
    if failed > 0 {
        println!("  Failed:             {failed}");
    }
    println!("  Orch throughput:    {throughput:.1} orch/sec");
    println!("  Activity throughput:{act_throughput:.1} act/sec");

    Ok(())
}

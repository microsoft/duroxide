// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "common/retirement_metrics.rs"]
mod retirement_metrics;

use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::task::{Context, Poll};
use std::time::Duration;

use duroxide::providers::{Provider, sqlite::SqliteProvider};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{ObservabilityConfig, Runtime, RuntimeOptions, RuntimeShutdownError, ShutdownOutcome};
use duroxide::{Client, OrchestrationContext, OrchestrationRegistry};
use metrics_util::debugging::DebuggingRecorder;
use retirement_metrics::retirement_gauge;
use tokio::sync::Semaphore;

const LIMIT: Duration = Duration::from_secs(5);

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, future)
        .await
        .expect("retirement metrics fixture made progress")
}

struct Completion {
    work: Pin<Box<dyn Future<Output = Result<(), String>> + Send>>,
    dropped: Arc<AtomicUsize>,
}

impl Future for Completion {
    type Output = Result<(), String>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.work.as_mut().poll(context)
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct Population {
    runtime: Arc<Runtime>,
    client: Client,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    dropped: Arc<AtomicUsize>,
    size: usize,
    fail_cleanup: bool,
}

impl Population {
    async fn prepare(size: usize, fail_cleanup: bool) -> Self {
        let provider: Arc<dyn Provider> = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let client = Client::new(Arc::clone(&provider));
        let entered = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let handler_entered = Arc::clone(&entered);
        let handler_release = Arc::clone(&release);
        let handler_dropped = Arc::clone(&dropped);
        let registry = OrchestrationRegistry::builder()
            .register("retirement", move |ctx: OrchestrationContext, _: String| {
                let entered = Arc::clone(&handler_entered);
                let release = Arc::clone(&handler_release);
                let dropped = Arc::clone(&handler_dropped);
                async move {
                    ctx.register_invocation_cleanup(Completion {
                        work: Box::pin(async move {
                            entered.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            if fail_cleanup {
                                Err("foreign completion returned an error".to_string())
                            } else {
                                Ok(())
                            }
                        }),
                        dropped,
                    })
                    .unwrap();
                    Ok("durably complete".to_string())
                }
            })
            .build();
        let options = RuntimeOptions {
            orchestration_concurrency: 1,
            worker_concurrency: 0,
            dispatcher_min_poll_interval: Duration::from_millis(5),
            dispatcher_long_poll_timeout: Duration::from_millis(50),
            observability: ObservabilityConfig {
                log_level: "off".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let runtime = Runtime::prepare(provider, ActivityRegistry::builder().build(), registry, options).unwrap();
        Self {
            runtime,
            client,
            entered,
            release,
            dropped,
            size,
            fail_cleanup,
        }
    }

    async fn start(&self) {
        Arc::clone(&self.runtime).start_execution().await.unwrap();
        for index in 0..self.size {
            self.client
                .start_orchestration(&format!("retained-{index}"), "retirement", "")
                .await
                .unwrap();
        }
        bounded(self.entered.acquire_many(self.size.try_into().unwrap()))
            .await
            .unwrap()
            .forget();
        assert_eq!(self.dropped.load(Ordering::SeqCst), 0);
    }

    async fn stop(self) {
        let lifetime = Arc::downgrade(&self.runtime);
        self.release.add_permits(self.size);
        assert_eq!(
            bounded(self.runtime.shutdown_with_grace(Duration::from_secs(1))).await,
            if self.fail_cleanup && self.size > 0 {
                Err(RuntimeShutdownError::Failed { quiescent: true })
            } else {
                Ok(ShutdownOutcome::Drained)
            }
        );
        assert_eq!(self.dropped.load(Ordering::SeqCst), self.size);
        bounded(async {
            while lifetime.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }
}

#[tokio::test]
async fn repeated_runtime_lifetimes_keep_one_unlabeled_retirement_series() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    for iteration in 0..100 {
        let population = Population::prepare(1, iteration % 2 == 1).await;
        if iteration > 0 {
            assert_eq!(retirement_gauge(&snapshotter), 0.0);
        }
        population.start().await;
        assert_eq!(retirement_gauge(&snapshotter), 1.0);
        population.stop().await;
        assert_eq!(retirement_gauge(&snapshotter), 0.0);
    }
    eprintln!("DUROXIDE_RETIREMENT_CARDINALITY lifetimes=100 series=1 value=0");
}

#[tokio::test]
async fn overlapping_runtime_populations_aggregate_without_creation_or_stop_resets() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let first = Population::prepare(2, false).await;
    first.start().await;
    assert_eq!(retirement_gauge(&snapshotter), 2.0);

    let second = Population::prepare(3, true).await;
    assert_eq!(retirement_gauge(&snapshotter), 2.0);
    let idle = Population::prepare(0, false).await;
    idle.start().await;
    assert_eq!(retirement_gauge(&snapshotter), 2.0);
    idle.stop().await;
    assert_eq!(retirement_gauge(&snapshotter), 2.0);

    second.start().await;
    assert_eq!(retirement_gauge(&snapshotter), 5.0);
    first.stop().await;
    assert_eq!(retirement_gauge(&snapshotter), 3.0);
    second.stop().await;
    assert_eq!(retirement_gauge(&snapshotter), 0.0);
    eprintln!("DUROXIDE_RETIREMENT_OVERLAP counts=2,2,2,5,3,0 series=1");
}

#[test]
fn concurrent_runtime_populations_balance_recorder_wide_deltas() {
    const THREADS: usize = 4;
    const TAILS: usize = 16;
    let recorder = Arc::new(DebuggingRecorder::new());
    let snapshotter = recorder.snapshotter();
    let (ready, ready_receiver) = mpsc::channel();
    let releases: Vec<_> = (0..THREADS).map(|_| Arc::new(Semaphore::new(0))).collect();
    std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for (index, release) in releases.iter().enumerate() {
            let recorder = Arc::clone(&recorder);
            let ready = ready.clone();
            let release = Arc::clone(release);
            threads.push(scope.spawn(move || {
                // Each executor stays on its own thread, sharing one real recorder
                // without installing a process-global recorder for unrelated tests.
                let _metrics = metrics::set_default_local_recorder(recorder.as_ref());
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        let population = Population::prepare(TAILS, index % 2 == 1).await;
                        population.start().await;
                        ready.send(()).unwrap();
                        bounded(release.acquire()).await.unwrap().forget();
                        population.stop().await;
                    });
            }));
        }
        for _ in 0..THREADS {
            ready_receiver.recv_timeout(LIMIT).unwrap();
        }
        assert_eq!(retirement_gauge(&snapshotter), (THREADS * TAILS) as f64);
        for release in releases {
            release.add_permits(1);
        }
        for thread in threads {
            thread.join().unwrap();
        }
    });
    assert_eq!(retirement_gauge(&snapshotter), 0.0);
    eprintln!("DUROXIDE_RETIREMENT_CONCURRENCY threads=4 tails=64 final=0 series=1");
}

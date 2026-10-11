// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
#![allow(clippy::expect_used, clippy::clone_on_ref_ptr)]

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use duroxide::providers::{ExecutionMetadata, WorkItem};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{Runtime, RuntimeOptions};
use duroxide::{Client, Event, EventKind, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::time::Duration;

#[tokio::test]
async fn invalid_stamp_warns_on_every_turn() {
    let (logs, _guard) = common::tracing_capture::install_tracing_capture();
    for stamp in ["", "garbage"] {
        let (provider, _temporary) = common::create_sqlite_store_disk().await;
        let instance = format!("invalid-version-{stamp}");
        let mut start = Event::with_event_id(
            1,
            &instance,
            1,
            None,
            EventKind::OrchestrationStarted {
                name: "Warning".into(),
                version: "1.0.0".into(),
                input: "".into(),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                carry_forward_events: None,
                initial_custom_status: None,
            },
        );
        start.duroxide_version = stamp.into();
        common::seed_history_turn(
            provider.as_ref(),
            WorkItem::StartOrchestration {
                instance: instance.clone(),
                orchestration: "Warning".into(),
                input: "".into(),
                version: Some("1.0.0".into()),
                execution_id: 1,
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
            },
            1,
            vec![start],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("Warning".into()),
                orchestration_version: Some("1.0.0".into()),
                ..Default::default()
            },
        )
        .await;
        let registry = OrchestrationRegistry::builder()
            .register("Warning", |ctx: OrchestrationContext, _: String| async move {
                let first = ctx.dequeue_event("q").await;
                let second = ctx.dequeue_event("q").await;
                Ok(format!("{first},{second}"))
            })
            .build();
        let runtime = Runtime::start_with_options(
            provider.clone(),
            ActivityRegistry::builder().build(),
            registry,
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .await;
        let client = Client::new(provider.clone());
        client
            .enqueue_event(&instance, "q", "first")
            .await
            .expect("first input must enqueue");
        assert!(
            common::wait_for_history(
                provider.clone(),
                &instance,
                |history| history
                    .iter()
                    .filter(|event| matches!(event.kind, EventKind::QueueSubscribed { .. }))
                    .count()
                    == 2,
                30_000,
            )
            .await,
            "first turn must reach its second subscription before the next input"
        );
        client
            .enqueue_event(&instance, "q", "second")
            .await
            .expect("second input must enqueue");
        let result = client.wait_for_orchestration(&instance, Duration::from_secs(30)).await;
        runtime.shutdown(None).await;
        assert!(matches!(
            result.expect("legacy sequential workflow must complete"),
            OrchestrationStatus::Completed { ref output, .. } if output == "first,second"
        ));
        let warnings = logs
            .lock()
            .expect("capture lock must not be poisoned")
            .iter()
            .filter(|event| {
                event.message.contains("Invalid pinned version")
                    && event.field("instance").as_deref() == Some(instance.as_str())
            })
            .count();
        // At least two turns ran (both reads need separate inputs): the warning is not
        // limited to the first. The legacy fallback itself is covered by q14 and ms8.
        assert!(
            warnings >= 2,
            "{stamp:?}: every turn must report the invalid stamp, got {warnings}"
        );
    }
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions, UnregisteredBackoffConfig};
use duroxide::{Client, EventKind, OrchestrationRegistry};
use std::time::Duration;
mod common;

/// Test that unregistered orchestrations eventually poison after backoff attempts.
///
/// Tests both unversioned and versioned orchestration starts:
/// - Unregistered orchestration name → backoff → poison
/// - Registered name but unregistered version → backoff → poison
///
/// With the new backoff logic:
/// - Unregistered orchestrations are abandoned with exponential backoff
/// - After max_attempts, the message is marked as poison
/// - No configuration error is created - the poison error is what we get
#[tokio::test]
async fn unknown_orchestration_fails_with_poison() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Register v1.0.0 only - used to test version mismatch
    let orchestration_registry = OrchestrationRegistry::builder()
        .register_versioned(
            "VersionedOrch",
            "1.0.0",
            |_ctx: duroxide::OrchestrationContext, input: String| async move { Ok(format!("v1: {input}")) },
        )
        .build();
    let activity_registry = ActivityRegistry::builder().build();

    // Use fast backoff for testing
    let options = RuntimeOptions {
        max_attempts: 3,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        unregistered_backoff: UnregisteredBackoffConfig {
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(50),
        },
        ..Default::default()
    };

    let rt =
        runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestration_registry, options).await;

    let client = duroxide::Client::new(store.clone());

    // Test 1: Completely unknown orchestration name
    client
        .start_orchestration("inst-unknown-name", "DoesNotExist", "")
        .await
        .unwrap();

    // Test 2: Known orchestration name but unknown version
    client
        .start_orchestration_versioned("inst-unknown-version", "VersionedOrch", "9.9.9", "input")
        .await
        .unwrap();

    // Both should poison
    for instance in ["inst-unknown-name", "inst-unknown-version"] {
        let status = client
            .wait_for_orchestration(instance, std::time::Duration::from_secs(10))
            .await
            .unwrap();

        let details = match status {
            duroxide::OrchestrationStatus::Failed { details, .. } => details,
            duroxide::OrchestrationStatus::Completed { output, .. } => {
                panic!("expected failure for {instance}, got success: {output}")
            }
            _ => panic!("unexpected orchestration status for {instance}"),
        };

        // Should eventually result in poison, not configuration error
        assert!(
            matches!(
                details,
                duroxide::ErrorDetails::Poison {
                    attempt_count,
                    max_attempts,
                    ..
                } if attempt_count > max_attempts
            ),
            "Expected Poison error for {instance}, got: {details:?}"
        );

        let hist = client.read_execution_history(instance, 1).await.unwrap();

        // History should show poison failure
        assert!(
            hist.iter().any(|e| {
                matches!(
                    &e.kind,
                    EventKind::OrchestrationFailed { details, .. } if matches!(
                        details,
                        duroxide::ErrorDetails::Poison { .. }
                    )
                )
            }),
            "Expected poison failure in history for {instance}"
        );
    }

    rt.shutdown(None).await;
}

/// Test that unregistered activities eventually poison after backoff attempts.
#[tokio::test]
async fn unknown_activity_fails_with_poison() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Register orchestration but not the activity it calls
    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "SimpleOrch",
            |ctx: duroxide::OrchestrationContext, _input: String| async move {
                // Try to call an activity that isn't registered
                let result = ctx.schedule_activity("UnknownActivity", "input").await?;
                Ok(result)
            },
        )
        .build();

    let activity_registry = ActivityRegistry::builder().build();

    // Use fast backoff for testing
    let options = RuntimeOptions {
        max_attempts: 3,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        unregistered_backoff: UnregisteredBackoffConfig {
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(50),
        },
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activity_registry, orchestrations, options).await;

    let client = duroxide::Client::new(store.clone());
    client
        .start_orchestration("inst-activity-unknown", "SimpleOrch", "")
        .await
        .unwrap();

    let client = duroxide::Client::new(store.clone());
    let status = client
        .wait_for_orchestration("inst-activity-unknown", std::time::Duration::from_secs(10))
        .await
        .unwrap();

    // Orchestration should fail because the activity eventually poisoned
    // When an activity poisons, the orchestration fails with Poison error details
    match status {
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(details, duroxide::ErrorDetails::Poison { .. }),
                "Expected Poison error from activity poison, got: {details:?}"
            );
        }
        _ => panic!("expected failure"),
    }

    rt.shutdown(None).await;
}

/// Test backoff calculation with different configurations.
#[test]
fn test_backoff_calculation() {
    let config = UnregisteredBackoffConfig {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(60),
    };

    // Attempt 1: 1s
    assert_eq!(config.delay(1), Duration::from_secs(1));

    // Attempt 2: 2s
    assert_eq!(config.delay(2), Duration::from_secs(2));

    // Attempt 3: 4s
    assert_eq!(config.delay(3), Duration::from_secs(4));

    // Attempt 4: 8s
    assert_eq!(config.delay(4), Duration::from_secs(8));

    // Attempt 5: 16s
    assert_eq!(config.delay(5), Duration::from_secs(16));

    // Attempt 6: 32s
    assert_eq!(config.delay(6), Duration::from_secs(32));

    // Attempt 7+: capped at 60s
    assert_eq!(config.delay(7), Duration::from_secs(60));
    assert_eq!(config.delay(8), Duration::from_secs(60));
    assert_eq!(config.delay(10), Duration::from_secs(60));
}

/// Test backoff respects custom config.
#[test]
fn test_backoff_custom_config() {
    let config = UnregisteredBackoffConfig {
        base_delay: Duration::from_millis(100),
        max_delay: Duration::from_millis(500),
    };

    // Attempt 1: 100ms
    assert_eq!(config.delay(1), Duration::from_millis(100));

    // Attempt 2: 200ms
    assert_eq!(config.delay(2), Duration::from_millis(200));

    // Attempt 3: 400ms
    assert_eq!(config.delay(3), Duration::from_millis(400));

    // Attempt 4: 800ms -> capped at 500ms
    assert_eq!(config.delay(4), Duration::from_millis(500));

    // All subsequent: 500ms
    assert_eq!(config.delay(5), Duration::from_millis(500));
    assert_eq!(config.delay(10), Duration::from_millis(500));
}

/// Test: Continue-as-new to missing version fails gracefully with poison
///
/// When an orchestration does continue_as_new to a version that doesn't exist,
/// it should follow the backoff → poison flow.
#[tokio::test]
async fn continue_as_new_to_missing_version_fails_with_poison() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // Register v1.0.0 that does CAN to non-existent v3.0.0
    let orchestrations = OrchestrationRegistry::builder()
        .register_versioned(
            "CanToMissing",
            "1.0.0",
            |ctx: duroxide::OrchestrationContext, _input: String| async move {
                // Continue as new to a version that doesn't exist
                ctx.continue_as_new_versioned("3.0.0", "new-input").await
            },
        )
        .build();

    let activities = ActivityRegistry::builder().build();

    let options = RuntimeOptions {
        max_attempts: 3,
        dispatcher_min_poll_interval: Duration::from_millis(10),
        unregistered_backoff: UnregisteredBackoffConfig {
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(50),
        },
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());

    // Start at v1.0.0 - it will CAN to v3.0.0 which doesn't exist
    client
        .start_orchestration_versioned("can-missing-test", "CanToMissing", "1.0.0", "start")
        .await
        .expect("start should succeed");

    // Wait for it to poison (after the CAN)
    let status = client
        .wait_for_orchestration("can-missing-test", Duration::from_secs(10))
        .await
        .expect("wait should succeed");

    // Should fail with poison error (the v3.0.0 execution poisoned)
    match status {
        duroxide::OrchestrationStatus::Failed { details, .. } => {
            assert!(
                matches!(details, duroxide::ErrorDetails::Poison { .. }),
                "Expected Poison error from CAN to missing version, got: {details:?}"
            );
        }
        _ => panic!("Expected failure status"),
    }

    rt.shutdown(None).await;
}

/// Test: Delete poisoned orchestration (no force needed)
///
/// After an orchestration poisons from unregistered backoff,
/// it should be deletable without force since it's in Failed state.
#[tokio::test]
async fn delete_poisoned_orchestration() {
    let (store, _td) = common::create_sqlite_store_disk().await;

    // No orchestrations registered - will poison after max_attempts
    let orchestrations = OrchestrationRegistry::builder().build();
    let activities = ActivityRegistry::builder().build();

    let options = RuntimeOptions {
        max_attempts: 3, // Low so it poisons quickly
        dispatcher_min_poll_interval: Duration::from_millis(10),
        unregistered_backoff: UnregisteredBackoffConfig {
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(50),
        },
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;

    let client = Client::new(store.clone());

    // Start unregistered orchestration - will poison
    let instance = "delete-after-poison";
    client
        .start_orchestration(instance, "UnregisteredOrch", "input")
        .await
        .expect("start should succeed");

    // Wait for poison
    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(10))
        .await
        .expect("wait should succeed");

    // Verify it poisoned
    assert!(
        matches!(status, duroxide::OrchestrationStatus::Failed { .. }),
        "Should have failed with poison"
    );

    // Delete without force (should work since it's completed/failed)
    let delete_result = client.delete_instance(instance, false).await;
    assert!(
        delete_result.is_ok(),
        "Delete should succeed for poisoned orchestration"
    );

    // Verify it's gone - get_orchestration_status returns NotFound for deleted instances
    let status = client.get_orchestration_status(instance).await;
    assert!(
        matches!(status, Ok(duroxide::OrchestrationStatus::NotFound)),
        "Instance should be NotFound after delete, got: {status:?}"
    );

    rt.shutdown(None).await;
}

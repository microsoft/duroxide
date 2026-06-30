// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Tests for registry composition features (merge, builder_from, register_all)
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Helper orchestrations
async fn orch1(_ctx: OrchestrationContext, input: String) -> Result<String, String> {
    Ok(format!("orch1: {input}"))
}

async fn orch2(_ctx: OrchestrationContext, input: String) -> Result<String, String> {
    Ok(format!("orch2: {input}"))
}

async fn orch3(_ctx: OrchestrationContext, input: String) -> Result<String, String> {
    Ok(format!("orch3: {input}"))
}

// Helper activities
async fn activity1(_ctx: ActivityContext, input: String) -> Result<String, String> {
    Ok(format!("activity1: {input}"))
}

async fn activity2(_ctx: ActivityContext, input: String) -> Result<String, String> {
    Ok(format!("activity2: {input}"))
}

async fn activity3(_ctx: ActivityContext, input: String) -> Result<String, String> {
    Ok(format!("activity3: {input}"))
}

#[test]
fn test_orchestration_registry_merge() {
    // Create first registry
    let registry1 = OrchestrationRegistry::builder()
        .register("orch1", orch1)
        .register("orch2", orch2)
        .build();

    // Create second registry
    let registry2 = OrchestrationRegistry::builder().register("orch3", orch3).build();

    // Merge both into a new registry
    let combined = OrchestrationRegistry::builder()
        .merge(registry1)
        .merge(registry2)
        .build();

    // Verify all three orchestrations are present
    let names = combined.list_names();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"orch1".to_string()));
    assert!(names.contains(&"orch2".to_string()));
    assert!(names.contains(&"orch3".to_string()));
}

#[test]
fn test_orchestration_registry_builder_from() {
    // Create base registry
    let base = OrchestrationRegistry::builder()
        .register("orch1", orch1)
        .register("orch2", orch2)
        .build();

    // Extend it with builder_from
    let extended = OrchestrationRegistry::builder_from(&base)
        .register("orch3", orch3)
        .build();

    // Verify all three orchestrations are present
    let names = extended.list_names();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"orch1".to_string()));
    assert!(names.contains(&"orch2".to_string()));
    assert!(names.contains(&"orch3".to_string()));

    // Verify base registry is unchanged
    let base_names = base.list_names();
    assert_eq!(base_names.len(), 2);
}

#[test]
fn test_orchestration_registry_chained_register() {
    // register_all requires same function types, so we use chained .register() instead
    let registry = OrchestrationRegistry::builder()
        .register("orch1", orch1)
        .register("orch2", orch2)
        .register("orch3", orch3)
        .build();

    let names = registry.list_names();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"orch1".to_string()));
    assert!(names.contains(&"orch2".to_string()));
    assert!(names.contains(&"orch3".to_string()));
}

#[test]
fn test_orchestration_registry_merge_with_chained_register() {
    let registry1 = OrchestrationRegistry::builder().register("orch1", orch1).build();

    let combined = OrchestrationRegistry::builder()
        .merge(registry1)
        .register("orch2", orch2)
        .register("orch3", orch3)
        .build();

    let names = combined.list_names();
    assert_eq!(names.len(), 3);
}

#[test]
fn test_activity_registry_merge() {
    // Create first registry
    let registry1 = ActivityRegistry::builder()
        .register("activity1", activity1)
        .register("activity2", activity2)
        .build();

    // Create second registry
    let registry2 = ActivityRegistry::builder().register("activity3", activity3).build();

    // Merge both into a new registry
    let combined = ActivityRegistry::builder().merge(registry1).merge(registry2).build();

    // Verify all three activities are present
    assert!(combined.has("activity1"));
    assert!(combined.has("activity2"));
    assert!(combined.has("activity3"));
}

#[test]
fn test_activity_registry_from_registry() {
    // Create base registry
    let base = ActivityRegistry::builder()
        .register("activity1", activity1)
        .register("activity2", activity2)
        .build();

    // Extend it with from_registry
    let extended = ActivityRegistry::builder_from(&base)
        .register("activity3", activity3)
        .build();

    // Verify all three activities are present
    assert!(extended.has("activity1"));
    assert!(extended.has("activity2"));
    assert!(extended.has("activity3"));

    // Verify base registry is unchanged
    assert!(base.has("activity1"));
    assert!(base.has("activity2"));
    assert!(!base.has("activity3"));
}

#[test]
fn test_activity_registry_chained_register() {
    // register_all requires same function types, so we use chained .register() instead
    let registry = ActivityRegistry::builder()
        .register("activity1", activity1)
        .register("activity2", activity2)
        .register("activity3", activity3)
        .build();

    assert!(registry.has("activity1"));
    assert!(registry.has("activity2"));
    assert!(registry.has("activity3"));
}

#[test]
fn test_activity_registry_merge_with_chained_register() {
    let registry1 = ActivityRegistry::builder().register("activity1", activity1).build();

    let combined = ActivityRegistry::builder()
        .merge(registry1)
        .register("activity2", activity2)
        .register("activity3", activity3)
        .build();

    assert!(combined.has("activity1"));
    assert!(combined.has("activity2"));
    assert!(combined.has("activity3"));
}

#[test]
fn test_activity_registry_builder_detects_duplicates() {
    let result = ActivityRegistry::builder()
        .register("activity1", activity1)
        .register("activity1", activity2)
        .build_result();

    assert!(result.is_err());
    assert!(result.err().unwrap().contains("duplicate activity registration"));
}

#[test]
fn test_activity_registry_merge_duplicate_errors() {
    let registry1 = ActivityRegistry::builder().register("activity1", activity1).build();
    let registry2 = ActivityRegistry::builder().register("activity1", activity2).build();

    let result = ActivityRegistry::builder()
        .merge(registry1)
        .merge(registry2)
        .build_result();
    assert!(result.is_err());
    assert!(result.err().unwrap().contains("duplicate activity in merge"));
}

// ---------------------------------------------------------------------------
// Registry Validation: Reserved Prefix / Builtins
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_reserved_prefix_rejected() {
    let result = ActivityRegistry::builder()
        .register("__duroxide_syscall:evil", |_: ActivityContext, _: String| async {
            Ok("evil".to_string())
        })
        .build_result();

    match result {
        Ok(_) => panic!("Should fail to register reserved prefix"),
        Err(err) => {
            assert!(
                err.contains("uses reserved prefix"),
                "Error should mention reserved prefix: {err}"
            );
            assert!(
                err.contains("__duroxide_syscall:"),
                "Error should mention the prefix: {err}"
            );
        }
    }
}

#[tokio::test]
async fn test_builtins_exist_with_empty_registry() {
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    // Empty registry - builtins should be injected automatically
    let activities = ActivityRegistry::builder().build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("TestBuiltins", |ctx: OrchestrationContext, _: String| async move {
            let _guid = ctx.new_guid().await?;
            let _time = ctx.utc_now().await?;
            Ok("ok".to_string())
        })
        .build();

    let _rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store);

    client
        .start_orchestration("test-builtins", "TestBuiltins", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("test-builtins", Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "ok"),
        other => panic!("Expected Completed, got {other:?}"),
    }
}

#[tokio::test]
async fn test_user_cannot_override_builtins() {
    let result = ActivityRegistry::builder()
        .register("__duroxide_syscall:new_guid", |_: ActivityContext, _: String| async {
            Ok("".into())
        })
        .build_result();

    match result {
        Ok(_) => panic!("Should fail to register reserved builtin name"),
        Err(err) => {
            assert!(
                err.contains("uses reserved prefix"),
                "Error should mention reserved prefix: {err}"
            );
            assert!(
                err.contains("__duroxide_syscall:"),
                "Error should mention the prefix: {err}"
            );
        }
    }
}

#[tokio::test]
async fn test_register_versioned_typed() {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct MyInput {
        value: i32,
    }

    #[derive(Serialize, Deserialize)]
    struct MyOutput {
        result: i32,
    }

    async fn typed_orch(_ctx: OrchestrationContext, input: MyInput) -> Result<MyOutput, String> {
        Ok(MyOutput {
            result: input.value * 2,
        })
    }

    let registry = OrchestrationRegistry::builder()
        .register_versioned_typed("typed-orch", "2.0.0", typed_orch)
        .build();

    // Verify it's registered
    let names = registry.list_names();
    assert!(names.contains(&"typed-orch".to_string()));

    // Verify version
    let versions = registry.list_versions("typed-orch");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].to_string(), "2.0.0");
}

#[tokio::test]
async fn activity_context_metadata() {
    #[derive(Debug, PartialEq, Eq)]
    struct RecordedMetadata {
        instance_id: String,
        execution_id: u64,
        orchestration_name: String,
        orchestration_version: String,
        activity_name: String,
    }

    let recorded = Arc::new(Mutex::new(Vec::<RecordedMetadata>::new()));
    let recorded_for_activity = recorded.clone();

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let activities = ActivityRegistry::builder()
        .register("Inspect", move |ctx: ActivityContext, _input: String| {
            let recorded_for_activity = recorded_for_activity.clone();
            async move {
                recorded_for_activity.lock().unwrap().push(RecordedMetadata {
                    instance_id: ctx.instance_id().to_string(),
                    execution_id: ctx.execution_id(),
                    orchestration_name: ctx.orchestration_name().to_string(),
                    orchestration_version: ctx.orchestration_version().to_string(),
                    activity_name: ctx.activity_name().to_string(),
                });
                Ok("ok".to_string())
            }
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register("InspectOrch", |ctx: OrchestrationContext, _input: String| async move {
            ctx.schedule_activity("Inspect", "payload").await
        })
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());
    client
        .start_orchestration("registry-test-instance", "InspectOrch", "")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("registry-test-instance", std::time::Duration::from_secs(5))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "ok"),
        OrchestrationStatus::Failed { details, .. } => {
            panic!("orchestration failed: {}", details.display_message())
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    rt.shutdown(None).await;

    let records = recorded.lock().unwrap();
    assert_eq!(records.len(), 1, "expected exactly one activity execution");
    let record = &records[0];
    assert_eq!(record.instance_id, "registry-test-instance");
    assert_eq!(record.execution_id, 1);
    assert_eq!(record.orchestration_name, "InspectOrch");
    assert_eq!(record.orchestration_version, "1.0.0");
    assert_eq!(record.activity_name, "Inspect");
}

#[tokio::test]
async fn test_cross_crate_composition_pattern() {
    // Simulate library crate 1
    fn create_azure_registry() -> OrchestrationRegistry {
        OrchestrationRegistry::builder()
            .register("duroxide-azure-arm::orchestration::provision-postgres", orch1)
            .register("duroxide-azure-arm::orchestration::deploy-webapp", orch2)
            .build()
    }

    // Simulate library crate 2
    fn create_aws_registry() -> OrchestrationRegistry {
        OrchestrationRegistry::builder()
            .register("duroxide-aws-ec2::orchestration::create-vpc", orch3)
            .build()
    }

    // Consumer code - compose both libraries
    let combined = OrchestrationRegistry::builder()
        .merge(create_azure_registry())
        .merge(create_aws_registry())
        .build();

    // Verify all orchestrations are present
    let names = combined.list_names();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"duroxide-azure-arm::orchestration::provision-postgres".to_string()));
    assert!(names.contains(&"duroxide-azure-arm::orchestration::deploy-webapp".to_string()));
    assert!(names.contains(&"duroxide-aws-ec2::orchestration::create-vpc".to_string()));
}

// Introspection tests

#[test]
fn test_orchestration_registry_list_names() {
    let registry = OrchestrationRegistry::builder()
        .register("orch1", orch1)
        .register("orch2", orch2)
        .build();

    let names = registry.list_names();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"orch1".to_string()));
    assert!(names.contains(&"orch2".to_string()));
}

#[test]
fn test_orchestration_registry_has_and_count() {
    let registry = OrchestrationRegistry::builder()
        .register("orch1", orch1)
        .register("orch2", orch2)
        .build();

    assert!(registry.has("orch1"));
    assert!(registry.has("orch2"));
    assert!(!registry.has("orch3"));
    assert_eq!(registry.count(), 2);
}

#[test]
fn test_orchestration_registry_list_versions() {
    let registry = OrchestrationRegistry::builder()
        .register("orch1", orch1)
        .register_versioned("orch1", "2.0.0", orch2)
        .register_versioned("orch1", "3.0.0", orch3)
        .build();

    let versions = registry.list_versions("orch1");
    assert_eq!(versions.len(), 3);
    assert!(versions.contains(&semver::Version::parse("1.0.0").unwrap()));
    assert!(versions.contains(&semver::Version::parse("2.0.0").unwrap()));
    assert!(versions.contains(&semver::Version::parse("3.0.0").unwrap()));

    // Non-existent orchestration returns empty vec
    let versions = registry.list_versions("non-existent");
    assert_eq!(versions.len(), 0);
}

#[test]
fn test_activity_registry_list_names() {
    let registry = ActivityRegistry::builder()
        .register("activity1", activity1)
        .register("activity2", activity2)
        .register("activity3", activity3)
        .build();

    let names = registry.list_names();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"activity1".to_string()));
    assert!(names.contains(&"activity2".to_string()));
    assert!(names.contains(&"activity3".to_string()));
}

#[test]
fn test_activity_registry_has() {
    let registry = ActivityRegistry::builder()
        .register("activity1", activity1)
        .register("activity2", activity2)
        .build();

    assert!(registry.has("activity1"));
    assert!(registry.has("activity2"));
    assert!(!registry.has("activity3"));
    assert!(!registry.has("non-existent"));
}

#[test]
fn test_activity_registry_count() {
    let empty = ActivityRegistry::builder().build();
    assert_eq!(empty.count(), 0);

    let registry = ActivityRegistry::builder()
        .register("activity1", activity1)
        .register("activity2", activity2)
        .register("activity3", activity3)
        .build();

    assert_eq!(registry.count(), 3);
}

#[test]
fn test_activity_registry_introspection_after_merge() {
    let registry1 = ActivityRegistry::builder()
        .register("lib1::activity1", activity1)
        .register("lib1::activity2", activity2)
        .build();

    let registry2 = ActivityRegistry::builder()
        .register("lib2::activity3", activity3)
        .build();

    let combined = ActivityRegistry::builder().merge(registry1).merge(registry2).build();

    // Test list_activity_names
    let names = combined.list_names();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"lib1::activity1".to_string()));
    assert!(names.contains(&"lib1::activity2".to_string()));
    assert!(names.contains(&"lib2::activity3".to_string()));

    // Test has
    assert!(combined.has("lib1::activity1"));
    assert!(combined.has("lib2::activity3"));
    assert!(!combined.has("non-existent"));

    // Test count
    assert_eq!(combined.count(), 3);
}

#[test]
fn test_registry_default() {
    let reg: OrchestrationRegistry = Default::default();
    assert_eq!(reg.count(), 0);
    assert!(reg.list_names().is_empty());
    assert!(reg.resolve_handler("nonexistent").is_none());
    assert!(reg.resolve_version("nonexistent").is_none());
    assert!(
        reg.resolve_handler_exact("nonexistent", &semver::Version::parse("1.0.0").unwrap())
            .is_none()
    );
    assert_eq!(reg.list_versions("nonexistent").len(), 0);
}

#[test]
fn test_registry_clone_shares_policy() {
    let reg1 = OrchestrationRegistry::builder()
        .register("orch1", orch1)
        .register_versioned("orch1", "2.0.0", orch2)
        .build();
    let reg2 = reg1.clone();

    // Change policy on clone - since policy is Arc<Mutex>, both share the same policy map
    reg2.set_version_policy(
        "orch1",
        duroxide::runtime::VersionPolicy::Exact(semver::Version::parse("1.0.0").unwrap()),
    );

    // Both should see the policy change since they share the Arc
    let (v1, _) = reg1.resolve_handler("orch1").expect("resolve exact");
    assert_eq!(v1, semver::Version::parse("1.0.0").unwrap());

    let (v2, _) = reg2.resolve_handler("orch1").expect("resolve exact");
    assert_eq!(v2, semver::Version::parse("1.0.0").unwrap());
}

#[test]
fn test_empty_registry_resolution() {
    let reg: OrchestrationRegistry = OrchestrationRegistry::builder().build();

    assert!(reg.resolve_handler("nonexistent").is_none());
    assert!(reg.resolve_version("nonexistent").is_none());
    assert!(
        reg.resolve_handler_exact("nonexistent", &semver::Version::parse("1.0.0").unwrap())
            .is_none()
    );
    assert_eq!(reg.list_versions("nonexistent").len(), 0);
    assert!(!reg.has("nonexistent"));
}

#[test]
fn test_resolve_handler_exact_policy_missing_version() {
    let reg = OrchestrationRegistry::builder()
        .register("orch1", orch1) // 1.0.0
        .build();

    reg.set_version_policy(
        "orch1",
        duroxide::runtime::VersionPolicy::Exact(semver::Version::parse("2.0.0").unwrap()),
    );

    // Should return None since 2.0.0 doesn't exist
    assert!(reg.resolve_handler("orch1").is_none());
    assert!(reg.resolve_version("orch1").is_none());
}

#[test]
fn test_resolve_version_latest() {
    let reg = OrchestrationRegistry::builder()
        .register("orch1", orch1)
        .register_versioned("orch1", "2.0.0", orch2)
        .build();

    let v = reg.resolve_version("orch1").expect("resolve version");
    assert_eq!(v, semver::Version::parse("2.0.0").unwrap());
}

#[test]
fn test_resolve_version_exact() {
    let reg = OrchestrationRegistry::builder()
        .register("orch1", orch1)
        .register_versioned("orch1", "2.0.0", orch2)
        .build();

    reg.set_version_policy(
        "orch1",
        duroxide::runtime::VersionPolicy::Exact(semver::Version::parse("1.0.0").unwrap()),
    );
    let v = reg.resolve_version("orch1").expect("resolve version");
    assert_eq!(v, semver::Version::parse("1.0.0").unwrap());
}

#[test]
fn test_activity_always_1_0_0_and_latest() {
    let reg = ActivityRegistry::builder().register("activity1", activity1).build();

    let versions = reg.list_versions("activity1");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0], semver::Version::parse("1.0.0").unwrap());

    // Policy should be Latest (can verify via resolve)
    let (v, _h) = reg.resolve_handler("activity1").expect("resolve handler");
    assert_eq!(v, semver::Version::parse("1.0.0").unwrap());
}

#[test]
fn test_activity_register_typed() {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct Input {
        value: i32,
    }

    #[derive(Serialize, Deserialize)]
    struct Output {
        result: i32,
    }

    let reg = ActivityRegistry::builder()
        .register_typed("typed-activity", |_ctx: ActivityContext, input: Input| async move {
            Ok(Output {
                result: input.value * 2,
            })
        })
        .build();

    assert!(reg.has("typed-activity"));
    let (v, _h) = reg.resolve_handler("typed-activity").expect("resolve handler");
    assert_eq!(v, semver::Version::parse("1.0.0").unwrap());
}

#[test]
fn test_register_all() {
    let handler = |_ctx: OrchestrationContext, input: String| async move { Ok(format!("processed: {input}")) };

    let reg = OrchestrationRegistry::builder()
        .register_all(vec![("orch1", handler), ("orch2", handler), ("orch3", handler)])
        .build();

    assert_eq!(reg.count(), 3);
    assert!(reg.has("orch1"));
    assert!(reg.has("orch2"));
    assert!(reg.has("orch3"));
}

#[test]
fn test_list_versions_ordering() {
    let reg = OrchestrationRegistry::builder()
        .register("orch1", orch1) // 1.0.0
        .register_versioned("orch1", "2.0.0", orch2)
        .register_versioned("orch1", "3.0.0", orch3)
        .build();

    let versions = reg.list_versions("orch1");
    // BTreeMap maintains sorted order
    assert_eq!(versions.len(), 3);
    assert_eq!(versions[0], semver::Version::parse("1.0.0").unwrap());
    assert_eq!(versions[1], semver::Version::parse("2.0.0").unwrap());
    assert_eq!(versions[2], semver::Version::parse("3.0.0").unwrap());
}

#[test]
fn test_multiple_policies() {
    let reg = OrchestrationRegistry::builder()
        .register("orch1", orch1)
        .register_versioned("orch1", "2.0.0", orch2)
        .register("orch2", orch3)
        .register_versioned("orch2", "2.0.0", orch1)
        .build();

    reg.set_version_policy(
        "orch1",
        duroxide::runtime::VersionPolicy::Exact(semver::Version::parse("1.0.0").unwrap()),
    );
    reg.set_version_policy("orch2", duroxide::runtime::VersionPolicy::Latest);

    let (v1, _) = reg.resolve_handler("orch1").expect("resolve orch1");
    assert_eq!(v1, semver::Version::parse("1.0.0").unwrap());

    let (v2, _) = reg.resolve_handler("orch2").expect("resolve orch2");
    assert_eq!(v2, semver::Version::parse("2.0.0").unwrap());
}
